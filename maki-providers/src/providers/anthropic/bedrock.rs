use std::env;
use std::fmt::Display;
use std::ops::ControlFlow;
use std::path::PathBuf;
use std::pin::pin;
use std::str;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aws_smithy_eventstream::frame::{DecodedFrame, MessageFrameDecoder};
use aws_smithy_eventstream::smithy::parse_response_headers;
use aws_smithy_types::event_stream::Message as FrameMessage;
use base64::Engine;
use flume::Sender;
use futures::{Stream, StreamExt, TryStreamExt};
use hmac::{Hmac, KeyInit, Mac};
use maki_storage::id::SessionRef;
use reqwest::Client;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::task::spawn_blocking;
use tracing::{debug, warn};

use crate::model::Model;
use crate::provider::{BoxFuture, Provider};
use crate::{AgentError, Message, ProviderEvent, RequestOptions, StreamResponse};

use super::shared;

pub(crate) const BEARER_TOKEN_ENV: &str = "AWS_BEARER_TOKEN_BEDROCK";
const CONTAINER_CREDENTIALS_URI_ENV: &str = "AWS_CONTAINER_CREDENTIALS_FULL_URI";
const BEDROCK_API_VERSION: &str = "bedrock-2023-05-31";
const MESSAGE_TYPE_EVENT: &str = "event";
const CHUNK_EVENT: &str = "chunk";
const THROTTLING_EXCEPTION: &str = "throttlingException";
const VALIDATION_EXCEPTION: &str = "validationException";
const TRUNCATED_STREAM: &str = "the event stream ended inside a frame";
const CONTAINER_METADATA_TIMEOUT: Duration = Duration::from_secs(5);
const REFRESH_MARGIN: Duration = Duration::from_secs(5 * 60);
const CONTAINER_CREDENTIALS_PENDING: &str =
    "Bedrock container credentials were not fetched before signing the request";

#[derive(Clone)]
enum AuthKind {
    SigV4 {
        access_key: String,
        secret_key: String,
        session_token: Option<String>,
        // epoch seconds; Some only for temporary creds from the container endpoint.
        expires_at: Option<u64>,
    },
    /// Fetched on the first request.
    ContainerPending,
    Bearer {
        token: String,
    },
    None,
}

#[derive(Clone)]
struct BedrockAuth {
    kind: AuthKind,
    region: String,
}

pub(crate) fn is_enabled() -> bool {
    env::var("CLAUDE_CODE_USE_BEDROCK").is_ok_and(|v| v == "1")
}

fn resolve_bedrock_top_p() -> Option<f64> {
    // Bedrock is built from the anthropic branch, so it honours the anthropic
    // slug's `top_p`; there is no `bedrock` section to read.
    maki_config::providers::top_p_for(super::SLUG)
}

fn resolve_bedrock_auth() -> Result<BedrockAuth, AgentError> {
    let region = env::var("AWS_REGION").map_err(|_| AgentError::Config {
        message: "AWS_REGION must be set when using Bedrock".into(),
    })?;

    let kind = if let Ok(token) = env::var(BEARER_TOKEN_ENV) {
        debug!("using Bedrock bearer token auth");
        AuthKind::Bearer { token }
    } else if env::var("CLAUDE_CODE_SKIP_BEDROCK_AUTH").is_ok_and(|v| v == "1") {
        debug!("skipping Bedrock auth (gateway proxy mode)");
        AuthKind::None
    } else if let (Ok(access_key), Ok(secret_key)) = (
        env::var("AWS_ACCESS_KEY_ID"),
        env::var("AWS_SECRET_ACCESS_KEY"),
    ) {
        let session_token = env::var("AWS_SESSION_TOKEN").ok();
        debug!("using Bedrock SigV4 auth from env vars");
        AuthKind::SigV4 {
            access_key,
            secret_key,
            session_token,
            expires_at: None,
        }
    } else if env::var(CONTAINER_CREDENTIALS_URI_ENV).is_ok() {
        debug!("using Bedrock SigV4 auth from container credentials endpoint");
        AuthKind::ContainerPending
    } else {
        let profile = env::var("AWS_PROFILE").unwrap_or_else(|_| "default".into());
        let creds_path = env::var("HOME")
            .map(|h| PathBuf::from(h).join(".aws").join("credentials"))
            .unwrap_or_default();
        if let Ok(content) = std::fs::read_to_string(&creds_path)
            && let Ok((access_key, secret_key, session_token)) =
                parse_aws_credentials_file(&content, &profile)
        {
            debug!(profile = %profile, "using Bedrock SigV4 auth from credentials file");
            AuthKind::SigV4 {
                access_key,
                secret_key,
                session_token,
                expires_at: None,
            }
        } else {
            return Err(AgentError::Config {
                message: "no AWS credentials found: set AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY, AWS_PROFILE, AWS_CONTAINER_CREDENTIALS_FULL_URI, or AWS_BEARER_TOKEN_BEDROCK".into(),
            });
        }
    };

    Ok(BedrockAuth { kind, region })
}

fn parse_aws_credentials_file(
    content: &str,
    profile: &str,
) -> Result<(String, String, Option<String>), AgentError> {
    let target_section = format!("[{profile}]");
    let mut in_section = false;
    let mut access_key = None;
    let mut secret_key = None;
    let mut session_token = None;

    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('[') {
            in_section = trimmed == target_section;
            continue;
        }
        if !in_section {
            continue;
        }
        if let Some((key, value)) = trimmed.split_once('=') {
            let key = key.trim();
            let value = value.trim();
            match key {
                "aws_access_key_id" => access_key = Some(value.to_string()),
                "aws_secret_access_key" => secret_key = Some(value.to_string()),
                "aws_session_token" => session_token = Some(value.to_string()),
                _ => {}
            }
        }
    }

    match (access_key, secret_key) {
        (Some(ak), Some(sk)) => Ok((ak, sk, session_token)),
        _ => Err(AgentError::Config {
            message: format!("profile '{profile}' not found or missing keys in credentials file"),
        }),
    }
}

fn fetch_container_auth() -> Result<AuthKind, AgentError> {
    let url = env::var(CONTAINER_CREDENTIALS_URI_ENV).map_err(|_| AgentError::Config {
        message: format!("{CONTAINER_CREDENTIALS_URI_ENV} is no longer set"),
    })?;
    let (access_key, secret_key, session_token, expires_at) = fetch_container_credentials(&url)?;
    Ok(AuthKind::SigV4 {
        access_key,
        secret_key,
        session_token,
        expires_at,
    })
}

fn fetch_container_credentials(
    url: &str,
) -> Result<(String, String, Option<String>, Option<u64>), AgentError> {
    // file rotates, so don't cache it.
    let auth_header = match env::var("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE") {
        Ok(path) => Some(
            std::fs::read_to_string(&path).map_err(|e| AgentError::Config {
                message: format!("read container auth token file {path}: {e}"),
            })?,
        ),
        Err(_) => env::var("AWS_CONTAINER_AUTHORIZATION_TOKEN").ok(),
    };

    let client = super::super::client_builder()
        .connect_timeout(CONTAINER_METADATA_TIMEOUT)
        .timeout(CONTAINER_METADATA_TIMEOUT)
        .build()
        .map_err(|e| AgentError::Config {
            message: format!("container creds http client: {e}"),
        })?;

    let mut builder = client.get(url);
    if let Some(token) = &auth_header {
        builder = builder.header("Authorization", token.trim());
    }
    let request = builder.build()?;

    let (status, body_text) = super::super::wait(super::super::status_and_text(&client, request))
        .map_err(|e| AgentError::Config {
        message: format!("container creds request: {e}"),
    })?;

    if status != 200 {
        return Err(AgentError::Config {
            message: format!("container creds endpoint returned {status}: {body_text}"),
        });
    }

    parse_container_credentials_response(&body_text)
}

fn parse_container_credentials_response(
    body: &str,
) -> Result<(String, String, Option<String>, Option<u64>), AgentError> {
    let v: Value = serde_json::from_str(body)?;
    let access_key = v
        .get("AccessKeyId")
        .and_then(|s| s.as_str())
        .ok_or_else(|| AgentError::Config {
            message: "container creds response missing AccessKeyId".into(),
        })?
        .to_string();
    let secret_key = v
        .get("SecretAccessKey")
        .and_then(|s| s.as_str())
        .ok_or_else(|| AgentError::Config {
            message: "container creds response missing SecretAccessKey".into(),
        })?
        .to_string();
    let session_token = v.get("Token").and_then(|s| s.as_str()).map(str::to_string);
    let expires_at = v
        .get("Expiration")
        .and_then(|s| s.as_str())
        .and_then(parse_iso8601_to_epoch);
    Ok((access_key, secret_key, session_token, expires_at))
}

// Hand-rolled to avoid pulling in chrono just for this one field.
fn parse_iso8601_to_epoch(s: &str) -> Option<u64> {
    let s = s.strip_suffix('Z')?;
    let (date, time) = s.split_once('T')?;
    let mut date_parts = date.split('-');
    let year: u64 = date_parts.next()?.parse().ok()?;
    let month: u64 = date_parts.next()?.parse().ok()?;
    let day: u64 = date_parts.next()?.parse().ok()?;
    let time = time.split('.').next()?; // drop fractional seconds
    let mut time_parts = time.split(':');
    let hour: u64 = time_parts.next()?.parse().ok()?;
    let minute: u64 = time_parts.next()?.parse().ok()?;
    let second: u64 = time_parts.next()?.parse().ok()?;
    Some(ymdhms_to_epoch(year, month, day, hour, minute, second))
}

// Inverse of days_to_ymd. Howard Hinnant's algorithm; valid for any civil date.
fn ymdhms_to_epoch(year: u64, month: u64, day: u64, hour: u64, minute: u64, second: u64) -> u64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = y / 400;
    let yoe = y - era * 400;
    let m = month as i64;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + day as i64 - 1;
    let doe = yoe as i64 * 365 + (yoe / 4) as i64 - (yoe / 100) as i64 + doy;
    let days_since_epoch = (era as i64 * 146097 + doe - 719468) as u64;
    days_since_epoch * 86400 + hour * 3600 + minute * 60 + second
}

#[allow(clippy::too_many_arguments)]
fn sign_request_sigv4(
    method: &str,
    url: &str,
    headers: &[(&str, &str)],
    body: &[u8],
    access_key: &str,
    secret_key: &str,
    session_token: Option<&str>,
    region: &str,
    service: &str,
    timestamp: &str,
) -> Vec<(String, String)> {
    let date = &timestamp[..8];

    let (_, path, query) = parse_url(url);
    let payload_hash = hex_sha256(body);

    // 'host' belongs to the HTTP request, so the caller already sets it. If we add
    // it here too it ends up signed twice and AWS rejects the signature.
    let mut canonical_headers: Vec<(&str, String)> =
        headers.iter().map(|(k, v)| (*k, v.to_string())).collect();
    canonical_headers.push(("x-amz-date", timestamp.to_string()));
    if let Some(tok) = session_token {
        canonical_headers.push(("x-amz-security-token", tok.to_string()));
    }
    canonical_headers.sort_by(|a, b| a.0.cmp(b.0));

    let canonical_headers_str: String = canonical_headers
        .iter()
        .map(|(k, v)| format!("{k}:{v}\n"))
        .collect();

    let signed_headers: String = canonical_headers
        .iter()
        .map(|(k, _)| *k)
        .collect::<Vec<_>>()
        .join(";");

    // Bedrock model IDs contain ':', which the URL already spells as '%3A'. SigV4
    // wants the path percent-encoded again when it goes into the canonical request,
    // so '%3A' becomes '%253A'. Skip this and the signature will not match.
    let canonical_path = encode_path(path);
    let canonical_request = format!(
        "{method}\n{canonical_path}\n{query}\n{canonical_headers_str}\n{signed_headers}\n{payload_hash}"
    );

    let credential_scope = format!("{date}/{region}/{service}/aws4_request");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{timestamp}\n{credential_scope}\n{}",
        hex_sha256(canonical_request.as_bytes())
    );

    let signing_key = derive_signing_key(secret_key, date, region, service);
    let signature = hex_encode(&hmac_sha256(&signing_key, string_to_sign.as_bytes()));

    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={access_key}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature}"
    );

    let mut result = vec![
        ("Authorization".into(), authorization),
        ("x-amz-date".into(), timestamp.into()),
    ];
    if let Some(tok) = session_token {
        result.push(("x-amz-security-token".into(), tok.into()));
    }
    result
}

fn encode_path(path: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = String::with_capacity(path.len());
    for b in path.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => {
                out.push(b as char);
            }
            _ => {
                out.push('%');
                out.push(HEX[(b >> 4) as usize] as char);
                out.push(HEX[(b & 0xF) as usize] as char);
            }
        }
    }
    out
}

fn parse_url(url: &str) -> (&str, &str, &str) {
    let after_scheme = url
        .strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))
        .unwrap_or(url);
    let (host, path_and_query) = after_scheme.split_once('/').unwrap_or((after_scheme, ""));
    let full_path = if path_and_query.is_empty() {
        "/"
    } else {
        // the leading slash got split off, so we grab it back from the original url
        let path_start = url.len() - path_and_query.len() - 1;
        &url[path_start..]
    };
    let (path, query) = full_path.split_once('?').unwrap_or((full_path, ""));
    (host, path, query)
}

fn hex_sha256(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex_encode(&hasher.finalize())
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn derive_signing_key(secret_key: &str, date: &str, region: &str, service: &str) -> Vec<u8> {
    let k_date = hmac_sha256(format!("AWS4{secret_key}").as_bytes(), date.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    hmac_sha256(&k_service, b"aws4_request")
}

fn hex_encode(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}

/// A stream nothing here can read, which trying again won't change: a gateway's
/// login page on a 200 fails exactly the same way every time.
fn malformed_stream(e: impl Display) -> AgentError {
    AgentError::Config {
        message: format!("Bedrock sent a malformed event stream: {e}"),
    }
}

fn exception_status(name: &str) -> u16 {
    if name.eq_ignore_ascii_case(THROTTLING_EXCEPTION) {
        429
    } else if name.eq_ignore_ascii_case(VALIDATION_EXCEPTION) {
        400
    } else {
        500
    }
}

/// The payload of a chunk event, nothing for any other event, or the error an
/// exception frame carries.
fn chunk_payload(message: &FrameMessage) -> Result<Option<&[u8]>, AgentError> {
    let headers = parse_response_headers(message).map_err(malformed_stream)?;
    let name = headers.smithy_type.as_str();
    if headers.message_type.as_str() == MESSAGE_TYPE_EVENT {
        let payload = &message.payload()[..];
        return Ok((name == CHUNK_EVENT && !payload.is_empty()).then_some(payload));
    }
    Err(AgentError::api(
        exception_status(name),
        String::from_utf8_lossy(message.payload()),
    ))
}

/// The decoder keeps a prelude it has read, so a stream ending mid-frame can leave
/// the buffer empty while half a message is still pending: `mid_frame` tracks that.
/// A dropped connection surfaces as an error from `chunks` and is retried. A body
/// that ends cleanly mid-frame isn't, because the decoder only checks CRCs on a
/// whole frame, so this is also what a gateway's login page on a 200 looks like.
async fn read_events<B: AsRef<[u8]>>(
    chunks: impl Stream<Item = Result<B, AgentError>>,
    event_tx: &Sender<ProviderEvent>,
) -> Result<StreamResponse, AgentError> {
    let mut chunks = pin!(chunks);
    let mut parser = shared::EventParser::new();
    let mut decoder = MessageFrameDecoder::new();
    let mut frame_buf = Vec::new();
    let mut mid_frame = false;

    while let Some(chunk) = chunks.next().await {
        frame_buf.extend_from_slice(chunk?.as_ref());
        let mut unread = &frame_buf[..];
        loop {
            let before = unread.len();
            let DecodedFrame::Complete(message) = decoder
                .decode_frame(&mut unread)
                .map_err(malformed_stream)?
            else {
                mid_frame |= unread.len() < before;
                break;
            };
            mid_frame = false;
            let Some(payload) = chunk_payload(&message)? else {
                continue;
            };
            let (event_type, json) = decode_event_payload(payload)?;
            if let ControlFlow::Break(()) = parser.process(&event_type, &json, event_tx).await? {
                return Ok(parser.finish());
            }
        }
        let consumed = frame_buf.len() - unread.len();
        frame_buf.drain(..consumed);
    }

    if mid_frame || !frame_buf.is_empty() {
        return Err(malformed_stream(TRUNCATED_STREAM));
    }
    Ok(parser.finish())
}

pub(crate) struct Bedrock {
    client: Client,
    auth: Arc<Mutex<BedrockAuth>>,
    base_url: Option<String>,
    top_p: Option<f64>,
}

impl Bedrock {
    pub fn new(timeouts: super::super::Timeouts) -> Result<Self, AgentError> {
        // is_available() upstream calls new().is_ok() and discards the error,
        // so the user sees a generic "no provider available" message. Surface
        // the real reason here before that happens.
        let auth = resolve_bedrock_auth().inspect_err(|e| {
            warn!(error = %e, "Bedrock auth resolution failed");
        })?;
        let base_url = env::var("ANTHROPIC_BEDROCK_BASE_URL").ok();
        Ok(Self {
            client: super::super::http_client(timeouts),
            auth: Arc::new(Mutex::new(auth)),
            base_url,
            top_p: resolve_bedrock_top_p(),
        })
    }

    fn needs_refresh(&self) -> bool {
        let auth = self.auth.lock().unwrap();
        match &auth.kind {
            AuthKind::ContainerPending => true,
            AuthKind::SigV4 {
                expires_at: Some(exp),
                ..
            } => {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0);
                now + REFRESH_MARGIN.as_secs() >= *exp
            }
            _ => false,
        }
    }
}

impl Provider for Bedrock {
    fn stream_message<'a>(
        &'a self,
        model: &'a Model,
        messages: &'a [Message],
        system: &'a str,
        tools: &'a Value,
        event_tx: &'a Sender<ProviderEvent>,
        opts: RequestOptions,
        _session_id: Option<&'a SessionRef>,
    ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
        Box::pin(async move {
            if self.needs_refresh() {
                debug!("Bedrock container creds missing or near expiry, fetching before request");
                let kind = spawn_blocking(fetch_container_auth).await??;
                self.auth.lock().unwrap().kind = kind;
            }
            let auth = self.auth.lock().unwrap().clone();
            let requested_id = env::var("ANTHROPIC_MODEL").unwrap_or_else(|_| model.id.clone());
            let long_context = requested_id.ends_with(shared::LONG_CONTEXT_SUFFIX);
            let model_id = shared::strip_long_context(&requested_id).to_string();

            let mut body = shared::build_request_body_with_system(
                model,
                messages,
                &[shared::SystemBlock {
                    r#type: "text",
                    text: system,
                    cache_control: Some(shared::EPHEMERAL),
                }],
                tools,
                opts.thinking,
                self.top_p,
            );
            // Fast mode lives only on the direct API, so Bedrock skips `opts.fast`
            // and never sends the `speed` param.
            body["anthropic_version"] = json!(BEDROCK_API_VERSION);
            let has_examples = tools
                .as_array()
                .is_some_and(|arr| arr.iter().any(|t| t.get("input_examples").is_some()));
            let mut betas = Vec::new();
            if has_examples {
                betas.push(shared::BETA_TOOL_EXAMPLES_BEDROCK);
            }
            if shared::has_deferred_tools(tools) {
                betas.push(shared::BETA_DEFERRED_TOOLS_BEDROCK);
            }
            if long_context {
                betas.push(shared::LONG_CONTEXT_BETA);
            }
            if !betas.is_empty() {
                body["anthropic_beta"] = json!(betas);
            }

            let encoded_model = super::super::urlenc(&model_id);
            let url = match &self.base_url {
                Some(base) => format!("{base}/model/{encoded_model}/invoke-with-response-stream"),
                None => format!(
                    "https://bedrock-runtime.{}.amazonaws.com/model/{encoded_model}/invoke-with-response-stream",
                    auth.region
                ),
            };

            let json_body = serde_json::to_vec(&body)?;

            let (host, _, _) = parse_url(&url);
            let host = host.to_string();
            let extra_headers = vec![("content-type", "application/json"), ("host", &host)];

            let timestamp = now_timestamp();
            let signing_headers = match &auth.kind {
                AuthKind::SigV4 {
                    access_key,
                    secret_key,
                    session_token,
                    expires_at: _,
                } => Some(sign_request_sigv4(
                    "POST",
                    &url,
                    &extra_headers,
                    &json_body,
                    access_key,
                    secret_key,
                    session_token.as_deref(),
                    &auth.region,
                    "bedrock",
                    &timestamp,
                )),
                AuthKind::Bearer { token } => {
                    Some(vec![("Authorization".into(), format!("Bearer {token}"))])
                }
                AuthKind::None => None,
                AuthKind::ContainerPending => {
                    return Err(AgentError::Config {
                        message: CONTAINER_CREDENTIALS_PENDING.into(),
                    });
                }
            };

            let mut builder = self
                .client
                .post(&url)
                .header("user-agent", super::super::user_agent());
            for (k, v) in &extra_headers {
                builder = builder.header(*k, *v);
            }
            if let Some(ref sign_hdrs) = signing_headers {
                for (k, v) in sign_hdrs {
                    builder = builder.header(k.as_str(), v.as_str());
                }
            }
            let request = builder.body(json_body).build()?;

            debug!(model = %model_id, region = %auth.region, "sending Bedrock request");

            let response = self.client.execute(request).await?;
            let status = response.status().as_u16();
            if status != 200 {
                return Err(AgentError::from_response(response).await);
            }

            read_events(response.bytes_stream().map_err(AgentError::from), event_tx).await
        })
    }

    fn list_models(&self) -> BoxFuture<'_, Result<Vec<crate::model::ModelInfo>, AgentError>> {
        Box::pin(async {
            let models: Vec<crate::model::ModelInfo> = super::SPEC
                .models()
                .iter()
                .map(|entry| crate::model::ModelInfo::id_only(entry.prefixes[0].to_string()))
                .collect();
            Ok(models)
        })
    }

    fn reload_auth(&self) -> BoxFuture<'_, Result<(), AgentError>> {
        Box::pin(async {
            let new_auth = spawn_blocking(resolve_bedrock_auth).await??;
            *self.auth.lock().unwrap() = new_auth;
            debug!("reloaded Bedrock auth from env");
            Ok(())
        })
    }
}

fn decode_event_payload(payload: &[u8]) -> Result<(String, String), AgentError> {
    let json_str = str::from_utf8(payload).map_err(malformed_stream)?;
    let outer: Value = serde_json::from_str(json_str)?;

    let (parsed, json) = if let Some(b64) = outer.get("bytes").and_then(|b| b.as_str()) {
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(b64)
            .map_err(malformed_stream)?;
        let json = String::from_utf8(decoded).map_err(malformed_stream)?;
        let parsed: Value = serde_json::from_str(&json)?;
        (parsed, json)
    } else {
        (outer, json_str.to_string())
    };

    let event_type = parsed
        .get("type")
        .and_then(|t| t.as_str())
        .unwrap_or("")
        .to_string();
    Ok((event_type, json))
}

fn now_timestamp() -> String {
    use std::time::SystemTime;
    let dur = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap();
    let secs = dur.as_secs();
    let days = secs / 86400;
    let day_secs = secs % 86400;
    let hours = day_secs / 3600;
    let minutes = (day_secs % 3600) / 60;
    let seconds = day_secs % 60;

    let (year, month, day) = days_to_ymd(days);
    format!("{year:04}{month:02}{day:02}T{hours:02}{minutes:02}{seconds:02}Z")
}

fn days_to_ymd(days_since_epoch: u64) -> (u64, u64, u64) {
    // Howard Hinnant's date algorithm: http://howardhinnant.github.io/date_algorithms.html
    let z = days_since_epoch + 719468;
    let era = z / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

#[cfg(test)]
mod tests {
    use aws_smithy_eventstream::frame::write_message_to;
    use aws_smithy_types::event_stream::{Header, HeaderValue};
    use futures::stream::iter;

    use super::*;
    use crate::{ContentBlock, ProviderEvent};
    use test_case::test_case;

    const MESSAGE_START: &str =
        r#"{"type":"message_start","message":{"usage":{"input_tokens":10}}}"#;
    const TEXT_BLOCK_START: &str =
        r#"{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}"#;
    const TEXT_DELTA: &str =
        r#"{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hello"}}"#;
    const MESSAGE_STOP: &str = r#"{"type":"message_stop"}"#;
    const STREAMED_TEXT: &str = "Hello";
    const EXCEPTION_MESSAGE: &str = r#"{"message":"Rate exceeded"}"#;
    const TOO_LONG_MESSAGE: &str = r#"{"message":"Input is too long for requested model."}"#;
    const LOGIN_PAGE: &str =
        "<html><body>Sign in to continue, this page is long enough</body></html>";
    const BAD_BASE64: &[u8] = br#"{"bytes":"not base64!"}"#;

    #[test]
    fn sigv4_signing_encodes_path_segments() {
        // The URL below has '%3A' from a real Bedrock model ID. The pinned signature
        // was computed against the doubly encoded '%253A' canonical path, so this
        // test fails the moment someone stops re-encoding the path.
        let url = "https://bedrock-runtime.us-east-1.amazonaws.com/model/anthropic.claude-3-5-sonnet-20241022-v2%3A0/invoke-with-response-stream";
        let headers = sign_request_sigv4(
            "POST",
            url,
            &[
                ("content-type", "application/json"),
                ("host", "bedrock-runtime.us-east-1.amazonaws.com"),
            ],
            b"{}",
            "AKID",
            "SECRET",
            None,
            "us-east-1",
            "bedrock",
            "20240101T000000Z",
        );
        let auth = headers
            .iter()
            .find(|(k, _)| k == "Authorization")
            .map(|(_, v)| v.as_str())
            .unwrap();
        assert!(
            auth.contains(
                "Signature=ff69b69725fa6fa2de10b57605b9fca403f357980e082d138d1772abe10fbea2"
            ),
            "unexpected signature: {auth}"
        );
    }

    #[test_case(None ; "without_session_token")]
    #[test_case(Some("TOKEN") ; "with_session_token")]
    fn sigv4_signing(session_token: Option<&str>) {
        let headers = sign_request_sigv4(
            "POST",
            "https://bedrock-runtime.us-east-1.amazonaws.com/model/test/invoke",
            &[
                ("content-type", "application/json"),
                ("host", "bedrock-runtime.us-east-1.amazonaws.com"),
            ],
            b"{}",
            "AKID",
            "SECRET",
            session_token,
            "us-east-1",
            "bedrock",
            "20240101T000000Z",
        );
        let auth = headers.iter().find(|(k, _)| k == "Authorization").unwrap();
        assert!(auth.1.starts_with(
            "AWS4-HMAC-SHA256 Credential=AKID/20240101/us-east-1/bedrock/aws4_request"
        ));
        let expected_signed = if session_token.is_some() {
            "SignedHeaders=content-type;host;x-amz-date;x-amz-security-token"
        } else {
            "SignedHeaders=content-type;host;x-amz-date"
        };
        assert!(
            auth.1.contains(expected_signed),
            "Authorization missing expected SignedHeaders: {}",
            auth.1
        );
        assert!(auth.1.contains("Signature="));
        assert_eq!(
            headers.iter().any(|(k, _)| k == "x-amz-security-token"),
            session_token.is_some(),
        );
    }

    fn frame(headers: &[(&'static str, &'static str)], payload: &[u8]) -> Vec<u8> {
        let headers = headers
            .iter()
            .map(|(name, value)| Header::new(*name, HeaderValue::String((*value).into())))
            .collect();
        let mut wire = Vec::new();
        write_message_to(
            &FrameMessage::new_from_parts(headers, payload.to_vec()),
            &mut wire,
        )
        .unwrap();
        wire
    }

    fn event(name: &'static str, payload: &[u8]) -> Vec<u8> {
        frame(
            &[(":message-type", "event"), (":event-type", name)],
            payload,
        )
    }

    fn exception(name: &'static str, message: &str) -> Vec<u8> {
        frame(
            &[(":message-type", "exception"), (":exception-type", name)],
            message.as_bytes(),
        )
    }

    fn chunk(json: &str) -> Vec<u8> {
        let wrapped = json!({
            "bytes": base64::engine::general_purpose::STANDARD.encode(json),
        });
        event(CHUNK_EVENT, wrapped.to_string().as_bytes())
    }

    fn text_stream() -> Vec<u8> {
        [MESSAGE_START, TEXT_BLOCK_START, TEXT_DELTA, MESSAGE_STOP]
            .map(chunk)
            .concat()
    }

    async fn read_all(parts: Vec<Vec<u8>>) -> Result<StreamResponse, AgentError> {
        let (tx, _rx) = flume::unbounded();
        read_events(iter(parts.into_iter().map(Ok)), &tx).await
    }

    fn response_text(response: &StreamResponse) -> &str {
        match &response.message.content[0] {
            ContentBlock::Text { text } => text,
            other => panic!("expected text, got: {other:?}"),
        }
    }

    #[test_case(1 ; "split_byte_by_byte")]
    #[test_case(7 ; "split_inside_the_prelude")]
    #[test_case(usize::MAX ; "one_read")]
    #[tokio::test]
    async fn frames_decode_however_the_reads_split_them(read_size: usize) {
        let wire = text_stream();
        let parts = wire.chunks(read_size).map(<[u8]>::to_vec).collect();
        let response = read_all(parts).await.unwrap();
        assert_eq!(response_text(&response), STREAMED_TEXT);
    }

    #[tokio::test]
    async fn other_events_and_empty_chunks_are_skipped() {
        let wire = [
            event("initial-response", b""),
            event(CHUNK_EVENT, b""),
            text_stream(),
        ]
        .concat();
        let response = read_all(vec![wire]).await.unwrap();
        assert_eq!(response_text(&response), STREAMED_TEXT);
    }

    #[test_case("throttlingException", 429 ; "throttling")]
    #[test_case("validationException", 400 ; "validation")]
    #[test_case("ThrottlingException", 429 ; "throttling_in_pascal_case")]
    #[test_case("ValidationException", 400 ; "validation_in_pascal_case")]
    #[test_case("serviceUnavailableException", 500 ; "anything_else_stays_retryable")]
    #[tokio::test]
    async fn exception_frames_keep_their_status(name: &'static str, expected: u16) {
        let error = read_all(vec![exception(name, EXCEPTION_MESSAGE)])
            .await
            .unwrap_err();
        let AgentError::Api {
            status, message, ..
        } = error
        else {
            panic!("expected Api error, got: {error:?}");
        };
        assert_eq!(status, expected);
        assert_eq!(message, EXCEPTION_MESSAGE);
    }

    #[tokio::test]
    async fn a_too_long_prompt_mid_stream_reads_as_overflow() {
        let error = read_all(vec![exception("validationException", TOO_LONG_MESSAGE)])
            .await
            .unwrap_err();
        assert!(error.is_context_overflow(), "{error:?}");
    }

    #[test_case(vec![0; 16] ; "zero_length_frame")]
    #[test_case(LOGIN_PAGE.as_bytes().to_vec() ; "login_page_on_a_200")]
    #[test_case(frame(&[(":event-type", CHUNK_EVENT)], b"{}") ; "no_message_type")]
    #[test_case(frame(&[(":message-type", "error")], b"") ; "framework_error")]
    #[test_case(event(CHUNK_EVENT, BAD_BASE64) ; "payload_is_not_base64")]
    #[tokio::test]
    async fn a_malformed_stream_fails_without_retrying(wire: Vec<u8>) {
        let error = read_all(vec![wire]).await.unwrap_err();
        assert!(!error.is_retryable(), "{error:?}");
    }

    #[tokio::test]
    async fn a_dropped_connection_stays_retryable() {
        let (tx, _rx) = flume::unbounded();
        let parts: Vec<Result<Vec<u8>, AgentError>> = vec![
            Ok(chunk(MESSAGE_START)),
            Err(AgentError::Io(std::io::ErrorKind::ConnectionReset.into())),
        ];
        let error = read_events(iter(parts), &tx).await.unwrap_err();
        assert!(error.is_retryable(), "{error:?}");
    }

    #[tokio::test]
    async fn a_corrupted_frame_is_rejected() {
        let mut wire = text_stream();
        let last = wire.len() - 1;
        wire[last] ^= 1;
        let error = read_all(vec![wire]).await.unwrap_err();
        assert!(!error.is_retryable(), "{error:?}");
    }

    #[test_case(5 ; "inside_the_prelude")]
    #[test_case(12 ; "right_after_the_prelude")]
    #[test_case(20 ; "after_the_prelude")]
    #[tokio::test]
    async fn a_body_ending_mid_frame_fails_without_retrying(kept: usize) {
        let wire = [chunk(MESSAGE_START), chunk(MESSAGE_START)[..kept].to_vec()].concat();
        let error = read_all(vec![wire]).await.unwrap_err();
        assert!(!error.is_retryable(), "{error:?}");
        assert!(error.to_string().contains(TRUNCATED_STREAM), "{error:?}");
    }

    #[test_case("default", "AKID", "SECRET", None ; "default_profile")]
    #[test_case("myprofile", "MYKEY", "MYSECRET", Some("MYTOKEN") ; "named_profile_with_token")]
    fn parse_aws_credentials(
        profile: &str,
        expected_key: &str,
        expected_secret: &str,
        expected_token: Option<&str>,
    ) {
        let content = "\
[default]\n\
aws_access_key_id = AKID\n\
aws_secret_access_key = SECRET\n\
\n\
[myprofile]\n\
aws_access_key_id = MYKEY\n\
aws_secret_access_key = MYSECRET\n\
aws_session_token = MYTOKEN\n";
        let (key, secret, token) = parse_aws_credentials_file(content, profile).unwrap();
        assert_eq!(key, expected_key);
        assert_eq!(secret, expected_secret);
        assert_eq!(token.as_deref(), expected_token);
    }

    #[test]
    fn parse_aws_credentials_missing_profile_errors() {
        let content = "[default]\naws_access_key_id = AKID\naws_secret_access_key = SECRET\n";
        assert!(parse_aws_credentials_file(content, "nonexistent").is_err());
    }

    #[test]
    fn parse_container_creds_full_response() {
        let body = r#"{
            "AccessKeyId": "ASIA123",
            "SecretAccessKey": "secret456",
            "Token": "session789",
            "Expiration": "2026-05-11T18:00:00Z"
        }"#;
        let (ak, sk, tok, exp) = parse_container_credentials_response(body).unwrap();
        assert_eq!(ak, "ASIA123");
        assert_eq!(sk, "secret456");
        assert_eq!(tok.as_deref(), Some("session789"));
        assert_eq!(exp, Some(1778522400));
    }

    #[test]
    fn parse_container_creds_missing_token_is_ok() {
        let body = r#"{"AccessKeyId":"AKIA1","SecretAccessKey":"s1"}"#;
        let (ak, sk, tok, exp) = parse_container_credentials_response(body).unwrap();
        assert_eq!(ak, "AKIA1");
        assert_eq!(sk, "s1");
        assert!(tok.is_none());
        assert!(exp.is_none());
    }

    #[test]
    fn parse_container_creds_missing_required_field_errors() {
        let body = r#"{"SecretAccessKey":"s1"}"#;
        assert!(parse_container_credentials_response(body).is_err());
    }

    #[test_case("2026-05-11T18:00:00Z",      Some(1778522400) ; "plain_seconds")]
    #[test_case("2026-05-11T18:00:00.123Z",  Some(1778522400) ; "fractional_dropped")]
    #[test_case("2024-01-01T00:00:00Z",      Some(1704067200) ; "epoch_2024")]
    #[test_case("1970-01-01T00:00:00Z",      Some(0)          ; "unix_epoch")]
    #[test_case("2000-02-29T23:59:59Z",      Some(951868799)  ; "leap_day")]
    #[test_case("2024-12-31T23:59:59Z",      Some(1735689599) ; "year_end")]
    #[test_case("2100-03-01T00:00:00Z",      Some(4107542400) ; "century_non_leap")]
    #[test_case("not a date",                None              ; "garbage")]
    #[test_case("2026-05-11T18:00:00",       None              ; "missing_z")]
    fn iso8601_parse(input: &str, expected: Option<u64>) {
        assert_eq!(parse_iso8601_to_epoch(input), expected);
    }

    #[tokio::test]
    async fn event_parser_text_stream() {
        let (tx, rx) = flume::unbounded();
        let mut parser = shared::EventParser::new();

        let steps: &[(&str, &str)] = &[
            (
                "message_start",
                r#"{"type":"message_start","message":{"usage":{"input_tokens":10}}}"#,
            ),
            (
                "content_block_start",
                r#"{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}"#,
            ),
            (
                "content_block_delta",
                r#"{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hello"}}"#,
            ),
        ];
        for (event, data) in steps {
            assert!(
                parser
                    .process(event, data, &tx)
                    .await
                    .unwrap()
                    .is_continue()
            );
        }
        assert!(
            parser
                .process("message_stop", r#"{"type":"message_stop"}"#, &tx)
                .await
                .unwrap()
                .is_break()
        );

        let resp = parser.finish();
        assert_eq!(resp.usage.input, 10);
        assert!(matches!(&resp.message.content[0], ContentBlock::Text { text } if text == "Hello"));
        assert!(
            rx.drain()
                .any(|e| matches!(e, ProviderEvent::TextDelta { text } if text == "Hello"))
        );
    }

    #[test_case("https://host.com/path?q=1", "host.com", "/path", "q=1" ; "with_query")]
    #[test_case("https://host.com/path", "host.com", "/path", "" ; "without_query")]
    fn url_parsing(url: &str, expected_host: &str, expected_path: &str, expected_query: &str) {
        let (host, path, query) = parse_url(url);
        assert_eq!(host, expected_host);
        assert_eq!(path, expected_path);
        assert_eq!(query, expected_query);
    }

    #[test_case(0, (1970, 1, 1) ; "epoch")]
    #[test_case(19723, (2024, 1, 1) ; "known_date")]
    fn days_to_ymd_conversion(days: u64, expected: (u64, u64, u64)) {
        assert_eq!(days_to_ymd(days), expected);
    }
}
