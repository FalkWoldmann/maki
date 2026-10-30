//! Amazon Bedrock on `bedrock-runtime`. Claude goes over the Messages route,
//! which takes the Anthropic API as is, every other vendor over Chat
//! Completions. Both are authenticated the same way, and the model list is
//! models.dev's `amazon-bedrock` entry.

use std::env;
use std::error::Error;
use std::fmt::Display;
use std::future::Future;
use std::iter;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use async_compat::Compat;
use aws_config::Region;
use aws_config::default_provider::credentials::DefaultCredentialsChain;
use aws_config::environment::EnvironmentVariableRegionProvider;
use aws_config::meta::region::RegionProviderChain;
use aws_config::profile::ProfileFileRegionProvider;
use aws_credential_types::Credentials;
use aws_credential_types::provider::ProvideCredentials;
use aws_credential_types::provider::error::CredentialsError;
use aws_sigv4::http_request::{self as sigv4, SignableBody, SignableRequest, SigningSettings};
use aws_sigv4::sign::v4;
use flume::Sender;
use futures_lite::future;
use isahc::http::request::Builder;
use maki_config::providers::{
    BEDROCK_API_KEY_PLAN, BEDROCK_AWS_PLAN, BEDROCK_SLUG, Protocol, ProviderDef, ProviderPlan,
    ProvidersConfig,
};
use maki_storage::id::SessionRef;
use serde_json::Value;
use smol::Timer;
use smol::lock::{Mutex as AsyncMutex, OnceCell};
use tracing::{debug, warn};

use super::anthropic::Anthropic;
use super::codec::{self, CodecOptions};
use super::{KeyPool, RequestSigner, ResolvedAuth, Timeouts, catalog};
use crate::model::{Model, ModelFamily, ModelInfo};
use crate::provider::{BoxFuture, Provider};
use crate::spec::{
    AuthDoc, Build, CatalogDoc, GeneratedDocs, LoginConfig, Native, ProviderSpec, api_decides,
};
use crate::types::claude_version;
use crate::{AgentError, Message, ProviderEvent, RequestOptions, StreamResponse};

pub(crate) const SLUG: &str = BEDROCK_SLUG;
const DISPLAY_NAME: &str = "Amazon Bedrock";
pub(crate) const BEARER_TOKEN_ENV: &str = "AWS_BEARER_TOKEN_BEDROCK";
const ENABLE_ENV: &str = "CLAUDE_CODE_USE_BEDROCK";
const SKIP_AUTH_ENV: &str = "CLAUDE_CODE_SKIP_BEDROCK_AUTH";
const BASE_URL_ENV: &str = "ANTHROPIC_BEDROCK_BASE_URL";
const DEFAULT_REGION: &str = "us-east-1";
const DEFAULT_MODEL: &str = "bedrock/global.anthropic.claude-sonnet-5-5";
const ENDPOINT: &str = "https://bedrock-runtime.<region>.amazonaws.com";
const API_KEYS_URL: &str = "https://console.aws.amazon.com/bedrock/home#/api-keys";
const SIGNING_NAME: &str = "bedrock";
const MESSAGES_PATH: &str = "/anthropic/v1/messages";
const CHAT_BASE_PATH: &str = "/openai/v1";
const ANTHROPIC_VENDOR: &str = "anthropic";
const GPT_PREFIX: &str = "openai.gpt-";
const GPT_OSS_PREFIX: &str = "openai.gpt-oss";
const HAIKU: &str = "haiku";
const GLOBAL: &str = "global";
const FORBIDDEN: u16 = 403;
const NO_CREDENTIALS_HINT: &str = "run `maki auth login bedrock`, or set AWS_PROFILE (after `aws sso login` for SSO), AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, or AWS_BEARER_TOKEN_BEDROCK";
const DISABLED: &str = "Amazon Bedrock is off: run `maki auth login bedrock`, or set CLAUDE_CODE_USE_BEDROCK=1 or AWS_BEARER_TOKEN_BEDROCK";
const PLANS: &[(&str, ProviderPlan)] = &[
    (
        BEDROCK_AWS_PLAN,
        ProviderPlan {
            display_name: "AWS credentials (profile, SSO, environment)",
            base_url: ENDPOINT,
            default_model: None,
            login_url: None,
        },
    ),
    (
        BEDROCK_API_KEY_PLAN,
        ProviderPlan {
            display_name: "Bedrock API key",
            base_url: ENDPOINT,
            default_model: None,
            login_url: Some(API_KEYS_URL),
        },
    ),
];
/// Temporary credentials are replaced this long before they expire, so a
/// request signed with them cannot outlive them. Credentials that live less
/// than twice as long are replaced halfway instead.
const REFRESH_MARGIN: Duration = Duration::from_secs(5 * 60);
/// Claude Code's limit, for a `credential_process` waiting on input it never
/// gets.
const LOOKUP_TIMEOUT: Duration = Duration::from_secs(60);

/// The first segment of a cross-region inference profile id.
const GEO_PREFIXES: &[&str] = &["us", "us-gov", "eu", "apac", "jp", "au", "ca", "in", GLOBAL];
const JP_REGIONS: &[&str] = &["ap-northeast-1", "ap-northeast-3"];
const AU_REGIONS: &[&str] = &["ap-southeast-2", "ap-southeast-4", "ap-southeast-6"];
const IN_REGIONS: &[&str] = &["ap-south-1", "ap-south-2"];

/// Vendors whose models take Chat Completions on `bedrock-runtime`, per
/// <https://docs.aws.amazon.com/bedrock/latest/userguide/models-api-compatibility.html>.
/// Meta, Amazon, Cohere and AI21 only take Converse.
const CHAT_VENDORS: &[&str] = &[
    "deepseek",
    "google",
    "minimax",
    "mistral",
    "moonshot",
    "moonshotai",
    "nvidia",
    "openai",
    "qwen",
    "writer",
    "xai",
    "zai",
];
/// Models of those vendors that only take Converse, or that only Mantle
/// serves, by id prefix.
const NO_CHAT_MODELS: &[&str] = &[
    "deepseek.r1",
    "google.gemma-4",
    "mistral.mistral-7b",
    "mistral.mistral-large-2",
    "mistral.mistral-small",
    "mistral.mixtral",
    "mistral.pixtral",
    "moonshot.kimi-k2-thinking",
    "openai.gpt-5.4",
    "openai.gpt-5.5",
    "writer.palmyra-x",
    "xai.grok-4.3",
];
/// Mantle's spelling of models `bedrock-runtime` knows with a version suffix.
const MANTLE_IDS: &[&str] = &["openai.gpt-oss-120b", "openai.gpt-oss-20b"];

const MESSAGES_DOC_URL: &str =
    "https://bedrock-runtime.<region>.amazonaws.com/anthropic/v1/messages";
const CHAT_DOC_URL: &str =
    "https://bedrock-runtime.<region>.amazonaws.com/openai/v1/chat/completions";
const FEATURES: &str = "Claude over the Messages API, other vendors over Chat Completions, AWS credentials or Bedrock API keys";
const AUTH_DOC: &str = "`maki auth login bedrock` with AWS credentials or an API key, or Claude Code's env vars (see below)";
const BEDROCK_NOTE: &str = r#"#### Setup

Run `maki auth login bedrock`, or pick Amazon Bedrock in `/login`, and choose how to authenticate: your AWS credentials (a profile, SSO or the environment) or a Bedrock API key. Login asks for the region, and for AWS credentials an optional profile, and saves them under `[bedrock]` in `providers.toml`. Then pick a model with `/model` or `-m bedrock/<model id>`.

The table lists the startup defaults. The picker lists every model models.dev knows on Bedrock that is reachable from your region: the cross-region profiles for your geography (`eu.`, `us.`, ...), the `global.` ones, and models served in your region directly.

Claude models go to the Messages API at `https://bedrock-runtime.<region>.amazonaws.com/anthropic/v1/messages`, so only Claude Haiku 4.5, Opus 4.7 and newer work. Other vendors go to Chat Completions at `https://bedrock-runtime.<region>.amazonaws.com/openai/v1/chat/completions`. Models that take only the Converse API, such as Llama and Nova, are not listed.

The region comes from `[bedrock] region`, then `AWS_REGION`, then `AWS_DEFAULT_REGION`, then your AWS profile, and falls back to `us-east-1`. Temporary AWS credentials are refreshed 5 minutes before they expire. After `aws sso login`, the next request picks up the new session.

Claude Code's env vars work as well, without a login, and win over it:

| Env var | Effect |
|---------|--------|
| `CLAUDE_CODE_USE_BEDROCK=1` | Turns Bedrock on with your AWS credentials |
| `AWS_BEARER_TOKEN_BEDROCK` | Turns Bedrock on with this API key |
| `CLAUDE_CODE_SKIP_BEDROCK_AUTH=1` | Sends requests unsigned, for a gateway that adds the credentials |
| `ANTHROPIC_BEDROCK_BASE_URL` | Replaces `https://bedrock-runtime.<region>.amazonaws.com`, as `[bedrock] base_url` does |

A gateway has to serve `/anthropic/v1/messages` and `/openai/v1/chat/completions`. Claude Code calls the InvokeModel API instead, so a gateway set up only for InvokeModel does not work here."#;

pub(crate) const SPEC: ProviderSpec = ProviderSpec {
    slug: SLUG,
    display_name: DISPLAY_NAME,
    api_key_env: BEARER_TOKEN_ENV,
    family: ModelFamily::Generic,
    supports_thinking: true,
    supports_deferred_tools: true,
    adaptive_effort: api_decides,
    thinking_fields_fallback: |_| None,
    accepts_arbitrary_models: true,
    fallback_max_output: Some(32_000),
    fallback_context_window: 128_000,
    models_toml: include_str!("../../models/bedrock.toml"),
    pricing_schedule: None,
    build: Build::Native(Native {
        new: create,
        with_auth: create_with_auth,
    }),
    aperture: None,
    login: Some(LoginConfig {
        protocol: Protocol::Anthropic,
        default_base_url: ENDPOINT,
        default_model: DEFAULT_MODEL,
        plans: Some(PLANS),
        login_url: None,
        needs_url: false,
    }),
    docs: GeneratedDocs {
        api_urls: &[MESSAGES_DOC_URL, CHAT_DOC_URL],
        features: Some(FEATURES),
        auth: AuthDoc::Custom(AUTH_DOC),
        catalog: CatalogDoc::Table,
        trailing_notes: &[BEDROCK_NOTE],
    },
};

inventory::submit!(SPEC.config_row());

/// A `[bedrock]` entry, which `maki auth login bedrock` writes, or one of
/// Claude Code's env vars.
fn is_enabled(def: Option<&ProviderDef>) -> bool {
    def.is_some()
        || env::var(ENABLE_ENV).is_ok_and(|v| v == "1")
        || env::var(BEARER_TOKEN_ENV).is_ok()
}

fn create(timeouts: Timeouts) -> Result<Box<dyn Provider>, AgentError> {
    let config = ProvidersConfig::load();
    let def = config.get(SLUG);
    if !is_enabled(def) {
        return Err(AgentError::Config {
            message: DISABLED.into(),
        });
    }
    let chat_auth = ResolvedAuth::new(SLUG, Vec::new())?;
    Ok(Box::new(BedrockProvider::new(
        Bedrock::from_config(def),
        chat_auth,
        timeouts,
    )))
}

/// Bedrock is no plugin base and has no Aperture route, so this is only ever
/// handed headers for the Chat Completions side. Signing stays Bedrock's.
fn create_with_auth(
    auth: Arc<Mutex<ResolvedAuth>>,
    timeouts: Timeouts,
    _system_prefix: Option<String>,
) -> Box<dyn Provider> {
    let chat_auth = auth.lock().unwrap().clone();
    Box::new(BedrockProvider::new(
        Bedrock::from_config(ProvidersConfig::load().get(SLUG)),
        chat_auth,
        timeouts,
    ))
}

/// Which `bedrock-runtime` API a model takes.
#[derive(Debug, PartialEq)]
enum Route {
    Messages,
    ChatCompletions,
}

impl Route {
    fn of(id: &str) -> Self {
        match split_geo(id).1.split_once('.') {
            Some((ANTHROPIC_VENDOR, _)) => Self::Messages,
            _ => Self::ChatCompletions,
        }
    }
}

/// `eu.anthropic.claude-opus-5-5` is `(Some("eu"), "anthropic.claude-opus-5-5")`.
fn split_geo(id: &str) -> (Option<&str>, &str) {
    match id.split_once('.') {
        Some((prefix, model)) if GEO_PREFIXES.contains(&prefix) => (Some(prefix), model),
        _ => (None, id),
    }
}

/// The cross-region profile prefix AWS gives `region`'s geography.
fn geo(region: &str) -> Option<&'static str> {
    if region.starts_with("us-gov-") {
        Some("us-gov")
    } else if region.starts_with("us-") || region.starts_with("ca-") {
        Some("us")
    } else if region.starts_with("eu-") {
        Some("eu")
    } else if JP_REGIONS.contains(&region) {
        Some("jp")
    } else if AU_REGIONS.contains(&region) {
        Some("au")
    } else if IN_REGIONS.contains(&region) {
        Some("in")
    } else if region.starts_with("ap-") {
        Some("apac")
    } else {
        None
    }
}

/// models.dev lists every geography's profiles and Mantle's ids alongside the
/// runtime's, and knows nothing about which API a model takes.
fn listed(id: &str, geo: Option<&str>) -> bool {
    let (prefix, model) = split_geo(id);
    let reachable = match prefix {
        Some(GLOBAL) => true,
        Some(prefix) => geo == Some(prefix),
        // Claude runs in-region in a handful of regions only, the closed GPT
        // models never do.
        None => {
            Route::of(model) == Route::ChatCompletions
                && (!model.starts_with(GPT_PREFIX) || model.starts_with(GPT_OSS_PREFIX))
        }
    };
    reachable && served(model)
}

fn served(model: &str) -> bool {
    match model.split_once('.') {
        Some((ANTHROPIC_VENDOR, _)) => takes_messages(model),
        Some((vendor, _)) => {
            CHAT_VENDORS.contains(&vendor)
                && !NO_CHAT_MODELS.iter().any(|m| model.starts_with(m))
                && !MANTLE_IDS.contains(&model)
        }
        None => false,
    }
}

/// Claude Haiku 4.5, Opus 4.7 and everything after them. The 4.x models in
/// between take only InvokeModel and Converse.
fn takes_messages(model: &str) -> bool {
    claude_version(model).is_some_and(|(family, version)| {
        version >= (4, 7) || (family == HAIKU && version >= (4, 5))
    })
}

fn family(id: &str) -> ModelFamily {
    match Route::of(id) {
        Route::Messages => ModelFamily::Claude,
        Route::ChatCompletions if split_geo(id).1.starts_with(GPT_PREFIX) => ModelFamily::Gpt,
        Route::ChatCompletions => ModelFamily::Generic,
    }
}

struct BedrockProvider {
    bedrock: Arc<Bedrock>,
    messages: Anthropic,
    chat: Box<dyn Provider>,
    chat_auth: Arc<Mutex<ResolvedAuth>>,
}

impl BedrockProvider {
    fn new(bedrock: Bedrock, chat_auth: ResolvedAuth, timeouts: Timeouts) -> Self {
        let bedrock = Arc::new(bedrock);
        let chat_auth = Arc::new(Mutex::new(chat_auth.with_signer(bedrock.clone())));
        Self {
            messages: Anthropic::on_bedrock(Arc::clone(&bedrock), timeouts),
            chat: codec::build(
                CodecOptions::new(Protocol::Openai, SLUG),
                Arc::clone(&chat_auth),
                timeouts,
            ),
            chat_auth,
            bedrock,
        }
    }
}

impl Provider for BedrockProvider {
    fn stream_message<'a>(
        &'a self,
        model: &'a Model,
        messages: &'a [Message],
        system: &'a str,
        tools: &'a Value,
        event_tx: &'a Sender<ProviderEvent>,
        opts: RequestOptions,
        session_id: Option<&'a SessionRef>,
    ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
        Box::pin(async move {
            let provider = match Route::of(&model.id) {
                Route::Messages => &self.messages as &dyn Provider,
                Route::ChatCompletions => {
                    let origin = self.bedrock.origin().await;
                    self.chat_auth.lock().unwrap().base_url =
                        Some(format!("{origin}{CHAT_BASE_PATH}"));
                    self.chat.as_ref()
                }
            };
            let result = provider
                .stream_message(model, messages, system, tools, event_tx, opts, session_id)
                .await;
            // SigV4 rejections are 403s, which never reach `refresh_auth`.
            if let Err(AgentError::Api {
                status: FORBIDDEN, ..
            }) = &result
            {
                self.bedrock.forget_credentials().await;
            }
            result
        })
    }

    fn list_models(&self) -> BoxFuture<'_, Result<Vec<ModelInfo>, AgentError>> {
        Box::pin(async move {
            let geo = geo(self.bedrock.region().await.as_ref());
            let models = smol::unblock(|| catalog::builtin_model_infos(SLUG)).await;
            Ok(models.into_iter().filter(|m| listed(&m.id, geo)).collect())
        })
    }

    fn refresh_auth(&self) -> BoxFuture<'_, Result<(), AgentError>> {
        Box::pin(async move {
            self.bedrock.forget_credentials().await;
            Ok(())
        })
    }

    fn reload_auth(&self) -> BoxFuture<'_, Result<(), AgentError>> {
        self.refresh_auth()
    }

    fn adjust_model(&self, model: &mut Model) {
        model.family = family(&model.id);
    }
}

enum Auth {
    /// `AWS_BEARER_TOKEN_BEDROCK`, a Bedrock API key.
    ApiKey(String),
    /// `CLAUDE_CODE_SKIP_BEDROCK_AUTH=1`: the gateway at the base URL signs.
    Gateway,
    /// SigV4 with credentials from the AWS SDK's default chain.
    SigV4(CredentialCache),
}

impl Auth {
    fn name(&self) -> &'static str {
        match self {
            Self::ApiKey(_) => "api key",
            Self::Gateway => "gateway",
            Self::SigV4(_) => "sigv4",
        }
    }
}

/// Claude Code's env vars first, then the method `maki auth login bedrock`
/// picked. A key saved under the API key method is only read when the login
/// did not switch to AWS credentials since. An empty key counts as unset, as
/// it does for Claude Code.
fn choose_auth(
    env_key: Option<String>,
    skip_auth: bool,
    plan: Option<&str>,
    saved_key: impl FnOnce() -> Option<String>,
) -> Auth {
    match (env_key.filter(|key| !key.is_empty()), skip_auth) {
        (Some(key), _) => Auth::ApiKey(key),
        (None, true) => Auth::Gateway,
        (None, false) => (plan != Some(BEDROCK_AWS_PLAN))
            .then(saved_key)
            .flatten()
            .map_or_else(|| Auth::SigV4(CredentialCache::default()), Auth::ApiKey),
    }
}

pub(crate) struct Bedrock {
    auth: Auth,
    base_url: Option<String>,
    /// `[bedrock] profile`. Unset leaves it to the SDK, which reads
    /// `AWS_PROFILE`.
    profile: Option<String>,
    pub(crate) top_p: Option<f64>,
    /// `[bedrock] region`, or resolved on the first request, so building the
    /// provider reads nothing.
    region: OnceCell<Region>,
}

impl Bedrock {
    fn from_config(def: Option<&ProviderDef>) -> Self {
        let auth = choose_auth(
            env::var(BEARER_TOKEN_ENV).ok(),
            env::var(SKIP_AUTH_ENV).is_ok_and(|v| v == "1"),
            def.and_then(|d| d.plan.as_deref()),
            || {
                KeyPool::resolve(SLUG, BEARER_TOKEN_ENV)
                    .ok()
                    .map(|pool| pool.current().to_owned())
            },
        );
        debug!(auth = auth.name(), "Bedrock auth");
        Self {
            auth,
            base_url: def
                .and_then(|d| d.base_url.clone())
                .or_else(|| env::var(BASE_URL_ENV).ok()),
            profile: def.and_then(|d| d.profile.clone()),
            top_p: def.and_then(|d| d.top_p),
            region: def
                .and_then(|d| d.region.clone())
                .map_or_else(OnceCell::new, |region| OnceCell::from(Region::new(region))),
        }
    }

    pub(crate) async fn messages_url(&self) -> String {
        format!("{}{MESSAGES_PATH}", self.origin().await)
    }

    async fn origin(&self) -> String {
        match &self.base_url {
            Some(base) => base.trim_end_matches('/').to_owned(),
            None => format!(
                "https://bedrock-runtime.{}.amazonaws.com",
                self.region().await
            ),
        }
    }

    /// The next request asks the credential chain again, which picks up a new
    /// `aws sso login` or rotated keys. The region stays until a restart.
    pub(crate) async fn forget_credentials(&self) {
        if let Auth::SigV4(cache) = &self.auth {
            cache.forget().await;
        }
    }

    /// Claude Code's order: `AWS_REGION`, `AWS_DEFAULT_REGION`, the profile's
    /// region, then `us-east-1`. `[bedrock] region` goes before all of them.
    async fn region(&self) -> &Region {
        self.region
            .get_or_init(|| async {
                let profile = match &self.profile {
                    Some(name) => ProfileFileRegionProvider::builder()
                        .profile_name(name)
                        .build(),
                    None => ProfileFileRegionProvider::new(),
                };
                let chain =
                    RegionProviderChain::first_try(EnvironmentVariableRegionProvider::new())
                        .or_else(profile);
                let region = Compat::new(chain.region())
                    .await
                    .unwrap_or_else(|| Region::from_static(DEFAULT_REGION));
                debug!(%region, "resolved Bedrock region");
                region
            })
            .await
    }
}

impl RequestSigner for Bedrock {
    fn sign<'a>(
        &'a self,
        builder: Builder,
        body: &'a [u8],
    ) -> BoxFuture<'a, Result<Builder, AgentError>> {
        Box::pin(async move {
            match &self.auth {
                Auth::ApiKey(key) => {
                    Ok(builder.header(super::AUTHORIZATION_HEADER, super::bearer_value(key)))
                }
                Auth::Gateway => Ok(builder),
                Auth::SigV4(cache) => {
                    let region = self.region().await;
                    let now = SystemTime::now();
                    let profile = self.profile.clone();
                    let credentials = cache
                        .get(now, || lookup_credentials(region.clone(), profile))
                        .await?;
                    sign_request(builder, credentials, region, body, now)
                }
            }
        })
    }
}

#[derive(Default)]
struct CredentialCache {
    /// Held across a lookup, so requests that arrive meanwhile wait for its
    /// result, and a forget waits for it instead of being overwritten by it.
    state: AsyncMutex<Cached>,
}

#[derive(Default)]
enum Cached {
    #[default]
    Empty,
    Fresh {
        credentials: Credentials,
        /// `None` for credentials that do not expire.
        refresh_at: Option<SystemTime>,
    },
    /// The chain found nothing. Kept until the next re-auth, so each request
    /// does not wait out the instance metadata lookup again.
    Missing(String),
}

impl CredentialCache {
    async fn get<F, Fut>(&self, now: SystemTime, lookup: F) -> Result<Credentials, AgentError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Credentials, CredentialsError>>,
    {
        let mut cached = self.state.lock().await;
        match &*cached {
            Cached::Fresh {
                credentials,
                refresh_at,
            } if refresh_at.is_none_or(|at| now < at) => return Ok(credentials.clone()),
            Cached::Missing(message) => return Err(no_credentials(message)),
            Cached::Fresh { .. } | Cached::Empty => {}
        }
        let error = match lookup().await {
            Ok(credentials) => {
                debug!(expiry = ?credentials.expiry(), "resolved AWS credentials for Bedrock");
                *cached = Cached::Fresh {
                    refresh_at: refresh_at(&credentials, now),
                    credentials: credentials.clone(),
                };
                return Ok(credentials);
            }
            Err(error) => error,
        };
        let message = error_chain(&error);
        if let Cached::Fresh { credentials, .. } = &*cached
            && !expired(credentials, now)
        {
            warn!(error = %message, "refreshing AWS credentials failed, keeping the current ones");
            return Ok(credentials.clone());
        }
        if matches!(error, CredentialsError::CredentialsNotLoaded(_)) {
            *cached = Cached::Missing(message.clone());
        }
        Err(no_credentials(&message))
    }

    async fn forget(&self) {
        *self.state.lock().await = Cached::Empty;
    }
}

fn refresh_at(credentials: &Credentials, now: SystemTime) -> Option<SystemTime> {
    let expiry = credentials.expiry()?;
    let lifetime = expiry.duration_since(now).unwrap_or_default();
    Some(expiry - REFRESH_MARGIN.min(lifetime / 2))
}

fn expired(credentials: &Credentials, now: SystemTime) -> bool {
    credentials.expiry().is_some_and(|expiry| expiry <= now)
}

fn no_credentials(message: &str) -> AgentError {
    AgentError::Config {
        message: format!("no AWS credentials for Bedrock ({message}): {NO_CREDENTIALS_HINT}"),
    }
}

/// Env keys, profiles (static keys, SSO, `credential_process`, assumed roles,
/// `aws login`), web identity, container and instance credentials. The chain
/// is built per lookup, so its profile files are read fresh.
async fn lookup_credentials(
    region: Region,
    profile: Option<String>,
) -> Result<Credentials, CredentialsError> {
    let mut builder = DefaultCredentialsChain::builder().region(region);
    if let Some(name) = &profile {
        builder = builder.profile_name(name);
    }
    let lookup = Compat::new(async {
        let chain = builder.build().await;
        chain.provide_credentials().await
    });
    let timeout = async {
        Timer::after(LOOKUP_TIMEOUT).await;
        Err(CredentialsError::provider_timed_out(LOOKUP_TIMEOUT))
    };
    future::or(lookup, timeout).await
}

/// Signs the method, URL and headers `builder` already holds, so the
/// signature covers what goes out. `user-agent` is left out because proxies
/// rewrite it.
fn sign_request(
    mut builder: Builder,
    credentials: Credentials,
    region: &Region,
    body: &[u8],
    time: SystemTime,
) -> Result<Builder, AgentError> {
    let identity = credentials.into();
    let params = v4::SigningParams::builder()
        .identity(&identity)
        .region(region.as_ref())
        .name(SIGNING_NAME)
        .time(time)
        .settings(SigningSettings::default())
        .build()
        .map_err(signing_error)?
        .into();
    let (Some(method), Some(uri), Some(headers)) = (
        builder.method_ref(),
        builder.uri_ref(),
        builder.headers_ref(),
    ) else {
        return Err(signing_error("the request is incomplete"));
    };
    let headers = headers
        .iter()
        .filter_map(|(name, value)| Some((name.as_str(), value.to_str().ok()?)));
    let request = SignableRequest::new(
        method.as_str(),
        uri.to_string(),
        headers,
        SignableBody::Bytes(body),
    )
    .map_err(signing_error)?;
    let (instructions, _) = sigv4::sign(request, &params)
        .map_err(signing_error)?
        .into_parts();
    for (name, value) in instructions.headers() {
        builder = builder.header(name, value);
    }
    Ok(builder)
}

fn signing_error(e: impl Display) -> AgentError {
    AgentError::Config {
        message: format!("signing the Bedrock request: {e}"),
    }
}

/// The SDK's errors keep the useful part (an expired SSO session, a missing
/// profile) in their sources.
fn error_chain(error: &(dyn Error + 'static)) -> String {
    iter::successors(Some(error), |&e| e.source())
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(": ")
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::UNIX_EPOCH;

    use serde_json::json;
    use test_case::test_case;

    use super::*;
    use crate::providers::anthropic;
    use crate::test_support::{Canned, Requests, serve};

    const POST: &str = "POST";

    const REGION: &str = "us-east-1";
    const ACCESS_KEY: &str = "AKIDEXAMPLE";
    const SECRET_KEY: &str = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
    const SESSION_TOKEN: &str = "session-token";
    const API_KEY: &str = "bedrock-api-key";
    const SIGNED_AT: u64 = 1_793_000_000;
    const SIGV4_PREFIX: &str = "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/";
    const SCOPE: &str = "/us-east-1/bedrock/aws4_request";
    const CLAUDE: &str = "bedrock/eu.anthropic.claude-opus-5-5";
    const QWEN: &str = "bedrock/qwen.qwen3-coder-480b-a35b-v1:0";
    const ANSWER: &str = "Hello";
    const PROMPT: &str = "hi";
    const MESSAGES_ANSWER: &str = "\
event: message_start\n\
data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":1}}}\n\
\n\
event: content_block_start\n\
data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\
\n\
event: content_block_delta\n\
data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Hello\"}}\n\
\n\
event: content_block_stop\n\
data: {\"type\":\"content_block_stop\",\"index\":0}\n\
\n\
event: message_delta\n\
data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":1}}\n\
\n\
event: message_stop\n\
data: {\"type\":\"message_stop\"}\n\n";
    const CHAT_ANSWER: &str = "\
data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hello\"}}]}\n\n\
data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1}}\n\n\
data: [DONE]\n\n";
    const MESSAGES_SCRIPT: &[Canned] = &[Canned::sse(MESSAGES_ANSWER)];
    const CHAT_SCRIPT: &[Canned] = &[Canned::sse(CHAT_ANSWER)];
    const CHAT_PATH: &str = "/openai/v1/chat/completions";
    const OTHER_ACCESS_KEY: &str = "AKIDOTHER";
    const CHAIN_FAILED: &str = "sso endpoint unreachable";

    fn credentials(session_token: Option<&str>, expiry: Option<SystemTime>) -> Credentials {
        Credentials::new(
            ACCESS_KEY,
            SECRET_KEY,
            session_token.map(String::from),
            expiry,
            "test",
        )
    }

    #[test_case("eu.anthropic.claude-opus-5-5", Route::Messages ; "claude_profile")]
    #[test_case("anthropic.claude-sonnet-5", Route::Messages ; "claude_in_region")]
    #[test_case("global.openai.gpt-6-sol", Route::ChatCompletions ; "gpt_profile")]
    #[test_case("qwen.qwen3-coder-next", Route::ChatCompletions ; "qwen_in_region")]
    fn route_by_vendor(id: &str, expected: Route) {
        assert_eq!(Route::of(id), expected);
    }

    #[test_case("eu-central-1", Some("eu") ; "eu")]
    #[test_case("us-west-2", Some("us") ; "us")]
    #[test_case("ca-central-1", Some("us") ; "canada_rides_us_profiles")]
    #[test_case("us-gov-west-1", Some("us-gov") ; "govcloud")]
    #[test_case("ap-northeast-1", Some("jp") ; "tokyo")]
    #[test_case("ap-southeast-2", Some("au") ; "sydney")]
    #[test_case("ap-southeast-1", Some("apac") ; "singapore")]
    #[test_case("sa-east-1", None ; "global_only")]
    fn geo_of_region(region: &str, expected: Option<&str>) {
        assert_eq!(geo(region), expected);
    }

    #[test_case("eu.anthropic.claude-opus-5-5", true ; "own_geography")]
    #[test_case("us.anthropic.claude-opus-5-5", false ; "other_geography")]
    #[test_case("global.anthropic.claude-haiku-4-5-20251001-v1:0", true ; "haiku_4_5_takes_messages")]
    #[test_case("global.anthropic.claude-opus-4-7", true ; "opus_4_7_takes_messages")]
    #[test_case("global.anthropic.claude-sonnet-4-6", false ; "sonnet_4_6_is_invoke_only")]
    #[test_case("global.anthropic.claude-opus-4-6-v1", false ; "opus_4_6_is_invoke_only")]
    #[test_case("anthropic.claude-opus-5-5", false ; "claude_in_region")]
    #[test_case("qwen.qwen3-coder-480b-a35b-v1:0", true ; "chat_vendor_in_region")]
    #[test_case("global.openai.gpt-6-sol", true ; "gpt_profile")]
    #[test_case("openai.gpt-6-sol", false ; "closed_gpt_in_region")]
    #[test_case("openai.gpt-oss-120b-1:0", true ; "runtime_gpt_oss")]
    #[test_case("openai.gpt-oss-120b", false ; "mantle_gpt_oss")]
    #[test_case("openai.gpt-5.5", false ; "mantle_only_gpt")]
    #[test_case("mistral.mistral-large-3-675b-instruct", true ; "mistral_large_3")]
    #[test_case("eu.mistral.pixtral-large-2502-v1:0", false ; "converse_only_mistral")]
    #[test_case("meta.llama4-maverick-17b-instruct-v1:0", false ; "converse_only_vendor")]
    fn listed_from_eu(id: &str, expected: bool) {
        assert_eq!(listed(id, Some("eu")), expected);
    }

    const SAVED_KEY: &str = "saved-key";

    #[test_case(Some(API_KEY), false, Some(BEDROCK_AWS_PLAN), Some(SAVED_KEY), "api key" ; "env_key_wins")]
    #[test_case(None, true, None, Some(SAVED_KEY), "gateway" ; "skip_auth_beats_saved_key")]
    #[test_case(None, false, Some(BEDROCK_API_KEY_PLAN), Some(SAVED_KEY), "api key" ; "saved_key")]
    #[test_case(None, false, Some(BEDROCK_AWS_PLAN), Some(SAVED_KEY), "sigv4" ; "aws_login_ignores_old_key")]
    #[test_case(None, false, None, None, "sigv4" ; "nothing_saved")]
    #[test_case(Some(""), false, None, None, "sigv4" ; "empty_env_key_is_unset")]
    fn choose_auth_order(
        env_key: Option<&str>,
        skip_auth: bool,
        plan: Option<&str>,
        saved: Option<&str>,
        expected: &str,
    ) {
        let auth = choose_auth(env_key.map(String::from), skip_auth, plan, || {
            saved.map(String::from)
        });
        assert_eq!(auth.name(), expected);
    }

    #[test_case(None, None ; "static_credentials_never_refresh")]
    #[test_case(Some(3600), Some(3300) ; "five_minutes_early")]
    #[test_case(Some(240), Some(120) ; "short_lived_halfway")]
    fn refresh_at_cases(expires_in: Option<u64>, refresh_in: Option<u64>) {
        let now = SystemTime::now();
        let expiry = expires_in.map(|secs| now + Duration::from_secs(secs));
        assert_eq!(
            refresh_at(&credentials(None, expiry), now),
            refresh_in.map(|secs| now + Duration::from_secs(secs))
        );
    }

    fn answer(
        calls: &AtomicUsize,
        result: Result<Credentials, CredentialsError>,
    ) -> impl FnOnce() -> future::Ready<Result<Credentials, CredentialsError>> + '_ {
        move || {
            calls.fetch_add(1, Ordering::Relaxed);
            future::ready(result)
        }
    }

    fn keyed(access_key: &str, expiry: Option<SystemTime>) -> Credentials {
        Credentials::new(access_key, SECRET_KEY, None, expiry, "test")
    }

    fn access_key(result: Result<Credentials, AgentError>) -> String {
        result.unwrap().access_key_id().to_owned()
    }

    #[test]
    fn cache_reuses_fresh_credentials() {
        smol::block_on(async {
            let (cache, calls, now) = (
                CredentialCache::default(),
                AtomicUsize::new(0),
                SystemTime::now(),
            );
            for _ in 0..2 {
                let found = cache
                    .get(now, answer(&calls, Ok(keyed(ACCESS_KEY, None))))
                    .await;
                assert_eq!(access_key(found), ACCESS_KEY);
            }
            assert_eq!(calls.load(Ordering::Relaxed), 1);
        })
    }

    #[test]
    fn cache_refreshes_inside_the_margin() {
        smol::block_on(async {
            let (cache, calls, now) = (
                CredentialCache::default(),
                AtomicUsize::new(0),
                SystemTime::now(),
            );
            let expiry = Some(now + Duration::from_secs(600));
            cache
                .get(now, answer(&calls, Ok(keyed(ACCESS_KEY, expiry))))
                .await
                .unwrap();
            let later = now + Duration::from_secs(420);
            let found = cache
                .get(later, answer(&calls, Ok(keyed(OTHER_ACCESS_KEY, None))))
                .await;
            assert_eq!(access_key(found), OTHER_ACCESS_KEY);
            assert_eq!(calls.load(Ordering::Relaxed), 2);
        })
    }

    #[test]
    fn failed_refresh_keeps_credentials_that_still_work() {
        smol::block_on(async {
            let (cache, calls, now) = (
                CredentialCache::default(),
                AtomicUsize::new(0),
                SystemTime::now(),
            );
            let expiry = Some(now + Duration::from_secs(600));
            cache
                .get(now, answer(&calls, Ok(keyed(ACCESS_KEY, expiry))))
                .await
                .unwrap();
            let later = now + Duration::from_secs(420);
            let failed = Err(CredentialsError::provider_error(CHAIN_FAILED));
            let found = cache.get(later, answer(&calls, failed)).await;
            assert_eq!(access_key(found), ACCESS_KEY);
        })
    }

    #[test]
    fn missing_credentials_are_remembered_until_forgotten() {
        smol::block_on(async {
            let (cache, calls, now) = (
                CredentialCache::default(),
                AtomicUsize::new(0),
                SystemTime::now(),
            );
            for _ in 0..2 {
                let missing = Err(CredentialsError::not_loaded_no_source());
                assert!(cache.get(now, answer(&calls, missing)).await.is_err());
            }
            assert_eq!(calls.load(Ordering::Relaxed), 1);
            cache.forget().await;
            let found = cache
                .get(now, answer(&calls, Ok(keyed(ACCESS_KEY, None))))
                .await;
            assert_eq!(access_key(found), ACCESS_KEY);
            assert_eq!(calls.load(Ordering::Relaxed), 2);
        })
    }

    #[test]
    fn transient_failures_are_not_remembered() {
        smol::block_on(async {
            let (cache, calls, now) = (
                CredentialCache::default(),
                AtomicUsize::new(0),
                SystemTime::now(),
            );
            let failed = Err(CredentialsError::provider_error(CHAIN_FAILED));
            let error = cache.get(now, answer(&calls, failed)).await.unwrap_err();
            assert!(error.to_string().contains(CHAIN_FAILED), "{error}");
            let found = cache
                .get(now, answer(&calls, Ok(keyed(ACCESS_KEY, None))))
                .await;
            assert_eq!(access_key(found), ACCESS_KEY);
            assert_eq!(calls.load(Ordering::Relaxed), 2);
        })
    }

    #[test_case(None, "anthropic-version;host;x-amz-date" ; "long_term_keys")]
    #[test_case(Some(SESSION_TOKEN), "anthropic-version;host;x-amz-date;x-amz-security-token" ; "temporary_credentials")]
    fn sign_request_scopes_to_bedrock(session_token: Option<&str>, signed_headers: &str) {
        let url = format!("https://bedrock-runtime.{REGION}.amazonaws.com{MESSAGES_PATH}");
        let builder = sign_request(
            anthropic::api_request(POST, &url),
            credentials(session_token, None),
            &Region::from_static(REGION),
            b"{}",
            UNIX_EPOCH + Duration::from_secs(SIGNED_AT),
        )
        .unwrap();
        let headers = builder.headers_ref().unwrap();
        let authorization = headers[super::super::AUTHORIZATION_HEADER]
            .to_str()
            .unwrap();

        assert!(authorization.starts_with(SIGV4_PREFIX), "{authorization}");
        assert!(authorization.contains(SCOPE), "{authorization}");
        assert!(
            authorization.contains(&format!("SignedHeaders={signed_headers},")),
            "{authorization}"
        );
        assert_eq!(
            headers
                .get("x-amz-security-token")
                .map(|v| v.to_str().unwrap()),
            session_token
        );
    }

    fn api_key() -> (Auth, Option<String>) {
        (
            Auth::ApiKey(API_KEY.into()),
            Some(format!("Bearer {API_KEY}")),
        )
    }

    fn gateway() -> (Auth, Option<String>) {
        (Auth::Gateway, None)
    }

    fn sigv4() -> (Auth, Option<String>) {
        (
            Auth::SigV4(CredentialCache {
                state: AsyncMutex::new(Cached::Fresh {
                    credentials: credentials(None, None),
                    refresh_at: None,
                }),
            }),
            Some(SIGV4_PREFIX.into()),
        )
    }

    fn serve_provider(
        auth: Auth,
        script: &'static [Canned],
    ) -> (BedrockProvider, String, Requests) {
        let (base_url, requests) = serve(script);
        let bedrock = Bedrock {
            auth,
            base_url: Some(base_url.clone()),
            profile: None,
            top_p: None,
            region: OnceCell::from(Region::from_static(REGION)),
        };
        let chat_auth = ResolvedAuth::new(SLUG, Vec::new()).unwrap();
        let provider = BedrockProvider::new(bedrock, chat_auth, Timeouts::default());
        let base_path = url::Url::parse(&base_url).unwrap().path().to_owned();
        (provider, base_path, requests)
    }

    #[test_case(api_key, CLAUDE, MESSAGES_SCRIPT, MESSAGES_PATH ; "messages_api_key")]
    #[test_case(gateway, CLAUDE, MESSAGES_SCRIPT, MESSAGES_PATH ; "messages_gateway")]
    #[test_case(sigv4, CLAUDE, MESSAGES_SCRIPT, MESSAGES_PATH ; "messages_sigv4")]
    #[test_case(api_key, QWEN, CHAT_SCRIPT, CHAT_PATH ; "chat_api_key")]
    #[test_case(gateway, QWEN, CHAT_SCRIPT, CHAT_PATH ; "chat_gateway")]
    #[test_case(sigv4, QWEN, CHAT_SCRIPT, CHAT_PATH ; "chat_sigv4")]
    fn streams_each_vendor_through_its_route(
        setup: fn() -> (Auth, Option<String>),
        spec: &str,
        script: &'static [Canned],
        path: &str,
    ) {
        let (auth, expected_authorization) = setup();
        let (provider, base_path, requests) = serve_provider(auth, script);
        let mut model = Model::from_spec(spec).unwrap();
        provider.adjust_model(&mut model);
        let (tx, _rx) = flume::unbounded();

        let response = smol::block_on(provider.stream_message(
            &model,
            &[Message::user(PROMPT.to_owned())],
            "",
            &json!([]),
            &tx,
            RequestOptions::default(),
            None,
        ))
        .unwrap();

        assert_eq!(response.message.first_text_content(), Some(ANSWER));
        let requests = requests.lock().unwrap();
        let [request] = requests.as_slice() else {
            panic!("expected one request, got {}", requests.len());
        };
        assert_eq!(request.method, POST);
        assert_eq!(request.path, format!("{base_path}{path}"));
        match expected_authorization {
            Some(expected) => assert!(
                request.authorization().starts_with(&expected),
                "{}",
                request.authorization()
            ),
            None => assert!(
                !request
                    .headers
                    .contains_key(super::super::AUTHORIZATION_HEADER)
            ),
        }
        assert_eq!(request.json()["model"], model.id);
    }
}
