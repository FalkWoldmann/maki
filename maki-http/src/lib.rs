//! reqwest behind the slice of isahc's API maki used, so swapping the HTTP
//! stack leaves call sites alone.
//!
//! One current-thread tokio runtime on its own thread drives every socket and
//! timer. Futures are still polled by whichever smol executor awaits them, with
//! the runtime's handle entered, so dropping a request future drops the hyper
//! connection with it.

use std::fs;
use std::future::{Future, pending};
use std::io::{self, Read};
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{LazyLock, Once};
use std::task::{Context, Poll, ready};
use std::thread;
use std::time::Duration;

use bytes::{Buf, Bytes};
use futures_lite::future::block_on;
use futures_lite::io::{AsyncRead, AsyncReadExt};
use http::HeaderMap;
use http_body::Body as _;
use reqwest::redirect::Policy;
use tokio::runtime::{Builder as RuntimeBuilder, Handle};

pub use http;
pub use http::{Request, Response};

const RUNTIME_THREAD_NAME: &str = "maki-http";

static RUNTIME: LazyLock<Handle> = LazyLock::new(|| {
    let runtime = RuntimeBuilder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to build the HTTP runtime");
    let handle = runtime.handle().clone();
    thread::Builder::new()
        .name(RUNTIME_THREAD_NAME.into())
        .spawn(move || runtime.block_on(pending::<()>()))
        .expect("failed to spawn the HTTP runtime thread");
    handle
});

static CRYPTO_PROVIDER: Once = Once::new();

fn install_crypto_provider() {
    CRYPTO_PROVIDER.call_once(|| {
        #[cfg(feature = "ring")]
        let provider = rustls::crypto::ring::default_provider();
        #[cfg(not(feature = "ring"))]
        let provider = rustls::crypto::aws_lc_rs::default_provider();
        let _ = provider.install_default();
    });
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Http(#[from] reqwest::Error),
    #[error(transparent)]
    Request(#[from] http::Error),
    #[error("cannot read CA certificate: {0}")]
    CaFile(#[from] io::Error),
}

impl Error {
    /// Nothing answered: the name did not resolve or no socket accepted.
    pub fn is_connect(&self) -> bool {
        matches!(self, Self::Http(e) if e.is_connect() || e.is_dns())
    }
}

struct InRuntime<F>(Pin<Box<F>>);

impl<F: Future> Future for InRuntime<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        let _guard = RUNTIME.enter();
        self.0.as_mut().poll(cx)
    }
}

#[derive(Clone, Debug)]
pub struct HttpClient(reqwest::Client);

impl HttpClient {
    pub fn new() -> Result<Self, Error> {
        Self::builder().build()
    }

    pub fn builder() -> HttpClientBuilder {
        HttpClientBuilder {
            inner: reqwest::Client::builder().redirect(Policy::none()),
            ca_file: None,
        }
    }

    pub async fn send_async<B: Into<AsyncBody>>(
        &self,
        request: Request<B>,
    ) -> Result<Response<AsyncBody>, Error> {
        let request = reqwest::Request::try_from(request.map(|b| reqwest::Body::from(b.into())))?;
        let response = {
            let _guard = RUNTIME.enter();
            InRuntime(Box::pin(self.0.execute(request)))
        }
        .await?;
        Ok(http::Response::from(response).map(AsyncBody::streaming))
    }

    pub fn send<B: Into<AsyncBody>>(&self, request: Request<B>) -> Result<Response<Body>, Error> {
        Ok(block_on(self.send_async(request))?.map(Body))
    }
}

pub fn get(uri: &str) -> Result<Response<Body>, Error> {
    HttpClient::new()?.send(Request::get(uri).body(())?)
}

pub struct HttpClientBuilder {
    inner: reqwest::ClientBuilder,
    ca_file: Option<PathBuf>,
}

impl HttpClientBuilder {
    pub fn timeout(self, timeout: Duration) -> Self {
        self.map(|b| b.timeout(timeout))
    }

    pub fn connect_timeout(self, timeout: Duration) -> Self {
        self.map(|b| b.connect_timeout(timeout))
    }

    /// reqwest has no byte-rate floor; a read that stalls for `window` is the
    /// closest it gets.
    pub fn low_speed_timeout(self, _bytes_per_sec: u32, window: Duration) -> Self {
        self.map(|b| b.read_timeout(window))
    }

    pub fn version_negotiation(self, version: config::VersionNegotiation) -> Self {
        self.map(|b| match version {
            config::VersionNegotiation::Http11 => b.http1_only(),
            config::VersionNegotiation::Http2 => b.http2_prior_knowledge(),
        })
    }

    pub fn redirect_policy(self, policy: config::RedirectPolicy) -> Self {
        self.map(|b| {
            b.redirect(match policy {
                config::RedirectPolicy::None => Policy::none(),
                config::RedirectPolicy::Limit(max) => Policy::limited(max as usize),
            })
        })
    }

    pub fn ssl_ca_certificate(mut self, cert: config::CaCertificate) -> Self {
        self.ca_file = Some(cert.0);
        self
    }

    pub fn dns_resolve(self, map: config::ResolveMap) -> Self {
        self.map(|b| map.0.iter().fold(b, |b, (host, addr)| b.resolve(host, *addr)))
    }

    pub fn build(self) -> Result<HttpClient, Error> {
        install_crypto_provider();
        let _guard = RUNTIME.enter();
        let mut inner = self.inner;
        if let Some(path) = self.ca_file {
            let pem = fs::read(path)?;
            inner = inner.add_root_certificate(reqwest::Certificate::from_pem(&pem)?);
        }
        Ok(HttpClient(inner.build()?))
    }

    fn map(self, f: impl FnOnce(reqwest::ClientBuilder) -> reqwest::ClientBuilder) -> Self {
        Self {
            inner: f(self.inner),
            ..self
        }
    }
}

pub mod config {
    use super::{IpAddr, PathBuf, SocketAddr};

    pub enum VersionNegotiation {
        Http11,
        /// Prior knowledge, so cleartext h2c works without an upgrade.
        Http2,
    }

    impl VersionNegotiation {
        pub fn http11() -> Self {
            Self::Http11
        }

        pub fn http2() -> Self {
            Self::Http2
        }
    }

    pub enum RedirectPolicy {
        None,
        Limit(u32),
    }

    pub struct CaCertificate(pub(crate) PathBuf);

    impl CaCertificate {
        pub fn file(path: impl Into<PathBuf>) -> Self {
            Self(path.into())
        }
    }

    #[derive(Default)]
    pub struct ResolveMap(pub(crate) Vec<(String, SocketAddr)>);

    impl ResolveMap {
        pub fn new() -> Self {
            Self::default()
        }

        pub fn add(mut self, host: impl Into<String>, port: u16, addr: IpAddr) -> Self {
            self.0.push((host.into(), SocketAddr::new(addr, port)));
            self
        }
    }
}

/// A request body held in memory, or a response body still arriving.
#[derive(Debug)]
pub struct AsyncBody {
    buffered: Bytes,
    stream: Option<reqwest::Body>,
    trailers: Option<HeaderMap>,
}

impl AsyncBody {
    fn streaming(stream: reqwest::Body) -> Self {
        Self {
            buffered: Bytes::new(),
            stream: Some(stream),
            trailers: None,
        }
    }

    pub fn from_bytes_static(bytes: &'static [u8]) -> Self {
        Bytes::from_static(bytes).into()
    }

    pub fn len(&self) -> Option<u64> {
        match &self.stream {
            Some(stream) => stream.size_hint().exact(),
            None => Some(self.buffered.len() as u64),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == Some(0)
    }

    /// Only known once the body has been read to the end.
    pub fn trailers(&self) -> Option<&HeaderMap> {
        self.trailers.as_ref()
    }
}

impl AsyncRead for AsyncBody {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        loop {
            if !this.buffered.is_empty() {
                let n = buf.len().min(this.buffered.len());
                this.buffered.copy_to_slice(&mut buf[..n]);
                return Poll::Ready(Ok(n));
            }
            let Some(stream) = this.stream.as_mut() else {
                return Poll::Ready(Ok(0));
            };
            let _guard = RUNTIME.enter();
            match ready!(Pin::new(stream).poll_frame(cx)) {
                None => this.stream = None,
                Some(Err(e)) => return Poll::Ready(Err(io::Error::other(e))),
                Some(Ok(frame)) => match frame.into_data() {
                    Ok(data) => this.buffered = data,
                    Err(frame) => this.trailers = frame.into_trailers().ok(),
                },
            }
        }
    }
}

impl From<AsyncBody> for reqwest::Body {
    fn from(body: AsyncBody) -> Self {
        body.stream.unwrap_or_else(|| body.buffered.into())
    }
}

impl From<Bytes> for AsyncBody {
    fn from(buffered: Bytes) -> Self {
        Self {
            buffered,
            stream: None,
            trailers: None,
        }
    }
}

impl From<()> for AsyncBody {
    fn from((): ()) -> Self {
        Bytes::new().into()
    }
}

impl From<Vec<u8>> for AsyncBody {
    fn from(bytes: Vec<u8>) -> Self {
        Bytes::from(bytes).into()
    }
}

impl From<String> for AsyncBody {
    fn from(text: String) -> Self {
        Bytes::from(text).into()
    }
}

impl From<&'static str> for AsyncBody {
    fn from(text: &'static str) -> Self {
        Bytes::from_static(text.as_bytes()).into()
    }
}

pub trait AsyncReadResponseExt {
    fn bytes(&mut self) -> impl Future<Output = io::Result<Vec<u8>>> + Send;
    fn text(&mut self) -> impl Future<Output = io::Result<String>> + Send;
}

impl AsyncReadResponseExt for Response<AsyncBody> {
    async fn bytes(&mut self) -> io::Result<Vec<u8>> {
        let mut out = Vec::new();
        self.body_mut().read_to_end(&mut out).await?;
        Ok(out)
    }

    async fn text(&mut self) -> io::Result<String> {
        let bytes = AsyncReadResponseExt::bytes(self).await?;
        Ok(String::from_utf8_lossy(&bytes).into_owned())
    }
}

/// The body of a blocking [`HttpClient::send`].
#[derive(Debug)]
pub struct Body(AsyncBody);

impl Read for Body {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        block_on(self.0.read(buf))
    }
}

pub trait ReadResponseExt {
    fn bytes(&mut self) -> io::Result<Vec<u8>>;
    fn text(&mut self) -> io::Result<String>;
}

impl ReadResponseExt for Response<Body> {
    fn bytes(&mut self) -> io::Result<Vec<u8>> {
        let mut out = Vec::new();
        self.body_mut().read_to_end(&mut out)?;
        Ok(out)
    }

    fn text(&mut self) -> io::Result<String> {
        Ok(String::from_utf8_lossy(&self.bytes()?).into_owned())
    }
}
