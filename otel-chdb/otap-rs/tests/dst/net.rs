//! HTTP over the simulated network: the emulators served with hyper on any
//! stream (turmoil's in a simulation, a real socket for the differential
//! tests), object_store's `HttpConnector` onto turmoil's `TcpStream`, and the
//! ClickHouse client's `Transport` onto the same.

#![allow(dead_code)]

use super::chemu::{ChEmu, Reply};
use super::s3emu::{Answer, S3Emu};
use bytes::Bytes;
use http::{Request, Response};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper_util::rt::{TokioIo, TokioTimer};
use object_store::client::{HttpClient, HttpConnector, HttpError, HttpErrorKind, HttpRequest, HttpResponse, HttpResponseBody, HttpService};
use std::collections::BTreeMap;
use std::rc::Rc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};

/// Serves the S3 emulator on one connection.
pub async fn serve_s3<I: AsyncRead + AsyncWrite + Unpin + 'static>(emu: Rc<S3Emu>, io: I) {
    let svc = hyper::service::service_fn(move |req: Request<Incoming>| {
        let emu = emu.clone();
        async move {
            let (parts, body) = req.into_parts();
            let body = body.collect().await.map(|c| c.to_bytes()).unwrap_or_default();
            match emu.handle(Request::from_parts(parts, body)) {
                Answer::Http(r) => Ok::<_, std::io::Error>(r.map(Full::new)),
                Answer::Drop => Err(std::io::Error::other("connection dropped (injected)")),
            }
        }
    });
    let _ = hyper::server::conn::http1::Builder::new().timer(TokioTimer::new()).serve_connection(TokioIo::new(io), svc).await;
}

/// Serves the ClickHouse emulator on one connection.
pub async fn serve_ch<I: AsyncRead + AsyncWrite + Unpin + 'static>(emu: Rc<ChEmu>, io: I, peer: String) {
    let svc = hyper::service::service_fn(move |req: Request<Incoming>| {
        let emu = emu.clone();
        let peer = peer.clone();
        async move {
            let settings: BTreeMap<String, String> =
                url::form_urlencoded::parse(req.uri().query().unwrap_or("").as_bytes()).map(|(k, v)| (k.into_owned(), v.into_owned())).collect();
            let body = req.into_body().collect().await.map(|c| c.to_bytes()).unwrap_or_default();
            let sql = String::from_utf8_lossy(&body).into_owned();
            let mk = |status: u16, text: String| {
                let mut r = Response::new(Full::new(Bytes::from(text)));
                *r.status_mut() = http::StatusCode::from_u16(status).unwrap();
                r
            };
            match emu.handle(settings, sql, &peer).await {
                Reply::Ok(s) => Ok::<_, std::io::Error>(mk(200, s)),
                Reply::Err(c, s) => Ok(mk(c, s)),
                Reply::Drop => Err(std::io::Error::other("connection dropped (injected)")),
            }
        }
    });
    let _ = hyper::server::conn::http1::Builder::new().timer(TokioTimer::new()).serve_connection(TokioIo::new(io), svc).await;
}

#[derive(Debug)]
pub enum NetErr {
    Connect(std::io::Error),
    Http(hyper::Error),
}

impl std::fmt::Display for NetErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NetErr::Connect(e) => write!(f, "connect: {e}"),
            NetErr::Http(e) => write!(f, "{e}"),
        }
    }
}

/// One request on a fresh turmoil connection (no pooling: every request
/// opens one, so a dropped connection costs only its own request).
pub async fn round_trip<B>(host: &str, port: u16, mut req: Request<B>) -> Result<Response<Bytes>, NetErr>
where
    B: hyper::body::Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    let s = turmoil::net::TcpStream::connect((host, port)).await.map_err(NetErr::Connect)?;
    let (mut tx, conn) = hyper::client::conn::http1::handshake(TokioIo::new(s)).await.map_err(NetErr::Http)?;
    std::mem::drop(tokio::spawn(async move {
        let _ = conn.await;
    }));
    // origin-form, with a Host header
    let pq = req.uri().path_and_query().map(|p| p.as_str().to_string()).unwrap_or_else(|| "/".into());
    *req.uri_mut() = pq.parse().expect("uri");
    let _ = req.headers_mut().insert(http::header::HOST, format!("{host}:{port}").parse().expect("host"));
    let resp = tx.send_request(req).await.map_err(NetErr::Http)?;
    let (parts, body) = resp.into_parts();
    let bytes = body.collect().await.map_err(NetErr::Http)?.to_bytes();
    Ok(Response::from_parts(parts, bytes))
}

/// object_store's HTTP client onto turmoil: errors classified as its
/// reqwest client classifies them (`HttpError::reqwest`), so its retry
/// logic (what it retries, how often) runs as in production.
#[derive(Debug, Clone)]
pub struct TurmoilS3 {
    pub host: String,
    pub port: u16,
    /// reqwest's whole-request timeout (`S3Config::put_timeout`).
    pub timeout: Duration,
    pub pause: Pause,
}

/// A process pause (a stop-the-world GC, SIGSTOP, a frozen VM) as its
/// client I/O sees it: until the given simulated ms nothing is sent and no
/// answer is taken.
#[derive(Debug, Clone, Default)]
pub struct Pause(pub std::sync::Arc<std::sync::atomic::AtomicU64>);

impl Pause {
    pub fn until(&self, ms: u64) {
        self.0.store(ms, std::sync::atomic::Ordering::Relaxed);
    }
    pub async fn gate(&self) {
        loop {
            let until = self.0.load(std::sync::atomic::Ordering::Relaxed);
            let now = super::sim::now_ms();
            if now >= until {
                return;
            }
            tokio::time::sleep(Duration::from_millis(until - now)).await;
        }
    }
}

fn kind_of(e: &hyper::Error) -> HttpErrorKind {
    if e.is_closed() || e.is_incomplete_message() || e.is_body_write_aborted() {
        return HttpErrorKind::Request;
    }
    if e.is_timeout() {
        return HttpErrorKind::Timeout;
    }
    let mut src = std::error::Error::source(e);
    while let Some(s) = src {
        if let Some(io) = s.downcast_ref::<std::io::Error>() {
            match io.kind() {
                std::io::ErrorKind::TimedOut => return HttpErrorKind::Timeout,
                std::io::ErrorKind::ConnectionAborted
                | std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::BrokenPipe
                | std::io::ErrorKind::UnexpectedEof => return HttpErrorKind::Interrupted,
                _ => {}
            }
        }
        src = s.source();
    }
    HttpErrorKind::Unknown
}

#[async_trait::async_trait]
impl HttpService for TurmoilS3 {
    async fn call(&self, req: HttpRequest) -> Result<HttpResponse, HttpError> {
        self.pause.gate().await;
        let r = tokio::time::timeout(self.timeout, round_trip(&self.host, self.port, req)).await;
        self.pause.gate().await;
        match r {
            Err(_) => Err(HttpError::new(HttpErrorKind::Timeout, std::io::Error::new(std::io::ErrorKind::TimedOut, "operation timed out"))),
            Ok(Err(NetErr::Connect(e))) => Err(HttpError::new(HttpErrorKind::Connect, e)),
            Ok(Err(NetErr::Http(e))) => Err(HttpError::new(kind_of(&e), e)),
            Ok(Ok(r)) => Ok(r.map(HttpResponseBody::from)),
        }
    }
}

impl HttpConnector for TurmoilS3 {
    fn connect(&self, _options: &object_store::ClientOptions) -> object_store::Result<HttpClient> {
        Ok(HttpClient::new(self.clone()))
    }
}

/// The ClickHouse client's transport onto turmoil, with reqwest's timeout.
pub fn ch_transport(host: &str, port: u16, timeout: Duration, pause: Pause) -> otap_s3pq::central::Transport {
    let host = host.to_string();
    std::sync::Arc::new(move |req: Request<String>| {
        let host = host.clone();
        let pause = pause.clone();
        Box::pin(async move {
            let req = req.map(|b| Full::new(Bytes::from(b)));
            pause.gate().await;
            let r = tokio::time::timeout(timeout, round_trip(&host, port, req)).await;
            pause.gate().await;
            match r {
                Err(_) => Err("error sending request: operation timed out".to_string()),
                Ok(Err(e)) => Err(format!("error sending request: {e}")),
                Ok(Ok(r)) => Ok((r.status().as_u16(), String::from_utf8_lossy(r.body()).into_owned())),
            }
        })
    })
}
