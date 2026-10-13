//! A backend that never answers, for
//! [`calls::assert_source_call_deadline`](crate::calls::assert_source_call_deadline)
//! and [`calls::assert_sink_call_deadline`](crate::calls::assert_sink_call_deadline)
//! (#880).
//!
//! [`SilentListener`] accepts TCP connections and holds them open without
//! reading or writing a byte, so an HTTP request, a gRPC handshake, an SSH
//! banner exchange or a database login against it hangs until the client's
//! own deadline ends it.
//!
//! [`HangingProxy`] is for a client that must reach a live backend while it
//! is built (a session pool, an SSH handshake): it forwards to the real
//! backend until [`hang`](HangingProxy::hang), after which nothing more passes
//! in either direction on any connection.
//!
//! [`StallingBodyServer`] answers HTTP requests but can stop a response body
//! mid-stream, for proving a connector reads the body under its call context,
//! not only the request.

use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::JoinHandle;

/// A TCP listener on `127.0.0.1` that accepts and never answers. Dropping it
/// closes the listener and every connection it holds.
#[derive(Debug)]
pub struct SilentListener {
    addr: SocketAddr,
    task: JoinHandle<()>,
}

impl SilentListener {
    /// Bind an ephemeral port and start accepting.
    pub async fn start() -> std::io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let task = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((socket, _)) = listener.accept().await {
                held.push(socket);
            }
        });
        Ok(Self { addr, task })
    }

    /// The bound address.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// The bound port.
    pub fn port(&self) -> u16 {
        self.addr.port()
    }

    /// `http://127.0.0.1:<port>`.
    pub fn http_url(&self) -> String {
        format!("http://{}", self.addr)
    }
}

impl Drop for SilentListener {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// A TCP proxy on `127.0.0.1` in front of `target` that can be made to hang.
/// Dropping it closes the listener and every connection.
#[derive(Debug)]
pub struct HangingProxy {
    addr: SocketAddr,
    frozen: watch::Sender<bool>,
    task: JoinHandle<()>,
}

impl HangingProxy {
    /// Bind an ephemeral port forwarding to `target` (`host:port`).
    pub async fn start(target: &str) -> std::io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let (frozen, watch) = watch::channel(false);
        let target = target.to_string();
        let task = tokio::spawn(async move {
            let mut pumps = tokio::task::JoinSet::new();
            while let Ok((client, _)) = listener.accept().await {
                let Ok(server) = TcpStream::connect(&target).await else {
                    continue;
                };
                let (cr, cw) = client.into_split();
                let (sr, sw) = server.into_split();
                pumps.spawn(pump(cr, sw, watch.clone()));
                pumps.spawn(pump(sr, cw, watch.clone()));
            }
        });
        Ok(Self { addr, frozen, task })
    }

    /// Stop forwarding: every byte either side sends from now on is held.
    pub fn hang(&self) {
        let _ = self.frozen.send(true);
    }

    /// Forward again (bytes held while hung are dropped).
    pub fn resume(&self) {
        let _ = self.frozen.send(false);
    }

    /// The bound address.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }
}

impl Drop for HangingProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// What [`StallingBodyServer`] answers to one request: `200 OK` with a
/// content type, any extra headers, and either the whole body
/// ([`full`](Self::full)) or a body that stops mid-stream
/// ([`stall_body`](Self::stall_body)).
#[derive(Debug, Clone)]
pub struct Reply {
    content_type: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
    stall_at_total: Option<usize>,
}

impl Reply {
    /// A complete `200 OK` response.
    pub fn full(content_type: &str, body: impl Into<Vec<u8>>) -> Self {
        Self {
            content_type: content_type.to_string(),
            headers: Vec::new(),
            body: body.into(),
            stall_at_total: None,
        }
    }

    /// A `200 OK` announcing `total` body bytes that sends `prefix` and then
    /// nothing more: the headers arrive, the body stops mid-stream.
    pub fn stall_body(content_type: &str, prefix: impl Into<Vec<u8>>, total: usize) -> Self {
        Self {
            stall_at_total: Some(total),
            ..Self::full(content_type, prefix)
        }
    }

    /// Add a response header.
    #[must_use]
    pub fn header(mut self, name: &str, value: &str) -> Self {
        self.headers.push((name.to_string(), value.to_string()));
        self
    }
}

type Route = std::sync::Arc<dyn Fn(&str, &str) -> Reply + Send + Sync>;

/// An HTTP/1.1 server on `127.0.0.1` that answers each request with the
/// [`Reply`] its route returns for `(method, target)`. A
/// [`Reply::stall_body`] sends the response head and part of the body, then
/// holds the connection open — the backend for proving a connector bounds a
/// streamed response *body* by its call context ([`CallContext::reader`](faucet_core::io::CallContext::reader)),
/// not only the request. Requests must carry no body. Dropping it closes the
/// listener and every connection.
pub struct StallingBodyServer {
    addr: SocketAddr,
    task: JoinHandle<()>,
}

impl std::fmt::Debug for StallingBodyServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StallingBodyServer")
            .field("addr", &self.addr)
            .finish_non_exhaustive()
    }
}

impl StallingBodyServer {
    /// Bind an ephemeral port answering through `route(method, target)`.
    pub async fn start<F>(route: F) -> std::io::Result<Self>
    where
        F: Fn(&str, &str) -> Reply + Send + Sync + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let route: Route = std::sync::Arc::new(route);
        let task = tokio::spawn(async move {
            let mut conns = tokio::task::JoinSet::new();
            while let Ok((socket, _)) = listener.accept().await {
                conns.spawn(serve(socket, route.clone()));
            }
        });
        Ok(Self { addr, task })
    }

    /// The bound address.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// `http://127.0.0.1:<port>`.
    pub fn http_url(&self) -> String {
        format!("http://{}", self.addr)
    }
}

impl Drop for StallingBodyServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn serve(mut socket: TcpStream, route: Route) {
    let mut pending = Vec::new();
    let mut buf = [0u8; 4096];
    loop {
        let head_end = loop {
            if let Some(i) = pending.windows(4).position(|w| w == b"\r\n\r\n") {
                break i + 4;
            }
            match socket.read(&mut buf).await {
                Ok(0) | Err(_) => return,
                Ok(n) => pending.extend_from_slice(&buf[..n]),
            }
        };
        let head = String::from_utf8_lossy(&pending[..head_end]).into_owned();
        pending.drain(..head_end);
        let mut words = head.split_whitespace();
        let method = words.next().unwrap_or_default().to_string();
        let target = words.next().unwrap_or_default().to_string();
        let reply = route(&method, &target);
        let total = reply.stall_at_total.unwrap_or(reply.body.len());
        let mut response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: {}\r\ncontent-length: {total}\r\n",
            reply.content_type
        );
        for (name, value) in &reply.headers {
            response.push_str(&format!("{name}: {value}\r\n"));
        }
        response.push_str("\r\n");
        if socket.write_all(response.as_bytes()).await.is_err()
            || (method != "HEAD" && socket.write_all(&reply.body).await.is_err())
        {
            return;
        }
        if reply.stall_at_total.is_some() {
            std::future::pending::<()>().await;
        }
    }
}

async fn pump(
    mut from: tokio::net::tcp::OwnedReadHalf,
    mut to: tokio::net::tcp::OwnedWriteHalf,
    frozen: watch::Receiver<bool>,
) {
    let mut buf = vec![0u8; 16 * 1024];
    loop {
        let n = match from.read(&mut buf).await {
            Ok(0) | Err(_) => return,
            Ok(n) => n,
        };
        if *frozen.borrow() {
            continue;
        }
        if to.write_all(&buf[..n]).await.is_err() {
            return;
        }
    }
}

#[cfg(test)]
#[allow(clippy::disallowed_methods)] // the test bounds the read itself
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn the_proxy_forwards_until_it_hangs() {
        let echo = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target = echo.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            while let Ok((mut s, _)) = echo.accept().await {
                tokio::spawn(async move {
                    let mut buf = [0u8; 64];
                    while let Ok(n) = s.read(&mut buf).await {
                        if n == 0 || s.write_all(&buf[..n]).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });
        let proxy = HangingProxy::start(&target).await.unwrap();
        let mut conn = TcpStream::connect(proxy.addr()).await.unwrap();
        conn.write_all(b"ping").await.unwrap();
        let mut buf = [0u8; 4];
        conn.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"ping");

        proxy.hang();
        conn.write_all(b"held").await.unwrap();
        let read = tokio::time::timeout(Duration::from_millis(200), conn.read(&mut buf)).await;
        assert!(read.is_err(), "a hung proxy must not answer");

        proxy.resume();
        conn.write_all(b"back").await.unwrap();
        conn.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"back");
    }

    #[tokio::test]
    async fn the_stalling_server_answers_full_then_stalls_a_body() {
        let server = StallingBodyServer::start(|method, target| {
            if target.starts_with("/list") {
                Reply::full("application/json", format!("{method} ok"))
            } else {
                Reply::stall_body("text/plain", "abc", 100).header("x-extra", "1")
            }
        })
        .await
        .unwrap();
        assert!(
            server
                .http_url()
                .ends_with(&server.addr().port().to_string())
        );
        assert!(format!("{server:?}").contains("StallingBodyServer"));
        let mut conn = TcpStream::connect(server.addr()).await.unwrap();
        conn.write_all(b"GET /list HTTP/1.1\r\nhost: x\r\n\r\n")
            .await
            .unwrap();
        let mut got = Vec::new();
        while !got.ends_with(b"GET ok") {
            let mut buf = [0u8; 256];
            let n = conn.read(&mut buf).await.unwrap();
            assert!(n > 0, "connection closed early");
            got.extend_from_slice(&buf[..n]);
        }
        let text = String::from_utf8_lossy(&got);
        assert!(text.starts_with("HTTP/1.1 200 OK"), "{text}");
        assert!(text.contains("content-length: 6"), "{text}");

        conn.write_all(b"GET /obj HTTP/1.1\r\n\r\n").await.unwrap();
        let mut got = Vec::new();
        while !got.ends_with(b"abc") {
            let mut buf = [0u8; 256];
            let n = conn.read(&mut buf).await.unwrap();
            assert!(n > 0, "connection closed early");
            got.extend_from_slice(&buf[..n]);
        }
        let text = String::from_utf8_lossy(&got);
        assert!(text.contains("content-length: 100"), "{text}");
        assert!(text.contains("x-extra: 1"), "{text}");
        let mut buf = [0u8; 1];
        let read = tokio::time::timeout(Duration::from_millis(200), conn.read(&mut buf)).await;
        assert!(read.is_err(), "the body must stall");

        let mut closed = TcpStream::connect(server.addr()).await.unwrap();
        closed.write_all(b"GET /half").await.unwrap();
        drop(closed);
    }

    /// A client that hangs up is passed on: the proxy closes its side to the
    /// target, and a target that cannot be reached closes the client.
    #[tokio::test]
    async fn the_proxy_passes_a_hang_up_on() {
        let echo = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target = echo.local_addr().unwrap().to_string();
        let (eof_tx, eof_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let (mut s, _) = echo.accept().await.unwrap();
            let mut buf = [0u8; 8];
            let n = s.read(&mut buf).await.unwrap();
            let _ = eof_tx.send(n);
        });
        let proxy = HangingProxy::start(&target).await.unwrap();
        drop(TcpStream::connect(proxy.addr()).await.unwrap());
        let n = tokio::time::timeout(Duration::from_secs(5), eof_rx)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(n, 0, "the target sees the hang-up");

        let gone = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let unreachable = gone.local_addr().unwrap().to_string();
        drop(gone);
        let proxy = HangingProxy::start(&unreachable).await.unwrap();
        let mut conn = TcpStream::connect(proxy.addr()).await.unwrap();
        let mut buf = [0u8; 1];
        let n = tokio::time::timeout(Duration::from_secs(5), conn.read(&mut buf))
            .await
            .unwrap()
            .unwrap_or(0);
        assert_eq!(n, 0, "no target, so the client is closed");
    }

    /// A client that hangs up before finishing its request ends the
    /// connection; the server keeps answering others.
    #[tokio::test]
    async fn the_stalling_server_survives_a_half_request() {
        let server = StallingBodyServer::start(|_, _| Reply::full("text/plain", "ok"))
            .await
            .unwrap();
        let mut half = TcpStream::connect(server.addr()).await.unwrap();
        half.write_all(b"GET /half").await.unwrap();
        drop(half);
        tokio::time::sleep(Duration::from_millis(100)).await;
        let mut conn = TcpStream::connect(server.addr()).await.unwrap();
        conn.write_all(b"GET / HTTP/1.1\r\n\r\n").await.unwrap();
        let mut got = Vec::new();
        while !got.ends_with(b"ok") {
            let mut buf = [0u8; 64];
            let n = conn.read(&mut buf).await.unwrap();
            assert!(n > 0, "connection closed early");
            got.extend_from_slice(&buf[..n]);
        }
    }

    #[tokio::test]
    async fn accepts_and_never_answers() {
        let hung = SilentListener::start().await.unwrap();
        assert_eq!(hung.http_url(), format!("http://127.0.0.1:{}", hung.port()));
        let mut conn = tokio::net::TcpStream::connect(hung.addr()).await.unwrap();
        conn.write_all(b"GET / HTTP/1.1\r\n\r\n").await.unwrap();
        let mut buf = [0u8; 1];
        let read = tokio::time::timeout(Duration::from_millis(200), conn.read(&mut buf)).await;
        assert!(read.is_err(), "the listener must not answer");
    }
}
