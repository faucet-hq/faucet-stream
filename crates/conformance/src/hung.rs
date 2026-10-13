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
