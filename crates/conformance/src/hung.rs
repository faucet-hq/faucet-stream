//! A backend that never answers, for
//! [`calls::assert_source_call_deadline`](crate::calls::assert_source_call_deadline)
//! and [`calls::assert_sink_call_deadline`](crate::calls::assert_sink_call_deadline)
//! (#880).
//!
//! [`SilentListener`] accepts TCP connections and holds them open without
//! reading or writing a byte, so an HTTP request, a gRPC handshake, an SSH
//! banner exchange or a database login against it hangs until the client's
//! own deadline ends it.

use std::net::SocketAddr;
use tokio::net::TcpListener;
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

#[cfg(test)]
#[allow(clippy::disallowed_methods)] // the test bounds the read itself
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

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
