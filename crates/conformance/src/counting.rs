//! A backend that counts what it has served, for
//! [`assert_first_page_before_exhaustion`](crate::assert_first_page_before_exhaustion)
//! (check 22) against a source that reads a body over HTTP.
//!
//! [`CountingLinesServer`] answers every `GET` with a newline-delimited body
//! written one line at a time, counting each line once the socket accepted
//! it. A client that stops reading stops the writes (TCP back-pressure), so
//! the count at the client's first page shows how far it read ahead.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

/// An HTTP server on `127.0.0.1` serving `lines` lines built by a line
/// function, counting the lines written. Dropping it stops the server.
pub struct CountingLinesServer {
    url: String,
    served: Arc<AtomicUsize>,
    task: JoinHandle<()>,
}

impl std::fmt::Debug for CountingLinesServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CountingLinesServer")
            .field("url", &self.url)
            .field("served", &self.served())
            .finish()
    }
}

impl CountingLinesServer {
    /// Serve `lines` lines, `line(i)` for `i` in `0..lines` (each without its
    /// newline), at `/<file_name>`, with `content_type`. A `HEAD` gets the
    /// headers only.
    pub async fn start(
        file_name: &str,
        content_type: &str,
        lines: usize,
        line: fn(usize) -> String,
    ) -> std::io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}/{file_name}", listener.local_addr()?);
        let served = Arc::new(AtomicUsize::new(0));
        let length: usize = (0..lines).map(|i| line(i).len() + 1).sum();
        let content_type = content_type.to_string();
        let counter = served.clone();
        let task = tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let counter = counter.clone();
                let content_type = content_type.clone();
                tokio::spawn(async move {
                    let mut request = Vec::new();
                    let mut buf = [0u8; 1024];
                    while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                        match socket.read(&mut buf).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => request.extend_from_slice(&buf[..n]),
                        }
                    }
                    let head = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\n\
                         Content-Length: {length}\r\nConnection: close\r\n\r\n"
                    );
                    if socket.write_all(head.as_bytes()).await.is_err()
                        || request.starts_with(b"HEAD ")
                    {
                        return;
                    }
                    for i in 0..lines {
                        let mut text = line(i);
                        text.push('\n');
                        if socket.write_all(text.as_bytes()).await.is_err() {
                            return;
                        }
                        counter.fetch_add(1, Ordering::SeqCst);
                    }
                    let _ = socket.shutdown().await;
                });
            }
        });
        Ok(Self { url, served, task })
    }

    /// The URL of the served body.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Lines written to clients so far, across every request.
    pub fn served(&self) -> usize {
        self.served.load(Ordering::SeqCst)
    }
}

impl Drop for CountingLinesServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn serves_and_counts_every_line() {
        let server = CountingLinesServer::start("a.jsonl", "application/x-ndjson", 3, |i| {
            format!("{{\"i\":{i}}}")
        })
        .await
        .unwrap();
        let mut socket =
            tokio::net::TcpStream::connect(server.url()[7..].split('/').next().unwrap())
                .await
                .unwrap();
        socket
            .write_all(b"GET /a.jsonl HTTP/1.1\r\nHost: x\r\n\r\n")
            .await
            .unwrap();
        let mut body = String::new();
        socket.read_to_string(&mut body).await.unwrap();
        assert!(body.starts_with("HTTP/1.1 200 OK"), "{body}");
        assert!(
            body.ends_with("{\"i\":0}\n{\"i\":1}\n{\"i\":2}\n"),
            "{body}"
        );
        assert!(body.contains("Content-Length: 24"), "{body}");
        assert_eq!(server.served(), 3);
        assert!(format!("{server:?}").contains("served: 3"));
    }
}
