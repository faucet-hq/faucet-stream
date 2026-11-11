//! Bounded directory listings over the low-level SFTP session: entries are
//! filtered as each `READDIR` reply arrives, only matches are kept, and every
//! entry the server sends counts against a limit.

use std::sync::Arc;

use async_trait::async_trait;
use russh::ChannelMsg;
use russh_sftp::client::RawSftpSession;
use russh_sftp::protocol::StatusCode;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::sync::oneshot;

use crate::SftpError;
use crate::inbound::{InboundGuard, LimitedStream, MAX_INBOUND_PACKET};

/// Default for `max_listed_entries`: entries one directory listing may scan.
pub const DEFAULT_MAX_LISTED_ENTRIES: usize = 1_000_000;

/// The attributes of a remote file, as a listing or a stat reports them.
pub use russh_sftp::client::fs::Metadata as SftpMetadata;

/// How long closing a listing channel waits for the server's own close
/// before going on.
const CLOSE_WAIT: std::time::Duration = std::time::Duration::from_secs(5);

/// Bytes of server output the listing channel buffers ahead of the reader.
const PUMP_BUFFER: usize = 256 * 1024;

/// One entry a listing kept.
#[derive(Debug, Clone)]
pub struct ListedEntry {
    /// The entry's name inside the listed directory.
    pub name: String,
    /// The listed path joined with the name by `/`.
    pub path: String,
    /// The attributes the server sent with the entry.
    pub metadata: SftpMetadata,
}

/// Why a directory listing failed.
#[derive(Debug)]
#[non_exhaustive]
pub enum ListError {
    /// The directory holds more entries than the limit allows.
    TooManyEntries {
        /// The listed path.
        path: String,
        /// The limit (`max_listed_entries`).
        limit: usize,
    },
    /// The server sent a packet longer than
    /// [`MAX_INBOUND_PACKET`](crate::MAX_INBOUND_PACKET).
    PacketTooLarge(String),
    /// The server refused the extra channel the listing runs on.
    ChannelRefused {
        /// `host:port` of the server.
        server: String,
        /// The SSH error.
        reason: String,
    },
    /// The server refused an extra channel on the connection, so the listing
    /// opened a second connection, and that failed too.
    SecondConnectionRefused {
        /// `host:port` of the server.
        server: String,
        /// Why the second connection or its channel failed.
        reason: String,
    },
    /// Opening or starting the listing channel failed for another reason.
    Channel(String),
    /// An SFTP request failed.
    Sftp {
        /// The operation (`list`, `stat`).
        what: &'static str,
        /// The path it was for.
        path: String,
        /// The server's or the transport's error.
        error: SftpError,
    },
}

impl ListError {
    /// Whether the server answered with its typed "no such file" status.
    pub fn is_no_such_file(&self) -> bool {
        matches!(self, ListError::Sftp { error, .. } if crate::is_no_such_file(error))
    }

    fn sftp(what: &'static str, path: &str, error: SftpError) -> Self {
        ListError::Sftp {
            what,
            path: path.to_string(),
            error,
        }
    }

    /// A transport failure after a refused packet is reported as the refusal.
    pub(crate) fn explained(self, guard: &InboundGuard) -> Self {
        match (self, guard.refusal()) {
            (ListError::Sftp { .. } | ListError::Channel(_), Some(refusal)) => {
                ListError::PacketTooLarge(refusal)
            }
            (other, _) => other,
        }
    }
}

impl std::fmt::Display for ListError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ListError::TooManyEntries { path, limit } => write!(
                f,
                "SFTP listing of '{path}' stopped after {limit} entries (max_listed_entries); \
                 raise max_listed_entries or point the path at a smaller directory"
            ),
            ListError::PacketTooLarge(m) | ListError::Channel(m) => f.write_str(m),
            ListError::ChannelRefused { server, reason } => write!(
                f,
                "the SFTP server at {server} refused the extra SSH channel a directory listing \
                 runs on ({reason}); the server may allow only one session per connection \
                 (sshd MaxSessions)"
            ),
            ListError::SecondConnectionRefused { server, reason } => write!(
                f,
                "the SFTP server at {server} allows one session per connection (sshd \
                 MaxSessions), so the directory listing opened a second connection, which \
                 failed too ({reason}); allow two sessions per connection or two connections \
                 per user"
            ),
            ListError::Sftp { what, path, error } => {
                write!(f, "SFTP {what} '{path}' failed: {error}")
            }
        }
    }
}

impl std::error::Error for ListError {}

/// The three requests a listing makes, so the loop can be tested without a
/// server.
#[async_trait]
pub(crate) trait DirReader: Send + Sync {
    async fn open_dir(&self, path: &str) -> Result<String, SftpError>;
    /// The next reply's entries, or `None` at the end of the directory.
    async fn read_dir(
        &self,
        handle: &str,
    ) -> Result<Option<Vec<(String, SftpMetadata)>>, SftpError>;
    async fn close_dir(&self, handle: String);
}

#[async_trait]
impl DirReader for RawSftpSession {
    async fn open_dir(&self, path: &str) -> Result<String, SftpError> {
        Ok(self.opendir(path).await?.handle)
    }

    async fn read_dir(
        &self,
        handle: &str,
    ) -> Result<Option<Vec<(String, SftpMetadata)>>, SftpError> {
        match self.readdir(handle).await {
            Ok(name) => Ok(Some(
                name.files
                    .into_iter()
                    .map(|f| (f.filename, f.attrs))
                    .collect(),
            )),
            Err(SftpError::Status(s)) if s.status_code == StatusCode::Eof => Ok(None),
            Err(e) => Err(e),
        }
    }

    async fn close_dir(&self, handle: String) {
        let _ = self.close(handle).await;
    }
}

fn join(parent: &str, name: &str) -> String {
    if parent.is_empty() {
        name.to_string()
    } else if parent.ends_with('/') {
        format!("{parent}{name}")
    } else {
        format!("{parent}/{name}")
    }
}

/// List `path`, keeping the entries `keep` accepts. Every entry the server
/// sends (`.` and `..` aside) counts against `limit`; one more fails the
/// listing. The directory handle is closed on every outcome.
pub(crate) async fn list_with<D, F>(
    reader: &D,
    path: &str,
    limit: usize,
    mut keep: F,
) -> Result<Vec<ListedEntry>, ListError>
where
    D: DirReader + ?Sized,
    F: FnMut(&str, &SftpMetadata) -> bool + Send,
{
    let handle = reader
        .open_dir(path)
        .await
        .map_err(|e| ListError::sftp("list", path, e))?;
    let mut kept = Vec::new();
    let mut scanned = 0usize;
    let result = loop {
        let batch = match reader.read_dir(&handle).await {
            Ok(Some(batch)) => batch,
            Ok(None) => break Ok(()),
            Err(e) => break Err(ListError::sftp("list", path, e)),
        };
        let mut over = false;
        for (name, metadata) in batch {
            if name == "." || name == ".." {
                continue;
            }
            scanned += 1;
            if scanned > limit {
                over = true;
                break;
            }
            if keep(&name, &metadata) {
                kept.push(ListedEntry {
                    path: join(path, &name),
                    name,
                    metadata,
                });
            }
        }
        if over {
            break Err(ListError::TooManyEntries {
                path: path.to_string(),
                limit,
            });
        }
    };
    reader.close_dir(handle).await;
    result.map(|()| kept)
}

/// An `AsyncRead` and an `AsyncWrite` joined into one stream.
struct Joined<R, W> {
    read: R,
    write: W,
}

impl<R: AsyncRead + Unpin, W: Unpin> AsyncRead for Joined<R, W> {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.get_mut().read).poll_read(cx, buf)
    }
}

impl<R: Unpin, W: AsyncWrite + Unpin> AsyncWrite for Joined<R, W> {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.get_mut().write).poll_write(cx, buf)
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.get_mut().write).poll_flush(cx)
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.get_mut().write).poll_shutdown(cx)
    }
}

/// The SSH channel a listing runs on, kept so it can be closed and the
/// server's close awaited before another channel is opened.
struct ListingChannel {
    write: Option<russh::ChannelWriteHalf<russh::client::Msg>>,
    closed: Option<oneshot::Receiver<()>>,
}

impl ListingChannel {
    /// Start the SFTP client on `channel` (its subsystem already requested).
    fn start(
        channel: russh::Channel<russh::client::Msg>,
        guard: Arc<InboundGuard>,
    ) -> (Self, RawSftpSession) {
        let (mut read, write) = channel.split();
        let (output, mut pump) = tokio::io::duplex(PUMP_BUFFER);
        let (done, closed) = oneshot::channel();
        tokio::spawn(async move {
            let mut forwarding = true;
            while let Some(msg) = read.wait().await {
                match msg {
                    ChannelMsg::Data { data } => {
                        if forwarding && pump.write_all(&data).await.is_err() {
                            forwarding = false;
                        }
                    }
                    ChannelMsg::Eof => {
                        let _ = pump.shutdown().await;
                        forwarding = false;
                    }
                    ChannelMsg::Close => break,
                    _ => {}
                }
            }
            drop(pump);
            let _ = done.send(());
        });
        let stream = Joined {
            read: output,
            write: write.make_writer(),
        };
        let raw = RawSftpSession::new(LimitedStream::new(stream, MAX_INBOUND_PACKET, guard));
        (
            Self {
                write: Some(write),
                closed: Some(closed),
            },
            raw,
        )
    }

    async fn close(&mut self) {
        let Some(write) = self.write.take() else {
            return;
        };
        let _ = write.eof().await;
        let confirmed = match self.closed.take() {
            Some(closed) => tokio::time::timeout(CLOSE_WAIT, closed).await.is_ok(),
            None => true,
        };
        if !confirmed {
            tracing::debug!("SFTP listing channel: the server did not confirm its close in time");
        }
        let _ = write.close().await;
    }
}

impl Drop for ListingChannel {
    fn drop(&mut self) {
        if let Some(write) = self.write.take()
            && let Ok(runtime) = tokio::runtime::Handle::try_current()
        {
            runtime.spawn(async move {
                let _ = write.eof().await;
                let _ = write.close().await;
            });
        }
    }
}

/// A short-lived SFTP channel for listings and stats, opened by
/// [`SftpHandle::lister`](crate::SftpHandle::lister). Call
/// [`close`](Self::close) before opening another channel on a server that
/// allows one session at a time; dropping it closes the channel without
/// waiting.
pub struct SftpLister {
    raw: RawSftpSession,
    channel: ListingChannel,
    guard: Arc<InboundGuard>,
}

impl SftpLister {
    pub(crate) fn start(
        channel: russh::Channel<russh::client::Msg>,
        guard: Arc<InboundGuard>,
    ) -> Self {
        let (channel, raw) = ListingChannel::start(channel, guard.clone());
        Self {
            raw,
            channel,
            guard,
        }
    }

    pub(crate) async fn init(&self) -> Result<(), ListError> {
        self.raw.init().await.map(drop).map_err(|e| {
            ListError::Channel(format!("SFTP listing channel failed to start: {e}"))
                .explained(&self.guard)
        })
    }

    /// List `path`, keeping the entries `keep` accepts; see
    /// [`SftpConnection::list_dir`](crate::SftpConnection::list_dir).
    pub async fn list_dir<F>(
        &self,
        path: &str,
        limit: usize,
        keep: F,
    ) -> Result<Vec<ListedEntry>, ListError>
    where
        F: FnMut(&str, &SftpMetadata) -> bool + Send,
    {
        list_with(&self.raw, path, limit, keep)
            .await
            .map_err(|e| e.explained(&self.guard))
    }

    /// Whether this channel refused a packet (and so closed).
    pub(crate) fn refused(&self) -> bool {
        self.guard.refusal().is_some()
    }

    #[cfg(test)]
    pub(crate) fn guard_for_tests(&self) -> &InboundGuard {
        &self.guard
    }

    /// The attributes of `path`, following symbolic links.
    pub async fn metadata(&self, path: &str) -> Result<SftpMetadata, ListError> {
        self.raw
            .stat(path)
            .await
            .map(|a| a.attrs)
            .map_err(|e| ListError::sftp("stat", path, e).explained(&self.guard))
    }

    /// Close the channel and wait (briefly) for the server to confirm, so a
    /// server limited to one session accepts the next channel.
    pub async fn close(self) {
        let Self {
            raw, mut channel, ..
        } = self;
        drop(raw);
        channel.close().await;
    }
}

#[cfg(test)]
#[allow(clippy::disallowed_methods)] // a fake server paces its replies with tokio::time::sleep
mod tests;
