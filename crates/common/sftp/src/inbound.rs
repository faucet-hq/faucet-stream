//! Bounds on what the server can make the client hold: the length of one
//! inbound SFTP packet ([`client_config`], reported through [`Observed`]) and
//! the size of one read handed to a caller ([`ChunkedReader`]).

use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll, ready};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// Largest inbound SFTP packet accepted, in bytes (its 4-byte length prefix
/// excluded). A reply to a [`MAX_READ_CHUNK`] read fits with room to spare; a
/// longer packet closes the SFTP channel it arrived on with a typed error.
pub const MAX_INBOUND_PACKET: u32 = 1024 * 1024 + 64;

/// Largest read a [`ChunkedReader`] passes to the reader it wraps, in bytes.
pub const MAX_READ_CHUNK: usize = 1024 * 1024;

/// Records the first over-limit packet an [`Observed`] stream saw, shared by
/// the streams of one SFTP channel so callers can explain the failure.
#[derive(Debug, Default)]
pub(crate) struct InboundGuard {
    refused: OnceLock<u32>,
    count: AtomicU64,
}

impl InboundGuard {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub(crate) fn record(&self, len: u32) {
        let _ = self.refused.set(len);
        self.count.fetch_add(1, Ordering::SeqCst);
    }

    /// How many packets were refused so far: compared before and after an
    /// operation to tell whether it saw a refusal.
    pub(crate) fn refusals(&self) -> u64 {
        self.count.load(Ordering::SeqCst)
    }

    /// The error text for a refused packet, if one was refused.
    pub(crate) fn refusal(&self) -> Option<String> {
        self.refused.get().map(|len| refusal_message(*len))
    }
}

fn refusal_message(len: u32) -> String {
    format!(
        "the SFTP server sent a {len}-byte packet, larger than the {MAX_INBOUND_PACKET}-byte \
         limit on one packet; the SFTP channel was closed"
    )
}

/// Largest outgoing SFTP `WRITE` packet, unless the server advertises a
/// smaller write length: the 256 KiB message size servers accept by default.
const MAX_WRITE_PACKET: u32 = 256 * 1024;

/// File reads one open file keeps in flight; each holds up to one packet.
const MAX_READS_IN_FLIGHT: usize = 4;

/// SFTP client settings for every session this crate opens: the client
/// refuses an inbound packet longer than [`MAX_INBOUND_PACKET`].
pub(crate) fn client_config() -> russh_sftp::client::Config {
    russh_sftp::client::Config {
        max_packet_len: MAX_INBOUND_PACKET,
        max_write_packet_len: MAX_WRITE_PACKET,
        max_concurrent_reads: MAX_READS_IN_FLIGHT,
        ..Default::default()
    }
}

/// Follows SFTP packet framing over the bytes it has seen and finds the
/// first length prefix above `max`.
#[derive(Debug)]
struct Framing {
    max: u32,
    header: [u8; 4],
    got: usize,
    remaining: u32,
    found: bool,
}

impl Framing {
    fn new(max: u32) -> Self {
        Self {
            max,
            header: [0; 4],
            got: 0,
            remaining: 0,
            found: false,
        }
    }

    /// Advance over `bytes`; the length of an over-limit packet whose
    /// prefix completes in them, the first time one does.
    fn advance(&mut self, mut bytes: &[u8]) -> Option<u32> {
        while !self.found && !bytes.is_empty() {
            if self.remaining > 0 {
                let skip = (self.remaining as usize).min(bytes.len());
                self.remaining -= skip as u32;
                bytes = &bytes[skip..];
                continue;
            }
            let take = (4 - self.got).min(bytes.len());
            self.header[self.got..self.got + take].copy_from_slice(&bytes[..take]);
            self.got += take;
            bytes = &bytes[take..];
            if self.got == 4 {
                self.got = 0;
                let len = u32::from_be_bytes(self.header);
                if len > self.max {
                    self.found = true;
                    return Some(len);
                }
                self.remaining = len;
            }
        }
        None
    }
}

/// An SFTP channel stream that hands every byte through unchanged and, as
/// they pass, records an inbound packet longer than the limit in its
/// [`InboundGuard`]; the SFTP client itself refuses that packet.
pub(crate) struct Observed<S> {
    inner: S,
    framing: Framing,
    guard: Arc<InboundGuard>,
}

impl<S> Observed<S> {
    pub(crate) fn new(inner: S, max: u32, guard: Arc<InboundGuard>) -> Self {
        Self {
            inner,
            framing: Framing::new(max),
            guard,
        }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for Observed<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let before = buf.filled().len();
        ready!(Pin::new(&mut this.inner).poll_read(cx, buf))?;
        if let Some(len) = this.framing.advance(&buf.filled()[before..]) {
            this.guard.record(len);
        }
        Poll::Ready(Ok(()))
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for Observed<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

/// A reader that asks for at most [`MAX_READ_CHUNK`] bytes per read. The SFTP
/// client sizes each `READ` request to fit one inbound packet itself.
pub struct ChunkedReader<R> {
    inner: R,
}

impl<R> ChunkedReader<R> {
    /// Wrap `inner`.
    pub fn new(inner: R) -> Self {
        Self { inner }
    }

    /// The wrapped reader.
    pub fn into_inner(self) -> R {
        self.inner
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for ChunkedReader<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if buf.remaining() <= MAX_READ_CHUNK {
            return Pin::new(&mut this.inner).poll_read(cx, buf);
        }
        let mut part = ReadBuf::new(buf.initialize_unfilled_to(MAX_READ_CHUNK));
        ready!(Pin::new(&mut this.inner).poll_read(cx, &mut part))?;
        let n = part.filled().len();
        buf.advance(n);
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests;
