//! Bounds on what the server can make the client hold: the length of one
//! inbound SFTP packet ([`LimitedStream`]) and the size of one read request
//! ([`ChunkedReader`]).

use std::io;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll, ready};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// Largest inbound SFTP packet accepted, in bytes (its 4-byte length prefix
/// excluded). A reply to a [`MAX_READ_CHUNK`] read fits with room to spare; a
/// longer packet closes the connection's SFTP channel with a typed error.
pub const MAX_INBOUND_PACKET: u32 = 1024 * 1024 + 64;

/// Largest read requested from the server in one SFTP `READ`, in bytes.
pub const MAX_READ_CHUNK: usize = 1024 * 1024;

/// Records the first packet a [`LimitedStream`] refused, shared by every
/// channel of one SSH connection so callers can explain the failure.
#[derive(Debug, Default)]
pub(crate) struct InboundGuard {
    refused: OnceLock<u32>,
}

impl InboundGuard {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub(crate) fn record(&self, len: u32) {
        let _ = self.refused.set(len);
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

#[derive(Debug, Clone, Copy)]
enum Frame {
    Header { got: usize },
    Emit { pos: usize, body: u32 },
    Body { remaining: u32 },
    Refused,
}

/// An SFTP channel stream that reads each packet's 4-byte length prefix and
/// refuses one above `max` before any of its body is read: the read fails
/// once (the SFTP client logs it) and then reports end of stream, which ends
/// the client's reader. Writes pass through.
pub(crate) struct LimitedStream<S> {
    inner: S,
    max: u32,
    header: [u8; 4],
    frame: Frame,
    guard: Arc<InboundGuard>,
}

impl<S> LimitedStream<S> {
    pub(crate) fn new(inner: S, max: u32, guard: Arc<InboundGuard>) -> Self {
        Self {
            inner,
            max,
            header: [0; 4],
            frame: Frame::Header { got: 0 },
            guard,
        }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for LimitedStream<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        loop {
            match this.frame {
                Frame::Refused => return Poll::Ready(Ok(())),
                Frame::Header { got } => {
                    let mut head = ReadBuf::new(&mut this.header[got..]);
                    ready!(Pin::new(&mut this.inner).poll_read(cx, &mut head))?;
                    let n = head.filled().len();
                    if n == 0 {
                        return Poll::Ready(Ok(()));
                    }
                    let got = got + n;
                    if got < 4 {
                        this.frame = Frame::Header { got };
                        continue;
                    }
                    let len = u32::from_be_bytes(this.header);
                    if len > this.max {
                        this.guard.record(len);
                        this.frame = Frame::Refused;
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            refusal_message(len),
                        )));
                    }
                    this.frame = Frame::Emit { pos: 0, body: len };
                }
                Frame::Emit { pos, body } => {
                    let n = (4 - pos).min(buf.remaining());
                    buf.put_slice(&this.header[pos..pos + n]);
                    let pos = pos + n;
                    this.frame = match (pos, body) {
                        (4, 0) => Frame::Header { got: 0 },
                        (4, body) => Frame::Body { remaining: body },
                        (pos, body) => Frame::Emit { pos, body },
                    };
                    return Poll::Ready(Ok(()));
                }
                Frame::Body { remaining } => {
                    let want = (remaining as usize).min(buf.remaining());
                    let mut part = ReadBuf::new(buf.initialize_unfilled_to(want));
                    ready!(Pin::new(&mut this.inner).poll_read(cx, &mut part))?;
                    let n = part.filled().len();
                    buf.advance(n);
                    if n > 0 {
                        let left = remaining - n as u32;
                        this.frame = if left == 0 {
                            Frame::Header { got: 0 }
                        } else {
                            Frame::Body { remaining: left }
                        };
                    }
                    return Poll::Ready(Ok(()));
                }
            }
        }
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for LimitedStream<S> {
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

/// A reader that asks for at most [`MAX_READ_CHUNK`] bytes per read, so an
/// SFTP file read never requests more than one inbound packet can carry,
/// whatever read length the server advertises.
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
