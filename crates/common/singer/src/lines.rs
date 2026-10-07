//! Reading a subprocess's stdout one line at a time with a length cap, so a
//! huge message or binary output without a newline fails instead of growing a
//! buffer until the process is killed for memory.

use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt};

/// Default longest accepted line (64 MiB).
pub const DEFAULT_MAX_LINE_BYTES: usize = 64 * 1024 * 1024;

/// One read from [`read_capped_line`].
#[derive(Debug, PartialEq, Eq)]
pub enum CappedLine {
    /// A complete line, without its line terminator (lossy UTF-8).
    Line(String),
    /// The line exceeded the cap; nothing more should be read.
    TooLong,
    /// End of input.
    Eof,
}

/// Read the next line, refusing one longer than `max` bytes.
pub async fn read_capped_line<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    max: usize,
) -> std::io::Result<CappedLine> {
    let mut buf = Vec::new();
    let limit = u64::try_from(max).unwrap_or(u64::MAX).saturating_add(1);
    let n = (&mut *reader)
        .take(limit)
        .read_until(b'\n', &mut buf)
        .await?;
    if n == 0 {
        return Ok(CappedLine::Eof);
    }
    if buf.last() == Some(&b'\n') {
        buf.pop();
        if buf.last() == Some(&b'\r') {
            buf.pop();
        }
    } else if buf.len() > max {
        return Ok(CappedLine::TooLong);
    }
    Ok(CappedLine::Line(String::from_utf8_lossy(&buf).into_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn lines_are_split_and_capped() {
        let mut r = tokio::io::BufReader::new(&b"ab\r\ncdef\nlast"[..]);
        assert_eq!(
            read_capped_line(&mut r, 4).await.unwrap(),
            CappedLine::Line("ab".into())
        );
        assert_eq!(
            read_capped_line(&mut r, 4).await.unwrap(),
            CappedLine::Line("cdef".into())
        );
        assert_eq!(
            read_capped_line(&mut r, 4).await.unwrap(),
            CappedLine::Line("last".into())
        );
        assert_eq!(read_capped_line(&mut r, 4).await.unwrap(), CappedLine::Eof);
        let mut r = tokio::io::BufReader::new(&b"abcdefgh\n"[..]);
        assert_eq!(
            read_capped_line(&mut r, 4).await.unwrap(),
            CappedLine::TooLong
        );
        let mut r = tokio::io::BufReader::new(&b"abcdefgh"[..]);
        assert_eq!(
            read_capped_line(&mut r, 4).await.unwrap(),
            CappedLine::TooLong
        );
    }
}
