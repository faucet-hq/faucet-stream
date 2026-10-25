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

/// Read the next line, refusing one longer than `max` bytes. Invalid UTF-8 is
/// replaced; Singer messages use [`read_capped_utf8_line`].
pub async fn read_capped_line<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    max: usize,
) -> std::io::Result<CappedLine> {
    Ok(match read_capped_bytes(reader, max).await? {
        Raw::Line(buf) => CappedLine::Line(String::from_utf8_lossy(&buf).into_owned()),
        Raw::TooLong => CappedLine::TooLong,
        Raw::Eof => CappedLine::Eof,
    })
}

/// [`read_capped_line`] for Singer messages, which are UTF-8 JSON: a line that
/// is not valid UTF-8 is consumed and returned as an
/// [`InvalidData`](std::io::ErrorKind::InvalidData) error instead of having
/// its bytes replaced, so the caller can report it; the next call reads the
/// following line.
pub async fn read_capped_utf8_line<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    max: usize,
) -> std::io::Result<CappedLine> {
    Ok(match read_capped_bytes(reader, max).await? {
        Raw::Line(buf) => CappedLine::Line(String::from_utf8(buf).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("line is not valid UTF-8: {}", e.utf8_error()),
            )
        })?),
        Raw::TooLong => CappedLine::TooLong,
        Raw::Eof => CappedLine::Eof,
    })
}

enum Raw {
    Line(Vec<u8>),
    TooLong,
    Eof,
}

async fn read_capped_bytes<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    max: usize,
) -> std::io::Result<Raw> {
    let mut buf = Vec::new();
    let limit = u64::try_from(max).unwrap_or(u64::MAX).saturating_add(1);
    let n = (&mut *reader)
        .take(limit)
        .read_until(b'\n', &mut buf)
        .await?;
    if n == 0 {
        return Ok(Raw::Eof);
    }
    if buf.last() == Some(&b'\n') {
        buf.pop();
        if buf.last() == Some(&b'\r') {
            buf.pop();
        }
    } else if buf.len() > max {
        return Ok(Raw::TooLong);
    }
    Ok(Raw::Line(buf))
}

/// Longest stderr line kept for logs and the error tail (16 KiB).
pub const MAX_LOG_LINE_BYTES: usize = 16 * 1024;

/// Discard the rest of the current line, in bounded chunks, so the text after
/// an over-long line's cut is never read as a line of its own.
pub async fn skip_line_rest<R: AsyncBufRead + Unpin>(reader: &mut R) -> std::io::Result<()> {
    let mut scratch = Vec::new();
    loop {
        scratch.clear();
        let n = (&mut *reader)
            .take(64 * 1024)
            .read_until(b'\n', &mut scratch)
            .await?;
        if n == 0 || scratch.last() == Some(&b'\n') {
            return Ok(());
        }
    }
}

/// Read the next line for logging: one longer than `max` bytes is cut at
/// `max` (marked `…[truncated]`) and the rest of it skipped. `None` at end of
/// input.
pub async fn read_log_line<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    max: usize,
) -> std::io::Result<Option<String>> {
    let mut buf = Vec::new();
    let limit = u64::try_from(max).unwrap_or(u64::MAX).saturating_add(1);
    let n = (&mut *reader)
        .take(limit)
        .read_until(b'\n', &mut buf)
        .await?;
    if n == 0 {
        return Ok(None);
    }
    let mut truncated = false;
    if buf.last() == Some(&b'\n') {
        buf.pop();
        if buf.last() == Some(&b'\r') {
            buf.pop();
        }
    } else if buf.len() > max {
        buf.truncate(max);
        skip_line_rest(reader).await?;
        truncated = true;
    }
    let mut line = String::from_utf8_lossy(&buf).into_owned();
    if truncated {
        line.push_str(" …[truncated]");
    }
    Ok(Some(line))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn invalid_utf8_is_an_error_and_the_next_line_still_reads() {
        let input: &[u8] = b"{\"name\":\"Jos\xe9\"}\n{\"ok\":1}\n";
        let mut r = tokio::io::BufReader::new(input);
        let err = read_capped_utf8_line(&mut r, 1024).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("not valid UTF-8"), "{err}");
        assert_eq!(
            read_capped_utf8_line(&mut r, 1024).await.unwrap(),
            CappedLine::Line("{\"ok\":1}".into())
        );
        assert_eq!(
            read_capped_utf8_line(&mut r, 1024).await.unwrap(),
            CappedLine::Eof
        );
        let mut lossy = tokio::io::BufReader::new(input);
        assert_eq!(
            read_capped_line(&mut lossy, 1024).await.unwrap(),
            CappedLine::Line("{\"name\":\"Jos\u{FFFD}\"}".into())
        );
        let mut long = tokio::io::BufReader::new(&b"abcdef"[..]);
        assert_eq!(
            read_capped_utf8_line(&mut long, 3).await.unwrap(),
            CappedLine::TooLong
        );
    }

    #[tokio::test]
    async fn log_lines_are_cut_and_the_rest_skipped() {
        let mut r = tokio::io::BufReader::new(&b"abcdef\nok\r\nxyz"[..]);
        assert_eq!(
            read_log_line(&mut r, 3).await.unwrap().as_deref(),
            Some("abc …[truncated]")
        );
        assert_eq!(
            read_log_line(&mut r, 3).await.unwrap().as_deref(),
            Some("ok")
        );
        assert_eq!(
            read_log_line(&mut r, 3).await.unwrap().as_deref(),
            Some("xyz")
        );
        assert_eq!(read_log_line(&mut r, 3).await.unwrap(), None);
    }

    #[tokio::test]
    async fn skipping_stops_at_the_line_end() {
        let mut r = tokio::io::BufReader::new(&b"rest of a long line\nnext\n"[..]);
        skip_line_rest(&mut r).await.unwrap();
        assert_eq!(
            read_log_line(&mut r, 10).await.unwrap().as_deref(),
            Some("next")
        );
        skip_line_rest(&mut r).await.unwrap();
    }

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
