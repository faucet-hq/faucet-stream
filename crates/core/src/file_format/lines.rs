//! The one streamed JSON Lines reader every file and object-store source uses.

use super::{LineCapped, line_capped};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};

/// Bytes read from the underlying reader per call when streaming a
/// line-delimited body (256 KiB).
pub const READ_BUFFER_BYTES: usize = 256 * 1024;

/// Why [`JsonLines::next_value`] stopped.
#[derive(Debug)]
pub enum JsonLineError {
    /// The body could not be read: an I/O failure, a line past the cap (see
    /// [`line_capped`]), or a line that is not UTF-8.
    Read(std::io::Error),
    /// A line is not valid JSON. `line` counts from 1, blank lines included.
    Parse {
        /// The line's number in the body.
        line: usize,
        /// What the parser said.
        error: serde_json::Error,
    },
}

/// The records of a JSON Lines body, one per non-blank line, parsed with
/// [`fidelity::from_str`](crate::fidelity::from_str). No line may exceed the
/// cap given to [`json_lines`]. One line buffer is reused for the whole body.
#[derive(Debug)]
pub struct JsonLines<R> {
    reader: BufReader<LineCapped<R>>,
    buf: Vec<u8>,
    line: usize,
}

/// Read `reader` as JSON Lines, no line longer than `max` bytes.
pub fn json_lines<R: AsyncRead + Unpin>(reader: R, max: u64) -> JsonLines<R> {
    JsonLines {
        reader: BufReader::with_capacity(READ_BUFFER_BYTES, line_capped(reader, max)),
        buf: Vec::new(),
        line: 0,
    }
}

impl<R: AsyncRead + Unpin> JsonLines<R> {
    /// The next record, `None` at the end of the body. Blank lines are
    /// skipped and still counted.
    pub async fn next_value(&mut self) -> Result<Option<Value>, JsonLineError> {
        loop {
            self.buf.clear();
            let read = self
                .reader
                .read_until(b'\n', &mut self.buf)
                .await
                .map_err(JsonLineError::Read)?;
            if read == 0 {
                return Ok(None);
            }
            self.line += 1;
            let text = std::str::from_utf8(&self.buf).map_err(|e| {
                JsonLineError::Read(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
            })?;
            let text = text.trim();
            if text.is_empty() {
                continue;
            }
            return crate::fidelity::from_str(text).map(Some).map_err(|error| {
                JsonLineError::Parse {
                    line: self.line,
                    error,
                }
            });
        }
    }

    /// The number of the line last read (0 before the first).
    pub fn line(&self) -> usize {
        self.line
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq)]
    enum Step {
        Value(usize, Value),
        Parse(usize, String),
        Read(std::io::ErrorKind, String),
    }

    async fn old_shape(body: &[u8], max: u64) -> Vec<Step> {
        let mut lines = BufReader::new(line_capped(body, max)).lines();
        let mut out = Vec::new();
        let mut n = 0;
        loop {
            match lines.next_line().await {
                Ok(None) => return out,
                Ok(Some(line)) => {
                    n += 1;
                    let trimmed = line.trim();
                    if trimmed.is_empty() {
                        continue;
                    }
                    match crate::fidelity::from_str(trimmed) {
                        Ok(v) => out.push(Step::Value(n, v)),
                        Err(e) => {
                            out.push(Step::Parse(n, e.to_string()));
                            return out;
                        }
                    }
                }
                Err(e) => {
                    out.push(Step::Read(e.kind(), e.to_string()));
                    return out;
                }
            }
        }
    }

    async fn new_shape(body: &[u8], max: u64) -> Vec<Step> {
        let mut lines = json_lines(body, max);
        let mut out = Vec::new();
        loop {
            match lines.next_value().await {
                Ok(None) => return out,
                Ok(Some(v)) => out.push(Step::Value(lines.line(), v)),
                Err(JsonLineError::Parse { line, error }) => {
                    assert_eq!(line, lines.line());
                    out.push(Step::Parse(line, error.to_string()));
                    return out;
                }
                Err(JsonLineError::Read(e)) => {
                    out.push(Step::Read(e.kind(), e.to_string()));
                    return out;
                }
            }
        }
    }

    #[tokio::test]
    async fn reads_what_the_line_string_loop_read() {
        let long = format!("{{\"k\":\"{}\"}}\n", "x".repeat(300_000));
        let bodies: Vec<Vec<u8>> = vec![
            b"".to_vec(),
            b"\n\n".to_vec(),
            b"{\"a\":1}\n{\"a\":2}\n".to_vec(),
            b"{\"a\":1}\r\n\r\n  {\"a\":2}  \r\n{\"a\":3}".to_vec(),
            b"\t{\"a\":1}\t\n \n\n[1,2]\n\"s\"\n12345678901234567890\n".to_vec(),
            b"{\"a\":1}\n{bad}\n{\"a\":3}\n".to_vec(),
            b"{\"a\":1}\n\n\n{\"a\":\n".to_vec(),
            b"{\"a\":1}\n{\"a\":\"\xff\"}\n{\"a\":3}\n".to_vec(),
            "\u{a0}{\"a\":\"é\"}\u{2003}\n{\"b\":1}\u{feff}\n"
                .as_bytes()
                .to_vec(),
            b"{\"a\":1}\r{\"a\":2}\n".to_vec(),
            b"{\"a\":1}\n0123\n".to_vec(),
            long.repeat(3).into_bytes(),
            format!("{long}{{\"tail\":true}}").into_bytes(),
        ];
        for body in &bodies {
            for max in [8u64, 64, 1 << 30] {
                let old = old_shape(body, max).await;
                let new = new_shape(body, max).await;
                assert_eq!(new, old, "max {max}: {:?}", String::from_utf8_lossy(body));
            }
        }
    }

    #[tokio::test]
    async fn a_line_past_the_cap_fails_as_a_read_error() {
        let body = b"{\"a\":1}\n{\"a\":\"0123456789012345678901234567890123456789\"}";
        let mut lines = json_lines(&body[..], 16);
        let err = lines.next_value().await.unwrap_err();
        let JsonLineError::Read(e) = err else {
            panic!("expected a read error, got {err:?}");
        };
        assert_eq!(e.kind(), std::io::ErrorKind::InvalidData);
        assert!(
            e.get_ref()
                .is_some_and(|inner| inner.downcast_ref::<super::super::LineTooLong>().is_some()),
            "{e}"
        );
    }

    #[tokio::test]
    async fn blank_lines_are_counted_but_not_returned() {
        let mut lines = json_lines(&b"\n\n{\"a\":1}\n\n{oops\n"[..], 1024);
        assert_eq!(lines.line(), 0);
        assert_eq!(
            lines.next_value().await.unwrap(),
            Some(serde_json::json!({"a": 1}))
        );
        assert_eq!(lines.line(), 3);
        match lines.next_value().await {
            Err(JsonLineError::Parse { line, error }) => {
                assert_eq!(line, 5);
                assert!(
                    error.to_string().contains("key must be a string"),
                    "{error}"
                );
            }
            other => panic!("{other:?}"),
        }
    }
}
