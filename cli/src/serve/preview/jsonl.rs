//! A streaming **JSON Lines reader** as a [`Source`] (#586).
//!
//! The preview engine reads a sink's output back through the matching *source*
//! connector: `csv` → `faucet-source-csv`, `parquet` → `faucet-source-parquet`.
//! The `jsonl` sink has no counterpart — there is no `faucet-source-jsonl`
//! crate — so this is that counterpart, implemented as a real
//! [`Source`] rather than as a special case inside the
//! engine. The engine therefore stays one code path for every format, and this
//! reader could be lifted into a connector crate unchanged.
//!
//! It is deliberately **not** registered as a pipeline source kind
//! (`faucet list` / `faucet schema source …` do not advertise it): reading
//! NDJSON as a pipeline input is a connector-shaped capability with its own
//! config surface, schema, and docs, and claiming it here would be a half
//! promise.
//!
//! Streaming matters more than it looks: [`stream_pages`](Source::stream_pages)
//! yields as it parses, so a capped preview of a 4 GiB `out.jsonl` reads the
//! first few kilobytes and stops when the consumer drops the stream. The
//! optional [`limit`](JsonLinesConfig::limit) is the second line of defence for
//! a consumer that drains instead.

use faucet_core::{FaucetError, Source, Stream, StreamPage};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::pin::Pin;

/// The source kind this reader answers to inside the preview engine.
pub const KIND: &str = "jsonl";

fn default_batch_size() -> usize {
    faucet_core::DEFAULT_BATCH_SIZE
}

/// Configuration for [`JsonLinesSource`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonLinesConfig {
    /// Path to the `.jsonl` / `.ndjson` file.
    pub path: String,
    /// Records per emitted [`StreamPage`].
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Stop after this many records. `None` **or `Some(0)`** = read to EOF —
    /// `0` is "no limit" here, the same convention as `batch_size: 0` ("do not
    /// batch") and the preview caps, so a caller that computes a limit of zero
    /// meaning "unbounded" is not silently answered with an empty file.
    #[serde(default)]
    pub limit: Option<usize>,
    /// Compression codec. Defaults to `Auto` — a `.gz` / `.zst` suffix selects
    /// gzip / zstd, which is exactly what the jsonl sink's own `compression`
    /// setting produces.
    #[cfg(feature = "compression")]
    #[serde(default)]
    pub compression: faucet_core::CompressionConfig,
    /// Longest line read, in bytes; a longer one fails the read instead of
    /// being buffered whole. Defaults to the preview response budget.
    #[serde(default = "default_max_line_bytes")]
    pub max_line_bytes: usize,
}

fn default_max_line_bytes() -> usize {
    super::engine::PREVIEW_MAX_BYTES
}

/// Read one `\n`-terminated line into `buf` (cleared first), refusing one
/// longer than `max`. `Ok(false)` at end of input.
async fn read_line_bounded<R>(r: &mut R, buf: &mut Vec<u8>, max: usize) -> std::io::Result<bool>
where
    R: tokio::io::AsyncBufRead + Unpin + ?Sized,
{
    use tokio::io::AsyncBufReadExt;
    buf.clear();
    loop {
        let chunk = r.fill_buf().await?;
        if chunk.is_empty() {
            return Ok(!buf.is_empty());
        }
        let (take, done) = match chunk.iter().position(|b| *b == b'\n') {
            Some(i) => (i + 1, true),
            None => (chunk.len(), false),
        };
        if buf.len() + take > max.saturating_add(1) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("a line is longer than {max} bytes"),
            ));
        }
        buf.extend_from_slice(&chunk[..take]);
        r.consume(take);
        if done {
            return Ok(true);
        }
    }
}

impl JsonLinesConfig {
    /// A reader for `path` with defaults.
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            batch_size: default_batch_size(),
            limit: None,
            #[cfg(feature = "compression")]
            compression: faucet_core::CompressionConfig::Auto,
            max_line_bytes: default_max_line_bytes(),
        }
    }

    /// Set the longest line the reader accepts.
    pub fn max_line_bytes(mut self, max: usize) -> Self {
        self.max_line_bytes = max;
        self
    }

    /// Cap the number of records read.
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Set the per-page record count.
    pub fn batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }
}

/// Reads a JSON Lines file, one JSON value per non-blank line.
pub struct JsonLinesSource {
    config: JsonLinesConfig,
}

impl JsonLinesSource {
    pub fn new(config: JsonLinesConfig) -> Self {
        Self { config }
    }
}

#[async_trait::async_trait]
impl Source for JsonLinesSource {
    async fn fetch_with_context(
        &self,
        context: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        use futures::StreamExt;
        let mut out = Vec::new();
        let mut pages = self.stream_pages(context, self.config.batch_size);
        while let Some(page) = pages.next().await {
            out.extend(page?.records);
        }
        Ok(out)
    }

    /// Stream the file page by page.
    ///
    /// The trait-level `batch_size` hint is ignored in favour of the config
    /// field, matching [`faucet-source-csv`](faucet_source_csv)'s contract: the
    /// config is the knob the caller set explicitly, and letting the pipeline's
    /// hint override it would silently discard that choice.
    ///
    /// A line that is not valid JSON aborts the stream with a typed error naming
    /// the file and line number — a partially-written last line (the run died
    /// mid-flush) is a real and expected case, and reporting *where* is the
    /// difference between "malformed file" and a useful message.
    fn stream_pages<'a>(
        &'a self,
        _context: &'a HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        let path = self.config.path.clone();
        let chunk = if self.config.batch_size == 0 {
            usize::MAX
        } else {
            self.config.batch_size
        };
        let limit = match self.config.limit {
            None | Some(0) => usize::MAX,
            Some(n) => n,
        };
        #[cfg(feature = "compression")]
        let codec = self.config.compression.resolve(&path);
        let max_line = self.config.max_line_bytes;

        Box::pin(async_stream::try_stream! {
            let file = tokio::fs::File::open(&path).await.map_err(|e| {
                FaucetError::Source(format!("failed to open JSON Lines file '{path}': {e}"))
            })?;
            let reader = tokio::io::BufReader::new(file);
            #[cfg(feature = "compression")]
            let reader = faucet_core::compression::wrap_async_reader(reader, codec);
            let mut reader = reader;
            let mut buf = Vec::new();

            let mut batch: Vec<Value> = Vec::new();
            let mut emitted = 0usize;
            let mut lineno = 0usize;
            while emitted < limit {
                let more = read_line_bounded(&mut reader, &mut buf, max_line).await.map_err(|e| {
                    FaucetError::Source(format!("reading '{path}' line {}: {e}", lineno + 1))
                })?;
                if !more {
                    break;
                }
                lineno += 1;
                let line = std::str::from_utf8(&buf).map_err(|e| {
                    FaucetError::Source(format!("'{path}' line {lineno} is not UTF-8: {e}"))
                })?;
                // Blank lines are not records. The jsonl sink never writes one,
                // but a hand-edited or concatenated file has them and refusing
                // to read such a file would be pedantry.
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                let value: Value = serde_json::from_str(trimmed).map_err(|e| {
                    FaucetError::Source(format!(
                        "malformed JSON at '{path}' line {lineno}: {e}"
                    ))
                })?;
                batch.push(value);
                emitted += 1;
                if batch.len() >= chunk {
                    yield StreamPage { records: std::mem::take(&mut batch), bookmark: None };
                }
            }
            if !batch.is_empty() {
                yield StreamPage { records: batch, bookmark: None };
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    fn write(dir: &std::path::Path, name: &str, body: &str) -> String {
        let p = dir.join(name);
        std::fs::write(&p, body).unwrap();
        p.to_string_lossy().to_string()
    }

    #[tokio::test]
    async fn reads_one_record_per_line() {
        let dir = tempfile::tempdir().unwrap();
        let path = write(dir.path(), "out.jsonl", "{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n");
        let src = JsonLinesSource::new(JsonLinesConfig::new(path));
        let rows = src.fetch_all().await.unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[2]["a"], 3);
    }

    #[tokio::test]
    async fn blank_lines_are_skipped_not_records() {
        let dir = tempfile::tempdir().unwrap();
        let path = write(dir.path(), "out.jsonl", "{\"a\":1}\n\n   \n{\"a\":2}\n");
        let src = JsonLinesSource::new(JsonLinesConfig::new(path));
        assert_eq!(src.fetch_all().await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn an_empty_file_yields_no_pages() {
        let dir = tempfile::tempdir().unwrap();
        let path = write(dir.path(), "out.jsonl", "");
        let src = JsonLinesSource::new(JsonLinesConfig::new(path));
        assert!(src.fetch_all().await.unwrap().is_empty());
        let ctx = HashMap::new();
        let mut pages = src.stream_pages(&ctx, 0);
        assert!(pages.next().await.is_none(), "no empty trailing page");
    }

    #[tokio::test]
    async fn a_malformed_line_names_the_file_and_line() {
        let dir = tempfile::tempdir().unwrap();
        let path = write(dir.path(), "out.jsonl", "{\"a\":1}\n{\"a\":\n");
        let src = JsonLinesSource::new(JsonLinesConfig::new(path));
        let err = src.fetch_all().await.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("line 2"), "{msg}");
        assert!(msg.contains("out.jsonl"), "{msg}");
    }

    #[tokio::test]
    async fn a_line_longer_than_the_cap_fails_instead_of_buffering() {
        let dir = tempfile::tempdir().unwrap();
        let long = format!("{{\"s\":\"{}\"}}", "x".repeat(200));
        let path = write(dir.path(), "out.jsonl", &format!("{{\"a\":1}}\n{long}"));
        let src = JsonLinesSource::new(JsonLinesConfig::new(path).max_line_bytes(64));
        let msg = src.fetch_all().await.unwrap_err().to_string();
        assert!(
            msg.contains("line 2") && msg.contains("longer than 64 bytes"),
            "{msg}"
        );
        // A last line with no newline, and CRLF endings, read normally.
        let path = write(dir.path(), "ok.jsonl", "{\"a\":1}\r\n{\"a\":2}");
        let src = JsonLinesSource::new(JsonLinesConfig::new(path).max_line_bytes(64));
        assert_eq!(src.fetch_all().await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn a_missing_file_is_a_typed_source_error() {
        let src = JsonLinesSource::new(JsonLinesConfig::new("/definitely/not/here.jsonl"));
        let err = src.fetch_all().await.unwrap_err();
        assert!(matches!(err, FaucetError::Source(_)), "{err:?}");
        assert!(err.to_string().contains("failed to open"), "{err}");
    }

    #[tokio::test]
    async fn the_limit_stops_the_read_before_eof() {
        // The whole point: a 3-record cap on a 1000-line file must not parse
        // 1000 lines. A malformed line *after* the cap proves the read stopped.
        let dir = tempfile::tempdir().unwrap();
        let mut body = String::new();
        for i in 0..1000 {
            body.push_str(&format!("{{\"i\":{i}}}\n"));
        }
        body.push_str("this is not json\n");
        let path = write(dir.path(), "big.jsonl", &body);
        let src = JsonLinesSource::new(JsonLinesConfig::new(path).limit(3));
        let rows = src.fetch_all().await.unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0]["i"], 0);
    }

    #[tokio::test]
    async fn a_zero_limit_means_no_limit_not_no_rows() {
        // `limit: 0` is how the preview engine asks for a whole dataset. Reading
        // it as "zero records" would answer "the file is empty" about a file that
        // is not.
        let dir = tempfile::tempdir().unwrap();
        let path = write(dir.path(), "out.jsonl", "{\"a\":1}\n{\"a\":2}\n");
        let src = JsonLinesSource::new(JsonLinesConfig::new(path).limit(0));
        assert_eq!(src.fetch_all().await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn batch_size_controls_the_page_cadence() {
        let dir = tempfile::tempdir().unwrap();
        let path = write(
            dir.path(),
            "out.jsonl",
            "{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n{\"a\":4}\n{\"a\":5}\n",
        );
        let src = JsonLinesSource::new(JsonLinesConfig::new(path).batch_size(2));
        let ctx = HashMap::new();
        let mut pages = src.stream_pages(&ctx, 999);
        let mut sizes = Vec::new();
        while let Some(p) = pages.next().await {
            sizes.push(p.unwrap().records.len());
        }
        assert_eq!(sizes, vec![2, 2, 1], "trailing partial page is emitted");
    }

    #[tokio::test]
    async fn batch_size_zero_drains_into_one_page() {
        let dir = tempfile::tempdir().unwrap();
        let path = write(dir.path(), "out.jsonl", "{\"a\":1}\n{\"a\":2}\n");
        let src = JsonLinesSource::new(JsonLinesConfig::new(path).batch_size(0));
        let ctx = HashMap::new();
        let mut pages = src.stream_pages(&ctx, 1);
        let first = pages.next().await.unwrap().unwrap();
        assert_eq!(first.records.len(), 2);
        assert!(pages.next().await.is_none());
    }

    #[tokio::test]
    async fn non_object_lines_are_read_as_values() {
        // The sink writes whatever the pipeline produced; a scalar or array
        // record is unusual but legal NDJSON and must not error.
        let dir = tempfile::tempdir().unwrap();
        let path = write(dir.path(), "out.jsonl", "1\n[2,3]\n\"x\"\n");
        let src = JsonLinesSource::new(JsonLinesConfig::new(path));
        let rows = src.fetch_all().await.unwrap();
        assert_eq!(
            rows,
            vec![Value::from(1), serde_json::json!([2, 3]), Value::from("x")]
        );
    }

    #[test]
    fn config_round_trips_through_json() {
        let cfg: JsonLinesConfig =
            serde_json::from_value(serde_json::json!({"path": "/tmp/a.jsonl", "limit": 5}))
                .unwrap();
        assert_eq!(cfg.path, "/tmp/a.jsonl");
        assert_eq!(cfg.limit, Some(5));
        assert_eq!(cfg.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }
}
