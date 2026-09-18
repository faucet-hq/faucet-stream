//! SFTP source stream executor.
//!
//! The one place that performs SFTP I/O. Construction is lazy — [`SftpSource::new`]
//! only stores (and validates) the config; the SSH transport is opened on the
//! first `fetch_*` / `stream_pages` call, so an unreachable host surfaces as a
//! typed error on first poll rather than at construction time.

use crate::config::{SftpFormat, SftpSourceConfig, glob_match};
use async_trait::async_trait;
use faucet_common_sftp::{SftpFile, SftpSession, connect};
use faucet_core::{FaucetError, Stream, StreamPage};
use futures::stream::StreamExt;
use serde_json::Value;
use std::collections::HashMap;
use std::pin::Pin;
use tokio::io::AsyncBufReadExt;

/// One remote file's payload, fetched in the shape its `format` decodes from.
///
/// Prefetching these with a bounded look-ahead is what makes `concurrency`
/// real (#619). The bound differs by format and that difference is the point:
/// `Jsonl` overlaps only the `open` round-trip — the handle is read
/// line-by-line afterwards, so memory stays O(batch) — while the whole-file
/// formats hold at most `concurrency` bodies at once.
enum Fetched {
    Lines(SftpFile),
    RawText(String),
    JsonArray(String),
}

/// An SFTP source that lists and reads remote files.
pub struct SftpSource {
    config: SftpSourceConfig,
}

impl SftpSource {
    /// Create a new SFTP source. Lazy: performs no I/O and never connects here.
    /// The batch size is validated up front so a bad config fails fast.
    pub fn new(config: SftpSourceConfig) -> Result<Self, FaucetError> {
        faucet_core::validate_batch_size(config.batch_size)?;
        Ok(Self { config })
    }

    /// Resolve the effective remote path, substituting parent-context tokens
    /// when a non-empty context is supplied (parent/child matrix runs).
    fn effective_path(&self, context: &HashMap<String, Value>) -> String {
        if context.is_empty() {
            self.config.path.clone()
        } else {
            faucet_core::util::substitute_context(&self.config.path, context)
        }
    }

    /// List the files to read for `path`: the directory's regular files
    /// (glob-filtered, sorted for a deterministic order) when `path` is a
    /// directory, or `[path]` when it is a single file.
    async fn resolve_files(
        &self,
        sftp: &SftpSession,
        path: &str,
    ) -> Result<Vec<String>, FaucetError> {
        let meta = sftp
            .metadata(path)
            .await
            .map_err(|e| FaucetError::Source(format!("SFTP stat '{path}' failed: {e}")))?;

        if !meta.file_type().is_dir() {
            return Ok(vec![path.to_string()]);
        }

        let entries = sftp
            .read_dir(path)
            .await
            .map_err(|e| FaucetError::Source(format!("SFTP read_dir '{path}' failed: {e}")))?;

        let mut files = Vec::new();
        for entry in entries {
            if !entry.file_type().is_file() {
                continue;
            }
            let name = entry.file_name();
            if let Some(pattern) = &self.config.glob
                && !glob_match(pattern, &name)
            {
                continue;
            }
            files.push(entry.path());
        }
        files.sort();
        Ok(files)
    }

    /// Read a whole remote file into a UTF-8 `String` (used by `raw_text` and
    /// `json_array`, which cannot be parsed incrementally).
    async fn read_file_text(sftp: &SftpSession, path: &str) -> Result<String, FaucetError> {
        let bytes = sftp
            .read(path)
            .await
            .map_err(|e| FaucetError::Source(format!("SFTP read '{path}' failed: {e}")))?;
        String::from_utf8(bytes)
            .map_err(|e| FaucetError::Source(format!("SFTP file '{path}' is not valid UTF-8: {e}")))
    }

    /// Fetch one remote file in the shape its configured `format` needs.
    async fn fetch(
        sftp: &SftpSession,
        path: &str,
        format: SftpFormat,
    ) -> Result<Fetched, FaucetError> {
        Ok(match format {
            SftpFormat::Jsonl => Fetched::Lines(
                sftp.open(path)
                    .await
                    .map_err(|e| FaucetError::Source(format!("SFTP open '{path}' failed: {e}")))?,
            ),
            SftpFormat::RawText => Fetched::RawText(Self::read_file_text(sftp, path).await?),
            SftpFormat::JsonArray => Fetched::JsonArray(Self::read_file_text(sftp, path).await?),
        })
    }
}

#[async_trait]
impl faucet_core::Source for SftpSource {
    async fn fetch_with_context(
        &self,
        context: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        use futures::StreamExt;
        let mut all = Vec::new();
        let mut pages = self.stream_pages(context, self.config.batch_size);
        while let Some(page) = pages.next().await {
            all.extend(page?.records);
        }
        Ok(all)
    }

    /// Stream records from the resolved remote files without buffering the full
    /// scan. Each emitted [`StreamPage`] holds up to
    /// [`SftpSourceConfig::batch_size`] records.
    ///
    /// - `jsonl` / `raw_text`: files are decoded incrementally (line-by-line
    ///   for JSONL; one record per file for raw text) so client memory is
    ///   bounded regardless of file size. Multi-file scans flatten — a page
    ///   may carry records from more than one file.
    /// - `json_array`: each file is buffered fully, then its records chunked.
    ///
    /// `batch_size = 0` emits one page per file. Every page carries
    /// `bookmark: None` — the SFTP source is not resumable.
    fn stream_pages<'a>(
        &'a self,
        context: &'a HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        let batch_size = self.config.batch_size;

        Box::pin(async_stream::try_stream! {
            let sftp = connect(&self.config.connection).await?;
            let path = self.effective_path(context);
            let files = self.resolve_files(&sftp, &path).await?;
            tracing::info!(path = %path, files = files.len(), "SFTP source listed files");

            let chunk = if batch_size == 0 { usize::MAX } else { batch_size };
            let initial_capacity = if batch_size == 0 { 1024 } else { batch_size };
            let mut buffer: Vec<Value> = Vec::with_capacity(initial_capacity);
            let mut total = 0usize;

            // Overlap the file reads (#619). `buffered` keeps listing order,
            // so a file that fails surfaces at exactly the point it would have
            // serially — concurrency changes throughput, not semantics. The
            // path is moved into each future rather than borrowed: a closure
            // returning a borrow-capturing async block is not higher-ranked
            // enough for `buffered`.
            let format = self.config.format;
            let concurrency = self.config.concurrency.max(1);
            let mut fetched = futures::stream::iter(files.iter().cloned())
                .map(|file| {
                    let sftp = &sftp;
                    async move {
                        let payload = Self::fetch(sftp, &file, format).await;
                        (file, payload)
                    }
                })
                .buffered(concurrency);

            while let Some((file, payload)) = fetched.next().await {
                let file = &file;
                match payload? {
                    Fetched::Lines(handle) => {
                        let reader = tokio::io::BufReader::new(handle);
                        let mut lines = reader.lines();
                        let mut line_num = 0usize;
                        while let Some(line) = lines.next_line().await.map_err(|e| {
                            FaucetError::Source(format!("SFTP read '{file}' failed: {e}"))
                        })? {
                            line_num += 1;
                            let trimmed = line.trim();
                            if trimmed.is_empty() {
                                continue;
                            }
                            let value: Value = serde_json::from_str(trimmed).map_err(|e| {
                                FaucetError::Source(format!(
                                    "SFTP JSON parse error in '{file}' at line {line_num}: {e}"
                                ))
                            })?;
                            buffer.push(value);
                            if batch_size != 0 && buffer.len() >= chunk {
                                let page = std::mem::replace(
                                    &mut buffer,
                                    Vec::with_capacity(initial_capacity),
                                );
                                total += page.len();
                                yield StreamPage { records: page, bookmark: None };
                            }
                        }
                        if batch_size == 0 && !buffer.is_empty() {
                            let page = std::mem::take(&mut buffer);
                            total += page.len();
                            yield StreamPage { records: page, bookmark: None };
                        }
                    }
                    Fetched::RawText(text) => {
                        buffer.push(serde_json::json!({ "path": file, "content": text }));
                        if batch_size == 0 {
                            let page = std::mem::take(&mut buffer);
                            total += page.len();
                            yield StreamPage { records: page, bookmark: None };
                        } else if buffer.len() >= chunk {
                            let page = std::mem::replace(
                                &mut buffer,
                                Vec::with_capacity(initial_capacity),
                            );
                            total += page.len();
                            yield StreamPage { records: page, bookmark: None };
                        }
                    }
                    Fetched::JsonArray(text) => {
                        let value: Value = serde_json::from_str(&text).map_err(|e| {
                            FaucetError::Source(format!("SFTP JSON parse error in '{file}': {e}"))
                        })?;
                        let array = match value {
                            Value::Array(arr) => arr,
                            other => Err(FaucetError::Source(format!(
                                "SFTP expected JSON array in '{file}', got {}",
                                value_type_name(&other)
                            )))?,
                        };
                        if batch_size == 0 {
                            if !buffer.is_empty() {
                                let page = std::mem::take(&mut buffer);
                                total += page.len();
                                yield StreamPage { records: page, bookmark: None };
                            }
                            total += array.len();
                            yield StreamPage { records: array, bookmark: None };
                        } else {
                            for record in array {
                                buffer.push(record);
                                if buffer.len() >= chunk {
                                    let page = std::mem::replace(
                                        &mut buffer,
                                        Vec::with_capacity(initial_capacity),
                                    );
                                    total += page.len();
                                    yield StreamPage { records: page, bookmark: None };
                                }
                            }
                        }
                    }
                }
            }

            if !buffer.is_empty() {
                let page = std::mem::take(&mut buffer);
                total += page.len();
                yield StreamPage { records: page, bookmark: None };
            }

            tracing::info!(total_records = total, files = files.len(), "SFTP source stream complete");
        })
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(SftpSourceConfig))
            .expect("schema serialization")
    }

    fn connector_name(&self) -> &'static str {
        "sftp"
    }

    fn dataset_uri(&self) -> String {
        format!(
            "sftp://{}:{}/{}",
            self.config.connection.host,
            self.config.connection.port,
            self.config.path.trim_start_matches('/')
        )
    }
}

/// Return a human-readable name for a JSON value type.
fn value_type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_common_sftp::SftpConnectionConfig;
    use faucet_core::Source;

    fn cfg() -> SftpSourceConfig {
        SftpSourceConfig::new(SftpConnectionConfig::with_password("h", "u", "p"), "/data")
    }

    #[test]
    fn new_is_lazy_and_validates_batch_size() {
        // Valid config constructs with no I/O.
        assert!(SftpSource::new(cfg()).is_ok());
        // Out-of-range batch size is rejected up front.
        let bad = cfg().with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        assert!(matches!(SftpSource::new(bad), Err(FaucetError::Config(_))));
    }

    #[test]
    fn connector_name_is_sftp() {
        let src = SftpSource::new(cfg()).unwrap();
        assert_eq!(src.connector_name(), "sftp");
    }

    #[test]
    fn dataset_uri_has_no_credentials() {
        let src = SftpSource::new(cfg()).unwrap();
        let uri = src.dataset_uri();
        assert_eq!(uri, "sftp://h:22/data");
        assert!(!uri.contains('p') || !uri.contains("password"));
    }

    #[test]
    fn config_schema_is_valid_object() {
        let src = SftpSource::new(cfg()).unwrap();
        let schema = src.config_schema();
        assert!(schema.is_object());
        assert!(schema.get("properties").is_some());
    }
}
