//! Azure Blob source stream executor.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use faucet_common_azure::build_store;
use faucet_core::{FaucetError, Stream, StreamPage};
use futures::stream::{self, StreamExt, TryStreamExt};
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use serde_json::Value;
use tokio::io::AsyncBufReadExt;

use crate::config::{AzureBlobSourceConfig, AzureFileFormat};

/// An Azure Blob source that lists and reads objects from a container.
pub struct AzureBlobSource {
    config: AzureBlobSourceConfig,
    store: Arc<dyn ObjectStore>,
}

impl AzureBlobSource {
    /// Construct the source, building the object store eagerly so it is reused
    /// across calls.
    pub async fn new(config: AzureBlobSourceConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        let store = build_store(&config.connection)?;
        Ok(Self { config, store })
    }

    /// List object names under the configured (or override) prefix, capped at
    /// `max_objects` when set. When `object_keys` is configured, listing is
    /// skipped and those keys are used directly.
    async fn list_object_names(
        &self,
        prefix_override: Option<&str>,
    ) -> Result<Vec<String>, FaucetError> {
        if let Some(keys) = &self.config.object_keys {
            return Ok(cap_keys(keys.clone(), self.config.max_objects));
        }

        let effective_prefix = prefix_override.or(self.config.prefix.as_deref());
        let prefix_path = effective_prefix
            .filter(|p| !p.is_empty())
            .map(ObjectPath::from);

        let mut listing = self.store.list(prefix_path.as_ref());
        let mut names: Vec<String> = Vec::new();
        while let Some(item) = listing.next().await {
            let meta = item.map_err(|e| {
                FaucetError::Source(format!(
                    "azure list error for container '{}': {e}",
                    self.config.container()
                ))
            })?;
            let name = meta.location.to_string();
            if name.is_empty() {
                continue;
            }
            names.push(name);
            if let Some(max) = self.config.max_objects
                && names.len() >= max
            {
                break;
            }
        }
        Ok(names)
    }

    /// Read the full body of a single object into a UTF-8 `String`.
    async fn read_object_text(&self, key: &str) -> Result<String, FaucetError> {
        use tokio::io::AsyncReadExt as _;
        let mut reader = self.open_object_reader(key).await?;
        let mut text = String::new();
        reader.read_to_string(&mut text).await.map_err(|e| {
            FaucetError::Source(format!(
                "azure read/decode error for key '{key}' (not valid UTF-8?): {e}"
            ))
        })?;
        Ok(text)
    }

    /// Open an object as an `AsyncBufRead` over its (optionally decompressed)
    /// body so callers can decode line-by-line without buffering the whole
    /// object.
    async fn open_object_reader(
        &self,
        key: &str,
    ) -> Result<Pin<Box<dyn tokio::io::AsyncBufRead + Send + Unpin>>, FaucetError> {
        let path = ObjectPath::from(key);
        let result = self.store.get(&path).await.map_err(|e| {
            FaucetError::Source(format!(
                "azure get error for container '{}' key '{key}': {e}",
                self.config.container()
            ))
        })?;

        // Read the metadata BEFORE consuming the stream (which moves `result`),
        // so a cleanly-truncated transfer is rejected rather than silently
        // parsed as a complete object (#161).
        let checks = length_checks(
            result.meta.size,
            content_encoding(&result.attributes),
            self.config.verify_length,
        );
        if self.config.verify_length && checks.is_empty() {
            tracing::debug!(
                key = %key,
                "azure object length verification skipped (transcoded Content-Encoding)"
            );
        }

        let byte_stream = result
            .into_stream()
            .map_err(|e| std::io::Error::other(e.to_string()));
        let reader = tokio_util::io::StreamReader::new(byte_stream);
        // Wrap the RAW byte stream in the verifier first so the byte count
        // covers the stored bytes (below any client-side decompression).
        let verified = faucet_core::VerifyingReader::new(reader, checks);
        let buffered = tokio::io::BufReader::new(verified);
        #[cfg(feature = "compression")]
        {
            let codec = self.config.compression.resolve(key);
            faucet_core::compression::warn_mismatch(key, codec);
            Ok(faucet_core::compression::wrap_async_reader(buffered, codec))
        }
        #[cfg(not(feature = "compression"))]
        {
            Ok(Box::pin(buffered))
        }
    }

    /// Parse file content into records based on the configured file format.
    fn parse_content(&self, key: &str, text: &str) -> Result<Vec<Value>, FaucetError> {
        parse_file_content(&self.config.file_format, key, text)
    }
}

/// Parse object content into records for a given format. Free function (vs. an
/// `AzureBlobSource` method) so it is unit-testable without an Azure client —
/// the parsing logic is pure.
pub(crate) fn parse_file_content(
    format: &AzureFileFormat,
    key: &str,
    text: &str,
) -> Result<Vec<Value>, FaucetError> {
    match format {
        AzureFileFormat::JsonLines => {
            let mut records = Vec::new();
            for (line_num, line) in text.lines().enumerate() {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                let value: Value = serde_json::from_str(trimmed).map_err(|e| {
                    FaucetError::Source(format!(
                        "azure JSON parse error in '{key}' at line {}: {e}",
                        line_num + 1
                    ))
                })?;
                records.push(value);
            }
            Ok(records)
        }
        AzureFileFormat::JsonArray => {
            let value: Value = serde_json::from_str(text).map_err(|e| {
                FaucetError::Source(format!("azure JSON parse error in '{key}': {e}"))
            })?;
            match value {
                Value::Array(arr) => Ok(arr),
                other => Err(FaucetError::Source(format!(
                    "azure expected JSON array in '{key}', got {}",
                    value_type_name(&other)
                ))),
            }
        }
        AzureFileFormat::RawText => Ok(vec![serde_json::json!({
            "key": key,
            "content": text,
        })]),
    }
}

/// The [`IntegrityCheck`](faucet_core::IntegrityCheck) set for one blob read
/// (#161). Pure so the decision is unit-testable without an Azure endpoint.
///
/// Empty when verification is off, or when the blob is served with a non-empty
/// `Content-Encoding` — a store may decompressively transcode on read, so the
/// received byte count would not match the stored `size`. Azure Blob exposes no
/// body checksum through `object_store`, so the length check is the only one
/// available here (`verify_checksum` is refused at config load).
fn length_checks(
    size: u64,
    content_encoding: Option<&str>,
    verify_length: bool,
) -> Vec<Box<dyn faucet_core::IntegrityCheck>> {
    if verify_length && content_encoding.is_none_or(str::is_empty) {
        vec![Box::new(faucet_core::LengthCheck::new(size))]
    } else {
        Vec::new()
    }
}

/// The `Content-Encoding` an object-store `get` reported, if any.
fn content_encoding(attributes: &object_store::Attributes) -> Option<&str> {
    attributes
        .get(&object_store::Attribute::ContentEncoding)
        .map(AsRef::as_ref)
}

/// Truncate an explicit object-key list to the `max_objects` cap. `None` leaves
/// the list untouched.
fn cap_keys(mut keys: Vec<String>, max: Option<usize>) -> Vec<String> {
    if let Some(n) = max {
        keys.truncate(n);
    }
    keys
}

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

#[async_trait]
impl faucet_core::Source for AzureBlobSource {
    async fn fetch_with_context(
        &self,
        context: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        let substituted_prefix: Option<String> = if !context.is_empty() {
            self.config
                .prefix
                .as_ref()
                .map(|p| faucet_core::util::substitute_context(p, context))
        } else {
            None
        };

        let keys = self
            .list_object_names(substituted_prefix.as_deref())
            .await?;
        tracing::info!(
            container = %self.config.container(),
            objects = keys.len(),
            "Listed Azure objects",
        );

        let concurrency = self.config.concurrency.max(1);
        let results: Vec<Vec<Value>> = stream::iter(keys)
            .map(|key| async move {
                let text = self.read_object_text(&key).await?;
                let records = self.parse_content(&key, &text)?;
                tracing::debug!(key = %key, records = records.len(), "Read Azure object");
                Ok::<Vec<Value>, FaucetError>(records)
            })
            .buffer_unordered(concurrency)
            .try_collect()
            .await?;

        let all_records: Vec<Value> = results.into_iter().flatten().collect();
        tracing::info!(total_records = all_records.len(), "Azure fetch complete");
        Ok(all_records)
    }

    /// Stream records from listed Azure objects without buffering the full
    /// scan. Mirrors the S3/GCS object sources — see those for the per-format
    /// reasoning. `batch_size = 0` emits one page per object.
    fn stream_pages<'a>(
        &'a self,
        context: &'a HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        let batch_size = self.config.batch_size;

        Box::pin(async_stream::try_stream! {
            let substituted_prefix: Option<String> = if !context.is_empty() {
                self.config
                    .prefix
                    .as_ref()
                    .map(|p| faucet_core::util::substitute_context(p, context))
            } else {
                None
            };

            let keys = self.list_object_names(substituted_prefix.as_deref()).await?;
            tracing::info!(
                container = %self.config.container(),
                objects = keys.len(),
                "Listed Azure objects (stream)",
            );

            let chunk = if batch_size == 0 { usize::MAX } else { batch_size };
            let initial_capacity = if batch_size == 0 { 1024 } else { batch_size };
            let mut buffer: Vec<Value> = Vec::with_capacity(initial_capacity);
            let mut total = 0usize;

            for key in &keys {
                match self.config.file_format {
                    AzureFileFormat::JsonLines => {
                        let reader = self.open_object_reader(key).await?;
                        let mut lines = reader.lines();
                        let mut line_num: usize = 0;
                        while let Some(line) = lines
                            .next_line()
                            .await
                            .map_err(|e| FaucetError::Source(format!(
                                "azure read body error for key '{key}': {e}"
                            )))?
                        {
                            line_num += 1;
                            let trimmed = line.trim();
                            if trimmed.is_empty() { continue; }
                            let value: Value = serde_json::from_str(trimmed).map_err(|e| {
                                FaucetError::Source(format!(
                                    "azure JSON parse error in '{key}' at line {line_num}: {e}",
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
                    AzureFileFormat::RawText => {
                        let text = self.read_object_text(key).await?;
                        let record = serde_json::json!({ "key": key, "content": text });
                        buffer.push(record);
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
                    AzureFileFormat::JsonArray => {
                        let text = self.read_object_text(key).await?;
                        let value: Value = serde_json::from_str(&text).map_err(|e| {
                            FaucetError::Source(format!("azure JSON parse error in '{key}': {e}"))
                        })?;
                        let array = match value {
                            Value::Array(arr) => arr,
                            other => Err(FaucetError::Source(format!(
                                "azure expected JSON array in '{key}', got {}",
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

            tracing::info!(
                total_records = total,
                batch_size,
                objects = keys.len(),
                "Azure source stream complete",
            );
        })
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(AzureBlobSourceConfig))
            .expect("schema serialization")
    }

    fn connector_name(&self) -> &'static str {
        "azure-blob"
    }

    fn dataset_uri(&self) -> String {
        match &self.config.prefix {
            Some(p) => format!("az://{}/{}", self.config.container(), p),
            None => format!("az://{}", self.config.container()),
        }
    }

    /// Preflight probe: confirm the container is reachable and the credentials
    /// work via a non-mutating listing capped at a single item. Reads no object
    /// bodies.
    async fn check(
        &self,
        ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::{CheckReport, Probe};

        let started = std::time::Instant::now();
        let probe = match tokio::time::timeout(ctx.timeout, async {
            let mut listing = self.store.list(None);
            listing.next().await
        })
        .await
        {
            // Reachable — an empty container (None) is still a pass.
            Ok(None) | Ok(Some(Ok(_))) => Probe::pass("auth", started.elapsed()),
            Ok(Some(Err(e))) => Probe::fail_hint(
                "auth",
                started.elapsed(),
                e.to_string(),
                "check account, container, credentials, and network",
            ),
            Err(_) => Probe::fail("network", started.elapsed(), "timed out"),
        };
        Ok(CheckReport::single(probe))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::Source as _;
    use serde_json::json;

    #[test]
    fn value_type_name_covers_all_json_variants() {
        assert_eq!(value_type_name(&Value::Null), "null");
        assert_eq!(value_type_name(&json!(true)), "boolean");
        assert_eq!(value_type_name(&json!(7)), "number");
        assert_eq!(value_type_name(&json!("s")), "string");
        assert_eq!(value_type_name(&json!([1, 2])), "array");
        assert_eq!(value_type_name(&json!({"k": 1})), "object");
    }

    #[test]
    fn parse_json_lines() {
        let r = parse_file_content(&AzureFileFormat::JsonLines, "t", "{\"id\":1}\n{\"id\":2}\n")
            .unwrap();
        assert_eq!(r.len(), 2);
        assert_eq!(r[0]["id"], 1);
    }

    #[test]
    fn parse_json_lines_skips_blanks() {
        let r = parse_file_content(
            &AzureFileFormat::JsonLines,
            "t",
            "{\"id\":1}\n\n{\"id\":2}\n\n",
        )
        .unwrap();
        assert_eq!(r.len(), 2);
    }

    #[test]
    fn parse_json_lines_reports_line_number() {
        let err = parse_file_content(&AzureFileFormat::JsonLines, "t", "{\"id\":1}\nbad-line\n")
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("line 2"), "unexpected: {msg}");
    }

    #[test]
    fn parse_json_array() {
        let r = parse_file_content(
            &AzureFileFormat::JsonArray,
            "t.json",
            "[{\"id\":1},{\"id\":2}]",
        )
        .unwrap();
        assert_eq!(r.len(), 2);
    }

    #[test]
    fn parse_json_array_rejects_non_array() {
        let err =
            parse_file_content(&AzureFileFormat::JsonArray, "t.json", "{\"id\":1}").unwrap_err();
        assert!(err.to_string().contains("expected JSON array"));
    }

    #[test]
    fn parse_json_array_rejects_malformed_json() {
        let err =
            parse_file_content(&AzureFileFormat::JsonArray, "t.json", "[not json").unwrap_err();
        assert!(matches!(err, FaucetError::Source(_)));
    }

    #[test]
    fn parse_raw_text_yields_single_record() {
        let r = parse_file_content(&AzureFileFormat::RawText, "p/f.txt", "hello").unwrap();
        assert_eq!(r, vec![json!({"key": "p/f.txt", "content": "hello"})]);
    }

    #[test]
    fn cap_keys_truncates_explicit_list_to_max_objects() {
        let keys = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        assert_eq!(
            cap_keys(keys, Some(2)),
            vec!["a".to_string(), "b".to_string()]
        );
    }

    #[test]
    fn cap_keys_passes_through_when_no_max() {
        let keys = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        assert_eq!(cap_keys(keys.clone(), None), keys);
    }

    #[test]
    fn cap_keys_noop_when_max_exceeds_len() {
        let keys = vec!["a".to_string(), "b".to_string()];
        assert_eq!(cap_keys(keys.clone(), Some(10)), keys);
    }

    // dataset_uri logic mirrors the built source without needing an Azure
    // client (construction builds the object store).
    #[test]
    fn dataset_uri_no_prefix_logic() {
        let config = AzureBlobSourceConfig::new("my-container");
        let uri = match &config.prefix {
            Some(p) => format!("az://{}/{}", config.container(), p),
            None => format!("az://{}", config.container()),
        };
        assert_eq!(uri, "az://my-container");
    }

    #[test]
    fn dataset_uri_with_prefix_logic() {
        let config = AzureBlobSourceConfig::new("my-container").prefix("data/2026/");
        let uri = match &config.prefix {
            Some(p) => format!("az://{}/{}", config.container(), p),
            None => format!("az://{}", config.container()),
        };
        assert_eq!(uri, "az://my-container/data/2026/");
    }

    // ---- read-integrity verification (#161) ----

    /// Read `body` through the verifier with the checks `length_checks` picks
    /// for `size`/`content_encoding`, returning whether the read succeeded.
    async fn reads_ok(size: u64, content_encoding: Option<&str>, body: &[u8]) -> bool {
        use tokio::io::AsyncReadExt as _;
        let checks = length_checks(size, content_encoding, true);
        let mut reader = faucet_core::VerifyingReader::new(body, checks);
        let mut out = Vec::new();
        reader.read_to_end(&mut out).await.is_ok()
    }

    #[tokio::test]
    async fn length_check_accepts_a_complete_body() {
        assert!(reads_ok(5, None, b"hello").await);
    }

    #[tokio::test]
    async fn length_check_rejects_a_truncated_body() {
        assert!(
            !reads_ok(10, None, b"hello").await,
            "a cleanly-truncated transfer must fail, not parse as complete"
        );
    }

    #[tokio::test]
    async fn length_check_rejects_an_overlong_body() {
        assert!(!reads_ok(3, None, b"hello").await);
    }

    #[tokio::test]
    async fn length_check_skipped_for_a_transcoded_blob() {
        // A non-empty Content-Encoding means the received bytes need not match
        // the stored size, so no check is installed and the read passes.
        assert!(reads_ok(999, Some("gzip"), b"hello").await);
    }

    #[test]
    fn length_checks_empty_when_disabled() {
        assert!(length_checks(5, None, false).is_empty());
    }

    #[test]
    fn length_checks_installed_for_empty_content_encoding() {
        // An explicitly empty header is equivalent to absent.
        assert_eq!(length_checks(5, Some(""), true).len(), 1);
        assert_eq!(length_checks(5, None, true).len(), 1);
    }

    #[test]
    fn content_encoding_reads_the_attribute() {
        let mut attrs = object_store::Attributes::new();
        assert_eq!(content_encoding(&attrs), None);
        attrs.insert(object_store::Attribute::ContentEncoding, "gzip".into());
        assert_eq!(content_encoding(&attrs), Some("gzip"));
    }

    #[tokio::test]
    async fn new_rejects_verify_checksum() {
        // Azure exposes no body checksum through object_store, so the switch is
        // refused at load time rather than accepted and silently ignored.
        let mut config = AzureBlobSourceConfig::new("c");
        config.verify_checksum = true;
        match AzureBlobSource::new(config).await {
            Err(FaucetError::Config(m)) => {
                assert!(m.contains("verify_checksum"), "got: {m}")
            }
            Ok(_) => panic!("expected a verify_checksum Config error, got Ok(source)"),
            Err(e) => panic!("expected a verify_checksum Config error, got {e:?}"),
        }
    }

    #[tokio::test]
    async fn new_rejects_empty_container() {
        match AzureBlobSource::new(AzureBlobSourceConfig::new("   ")).await {
            Err(FaucetError::Config(m)) => assert!(m.contains("container"), "got: {m}"),
            Ok(_) => panic!("expected a container Config error, got Ok(source)"),
            Err(e) => panic!("expected a container Config error, got {e:?}"),
        }
    }

    #[tokio::test]
    async fn new_rejects_out_of_range_batch_size() {
        let config =
            AzureBlobSourceConfig::new("c").with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        match AzureBlobSource::new(config).await {
            Err(FaucetError::Config(m)) => assert!(m.contains("batch_size"), "got: {m}"),
            Ok(_) => panic!("expected a batch_size Config error, got Ok(source)"),
            Err(e) => panic!("expected a batch_size Config error, got {e:?}"),
        }
    }

    #[tokio::test]
    async fn new_builds_lazily_with_emulator() {
        // The object-store builder is lazy — no I/O — so a well-formed emulator
        // config constructs a source without a reachable backend.
        let config = AzureBlobSourceConfig::new("c")
            .use_emulator(true)
            .allow_http(true);
        let source = AzureBlobSource::new(config).await.unwrap();
        assert_eq!(source.connector_name(), "azure-blob");
        assert_eq!(source.dataset_uri(), "az://c");
    }
}
