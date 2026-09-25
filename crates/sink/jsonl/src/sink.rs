//! JSON Lines file sink.

use crate::config::JsonlSinkConfig;
use async_trait::async_trait;
use faucet_core::FaucetError;
use serde_json::Value;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

/// A sink that writes JSON records to a file in JSON Lines format.
///
/// Each record is written as a single line of JSON followed by a newline.
/// The file is opened lazily on the first `write_batch` call.
///
/// With the `compression` feature, the writer transparently wraps the file
/// with a gzip / zstd encoder based on the `compression` config field.
/// [`Sink::flush`](faucet_core::Sink::flush) finalises the encoder (writes the trailer) and clears the
/// writer slot — a subsequent `write_batch` reopens the file in append mode
/// (independent of `config.append`) and starts a fresh encoder, producing a
/// multi-member compressed file that decoders read back correctly. This makes
/// the per-page `flush` the pipeline emits for bookmarked pages safe for CDC
/// sources — every transaction appends rather than truncates.
pub struct JsonlSink {
    config: JsonlSinkConfig,
    /// Compiled at-rest encryption (#207), initialized on first use so
    /// `new()` stays infallible. `Err` results surface as typed sink errors.
    #[cfg(feature = "encryption")]
    encryption: tokio::sync::OnceCell<Option<faucet_core::CompiledEncryption>>,
    /// Mutex-protected writer for thread-safe concurrent writes.
    writer: Mutex<Option<std::pin::Pin<Box<dyn tokio::io::AsyncWrite + Send + Unpin>>>>,
    /// Tracks whether `ensure_open` has opened the file at least once.
    /// On re-opens (after `flush()` clears the writer), we always use
    /// append mode regardless of `config.append` so the new gzip / zstd
    /// member appends instead of truncating the file. Without this, the
    /// pipeline's per-bookmark flush would silently lose data when
    /// `config.append = false` (the default).
    opened_once: std::sync::atomic::AtomicBool,
    /// The file this sink opened, for the local-output retention GC (#587).
    /// Recorded at the first open — before the file is created — so a path that
    /// already held someone else's data is flagged `pre_existing` and never
    /// collected. See `faucet_core::local_outputs`.
    outputs: faucet_core::LocalOutputLog,
}

impl JsonlSink {
    /// Create a new JSON Lines sink. The file is opened on first write.
    pub fn new(config: JsonlSinkConfig) -> Self {
        Self {
            config,
            #[cfg(feature = "encryption")]
            encryption: tokio::sync::OnceCell::new(),
            writer: Mutex::new(None),
            opened_once: std::sync::atomic::AtomicBool::new(false),
            outputs: faucet_core::LocalOutputLog::new(),
        }
    }

    /// Compile (once) the configured at-rest encryption, validating the
    /// compression conflict. `Ok(None)` when no `encryption:` block is set.
    #[cfg(feature = "encryption")]
    async fn compiled_encryption(
        &self,
    ) -> Result<&Option<faucet_core::CompiledEncryption>, FaucetError> {
        self.encryption
            .get_or_try_init(|| async {
                let Some(spec) = &self.config.encryption else {
                    return Ok(None);
                };
                #[cfg(feature = "compression")]
                {
                    let path_str = self.config.path.to_string_lossy();
                    if self.config.compression.resolve(&path_str) != faucet_core::Compression::None
                    {
                        return Err(FaucetError::Config(
                            "jsonl sink: `encryption` and `compression` are mutually \
                             exclusive — per-line sealed records cannot form a valid \
                             gzip/zstd stream"
                                .into(),
                        ));
                    }
                }
                faucet_core::CompiledEncryption::compile(spec).map(Some)
            })
            .await
    }

    /// Ensure the file is open and return a mutable reference to the writer.
    async fn ensure_open(
        &self,
    ) -> Result<
        tokio::sync::MutexGuard<
            '_,
            Option<std::pin::Pin<Box<dyn tokio::io::AsyncWrite + Send + Unpin>>>,
        >,
        FaucetError,
    > {
        let mut guard = self.writer.lock().await;
        if guard.is_none() {
            let opened_before = self.opened_once.load(std::sync::atomic::Ordering::Relaxed);
            // First open obeys `config.append`. Re-opens (after flush()
            // cleared the writer) always append, so flush-then-write
            // sequences do not truncate previously-written data.
            let (append, truncate) = if opened_before {
                (true, false)
            } else {
                (self.config.append, !self.config.append)
            };
            if let Some(parent) = self.config.path.parent()
                && !parent.as_os_str().is_empty()
            {
                tokio::fs::create_dir_all(parent).await.map_err(|e| {
                    FaucetError::Sink(format!(
                        "failed to create parent directory '{}': {e}",
                        parent.display()
                    ))
                })?;
            }
            // Provenance for the retention GC (#587): probe before the open, so
            // `create(true)` cannot make a file faucet did not create look like
            // one it did. Idempotent + first-open-wins, so the flush→reopen
            // cycle above never reclassifies it.
            self.outputs.record_open_probing_with(&self.config.path, truncate);
            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .append(append)
                .truncate(truncate)
                .open(&self.config.path)
                .await
                .map_err(|e| {
                    FaucetError::Sink(format!(
                        "failed to open {}: {e}",
                        self.config.path.display()
                    ))
                })?;
            self.opened_once
                .store(true, std::sync::atomic::Ordering::Relaxed);
            let buffered = tokio::io::BufWriter::new(file);
            #[cfg(feature = "compression")]
            let writer: std::pin::Pin<Box<dyn tokio::io::AsyncWrite + Send + Unpin>> = {
                let path_str = self.config.path.to_string_lossy();
                let codec = self.config.compression.resolve(&path_str);
                faucet_core::compression::warn_mismatch(&path_str, codec);
                faucet_core::compression::wrap_async_writer(buffered, codec)
            };
            #[cfg(not(feature = "compression"))]
            let writer: std::pin::Pin<Box<dyn tokio::io::AsyncWrite + Send + Unpin>> =
                Box::pin(buffered);
            *guard = Some(writer);
        }
        Ok(guard)
    }
}

#[async_trait]
impl faucet_core::Sink for JsonlSink {
    fn connector_name(&self) -> &'static str {
        "jsonl"
    }

    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(JsonlSinkConfig))
            .expect("schema serialization")
    }

    fn dataset_uri(&self) -> String {
        format!("file://{}", self.config.path.display())
    }

    async fn local_outputs(&self) -> Vec<faucet_core::LocalOutput> {
        self.outputs.snapshot()
    }

    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }

        // Validate/compile encryption before touching the file so a bad
        // `encryption:` block never creates an empty output file.
        #[cfg(feature = "encryption")]
        let encryption = self.compiled_encryption().await?;

        let mut guard = self.ensure_open().await?;
        let writer = guard.as_mut().expect("writer opened in ensure_open");

        for record in records {
            let line = if self.config.pretty {
                serde_json::to_string_pretty(record)
            } else {
                serde_json::to_string(record)
            }
            .map_err(|e| FaucetError::Sink(format!("JSON serialization failed: {e}")))?;

            #[cfg(feature = "encryption")]
            let line = match encryption {
                Some(enc) => {
                    use base64::Engine as _;
                    base64::engine::general_purpose::STANDARD.encode(enc.encrypt(line.as_bytes()))
                }
                None => line,
            };

            writer
                .write_all(line.as_bytes())
                .await
                .map_err(|e| FaucetError::Sink(format!("write failed: {e}")))?;
            writer
                .write_all(b"\n")
                .await
                .map_err(|e| FaucetError::Sink(format!("write failed: {e}")))?;
        }

        tracing::debug!(records = records.len(), "JSONL batch written");
        Ok(records.len())
    }

    async fn flush(&self) -> Result<(), FaucetError> {
        let mut guard = self.writer.lock().await;
        if let Some(mut writer) = guard.take() {
            use tokio::io::AsyncWriteExt;
            writer
                .shutdown()
                .await
                .map_err(|e| FaucetError::Sink(format!("flush failed: {e}")))?;
        }
        Ok(())
    }

    /// Preflight probe for `faucet doctor`. Verifies the configured output
    /// path's parent directory exists and is writable by creating, then
    /// immediately removing, a uniquely-named temp file there. Never touches
    /// the user's actual output file, so it is fully idempotent.
    async fn check(
        &self,
        _ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::CheckReport;
        let start = std::time::Instant::now();
        let probe = crate::probe::probe_parent_writable(&self.config.path, start).await;
        Ok(CheckReport::single(probe))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::Sink;
    use serde_json::json;
    use tempfile::NamedTempFile;

    #[test]
    fn dataset_uri_uses_display_on_pathbuf() {
        let sink = JsonlSink::new(JsonlSinkConfig::new("/tmp/output.jsonl"));
        assert_eq!(sink.dataset_uri(), "file:///tmp/output.jsonl");
    }

    #[tokio::test]
    async fn writes_jsonl_records() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();
        let sink = JsonlSink::new(JsonlSinkConfig::new(&path));

        let records = vec![
            json!({"id": 1, "name": "Alice"}),
            json!({"id": 2, "name": "Bob"}),
        ];
        let count = sink.write_batch(&records).await.unwrap();
        sink.flush().await.unwrap();

        assert_eq!(count, 2);
        let content = tokio::fs::read_to_string(&path).await.unwrap();
        let lines: Vec<&str> = content.trim().split('\n').collect();
        assert_eq!(lines.len(), 2);

        let first: Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(first["id"], 1);
    }

    #[tokio::test]
    async fn append_mode() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        // Write first batch.
        let sink = JsonlSink::new(JsonlSinkConfig::new(&path));
        sink.write_batch(&[json!({"id": 1})]).await.unwrap();
        sink.flush().await.unwrap();
        drop(sink);

        // Write second batch in append mode.
        let sink = JsonlSink::new(JsonlSinkConfig::new(&path).append(true));
        sink.write_batch(&[json!({"id": 2})]).await.unwrap();
        sink.flush().await.unwrap();

        let content = tokio::fs::read_to_string(&path).await.unwrap();
        let lines: Vec<&str> = content.trim().split('\n').collect();
        assert_eq!(lines.len(), 2);
    }

    #[tokio::test]
    async fn empty_batch_returns_zero() {
        let tmp = NamedTempFile::new().unwrap();
        let sink = JsonlSink::new(JsonlSinkConfig::new(tmp.path()));
        let count = sink.write_batch(&[]).await.unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn flush_without_write_is_noop() {
        let tmp = NamedTempFile::new().unwrap();
        let sink = JsonlSink::new(JsonlSinkConfig::new(tmp.path()));
        assert!(sink.flush().await.is_ok());
    }

    #[tokio::test]
    async fn multiple_batches_accumulate() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();
        let sink = JsonlSink::new(JsonlSinkConfig::new(&path));

        sink.write_batch(&[json!({"a": 1})]).await.unwrap();
        sink.write_batch(&[json!({"b": 2}), json!({"c": 3})])
            .await
            .unwrap();
        sink.flush().await.unwrap();

        let content = tokio::fs::read_to_string(&path).await.unwrap();
        let lines: Vec<&str> = content.trim().split('\n').collect();
        assert_eq!(lines.len(), 3);
    }

    #[tokio::test]
    async fn jsonl_sink_connector_name_is_jsonl() {
        use faucet_core::Sink;
        let tmp = NamedTempFile::new().unwrap();
        let sink = JsonlSink::new(JsonlSinkConfig::new(tmp.path()));
        assert_eq!(sink.connector_name(), "jsonl");
    }

    #[tokio::test]
    async fn check_passes_when_parent_dir_exists() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.jsonl");
        let sink = JsonlSink::new(JsonlSinkConfig::new(&path));
        let report = sink
            .check(&faucet_core::check::CheckContext::default())
            .await
            .unwrap();
        assert_eq!(report.failed_count(), 0);
        assert_eq!(report.probes[0].name, "io");
        // The probe must not have created the user's output file.
        assert!(!path.exists(), "check() must not create the output file");
    }

    #[tokio::test]
    async fn check_fails_when_parent_dir_missing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nope").join("out.jsonl");
        let sink = JsonlSink::new(JsonlSinkConfig::new(&path));
        let report = sink
            .check(&faucet_core::check::CheckContext::default())
            .await
            .unwrap();
        assert_eq!(report.failed_count(), 1);
        assert_eq!(report.probes[0].name, "io");
    }

    #[tokio::test]
    async fn creates_missing_parent_directories() {
        let dir = tempfile::tempdir().unwrap();
        let nested = dir.path().join("a").join("b").join("out.jsonl");
        let sink = JsonlSink::new(JsonlSinkConfig::new(&nested));

        let records = vec![json!({"id": 1})];
        let count = sink.write_batch(&records).await.unwrap();
        sink.flush().await.unwrap();

        assert_eq!(count, 1);
        assert!(nested.exists(), "output file must exist after write");
        let content = tokio::fs::read_to_string(&nested).await.unwrap();
        let first: Value = serde_json::from_str(content.trim()).unwrap();
        assert_eq!(first["id"], 1);
    }

    #[cfg(feature = "compression")]
    #[tokio::test]
    async fn roundtrip_gzip() {
        use faucet_core::CompressionConfig;
        let tmp = NamedTempFile::with_suffix(".jsonl.gz").unwrap();
        let path = tmp.path().to_path_buf();
        let sink = JsonlSink::new(JsonlSinkConfig::new(&path).compression(CompressionConfig::Auto));

        let records = vec![
            json!({"id": 1, "name": "Alice"}),
            json!({"id": 2, "name": "Bob"}),
        ];
        sink.write_batch(&records).await.unwrap();
        sink.flush().await.unwrap();

        // Read raw bytes, decompress via faucet_core, parse JSONL.
        let bytes = tokio::fs::read(&path).await.unwrap();
        use tokio::io::AsyncReadExt;
        let mut decoded = Vec::new();
        let mut r = faucet_core::compression::wrap_async_reader(
            tokio::io::BufReader::new(&bytes[..]),
            faucet_core::Compression::Gzip,
        );
        r.read_to_end(&mut decoded).await.unwrap();
        let text = String::from_utf8(decoded).unwrap();
        let lines: Vec<&str> = text.trim().split('\n').collect();
        assert_eq!(lines.len(), 2);
        let first: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(first["id"], 1);
    }

    #[cfg(feature = "compression")]
    #[tokio::test]
    async fn roundtrip_zstd() {
        use faucet_core::CompressionConfig;
        let tmp = NamedTempFile::with_suffix(".jsonl.zst").unwrap();
        let path = tmp.path().to_path_buf();
        let sink = JsonlSink::new(JsonlSinkConfig::new(&path).compression(CompressionConfig::Auto));
        sink.write_batch(&[json!({"x": 42})]).await.unwrap();
        sink.flush().await.unwrap();

        let bytes = tokio::fs::read(&path).await.unwrap();
        use tokio::io::AsyncReadExt;
        let mut decoded = Vec::new();
        let mut r = faucet_core::compression::wrap_async_reader(
            tokio::io::BufReader::new(&bytes[..]),
            faucet_core::Compression::Zstd,
        );
        r.read_to_end(&mut decoded).await.unwrap();
        let text = String::from_utf8(decoded).unwrap();
        let v: serde_json::Value = serde_json::from_str(text.trim()).unwrap();
        assert_eq!(v["x"], 42);
    }

    #[tokio::test]
    async fn write_flush_write_does_not_truncate() {
        // Regression: flush() clears the writer; the next write_batch
        // must reopen in append mode regardless of config.append (which
        // defaults to false). Without the opened_once guard, the second
        // open would truncate and lose the first batch's records.
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();
        let sink = JsonlSink::new(JsonlSinkConfig::new(&path));

        sink.write_batch(&[json!({"first": 1})]).await.unwrap();
        sink.flush().await.unwrap();
        sink.write_batch(&[json!({"second": 2})]).await.unwrap();
        sink.flush().await.unwrap();

        let content = tokio::fs::read_to_string(&path).await.unwrap();
        let lines: Vec<&str> = content.trim().split('\n').collect();
        assert_eq!(
            lines.len(),
            2,
            "both batches must survive the mid-stream flush"
        );
        let first: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(first["first"], 1);
        let second: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(second["second"], 2);
    }

    #[cfg(feature = "compression")]
    #[tokio::test]
    async fn write_flush_write_produces_multi_member_gzip() {
        // With compression, flush() finalises one gzip member; the
        // next write_batch starts a fresh member appended after it.
        // The decoder reads both members back correctly.
        use faucet_core::CompressionConfig;
        let tmp = NamedTempFile::with_suffix(".jsonl.gz").unwrap();
        let path = tmp.path().to_path_buf();
        let sink = JsonlSink::new(JsonlSinkConfig::new(&path).compression(CompressionConfig::Auto));
        sink.write_batch(&[json!({"first": 1})]).await.unwrap();
        sink.flush().await.unwrap();
        sink.write_batch(&[json!({"second": 2})]).await.unwrap();
        sink.flush().await.unwrap();

        let bytes = tokio::fs::read(&path).await.unwrap();
        use tokio::io::AsyncReadExt;
        let mut decoded = Vec::new();
        let mut r = faucet_core::compression::wrap_async_reader(
            tokio::io::BufReader::new(&bytes[..]),
            faucet_core::Compression::Gzip,
        );
        r.read_to_end(&mut decoded).await.unwrap();
        let text = String::from_utf8(decoded).unwrap();
        let lines: Vec<&str> = text.trim().split('\n').collect();
        assert_eq!(lines.len(), 2);
    }

    #[cfg(feature = "encryption")]
    mod encryption_at_rest {
        use super::*;
        use base64::Engine as _;
        use faucet_core::{EncryptionSpec, Sink};
        use serde_json::json;
        use tempfile::NamedTempFile;

        fn spec(key: &str) -> EncryptionSpec {
            EncryptionSpec {
                key: key.into(),
                previous_keys: vec![],
                algorithm: Default::default(),
            }
        }

        fn decrypt_lines(path: &std::path::Path, key: &str) -> Vec<Value> {
            let enc = faucet_core::CompiledEncryption::compile(&spec(key)).unwrap();
            std::fs::read_to_string(path)
                .unwrap()
                .lines()
                .map(|line| {
                    let sealed = base64::engine::general_purpose::STANDARD
                        .decode(line)
                        .expect("line is base64");
                    let plain = enc.decrypt(&sealed).expect("line decrypts");
                    serde_json::from_slice(&plain).expect("plaintext is JSON")
                })
                .collect()
        }

        #[tokio::test]
        async fn per_line_encrypted_output_round_trips() {
            let tmp = NamedTempFile::new().unwrap();
            let path = tmp.path().to_path_buf();
            let sink = JsonlSink::new(JsonlSinkConfig::new(&path).encryption(spec("dlq-key")));
            let records = vec![json!({"id": 1, "pii": "s3cr3t"}), json!({"id": 2})];
            sink.write_batch(&records).await.unwrap();
            sink.flush().await.unwrap();

            // Raw file: no plaintext leakage, every line base64.
            let raw = std::fs::read_to_string(&path).unwrap();
            assert!(!raw.contains("s3cr3t"));
            assert_eq!(raw.trim().lines().count(), 2);

            assert_eq!(decrypt_lines(&path, "dlq-key"), records);
        }

        #[tokio::test]
        async fn append_across_flush_reopen_keeps_all_sealed_lines() {
            let tmp = NamedTempFile::new().unwrap();
            let path = tmp.path().to_path_buf();
            let sink = JsonlSink::new(JsonlSinkConfig::new(&path).encryption(spec("k")));
            sink.write_batch(&[json!({"first": 1})]).await.unwrap();
            sink.flush().await.unwrap();
            sink.write_batch(&[json!({"second": 2})]).await.unwrap();
            sink.flush().await.unwrap();

            let lines = decrypt_lines(&path, "k");
            assert_eq!(lines, vec![json!({"first": 1}), json!({"second": 2})]);
        }

        #[tokio::test]
        async fn empty_key_is_a_typed_error_before_any_file_io() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("out.jsonl");
            let sink = JsonlSink::new(JsonlSinkConfig::new(&path).encryption(spec(" ")));
            let err = sink.write_batch(&[json!(1)]).await.unwrap_err();
            assert!(matches!(err, FaucetError::Config(_)));
            assert!(!path.exists(), "a bad config must not create the file");
        }

        #[cfg(feature = "compression")]
        #[tokio::test]
        async fn compression_and_encryption_are_mutually_exclusive() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("out.jsonl.gz"); // Auto resolves gzip
            let sink = JsonlSink::new(JsonlSinkConfig::new(&path).encryption(spec("k")));
            let err = sink.write_batch(&[json!(1)]).await.unwrap_err();
            assert!(err.to_string().contains("mutually"), "{err}");
        }

        #[cfg(feature = "compression")]
        #[tokio::test]
        async fn encryption_with_plain_path_and_auto_compression_is_fine() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("out.jsonl"); // Auto resolves None
            let sink = JsonlSink::new(JsonlSinkConfig::new(&path).encryption(spec("k")));
            sink.write_batch(&[json!({"ok": true})]).await.unwrap();
            sink.flush().await.unwrap();
            assert_eq!(decrypt_lines(&path, "k"), vec![json!({"ok": true})]);
        }
    }
    #[tokio::test]
    async fn local_outputs_reports_a_file_faucet_created() {
        // Retention GC provenance (#587): a path that did not exist before the
        // run is faucet's own output and is collectable.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.jsonl");
        let sink = JsonlSink::new(JsonlSinkConfig::new(&path));
        assert!(
            sink.local_outputs().await.is_empty(),
            "nothing is recorded before the first write — the file does not exist yet"
        );

        sink.write_batch(&[json!({"id": 1})]).await.unwrap();
        sink.flush().await.unwrap();

        let outs = sink.local_outputs().await;
        assert_eq!(outs.len(), 1);
        assert_eq!(outs[0].path, path);
        assert!(!outs[0].pre_existing);
    }

    #[tokio::test]
    async fn local_outputs_flags_a_file_faucet_did_not_create() {
        // `NamedTempFile` already exists on disk, so faucet is appending to (or
        // truncating) someone else's file — the GC must never delete it.
        let tmp = NamedTempFile::new().unwrap();
        let sink = JsonlSink::new(JsonlSinkConfig::new(tmp.path()));
        sink.write_batch(&[json!({"id": 1})]).await.unwrap();

        let outs = sink.local_outputs().await;
        assert_eq!(outs.len(), 1);
        assert!(outs[0].pre_existing);
    }

    #[tokio::test]
    async fn local_outputs_stays_one_entry_across_the_flush_reopen_cycle() {
        // The per-page flush clears the writer and the next write re-opens the
        // path — which now exists. That re-open must neither add a second entry
        // nor reclassify the file as pre-existing (which would make it
        // permanently un-collectable).
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.jsonl");
        let sink = JsonlSink::new(JsonlSinkConfig::new(&path));
        sink.write_batch(&[json!({"id": 1})]).await.unwrap();
        sink.flush().await.unwrap();
        sink.write_batch(&[json!({"id": 2})]).await.unwrap();
        sink.flush().await.unwrap();

        let outs = sink.local_outputs().await;
        assert_eq!(outs.len(), 1);
        assert!(!outs[0].pre_existing);
    }
    #[tokio::test]
    async fn local_outputs_marks_a_truncated_existing_file_replaced_and_an_appended_one_not() {
        // Truncating a file faucet did not create leaves only faucet's bytes in
        // it — safe to preview, still never collected. Appending keeps the
        // original owner's content, so it stays plain pre-existing.
        for (append, replaced) in [(false, true), (true, false)] {
            let tmp = NamedTempFile::new().unwrap();
            let sink = JsonlSink::new(JsonlSinkConfig::new(tmp.path()).append(append));
            sink.write_batch(&[json!({"id": 1})]).await.unwrap();
            let outs = sink.local_outputs().await;
            assert!(outs[0].pre_existing);
            assert_eq!(outs[0].replaced, replaced, "append = {append}");
        }
    }
}
