//! CSV file sink.

use crate::config::CsvSinkConfig;
use async_trait::async_trait;
use faucet_core::FaucetError;
use serde_json::Value;
use std::fs::OpenOptions;
use std::sync::Mutex;

/// The inner writer the CSV serializer writes into. With compression enabled
/// it is a [`SyncCompressWriter`](faucet_core::compression::SyncCompressWriter)
/// that retains the concrete encoder so `finish()` errors surface on flush
/// (#78/#41); otherwise it is the raw file.
#[cfg(feature = "compression")]
type SinkWriter = faucet_core::compression::SyncCompressWriter<std::fs::File>;
#[cfg(not(feature = "compression"))]
type SinkWriter = std::fs::File;

/// State for the CSV writer, including the determined column order.
struct WriterState {
    writer: csv::Writer<SinkWriter>,
    columns: Vec<String>,
}

/// A sink that writes JSON records to a CSV file.
///
/// Column order is the union of keys across the records of the first
/// `write_batch` call, in first-seen order (so a field present only in a later
/// record of that batch is still captured). Subsequent records use the same
/// column order; missing fields are written as empty strings.
///
/// [`Sink::flush`](faucet_core::Sink::flush) finalises the encoder (writes the trailer) and clears the
/// writer slot — a subsequent `write_batch` reopens the file in append mode
/// (independent of `config.append`) and starts a fresh encoder. This makes
/// the per-page `flush` the pipeline emits for bookmarked pages safe for CDC
/// sources — every transaction appends rather than truncates.
pub struct CsvSink {
    config: CsvSinkConfig,
    state: Mutex<Option<WriterState>>,
    /// The column order frozen at the first open, retained **across `flush()`**.
    /// `flush()` drops `state` (and its `columns`), so without this a re-open
    /// after flush would re-derive columns from the *current* batch — a
    /// different order or subset than the already-written header — silently
    /// misaligning later rows and defeating the `on_unknown_field` guard (audit
    /// #321 H2). Set once on the first write; every re-open reuses it.
    frozen_columns: Mutex<Option<Vec<String>>>,
    /// Tracks whether the file has been opened at least once.
    /// On re-opens (after `flush()` clears the writer), we always use
    /// append mode regardless of `config.append` so the new gzip / zstd
    /// member appends instead of truncating the file. Without this, the
    /// pipeline's per-bookmark flush would silently lose data when
    /// `config.append = false` (the default).
    opened_once: std::sync::atomic::AtomicBool,
    /// One-shot guard for the "dropping unknown field" warning. The CSV header
    /// is frozen from the first batch, so a field that first appears later
    /// cannot be added to the header and its value is dropped. We warn at most
    /// once per run (like the parquet sink's `warn_on_unknown_fields`) so the
    /// loss is visible rather than silent, without flooding the log.
    warned_unknown: std::sync::atomic::AtomicBool,
    /// The file this sink opened, for the local-output retention GC (#587).
    /// Recorded at the first open — before the file is created — so a path that
    /// already held someone else's data is flagged `pre_existing` and never
    /// collected. See `faucet_core::local_outputs`.
    outputs: faucet_core::LocalOutputLog,
}

impl CsvSink {
    /// Create a new CSV sink. The file is opened on the first `write_batch` call.
    pub fn new(config: CsvSinkConfig) -> Self {
        Self {
            config,
            state: Mutex::new(None),
            frozen_columns: Mutex::new(None),
            opened_once: std::sync::atomic::AtomicBool::new(false),
            warned_unknown: std::sync::atomic::AtomicBool::new(false),
            outputs: faucet_core::LocalOutputLog::new(),
        }
    }

    /// Convert a JSON value to a string suitable for a CSV field.
    fn value_to_csv_field(value: &Value) -> String {
        match value {
            Value::Null => String::new(),
            Value::String(s) => s.clone(),
            Value::Bool(b) => b.to_string(),
            Value::Number(n) => n.to_string(),
            // For nested objects/arrays, serialize as JSON.
            other => other.to_string(),
        }
    }
}

#[async_trait]
impl faucet_core::Sink for CsvSink {
    fn connector_name(&self) -> &'static str {
        "csv"
    }

    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(CsvSinkConfig)).expect("schema serialization")
    }

    fn dataset_uri(&self) -> String {
        format!("file://{}", self.config.path)
    }

    async fn local_outputs(&self) -> Vec<faucet_core::LocalOutput> {
        self.outputs.snapshot()
    }

    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }

        let config = self.config.clone();
        let records: Vec<Value> = records.to_vec();

        // Extract state from the mutex before entering the blocking task.
        // This avoids holding the MutexGuard across an await point.
        let current_state = {
            let mut guard = self
                .state
                .lock()
                .map_err(|e| FaucetError::Sink(format!("CSV sink lock poisoned: {e}")))?;
            guard.take()
        };

        let opened_before = self.opened_once.load(std::sync::atomic::Ordering::Relaxed);
        let already_warned = self
            .warned_unknown
            .load(std::sync::atomic::Ordering::Relaxed);
        // The frozen header, if a prior open already established it. On a re-open
        // after `flush()` this is reused verbatim so the column order never
        // drifts from the written header (#321 H2).
        let frozen_columns = {
            let guard = self
                .frozen_columns
                .lock()
                .map_err(|e| FaucetError::Sink(format!("CSV sink lock poisoned: {e}")))?;
            guard.clone()
        };

        // Provenance for the retention GC (#587). Probed here — on the async side,
        // before the blocking task can create the file — so a path that already
        // held someone else's data is never mistaken for one faucet created.
        // Idempotent and first-open-wins, so the flush→reopen cycle cannot
        // reclassify it.
        self.outputs.record_open_probing_with(
            std::path::PathBuf::from(&self.config.path),
            !opened_before && !self.config.append,
        );

        let result = tokio::task::spawn_blocking(move || {
            write_csv_blocking(
                config,
                current_state,
                &records,
                opened_before,
                already_warned,
                frozen_columns,
            )
        })
        .await
        .map_err(|e| FaucetError::Sink(format!("CSV write task failed: {e}")))?;

        let WriteOutcome {
            state: new_state,
            count,
            warned_unknown,
        } = result?;

        // Mark opened. From now on, re-opens (after flush) use append mode.
        self.opened_once
            .store(true, std::sync::atomic::Ordering::Relaxed);

        // Latch the frozen column order on the first open so it survives future
        // `flush()`-then-write cycles (set once; never changes thereafter).
        {
            let mut guard = self
                .frozen_columns
                .lock()
                .map_err(|e| FaucetError::Sink(format!("CSV sink lock poisoned: {e}")))?;
            if guard.is_none() {
                *guard = Some(new_state.columns.clone());
            }
        }

        // Latch the one-shot unknown-field warning so it fires once per run.
        if warned_unknown {
            self.warned_unknown
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }

        // Put the state back.
        {
            let mut guard = self
                .state
                .lock()
                .map_err(|e| FaucetError::Sink(format!("CSV sink lock poisoned: {e}")))?;
            *guard = Some(new_state);
        }

        Ok(count)
    }

    async fn flush(&self) -> Result<(), FaucetError> {
        // Take the state out of the mutex so we can move it into a blocking
        // task. Replacing it with None means the next write_batch reopens
        // the file in append mode — for compressed output this starts a
        // fresh gzip/zstd member, which decoders read back transparently.
        let state = {
            let mut guard = self
                .state
                .lock()
                .map_err(|e| FaucetError::Sink(format!("CSV sink lock poisoned: {e}")))?;
            guard.take()
        };
        if let Some(state) = state {
            tokio::task::spawn_blocking(move || -> Result<(), FaucetError> {
                let WriterState { writer, .. } = state;
                // Flush the csv serializer's buffer and recover the inner
                // writer so the compression encoder can be finalised with its
                // error captured, rather than swallowed on drop (#78/#41).
                let inner = writer
                    .into_inner()
                    .map_err(|e| FaucetError::Sink(format!("CSV flush failed: {e}")))?;
                #[cfg(feature = "compression")]
                {
                    // Writes the gzip/zstd trailer and surfaces any I/O error.
                    inner.finish().map_err(|e| {
                        FaucetError::Sink(format!("CSV compression finalise failed: {e}"))
                    })?;
                }
                #[cfg(not(feature = "compression"))]
                {
                    let mut f = inner;
                    std::io::Write::flush(&mut f)
                        .map_err(|e| FaucetError::Sink(format!("CSV flush failed: {e}")))?;
                }
                Ok(())
            })
            .await
            .map_err(|e| FaucetError::Sink(format!("CSV flush task failed: {e}")))??;
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
        let path = self.config.path.clone();
        // The filesystem probe is synchronous; run it on a blocking thread to
        // stay off the async runtime, matching how the sink does its I/O.
        let probe = tokio::task::spawn_blocking(move || {
            crate::probe::probe_parent_writable(&path, std::time::Instant::now())
        })
        .await
        .map_err(|e| FaucetError::Sink(format!("CSV check task failed: {e}")))?;
        Ok(CheckReport::single(probe))
    }
}

/// Result of one blocking CSV write: the (returned) writer state, the number
/// of records written, and whether this batch emitted the one-shot
/// "dropping unknown field" warning (so the caller can latch its atomic flag).
struct WriteOutcome {
    state: WriterState,
    count: usize,
    warned_unknown: bool,
}

/// Identify record keys that are absent from the frozen `columns` set, in
/// first-seen order across `records`, deduplicated. These fields cannot be
/// written (the header is already fixed) and would otherwise be silently
/// dropped. Pure — no I/O — so it is unit-testable.
fn unknown_fields(columns: &[String], records: &[Value]) -> Vec<String> {
    let known: std::collections::HashSet<&str> = columns.iter().map(String::as_str).collect();
    let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
    let mut out: Vec<String> = Vec::new();
    for record in records {
        if let Value::Object(map) = record {
            for k in map.keys() {
                if !known.contains(k.as_str()) && seen.insert(k.as_str()) {
                    out.push(k.clone());
                }
            }
        }
    }
    out
}

/// Synchronous CSV writing logic, run inside `spawn_blocking`.
fn write_csv_blocking(
    config: CsvSinkConfig,
    existing_state: Option<WriterState>,
    records: &[Value],
    opened_before: bool,
    already_warned: bool,
    frozen_columns: Option<Vec<String>>,
) -> Result<WriteOutcome, FaucetError> {
    let mut state = match existing_state {
        Some(s) => s,
        None => {
            // Column order. On a re-open after `flush()` reuse the header frozen
            // at the first open (#321 H2) so later rows never drift out of
            // alignment with the already-written header. Only on the very first
            // open do we derive columns from the UNION of keys across the first
            // batch's records, in first-seen order — not just `records[0]`.
            // Otherwise a field present only in a later record of the first batch
            // would be absent from the header and silently dropped from every row
            // (audit #146 H2). (A later flush-segment cannot change the
            // already-written header — that is a separate, documented limitation.)
            let mut columns: Vec<String> = Vec::new();
            match frozen_columns {
                Some(frozen) => columns = frozen,
                None => {
                    let mut seen: std::collections::HashSet<&str> =
                        std::collections::HashSet::new();
                    for record in records {
                        match record {
                            Value::Object(map) => {
                                for k in map.keys() {
                                    if seen.insert(k.as_str()) {
                                        columns.push(k.clone());
                                    }
                                }
                            }
                            _ => {
                                return Err(FaucetError::Sink(
                                    "CSV sink expects JSON objects, got non-object record".into(),
                                ));
                            }
                        }
                    }
                }
            }

            // First open obeys `config.append`. Re-opens (after flush()
            // cleared the writer) always append, so flush-then-write
            // sequences do not truncate previously-written data.
            let (append, truncate) = if opened_before {
                (true, false)
            } else {
                (config.append, !config.append)
            };

            if let Some(parent) = std::path::Path::new(&config.path).parent()
                && !parent.as_os_str().is_empty()
            {
                std::fs::create_dir_all(parent).map_err(|e| {
                    FaucetError::Sink(format!(
                        "failed to create parent directory '{}': {e}",
                        parent.display()
                    ))
                })?;
            }
            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .append(append)
                .truncate(truncate)
                .open(&config.path)
                .map_err(|e| {
                    FaucetError::Sink(format!("failed to open CSV file '{}': {e}", config.path))
                })?;

            #[cfg(feature = "compression")]
            let inner: SinkWriter = {
                let codec = config.compression.resolve(&config.path);
                faucet_core::compression::warn_mismatch(&config.path, codec);
                faucet_core::compression::sync_compress_writer(file, codec)
            };
            #[cfg(not(feature = "compression"))]
            let inner: SinkWriter = file;

            let mut writer = csv::WriterBuilder::new()
                .delimiter(config.delimiter)
                .from_writer(inner);

            // Write header row if configured and this is the first open.
            if config.write_headers && !append {
                writer
                    .write_record(&columns)
                    .map_err(|e| FaucetError::Sink(format!("failed to write CSV headers: {e}")))?;
            }

            WriterState { writer, columns }
        }
    };

    // The header is now frozen (either just written, or carried over from a
    // prior batch). Detect any record key that is not a known column — it
    // cannot be added to the header and would be dropped from the output.
    // Under `error` this aborts the write before any row is written; under
    // `warn` (default) it emits a single warning per run and continues.
    let unknown = unknown_fields(&state.columns, records);
    let mut warned_unknown = false;
    if !unknown.is_empty() {
        match config.on_unknown_field {
            crate::config::OnUnknownField::Error => {
                return Err(FaucetError::Sink(format!(
                    "CSV sink received record field(s) not in the frozen column set \
                     and on_unknown_field=error: [{}]. The CSV header is fixed from the \
                     first batch and cannot be extended; these values would be dropped.",
                    unknown.join(", ")
                )));
            }
            crate::config::OnUnknownField::Warn => {
                if !already_warned {
                    warned_unknown = true;
                    tracing::warn!(
                        fields = %unknown.join(", "),
                        path = %config.path,
                        "dropping field(s) not in the frozen CSV column set — the header is \
                         fixed from the first batch and cannot be extended; set \
                         on_unknown_field=error to fail instead"
                    );
                }
            }
        }
    }

    let mut count = 0;
    for record in records {
        let row: Vec<String> = state
            .columns
            .iter()
            .map(|col| {
                record
                    .get(col)
                    .map(CsvSink::value_to_csv_field)
                    .unwrap_or_default()
            })
            .collect();

        state
            .writer
            .write_record(&row)
            .map_err(|e| FaucetError::Sink(format!("CSV write error: {e}")))?;
        count += 1;
    }

    tracing::debug!(records = count, path = %config.path, "CSV batch written");

    Ok(WriteOutcome {
        state,
        count,
        warned_unknown,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::Sink;
    use serde_json::json;
    use tempfile::NamedTempFile;

    #[test]
    fn dataset_uri_returns_file_scheme() {
        let sink = CsvSink::new(CsvSinkConfig::new("/tmp/output.csv"));
        assert_eq!(sink.dataset_uri(), "file:///tmp/output.csv");
    }

    #[tokio::test]
    async fn writes_csv_records() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        let sink = CsvSink::new(CsvSinkConfig::new(&path));

        let records = vec![
            json!({"id": 1, "name": "Alice"}),
            json!({"id": 2, "name": "Bob"}),
        ];
        let count = sink.write_batch(&records).await.unwrap();
        sink.flush().await.unwrap();

        assert_eq!(count, 2);

        let content = tokio::fs::read_to_string(&path).await.unwrap();
        let lines: Vec<&str> = content.trim().split('\n').collect();
        // Header + 2 data rows.
        assert_eq!(lines.len(), 3);
    }

    #[tokio::test]
    async fn columns_union_across_first_batch_not_just_first_record() {
        // H2 (audit #146): column order is the union of keys across the first
        // batch's records. The first record lacks `email`; before the fix the
        // header was fixed from record 0 and `email` was silently dropped from
        // every row. After the fix `email` is a column and the second record's
        // value appears (the first row leaves it empty).
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        let sink = CsvSink::new(CsvSinkConfig::new(&path));

        let records = vec![
            json!({ "id": 1, "name": "Alice" }),
            json!({ "id": 2, "name": "Bob", "email": "bob@x.y" }),
        ];
        sink.write_batch(&records).await.unwrap();
        sink.flush().await.unwrap();

        let content = tokio::fs::read_to_string(&path).await.unwrap();
        let lines: Vec<&str> = content.trim().split('\n').collect();
        assert_eq!(lines.len(), 3, "header + 2 rows");
        assert!(
            lines[0].contains("email"),
            "header must include the later-record-only column: {}",
            lines[0]
        );
        // Row 2 carries the email value; row 1 leaves it empty.
        assert!(
            lines[2].contains("bob@x.y"),
            "second row must carry the unioned column value: {}",
            lines[2]
        );
    }

    #[test]
    fn unknown_fields_detects_late_keys_in_first_seen_order() {
        let columns = vec!["id".to_string(), "name".to_string()];
        let records = vec![
            json!({ "id": 1, "name": "Alice" }),
            json!({ "id": 2, "email": "b@x.y", "name": "Bob", "phone": "555" }),
            json!({ "id": 3, "email": "c@x.y" }), // email is a dup, skipped
            json!("not-an-object"),               // non-objects are ignored here
        ];
        let unknown = unknown_fields(&columns, &records);
        assert_eq!(unknown, vec!["email".to_string(), "phone".to_string()]);
    }

    #[test]
    fn unknown_fields_empty_when_all_known() {
        let columns = vec!["a".to_string(), "b".to_string()];
        let records = vec![json!({ "a": 1 }), json!({ "b": 2, "a": 3 })];
        assert!(unknown_fields(&columns, &records).is_empty());
    }

    #[tokio::test]
    async fn later_page_new_field_is_dropped_and_warns() {
        // F31: the column set is frozen from the first batch. A field that
        // first appears in a later batch (page 2) cannot be added to the
        // already-written header, so its value is dropped — but the loss is
        // now visible via a one-shot warning (default on_unknown_field=warn).
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        let sink = CsvSink::new(CsvSinkConfig::new(&path));

        // Page 1 freezes columns to {id, name}.
        sink.write_batch(&[json!({ "id": 1, "name": "Alice" })])
            .await
            .unwrap();
        // Page 2 introduces a new field `email` absent from page 1.
        let count = sink
            .write_batch(&[json!({ "id": 2, "name": "Bob", "email": "bob@x.y" })])
            .await
            .unwrap();
        sink.flush().await.unwrap();

        assert_eq!(count, 1);
        // The one-shot warn flag must have latched (warning path exercised).
        assert!(
            sink.warned_unknown
                .load(std::sync::atomic::Ordering::Relaxed),
            "the unknown-field warning must have fired"
        );

        let content = tokio::fs::read_to_string(&path).await.unwrap();
        let lines: Vec<&str> = content.trim().split('\n').collect();
        assert_eq!(lines.len(), 3, "header + 2 rows");
        // The header carries only the first-page columns.
        assert!(
            !lines[0].contains("email"),
            "header must not gain the late field: {}",
            lines[0]
        );
        // The dropped field's value must NOT appear anywhere in the output.
        assert!(
            !content.contains("bob@x.y"),
            "late field value must be dropped from output: {content}"
        );
    }

    #[tokio::test]
    async fn on_unknown_field_error_aborts_with_sink_error() {
        use crate::config::OnUnknownField;
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        let sink = CsvSink::new(CsvSinkConfig::new(&path).on_unknown_field(OnUnknownField::Error));

        sink.write_batch(&[json!({ "id": 1, "name": "Alice" })])
            .await
            .unwrap();
        let err = sink
            .write_batch(&[json!({ "id": 2, "name": "Bob", "email": "bob@x.y" })])
            .await
            .expect_err("a late field must abort under on_unknown_field=error");
        match err {
            FaucetError::Sink(msg) => {
                assert!(msg.contains("email"), "error must name the field: {msg}");
                assert!(
                    msg.contains("on_unknown_field=error"),
                    "error must explain: {msg}"
                );
            }
            other => panic!("expected FaucetError::Sink, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn first_batch_union_does_not_trigger_unknown_warning() {
        // The first batch's column union already covers all its keys, so a
        // later-record-only field within the first batch must NOT warn.
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        let sink = CsvSink::new(CsvSinkConfig::new(&path));
        sink.write_batch(&[
            json!({ "id": 1, "name": "Alice" }),
            json!({ "id": 2, "name": "Bob", "email": "bob@x.y" }),
        ])
        .await
        .unwrap();
        sink.flush().await.unwrap();
        assert!(
            !sink
                .warned_unknown
                .load(std::sync::atomic::Ordering::Relaxed),
            "first-batch union must not trigger the unknown-field warning"
        );
        // And the email value IS present (it was part of the frozen union).
        let content = tokio::fs::read_to_string(&path).await.unwrap();
        assert!(content.contains("bob@x.y"));
    }

    #[tokio::test]
    async fn writes_csv_without_headers() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        let sink = CsvSink::new(CsvSinkConfig::new(&path).write_headers(false));

        let records = vec![json!({"a": "1", "b": "2"})];
        sink.write_batch(&records).await.unwrap();
        sink.flush().await.unwrap();

        let content = tokio::fs::read_to_string(&path).await.unwrap();
        let lines: Vec<&str> = content.trim().split('\n').collect();
        // No header, just 1 data row.
        assert_eq!(lines.len(), 1);
    }

    #[tokio::test]
    async fn empty_batch_returns_zero() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        let sink = CsvSink::new(CsvSinkConfig::new(&path));
        let count = sink.write_batch(&[]).await.unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn multiple_batches_accumulate() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        let sink = CsvSink::new(CsvSinkConfig::new(&path));

        sink.write_batch(&[json!({"x": "1"})]).await.unwrap();
        sink.write_batch(&[json!({"x": "2"}), json!({"x": "3"})])
            .await
            .unwrap();
        sink.flush().await.unwrap();

        let content = tokio::fs::read_to_string(&path).await.unwrap();
        let lines: Vec<&str> = content.trim().split('\n').collect();
        // Header + 3 data rows.
        assert_eq!(lines.len(), 4);
    }

    #[tokio::test]
    async fn missing_fields_written_as_empty() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        let sink = CsvSink::new(CsvSinkConfig::new(&path));

        let records = vec![
            json!({"a": "1", "b": "2"}),
            json!({"a": "3"}), // missing "b"
        ];
        sink.write_batch(&records).await.unwrap();
        sink.flush().await.unwrap();

        let content = tokio::fs::read_to_string(&path).await.unwrap();
        let lines: Vec<&str> = content.trim().split('\n').collect();
        assert_eq!(lines.len(), 3); // header + 2 rows
    }

    #[tokio::test]
    async fn value_to_csv_field_handles_types() {
        assert_eq!(CsvSink::value_to_csv_field(&json!(null)), "");
        assert_eq!(CsvSink::value_to_csv_field(&json!("hello")), "hello");
        assert_eq!(CsvSink::value_to_csv_field(&json!(42)), "42");
        assert_eq!(CsvSink::value_to_csv_field(&json!(true)), "true");
        assert_eq!(CsvSink::value_to_csv_field(&json!(2.72)), "2.72");
    }

    #[tokio::test]
    async fn flush_without_write_is_noop() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        let sink = CsvSink::new(CsvSinkConfig::new(&path));
        assert!(sink.flush().await.is_ok());
    }

    #[tokio::test]
    async fn check_passes_when_parent_dir_exists() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.csv");
        let path_str = path.to_str().unwrap().to_string();
        let sink = CsvSink::new(CsvSinkConfig::new(&path_str));
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
        let path = dir.path().join("nope").join("out.csv");
        let path_str = path.to_str().unwrap().to_string();
        let sink = CsvSink::new(CsvSinkConfig::new(&path_str));
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
        let nested = dir.path().join("a").join("b").join("out.csv");
        let path_str = nested.to_str().unwrap().to_string();
        let sink = CsvSink::new(CsvSinkConfig::new(&path_str));

        let records = vec![json!({"id": "1", "name": "Alice"})];
        let count = sink.write_batch(&records).await.unwrap();
        sink.flush().await.unwrap();

        assert_eq!(count, 1);
        assert!(nested.exists(), "output file must exist after write");
        let content = tokio::fs::read_to_string(&nested).await.unwrap();
        let lines: Vec<&str> = content.trim().split('\n').collect();
        // Header + 1 data row.
        assert_eq!(lines.len(), 2);
    }

    #[cfg(feature = "compression")]
    #[tokio::test]
    async fn roundtrip_gzip() {
        use faucet_core::CompressionConfig;
        let tmp = NamedTempFile::with_suffix(".csv.gz").unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        let sink = CsvSink::new(CsvSinkConfig::new(&path).compression(CompressionConfig::Auto));

        let records = vec![
            json!({"id": "1", "name": "Alice"}),
            json!({"id": "2", "name": "Bob"}),
        ];
        sink.write_batch(&records).await.unwrap();
        sink.flush().await.unwrap();

        let bytes = tokio::fs::read(&path).await.unwrap();
        use std::io::Read;
        let mut r =
            faucet_core::compression::wrap_sync_reader(&bytes[..], faucet_core::Compression::Gzip);
        let mut text = String::new();
        r.read_to_string(&mut text).unwrap();
        let lines: Vec<&str> = text.trim().split('\n').collect();
        // Header + 2 rows.
        assert_eq!(lines.len(), 3);
    }

    #[cfg(feature = "compression")]
    #[tokio::test]
    async fn roundtrip_zstd() {
        use faucet_core::CompressionConfig;
        let tmp = NamedTempFile::with_suffix(".csv.zst").unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        let sink = CsvSink::new(CsvSinkConfig::new(&path).compression(CompressionConfig::Auto));

        sink.write_batch(&[json!({"x": "42"})]).await.unwrap();
        sink.flush().await.unwrap();

        let bytes = tokio::fs::read(&path).await.unwrap();
        use std::io::Read;
        let mut r =
            faucet_core::compression::wrap_sync_reader(&bytes[..], faucet_core::Compression::Zstd);
        let mut text = String::new();
        r.read_to_string(&mut text).unwrap();
        let lines: Vec<&str> = text.trim().split('\n').collect();
        // Header + 1 row.
        assert_eq!(lines.len(), 2);
    }

    #[tokio::test]
    async fn write_flush_write_does_not_truncate() {
        // Regression: flush() clears the writer; the next write_batch
        // must reopen in append mode regardless of config.append (which
        // defaults to false). Without the opened_once guard, the second
        // open would truncate and lose the first batch's records.
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        let sink = CsvSink::new(CsvSinkConfig::new(&path));

        sink.write_batch(&[json!({"id": "1"})]).await.unwrap();
        sink.flush().await.unwrap();
        sink.write_batch(&[json!({"id": "2"})]).await.unwrap();
        sink.flush().await.unwrap();

        let content = tokio::fs::read_to_string(&path).await.unwrap();
        let lines: Vec<&str> = content.trim().split('\n').collect();
        // Header + 2 data rows (header is written only on the first open).
        assert_eq!(
            lines.len(),
            3,
            "both batches must survive the mid-stream flush"
        );
    }

    #[tokio::test]
    async fn column_order_frozen_across_flush() {
        // #321 H2: flush() drops the writer state; the next write_batch reopens
        // in append mode. The reopened batch must reuse the header frozen at the
        // first open rather than re-deriving columns from its own keys —
        // otherwise a batch with a different key set writes rows misaligned with
        // the already-written header. Here page 2 has only `name`; before the fix
        // it re-derived columns=[name] and wrote "Bob" under the `id` column.
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        let sink = CsvSink::new(CsvSinkConfig::new(&path));

        sink.write_batch(&[json!({ "id": "1", "name": "Alice" })])
            .await
            .unwrap();
        sink.flush().await.unwrap();
        // Page 2: a subset of the frozen columns, in a different shape.
        sink.write_batch(&[json!({ "name": "Bob" })]).await.unwrap();
        sink.flush().await.unwrap();

        let content = tokio::fs::read_to_string(&path).await.unwrap();
        let lines: Vec<&str> = content.trim().split('\n').collect();
        assert_eq!(lines.len(), 3, "header + 2 rows");
        assert_eq!(lines[0], "id,name", "header frozen from first open");
        assert_eq!(lines[1], "1,Alice");
        // Bob must land in the `name` column (id empty), not the first column.
        assert_eq!(
            lines[2], ",Bob",
            "reopened batch must align with the frozen header"
        );
    }

    #[tokio::test]
    async fn on_unknown_field_error_guard_holds_across_flush() {
        // #321 H2: the on_unknown_field guard compares against the frozen header,
        // not a per-batch re-derived set. A new field appearing in a post-flush
        // batch must still trip the guard.
        use crate::config::OnUnknownField;
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        let sink = CsvSink::new(CsvSinkConfig::new(&path).on_unknown_field(OnUnknownField::Error));

        sink.write_batch(&[json!({ "id": 1, "name": "Alice" })])
            .await
            .unwrap();
        sink.flush().await.unwrap();
        let err = sink
            .write_batch(&[json!({ "id": 2, "name": "Bob", "email": "b@x.y" })])
            .await
            .expect_err("a new field after flush must still abort under error mode");
        match err {
            FaucetError::Sink(msg) => assert!(msg.contains("email"), "must name the field: {msg}"),
            other => panic!("expected FaucetError::Sink, got {other:?}"),
        }
    }

    #[cfg(feature = "compression")]
    #[tokio::test]
    async fn write_flush_write_produces_multi_member_gzip_csv() {
        // With compression, flush() finalises one gzip member; the
        // next write_batch starts a fresh member appended after it.
        // The decoder reads both members back correctly.
        use faucet_core::CompressionConfig;
        let tmp = NamedTempFile::with_suffix(".csv.gz").unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        let sink = CsvSink::new(CsvSinkConfig::new(&path).compression(CompressionConfig::Auto));
        sink.write_batch(&[json!({"id": "1"})]).await.unwrap();
        sink.flush().await.unwrap();
        sink.write_batch(&[json!({"id": "2"})]).await.unwrap();
        sink.flush().await.unwrap();

        let bytes = tokio::fs::read(&path).await.unwrap();
        use std::io::Read;
        let mut r =
            faucet_core::compression::wrap_sync_reader(&bytes[..], faucet_core::Compression::Gzip);
        let mut text = String::new();
        r.read_to_string(&mut text).unwrap();
        let lines: Vec<&str> = text.trim().split('\n').collect();
        // Header (from first open) + 2 data rows. The re-open uses
        // append=true so no second header is written.
        assert_eq!(lines.len(), 3);
    }
    #[tokio::test]
    async fn local_outputs_reports_a_file_faucet_created() {
        // Retention GC provenance (#587).
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.csv");
        let sink = CsvSink::new(CsvSinkConfig::new(path.to_str().unwrap()));
        assert!(sink.local_outputs().await.is_empty());

        sink.write_batch(&[json!({"id": "1"})]).await.unwrap();
        sink.flush().await.unwrap();

        let outs = sink.local_outputs().await;
        assert_eq!(outs.len(), 1);
        assert_eq!(outs[0].path, path);
        assert!(!outs[0].pre_existing);
    }

    #[tokio::test]
    async fn local_outputs_flags_a_file_faucet_did_not_create() {
        let tmp = NamedTempFile::with_suffix(".csv").unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        let sink = CsvSink::new(CsvSinkConfig::new(&path));
        sink.write_batch(&[json!({"id": "1"})]).await.unwrap();

        let outs = sink.local_outputs().await;
        assert_eq!(outs.len(), 1);
        assert!(outs[0].pre_existing);
    }

    #[tokio::test]
    async fn local_outputs_stays_one_entry_across_the_flush_reopen_cycle() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.csv");
        let sink = CsvSink::new(CsvSinkConfig::new(path.to_str().unwrap()));
        sink.write_batch(&[json!({"id": "1"})]).await.unwrap();
        sink.flush().await.unwrap();
        sink.write_batch(&[json!({"id": "2"})]).await.unwrap();
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
            let tmp = NamedTempFile::with_suffix(".csv").unwrap();
            let sink =
                CsvSink::new(CsvSinkConfig::new(tmp.path().to_str().unwrap()).append(append));
            sink.write_batch(&[json!({"id": "1"})]).await.unwrap();
            let outs = sink.local_outputs().await;
            assert!(outs[0].pre_existing);
            assert_eq!(outs[0].replaced, replaced, "append = {append}");
        }
    }
}
