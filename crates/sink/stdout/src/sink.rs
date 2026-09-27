//! Stdout/stderr sink implementation.

use crate::config::{StdStream, StdoutFormat, StdoutSinkConfig};
use async_trait::async_trait;
use faucet_core::FaucetError;
use serde_json::Value;
use std::io;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::Mutex;

/// State guarded by a single mutex so the running record count, the writer,
/// and the "consumer closed the pipe" flag can't race against each other.
struct State {
    writer: Box<dyn AsyncWrite + Unpin + Send>,
    written: usize,
    closed: bool,
}

/// A sink that writes records to standard output or standard error.
pub struct StdoutSink {
    config: StdoutSinkConfig,
    state: Mutex<State>,
}

impl StdoutSink {
    /// Create a new stdout/stderr sink. Opens the underlying stream eagerly.
    pub fn new(config: StdoutSinkConfig) -> Self {
        let writer: Box<dyn AsyncWrite + Unpin + Send> = match config.destination {
            StdStream::Stdout => Box::new(tokio::io::stdout()),
            StdStream::Stderr => Box::new(tokio::io::stderr()),
        };
        Self::with_writer(config, writer)
    }

    /// Construct with a caller-provided async writer. Used by tests to capture
    /// output, and by integrators who want to redirect into something other
    /// than the real stdio handles (e.g. a log file, an in-memory buffer).
    pub fn with_writer(
        config: StdoutSinkConfig,
        writer: Box<dyn AsyncWrite + Unpin + Send>,
    ) -> Self {
        Self {
            config,
            state: Mutex::new(State {
                writer,
                written: 0,
                closed: false,
            }),
        }
    }

    fn encode(&self, record: &Value) -> Result<Vec<u8>, FaucetError> {
        match self.config.format {
            StdoutFormat::JsonLines => {
                let mut bytes = serde_json::to_vec(record)
                    .map_err(|e| FaucetError::Sink(format!("JSON serialization failed: {e}")))?;
                bytes.push(b'\n');
                Ok(bytes)
            }
            StdoutFormat::PrettyJson => {
                let mut bytes = serde_json::to_vec_pretty(record)
                    .map_err(|e| FaucetError::Sink(format!("JSON serialization failed: {e}")))?;
                bytes.push(b'\n');
                Ok(bytes)
            }
            StdoutFormat::Tsv => encode_tsv(record),
            StdoutFormat::Csv => encode_csv(record),
        }
    }
}

fn encode_tsv(record: &Value) -> Result<Vec<u8>, FaucetError> {
    let obj = record.as_object().ok_or_else(|| {
        FaucetError::Sink("Tsv format requires each record to be a JSON object".into())
    })?;
    let mut keys: Vec<&String> = obj.keys().collect();
    keys.sort();
    let mut line = String::new();
    for (i, key) in keys.iter().enumerate() {
        if i > 0 {
            line.push('\t');
        }
        let value = &obj[*key];
        line.push_str(&tsv_cell(value)?);
    }
    line.push('\n');
    Ok(line.into_bytes())
}

fn tsv_cell(value: &Value) -> Result<String, FaucetError> {
    Ok(match value {
        // Render strings without JSON quoting, but neutralise control chars
        // that would corrupt the TSV layout.
        Value::String(s) => s.replace(['\t', '\n', '\r'], " "),
        Value::Null => String::new(),
        Value::Bool(_) | Value::Number(_) => value.to_string(),
        Value::Array(_) | Value::Object(_) => serde_json::to_string(value)
            .map_err(|e| FaucetError::Sink(format!("JSON serialization failed: {e}")))?,
    })
}

/// Encode one record as an RFC-4180 CSV line. Same column resolution as
/// [`encode_tsv`] (keys sorted, no header row), but the `csv` crate handles
/// quoting/escaping so values with commas, quotes, or newlines round-trip.
/// The line terminator is `\n` to match the sink's other formats.
fn encode_csv(record: &Value) -> Result<Vec<u8>, FaucetError> {
    let obj = record.as_object().ok_or_else(|| {
        FaucetError::Sink("Csv format requires each record to be a JSON object".into())
    })?;
    let mut keys: Vec<&String> = obj.keys().collect();
    keys.sort();
    let mut wtr = csv::WriterBuilder::new()
        .terminator(csv::Terminator::Any(b'\n'))
        .from_writer(Vec::new());
    let cells: Vec<String> = keys
        .iter()
        .map(|key| csv_cell(&obj[*key]))
        .collect::<Result<_, _>>()?;
    wtr.write_record(&cells)
        .map_err(|e| FaucetError::Sink(format!("CSV serialization failed: {e}")))?;
    wtr.into_inner()
        .map_err(|e| FaucetError::Sink(format!("CSV flush failed: {e}")))
}

/// A single CSV cell. Unlike [`tsv_cell`] we do NOT neutralise control chars —
/// the `csv` writer quotes them per RFC-4180 instead.
fn csv_cell(value: &Value) -> Result<String, FaucetError> {
    Ok(match value {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        Value::Bool(_) | Value::Number(_) => value.to_string(),
        Value::Array(_) | Value::Object(_) => serde_json::to_string(value)
            .map_err(|e| FaucetError::Sink(format!("JSON serialization failed: {e}")))?,
    })
}

#[async_trait]
impl faucet_core::Sink for StdoutSink {
    fn batch_atomicity(&self) -> faucet_core::BatchAtomicity {
        self.config.batch_atomicity()
    }

    fn connector_name(&self) -> &'static str {
        "stdout"
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(StdoutSinkConfig))
            .expect("schema serialization")
    }

    fn dataset_uri(&self) -> String {
        use crate::config::StdStream;
        match self.config.destination {
            StdStream::Stdout => "stdout://".to_string(),
            StdStream::Stderr => "stderr://".to_string(),
        }
    }

    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }

        let mut state = self.state.lock().await;
        if state.closed {
            return Ok(0);
        }

        let remaining = match self.config.max_records {
            Some(max) => max.saturating_sub(state.written),
            None => usize::MAX,
        };
        if remaining == 0 {
            return Ok(0);
        }

        let take = records.len().min(remaining);
        let mut written_this_call = 0usize;
        for record in records.iter().take(take) {
            let bytes = self.encode(record)?;
            match state.writer.write_all(&bytes).await {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::BrokenPipe => {
                    state.closed = true;
                    tracing::debug!("stdout consumer closed pipe; stopping writes");
                    return Ok(written_this_call);
                }
                Err(e) => return Err(FaucetError::Sink(format!("write failed: {e}"))),
            }
            if self.config.flush_per_record {
                state
                    .writer
                    .flush()
                    .await
                    .map_err(|e| FaucetError::Sink(format!("flush failed: {e}")))?;
            }
            state.written += 1;
            written_this_call += 1;
        }
        Ok(written_this_call)
    }

    async fn flush(&self) -> Result<(), FaucetError> {
        let mut state = self.state.lock().await;
        state
            .writer
            .flush()
            .await
            .map_err(|e| FaucetError::Sink(format!("flush failed: {e}")))
    }

    /// Preflight probe for `faucet doctor`. The standard streams are always
    /// reachable (the OS hands them to every process), so there is nothing to
    /// fail on — this always passes immediately.
    async fn check(
        &self,
        _ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::{CheckReport, Probe};
        Ok(CheckReport::single(Probe::pass(
            "io",
            std::time::Duration::ZERO,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::Sink;
    use serde_json::json;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::Mutex as StdMutex;
    use std::task::{Context, Poll};
    use tokio::io::AsyncWrite;

    #[test]
    fn dataset_uri_stdout() {
        let sink = StdoutSink::new(StdoutSinkConfig::new());
        assert_eq!(sink.dataset_uri(), "stdout://");
    }

    #[test]
    fn dataset_uri_stderr() {
        use crate::config::StdStream;
        let sink = StdoutSink::new(StdoutSinkConfig::new().destination(StdStream::Stderr));
        assert_eq!(sink.dataset_uri(), "stderr://");
    }

    /// In-memory async writer that records bytes for assertions and can
    /// optionally simulate a broken-pipe error after a fixed number of writes.
    #[derive(Clone, Default)]
    struct CaptureWriter {
        inner: Arc<StdMutex<CaptureInner>>,
    }

    #[derive(Default)]
    struct CaptureInner {
        bytes: Vec<u8>,
        flushes: usize,
        fail_after: Option<usize>,
        writes: usize,
    }

    impl CaptureWriter {
        fn fail_after(n: usize) -> Self {
            let me = Self::default();
            me.inner.lock().unwrap().fail_after = Some(n);
            me
        }
        fn captured(&self) -> Vec<u8> {
            self.inner.lock().unwrap().bytes.clone()
        }
        fn flushes(&self) -> usize {
            self.inner.lock().unwrap().flushes
        }
        fn as_str(&self) -> String {
            String::from_utf8(self.captured()).unwrap()
        }
    }

    impl AsyncWrite for CaptureWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            let mut inner = self.inner.lock().unwrap();
            inner.writes += 1;
            if let Some(fail_after) = inner.fail_after
                && inner.writes > fail_after
            {
                return Poll::Ready(Err(io::Error::from(io::ErrorKind::BrokenPipe)));
            }
            inner.bytes.extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }
        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.inner.lock().unwrap().flushes += 1;
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    fn sink_with(config: StdoutSinkConfig) -> (StdoutSink, CaptureWriter) {
        let writer = CaptureWriter::default();
        let sink = StdoutSink::with_writer(config, Box::new(writer.clone()));
        (sink, writer)
    }

    #[tokio::test]
    async fn json_lines_emits_one_record_per_line() {
        let (sink, capture) = sink_with(StdoutSinkConfig::new());
        let records = vec![json!({"id": 1}), json!({"id": 2})];
        let n = sink.write_batch(&records).await.unwrap();
        assert_eq!(n, 2);
        let out = capture.as_str();
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines.len(), 2);
        assert_eq!(
            serde_json::from_str::<Value>(lines[0]).unwrap(),
            json!({"id": 1})
        );
        assert_eq!(
            serde_json::from_str::<Value>(lines[1]).unwrap(),
            json!({"id": 2})
        );
    }

    #[tokio::test]
    async fn pretty_json_indents_and_separates_records() {
        let (sink, capture) = sink_with(StdoutSinkConfig::new().format(StdoutFormat::PrettyJson));
        sink.write_batch(&[json!({"id": 1, "nested": {"k": "v"}})])
            .await
            .unwrap();
        let out = capture.as_str();
        assert!(out.contains("  \"id\": 1"));
        assert!(out.contains("  \"nested\": {"));
        assert!(out.ends_with('\n'));
    }

    #[tokio::test]
    async fn tsv_emits_keys_sorted_with_tab_separators() {
        let (sink, capture) = sink_with(StdoutSinkConfig::new().format(StdoutFormat::Tsv));
        sink.write_batch(&[json!({"name": "alice", "id": 7, "tags": ["a","b"], "active": true})])
            .await
            .unwrap();
        let out = capture.as_str();
        let line = out.lines().next().unwrap();
        let cells: Vec<&str> = line.split('\t').collect();
        // sorted: active, id, name, tags
        assert_eq!(cells, vec!["true", "7", "alice", r#"["a","b"]"#]);
    }

    #[tokio::test]
    async fn tsv_replaces_tabs_and_newlines_in_string_values() {
        let (sink, capture) = sink_with(StdoutSinkConfig::new().format(StdoutFormat::Tsv));
        sink.write_batch(&[json!({"a": "tab\there\nand-newline"})])
            .await
            .unwrap();
        let out = capture.as_str();
        let line = out.lines().next().unwrap();
        assert_eq!(line, "tab here and-newline");
    }

    #[tokio::test]
    async fn tsv_rejects_non_object_records() {
        let (sink, _capture) = sink_with(StdoutSinkConfig::new().format(StdoutFormat::Tsv));
        let result = sink.write_batch(&[json!([1, 2, 3])]).await;
        assert!(matches!(result, Err(FaucetError::Sink(_))));
    }

    #[tokio::test]
    async fn csv_emits_keys_sorted_with_comma_separators() {
        let (sink, capture) = sink_with(StdoutSinkConfig::new().format(StdoutFormat::Csv));
        sink.write_batch(&[json!({"name": "alice", "id": 7, "tags": ["a","b"], "active": true})])
            .await
            .unwrap();
        let out = capture.as_str();
        let line = out.lines().next().unwrap();
        // sorted keys: active, id, name, tags — nested array as compact JSON,
        // which the csv writer quotes because it contains a comma.
        assert_eq!(line, r#"true,7,alice,"[""a"",""b""]""#);
    }

    #[tokio::test]
    async fn csv_quotes_commas_quotes_and_newlines() {
        let (sink, capture) = sink_with(StdoutSinkConfig::new().format(StdoutFormat::Csv));
        sink.write_batch(&[json!({"a": "x,y", "b": "he said \"hi\"", "c": "line1\nline2"})])
            .await
            .unwrap();
        let out = capture.as_str();
        // Round-trip through a csv reader to prove RFC-4180 validity.
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(out.as_bytes());
        let rec = rdr.records().next().unwrap().unwrap();
        assert_eq!(&rec[0], "x,y");
        assert_eq!(&rec[1], "he said \"hi\"");
        assert_eq!(&rec[2], "line1\nline2");
    }

    #[tokio::test]
    async fn csv_line_terminator_is_lf_not_crlf() {
        let (sink, capture) = sink_with(StdoutSinkConfig::new().format(StdoutFormat::Csv));
        sink.write_batch(&[json!({"a": 1}), json!({"a": 2})])
            .await
            .unwrap();
        let out = capture.as_str();
        assert_eq!(out, "1\n2\n");
        assert!(!out.contains('\r'));
    }

    #[tokio::test]
    async fn csv_rejects_non_object_records() {
        let (sink, _capture) = sink_with(StdoutSinkConfig::new().format(StdoutFormat::Csv));
        let result = sink.write_batch(&[json!([1, 2, 3])]).await;
        assert!(matches!(result, Err(FaucetError::Sink(_))));
    }

    #[tokio::test]
    async fn csv_renders_null_as_empty_and_object_as_json() {
        let (sink, capture) = sink_with(StdoutSinkConfig::new().format(StdoutFormat::Csv));
        // keys sorted: meta (object), val (null) — exercises the Null and
        // Object/Array cell arms.
        sink.write_batch(&[json!({"val": null, "meta": {"k": 1}})])
            .await
            .unwrap();
        let out = capture.as_str();
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(out.as_bytes());
        let rec = rdr.records().next().unwrap().unwrap();
        assert_eq!(&rec[0], r#"{"k":1}"#); // meta (object → compact JSON)
        assert_eq!(&rec[1], ""); // val (null → empty)
    }

    #[tokio::test]
    async fn empty_batch_returns_zero() {
        let (sink, _capture) = sink_with(StdoutSinkConfig::new());
        let n = sink.write_batch(&[]).await.unwrap();
        assert_eq!(n, 0);
    }

    #[tokio::test]
    async fn max_records_caps_output() {
        let (sink, capture) = sink_with(StdoutSinkConfig::new().max_records(2));
        let n = sink
            .write_batch(&[json!({"id": 1}), json!({"id": 2}), json!({"id": 3})])
            .await
            .unwrap();
        assert_eq!(n, 2);
        assert_eq!(capture.as_str().lines().count(), 2);
        // Subsequent calls become no-ops.
        let n2 = sink.write_batch(&[json!({"id": 4})]).await.unwrap();
        assert_eq!(n2, 0);
        assert_eq!(capture.as_str().lines().count(), 2);
    }

    #[tokio::test]
    async fn flush_per_record_flushes_after_each() {
        let (sink, capture) = sink_with(StdoutSinkConfig::new().flush_per_record(true));
        sink.write_batch(&[json!({"id": 1}), json!({"id": 2})])
            .await
            .unwrap();
        assert_eq!(capture.flushes(), 2);
    }

    #[tokio::test]
    async fn batch_boundary_flush_only_on_explicit_flush() {
        let (sink, capture) = sink_with(StdoutSinkConfig::new());
        sink.write_batch(&[json!({"id": 1})]).await.unwrap();
        assert_eq!(capture.flushes(), 0);
        sink.flush().await.unwrap();
        assert_eq!(capture.flushes(), 1);
    }

    #[tokio::test]
    async fn broken_pipe_is_treated_as_clean_termination() {
        // Writer accepts 1 write then errors with BrokenPipe.
        let capture = CaptureWriter::fail_after(1);
        let sink = StdoutSink::with_writer(StdoutSinkConfig::new(), Box::new(capture.clone()));
        let n = sink
            .write_batch(&[json!({"id": 1}), json!({"id": 2}), json!({"id": 3})])
            .await
            .unwrap();
        assert_eq!(n, 1);
        // Further writes are no-ops because the sink is now marked closed.
        let n2 = sink.write_batch(&[json!({"id": 4})]).await.unwrap();
        assert_eq!(n2, 0);
    }

    #[tokio::test]
    async fn as_trait_object() {
        let capture = CaptureWriter::default();
        let sink: Box<dyn Sink> = Box::new(StdoutSink::with_writer(
            StdoutSinkConfig::new(),
            Box::new(capture.clone()),
        ));
        let n = sink.write_batch(&[json!({"id": 1})]).await.unwrap();
        assert_eq!(n, 1);
        assert!(capture.as_str().contains("\"id\":1"));
    }

    #[tokio::test]
    async fn config_schema_is_well_formed_object() {
        let sink = StdoutSink::new(StdoutSinkConfig::new());
        let schema = sink.config_schema();
        assert_eq!(schema["type"], "object");
        assert!(schema["properties"].is_object());
    }

    #[tokio::test]
    async fn check_always_passes() {
        let sink = StdoutSink::new(StdoutSinkConfig::new());
        let report = sink
            .check(&faucet_core::check::CheckContext::default())
            .await
            .unwrap();
        assert_eq!(report.failed_count(), 0);
        assert_eq!(report.probes.len(), 1);
        assert_eq!(report.probes[0].name, "io");
    }
}
