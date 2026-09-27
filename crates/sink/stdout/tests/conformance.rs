//! `faucet-conformance` battery against the real stdout sink.
//!
//! The stdout/stderr sink is append-only — it advertises no idempotency
//! mechanism, so the battery exercises the **honest branch**:
//! - check 1 `assert_config_schema_valid_value` (value form, for sinks);
//! - check 5 `assert_capabilities_truthful` — Append works, and the sink does
//!   *not* claim idempotent/keyed dedup (so the pipeline correctly refuses
//!   `delivery: exactly_once` for it).
//!
//! Counting durable output is done by pointing the sink at an in-memory
//! capture writer (via [`StdoutSink::with_writer`]) instead of the real OS
//! stream, so the test runs fully offline with no dependency on stdout/stderr.
use faucet_conformance::assert_config_schema_valid_value;
use faucet_core::Sink;
use faucet_sink_stdout::{StdoutSink, StdoutSinkConfig};
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use tokio::io::AsyncWrite;

/// In-memory async writer that records every byte, shareable across the sink
/// and the count closure. In the default JSON-Lines format the sink emits one
/// newline-terminated line per record, so the durable record count is the
/// number of non-empty captured lines.
#[derive(Clone, Default)]
struct CaptureWriter {
    bytes: Arc<Mutex<Vec<u8>>>,
}

impl CaptureWriter {
    fn line_count(&self) -> usize {
        String::from_utf8(self.bytes.lock().unwrap().clone())
            .unwrap()
            .lines()
            .filter(|l| !l.trim().is_empty())
            .count()
    }
}

impl AsyncWrite for CaptureWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.bytes.lock().unwrap().extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[test]
fn conformance_config_schema_valid() {
    let schema =
        serde_json::to_value(schemars::schema_for!(faucet_sink_stdout::StdoutSinkConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "stdout");
}

// ── Check 10: connector_name is non-empty ─────────────────────────────────────
#[test]
fn conformance_connector_name_nonempty() {
    let sink = StdoutSink::new(StdoutSinkConfig::new());
    faucet_conformance::assert_batch_atomicity_declared(&sink);
    faucet_conformance::assert_connector_name_nonempty_value(
        sink.connector_name(),
        sink.connector_name(),
    );
    assert_eq!(sink.connector_name(), "stdout");
}

// ── Check 11: preflight check() is well-formed ────────────────────────────────
#[tokio::test]
async fn conformance_preflight_check_wellformed() {
    // The stdout sink's probe always passes; the check must return Ok(report)
    // with a well-formed probe.
    let sink = StdoutSink::new(StdoutSinkConfig::new());
    faucet_conformance::assert_sink_preflight_check_wellformed(
        &sink,
        &faucet_core::check::CheckContext::default(),
    )
    .await;
}

#[tokio::test]
async fn conformance_capabilities_truthful() {
    let capture = CaptureWriter::default();
    let sink = StdoutSink::with_writer(StdoutSinkConfig::new(), Box::new(capture.clone()));
    let sink_ref = &sink;
    let capture_ref = &capture;

    faucet_conformance::assert_capabilities_truthful(&sink, || async move {
        // The sink buffers into the capture writer; flush so the durable line
        // count reflects the writes.
        sink_ref.flush().await.expect("flush");
        capture_ref.line_count()
    })
    .await;

    // The honest branch must have left the append-only sink non-idempotent.
    assert!(!sink.supports_idempotent_writes());
    assert!(!sink.dedups_by_key());
}
