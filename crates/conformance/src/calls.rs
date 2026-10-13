//! Proof that a connector's backend calls honour the run's
//! [`CallContext`] (RFC 0013, #880): a call to a hung backend must end at the
//! context's deadline, and promptly when the run is cancelled.
//!
//! Point the connector at a backend that never answers (a wiremock server
//! with a long `set_delay`, a TCP listener that accepts and stays silent),
//! then:
//!
//! ```ignore
//! use faucet_conformance::calls::assert_source_call_deadline;
//!
//! assert_source_call_deadline(|| MySource::new(config_for(&server.uri())).unwrap()).await;
//! ```
//!
//! Each assertion builds a fresh connector, installs a context through
//! [`Source::set_call_context`] / [`Sink::set_call_context`], and checks that
//! the read or write fails within [`CALL_BOUND`] — first with a short
//! deadline, then with a cancel token fired shortly after the call starts.
//! (A sink is never handed the run's cancel token by the pipeline, but a
//! caller may cancel it directly, so both are asserted on both sides.)

use faucet_core::io::CallContext;
use faucet_core::serde_json::Value;
use faucet_core::traits::{Sink, Source};
use faucet_core::{CancellationToken, FaucetError};
use futures::StreamExt;
use std::collections::HashMap;
use std::future::Future;
use std::time::Duration;
use tokio::time::Instant;

/// The deadline (and the cancel delay) the assertions use.
pub const CALL_DEADLINE: Duration = Duration::from_millis(300);

/// How long after the deadline / cancel a call may take to end. Generous so a
/// loaded CI runner does not flake; a call that ignores the context runs until
/// its backend's delay, which a test makes far longer.
pub const CALL_BOUND: Duration = Duration::from_secs(5);

async fn ends_within<F: Future<Output = Result<(), FaucetError>>>(what: &str, f: F) -> FaucetError {
    let started = Instant::now();
    let outcome = f.await;
    let took = started.elapsed();
    assert!(
        took < CALL_BOUND,
        "{what}: the call took {took:?}; it must end within {CALL_BOUND:?} of the deadline / cancel"
    );
    match outcome {
        Err(e) => e,
        Ok(()) => panic!("{what}: the call against a hung backend succeeded"),
    }
}

#[allow(clippy::disallowed_methods)] // the harness fires the cancel itself
async fn cancel_after(token: CancellationToken) {
    tokio::time::sleep(CALL_DEADLINE).await;
    token.cancel();
}

async fn drain(source: &dyn Source) -> Result<(), FaucetError> {
    let ctx = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 0);
    while let Some(page) = pages.next().await {
        page?;
    }
    Ok(())
}

/// Assert a source's reads end at the call deadline and on cancel. `make`
/// builds a fresh source pointed at a hung backend.
pub async fn assert_source_call_deadline<S, F>(make: F)
where
    S: Source,
    F: Fn() -> S,
{
    let source = make();
    source.set_call_context(CallContext::new().with_deadline(Instant::now() + CALL_DEADLINE));
    let err = ends_within("source read with a deadline", drain(&source)).await;
    assert_timeout_like(&err, "source read with a deadline");

    let source = make();
    let token = CancellationToken::new();
    source.set_call_context(CallContext::new().with_cancel(token.clone()));
    let (outcome, ()) = futures::future::join(
        ends_within("source read after cancel", drain(&source)),
        cancel_after(token),
    )
    .await;
    assert_cancel_like(&outcome, "source read after cancel");
}

/// Assert a sink's writes of `records` end at the call deadline and on
/// cancel. `make` builds a fresh sink pointed at a hung backend.
pub async fn assert_sink_call_deadline<S, F>(make: F, records: &[Value])
where
    S: Sink,
    F: Fn() -> S,
{
    let write = |sink: S| async move {
        sink.write_batch(records).await?;
        sink.flush().await
    };
    let sink = make();
    sink.set_call_context(CallContext::new().with_deadline(Instant::now() + CALL_DEADLINE));
    let err = ends_within("sink write with a deadline", write(sink)).await;
    assert_timeout_like(&err, "sink write with a deadline");

    let sink = make();
    let token = CancellationToken::new();
    sink.set_call_context(CallContext::new().with_cancel(token.clone()));
    let (outcome, ()) = futures::future::join(
        ends_within("sink write after cancel", write(sink)),
        cancel_after(token),
    )
    .await;
    assert_cancel_like(&outcome, "sink write after cancel");
}

fn metered_recorder(
    side: faucet_core::observability::RoundtripSide,
    connector: &'static str,
) -> (
    std::sync::Arc<faucet_core::observability::RoundtripRecorder>,
    std::sync::Arc<faucet_core::UsageMeter>,
) {
    let meter = std::sync::Arc::new(faucet_core::UsageMeter::new());
    let recorder =
        faucet_core::observability::RoundtripRecorder::new(side, "conformance", "", connector)
            .with_meter(std::sync::Arc::clone(&meter));
    (std::sync::Arc::new(recorder), meter)
}

/// Assert a recorder installed through [`Source::set_roundtrip_recorder`]
/// counts the source's backend calls: `source` (pointed at a live backend) is
/// drained and at least one source round trip must be tallied.
pub async fn assert_source_records_roundtrips<S: Source>(source: &S) {
    let (recorder, meter) = metered_recorder(
        faucet_core::observability::RoundtripSide::Source,
        source.connector_name(),
    );
    source.set_roundtrip_recorder(recorder);
    drain(source)
        .await
        .expect("the source reads from its live backend");
    let n = meter.snapshot().roundtrips(faucet_core::UsageSide::Source);
    assert!(
        n > 0,
        "the installed recorder counted no source round trips"
    );
}

/// Assert a recorder installed through [`Sink::set_roundtrip_recorder`]
/// counts the sink's backend calls while `records` are written and flushed.
pub async fn assert_sink_records_roundtrips<S: Sink>(sink: &S, records: &[Value]) {
    let (recorder, meter) = metered_recorder(
        faucet_core::observability::RoundtripSide::Sink,
        sink.connector_name(),
    );
    sink.set_roundtrip_recorder(recorder);
    sink.write_batch(records)
        .await
        .expect("the sink writes to its live backend");
    sink.flush().await.expect("the sink flushes");
    let n = meter.snapshot().roundtrips(faucet_core::UsageSide::Sink);
    assert!(n > 0, "the installed recorder counted no sink round trips");
}

/// A deadline surfaces as a timeout; a connector may wrap it in its own error
/// text, so the text is accepted too.
fn assert_timeout_like(err: &FaucetError, what: &str) {
    let ok = matches!(err, FaucetError::Timeout { .. }) || err.to_string().contains("timed out");
    assert!(ok, "{what}: expected a timeout, got {err:?}");
}

fn assert_cancel_like(err: &FaucetError, what: &str) {
    let ok = matches!(err, FaucetError::Cancelled { .. }) || err.to_string().contains("cancelled");
    assert!(ok, "{what}: expected a cancellation, got {err:?}");
}

#[cfg(test)]
#[allow(clippy::disallowed_methods)] // a connector that ignores the context sleeps
mod tests {
    use super::*;
    use faucet_core::async_trait;

    /// A connector whose only backend call never answers, routed through a
    /// `CallSlot` the way a migrated connector does it.
    #[derive(Default)]
    struct Hung {
        slot: faucet_core::CallSlot,
        honour: bool,
    }

    impl Hung {
        async fn call(&self) -> Result<(), FaucetError> {
            if self.honour {
                self.slot.call("call", std::future::pending).await
            } else {
                tokio::time::sleep(Duration::from_secs(30)).await;
                Ok(())
            }
        }
    }

    #[async_trait]
    impl Source for Hung {
        async fn fetch_with_context(
            &self,
            _ctx: &HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            self.call().await?;
            Ok(vec![])
        }
        fn set_call_context(&self, ctx: CallContext) {
            self.slot.install(ctx);
        }
    }

    #[async_trait]
    impl Sink for Hung {
        async fn write_batch(&self, _records: &[Value]) -> Result<usize, FaucetError> {
            self.call().await?;
            Ok(0)
        }
        fn set_call_context(&self, ctx: CallContext) {
            self.slot.install(ctx);
        }
    }

    /// A connector whose one backend call answers at once, counted through
    /// the recorder it is handed.
    #[derive(Default)]
    struct Talks {
        slot: faucet_core::CallSlot,
        forward: bool,
    }

    impl Talks {
        async fn call(&self) -> Result<(), FaucetError> {
            self.slot.call("call", || async { Ok(()) }).await
        }
        fn install(&self, r: std::sync::Arc<faucet_core::observability::RoundtripRecorder>) {
            if self.forward {
                self.slot.install_recorder(r);
            }
        }
    }

    #[async_trait]
    impl Source for Talks {
        async fn fetch_with_context(
            &self,
            _ctx: &HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            self.call().await?;
            Ok(vec![])
        }
        fn set_roundtrip_recorder(
            &self,
            r: std::sync::Arc<faucet_core::observability::RoundtripRecorder>,
        ) {
            self.install(r);
        }
    }

    #[async_trait]
    impl Sink for Talks {
        async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
            self.call().await?;
            Ok(records.len())
        }
        fn set_roundtrip_recorder(
            &self,
            r: std::sync::Arc<faucet_core::observability::RoundtripRecorder>,
        ) {
            self.install(r);
        }
    }

    /// A sink whose writes buffer and whose flush reaches the hung backend.
    #[derive(Default)]
    struct FlushHung(faucet_core::CallSlot);

    #[async_trait]
    impl Sink for FlushHung {
        async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
            Ok(records.len())
        }
        async fn flush(&self) -> Result<(), FaucetError> {
            self.0.call("flush", std::future::pending).await
        }
        fn set_call_context(&self, ctx: CallContext) {
            self.0.install(ctx);
        }
    }

    #[tokio::test]
    async fn a_sink_hanging_in_flush_is_bounded_too() {
        assert_sink_call_deadline(FlushHung::default, &[faucet_core::json!({"a": 1})]).await;
    }

    #[tokio::test]
    async fn a_connector_counting_through_its_recorder_passes() {
        let t = Talks {
            forward: true,
            ..Talks::default()
        };
        assert_source_records_roundtrips(&t).await;
        let t = Talks {
            forward: true,
            ..Talks::default()
        };
        assert_sink_records_roundtrips(&t, &[faucet_core::json!({"a": 1})]).await;
    }

    #[tokio::test]
    #[should_panic(expected = "counted no source round trips")]
    async fn a_source_dropping_its_recorder_fails() {
        assert_source_records_roundtrips(&Talks::default()).await;
    }

    #[tokio::test]
    #[should_panic(expected = "counted no sink round trips")]
    async fn a_sink_dropping_its_recorder_fails() {
        assert_sink_records_roundtrips(&Talks::default(), &[]).await;
    }

    fn honouring() -> Hung {
        Hung {
            honour: true,
            ..Hung::default()
        }
    }

    #[tokio::test]
    async fn a_connector_on_the_call_slot_passes() {
        assert_source_call_deadline(honouring).await;
        assert_sink_call_deadline(honouring, &[faucet_core::json!({"a": 1})]).await;
    }

    #[tokio::test(start_paused = true)]
    #[should_panic(expected = "must end within")]
    async fn a_connector_that_ignores_the_context_fails() {
        assert_source_call_deadline(Hung::default).await;
    }

    #[test]
    #[should_panic(expected = "expected a timeout")]
    fn a_non_timeout_error_is_refused() {
        assert_timeout_like(&FaucetError::Source("x".into()), "w");
    }

    #[test]
    #[should_panic(expected = "expected a cancellation")]
    fn a_non_cancel_error_is_refused() {
        assert_cancel_like(&FaucetError::Source("x".into()), "w");
    }

    #[tokio::test]
    #[should_panic(expected = "succeeded")]
    async fn a_call_that_succeeds_is_refused() {
        ends_within("w", async { Ok(()) }).await;
    }
}
