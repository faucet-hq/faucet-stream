//! Protocol-agnostic upstream round-trip counting (#638).
//!
//! faucet counts records, pages, and errors, but until now had no metric for
//! **how many times a connector actually talked to its backend**. That number
//! is what drives API-quota consumption, egress cost, database load, and poll
//! overhead — and `faucet_source_pages_total` only proxies it for paged HTTP
//! sources, missing job submits, poll loops, and every non-HTTP connector.
//!
//! The counter is deliberately not HTTP-shaped: each connector decides what one
//! round trip means for it and names it with a **closed** `op` label (a SQL
//! source counts `query`, an object store `list`/`get`, Kafka `poll`, a REST
//! async job `submit`/`poll`/`fetch`/`page`).
//!
//! ## Why a recorder handle rather than a free function
//!
//! A connector's I/O sites sit far below the pipeline, which is the only place
//! that knows the `pipeline` / `row` / `connector` labels every other metric
//! carries. Handing the connector a pre-labelled handle keeps this metric
//! consistent with the rest, and — unlike a tokio task-local — the `Arc`
//! survives `tokio::spawn`, which the S3 and Parquet fan-out paths rely on.

use crate::usage::{CostSignal, UsageMeter, UsageSide};
use metrics::{Label, SharedString, counter, histogram};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

/// Which side of the pipeline a round trip belongs to. Selects the metric
/// name, so the counter reads the same way as every other source/sink pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoundtripSide {
    /// `faucet_source_roundtrips_total` / `faucet_source_roundtrip_duration_seconds`.
    Source,
    /// `faucet_sink_roundtrips_total` / `faucet_sink_roundtrip_duration_seconds`.
    Sink,
}

impl RoundtripSide {
    const fn counter_name(self) -> &'static str {
        match self {
            Self::Source => "faucet_source_roundtrips_total",
            Self::Sink => "faucet_sink_roundtrips_total",
        }
    }

    const fn histogram_name(self) -> &'static str {
        match self {
            Self::Source => "faucet_source_roundtrip_duration_seconds",
            Self::Sink => "faucet_sink_roundtrip_duration_seconds",
        }
    }
}

/// A pre-labelled handle a connector uses to count its own backend calls.
///
/// Cheap to clone (the label vec is built once at construction and cloned per
/// call, exactly like the decorators' `base_labels`). Held behind an
/// `Arc`/`OnceLock` by connectors that opt in; a connector that never calls
/// [`record`](Self::record) emits nothing at all, so instrumentation can land
/// connector by connector without any behaviour change in between.
#[derive(Debug, Clone)]
pub struct RoundtripRecorder {
    side: RoundtripSide,
    /// `pipeline` / `row` / `connector`, resolved once by the pipeline.
    base: Vec<Label>,
    /// The run's usage meter (#704), when one is attached: every round trip
    /// and cost signal is tallied there as well as emitted as a metric.
    meter: Option<Arc<UsageMeter>>,
    connector: SharedString,
}

impl RoundtripSide {
    fn usage_side(self) -> UsageSide {
        match self {
            Self::Source => UsageSide::Source,
            Self::Sink => UsageSide::Sink,
        }
    }
}

impl RoundtripRecorder {
    /// Build a recorder for one connector instance.
    pub fn new(
        side: RoundtripSide,
        pipeline: impl Into<SharedString>,
        row: impl Into<SharedString>,
        connector: impl Into<SharedString>,
    ) -> Self {
        let connector: SharedString = connector.into();
        Self {
            side,
            base: vec![
                Label::new("pipeline", pipeline.into()),
                Label::new("row", row.into()),
                Label::new("connector", connector.clone()),
            ],
            meter: None,
            connector,
        }
    }

    /// Also tally into a run's usage meter (#704).
    pub fn with_meter(mut self, meter: Arc<UsageMeter>) -> Self {
        self.meter = Some(meter);
        self
    }

    /// Report a backend-measured usage figure (#704) — BigQuery's bytes
    /// billed for a job, the payload size of a streaming insert, a
    /// warehouse's credits. Emitted as
    /// `faucet_cost_signals_total{pipeline,row,connector,kind,unit}` (the
    /// quantity rounded to a whole unit) and, when a meter is attached, kept
    /// verbatim for the run's usage record. `kind` and `unit` are a closed
    /// set per connector, documented in its README.
    pub fn signal(&self, kind: &'static str, unit: &'static str, quantity: f64) {
        let mut labels = self.base.clone();
        labels.push(Label::new("kind", SharedString::const_str(kind)));
        labels.push(Label::new("unit", SharedString::const_str(unit)));
        counter!("faucet_cost_signals_total", labels).increment(quantity.max(0.0).round() as u64);
        if let Some(m) = &self.meter {
            m.add_signal(CostSignal {
                kind: kind.to_string(),
                unit: unit.to_string(),
                quantity,
                side: self.side.usage_side(),
                connector: self.connector.to_string(),
            });
        }
    }

    /// Count one round trip to the backend.
    ///
    /// `op` must come from the connector's own **closed** set — never a URL,
    /// query, status code, or host, which would make the series unbounded.
    /// A retried call is a real round trip and must be counted again.
    pub fn record(&self, op: &'static str) {
        counter!(self.side.counter_name(), self.labels_for(op)).increment(1);
        if let Some(m) = &self.meter {
            m.add_roundtrip(self.side.usage_side(), op);
        }
    }

    /// Count one round trip and record how long it took.
    pub fn record_timed(&self, op: &'static str, elapsed: Duration) {
        let labels = self.labels_for(op);
        counter!(self.side.counter_name(), labels.clone()).increment(1);
        histogram!(self.side.histogram_name(), labels).record(elapsed.as_secs_f64());
        if let Some(m) = &self.meter {
            m.add_roundtrip(self.side.usage_side(), op);
        }
    }

    fn labels_for(&self, op: &'static str) -> Vec<Label> {
        let mut labels = self.base.clone();
        labels.push(Label::new("op", SharedString::const_str(op)));
        labels
    }

    /// The label set this recorder emits for `op` — exposed so a connector's
    /// tests can assert what they would produce without a metrics recorder
    /// installed.
    #[doc(hidden)]
    pub fn labels_for_test(&self, op: &'static str) -> Vec<(String, String)> {
        self.labels_for(op)
            .into_iter()
            .map(|l| (l.key().to_string(), l.value().to_string()))
            .collect()
    }
}

/// Register descriptions for both sides' counters and histograms. Called once
/// by `install_observability`.
pub fn describe_roundtrip_metrics() {
    metrics::describe_counter!(
        "faucet_cost_signals_total",
        "Backend-reported usage a connector measured during a run (BigQuery bytes billed, streamed payload bytes, …), by kind and unit"
    );
    metrics::describe_counter!(
        "faucet_source_roundtrips_total",
        "Calls a source made to its upstream backend, by connector-defined op"
    );
    metrics::describe_counter!(
        "faucet_sink_roundtrips_total",
        "Calls a sink made to its upstream backend, by connector-defined op"
    );
    metrics::describe_histogram!(
        "faucet_source_roundtrip_duration_seconds",
        metrics::Unit::Seconds,
        "Duration of one source round trip to its upstream backend"
    );
    metrics::describe_histogram!(
        "faucet_sink_roundtrip_duration_seconds",
        metrics::Unit::Seconds,
        "Duration of one sink round trip to its upstream backend"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metered_recorders_feed_the_usage_meter_through_a_slot() {
        let meter = Arc::new(crate::usage::UsageMeter::new());
        for side in [RoundtripSide::Source, RoundtripSide::Sink] {
            let slot = RecorderSlot::new();
            slot.record("get");
            slot.record_timed("get", Duration::from_millis(1));
            slot.signal("bytes_billed", "bytes", 10.0);
            assert!(slot.recorder().is_none());
            slot.install(Arc::new(
                RoundtripRecorder::new(side, "p", "r", "c").with_meter(meter.clone()),
            ));
            slot.record("get");
            slot.record_timed("put", Duration::from_millis(2));
            slot.signal("bytes_billed", "bytes", 1024.4);
            assert!(slot.recorder().is_some());
        }
        let snap = meter.snapshot();
        assert_eq!(snap.source_roundtrips["get"], 1);
        assert_eq!(snap.source_roundtrips["put"], 1);
        assert_eq!(snap.sink_roundtrips["get"], 1);
        assert_eq!(snap.signals.len(), 2);
        assert_eq!(snap.signals[0].kind, "bytes_billed");
        assert_eq!(snap.signals[0].connector, "c");
        assert_eq!(snap.signals[1].side, crate::usage::UsageSide::Sink);
    }

    #[test]
    fn side_selects_the_metric_names() {
        assert_eq!(
            RoundtripSide::Source.counter_name(),
            "faucet_source_roundtrips_total"
        );
        assert_eq!(
            RoundtripSide::Sink.counter_name(),
            "faucet_sink_roundtrips_total"
        );
        assert_eq!(
            RoundtripSide::Source.histogram_name(),
            "faucet_source_roundtrip_duration_seconds"
        );
        assert_eq!(
            RoundtripSide::Sink.histogram_name(),
            "faucet_sink_roundtrip_duration_seconds"
        );
    }

    #[test]
    fn labels_carry_the_universal_trio_plus_op() {
        let r = RoundtripRecorder::new(RoundtripSide::Source, "p", "rowA", "rest");
        let labels = r.labels_for_test("poll");
        assert_eq!(
            labels,
            vec![
                ("pipeline".to_string(), "p".to_string()),
                ("row".to_string(), "rowA".to_string()),
                ("connector".to_string(), "rest".to_string()),
                ("op".to_string(), "poll".to_string()),
            ],
            "the trio must match every other metric, or this one can't be joined to them"
        );
    }

    #[test]
    fn op_is_the_only_thing_that_varies_between_calls() {
        // Guards against a future refactor that rebuilds the base labels
        // per call and lets them drift.
        let r = RoundtripRecorder::new(RoundtripSide::Sink, "p", "", "s3");
        let a = r.labels_for_test("put");
        let b = r.labels_for_test("list");
        assert_eq!(a[..3], b[..3]);
        assert_eq!(a[3].1, "put");
        assert_eq!(b[3].1, "list");
    }

    #[test]
    fn record_emits_the_counter_under_an_installed_recorder() {
        use crate::observability::decorator::source_tests::{LOCK, snapshotter};
        use metrics_util::debugging::DebugValue;

        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let snap = snapshotter();
        let r = RoundtripRecorder::new(RoundtripSide::Source, "pipe", "rowA", "rest");
        r.record("submit");
        r.record("poll");
        r.record("poll");

        let counts: Vec<(String, u64)> = snap
            .snapshot()
            .into_vec()
            .into_iter()
            .filter(|(k, _, _, _)| k.key().name() == "faucet_source_roundtrips_total")
            .filter_map(|(k, _, _, v)| {
                let op = k
                    .key()
                    .labels()
                    .find(|l| l.key() == "op")?
                    .value()
                    .to_string();
                match v {
                    DebugValue::Counter(c) => Some((op, c)),
                    _ => None,
                }
            })
            .collect();

        let poll = counts.iter().find(|(op, _)| op == "poll").map(|(_, c)| *c);
        let submit = counts
            .iter()
            .find(|(op, _)| op == "submit")
            .map(|(_, c)| *c);
        assert_eq!(submit, Some(1), "one submit: {counts:?}");
        assert_eq!(
            poll,
            Some(2),
            "each poll counts — the poll loop's overhead is the whole signal: {counts:?}"
        );
    }

    #[test]
    fn recording_without_an_installed_recorder_is_a_no_op() {
        // The metrics facade discards into a no-op recorder when none is
        // installed, so a connector can always call these — there is no
        // "is observability on?" check to get wrong.
        let r = RoundtripRecorder::new(RoundtripSide::Source, "p", "r", "postgres");
        r.record("query");
        r.record_timed("query", Duration::from_millis(5));
    }

    #[test]
    fn clone_shares_the_prebuilt_labels() {
        let r = RoundtripRecorder::new(RoundtripSide::Source, "p", "r", "kafka");
        let c = r.clone();
        assert_eq!(r.labels_for_test("poll"), c.labels_for_test("poll"));
    }
}

/// A connector's slot for the recorder the pipeline installs (#638 / #704).
///
/// Connectors keep one of these in their struct and forward
/// [`set_roundtrip_recorder`](crate::Source::set_roundtrip_recorder) to
/// [`install`](Self::install); every call site then does
/// `self.roundtrips.record("get")` without checking whether a pipeline
/// installed anything. First install wins — the pipeline installs exactly
/// once per run, and a re-used connector instance keeps the labels it is
/// already counting under.
#[derive(Debug, Default)]
pub struct RecorderSlot(OnceLock<Arc<RoundtripRecorder>>);

impl RecorderSlot {
    pub const fn new() -> Self {
        Self(OnceLock::new())
    }

    /// Install the pipeline's recorder (no-op when one is already installed).
    pub fn install(&self, recorder: Arc<RoundtripRecorder>) {
        let _ = self.0.set(recorder);
    }

    /// The installed recorder, for handing to a helper that performs I/O on
    /// the connector's behalf.
    pub fn recorder(&self) -> Option<Arc<RoundtripRecorder>> {
        self.0.get().cloned()
    }

    /// Count one round trip when a recorder is installed.
    pub fn record(&self, op: &'static str) {
        if let Some(r) = self.0.get() {
            r.record(op);
        }
    }

    /// Count one timed round trip when a recorder is installed.
    pub fn record_timed(&self, op: &'static str, elapsed: Duration) {
        if let Some(r) = self.0.get() {
            r.record_timed(op, elapsed);
        }
    }

    /// Report a backend-measured usage figure when a recorder is installed.
    pub fn signal(&self, kind: &'static str, unit: &'static str, quantity: f64) {
        if let Some(r) = self.0.get() {
            r.signal(kind, unit, quantity);
        }
    }
}
