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

use metrics::{Label, SharedString, counter, histogram};
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
}

impl RoundtripRecorder {
    /// Build a recorder for one connector instance.
    pub fn new(
        side: RoundtripSide,
        pipeline: impl Into<SharedString>,
        row: impl Into<SharedString>,
        connector: impl Into<SharedString>,
    ) -> Self {
        Self {
            side,
            base: vec![
                Label::new("pipeline", pipeline.into()),
                Label::new("row", row.into()),
                Label::new("connector", connector.into()),
            ],
        }
    }

    /// Count one round trip to the backend.
    ///
    /// `op` must come from the connector's own **closed** set — never a URL,
    /// query, status code, or host, which would make the series unbounded.
    /// A retried call is a real round trip and must be counted again.
    pub fn record(&self, op: &'static str) {
        counter!(self.side.counter_name(), self.labels_for(op)).increment(1);
    }

    /// Count one round trip and record how long it took.
    pub fn record_timed(&self, op: &'static str, elapsed: Duration) {
        let labels = self.labels_for(op);
        counter!(self.side.counter_name(), labels.clone()).increment(1);
        histogram!(self.side.histogram_name(), labels).record(elapsed.as_secs_f64());
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
