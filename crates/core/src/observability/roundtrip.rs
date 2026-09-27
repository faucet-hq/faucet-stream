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

use crate::resilience::RetryClass;
use crate::usage::{CostSignal, UsageMeter, UsageSide};
use metrics::{Label, SharedString, counter, histogram};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

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

    const fn throttled_name(self) -> &'static str {
        match self {
            Self::Source => "faucet_source_throttled_total",
            Self::Sink => "faucet_sink_throttled_total",
        }
    }

    const fn throttle_wait_name(self) -> &'static str {
        match self {
            Self::Source => "faucet_source_throttle_wait_seconds",
            Self::Sink => "faucet_sink_throttle_wait_seconds",
        }
    }

    const fn retries_name(self) -> &'static str {
        match self {
            Self::Source => "faucet_source_retries_total",
            Self::Sink => "faucet_sink_retries_total",
        }
    }
}

/// Running totals of the rate limiting one recorder observed (#734), shared by
/// every clone so the pipeline can compare the wait against the run's length.
#[derive(Debug, Default)]
pub struct ThrottleTally {
    throttled: AtomicU64,
    wait_nanos: AtomicU64,
}

impl ThrottleTally {
    /// Rate-limit responses received.
    pub fn throttled(&self) -> u64 {
        self.throttled.load(Ordering::Relaxed)
    }

    /// Time actually slept because of them.
    pub fn wait(&self) -> Duration {
        Duration::from_nanos(self.wait_nanos.load(Ordering::Relaxed))
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
    throttle: Arc<ThrottleTally>,
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
            throttle: Arc::new(ThrottleTally::default()),
        }
    }

    /// The rate-limit totals this recorder has observed so far.
    pub fn throttle_tally(&self) -> Arc<ThrottleTally> {
        Arc::clone(&self.throttle)
    }

    /// Count one rate-limit response (HTTP 429, a `RateLimited` error, a
    /// backend's throttling code) — every one received, whether or not it is
    /// retried. Emits `faucet_source_throttled_total`.
    pub fn throttled(&self) {
        counter!(self.side.throttled_name(), self.base.clone()).increment(1);
        self.throttle.throttled.fetch_add(1, Ordering::Relaxed);
        if let Some(m) = self.metered_source() {
            m.add_throttled();
        }
    }

    /// Record time actually slept because of a rate limit. Pass the measured
    /// sleep, never the server's `Retry-After` value; [`ThrottleWait`] measures
    /// it for you, including a sleep cut short by cancellation.
    pub fn throttle_wait(&self, slept: Duration) {
        histogram!(self.side.throttle_wait_name(), self.base.clone()).record(slept.as_secs_f64());
        self.throttle.wait_nanos.fetch_add(
            u64::try_from(slept.as_nanos()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        if let Some(m) = self.metered_source() {
            m.add_throttle_wait(slept);
        }
    }

    /// Count one retry the connector is about to make, by class. Emits
    /// `faucet_source_retries_total{class}`.
    pub fn retry(&self, class: RetryClass) {
        let mut labels = self.base.clone();
        labels.push(Label::new("class", SharedString::const_str(class.as_str())));
        counter!(self.side.retries_name(), labels).increment(1);
        if let Some(m) = self.metered_source() {
            m.add_source_retry(class.as_str());
        }
    }

    fn metered_source(&self) -> Option<&Arc<UsageMeter>> {
        match self.side {
            RoundtripSide::Source => self.meter.as_ref(),
            RoundtripSide::Sink => None,
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

    /// Count records whose incremental replication key was missing or `null`
    /// (#747). Emits `faucet_source_replication_key_missing_total`.
    pub fn replication_key_missing(&self, n: u64) {
        if n > 0 {
            counter!(
                "faucet_source_replication_key_missing_total",
                self.base.clone()
            )
            .increment(n);
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

/// Measures one rate-limit sleep (#734): hold it across the sleep and the
/// elapsed time is recorded when it drops — after the sleep completes, when a
/// cancellation branch wins a `select!`, or when the future is dropped mid-sleep
/// — so the recorded wait is always what was actually slept.
#[derive(Debug)]
#[must_use = "the wait is recorded when the guard drops"]
pub struct ThrottleWait {
    recorder: Option<Arc<RoundtripRecorder>>,
    start: Instant,
}

impl ThrottleWait {
    /// Start timing a sleep on behalf of `recorder` (a no-op guard for `None`).
    pub fn start(recorder: Option<Arc<RoundtripRecorder>>) -> Self {
        Self {
            recorder,
            start: Instant::now(),
        }
    }
}

impl Drop for ThrottleWait {
    fn drop(&mut self) {
        if let Some(r) = &self.recorder {
            r.throttle_wait(self.start.elapsed());
        }
    }
}

/// Sleep `wait` on behalf of a rate limit, recording the time actually slept
/// (#734). Returns `false` when `cancel` fired first; the partial wait is still
/// recorded.
pub async fn throttle_sleep(
    recorder: Option<Arc<RoundtripRecorder>>,
    wait: Duration,
    cancel: Option<&tokio_util::sync::CancellationToken>,
) -> bool {
    let _timer = ThrottleWait::start(recorder);
    match cancel {
        Some(token) => {
            tokio::select! {
                biased;
                _ = token.cancelled() => false,
                _ = tokio::time::sleep(wait) => true,
            }
        }
        None => {
            tokio::time::sleep(wait).await;
            true
        }
    }
}

/// The one-line warning a run logs when rate-limit waits took more than a
/// tenth of it (#734), or `None` when they did not.
pub fn throttle_warning(throttled: u64, wait: Duration, run: Duration) -> Option<String> {
    if wait.is_zero() || run.is_zero() || wait.as_secs_f64() * 10.0 <= run.as_secs_f64() {
        return None;
    }
    let pct = (wait.as_secs_f64() / run.as_secs_f64() * 100.0).min(100.0);
    Some(format!(
        "source spent {:.1}s of a {:.1}s run ({pct:.0}%) waiting on rate limits \
         ({throttled} throttled responses); lower concurrency, stagger schedules or raise the quota",
        wait.as_secs_f64(),
        run.as_secs_f64(),
    ))
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
    metrics::describe_counter!(
        "faucet_source_throttled_total",
        "Rate-limit responses (HTTP 429 and equivalents) a source received"
    );
    metrics::describe_histogram!(
        "faucet_source_throttle_wait_seconds",
        metrics::Unit::Seconds,
        "Time a source actually slept because of one rate-limit response"
    );
    metrics::describe_counter!(
        "faucet_source_replication_key_missing_total",
        "Records an incremental source received without its replication key (kept, dropped or failed per on_missing_key)"
    );
    metrics::describe_counter!(
        "faucet_source_retries_total",
        "Retries a source made against its upstream backend, by retry class"
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
    fn replication_key_missing_counts_through_the_slot() {
        use crate::observability::decorator::source_tests::{LOCK, snapshotter};
        use metrics_util::debugging::DebugValue;

        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let snap = snapshotter();
        let slot = RecorderSlot::new();
        slot.replication_key_missing(4);
        slot.install(Arc::new(RoundtripRecorder::new(
            RoundtripSide::Source,
            "pipe",
            "rowK",
            "rest",
        )));
        slot.replication_key_missing(0);
        slot.replication_key_missing(3);
        let total: u64 = snap
            .snapshot()
            .into_vec()
            .into_iter()
            .filter(|(k, _, _, _)| {
                k.key().name() == "faucet_source_replication_key_missing_total"
                    && k.key().labels().any(|l| l.value() == "rowK")
            })
            .map(|(_, _, _, v)| match v {
                DebugValue::Counter(c) => c,
                _ => 0,
            })
            .sum();
        assert_eq!(total, 3);
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
    fn throttling_feeds_the_tally_the_meter_and_the_metrics() {
        use crate::observability::decorator::source_tests::{LOCK, snapshotter};
        use metrics_util::debugging::DebugValue;

        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let snap = snapshotter();
        let meter = Arc::new(UsageMeter::new());
        let r = RoundtripRecorder::new(RoundtripSide::Source, "pipe", "rowT", "rest")
            .with_meter(meter.clone());
        let tally = r.throttle_tally();
        r.throttled();
        r.throttled();
        r.throttle_wait(Duration::from_millis(300));
        r.retry(RetryClass::RateLimited);
        r.retry(RetryClass::Http5xx);
        assert_eq!(tally.throttled(), 2);
        assert_eq!(tally.wait(), Duration::from_millis(300));
        let usage = meter.snapshot();
        assert_eq!(usage.throttled, 2);
        assert!((usage.throttle_wait_secs - 0.3).abs() < 1e-9);
        assert_eq!(usage.source_retries["rate_limited"], 1);
        assert_eq!(usage.source_retries["http_5xx"], 1);

        let entries: Vec<_> = snap
            .snapshot()
            .into_vec()
            .into_iter()
            .filter(|(k, _, _, _)| k.key().labels().any(|l| l.value() == "rowT"))
            .collect();
        let counter = |name: &str, class: Option<&str>| {
            entries.iter().find_map(|(k, _, _, v)| {
                let class_ok = class.is_none_or(|c| {
                    k.key()
                        .labels()
                        .any(|l| l.key() == "class" && l.value() == c)
                });
                match v {
                    DebugValue::Counter(n) if k.key().name() == name && class_ok => Some(*n),
                    _ => None,
                }
            })
        };
        assert_eq!(counter("faucet_source_throttled_total", None), Some(2));
        assert_eq!(
            counter("faucet_source_retries_total", Some("rate_limited")),
            Some(1)
        );
        assert_eq!(
            counter("faucet_source_retries_total", Some("http_5xx")),
            Some(1)
        );
        assert!(entries.iter().any(|(k, _, _, v)| {
            k.key().name() == "faucet_source_throttle_wait_seconds"
                && matches!(v, DebugValue::Histogram(h) if h.len() == 1)
        }));
    }

    #[test]
    fn sink_side_throttling_emits_metrics_but_stays_off_the_usage_record() {
        let meter = Arc::new(UsageMeter::new());
        let r =
            RoundtripRecorder::new(RoundtripSide::Sink, "p", "r", "http").with_meter(meter.clone());
        r.throttled();
        r.throttle_wait(Duration::from_millis(5));
        r.retry(RetryClass::Timeout);
        assert_eq!(r.throttle_tally().throttled(), 1);
        let usage = meter.snapshot();
        assert_eq!(usage.throttled, 0);
        assert!(usage.source_retries.is_empty());
        assert_eq!(
            RoundtripSide::Sink.throttled_name(),
            "faucet_sink_throttled_total"
        );
        assert_eq!(
            RoundtripSide::Sink.throttle_wait_name(),
            "faucet_sink_throttle_wait_seconds"
        );
        assert_eq!(
            RoundtripSide::Sink.retries_name(),
            "faucet_sink_retries_total"
        );
    }

    #[tokio::test]
    async fn throttle_sleep_records_the_time_actually_slept() {
        let r = Arc::new(RoundtripRecorder::new(
            RoundtripSide::Source,
            "p",
            "r",
            "rest",
        ));
        let tally = r.throttle_tally();
        assert!(throttle_sleep(Some(r.clone()), Duration::from_millis(40), None).await);
        let full = tally.wait();
        assert!(full >= Duration::from_millis(40), "{full:?}");

        let token = tokio_util::sync::CancellationToken::new();
        let t = token.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            t.cancel();
        });
        let completed =
            throttle_sleep(Some(r.clone()), Duration::from_secs(30), Some(&token)).await;
        assert!(!completed, "cancellation wins");
        let partial = tally.wait() - full;
        assert!(
            partial >= Duration::from_millis(25) && partial < Duration::from_secs(5),
            "the partial wait is recorded, not the requested 30 s: {partial:?}"
        );

        let token = tokio_util::sync::CancellationToken::new();
        assert!(throttle_sleep(None, Duration::from_millis(1), Some(&token)).await);
    }

    #[tokio::test]
    async fn a_dropped_sleep_still_records_its_partial_wait() {
        let slot = RecorderSlot::new();
        slot.throttled();
        slot.retry(RetryClass::Connection);
        drop(slot.throttle_wait_timer());
        let r = Arc::new(RoundtripRecorder::new(
            RoundtripSide::Source,
            "p",
            "r",
            "rest",
        ));
        slot.install(r.clone());
        slot.throttled();
        slot.retry(RetryClass::Connection);
        let tally = r.throttle_tally();
        let sleeping = async {
            let _t = slot.throttle_wait_timer();
            tokio::time::sleep(Duration::from_secs(30)).await;
        };
        let _ = tokio::time::timeout(Duration::from_millis(30), sleeping).await;
        assert_eq!(tally.throttled(), 1);
        assert!(
            tally.wait() >= Duration::from_millis(25),
            "{:?}",
            tally.wait()
        );
        assert!(tally.wait() < Duration::from_secs(5));
    }

    #[test]
    fn warns_only_when_waiting_exceeds_a_tenth_of_the_run() {
        assert_eq!(
            throttle_warning(0, Duration::ZERO, Duration::from_secs(10)),
            None
        );
        assert_eq!(
            throttle_warning(3, Duration::from_secs(1), Duration::from_secs(10)),
            None,
            "exactly 10% does not warn"
        );
        assert_eq!(
            throttle_warning(3, Duration::from_secs(1), Duration::ZERO),
            None
        );
        let msg = throttle_warning(312, Duration::from_secs(2460), Duration::from_secs(3600))
            .expect("68% warns");
        assert!(msg.contains("2460.0s of a 3600.0s run (68%)"), "{msg}");
        assert!(msg.contains("312 throttled responses"), "{msg}");
        let capped = throttle_warning(1, Duration::from_secs(20), Duration::from_secs(10)).unwrap();
        assert!(capped.contains("(100%)"), "{capped}");
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

    /// Count records missing their replication key when a recorder is installed.
    pub fn replication_key_missing(&self, n: u64) {
        if let Some(r) = self.0.get() {
            r.replication_key_missing(n);
        }
    }

    /// Count one rate-limit response when a recorder is installed.
    pub fn throttled(&self) {
        if let Some(r) = self.0.get() {
            r.throttled();
        }
    }

    /// Count one retry of `class` when a recorder is installed.
    pub fn retry(&self, class: RetryClass) {
        if let Some(r) = self.0.get() {
            r.retry(class);
        }
    }

    /// Start timing a rate-limit sleep ([`ThrottleWait`]).
    pub fn throttle_wait_timer(&self) -> ThrottleWait {
        ThrottleWait::start(self.recorder())
    }

    /// Report a backend-measured usage figure when a recorder is installed.
    pub fn signal(&self, kind: &'static str, unit: &'static str, quantity: f64) {
        if let Some(r) = self.0.get() {
            r.signal(kind, unit, quantity);
        }
    }
}
