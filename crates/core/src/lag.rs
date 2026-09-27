//! Source lag (#733): how far a CDC / streaming pipeline is behind its source.
//!
//! A source that has a notion of "the head" (a WAL position, a binlog end, a
//! partition high watermark) reports the distance from where it has read to
//! that head through [`Source::lag`](crate::Source::lag). The pipeline polls it
//! at page boundaries and exports `faucet_source_lag_bytes`,
//! `faucet_source_lag_events` and `faucet_source_lag_seconds`.

use metrics::{Label, gauge};
use serde::{Deserialize, Serialize};

/// Distance from the source's head, in whichever units the source can measure.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct SourceLag {
    /// Bytes of unread change log (Postgres WAL, MySQL binlog).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytes: Option<u64>,
    /// Unread events / messages (Kafka offsets, SQL Server change rows).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub events: Option<u64>,
    /// Age of the oldest unread change, in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seconds: Option<f64>,
}

impl SourceLag {
    /// Lag measured in bytes.
    pub fn bytes(n: u64) -> Self {
        Self {
            bytes: Some(n),
            ..Default::default()
        }
    }

    /// Lag measured in events.
    pub fn events(n: u64) -> Self {
        Self {
            events: Some(n),
            ..Default::default()
        }
    }

    /// Lag measured in seconds (negative clock skew clamps to zero).
    pub fn seconds(s: f64) -> Self {
        Self {
            seconds: Some(if s.is_finite() { s.max(0.0) } else { 0.0 }),
            ..Default::default()
        }
    }

    /// Whether no unit carries a value.
    pub fn is_empty(&self) -> bool {
        self.bytes.is_none() && self.events.is_none() && self.seconds.is_none()
    }

    /// A compact human rendering: `412 MiB`, `1,204 events`, `3m 20s`, joined.
    pub fn human(&self) -> String {
        let mut parts = Vec::new();
        if let Some(b) = self.bytes {
            parts.push(human_bytes(b));
        }
        if let Some(e) = self.events {
            parts.push(format!("{e} event{}", if e == 1 { "" } else { "s" }));
        }
        if let Some(s) = self.seconds {
            parts.push(human_seconds(s));
        }
        parts.join(" · ")
    }
}

fn human_bytes(b: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = b as f64;
    let mut i = 0;
    while v >= 1024.0 && i + 1 < UNITS.len() {
        v /= 1024.0;
        i += 1;
    }
    if i == 0 {
        format!("{b} B")
    } else {
        format!("{v:.0} {}", UNITS[i])
    }
}

fn human_seconds(s: f64) -> String {
    let s = s.max(0.0);
    if s < 60.0 {
        return format!("{s:.0}s");
    }
    let total = s as u64;
    let (h, m, sec) = (total / 3600, (total % 3600) / 60, total % 60);
    if h > 0 {
        format!("{h}h {m}m")
    } else {
        format!("{m}m {sec}s")
    }
}

/// The latest lag sample of one run, shared with the caller (#733). Attach
/// with [`Pipeline::with_lag_observer`](crate::Pipeline::with_lag_observer).
#[derive(Debug, Default)]
pub struct LagObserver {
    last: std::sync::Mutex<Option<SourceLag>>,
}

impl LagObserver {
    /// An observer with no sample yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// Replace the latest sample.
    pub fn record(&self, lag: SourceLag) {
        if let Ok(mut g) = self.last.lock() {
            *g = Some(lag);
        }
    }

    /// The latest sample, if the source reported one.
    pub fn last(&self) -> Option<SourceLag> {
        self.last.lock().ok().and_then(|g| *g)
    }
}

/// Export one lag sample as the `faucet_source_lag_*` gauges.
pub fn record_lag_gauges(labels: &[Label], lag: &SourceLag) {
    if let Some(b) = lag.bytes {
        gauge!("faucet_source_lag_bytes", labels.to_vec()).set(b as f64);
    }
    if let Some(e) = lag.events {
        gauge!("faucet_source_lag_events", labels.to_vec()).set(e as f64);
    }
    if let Some(s) = lag.seconds {
        gauge!("faucet_source_lag_seconds", labels.to_vec()).set(s);
    }
}

/// How often the pipeline asks a source for its lag while pages flow.
pub const LAG_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(15);

/// Polls [`Source::lag`](crate::Source::lag) for one run: on the first page,
/// then at most every [`LAG_POLL_INTERVAL`], and once more when the run ends.
/// A failing query is logged once and never fails the run.
pub(crate) struct LagPoller<'a> {
    source: &'a dyn crate::Source,
    labels: Vec<Label>,
    observer: Option<std::sync::Arc<LagObserver>>,
    interval: std::time::Duration,
    last_poll: std::sync::Mutex<Option<std::time::Instant>>,
    warned: std::sync::atomic::AtomicBool,
}

impl<'a> LagPoller<'a> {
    pub(crate) fn new(
        source: &'a dyn crate::Source,
        pipeline: &str,
        row: &str,
        observer: Option<std::sync::Arc<LagObserver>>,
    ) -> Self {
        use metrics::SharedString;
        Self {
            labels: vec![
                Label::new("pipeline", SharedString::from(pipeline.to_string())),
                Label::new("row", SharedString::from(row.to_string())),
                Label::new(
                    "connector",
                    SharedString::from(source.connector_name().to_string()),
                ),
            ],
            source,
            observer,
            interval: LAG_POLL_INTERVAL,
            last_poll: std::sync::Mutex::new(None),
            warned: std::sync::atomic::AtomicBool::new(false),
        }
    }

    #[cfg(test)]
    fn with_interval(mut self, interval: std::time::Duration) -> Self {
        self.interval = interval;
        self
    }

    /// Poll unless the last poll was under the interval ago (`force` skips
    /// the throttle).
    pub(crate) async fn poll(&self, force: bool) {
        let now = std::time::Instant::now();
        {
            let Ok(mut last) = self.last_poll.lock() else {
                return;
            };
            if !force && last.is_some_and(|t| now.duration_since(t) < self.interval) {
                return;
            }
            *last = Some(now);
        }
        match self.source.lag().await {
            Ok(Some(lag)) if !lag.is_empty() => {
                record_lag_gauges(&self.labels, &lag);
                if let Some(o) = &self.observer {
                    o.record(lag);
                }
            }
            Ok(_) => {}
            Err(e) => {
                if !self.warned.swap(true, std::sync::atomic::Ordering::Relaxed) {
                    tracing::warn!(
                        connector = self.source.connector_name(),
                        error = %e,
                        "source lag query failed; lag is not reported for this run (logged once)"
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constructors_and_rendering() {
        assert_eq!(SourceLag::bytes(10).bytes, Some(10));
        assert_eq!(SourceLag::events(3).events, Some(3));
        assert_eq!(SourceLag::seconds(-4.0).seconds, Some(0.0));
        assert_eq!(SourceLag::seconds(f64::NAN).seconds, Some(0.0));
        assert!(SourceLag::default().is_empty());
        assert!(!SourceLag::bytes(0).is_empty());
        assert_eq!(SourceLag::bytes(512).human(), "512 B");
        assert_eq!(SourceLag::bytes(412 * 1024 * 1024).human(), "412 MiB");
        assert_eq!(SourceLag::events(1).human(), "1 event");
        assert_eq!(SourceLag::events(5).human(), "5 events");
        assert_eq!(SourceLag::seconds(42.0).human(), "42s");
        assert_eq!(SourceLag::seconds(200.0).human(), "3m 20s");
        assert_eq!(SourceLag::seconds(7300.0).human(), "2h 1m");
        let both = SourceLag {
            bytes: Some(2048),
            seconds: Some(5.0),
            events: None,
        };
        assert_eq!(both.human(), "2 KiB · 5s");
    }

    struct LagSource {
        calls: std::sync::atomic::AtomicUsize,
        result: Result<Option<SourceLag>, ()>,
    }

    #[async_trait::async_trait]
    impl crate::Source for LagSource {
        async fn fetch_with_context(
            &self,
            _: &std::collections::HashMap<String, serde_json::Value>,
        ) -> Result<Vec<serde_json::Value>, crate::FaucetError> {
            Ok(Vec::new())
        }
        async fn lag(&self) -> Result<Option<SourceLag>, crate::FaucetError> {
            self.calls
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.result
                .map_err(|_| crate::FaucetError::Source("lag query failed".into()))
        }
    }

    fn lag_source(result: Result<Option<SourceLag>, ()>) -> LagSource {
        LagSource {
            calls: std::sync::atomic::AtomicUsize::new(0),
            result,
        }
    }

    #[tokio::test]
    async fn poller_throttles_records_and_forces() {
        let src = lag_source(Ok(Some(SourceLag::bytes(7))));
        let obs = std::sync::Arc::new(LagObserver::new());
        assert_eq!(obs.last(), None);
        let p = LagPoller::new(&src, "p", "r", Some(std::sync::Arc::clone(&obs)))
            .with_interval(std::time::Duration::from_secs(3600));
        p.poll(false).await;
        p.poll(false).await;
        assert_eq!(src.calls.load(std::sync::atomic::Ordering::Relaxed), 1);
        p.poll(true).await;
        assert_eq!(src.calls.load(std::sync::atomic::Ordering::Relaxed), 2);
        assert_eq!(obs.last(), Some(SourceLag::bytes(7)));
    }

    #[tokio::test]
    async fn poller_ignores_empty_and_failing_lag() {
        let obs = std::sync::Arc::new(LagObserver::new());
        let empty = lag_source(Ok(Some(SourceLag::default())));
        LagPoller::new(&empty, "p", "r", Some(std::sync::Arc::clone(&obs)))
            .poll(true)
            .await;
        let none = lag_source(Ok(None));
        LagPoller::new(&none, "p", "r", Some(std::sync::Arc::clone(&obs)))
            .poll(true)
            .await;
        let failing = lag_source(Err(()));
        let p = LagPoller::new(&failing, "p", "r", Some(std::sync::Arc::clone(&obs)));
        p.poll(true).await;
        p.poll(true).await;
        assert_eq!(failing.calls.load(std::sync::atomic::Ordering::Relaxed), 2);
        assert_eq!(obs.last(), None);
        LagPoller::new(&none, "p", "r", None).poll(true).await;
        use crate::Source;
        assert!(
            none.fetch_with_context(&Default::default())
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn gauges_record_without_a_recorder() {
        record_lag_gauges(
            &[],
            &SourceLag {
                bytes: Some(1),
                events: Some(2),
                seconds: Some(3.0),
            },
        );
        record_lag_gauges(&[], &SourceLag::default());
    }
}
