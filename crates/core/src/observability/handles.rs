//! Metric handles kept between calls, so a per-page emit does not clone a
//! label vector and look the series up in the recorder each time.

use metrics::{Counter, Gauge, Histogram, Label, counter, gauge, histogram};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// A handle unused for this long is looked up again before its next use. An
/// exporter with an idle timeout drops a series nothing wrote to for that
/// long, and a write through a handle to a dropped series would be lost; a
/// handle used within the last second is always still registered.
const FRESH: Duration = Duration::from_secs(1);

pub(crate) trait Handle: Sized {
    fn resolve(name: &'static str, labels: Vec<Label>) -> Self;
}

impl Handle for Counter {
    fn resolve(name: &'static str, labels: Vec<Label>) -> Self {
        counter!(name, labels)
    }
}

impl Handle for Gauge {
    fn resolve(name: &'static str, labels: Vec<Label>) -> Self {
        gauge!(name, labels)
    }
}

impl Handle for Histogram {
    fn resolve(name: &'static str, labels: Vec<Label>) -> Self {
        histogram!(name, labels)
    }
}

/// One series (`name` + `labels`) whose handle is resolved on first use, in
/// the recorder current at that moment, and reused while it stays in use.
pub(crate) struct Cached<H> {
    name: &'static str,
    labels: Vec<Label>,
    slot: Mutex<Option<(H, Instant)>>,
}

impl<H: Handle> Cached<H> {
    pub(crate) fn new(name: &'static str, labels: Vec<Label>) -> Self {
        Self {
            name,
            labels,
            slot: Mutex::new(None),
        }
    }

    pub(crate) fn with<R>(&self, f: impl FnOnce(&H) -> R) -> R {
        self.with_at(Instant::now(), f)
    }

    fn with_at<R>(&self, now: Instant, f: impl FnOnce(&H) -> R) -> R {
        let mut slot = self.slot.lock().unwrap_or_else(|e| e.into_inner());
        match slot.as_mut() {
            Some((handle, used)) if now.saturating_duration_since(*used) < FRESH => {
                *used = now;
                f(handle)
            }
            _ => {
                let (handle, _) = slot.insert((H::resolve(self.name, self.labels.clone()), now));
                f(handle)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    fn value(snapshot: metrics_util::debugging::Snapshot, name: &str) -> Option<DebugValue> {
        snapshot
            .into_vec()
            .into_iter()
            .find(|(key, ..)| key.key().name() == name)
            .map(|(_, _, _, v)| v)
    }

    #[test]
    fn a_cached_handle_writes_the_same_series_as_the_macro() {
        let recorder = DebuggingRecorder::new();
        let snap = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            let labels = vec![Label::new("pipeline", "p"), Label::new("row", "r")];
            let c = Cached::<Counter>::new("cached_total", labels.clone());
            c.with(|c| c.increment(2));
            c.with(|c| c.increment(3));
            counter!("cached_total", labels.clone()).increment(1);
            let g = Cached::<Gauge>::new("cached_gauge", labels.clone());
            g.with(|g| g.increment(2.0));
            g.with(|g| g.decrement(1.0));
            let h = Cached::<Histogram>::new("cached_seconds", labels.clone());
            h.with(|h| h.record(0.5));
            h.with(|h| h.record(1.5));
        });
        let all = snap.snapshot().into_vec();
        assert_eq!(all.len(), 3, "one series per name: {all:?}");
        for (key, ..) in &all {
            let labels: Vec<_> = key.key().labels().map(|l| (l.key(), l.value())).collect();
            assert_eq!(labels, vec![("pipeline", "p"), ("row", "r")]);
        }
        let find = |name: &str| {
            all.iter()
                .find(|(key, ..)| key.key().name() == name)
                .map(|(_, _, _, v)| v)
        };
        assert_eq!(find("cached_total"), Some(&DebugValue::Counter(6)));
        assert_eq!(find("cached_gauge"), Some(&DebugValue::Gauge(1.0.into())));
        assert_eq!(
            find("cached_seconds"),
            Some(&DebugValue::Histogram(vec![0.5.into(), 1.5.into()]))
        );
    }

    #[test]
    fn a_handle_idle_past_the_window_is_looked_up_again() {
        let first = DebuggingRecorder::new();
        let second = DebuggingRecorder::new();
        let (first_snap, second_snap) = (first.snapshotter(), second.snapshotter());
        let c = Cached::<Counter>::new("rebind_total", Vec::new());
        let start = Instant::now();
        metrics::with_local_recorder(&first, || c.with_at(start, |c| c.increment(1)));
        metrics::with_local_recorder(&second, || {
            c.with_at(start + FRESH / 2, |c| c.increment(10));
            c.with_at(start + FRESH / 2 + FRESH, |c| c.increment(100));
            c.with_at(start + FRESH * 2, |c| c.increment(1000));
        });
        assert_eq!(
            value(first_snap.snapshot(), "rebind_total"),
            Some(DebugValue::Counter(11)),
            "a handle in use keeps its series"
        );
        assert_eq!(
            value(second_snap.snapshot(), "rebind_total"),
            Some(DebugValue::Counter(1100)),
            "an idle handle is resolved again"
        );
    }
}
