//! RAII timer that records a histogram sample on `Drop`. Ensures duration
//! samples are recorded even on future cancellation or panic unwind.

use metrics::{KeyName, Label, SharedString, histogram};
use std::time::Instant;

/// On `Drop`, records the elapsed time since construction into the named
/// histogram with the supplied labels. Recording on drop guarantees a sample
/// even if the surrounding future is cancelled or panics.
#[must_use = "DurationGuard must be bound to a variable; otherwise it records elapsed=0"]
pub struct DurationGuard {
    name: KeyName,
    labels: Vec<Label>,
    started_at: Instant,
    armed: bool,
}

impl DurationGuard {
    pub fn new(name: impl Into<KeyName>, labels: Vec<Label>) -> Self {
        Self {
            name: name.into(),
            labels,
            started_at: Instant::now(),
            armed: true,
        }
    }

    /// Disarm the guard so dropping it records nothing. Used when the timed
    /// span turns out not to represent real work — e.g. the terminal empty
    /// poll at the end of a source page stream, which would otherwise record a
    /// spurious ~0 sample into the page-duration histogram.
    pub fn disarm(&mut self) {
        self.armed = false;
    }

    /// Record the elapsed sample now and disarm (so a later `Drop` does not
    /// double-record). Used to close the timing window at a generator `yield`
    /// point: locals persist across a `yield`, so a drop-on-scope-exit timer
    /// would wrongly include the time the downstream consumer spends before
    /// polling the next item. Recording before the yield measures only the
    /// producer's own work (audit #321 M10).
    pub fn record_now(&mut self) {
        if !self.armed {
            return;
        }
        let elapsed = self.started_at.elapsed().as_secs_f64();
        histogram!(self.name.clone(), self.labels.clone()).record(elapsed);
        self.armed = false;
    }

    /// Build the canonical (name, pipeline, row, connector) label trio.
    pub fn with_connector(
        name: impl Into<KeyName>,
        pipeline: SharedString,
        row: SharedString,
        connector: SharedString,
    ) -> Self {
        Self::new(
            name,
            vec![
                Label::new("pipeline", pipeline),
                Label::new("row", row),
                Label::new("connector", connector),
            ],
        )
    }
}

impl Drop for DurationGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let elapsed = self.started_at.elapsed().as_secs_f64();
        histogram!(self.name.clone(), self.labels.clone()).record(elapsed);
    }
}

#[cfg(test)]
#[allow(clippy::disallowed_methods)] // tests drive time and raw clients directly
mod tests {
    use super::*;
    use metrics::SharedString;
    use metrics_util::debugging::DebugValue;
    use std::thread;
    use std::time::Duration;

    // Delegate to the single process-global recorder installed by
    // `decorator::source_tests`. All observability tests must share one
    // `OnceLock<Snapshotter>` because `metrics::set_global_recorder` can only
    // be called once per process; whoever calls it second gets an error and
    // their snapshotter sees no metrics.
    use crate::observability::decorator::source_tests::{LOCK, snapshotter};

    #[test]
    fn records_sample_on_drop() {
        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let snap = snapshotter();
        {
            let _guard = DurationGuard::with_connector(
                "test_duration_records_sample",
                SharedString::const_str("p"),
                SharedString::const_str("r"),
                SharedString::const_str("c"),
            );
            thread::sleep(Duration::from_millis(2));
        }
        let snapshot = snap.snapshot();
        let found = snapshot.into_vec().into_iter().any(|(key, _u, _d, value)| {
            key.key().name() == "test_duration_records_sample"
                && matches!(
                    value,
                    DebugValue::Histogram(samples)
                        if samples.first().map(|s| s.into_inner()).unwrap_or(0.0) > 0.0
                )
        });
        assert!(
            found,
            "expected a histogram sample > 0 on test_duration_records_sample"
        );
    }

    #[test]
    fn record_now_records_once_and_disarms() {
        // #321 M10: record_now() emits exactly one sample and the later drop
        // does not double-record.
        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let snap = snapshotter();
        {
            let mut guard = DurationGuard::with_connector(
                "test_duration_record_now",
                SharedString::const_str("p"),
                SharedString::const_str("r"),
                SharedString::const_str("c"),
            );
            thread::sleep(Duration::from_millis(2));
            guard.record_now();
        } // drop here must be a no-op (already disarmed)
        let snapshot = snap.snapshot();
        let samples = snapshot
            .into_vec()
            .into_iter()
            .find_map(|(key, _u, _d, value)| {
                (key.key().name() == "test_duration_record_now").then_some(value)
            });
        match samples {
            Some(DebugValue::Histogram(s)) => {
                assert_eq!(
                    s.len(),
                    1,
                    "record_now + drop must record exactly one sample"
                );
                assert!(s[0].into_inner() > 0.0);
            }
            other => panic!("expected one histogram sample, got {other:?}"),
        }
    }

    #[test]
    fn records_sample_when_dropped_early() {
        // Simulate cancellation: build the guard and drop it immediately
        // without doing any work. A sample is still recorded.
        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let snap = snapshotter();
        {
            let _guard = DurationGuard::with_connector(
                "test_duration_drop_early",
                SharedString::const_str("p"),
                SharedString::const_str("r"),
                SharedString::const_str("c"),
            );
        }
        let snapshot = snap.snapshot();
        let found = snapshot
            .into_vec()
            .into_iter()
            .any(|(key, _u, _d, _v)| key.key().name() == "test_duration_drop_early");
        assert!(
            found,
            "expected a histogram entry for test_duration_drop_early"
        );
    }
}
