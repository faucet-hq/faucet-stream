//! Data-freshness & volume SLA monitoring (#202).
//!
//! The top-level `sla:` block declares freshness/volume expectations for a
//! pipeline; the executor evaluates them after every **root** invocation
//! (`faucet run`, `schedule`, `serve`, and `replicate` all flow through
//! [`crate::executor::run_expanded`], so every runtime gets the same
//! evaluation). Violations emit
//! `faucet_pipeline_sla_violations_total{pipeline,row,kind}` and a structured
//! warning — they never fail or abort the run. `faucet doctor` additionally
//! reports staleness / baseline health read-only.
//!
//! Module layout (mirrors `schedule/` / `replication/`):
//! - [`spec`] — serde config types + validation (`faucet schema sla`).
//! - [`state`] — the persisted history (`{state_key}::__sla__`).
//! - [`eval`] — pure staleness / floor / anomaly math.
//! - [`metrics`] — the Prometheus surface.

pub mod eval;
pub mod metrics;
pub mod spec;
pub mod state;

pub use eval::SlaViolation;
pub use spec::{AnomalyMethod, SlaSpec, VolumeAnomalySpec};
pub use state::{SLA_STATE_SUFFIX, SlaState, sla_state_key};

use faucet_core::StateStore;
use faucet_core::check::Probe;
use std::sync::Arc;
use std::time::Instant;

/// How the run being evaluated ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunOutcome {
    /// The pipeline (and final flush) succeeded, writing `rows` records.
    Success { rows: u64 },
    /// The pipeline failed; staleness is measured against the prior success.
    Failure,
}

/// Post-run SLA evaluation for one root invocation: load prior history,
/// evaluate, persist the updated baseline on success, and emit
/// metrics/warnings for every violation. Returns the violations found.
///
/// This is monitoring — it must never take down the run it observes. State
/// I/O errors are logged and swallowed; a failed *read* also skips the
/// baseline update so a transient state-store outage cannot clobber the
/// accumulated history with a fresh one.
pub async fn evaluate_post_run(
    spec: &SlaSpec,
    store: Option<&Arc<dyn StateStore>>,
    base_state_key: &str,
    pipeline: &str,
    row: &str,
    outcome: RunOutcome,
    now_unix: i64,
) -> Vec<SlaViolation> {
    let key = sla_state_key(base_state_key);
    let (history, raw, store) = match store {
        // No state store: only the stateless floor check can run (the expand
        // gate guarantees staleness/volume checks come with a `state:` block;
        // this is the defensive path).
        None => (SlaState::default(), None, None),
        Some(s) => match s.get(&key).await {
            Ok(v) => (
                v.clone().map(SlaState::from_value).unwrap_or_default(),
                v,
                Some(s),
            ),
            Err(e) => {
                tracing::warn!(
                    pipeline,
                    row,
                    key,
                    error = %e,
                    "reading SLA state failed — skipping SLA evaluation for this run"
                );
                return Vec::new();
            }
        },
    };

    let violations = match outcome {
        RunOutcome::Success { rows } => {
            let mut violations = eval::evaluate_success(spec, &history, rows);
            violations.extend(eval::evaluate_late_success(spec, &history, now_unix));
            if let Some(s) = store {
                match record_success_cas(s.as_ref(), &key, raw, rows, now_unix, spec.window())
                    .await
                {
                    Ok(updated) => {
                        metrics::set_baseline_runs(pipeline, row, updated.volumes.len());
                        metrics::set_last_success(pipeline, row, now_unix);
                    }
                    Err(e) => tracing::warn!(
                        pipeline,
                        row,
                        key,
                        error = %e,
                        "persisting SLA state failed — baseline not updated"
                    ),
                }
            }
            violations
        }
        RunOutcome::Failure => eval::evaluate_failure(spec, &history, now_unix),
    };

    for v in &violations {
        metrics::record_violation(pipeline, row, v.kind());
        tracing::warn!(pipeline, row, kind = v.kind(), "SLA violation: {v}");
    }
    violations
}

/// Attempts at folding a success into the stored history before giving up.
const CAS_ATTEMPTS: usize = 8;

/// Fold a success into the stored history with compare-and-put, re-reading on
/// a conflict so a concurrent run of the same row never loses its sample.
async fn record_success_cas(
    store: &dyn StateStore,
    key: &str,
    mut current: Option<serde_json::Value>,
    rows: u64,
    now_unix: i64,
    window: usize,
) -> Result<SlaState, faucet_core::FaucetError> {
    for _ in 0..CAS_ATTEMPTS {
        let mut next = current
            .clone()
            .map(SlaState::from_value)
            .unwrap_or_default();
        next.record_success(rows, now_unix, window);
        if store
            .compare_and_put(key, current.as_ref(), &next.to_value())
            .await?
        {
            return Ok(next);
        }
        current = store.get(key).await?;
    }
    Err(faucet_core::FaucetError::State(format!(
        "SLA state for `{key}` kept changing under {CAS_ATTEMPTS} concurrent updates"
    )))
}

/// Post-run lag check (#733): the lag the source reported when the run ended
/// against the `max_lag_*` thresholds. Monitoring only, like the rest.
pub fn evaluate_lag_post_run(
    spec: &SlaSpec,
    pipeline: &str,
    row: &str,
    lag: Option<&faucet_core::SourceLag>,
) -> Vec<SlaViolation> {
    let Some(lag) = lag else {
        return Vec::new();
    };
    let violations = eval::evaluate_lag(spec, lag);
    for v in &violations {
        metrics::record_violation(pipeline, row, v.kind());
        tracing::warn!(pipeline, row, kind = v.kind(), "SLA violation: {v}");
    }
    violations
}

/// Read-only SLA probes for `faucet doctor` (and serve's `doctor_first`):
/// staleness of the last recorded success and volume-baseline warm-up state.
/// `min_rows_per_run` has nothing to probe without a run, so it is not
/// represented here.
pub async fn doctor_probes(
    spec: &SlaSpec,
    store: Option<&Arc<dyn StateStore>>,
    base_state_key: &str,
    now_unix: i64,
) -> Vec<Probe> {
    let start = Instant::now();
    let history = match store {
        None => {
            // Only reachable when every configured check is stateless.
            return if spec.needs_state() {
                vec![Probe::skip("history", "no state store configured")]
            } else {
                Vec::new()
            };
        }
        Some(s) => match s.get(&sla_state_key(base_state_key)).await {
            Ok(v) => v.map(SlaState::from_value).unwrap_or_default(),
            Err(e) => {
                return vec![Probe::fail(
                    "history",
                    start.elapsed(),
                    format!("reading SLA state: {e}"),
                )];
            }
        },
    };

    let mut probes = Vec::new();
    if let Some(max_secs) = spec.max_staleness_secs {
        probes.push(match history.last_success_unix {
            None => Probe::skip("staleness", "no successful run recorded yet"),
            Some(last) => {
                let since = now_unix.saturating_sub(last).max(0) as u64;
                if since > max_secs {
                    Probe::fail_hint(
                        "staleness",
                        start.elapsed(),
                        format!("last success {since}s ago exceeds max_staleness_secs {max_secs}"),
                        "check the pipeline's schedule and recent run failures",
                    )
                } else {
                    Probe::pass("staleness", start.elapsed())
                }
            }
        });
    }
    if let Some(va) = &spec.volume_anomaly {
        let n = history.volumes.len();
        probes.push(if n < va.min_history as usize {
            Probe::skip(
                "baseline",
                format!(
                    "volume baseline warming up: {n}/{} successful runs",
                    va.min_history
                ),
            )
        } else {
            Probe::pass("baseline", start.elapsed())
        });
    }
    probes
}

#[cfg(test)]
mod tests {
    use super::spec::{AnomalyMethod, VolumeAnomalySpec};
    use super::*;
    use faucet_core::MemoryStateStore;
    use faucet_core::check::ProbeStatus;

    fn full_spec() -> SlaSpec {
        SlaSpec {
            max_staleness_secs: Some(3600),
            min_rows_per_run: Some(5),
            max_lag_bytes: None,
            max_lag_events: None,
            max_lag_seconds: None,
            volume_anomaly: Some(VolumeAnomalySpec {
                method: AnomalyMethod::Zscore,
                sensitivity: None,
                min_history: 3,
                window: 10,
            }),
        }
    }

    fn mem() -> Arc<dyn StateStore> {
        Arc::new(MemoryStateStore::new())
    }

    /// A store where another run writes its success between our read and our
    /// first compare-and-put.
    struct RacingStore {
        inner: MemoryStateStore,
        raced: std::sync::atomic::AtomicBool,
    }

    #[faucet_core::async_trait]
    impl StateStore for RacingStore {
        async fn get(&self, key: &str) -> Result<Option<serde_json::Value>, faucet_core::FaucetError> {
            self.inner.get(key).await
        }
        async fn put(&self, key: &str, value: &serde_json::Value) -> Result<(), faucet_core::FaucetError> {
            self.inner.put(key, value).await
        }
        async fn delete(&self, key: &str) -> Result<(), faucet_core::FaucetError> {
            self.inner.delete(key).await
        }
        async fn compare_and_put(
            &self,
            key: &str,
            expected: Option<&serde_json::Value>,
            value: &serde_json::Value,
        ) -> Result<bool, faucet_core::FaucetError> {
            if !self.raced.swap(true, std::sync::atomic::Ordering::SeqCst) {
                let mut other = SlaState::default();
                other.record_success(7, 50, 10);
                self.inner.put(key, &other.to_value()).await?;
            }
            self.inner.compare_and_put(key, expected, value).await
        }
    }

    #[tokio::test]
    async fn a_concurrent_success_of_the_same_row_keeps_both_samples() {
        let store: Arc<dyn StateStore> = Arc::new(RacingStore {
            inner: MemoryStateStore::new(),
            raced: Default::default(),
        });
        evaluate_post_run(
            &full_spec(),
            Some(&store),
            "p::r",
            "p",
            "r",
            RunOutcome::Success { rows: 9 },
            100,
        )
        .await;
        let raw = store.get(&sla_state_key("p::r")).await.unwrap().unwrap();
        let state = SlaState::from_value(raw);
        assert_eq!(state.volumes, vec![7, 9]);
        assert_eq!(state.last_success_unix, Some(100));
    }

    #[tokio::test]
    async fn success_updates_baseline_and_failure_reads_it() {
        let spec = full_spec();
        let store = mem();
        // Three successful runs at t=0, 10, 20 warm the baseline.
        for (i, rows) in [100u64, 101, 99].iter().enumerate() {
            let v = evaluate_post_run(
                &spec,
                Some(&store),
                "p::default",
                "p",
                "default",
                RunOutcome::Success { rows: *rows },
                (i as i64) * 10,
            )
            .await;
            assert!(v.is_empty(), "warm-up run {i} should not violate: {v:?}");
        }
        let stored = store.get("p::default::__sla__").await.unwrap().unwrap();
        let st = SlaState::from_value(stored);
        assert_eq!(st.volumes, vec![100, 101, 99]);
        assert_eq!(st.last_success_unix, Some(20));

        // A failure 2h later is stale.
        let v = evaluate_post_run(
            &spec,
            Some(&store),
            "p::default",
            "p",
            "default",
            RunOutcome::Failure,
            20 + 7200,
        )
        .await;
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].kind(), "staleness");

        // A failure within the window is not.
        let v = evaluate_post_run(
            &spec,
            Some(&store),
            "p::default",
            "p",
            "default",
            RunOutcome::Failure,
            20 + 60,
        )
        .await;
        assert!(v.is_empty(), "{v:?}");
    }

    #[tokio::test]
    async fn success_detects_floor_and_anomaly_against_prior_baseline() {
        let spec = full_spec();
        let store = mem();
        for (i, rows) in [100u64, 100, 100].iter().enumerate() {
            evaluate_post_run(
                &spec,
                Some(&store),
                "p::default",
                "p",
                "default",
                RunOutcome::Success { rows: *rows },
                i as i64,
            )
            .await;
        }
        // rows=2: below the floor of 5 AND anomalous vs the constant baseline.
        let v = evaluate_post_run(
            &spec,
            Some(&store),
            "p::default",
            "p",
            "default",
            RunOutcome::Success { rows: 2 },
            100,
        )
        .await;
        let kinds: Vec<_> = v.iter().map(|x| x.kind()).collect();
        assert_eq!(kinds, vec!["min_rows", "volume"]);
        // The anomalous volume still folds into the (adaptive) baseline.
        let st = SlaState::from_value(store.get("p::default::__sla__").await.unwrap().unwrap());
        assert_eq!(st.volumes, vec![100, 100, 100, 2]);
    }

    #[tokio::test]
    async fn no_store_runs_only_stateless_checks() {
        let spec = SlaSpec {
            max_staleness_secs: None,
            min_rows_per_run: Some(10),
            max_lag_bytes: None,
            max_lag_events: None,
            max_lag_seconds: None,
            volume_anomaly: None,
        };
        let v = evaluate_post_run(
            &spec,
            None,
            "p::default",
            "p",
            "default",
            RunOutcome::Success { rows: 1 },
            0,
        )
        .await;
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].kind(), "min_rows");
        // Failure with no store and no staleness config → nothing.
        let v = evaluate_post_run(
            &spec,
            None,
            "p::default",
            "p",
            "default",
            RunOutcome::Failure,
            0,
        )
        .await;
        assert!(v.is_empty());
    }

    #[tokio::test]
    async fn doctor_probes_cover_cold_fresh_and_stale() {
        let spec = full_spec();
        let store = mem();

        // Cold start: staleness + baseline both skip.
        let probes = doctor_probes(&spec, Some(&store), "p::default", 0).await;
        assert_eq!(probes.len(), 2);
        assert!(matches!(probes[0].status, ProbeStatus::Skip { .. }));
        assert!(matches!(probes[1].status, ProbeStatus::Skip { .. }));

        // Warm history: both pass while fresh.
        for i in 0..3i64 {
            evaluate_post_run(
                &spec,
                Some(&store),
                "p::default",
                "p",
                "default",
                RunOutcome::Success { rows: 100 },
                i,
            )
            .await;
        }
        let probes = doctor_probes(&spec, Some(&store), "p::default", 10).await;
        assert!(matches!(probes[0].status, ProbeStatus::Pass), "{probes:?}");
        assert!(matches!(probes[1].status, ProbeStatus::Pass), "{probes:?}");

        // Long after the last success the staleness probe fails.
        let probes = doctor_probes(&spec, Some(&store), "p::default", 2 + 7200).await;
        assert!(
            matches!(probes[0].status, ProbeStatus::Fail { .. }),
            "{probes:?}"
        );
    }

    #[tokio::test]
    async fn doctor_probes_without_store() {
        // Stateless spec → no probes at all.
        let stateless = SlaSpec {
            max_staleness_secs: None,
            min_rows_per_run: Some(1),
            max_lag_bytes: None,
            max_lag_events: None,
            max_lag_seconds: None,
            volume_anomaly: None,
        };
        assert!(doctor_probes(&stateless, None, "k", 0).await.is_empty());
        // Stateful spec, missing store (defensive) → a single skip.
        let probes = doctor_probes(&full_spec(), None, "k", 0).await;
        assert_eq!(probes.len(), 1);
        assert!(matches!(probes[0].status, ProbeStatus::Skip { .. }));
    }

    #[test]
    fn lag_post_run_reports_breaches_only_with_a_reading() {
        let mut spec = full_spec();
        spec.max_lag_events = Some(5);
        assert!(evaluate_lag_post_run(&spec, "p", "r", None).is_empty());
        let v = evaluate_lag_post_run(&spec, "p", "r", Some(&faucet_core::SourceLag::events(9)));
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].kind(), "lag");
        assert!(
            evaluate_lag_post_run(&spec, "p", "r", Some(&faucet_core::SourceLag::events(5)))
                .is_empty()
        );
    }
}
