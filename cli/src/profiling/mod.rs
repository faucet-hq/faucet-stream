//! Learned column profiles with drift detection (#708) — the CLI layer.
//!
//! The top-level `profiling:` block (or a matrix row's own) makes every
//! **root** invocation profile the records it wrote (a
//! [`ProfilingSink`](faucet_core::ProfilingSink) decorator installed by the
//! executor, so the profile describes what landed after transforms and
//! masking), compare the profile against a rolling baseline of earlier runs
//! held in the pipeline's `StateStore`, and act on drift per `on_drift`:
//! `warn` (log + metric), `notify` (+ a `profile_drift` notification), or
//! `fail` (+ the run is reported failed). Every runtime — `faucet run`,
//! `schedule`, `serve`, `mirror` — flows through the executor, so all get it.
//!
//! Module layout (mirrors `sla/`):
//! - [`state`] — the persisted history (`{state_key}::__profiling__`).
//! - [`metrics`] — the Prometheus surface.
//! - this file — `evaluate_post_run`, the `show` / `reset` helpers behind
//!   `faucet profiling`, and the `faucet doctor` probes.
//!
//! The pure profiler, sketches and drift detector live in
//! [`faucet_core::profiling`]; the catalog keeps a browsable copy of each
//! run's profile (`catalog_record_profile`) but is never the detector's input.

pub mod metrics;
pub mod state;

pub use state::{PROFILING_STATE_SUFFIX, ProfileHistory, ProfileRecord, profiling_state_key};

use crate::config::PipelineConfig;
use crate::error::{CliError, CliResult};
use crate::expand::{ExpandedNode, NodeRole, expand};
use crate::state::build_state_store;
use chrono::{DateTime, Utc};
use faucet_core::check::Probe;
use faucet_core::{
    FaucetError, OnProfileDrift, ProfileDrift, ProfilingSpec, RunProfile, StateStore,
};
use serde::Serialize;
use std::sync::Arc;
use std::time::Instant;

/// What the post-run evaluation found.
#[derive(Debug, Clone, Serialize)]
pub struct ProfileOutcome {
    pub profile: RunProfile,
    /// Findings against the baseline (empty while warming up or when stable).
    pub drift: Vec<ProfileDrift>,
    /// Runs in the baseline the profile was compared against.
    pub baseline_runs: usize,
    /// Whether the baseline was deep enough (`min_history`) to compare.
    pub compared: bool,
    /// The history was persisted with this run folded in.
    pub persisted: bool,
}

impl ProfileOutcome {
    /// The metric-label outcome for `faucet_profile_runs_total`.
    pub fn label(&self) -> &'static str {
        if !self.compared {
            "warming"
        } else if self.drift.is_empty() {
            "stable"
        } else {
            "drifted"
        }
    }

    /// Whether `on_drift: fail` turns this run into a failure.
    pub fn fails_run(&self, spec: &ProfilingSpec) -> bool {
        spec.on_drift == OnProfileDrift::Fail && !self.drift.is_empty()
    }

    /// The typed error for a failing run.
    pub fn error(&self) -> FaucetError {
        let mut columns: Vec<String> = self.drift.iter().map(|d| d.column.clone()).collect();
        columns.dedup();
        FaucetError::ProfileDrift {
            columns,
            message: self
                .drift
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join("; "),
        }
    }
}

/// Post-run evaluation for one root invocation: load the baseline, detect
/// drift, emit metrics / warnings, and persist the history with this run
/// folded in. Returns what it found; the caller decides the `on_drift`
/// consequence (notify / fail) because those live in the executor.
///
/// Like SLA monitoring, state I/O errors are logged and swallowed — and a
/// failed *read* skips the baseline update so a transient outage never
/// clobbers the accumulated history.
#[allow(clippy::too_many_arguments)]
pub async fn evaluate_post_run(
    spec: &ProfilingSpec,
    store: Option<&Arc<dyn StateStore>>,
    base_state_key: &str,
    pipeline: &str,
    row: &str,
    run_id: &str,
    profile: RunProfile,
    now: DateTime<Utc>,
) -> ProfileOutcome {
    let key = profiling_state_key(base_state_key);
    let (mut history, store) = match store {
        None => (ProfileHistory::default(), None),
        Some(s) => match s.get(&key).await {
            Ok(v) => (
                v.map(ProfileHistory::from_value).unwrap_or_default(),
                Some(s),
            ),
            Err(e) => {
                tracing::warn!(
                    pipeline,
                    row,
                    key,
                    error = %e,
                    "reading profiling state failed — skipping drift detection for this run"
                );
                return ProfileOutcome {
                    profile,
                    drift: Vec::new(),
                    baseline_runs: 0,
                    compared: false,
                    persisted: false,
                };
            }
        },
    };
    let baseline = history.profiles();
    let baseline_runs = baseline.len();
    let compared = baseline_runs >= spec.min_history as usize;
    let drift = faucet_core::detect_drift(&baseline, &profile, spec);
    for d in &drift {
        metrics::record_drift(pipeline, row, &d.column, d.metric.as_str());
        tracing::warn!(
            pipeline,
            row,
            column = %d.column,
            metric = d.metric.as_str(),
            "profile drift: {d}"
        );
    }
    let mut persisted = false;
    if let Some(s) = store {
        history.record(
            ProfileRecord {
                run_id: run_id.to_string(),
                recorded_at: now,
                profile: profile.clone(),
                drift: drift.clone(),
            },
            spec.window as usize,
        );
        match s.put(&key, &history.to_value()).await {
            Ok(()) => persisted = true,
            Err(e) => tracing::warn!(
                pipeline,
                row,
                key,
                error = %e,
                "persisting profiling state failed — baseline not updated"
            ),
        }
    }
    let outcome = ProfileOutcome {
        profile,
        drift,
        baseline_runs,
        compared,
        persisted,
    };
    metrics::record_run(pipeline, row, outcome.label());
    metrics::set_gauges(
        pipeline,
        row,
        outcome.profile.columns.len(),
        history.runs.len(),
    );
    outcome
}

/// A root row's profiling history, as read by `faucet profiling show`.
#[derive(Debug, Clone, Serialize)]
pub struct RowHistory {
    pub row: String,
    pub state_key: String,
    pub history: ProfileHistory,
}

/// What `faucet profiling reset` did to one root row.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ResetOutcome {
    pub row: String,
    /// Runs in the history before the reset.
    pub runs: usize,
    /// For a column reset: how many run records held the column.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub column_runs: Option<usize>,
}

/// The root rows (optionally one) with their state store and base key.
async fn roots(
    cfg: &PipelineConfig,
    pipeline_name: &str,
    row: Option<&str>,
) -> CliResult<Vec<(ExpandedNode, Arc<dyn StateStore>, String)>> {
    let nodes = expand(cfg)?;
    let selected: Vec<&ExpandedNode> = nodes
        .iter()
        .filter(|n| matches!(n.role, NodeRole::Root) && row.is_none_or(|r| n.id == r))
        .collect();
    if selected.is_empty() {
        return Err(CliError::Config(match row {
            Some(r) => format!("profiling: row '{r}' is not a root row of this config"),
            None => "profiling: config has no root pipeline".to_string(),
        }));
    }
    let mut out = Vec::new();
    for node in selected {
        let Some(spec) = &node.state else {
            return Err(CliError::Config(format!(
                "profiling: row '{}' has no `state:` block — the baseline lives in the state store",
                node.id
            )));
        };
        let store = build_state_store(spec).await?;
        let key = crate::executor::build_state_key(pipeline_name, &node.id, None);
        out.push((node.clone(), store, key));
    }
    Ok(out)
}

/// Read every selected root row's profiling history.
pub async fn show(
    cfg: &PipelineConfig,
    pipeline_name: &str,
    row: Option<&str>,
) -> CliResult<Vec<RowHistory>> {
    let mut out = Vec::new();
    for (node, store, key) in roots(cfg, pipeline_name, row).await? {
        let history = store
            .get(&profiling_state_key(&key))
            .await?
            .map(ProfileHistory::from_value)
            .unwrap_or_default();
        out.push(RowHistory {
            row: node.id,
            state_key: key,
            history,
        });
    }
    Ok(out)
}

/// Re-baseline: drop the whole history (or one column's) for every selected
/// root row, so the next `min_history` runs learn the new normal.
pub async fn reset(
    cfg: &PipelineConfig,
    pipeline_name: &str,
    row: Option<&str>,
    column: Option<&str>,
) -> CliResult<Vec<ResetOutcome>> {
    let mut out = Vec::new();
    for (node, store, key) in roots(cfg, pipeline_name, row).await? {
        let skey = profiling_state_key(&key);
        let mut history = store
            .get(&skey)
            .await?
            .map(ProfileHistory::from_value)
            .unwrap_or_default();
        let runs = history.runs.len();
        let column_runs = match column {
            Some(c) => {
                let n = history.reset_column(c);
                store.put(&skey, &history.to_value()).await?;
                Some(n)
            }
            None => {
                if runs > 0 {
                    store.delete(&skey).await?;
                }
                None
            }
        };
        out.push(ResetOutcome {
            row: node.id,
            runs,
            column_runs,
        });
    }
    Ok(out)
}

/// Read-only probes for `faucet doctor`: baseline depth vs `min_history`.
pub async fn doctor_probes(
    spec: &ProfilingSpec,
    store: Option<&Arc<dyn StateStore>>,
    base_state_key: &str,
) -> Vec<Probe> {
    let start = Instant::now();
    let Some(store) = store else {
        return vec![Probe::skip("baseline", "no state store configured")];
    };
    let history = match store.get(&profiling_state_key(base_state_key)).await {
        Ok(v) => v.map(ProfileHistory::from_value).unwrap_or_default(),
        Err(e) => {
            return vec![Probe::fail(
                "baseline",
                start.elapsed(),
                format!("reading profiling state: {e}"),
            )];
        }
    };
    let runs = history.runs.len();
    let need = spec.min_history as usize;
    let latest_drift = history.latest().map(|r| r.drift.len()).unwrap_or(0);
    let mut baseline = Probe::pass("baseline", start.elapsed());
    baseline.hint = Some(if runs >= need {
        format!("{runs} run(s) in the baseline (window {})", spec.window)
    } else {
        format!("warming up: {runs} of {need} run(s) before drift detection starts")
    });
    let mut probes = vec![baseline];
    if latest_drift > 0 {
        let mut last = Probe::pass("last_run", start.elapsed());
        last.hint = Some(format!("last run raised {latest_drift} drift finding(s)"));
        probes.push(last);
    }
    probes
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::MemoryStateStore;
    use serde_json::{Value, json};

    fn profile(rows: &[Value]) -> RunProfile {
        let mut p = faucet_core::Profiler::new(ProfilingSpec::default());
        p.observe_page(rows);
        p.finish()
    }

    fn stable(seed: u64) -> RunProfile {
        profile(
            &(0..100u64)
                .map(|i| json!({"a": ((i * 31 + seed) % 50) as f64, "c": if i % 2 == 0 { "x" } else { "y" }}))
                .collect::<Vec<_>>(),
        )
    }

    #[tokio::test]
    async fn warms_up_then_detects_and_persists_windowed() {
        let store: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
        let spec = ProfilingSpec {
            min_history: 3,
            window: 4,
            ..Default::default()
        };
        for i in 0..3u64 {
            let o = evaluate_post_run(
                &spec,
                Some(&store),
                "p::r",
                "p",
                "r",
                &format!("run{i}"),
                stable(i),
                Utc::now(),
            )
            .await;
            assert!(!o.compared, "run {i} warms up");
            assert_eq!(o.label(), "warming");
            assert!(o.persisted);
            assert_eq!(o.baseline_runs, i as usize);
        }
        let stable_o = evaluate_post_run(
            &spec,
            Some(&store),
            "p::r",
            "p",
            "r",
            "run3",
            stable(9),
            Utc::now(),
        )
        .await;
        assert!(stable_o.compared && stable_o.drift.is_empty());
        assert_eq!(stable_o.label(), "stable");
        assert!(!stable_o.fails_run(&spec));
        let nulls = profile(
            &(0..100u64)
                .map(|i| json!({"a": if i % 2 == 0 { Value::Null } else { json!(1.0) }, "c": "x"}))
                .collect::<Vec<_>>(),
        );
        let drifted = evaluate_post_run(
            &spec,
            Some(&store),
            "p::r",
            "p",
            "r",
            "run4",
            nulls,
            Utc::now(),
        )
        .await;
        assert_eq!(drifted.label(), "drifted");
        assert!(
            drifted
                .drift
                .iter()
                .any(|d| d.column == "a" && d.metric == faucet_core::DriftMetric::NullRate),
            "{:?}",
            drifted.drift
        );
        let fail_spec = ProfilingSpec {
            on_drift: OnProfileDrift::Fail,
            ..spec.clone()
        };
        assert!(drifted.fails_run(&fail_spec));
        let err = drifted.error().to_string();
        assert!(err.contains("Profile drift on columns"), "{err}");
        assert!(err.contains("\"a\""), "{err}");
        let history = ProfileHistory::from_value(
            store
                .get(&profiling_state_key("p::r"))
                .await
                .unwrap()
                .unwrap(),
        );
        assert_eq!(history.runs.len(), 4, "windowed to 4");
        assert_eq!(history.latest().unwrap().run_id, "run4");
        assert!(
            !history.latest().unwrap().drift.is_empty(),
            "the drift is stored with the run"
        );
    }

    #[tokio::test]
    async fn no_store_means_no_baseline_and_nothing_persisted() {
        let o = evaluate_post_run(
            &ProfilingSpec::default(),
            None,
            "p::r",
            "p",
            "r",
            "run",
            stable(1),
            Utc::now(),
        )
        .await;
        assert!(!o.compared && !o.persisted && o.drift.is_empty());
    }

    struct FailingStore;
    #[faucet_core::async_trait]
    impl StateStore for FailingStore {
        async fn get(&self, _key: &str) -> Result<Option<Value>, FaucetError> {
            Err(FaucetError::State("down".into()))
        }
        async fn put(&self, _key: &str, _value: &Value) -> Result<(), FaucetError> {
            Err(FaucetError::State("down".into()))
        }
        async fn delete(&self, _key: &str) -> Result<(), FaucetError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn a_failed_read_skips_evaluation_and_a_failed_write_is_reported() {
        let store: Arc<dyn StateStore> = Arc::new(FailingStore);
        let o = evaluate_post_run(
            &ProfilingSpec::default(),
            Some(&store),
            "p::r",
            "p",
            "r",
            "run",
            stable(1),
            Utc::now(),
        )
        .await;
        assert!(!o.compared && !o.persisted);
        let probes = doctor_probes(&ProfilingSpec::default(), Some(&store), "p::r").await;
        assert!(matches!(
            probes[0].status,
            faucet_core::ProbeStatus::Fail { .. }
        ));
    }

    #[tokio::test]
    async fn doctor_probes_report_warmup_depth_and_last_drift() {
        let store: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
        let spec = ProfilingSpec {
            min_history: 2,
            ..Default::default()
        };
        let none = doctor_probes(&spec, None, "p::r").await;
        assert!(matches!(
            none[0].status,
            faucet_core::ProbeStatus::Skip { .. }
        ));
        let warm = doctor_probes(&spec, Some(&store), "p::r").await;
        assert!(
            warm[0].hint.as_deref().unwrap_or("").contains("warming up"),
            "{warm:?}"
        );
        for i in 0..2u64 {
            evaluate_post_run(
                &spec,
                Some(&store),
                "p::r",
                "p",
                "r",
                &format!("r{i}"),
                stable(i),
                Utc::now(),
            )
            .await;
        }
        let nulls = profile(
            &(0..50)
                .map(|_| json!({"a": Value::Null, "c": "x"}))
                .collect::<Vec<_>>(),
        );
        evaluate_post_run(
            &spec,
            Some(&store),
            "p::r",
            "p",
            "r",
            "r2",
            nulls,
            Utc::now(),
        )
        .await;
        let probes = doctor_probes(&spec, Some(&store), "p::r").await;
        assert_eq!(probes.len(), 2, "{probes:?}");
        assert!(
            probes[1]
                .hint
                .as_deref()
                .unwrap_or("")
                .contains("drift finding")
        );
    }

    fn cfg(dir: &std::path::Path) -> PipelineConfig {
        PipelineConfig::from_text(
            &format!(
                "version: 1\nname: p\nprofiling: {{ min_history: 2 }}\npipeline:\n  source: {{ type: csv, config: {{ path: in.csv }} }}\n  sink: {{ type: jsonl, config: {{ path: out.jsonl }} }}\n  state: {{ type: file, config: {{ path: {} }} }}\n",
                dir.display()
            ),
            std::path::Path::new("p.yaml"),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn show_and_reset_walk_the_root_rows() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = cfg(dir.path());
        let spec = ProfilingSpec::default();
        let key = crate::executor::build_state_key("p", "row-0", None);
        let store = build_state_store(cfg.pipeline.state.as_ref().unwrap())
            .await
            .unwrap();
        for i in 0..2u64 {
            evaluate_post_run(
                &spec,
                Some(&store),
                &key,
                "p",
                "row-0",
                &format!("r{i}"),
                stable(i),
                Utc::now(),
            )
            .await;
        }
        let shown = show(&cfg, "p", None).await.unwrap();
        assert_eq!(shown.len(), 1);
        assert_eq!(shown[0].row, "row-0");
        assert_eq!(shown[0].history.runs.len(), 2);
        assert!(shown[0].history.runs[0].profile.columns.contains_key("a"));

        let col = reset(&cfg, "p", Some("row-0"), Some("a")).await.unwrap();
        assert_eq!(col[0].column_runs, Some(2));
        assert_eq!(col[0].runs, 2);
        let shown = show(&cfg, "p", Some("row-0")).await.unwrap();
        assert!(
            shown[0]
                .history
                .runs
                .iter()
                .all(|r| !r.profile.columns.contains_key("a"))
        );
        assert!(shown[0].history.runs[0].profile.columns.contains_key("c"));

        let all = reset(&cfg, "p", None, None).await.unwrap();
        assert_eq!(all[0].runs, 2);
        assert_eq!(all[0].column_runs, None);
        assert!(
            show(&cfg, "p", None).await.unwrap()[0]
                .history
                .runs
                .is_empty()
        );
        // Resetting an empty history is a no-op, not an error.
        assert_eq!(reset(&cfg, "p", None, None).await.unwrap()[0].runs, 0);

        let err = show(&cfg, "p", Some("nope")).await.unwrap_err().to_string();
        assert!(err.contains("not a root row"), "{err}");
    }

    #[tokio::test]
    async fn rows_without_state_are_refused() {
        let cfg = PipelineConfig::from_text(
            "version: 1\nname: p\npipeline:\n  source: { type: csv, config: { path: in.csv } }\n  sink: { type: jsonl, config: { path: out.jsonl } }\n",
            std::path::Path::new("p.yaml"),
        )
        .unwrap();
        let err = show(&cfg, "p", None).await.unwrap_err().to_string();
        assert!(err.contains("no `state:` block"), "{err}");
    }
}
