//! End-to-end wiring for the top-level `profiling:` block (#708): the
//! expand-time gates, the executor's per-run profile + baseline persistence,
//! drift detection across runs, the `on_drift` policies, the masked-values
//! guarantee, the `faucet profiling show|reset` verbs, the `faucet doctor`
//! probes, and the catalog copy of each run's profile.
#![cfg(all(feature = "source-csv", feature = "sink-jsonl"))]

use faucet_cli::cli::{
    ProfilingArgs, ProfilingCommand, ProfilingConfigArgs, ProfilingResetArgs, ProfilingShowArgs,
};
use faucet_cli::config::PipelineConfig;
use faucet_cli::error::CliError;
use faucet_cli::expand::expand;
use faucet_cli::profiling::{ProfileHistory, profiling_state_key};
use faucet_core::{DriftMetric, StateStore};
use std::path::{Path, PathBuf};

fn yaml(dir: &Path, extra_top: &str, extra_pipeline: &str) -> String {
    format!(
        r#"version: 1
name: proftest
pipeline:
  source: {{ type: csv, config: {{ path: {input} }} }}
  sink: {{ type: jsonl, config: {{ path: {output}, append: false }} }}
  state: {{ type: file, config: {{ path: {state} }} }}
  transforms:
    - type: cast
      config: {{ fields: {{ amount: float }}, on_error: "null" }}
{extra_pipeline}
profiling:
  min_history: 3
  window: 5
{extra_top}
"#,
        input = dir.join("in.csv").display(),
        output = dir.join("out.jsonl").display(),
        state = dir.join("state").display(),
    )
}

fn parse(text: &str) -> PipelineConfig {
    PipelineConfig::from_text(text, Path::new("prof.yaml")).expect("config parses")
}

/// A stable dataset: `amount` numeric, `region` categorical, `email` PII.
fn stable_csv(seed: u64) -> String {
    let mut s = String::from("id,amount,region,email\n");
    for i in 0..120u64 {
        let k = (i * 7 + seed) % 1000;
        let region = match k % 3 {
            0 => "eu",
            1 => "us",
            _ => "apac",
        };
        s.push_str(&format!(
            "{i},{}.{},{region},user{k}@example.com\n",
            k % 90 + 10,
            k % 10
        ));
    }
    s
}

/// The same shape with 40 % of `amount` empty (null after CSV typing) and a
/// region the baseline never held.
fn drifted_csv() -> String {
    let mut s = String::from("id,amount,region,email\n");
    for i in 0..120u64 {
        let amount = if i % 5 < 2 {
            String::new()
        } else {
            format!("{}.5", i % 90 + 10)
        };
        let region = if i % 2 == 0 { "eu" } else { "latam" };
        s.push_str(&format!("{i},{amount},{region},user{i}@example.com\n"));
    }
    s
}

async fn history(dir: &Path) -> ProfileHistory {
    let store = faucet_core::FileStateStore::new(dir.join("state"));
    let key = profiling_state_key("proftest::row-0");
    store
        .get(&key)
        .await
        .unwrap()
        .map(ProfileHistory::from_value)
        .unwrap_or_default()
}

// ── expand-time gates ────────────────────────────────────────────────────────

#[test]
fn expand_requires_a_state_block_and_a_valid_spec() {
    let cfg = parse(
        r#"version: 1
pipeline:
  source: { type: csv, config: { path: in.csv } }
  sink: { type: jsonl, config: { path: out.jsonl } }
profiling: {}
"#,
    );
    let err = expand(&cfg).unwrap_err();
    assert!(
        matches!(&err, CliError::Config(m) if m.contains("profiling") && m.contains("state")),
        "expected the needs-state gate, got: {err}"
    );

    let cfg = parse(
        r#"version: 1
pipeline:
  source: { type: csv, config: { path: in.csv } }
  sink: { type: jsonl, config: { path: out.jsonl } }
  state: { type: memory, config: {} }
profiling: { min_history: 1 }
"#,
    );
    let err = expand(&cfg).unwrap_err();
    assert!(
        matches!(&err, CliError::Config(m) if m.contains("min_history")),
        "expected the validation error, got: {err}"
    );

    // A per-row override is validated and resolved onto the node.
    let cfg = parse(
        r#"version: 1
pipeline:
  source: { type: csv, config: { path: in.csv } }
  sink: { type: jsonl, config: { path: out.jsonl } }
  state: { type: memory, config: {} }
profiling: { window: 30 }
matrix:
  - id: a
  - id: b
    profiling: { window: 10, on_drift: fail }
"#,
    );
    let nodes = expand(&cfg).unwrap();
    assert_eq!(nodes[0].profiling.as_ref().unwrap().window, 30);
    assert_eq!(nodes[1].profiling.as_ref().unwrap().window, 10);
    assert_eq!(
        nodes[1].profiling.as_ref().unwrap().on_drift,
        faucet_core::OnProfileDrift::Fail
    );
    let without = parse(
        r#"version: 1
pipeline:
  source: { type: csv, config: { path: in.csv } }
  sink: { type: jsonl, config: { path: out.jsonl } }
"#,
    );
    assert!(expand(&without).unwrap()[0].profiling.is_none());
}

// ── executor: baseline, drift, on_drift ──────────────────────────────────────

#[tokio::test]
async fn runs_learn_a_baseline_then_flag_drift_without_failing_under_warn() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = yaml(dir.path(), "", "");
    for seed in 0..3u64 {
        std::fs::write(dir.path().join("in.csv"), stable_csv(seed)).unwrap();
        let summary = faucet_cli::run_from_yaml_str(&cfg).await.unwrap();
        assert!(!summary.had_failures(), "{summary:?}");
        let h = history(dir.path()).await;
        assert_eq!(h.runs.len(), seed as usize + 1);
        assert!(h.latest().unwrap().drift.is_empty(), "warming up");
    }
    let h = history(dir.path()).await;
    let amount = &h.latest().unwrap().profile.columns["amount"];
    assert_eq!(amount.rows, 120);
    assert!(amount.numeric.is_some(), "csv typed the column numeric");
    assert!(amount.null_rate < 0.01);
    let region = &h.latest().unwrap().profile.columns["region"];
    assert_eq!(region.top_values.as_ref().unwrap().len(), 3);
    assert!(
        !h.latest()
            .unwrap()
            .profile
            .columns
            .contains_key("_faucet_run_id")
    );

    // A stable fourth run compares against the baseline and stays clean.
    std::fs::write(dir.path().join("in.csv"), stable_csv(9)).unwrap();
    faucet_cli::run_from_yaml_str(&cfg).await.unwrap();
    assert!(history(dir.path()).await.latest().unwrap().drift.is_empty());

    // The drifted run: null-rate jump on `amount`, a new `region` value —
    // recorded, not failed (`on_drift: warn`).
    std::fs::write(dir.path().join("in.csv"), drifted_csv()).unwrap();
    let summary = faucet_cli::run_from_yaml_str(&cfg).await.unwrap();
    assert!(!summary.had_failures(), "{summary:?}");
    let h = history(dir.path()).await;
    assert_eq!(h.runs.len(), 5, "windowed to 5");
    let drift = &h.latest().unwrap().drift;
    assert!(
        drift.iter().any(|d| d.column == "amount"
            && d.metric == DriftMetric::NullRate
            && d.observed >= 0.39),
        "{drift:?}"
    );
    assert!(
        drift.iter().any(|d| d.column == "region"
            && d.metric == DriftMetric::NewValue
            && d.value.as_deref() == Some("latam")),
        "{drift:?}"
    );
    assert!(
        drift
            .iter()
            .all(|d| d.column != "id" || d.metric != DriftMetric::NullRate),
        "the untouched id column raised no null-rate drift: {drift:?}"
    );
}

#[tokio::test]
async fn on_drift_fail_marks_the_run_failed_after_writing() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = yaml(dir.path(), "  on_drift: fail", "");
    for seed in 0..3u64 {
        std::fs::write(dir.path().join("in.csv"), stable_csv(seed)).unwrap();
        assert!(
            !faucet_cli::run_from_yaml_str(&cfg)
                .await
                .unwrap()
                .had_failures()
        );
    }
    std::fs::write(dir.path().join("in.csv"), drifted_csv()).unwrap();
    let summary = faucet_cli::run_from_yaml_str(&cfg).await.unwrap();
    assert!(summary.had_failures(), "{summary:?}");
    let err = summary.invocations[0].error.clone().unwrap();
    assert!(err.contains("Profile drift on columns"), "{err}");
    assert!(err.contains("amount"), "{err}");
    // The data was written before the verdict, and the run joined the baseline.
    let written = std::fs::read_to_string(dir.path().join("out.jsonl")).unwrap();
    assert_eq!(written.lines().count(), 120);
    assert_eq!(history(dir.path()).await.runs.len(), 4);
}

#[cfg(feature = "masking")]
#[tokio::test]
async fn profiles_never_hold_unmasked_values() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = yaml(
        dir.path(),
        "",
        "  masking:\n    rules:\n      - name: emails\n        match: { value_detector: email }\n        action: { type: redact }\n",
    );
    std::fs::write(dir.path().join("in.csv"), stable_csv(1)).unwrap();
    faucet_cli::run_from_yaml_str(&cfg).await.unwrap();
    let h = history(dir.path()).await;
    let email = &h.latest().unwrap().profile.columns["email"];
    assert_eq!(email.distinct, 1, "every value is the mask");
    let top = email.top_values.as_ref().unwrap();
    assert_eq!(top.len(), 1);
    assert_eq!(top[0].value, "***");
    assert!(!serde_json::to_string(&h).unwrap().contains("@example.com"));
}

#[tokio::test]
async fn dry_run_and_limit_do_not_touch_the_baseline() {
    let dir = tempfile::tempdir().unwrap();
    let text = yaml(dir.path(), "", "");
    std::fs::write(dir.path().join("in.csv"), stable_csv(1)).unwrap();
    let cfg = parse(&text);
    let nodes = expand(&cfg).unwrap();
    let opts = |dry_run: bool, limit: Option<usize>| faucet_cli::executor::ExecuteOptions {
        pipeline_name: "proftest".into(),
        run_id: None,
        execution: None,
        concurrency: None,
        dry_run,
        limit,
        state_path_override: None,
        shard: None,
        auth: Default::default(),
        clock: chrono::Utc::now().fixed_offset(),
        cancel: None,
        resilience: None,
        sla: None,
        reconcile: None,
        verify: None,
        rollback: None,
        #[cfg(feature = "lineage")]
        lineage: None,
        #[cfg(feature = "lineage")]
        lineage_cfg: None,
        #[cfg(feature = "notify")]
        notifier: None,
        #[cfg(feature = "catalog")]
        catalog: None,
    };
    faucet_cli::executor::run_expanded(nodes.clone(), opts(true, None))
        .await
        .unwrap();
    faucet_cli::executor::run_expanded(nodes, opts(false, Some(10)))
        .await
        .unwrap();
    assert!(history(dir.path()).await.runs.is_empty());
}

// ── `faucet profiling show|reset` ────────────────────────────────────────────

fn common(config: PathBuf, row: Option<&str>, json: bool) -> ProfilingConfigArgs {
    ProfilingConfigArgs {
        config: Some(config),
        row: row.map(String::from),
        json,
        env_file: None,
        no_env_file: true,
        profile: None,
    }
}

#[tokio::test]
async fn show_and_reset_verbs_drive_the_state_store() {
    let dir = tempfile::tempdir().unwrap();
    let text = yaml(dir.path(), "", "");
    let path = dir.path().join("prof.yaml");
    std::fs::write(&path, &text).unwrap();
    for seed in 0..2u64 {
        std::fs::write(dir.path().join("in.csv"), stable_csv(seed)).unwrap();
        faucet_cli::run_from_yaml_str(&text).await.unwrap();
    }
    for json in [false, true] {
        faucet_cli::commands::profiling::run(ProfilingArgs {
            command: ProfilingCommand::Show(ProfilingShowArgs {
                common: common(path.clone(), None, json),
                full: json,
            }),
        })
        .await
        .expect("show");
    }
    faucet_cli::commands::profiling::run(ProfilingArgs {
        command: ProfilingCommand::Reset(ProfilingResetArgs {
            common: common(path.clone(), Some("row-0"), false),
            column: Some("amount".into()),
        }),
    })
    .await
    .expect("column reset");
    let h = history(dir.path()).await;
    assert_eq!(h.runs.len(), 2);
    assert!(
        h.runs
            .iter()
            .all(|r| !r.profile.columns.contains_key("amount"))
    );
    faucet_cli::commands::profiling::run(ProfilingArgs {
        command: ProfilingCommand::Reset(ProfilingResetArgs {
            common: common(path.clone(), None, true),
            column: None,
        }),
    })
    .await
    .expect("full reset");
    assert!(history(dir.path()).await.runs.is_empty());

    // Unknown row / no profiling block are user errors, not panics.
    let err = faucet_cli::commands::profiling::run(ProfilingArgs {
        command: ProfilingCommand::Show(ProfilingShowArgs {
            common: common(path.clone(), Some("nope"), false),
            full: false,
        }),
    })
    .await
    .unwrap_err()
    .to_string();
    assert!(err.contains("not a root row"), "{err}");
    let plain = dir.path().join("plain.yaml");
    std::fs::write(
        &plain,
        "version: 1\npipeline:\n  source: { type: csv, config: { path: in.csv } }\n  sink: { type: jsonl, config: { path: out.jsonl } }\n",
    )
    .unwrap();
    let err = faucet_cli::commands::profiling::run(ProfilingArgs {
        command: ProfilingCommand::Show(ProfilingShowArgs {
            common: common(plain, None, false),
            full: false,
        }),
    })
    .await
    .unwrap_err()
    .to_string();
    assert!(err.contains("no `profiling:` block"), "{err}");
}

// ── `faucet doctor` probes ───────────────────────────────────────────────────

#[tokio::test]
async fn doctor_reports_the_baseline_depth() {
    use faucet_cli::commands::doctor::probe_roots;
    let dir = tempfile::tempdir().unwrap();
    let text = yaml(dir.path(), "", "");
    let cfg = parse(&text);
    let nodes = expand(&cfg).unwrap();
    let auth = faucet_cli::auth_catalog::build_auth_catalog(None).unwrap();
    let ctx = faucet_core::CheckContext::default();
    std::fs::write(dir.path().join("in.csv"), stable_csv(0)).unwrap();
    let invs = probe_roots(
        &nodes,
        &auth,
        &ctx,
        None,
        cfg.profiling.as_ref(),
        "proftest",
    )
    .await;
    let probe = invs[0]
        .probes
        .iter()
        .find(|p| p.role == "profiling")
        .expect("profiling probe");
    assert!(
        probe.hint.as_deref().unwrap_or("").contains("warming up"),
        "{probe:?}"
    );
    faucet_cli::run_from_yaml_str(&text).await.unwrap();
    let invs = probe_roots(
        &nodes,
        &auth,
        &ctx,
        None,
        cfg.profiling.as_ref(),
        "proftest",
    )
    .await;
    let probe = invs[0]
        .probes
        .iter()
        .find(|p| p.role == "profiling")
        .unwrap();
    assert!(
        probe.hint.as_deref().unwrap_or("").contains("1 of 3"),
        "{probe:?}"
    );
    let none = probe_roots(&nodes, &auth, &ctx, None, None, "proftest").await;
    assert!(
        none[0].probes.iter().any(|p| p.role == "profiling"),
        "the node's own spec applies"
    );
}

// ── catalog copy ─────────────────────────────────────────────────────────────

#[cfg(all(feature = "catalog", feature = "serve-history-sqlite"))]
#[tokio::test]
async fn each_run_profile_is_recorded_on_the_sink_dataset() {
    use faucet_cli::serve::history::catalog::CatalogListFilter;
    let dir = tempfile::tempdir().unwrap();
    let cfg = yaml(
        dir.path(),
        &format!(
            "catalog:\n  url: \"sqlite:{}/cat.db\"",
            dir.path().display()
        ),
        "",
    );
    for seed in 0..2u64 {
        std::fs::write(dir.path().join("in.csv"), stable_csv(seed)).unwrap();
        faucet_cli::run_from_yaml_str(&cfg).await.unwrap();
    }
    let handle = faucet_cli::catalog::connect_from_spec(&faucet_cli::catalog::CatalogSpec {
        url: format!("sqlite:{}/cat.db", dir.path().display()),
        sample_records: 10,
    })
    .await
    .unwrap();
    let page = handle
        .store
        .catalog_list_datasets(&CatalogListFilter {
            limit: 10,
            ..Default::default()
        })
        .await
        .unwrap();
    let sink = page.datasets.iter().find(|d| d.kind == "jsonl").unwrap();
    let detail = handle
        .store
        .catalog_get_dataset(&sink.id)
        .await
        .unwrap()
        .unwrap();
    let profile = detail
        .profile
        .expect("profile recorded on the sink dataset");
    assert_eq!(profile.history.len(), 2);
    assert_eq!(profile.latest.baseline_runs, 1);
    assert!(profile.latest.profile.columns.contains_key("amount"));
    let source = page.datasets.iter().find(|d| d.kind == "csv").unwrap();
    let source_detail = handle
        .store
        .catalog_get_dataset(&source.id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        source_detail.profile.is_none(),
        "profiles describe the destination"
    );
    let history = handle
        .store
        .catalog_profile_history(&sink.id, 1)
        .await
        .unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].run_id, profile.latest.run_id);
}
