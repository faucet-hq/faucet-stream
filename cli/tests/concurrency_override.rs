//! Run-level connector-concurrency override (#610) through the real CLI path.
//!
//! The multi-tenant case this exists for: one registered template driving a
//! customer whose read replicas take 20 connections and one whose small
//! instance takes 5 — without per-customer copies of the config, and without
//! the template author having to pre-declare a `${param.*}` for it.
//!
//! What matters is that the override reaches the **built connector**, wins over
//! whatever the config said, and is a no-op (not an error) for a connector with
//! no such knob. The mapping itself is unit-tested in `registry`; these tests
//! pin the wiring and the observable end-to-end behaviour.

use faucet_cli::config::PipelineConfig;
use faucet_cli::executor::{ExecuteOptions, run_expanded};
use faucet_cli::expand::expand;
use serde_json::json;

fn opts(name: &str, concurrency: Option<usize>) -> ExecuteOptions {
    ExecuteOptions {
        pipeline_name: name.into(),
        run_id: None,
        execution: None,
        concurrency,
        dry_run: false,
        limit: None,
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
    }
}

fn config(dir: &std::path::Path, out: &std::path::Path) -> String {
    format!(
        r#"
version: 1
name: conc
pipeline:
  source:
    type: csv
    config:
      path: {}
  sink:
    type: jsonl
    config:
      path: {}
"#,
        dir.join("in.csv").display(),
        out.display()
    )
}

/// The headline guarantee: an override on a connector with **no** concurrency
/// knob must not break the run. A CSV source and a JSONL sink have none, so
/// this is the "ignored gracefully" half of the acceptance criteria.
#[tokio::test]
async fn an_override_on_a_knobless_connector_runs_normally() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("in.csv"), "id,name\n1,a\n2,b\n").unwrap();
    let out = dir.path().join("out.jsonl");

    let cfg =
        PipelineConfig::from_text(&config(dir.path(), &out), &dir.path().join("p.yaml")).unwrap();
    let nodes = expand(&cfg).unwrap();
    let summary = run_expanded(nodes, opts("conc", Some(7))).await.unwrap();

    let errs: Vec<String> = summary
        .invocations
        .iter()
        .filter_map(|i| i.error.clone())
        .collect();
    assert!(
        !summary.had_failures(),
        "an override a connector cannot use must be ignored, not fatal: {errs:?}"
    );
    let written = std::fs::read_to_string(&out).unwrap();
    assert_eq!(written.lines().count(), 2);
}

/// The override wins over an explicit config value — otherwise a template that
/// happens to set a pool size could never be retuned per tenant, which is the
/// whole point.
#[test]
fn the_override_replaces_an_explicit_config_value() {
    let mut cfg = json!({
        "connection_url": "postgres://u:p@h/db",
        "query": "SELECT 1",
        "max_connections": 10
    });
    let knob = faucet_cli::registry::override_source_concurrency("postgres", &mut cfg, 20);
    assert_eq!(knob, Some("max_connections"));
    assert_eq!(cfg["max_connections"], 20);
}

/// Two triggers of the same config with different overrides produce different
/// connector configs — the acceptance criterion, expressed at the layer where
/// it is observable without a live database.
#[test]
fn two_overrides_of_one_config_produce_two_pool_sizes() {
    let base = json!({ "connection_url": "postgres://u:p@h/db", "query": "SELECT 1" });

    let mut big = base.clone();
    faucet_cli::registry::override_source_concurrency("postgres", &mut big, 20);
    let mut small = base.clone();
    faucet_cli::registry::override_source_concurrency("postgres", &mut small, 5);

    assert_eq!(big["max_connections"], 20);
    assert_eq!(small["max_connections"], 5);
    assert_ne!(big, small, "one config, two tenants, two pool sizes");
    // And the untouched base still carries no knob, so an un-overridden run
    // keeps the connector's own default rather than inheriting a neighbour's.
    assert!(base.get("max_connections").is_none());
}
