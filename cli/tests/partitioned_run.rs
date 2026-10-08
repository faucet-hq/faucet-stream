//! End-to-end range partitioning (#479) through the real CLI path:
//! config → `expand` → `run_expanded`.
//!
//! The fan-out happens in `expand`, so a partitioned row becomes ordinary
//! sibling root nodes. These tests pin the consequences of that: every chunk
//! actually runs, each with its own substituted config; they share the one
//! `execution.max_concurrent` semaphore rather than a private pool; and each gets
//! a distinct, valid state key.

use faucet_cli::config::PipelineConfig;
use faucet_cli::executor::{ExecuteOptions, run_expanded};
use faucet_cli::expand::expand;

fn opts(name: &str, max_concurrent: Option<usize>) -> ExecuteOptions {
    ExecuteOptions {
        legacy_state_writes: false,
        force_lease: false,
        pipeline_name: name.into(),
        run_id: None,
        execution: max_concurrent.map(|n| faucet_cli::config::ExecutionSpec {
            schedule: Default::default(),
            max_concurrent: Some(n),
            on_error: faucet_cli::config::OnError::Continue,
            adaptive_batch_size: None,
        }),
        concurrency: None,
        dry_run: false,
        limit: None,
        state_path_override: None,
        state_scope: Default::default(),
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
        usage: Default::default(),
        budget: None,
    }
}

/// One CSV per chunk, so a chunk that did not run leaves its rows missing.
fn seed(dir: &std::path::Path, chunks: usize) {
    for i in 0..chunks {
        std::fs::write(
            dir.join(format!("in-{i}.csv")),
            format!("id,chunk\n{i}00,{i}\n{i}01,{i}\n"),
        )
        .unwrap();
    }
}

fn config(dir: &std::path::Path, out: &std::path::Path, to: i64, chunk_size: u64) -> String {
    format!(
        r#"
version: 1
name: partitioned
pipeline:
  source:
    type: csv
    config:
      path: "{dir}/in-${{partition.start}}.csv"
  sink:
    type: jsonl
    config:
      path: "{out}"
      append: true
partition:
  kind: integer
  from: 0
  to: {to}
  chunk_size: {chunk_size}
  bounds: inclusive
"#,
        dir = dir.display(),
        out = out.display(),
    )
}

async fn run(yaml: &str, dir: &std::path::Path, o: ExecuteOptions) -> usize {
    let path = dir.join("p.yaml");
    std::fs::write(&path, yaml).unwrap();
    let cfg = PipelineConfig::from_text(yaml, &path).expect("config parses");
    let nodes = expand(&cfg).expect("expand");
    let n = nodes.len();
    let summary = run_expanded(nodes, o).await.expect("run");
    let errs: Vec<String> = summary
        .invocations
        .iter()
        .filter_map(|i| i.error.clone())
        .collect();
    assert!(
        !summary.had_failures(),
        "every chunk should succeed: {errs:?}"
    );
    n
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_chunk_runs_and_contributes_its_own_rows() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out.jsonl");
    seed(dir.path(), 3);

    // chunk_size 1 over [0, 2] → chunks starting at 0, 1, 2.
    let nodes = run(
        &config(dir.path(), &out, 2, 1),
        dir.path(),
        opts("p", Some(4)),
    )
    .await;
    assert_eq!(nodes, 3, "one node per chunk");

    let body = std::fs::read_to_string(&out).unwrap();
    assert_eq!(
        body.lines().count(),
        6,
        "2 rows from each of 3 chunks — a chunk that silently did not run would show here"
    );
    // Each chunk read a *different* file, proving substitution reached the source.
    for c in 0..3 {
        assert!(
            body.contains(&format!("\"chunk\":\"{c}\"")),
            "chunk {c}'s rows are missing from the output"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn chunks_are_serialised_by_the_shared_concurrency_limit() {
    // The fan-out reuses the executor's single semaphore rather than a private
    // pool, so `max_concurrent: 1` must still complete every chunk. (A private
    // pool would also pass this, but a *deadlock* on the shared one would not —
    // which is the failure this guards.)
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out.jsonl");
    seed(dir.path(), 4);

    let nodes = run(
        &config(dir.path(), &out, 3, 1),
        dir.path(),
        opts("p", Some(1)),
    )
    .await;
    assert_eq!(nodes, 4);
    assert_eq!(std::fs::read_to_string(&out).unwrap().lines().count(), 8);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn each_chunk_gets_a_distinct_valid_state_key() {
    // Chunk ids become part of the state key, so they must be unique and pass
    // core's state-key charset validation — otherwise resumable partitioned runs
    // would collide or be rejected at run time.
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out.jsonl");
    seed(dir.path(), 3);
    let yaml = config(dir.path(), &out, 2, 1);
    let cfg = PipelineConfig::from_text(&yaml, &dir.path().join("p.yaml")).unwrap();
    let nodes = expand(&cfg).unwrap();

    let mut keys = std::collections::BTreeSet::new();
    for n in &nodes {
        let key = format!("partitioned::{}", n.id);
        faucet_core::state::validate_state_key(&key)
            .unwrap_or_else(|e| panic!("state key {key} is invalid: {e}"));
        assert!(keys.insert(key.clone()), "duplicate state key {key}");
    }
    assert_eq!(keys.len(), 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failing_chunk_does_not_silently_pass() {
    // Chunk 2's file is missing. `on_error: continue` (the default) keeps
    // siblings running, but the run must still report the failure — a
    // partitioned run that swallowed a chunk would silently under-read.
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out.jsonl");
    seed(dir.path(), 2); // only in-0.csv and in-1.csv

    let yaml = config(dir.path(), &out, 2, 1); // plans chunks 0,1,2
    let path = dir.path().join("p.yaml");
    std::fs::write(&path, &yaml).unwrap();
    let cfg = PipelineConfig::from_text(&yaml, &path).unwrap();
    let nodes = expand(&cfg).unwrap();
    let summary = run_expanded(nodes, opts("p", Some(4))).await.unwrap();

    assert!(summary.had_failures(), "the missing chunk must be reported");
    assert_eq!(summary.failure_count(), 1, "exactly the one bad chunk");
    // The healthy chunks still wrote their rows.
    assert_eq!(std::fs::read_to_string(&out).unwrap().lines().count(), 4);
}

#[cfg(feature = "source-sqlite")]
async fn sqlite_ids(dir: &std::path::Path, ids: &[i64]) -> String {
    let url = format!("sqlite://{}?mode=rwc", dir.join("src.db").display());
    let pool = sqlx::SqlitePool::connect(&url).await.unwrap();
    sqlx::query("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .execute(&pool)
        .await
        .unwrap();
    for id in ids {
        sqlx::query("INSERT INTO t VALUES (?)")
            .bind(id)
            .execute(&pool)
            .await
            .unwrap();
    }
    pool.close().await;
    url
}

#[cfg(feature = "source-sqlite")]
async fn probed_run(bounds: &str, to_unbounded: &str, probe_max: i64, ids: &[i64]) -> usize {
    let dir = tempfile::tempdir().unwrap();
    let src = sqlite_ids(dir.path(), ids).await;
    let probe = dir.path().join("probe.csv");
    std::fs::write(&probe, format!("max_id\n{probe_max}\n")).unwrap();
    let out = dir.path().join("out.jsonl");
    let op = if bounds == "inclusive" { "<=" } else { "<" };
    let yaml = format!(
        r#"
version: 1
name: probed
pipeline:
  source:
    type: sqlite
    config:
      database_url: "{src}"
      query: "SELECT id FROM t WHERE id >= ${{partition.start}} AND id {op} ${{partition.end}}"
  sink:
    type: jsonl
    config:
      path: "{out}"
      append: true
partition:
  kind: integer
  from: 1
  chunk_size: 100
  bounds: {bounds}{to_unbounded}
  to:
    from_source:
      type: csv
      config:
        path: "{probe}"
    value_path: "$.max_id"
"#,
        out = out.display(),
        probe = probe.display(),
    );
    let path = dir.path().join("p.yaml");
    std::fs::write(&path, &yaml).unwrap();
    let mut cfg = PipelineConfig::from_text(&yaml, &path).expect("config parses");
    faucet_cli::partition::resolve_config_bounds(&mut cfg, &Default::default())
        .await
        .expect("probe");
    let summary = run_expanded(expand(&cfg).expect("expand"), opts("probed", None))
        .await
        .expect("run");
    assert!(!summary.had_failures(), "{summary:?}");
    std::fs::read_to_string(&out)
        .map(|b| b.lines().count())
        .unwrap_or(0)
}

#[cfg(feature = "source-sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stale_probe_still_reads_rows_above_it() {
    // The probe saw MAX = 3, rows 4 and 5 arrived before the last chunk ran.
    assert_eq!(probed_run("inclusive", "", 3, &[1, 2, 3, 4, 5]).await, 5);
    assert_eq!(probed_run("half_open", "", 3, &[1, 2, 3, 4, 5]).await, 5);
}

#[cfg(feature = "source-sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_closed_half_open_probe_reads_the_maximum_row() {
    let closed = "\n  to_unbounded: false";
    assert_eq!(
        probed_run("half_open", closed, 5, &[1, 2, 3, 4, 5]).await,
        5
    );
    assert_eq!(probed_run("half_open", closed, 1, &[1]).await, 1);
}

/// A probed partition validates offline (no probe runs, so a missing probe
/// file is fine), and every executing command plans the real chunks through
/// the shared runtime pass (#789 CLI-57).
#[cfg(feature = "source-sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn probed_partitions_validate_offline_and_resolve_for_every_command() {
    let dir = tempfile::tempdir().unwrap();
    let probe = dir.path().join("probe.csv");
    let yaml = format!(
        r#"
version: 1
name: probed
pipeline:
  source:
    type: sqlite
    config:
      database_url: "sqlite://{db}?mode=rwc"
      query: "SELECT 1 AS id WHERE ${{partition.start}} <= ${{partition.end}}"
  sink: {{ type: jsonl, config: {{ path: "{out}", append: true }} }}
partition:
  kind: integer
  from: 1
  chunk_size: 100
  bounds: inclusive
  to:
    from_source: {{ type: csv, config: {{ path: "{probe}" }} }}
    value_path: "$.max_id"
"#,
        db = dir.path().join("src.db").display(),
        out = dir.path().join("out.jsonl").display(),
        probe = probe.display(),
    );
    let path = dir.path().join("p.yaml");
    std::fs::write(&path, &yaml).unwrap();
    let cfg = PipelineConfig::from_text(&yaml, &path).expect("config parses");
    assert!(expand(&cfg).is_err(), "unresolved bounds cannot be planned");
    assert_eq!(
        expand(&faucet_cli::partition::offline(&cfg)).unwrap().len(),
        1
    );
    let cli = <faucet_cli::cli::Cli as clap::Parser>::try_parse_from([
        "faucet",
        "validate",
        path.to_str().unwrap(),
        "--no-env-file",
    ])
    .unwrap();
    faucet_cli::run_command(cli)
        .await
        .expect("validate needs no probe");

    std::fs::write(&probe, "max_id\n250\n").unwrap();
    let resolved = faucet_cli::partition::resolve_runtime(&cfg).await.unwrap();
    assert_eq!(expand(&resolved).unwrap().len(), 3);
    let plain = PipelineConfig::from_text(
        "version: 1\nname: p\npipeline:\n  source: { type: csv, config: { path: a.csv } }\n  sink: { type: jsonl, config: { path: o.jsonl } }\n",
        &path,
    )
    .unwrap();
    assert_eq!(
        faucet_cli::partition::resolve_runtime(&plain)
            .await
            .unwrap()
            .name,
        plain.name
    );
}
