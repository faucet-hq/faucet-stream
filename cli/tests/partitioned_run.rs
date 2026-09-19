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
        pipeline_name: name.into(),
        run_id: None,
        execution: max_concurrent.map(|n| faucet_cli::config::ExecutionSpec {
            schedule: Default::default(),
            max_concurrent: Some(n),
            on_error: faucet_cli::config::OnError::Continue,
            adaptive_batch_size: None,
        }),
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
