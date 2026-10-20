//! End-to-end tests for `faucet dlq replay` (#281): drive the real
//! `dlq_replay::replay` orchestration through the executor + a real JSONL
//! sink, and assert the now-valid rows land while rows that fail *again* are
//! routed to a fresh DLQ (never back to the source).

use faucet_cli::auth_catalog::build_auth_catalog;
use faucet_cli::config::parse_with_extension;
use faucet_cli::dlq_replay::{self, ReplayInputs};
use serde_json::{Value, json};
use std::io::Write;
use std::path::Path;

/// Build a DLQ envelope line the way `faucet_core::dlq::build_envelope` does.
fn envelope(reason: &str, kind: &str, payload: Value) -> String {
    json!({
        "error": { "kind": kind, "message": "boom" },
        "reason": reason,
        "payload": payload,
        "ts_ms": 1_751_760_000_000i64,
        "sink": "jsonl",
        "pipeline": "replay",
        "row": "",
        "record_index": 0,
    })
    .to_string()
}

fn write_file(path: &Path, body: &str) {
    let mut f = std::fs::File::create(path).unwrap();
    f.write_all(body.as_bytes()).unwrap();
    f.flush().unwrap();
}

fn read_lines(path: &Path) -> Vec<Value> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

async fn inputs(pipeline: &str, dry_run: bool) -> ReplayInputs<'static> {
    ReplayInputs {
        decryptor: Default::default(),
        reason: None,
        failed_dlq: None,
        row: None,
        all_rows: false,
        dry_run,
        pipeline_name: pipeline.to_string(),
        execution: None,
        auth: build_auth_catalog(None).unwrap(),
        clock: chrono::Utc::now().fixed_offset(),
        budget: None,
        cancel: None,
    }
}

#[tokio::test]
async fn replay_lands_payloads_in_the_sink() {
    let dir = tempfile::tempdir().unwrap();
    let dlq = dir.path().join("dlq.jsonl");
    let out = dir.path().join("out.jsonl");
    write_file(
        &dlq,
        &format!(
            "{}\n{}\n",
            envelope("quality", "QualityFailure", json!({"id": 1, "name": "a"})),
            envelope("quality", "QualityFailure", json!({"id": 2, "name": "b"})),
        ),
    );

    // Source kind is irrelevant — the replay overrides it with a DLQ reader —
    // but must be a valid connector kind for the config to parse/expand.
    let cfg_yaml = format!(
        "version: 1\nname: replay\npipeline:\n  source: {{ type: csv, config: {{ path: /dev/null }} }}\n  sink: {{ type: jsonl, config: {{ path: {} }} }}\n",
        out.display()
    );
    let cfg = parse_with_extension(&cfg_yaml, "yaml").unwrap();

    let outcome = dlq_replay::replay(&cfg, dlq.to_str().unwrap(), inputs("replay", false).await)
        .await
        .expect("replay succeeds");

    assert_eq!(outcome.candidates, 2);
    assert_eq!(outcome.records_written, 2);
    assert!(!outcome.dry_run);

    let landed = read_lines(&out);
    assert_eq!(landed.len(), 2);
    assert_eq!(landed[0], json!({"id": 1, "name": "a"}));
    assert_eq!(landed[1], json!({"id": 2, "name": "b"}));
}

#[tokio::test]
async fn replay_dry_run_writes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let dlq = dir.path().join("dlq.jsonl");
    let out = dir.path().join("out.jsonl");
    write_file(
        &dlq,
        &format!(
            "{}\n",
            envelope("quality", "QualityFailure", json!({"id": 1}))
        ),
    );
    let cfg_yaml = format!(
        "version: 1\nname: replay\npipeline:\n  source: {{ type: csv, config: {{ path: /dev/null }} }}\n  sink: {{ type: jsonl, config: {{ path: {} }} }}\n",
        out.display()
    );
    let cfg = parse_with_extension(&cfg_yaml, "yaml").unwrap();

    let outcome = dlq_replay::replay(&cfg, dlq.to_str().unwrap(), inputs("replay", true).await)
        .await
        .unwrap();
    assert_eq!(outcome.candidates, 1);
    assert!(outcome.dry_run);
    // The real sink was never touched under --dry-run.
    assert!(!out.exists() || read_lines(&out).is_empty());
}

#[cfg(feature = "contract")]
#[tokio::test]
async fn replay_refailures_go_to_fresh_dlq_not_the_source() {
    let dir = tempfile::tempdir().unwrap();
    let dlq = dir.path().join("dlq.jsonl");
    let out = dir.path().join("out.jsonl");
    // One payload that now satisfies the contract, one that still breaches it.
    write_file(
        &dlq,
        &format!(
            "{}\n{}\n",
            envelope("contract", "ContractViolation", json!({"status": "ok"})),
            envelope(
                "contract",
                "ContractViolation",
                json!({"status": "still-bad"})
            ),
        ),
    );

    // The replay config re-applies a contract that quarantines a bad `status`.
    // Its original DLQ is the source location; the replay swaps in a fresh one.
    let cfg_yaml = format!(
        r#"
version: 1
name: replay
pipeline:
  source: {{ type: csv, config: {{ path: /dev/null }} }}
  contract:
    version: "1.0.0"
    on_breach: quarantine
    fields:
      - name: status
        type: string
        enum: [ok]
  dlq:
    sink: {{ type: jsonl, config: {{ path: {dlq} }} }}
  sink: {{ type: jsonl, config: {{ path: {out} }} }}
"#,
        dlq = dlq.display(),
        out = out.display()
    );
    let cfg = parse_with_extension(&cfg_yaml, "yaml").unwrap();

    let outcome = dlq_replay::replay(&cfg, dlq.to_str().unwrap(), inputs("replay", false).await)
        .await
        .expect("replay succeeds");
    assert_eq!(outcome.candidates, 2);
    // Only the now-valid row reached the sink.
    assert_eq!(outcome.records_written, 1);
    let landed = read_lines(&out);
    assert_eq!(landed, vec![json!({"status": "ok"})]);

    // The still-breaching row was quarantined to the FRESH replay-failed DLQ…
    let fresh = Path::new(&outcome.failed_dlq);
    assert!(
        fresh.exists(),
        "fresh DLQ should exist at {}",
        outcome.failed_dlq
    );
    let refailed = read_lines(fresh);
    assert_eq!(refailed.len(), 1);
    assert_eq!(refailed[0]["payload"], json!({"status": "still-bad"}));

    // …and NOT re-appended to the source DLQ (no infinite loop). The source
    // still holds exactly its original two envelope lines.
    assert_eq!(read_lines(&dlq).len(), 2);
    assert_ne!(
        std::fs::canonicalize(fresh).unwrap(),
        std::fs::canonicalize(&dlq).unwrap()
    );
}

async fn sqlite_table(db: &Path, rows: &[(i64, &str, i64)]) -> sqlx::SqlitePool {
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&format!("sqlite://{}?mode=rwc", db.display()))
        .await
        .unwrap();
    sqlx::query("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, acct INTEGER)")
        .execute(&pool)
        .await
        .unwrap();
    for (id, name, acct) in rows {
        sqlx::query("INSERT INTO t (id, name, acct) VALUES (?, ?, ?)")
            .bind(id)
            .bind(name)
            .bind(acct)
            .execute(&pool)
            .await
            .unwrap();
    }
    pool
}

async fn ids(pool: &sqlx::SqlitePool) -> Vec<i64> {
    sqlx::query_scalar("SELECT id FROM t ORDER BY id")
        .fetch_all(pool)
        .await
        .unwrap()
}

/// A replay through a `write_mode: overwrite` row appends the replayed
/// records: it never swaps in a table holding only them.
#[tokio::test]
async fn replay_into_an_overwrite_sink_keeps_the_destination() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("t.db");
    let pool = sqlite_table(&db, &[(1, "a", 7), (2, "b", 7), (3, "c", 7)]).await;
    let dlq = dir.path().join("dlq.jsonl");
    write_file(
        &dlq,
        &format!(
            "{}\n",
            envelope("partial", "Sink", json!({"id": 4, "name": "d", "acct": 7}))
        ),
    );
    let cfg_yaml = format!(
        "version: 1\nname: replay\npipeline:\n  source: {{ type: csv, config: {{ path: /dev/null }} }}\n  sink: {{ type: sqlite, config: {{ database_url: \"sqlite://{}\", table_name: t, column_mapping: auto_map, write_mode: overwrite }} }}\n",
        db.display()
    );
    let cfg = parse_with_extension(&cfg_yaml, "yaml").unwrap();
    let outcome = dlq_replay::replay(&cfg, dlq.to_str().unwrap(), inputs("replay", false).await)
        .await
        .expect("replay succeeds");
    assert_eq!(outcome.records_written, 1);
    assert_eq!(ids(&pool).await, vec![1, 2, 3, 4]);
}

/// A replay never runs the row's scoped cleanup: the replayed rows are not
/// the complete set for the scope.
#[tokio::test]
async fn replay_does_not_run_scoped_cleanup() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("t.db");
    let pool = sqlite_table(&db, &[(1, "a", 7), (2, "b", 7), (3, "c", 7)]).await;
    let dlq = dir.path().join("dlq.jsonl");
    write_file(
        &dlq,
        &format!(
            "{}\n",
            envelope("partial", "Sink", json!({"id": 2, "name": "B", "acct": 7}))
        ),
    );
    let cfg_yaml = format!(
        "version: 1\nname: replay\npipeline:\n  source:\n    type: csv\n    config: {{ path: /dev/null }}\n    complete_for: {{ scope: {{ acct: 7 }}, on_missing: delete }}\n  sink: {{ type: sqlite, config: {{ database_url: \"sqlite://{}\", table_name: t, column_mapping: auto_map, write_mode: upsert, key: [id] }} }}\n",
        db.display()
    );
    let cfg = parse_with_extension(&cfg_yaml, "yaml").unwrap();
    dlq_replay::replay(&cfg, dlq.to_str().unwrap(), inputs("replay", false).await)
        .await
        .expect("replay succeeds");
    assert_eq!(ids(&pool).await, vec![1, 2, 3]);
    let name: String = sqlx::query_scalar("SELECT name FROM t WHERE id = 2")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(name, "B");
}

/// Two matrix rows share a pipeline-level DLQ: replaying through `us` lands
/// only `us`'s envelopes; `eu`'s are skipped and counted.
#[tokio::test]
async fn replay_of_a_shared_dlq_keeps_only_the_target_rows_envelopes() {
    let dir = tempfile::tempdir().unwrap();
    let dlq = dir.path().join("dlq.jsonl");
    let us = dir.path().join("us.jsonl");
    let eu = dir.path().join("eu.jsonl");
    let tagged = |row: &str, id: i64| {
        let mut v: Value =
            serde_json::from_str(&envelope("partial", "Sink", json!({"id": id}))).unwrap();
        v["row"] = json!(row);
        v.to_string()
    };
    write_file(
        &dlq,
        &format!(
            "{}\n{}\n{}\n",
            tagged("us", 1),
            tagged("eu", 2),
            tagged("us::partition::0001", 3)
        ),
    );
    let cfg_yaml = format!(
        "version: 1\nname: replay\npipeline:\n  source: {{ type: csv, config: {{ path: /dev/null }} }}\n  sink: {{ type: jsonl, config: {{ path: /dev/null }} }}\n  dlq: {{ sink: {{ type: jsonl, config: {{ path: {dlq} }} }} }}\nmatrix:\n  - id: us\n    sink: {{ config: {{ path: {us} }} }}\n  - id: eu\n    sink: {{ config: {{ path: {eu} }} }}\n",
        dlq = dlq.display(),
        us = us.display(),
        eu = eu.display()
    );
    let cfg = parse_with_extension(&cfg_yaml, "yaml").unwrap();
    let failed = dir
        .path()
        .join("failed.jsonl")
        .to_string_lossy()
        .into_owned();
    let mut inp: ReplayInputs<'_> = inputs("replay", false).await;
    inp.row = Some("us");
    inp.failed_dlq = Some(&failed);
    let outcome = dlq_replay::replay(&cfg, dlq.to_str().unwrap(), inp)
        .await
        .expect("replay succeeds");
    assert_eq!(outcome.candidates, 2);
    assert_eq!(outcome.skipped_other_rows, 1);
    let landed = read_lines(&us);
    assert_eq!(landed, vec![json!({"id": 1}), json!({"id": 3})]);
    assert!(!eu.exists());
}
