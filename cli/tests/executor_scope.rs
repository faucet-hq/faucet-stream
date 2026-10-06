//! Executor regressions from the #789 audit (group B3a): partial overwrite
//! swaps (CLI-02, CLI-04, CLI-05), DLQ files replaced on every run (CLI-03),
//! children that depend on a sibling child (CLI-06), post-run verify where it
//! cannot be meaningful (CLI-08, CLI-09) and fan-out values spliced into SQL
//! (SQL-01).

#![cfg(all(
    feature = "source-sqlite",
    feature = "sink-sqlite",
    feature = "source-csv",
    feature = "sink-jsonl"
))]

use std::path::Path;

use assert_cmd::Command;
use faucet_cli::config::PipelineConfig;
use faucet_cli::executor::{ExecuteOptions, RunSummary, run_expanded};
use faucet_cli::expand::expand;
use serde_json::Value;
use sqlx::Connection;

fn opts(name: &str) -> ExecuteOptions {
    ExecuteOptions {
        legacy_state_writes: false,
        pipeline_name: name.into(),
        run_id: None,
        execution: None,
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

fn write(dir: &Path, name: &str, body: &str) -> String {
    let p = dir.join(name);
    std::fs::write(&p, body).unwrap();
    p.to_str().unwrap().replace('\\', "/")
}

fn lines(path: &Path) -> Vec<Value> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

async fn sqlite(dir: &Path, name: &str, setup: &[&str]) -> String {
    let path = dir.join(name);
    let url = format!("sqlite://{}?mode=rwc", path.to_str().unwrap());
    let mut conn = sqlx::SqliteConnection::connect(&url).await.unwrap();
    for stmt in setup {
        sqlx::query(stmt).execute(&mut conn).await.unwrap();
    }
    url
}

async fn count(url: &str, table: &str) -> i64 {
    let mut conn = sqlx::SqliteConnection::connect(url).await.unwrap();
    sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
        .fetch_one(&mut conn)
        .await
        .unwrap()
}

async fn run(yaml: &str, opts: ExecuteOptions) -> faucet_cli::error::CliResult<RunSummary> {
    let cfg = PipelineConfig::from_text(yaml, Path::new("t.yaml"))?;
    run_expanded(expand(&cfg)?, opts).await
}

fn expand_err(yaml: &str) -> String {
    let cfg = PipelineConfig::from_text(yaml, Path::new("t.yaml")).unwrap();
    expand(&cfg).unwrap_err().to_string()
}

// ── SQL-01 ──────────────────────────────────────────────────────────────────

#[tokio::test]
async fn parent_values_are_bound_not_spliced_into_child_sql() {
    let dir = tempfile::tempdir().unwrap();
    let db = sqlite(
        dir.path(),
        "items.db",
        &[
            "CREATE TABLE items (id INTEGER, name TEXT)",
            "INSERT INTO items VALUES (1, 'one'), (2, 'two'), (3, 'three')",
        ],
    )
    .await;
    let parents = write(
        dir.path(),
        "parents.csv",
        "ix,id,name\na,1,one\nb,1 OR 1=1,x' OR 'a'='a\n",
    );
    let out = dir.path().join("out");
    let yaml = format!(
        "version: 1\nname: sqlbind\npipeline:\n  sources:\n    parents:\n      type: csv\n      \
         config: {{ path: '{parents}' }}\n    items:\n      type: sqlite\n      config:\n        \
         database_url: '{db}'\n        query: \"SELECT id, name FROM items WHERE id = ${{p.id}} \
         OR name = '${{p.name}}'\"\n  sinks:\n    trash:\n      type: jsonl\n      config: {{ \
         path: '{trash}' }}\n    child:\n      type: jsonl\n      config: {{ path: \
         '{out}/${{p.ix}}.jsonl' }}\nmatrix:\n  - id: p\n    source: {{ ref: parents }}\n    \
         sink: {{ ref: trash }}\n  - id: c\n    parent: p\n    parent_key: ix\n    source: {{ \
         ref: items }}\n    sink: {{ ref: child }}\n",
        trash = dir.path().join("trash.jsonl").to_str().unwrap(),
        out = out.to_str().unwrap(),
    );
    let summary = run(&yaml, opts("sqlbind")).await.unwrap();
    assert!(!summary.had_failures(), "{summary:?}");
    let a = lines(&out.join("a.jsonl"));
    assert_eq!(a.len(), 1, "id 1 / name 'one' matches one row: {a:?}");
    assert!(
        lines(&out.join("b.jsonl")).is_empty(),
        "injection payloads are compared as values and match nothing"
    );
}

#[test]
fn a_fanout_token_inside_a_longer_sql_string_is_refused() {
    let yaml = "version: 1\nname: x\npipeline:\n  sources:\n    parents:\n      type: csv\n      \
                config: { path: p.csv }\n    items:\n      type: sqlite\n      config:\n        \
                database_url: 'sqlite::memory:'\n        query: \"SELECT * FROM t WHERE n = \
                'k-${p.id}'\"\n  sinks:\n    out:\n      type: jsonl\n      config: { path: \
                'o/${p.id}.jsonl' }\nmatrix:\n  - id: p\n    source: { ref: parents }\n    \
                sink: { ref: out }\n  - id: c\n    parent: p\n    parent_key: id\n    source: { \
                ref: items }\n    sink: { ref: out }\n";
    let e = expand_err(yaml);
    assert!(e.contains("longer quoted string"), "{e}");
}

#[tokio::test]
async fn a_fanout_table_name_must_be_a_plain_identifier() {
    let dir = tempfile::tempdir().unwrap();
    let db = sqlite(dir.path(), "dst.db", &[]).await;
    let parents = write(dir.path(), "parents.csv", "t\n\"x; DROP TABLE y\"\n");
    let yaml = format!(
        "version: 1\nname: ident\npipeline:\n  sources:\n    parents:\n      type: csv\n      \
         config: {{ path: '{parents}' }}\n  sinks:\n    trash:\n      type: jsonl\n      config: \
         {{ path: '{trash}' }}\n    t:\n      type: sqlite\n      config:\n        database_url: \
         '{db}'\n        table_name: 'tbl_${{p.t}}'\n        column_mapping: auto_map\n\
         matrix:\n  - id: p\n    source: {{ ref: parents }}\n    sink: {{ ref: trash }}\n  - id: \
         c\n    parent: p\n    parent_key: t\n    source: {{ ref: parents }}\n    sink: {{ ref: t \
         }}\n",
        trash = dir.path().join("trash.jsonl").to_str().unwrap(),
    );
    let summary = run(&yaml, opts("ident")).await.unwrap();
    let err = summary
        .invocations
        .iter()
        .find_map(|i| i.error.clone())
        .expect("the child must fail");
    assert!(err.contains("not a plain identifier"), "{err}");
}

// ── CLI-06 ──────────────────────────────────────────────────────────────────

#[tokio::test]
async fn a_child_that_depends_on_a_sibling_child_still_runs() {
    let dir = tempfile::tempdir().unwrap();
    let accounts = write(dir.path(), "accounts.csv", "id\n1\n2\n");
    let out = dir.path().join("out");
    let yaml = format!(
        "version: 1\nname: deps\npipeline:\n  source:\n    type: csv\n    config: {{ path: \
         '{accounts}' }}\n  sink:\n    type: jsonl\n    config: {{ path: '{out}/x.jsonl' }}\n\
         matrix:\n  - id: accounts\n    sink: {{ config: {{ path: '{out}/accounts.jsonl' }} }}\n  \
         - id: contacts\n    parent: accounts\n    parent_key: id\n    sink: {{ config: {{ path: \
         '{out}/contacts-${{accounts.id}}.jsonl' }} }}\n  - id: deals\n    parent: accounts\n    \
         parent_key: id\n    depends_on: [contacts]\n    sink: {{ config: {{ path: \
         '{out}/deals-${{accounts.id}}.jsonl' }} }}\n",
        out = out.to_str().unwrap(),
    );
    let summary = run(&yaml, opts("deps")).await.unwrap();
    assert!(!summary.had_failures(), "{summary:?}");
    let deals = summary
        .invocations
        .iter()
        .filter(|i| i.row_id == "deals")
        .count();
    assert_eq!(deals, 2, "one deals invocation per account");
    assert_eq!(lines(&out.join("deals-2.jsonl")).len(), 2);
}

// ── CLI-03 ──────────────────────────────────────────────────────────────────

#[cfg(feature = "contract")]
fn dlq_yaml(dir: &Path, input: &str, dlq_kind: &str, dlq_path: &str, rows: &str) -> String {
    format!(
        "version: 1\nname: dlq\npipeline:\n  source:\n    type: csv\n    config: {{ path: \
         '{input}' }}\n  sink:\n    type: jsonl\n    config: {{ path: '{out}' }}\n  contract:\n    \
         version: '1'\n    on_breach: quarantine\n    fields:\n      - name: status\n        type: \
         string\n        enum: [ok]\n  dlq:\n    sink:\n      type: {dlq_kind}\n      config: {{ \
         path: '{dlq_path}' }}\n{rows}",
        out = dir.join("out-${now.unix}.jsonl").to_str().unwrap(),
    )
}

#[cfg(feature = "contract")]
#[tokio::test]
async fn a_dlq_file_keeps_the_dead_letters_of_earlier_runs() {
    let dir = tempfile::tempdir().unwrap();
    let input = write(dir.path(), "in.csv", "id,status\n1,ok\n2,bad\n");
    let dlq = dir.path().join("dead.jsonl");
    let yaml = dlq_yaml(dir.path(), &input, "jsonl", dlq.to_str().unwrap(), "");
    for _ in 0..2 {
        let summary = run(&yaml, opts("dlq")).await.unwrap();
        assert!(!summary.had_failures(), "{summary:?}");
    }
    assert_eq!(lines(&dlq).len(), 2, "each run appends its dead letter");
}

#[cfg(all(feature = "contract", feature = "sink-file"))]
#[tokio::test]
async fn rows_sharing_a_file_dlq_append_through_one_writer() {
    let dir = tempfile::tempdir().unwrap();
    let input = write(dir.path(), "in.csv", "id,status\n1,bad\n2,bad\n3,ok\n");
    let dlq = dir.path().join("dead.jsonl");
    let rows = format!(
        "matrix:\n  - id: a\n    sink: {{ config: {{ path: '{a}' }} }}\n  - id: b\n    sink: {{ \
         config: {{ path: '{b}' }} }}\n",
        a = dir.path().join("a.jsonl").to_str().unwrap(),
        b = dir.path().join("b.jsonl").to_str().unwrap(),
    );
    let yaml = dlq_yaml(dir.path(), &input, "file", dlq.to_str().unwrap(), &rows);
    for _ in 0..2 {
        let summary = run(&yaml, opts("dlq")).await.unwrap();
        assert!(!summary.had_failures(), "{summary:?}");
    }
    assert_eq!(
        lines(&dlq).len(),
        8,
        "two rows × two dead letters × two runs"
    );
}

#[cfg(feature = "contract")]
#[test]
fn a_dlq_that_would_replace_its_file_is_refused() {
    let yaml = dlq_yaml(Path::new("/tmp"), "in.csv", "jsonl", "dead.jsonl", "").replace(
        "path: 'dead.jsonl' }",
        "path: 'dead.jsonl', append: false }",
    );
    let e = expand_err(&yaml);
    assert!(e.contains("append: false"), "{e}");
}

#[test]
fn a_dlq_path_shared_with_a_data_sink_is_refused() {
    let yaml = "version: 1\nname: x\npipeline:\n  source:\n    type: csv\n    config: { path: \
                in.csv }\n  sink:\n    type: jsonl\n    config: { path: same.jsonl }\n  dlq:\n    \
                sink:\n      type: jsonl\n      config: { path: same.jsonl }\n";
    let e = expand_err(yaml);
    assert!(e.contains("give the DLQ its own path"), "{e}");
}

// ── CLI-02 / CLI-04 / CLI-05 / CLI-08 / CLI-09 ─────────────────────────────

fn overwrite_rows(db: &str, input: &str, rows: &str, extra: &str) -> String {
    format!(
        "version: 1\nname: ow\n{extra}pipeline:\n  source:\n    type: csv\n    config: {{ path: \
         '{input}' }}\n  sink:\n    type: sqlite\n    config:\n      database_url: '{db}'\n      \
         table_name: customers\n      column_mapping: auto_map\n      write_mode: overwrite\n\
         {rows}"
    )
}

const US_EU: &str = "matrix:\n  - id: us\n  - id: eu\n";

#[tokio::test]
async fn a_selection_that_leaves_out_an_overwrite_peer_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let db = sqlite(dir.path(), "dst.db", &[]).await;
    let input = write(dir.path(), "in.csv", "id\n1\n");
    let yaml = overwrite_rows(&db, &input, US_EU, "");
    let cfg = PipelineConfig::from_text(&yaml, Path::new("t.yaml")).unwrap();
    let sel: faucet_cli::select::SelectionRequest =
        serde_json::from_value(serde_json::json!({"select": ["eu"]})).unwrap();
    let e = sel
        .apply(&cfg, expand(&cfg).unwrap())
        .unwrap_err()
        .to_string();
    assert!(e.contains("'us'") && e.contains("customers"), "{e}");
    let both: faucet_cli::select::SelectionRequest =
        serde_json::from_value(serde_json::json!({"select": ["us", "eu"]})).unwrap();
    assert_eq!(both.apply(&cfg, expand(&cfg).unwrap()).unwrap().len(), 2);

    let cfg_path = write(dir.path(), "ow.yaml", &yaml);
    Command::cargo_bin("faucet")
        .unwrap()
        .args(["run", &cfg_path, "--select", "eu", "--no-env-file"])
        .assert()
        .failure()
        .stderr(predicates::str::contains("not part of this run"));
}

#[tokio::test]
async fn overwrite_peers_in_different_levels_are_refused_before_any_write() {
    let dir = tempfile::tempdir().unwrap();
    let db = sqlite(
        dir.path(),
        "dst.db",
        &[
            "CREATE TABLE customers (id TEXT)",
            "INSERT INTO customers VALUES ('old')",
        ],
    )
    .await;
    let input = write(dir.path(), "in.csv", "id\n1\n");
    let rows = "matrix:\n  - id: first\n  - id: second\n    depends_on: [first]\n";
    let e = run(&overwrite_rows(&db, &input, rows, ""), opts("ow"))
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("one runs after the other"), "{e}");
    assert_eq!(count(&db, "customers").await, 1, "nothing was replaced");
}

#[tokio::test]
async fn a_sharded_run_cannot_overwrite() {
    let dir = tempfile::tempdir().unwrap();
    let db = sqlite(dir.path(), "dst.db", &[]).await;
    let input = write(dir.path(), "in.csv", "id\n1\n");
    let yaml = overwrite_rows(&db, &input, "", "");
    let mut o = opts("ow");
    o.shard = Some(faucet_core::ShardSpec::new("s0", serde_json::json!({})));
    let e = run(&yaml, o).await.unwrap_err().to_string();
    assert!(e.contains("source-sharded"), "{e}");

    let e = expand_err(&overwrite_rows(&db, &input, "", "shard: { count: 2 }\n"));
    assert!(e.contains("`shard:`"), "{e}");
}

#[tokio::test]
async fn limit_on_an_overwrite_row_leaves_the_destination_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let db = sqlite(
        dir.path(),
        "dst.db",
        &[
            "CREATE TABLE customers (id TEXT)",
            "INSERT INTO customers VALUES ('a'), ('b'), ('c'), ('d'), ('e')",
        ],
    )
    .await;
    let input = write(dir.path(), "in.csv", "id\n1\n2\n3\n");
    let mut o = opts("ow");
    o.limit = Some(1);
    let summary = run(&overwrite_rows(&db, &input, "", ""), o).await.unwrap();
    assert!(!summary.had_failures(), "{summary:?}");
    assert_eq!(count(&db, "customers").await, 5);

    let summary = run(&overwrite_rows(&db, &input, "", ""), opts("ow"))
        .await
        .unwrap();
    assert!(!summary.had_failures(), "{summary:?}");
    assert_eq!(count(&db, "customers").await, 3, "a real run still swaps");
}

#[test]
fn post_run_verify_is_refused_on_overwrite_and_shared_destinations() {
    let verify = "verify: { key: [id] }\n";
    let e = expand_err(&overwrite_rows("sqlite::memory:", "in.csv", "", verify));
    assert!(e.contains("after_run"), "{e}");

    let shared = "version: 1\nname: v\nverify: { key: [id] }\npipeline:\n  source:\n    type: \
                  csv\n    config: { path: in.csv }\n  sink:\n    type: sqlite\n    config:\n      \
                  database_url: 'sqlite::memory:'\n      table_name: t\n      column_mapping: \
                  auto_map\nmatrix:\n  - id: a\n  - id: b\n";
    let e = expand_err(shared);
    assert!(e.contains("covers only part of the destination"), "{e}");

    let off = shared.replace(
        "verify: { key: [id] }",
        "verify: { key: [id], after_run: false }",
    );
    let cfg = PipelineConfig::from_text(&off, Path::new("t.yaml")).unwrap();
    assert_eq!(expand(&cfg).unwrap().len(), 2);
}
