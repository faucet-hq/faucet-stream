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
