//! `faucet run` / `validate` over the `feed` source against a real SQLite
//! feed store.
#![cfg(all(
    feature = "source-feed",
    feature = "sink-file",
    feature = "sink-sqlite"
))]

use assert_cmd::Command;
use faucet_common_feed::{
    DeliveryUid, FeedStore, FeedUid, NewEvent, SqliteFeedStore, SqliteOptions, Start, StoreLimits,
};
use faucet_core::io::CallContext;
use predicates::prelude::*;
use std::path::Path;
use tempfile::TempDir;

struct Store {
    store: SqliteFeedStore,
    feed: FeedUid,
    delivery: DeliveryUid,
}

impl Store {
    async fn create(dir: &Path) -> Self {
        let store = SqliteFeedStore::open(dir.join("feeds.db"), &SqliteOptions::new())
            .await
            .expect("open the feed store");
        let feed = FeedUid::new("orders").unwrap();
        let delivery = DeliveryUid::new(feed.clone(), "archive").unwrap();
        let calls = CallContext::new();
        store
            .ensure_feed(&calls, &feed, None, &StoreLimits::new())
            .await
            .unwrap();
        store
            .ensure_delivery(&calls, &delivery, Start::Earliest)
            .await
            .unwrap();
        Self {
            store,
            feed,
            delivery,
        }
    }

    async fn append(&self, from: u64, to: u64) {
        let events = (from..=to)
            .map(|n| NewEvent::new("application/json", format!(r#"{{"n": {n}}}"#)))
            .collect();
        self.store
            .append(&CallContext::new(), &self.feed, events)
            .await
            .unwrap();
    }

    async fn position(&self) -> (u64, bool) {
        let heads = self
            .store
            .heads(&CallContext::new(), std::slice::from_ref(&self.feed))
            .await
            .unwrap();
        let d = heads[0]
            .deliveries
            .iter()
            .find(|d| d.delivery == self.delivery)
            .unwrap();
        (d.cursor.get(), d.claim.is_some())
    }
}

fn faucet(dir: &Path) -> Command {
    let mut cmd = Command::cargo_bin("faucet").unwrap();
    cmd.current_dir(dir);
    cmd
}

fn write(dir: &Path, name: &str, yaml: &str) -> std::path::PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, yaml).unwrap();
    path
}

const SOURCE: &str = r#"
    type: feed
    config:
      feed: orders
      delivery: archive
      store: { type: sqlite, config: { path: ./feeds.db } }
"#;

fn ns(path: &Path) -> Vec<u64> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .map(|l| {
            serde_json::from_str::<serde_json::Value>(l).unwrap()["n"]
                .as_u64()
                .unwrap()
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_run_drains_the_delivery_and_the_next_run_reads_only_what_is_new() {
    let dir = TempDir::new().unwrap();
    let store = Store::create(dir.path()).await;
    store.append(1, 5).await;
    let cfg = write(
        dir.path(),
        "faucet.yaml",
        &format!(
            r#"version: 1
name: orders_archive
pipeline:
  source:{SOURCE}
  sink:
    type: file
    config: {{ path: ./out/orders.jsonl, if_exists: append }}
  state:
    type: file
    config: {{ path: ./state }}
"#
        ),
    );
    let out = dir.path().join("out/orders.jsonl");

    faucet(dir.path()).arg("run").arg(&cfg).assert().success();
    assert_eq!(ns(&out), vec![1, 2, 3, 4, 5]);
    assert_eq!(store.position().await, (6, false), "moved and released");
    let first = std::fs::read_to_string(&out).unwrap();
    let row: serde_json::Value = serde_json::from_str(first.lines().next().unwrap()).unwrap();
    assert_eq!(row["_feed"]["seq"], 1);

    faucet(dir.path()).arg("run").arg(&cfg).assert().success();
    assert_eq!(ns(&out).len(), 5, "nothing new, nothing written");

    store.append(6, 7).await;
    faucet(dir.path()).arg("run").arg(&cfg).assert().success();
    assert_eq!(ns(&out), vec![1, 2, 3, 4, 5, 6, 7]);
    assert_eq!(store.position().await, (8, false));
}

#[tokio::test(flavor = "multi_thread")]
async fn exactly_once_into_an_idempotent_sink_validates_and_runs() {
    let dir = TempDir::new().unwrap();
    let store = Store::create(dir.path()).await;
    store.append(1, 4).await;
    let cfg = write(
        dir.path(),
        "faucet.yaml",
        &format!(
            r#"version: 1
name: orders_mirror
delivery: exactly_once
pipeline:
  source:{SOURCE}
      include_metadata: false
  sink:
    type: sqlite
    config:
      database_url: "sqlite://./mirror.db?mode=rwc"
      table_name: orders
      column_mapping: auto_map
  state:
    type: file
    config: {{ path: ./state }}
"#
        ),
    );
    faucet(dir.path())
        .args(["validate", "--no-secrets"])
        .arg(&cfg)
        .assert()
        .success()
        .stdout(predicate::str::contains("source=feed sink=sqlite"))
        .stdout(predicate::str::contains(
            "effectively-once (atomic watermark)",
        ));
    faucet(dir.path()).arg("run").arg(&cfg).assert().success();
    assert_eq!(store.position().await, (5, false));
    store.append(5, 5).await;
    faucet(dir.path()).arg("run").arg(&cfg).assert().success();
    assert_eq!(store.position().await, (6, false));

    use sqlx::Row;
    let pool = sqlx::SqlitePool::connect(&format!(
        "sqlite://{}",
        dir.path().join("mirror.db").display()
    ))
    .await
    .unwrap();
    let rows = sqlx::query("SELECT n FROM orders ORDER BY n")
        .fetch_all(&pool)
        .await
        .unwrap();
    let got: Vec<i64> = rows.iter().map(|r| r.get(0)).collect();
    assert_eq!(got, vec![1, 2, 3, 4, 5], "every event once");
}

#[tokio::test(flavor = "multi_thread")]
async fn reads_that_would_not_write_are_refused_and_move_nothing() {
    let dir = TempDir::new().unwrap();
    let store = Store::create(dir.path()).await;
    store.append(1, 3).await;
    let append = write(
        dir.path(),
        "append.yaml",
        &format!(
            r#"version: 1
name: orders_archive
pipeline:
  source:{SOURCE}
  sink:
    type: file
    config: {{ path: ./out/orders.jsonl, if_exists: append }}
"#
        ),
    );
    faucet(dir.path())
        .args(["run", "--dry-run"])
        .arg(&append)
        .assert()
        .failure()
        .stderr(predicate::str::contains("feed"));
    faucet(dir.path())
        .args(["run", "--limit", "1"])
        .arg(&append)
        .assert()
        .failure();

    let replacing = write(
        dir.path(),
        "replacing.yaml",
        &format!(
            r#"version: 1
name: orders_archive
pipeline:
  source:{SOURCE}
  sink:
    type: file
    config: {{ path: ./out/orders.jsonl }}
"#
        ),
    );
    faucet(dir.path())
        .args(["validate", "--no-secrets"])
        .arg(&replacing)
        .assert()
        .failure()
        .stderr(predicate::str::contains("`feed` source"))
        .stderr(predicate::str::contains("on every run"));

    let fan_out = write(
        dir.path(),
        "fan_out.yaml",
        &format!(
            r#"version: 1
name: orders_archive
pipeline:
  source:{SOURCE}
  sink:
    type: file
    config: {{ path: "./out/${{ids.n}}.jsonl", if_exists: append }}
matrix:
  - id: ids
    fan_out:
      source:{}
      select: "$.n"
      as: n
  - id: each
    for_each: [ids]
"#,
            SOURCE.replace("\n    ", "\n        ")
        ),
    );
    faucet(dir.path())
        .args(["validate", "--no-secrets"])
        .arg(&fan_out)
        .assert()
        .failure()
        .stderr(predicate::str::contains("matrix row 'ids'"))
        .stderr(predicate::str::contains("`fan_out:` row"))
        .stderr(predicate::str::contains("`feed` source"));

    assert_eq!(store.position().await, (1, false), "nothing moved");
    assert!(!dir.path().join("out").exists());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_missing_delivery_is_an_error_that_names_it() {
    let dir = TempDir::new().unwrap();
    let _store = Store::create(dir.path()).await;
    let cfg = write(
        dir.path(),
        "faucet.yaml",
        r#"version: 1
name: orders_archive
pipeline:
  source:
    type: feed
    config:
      feed: orders
      delivery: lake
      store: { type: sqlite, config: { path: ./feeds.db } }
  sink:
    type: file
    config: { path: ./out/orders.jsonl, if_exists: append }
"#,
    );
    faucet(dir.path())
        .arg("run")
        .arg(&cfg)
        .assert()
        .failure()
        .stderr(predicate::str::contains("delivery lake of feed orders"))
        .stderr(predicate::str::contains("faucet feed create"));
}

#[test]
fn the_shipped_example_validates_and_matches_the_config_schema() {
    let example = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/feed_to_jsonl.yaml");
    let dir = TempDir::new().unwrap();
    faucet(dir.path())
        .args(["validate", "--no-secrets"])
        .arg(&example)
        .assert()
        .success()
        .stdout(predicate::str::contains("source=feed sink=file"));

    let text = std::fs::read_to_string(&example).unwrap();
    let document: serde_json::Value = faucet_cli::yaml::from_str(&text).unwrap();
    for schema in [
        faucet_cli::schema_compose::config_schema(),
        serde_json::from_str(include_str!("../../schemas/faucet.schema.json")).unwrap(),
    ] {
        let validator = jsonschema::validator_for(&schema).expect("the schema compiles");
        let errors: Vec<String> = validator
            .iter_errors(&document)
            .map(|e| e.to_string())
            .collect();
        assert!(errors.is_empty(), "{}", errors.join("\n"));
    }
}
