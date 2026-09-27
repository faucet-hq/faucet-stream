//! End-to-end reads against a SQLite SQL catalog + local-filesystem warehouse
//! (no Docker). Snapshots are written with `faucet-sink-iceberg`; delete
//! snapshots are committed by hand because iceberg-rust has no row-delta
//! transaction yet.

mod common;

use std::collections::HashMap;
use std::sync::Arc;

use common::Lake;
use faucet_core::{Sink, Source, StreamPage};
use faucet_source_iceberg::IcebergSource;
use futures::StreamExt;
use iceberg::Catalog;
use serde_json::{Value, json};

async fn pages(source: &IcebergSource) -> Result<Vec<StreamPage>, faucet_core::FaucetError> {
    let ctx = HashMap::new();
    let mut s = source.stream_pages(&ctx, 0);
    let mut out = Vec::new();
    while let Some(p) = s.next().await {
        out.push(p?);
    }
    Ok(out)
}

fn ids(pages: &[StreamPage]) -> Vec<i64> {
    let mut v: Vec<i64> = pages
        .iter()
        .flat_map(|p| p.records.iter().map(|r| r["id"].as_i64().unwrap()))
        .collect();
    v.sort();
    v
}

fn bookmarks(pages: &[StreamPage]) -> Vec<i64> {
    pages
        .iter()
        .filter_map(|p| {
            p.bookmark
                .as_ref()
                .map(|b| b["snapshot_id"].as_i64().unwrap())
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_read_projection_and_filter_pushdown() {
    let lake = Lake::new();
    lake.append("events", 0..10).await;
    lake.append("events", 10..15).await;

    let src = lake.source("events", json!({ "batch_size": 4 })).await;
    let p = pages(&src).await.unwrap();
    assert_eq!(ids(&p), (0..15).collect::<Vec<_>>());
    assert!(p.iter().all(|pg| pg.records.len() <= 4));
    assert!(bookmarks(&p).is_empty(), "full mode never bookmarks");
    assert_eq!(
        p[0].records[0]["name"],
        json!(format!("n{}", p[0].records[0]["id"]))
    );
    assert!(src.state_key().is_none());
    assert_eq!(src.connector_name(), "iceberg");
    assert_eq!(src.dataset_uri(), "iceberg://sql/db.events");

    let src = lake.source("events", json!({ "columns": ["id"] })).await;
    let p = pages(&src).await.unwrap();
    assert!(
        p.iter()
            .flat_map(|pg| &pg.records)
            .all(|r| r.as_object().unwrap().len() == 1)
    );

    let src = lake
        .source("events", json!({ "filter": "id >= 12 or name = 'n3'" }))
        .await;
    assert_eq!(ids(&pages(&src).await.unwrap()), vec![3, 12, 13, 14]);

    let src = lake
        .source(
            "events",
            json!({ "filter": "amount < 2 and id not in (0)" }),
        )
        .await;
    assert_eq!(ids(&pages(&src).await.unwrap()), vec![1]);

    let src = lake.source("events", json!({ "filter": "nope = 1" })).await;
    assert!(
        pages(&src)
            .await
            .unwrap_err()
            .to_string()
            .contains("not in the table schema")
    );

    let rows = src_fetch(&lake.source("events", json!({ "batch_size": 0 })).await).await;
    assert_eq!(rows.len(), 15);
}

async fn src_fetch(src: &IcebergSource) -> Vec<Value> {
    src.fetch_with_context(&HashMap::new()).await.unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn time_travel_by_snapshot_and_timestamp() {
    let lake = Lake::new();
    lake.append("tt", 0..3).await;
    lake.append("tt", 3..8).await;
    let snaps = lake.snapshots("tt").await;
    let (first, first_ts) = snaps[0];

    let src = lake.source("tt", json!({ "snapshot_id": first })).await;
    assert_eq!(ids(&pages(&src).await.unwrap()), vec![0, 1, 2]);

    let ts = chrono::DateTime::from_timestamp_millis(first_ts)
        .unwrap()
        .to_rfc3339();
    let src = lake.source("tt", json!({ "as_of_timestamp": ts })).await;
    assert_eq!(ids(&pages(&src).await.unwrap()), vec![0, 1, 2]);

    let src = lake.source("tt", json!({ "snapshot_id": 1 })).await;
    assert!(
        pages(&src)
            .await
            .unwrap_err()
            .to_string()
            .contains("not in the table metadata")
    );

    let src = lake
        .source("tt", json!({ "as_of_timestamp": "2000-01-01T00:00:00Z" }))
        .await;
    assert!(
        pages(&src)
            .await
            .unwrap_err()
            .to_string()
            .contains("at or before")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incremental_reads_resume_from_the_bookmark() {
    let lake = Lake::new();
    lake.append("inc", 0..4).await;

    let extra = json!({ "mode": "incremental" });
    let src = lake.source("inc", extra.clone()).await;
    assert_eq!(src.state_key().as_deref(), Some("iceberg:db.inc"));
    let p = pages(&src).await.unwrap();
    let s1 = lake.snapshots("inc").await[0].0;
    assert_eq!(ids(&p), vec![0, 1, 2, 3]);
    assert_eq!(bookmarks(&p), vec![s1]);
    assert!(
        p.last().unwrap().bookmark.is_some(),
        "bookmark rides the final page"
    );

    lake.append("inc", 4..6).await;
    lake.append("inc", 6..9).await;
    let snaps = lake.snapshots("inc").await;

    let src = lake
        .source("inc", json!({ "mode": "incremental", "batch_size": 2 }))
        .await;
    src.apply_start_bookmark(json!({ "snapshot_id": s1 }))
        .await
        .unwrap();
    let p = pages(&src).await.unwrap();
    assert_eq!(ids(&p), vec![4, 5, 6, 7, 8]);
    assert_eq!(bookmarks(&p), vec![snaps[1].0, snaps[2].0]);

    let src = lake.source("inc", extra.clone()).await;
    src.apply_start_bookmark(json!(snaps[2].0)).await.unwrap();
    let p = pages(&src).await.unwrap();
    assert!(ids(&p).is_empty());
    assert_eq!(bookmarks(&p), vec![snaps[2].0]);

    let src = lake.source("inc", extra.clone()).await;
    assert!(src.apply_start_bookmark(json!("garbage")).await.is_err());

    // A bookmark missing from the metadata is treated as expired.
    let src = lake.source("inc", extra.clone()).await;
    src.apply_start_bookmark(json!(424242)).await.unwrap();
    assert!(
        pages(&src)
            .await
            .unwrap_err()
            .to_string()
            .contains("on_expired")
    );
    let src = lake
        .source(
            "inc",
            json!({ "mode": "incremental", "on_expired": "full_refresh" }),
        )
        .await;
    src.apply_start_bookmark(json!(424242)).await.unwrap();
    let p = pages(&src).await.unwrap();
    assert_eq!(ids(&p), (0..9).collect::<Vec<_>>());
    assert_eq!(bookmarks(&p), vec![snaps[2].0]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pipeline_persists_and_resumes_the_bookmark() {
    use faucet_core::state::{MemoryStateStore, StateStore};

    let lake = Lake::new();
    lake.append("pipe", 0..3).await;
    let store: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());

    let src = lake.source("pipe", json!({ "mode": "incremental" })).await;
    let sink = CountingSink::default();
    let r1 = faucet_core::Pipeline::new(&src, &sink)
        .with_state_store(store.clone())
        .run()
        .await
        .unwrap();
    assert_eq!(r1.records_written, 3);

    lake.append("pipe", 3..5).await;
    let src = lake.source("pipe", json!({ "mode": "incremental" })).await;
    let sink = CountingSink::default();
    let r2 = faucet_core::Pipeline::new(&src, &sink)
        .with_state_store(store.clone())
        .run()
        .await
        .unwrap();
    assert_eq!(
        r2.records_written, 2,
        "second run reads only the new append"
    );
    assert_eq!(sink.rows.lock().unwrap().len(), 2);
    let saved = store.get("iceberg:db.pipe").await.unwrap().unwrap();
    assert_eq!(
        saved["snapshot_id"],
        json!(lake.snapshots("pipe").await[1].0)
    );
}

#[derive(Default)]
struct CountingSink {
    rows: std::sync::Mutex<Vec<Value>>,
}

#[async_trait::async_trait]
impl Sink for CountingSink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, faucet_core::FaucetError> {
        self.rows.lock().unwrap().extend_from_slice(records);
        Ok(records.len())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn equality_deletes_are_applied_and_rewrites_detected() {
    let lake = Lake::new();
    lake.append("del", 0..6).await;
    let s1 = lake.snapshots("del").await[0].0;
    let s2 = lake.delete_ids("del", &[1, 4]).await;

    let src = lake.source("del", json!({})).await;
    assert_eq!(
        ids(&pages(&src).await.unwrap()),
        vec![0, 2, 3, 5],
        "deleted rows never appear"
    );

    let src = lake.source("del", json!({ "mode": "incremental" })).await;
    src.apply_start_bookmark(json!(s1)).await.unwrap();
    let e = pages(&src).await.unwrap_err().to_string();
    assert!(
        e.contains("`delete` snapshot") && e.contains(&s2.to_string()),
        "{e}"
    );

    let src = lake
        .source(
            "del",
            json!({ "mode": "incremental", "on_rewrite": "full_refresh" }),
        )
        .await;
    src.apply_start_bookmark(json!(s1)).await.unwrap();
    let p = pages(&src).await.unwrap();
    assert_eq!(ids(&p), vec![0, 2, 3, 5]);
    assert_eq!(bookmarks(&p), vec![s2]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shards_partition_the_data_files() {
    let lake = Lake::new();
    for i in 0..4 {
        lake.append("sh", i * 5..i * 5 + 5).await;
    }
    let src = lake.source("sh", json!({})).await;
    assert!(src.is_shardable());
    let shards = src.enumerate_shards(3).await.unwrap();
    assert_eq!(shards.len(), 3);
    let mut all = Vec::new();
    for shard in &shards {
        let s = lake.source("sh", json!({})).await;
        s.apply_shard(shard).await.unwrap();
        all.extend(ids(&pages(&s).await.unwrap()));
    }
    all.sort();
    assert_eq!(all, (0..20).collect::<Vec<_>>());

    let whole = lake.source("sh", json!({})).await;
    whole
        .apply_shard(&faucet_core::shard::ShardSpec::whole())
        .await
        .unwrap();
    assert_eq!(ids(&pages(&whole).await.unwrap()).len(), 20);
    let bad = faucet_core::shard::ShardSpec::new("x", json!({ "nope": 1 }));
    assert!(whole.apply_shard(&bad).await.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn discover_lists_tables_with_schemas() {
    let lake = Lake::new();
    lake.append("a", 0..2).await;
    lake.append("b", 0..3).await;
    let src = lake.source("a", json!({})).await;
    assert!(src.supports_discover());
    let found = src.discover().await.unwrap();
    let names: Vec<&str> = found.iter().map(|d| d.name.as_str()).collect();
    assert_eq!(names, vec!["db.a", "db.b"]);
    assert_eq!(found[1].config_patch, json!({ "table": "db.b" }));
    assert_eq!(found[1].estimated_rows, Some(3));
    assert!(found[0].schema.as_ref().unwrap()["properties"]["id"].is_object());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn check_probes_table_columns_and_filter() {
    use faucet_core::check::{CheckContext, ProbeStatus};
    let lake = Lake::new();
    lake.append("chk", 0..2).await;
    let ctx = CheckContext::default();
    let status = |r: faucet_core::check::CheckReport| r.probes[0].status.clone();

    let ok = lake
        .source("chk", json!({ "columns": ["id"], "filter": "id > 0" }))
        .await;
    assert!(matches!(
        status(ok.check(&ctx).await.unwrap()),
        ProbeStatus::Pass
    ));

    for (extra, needle) in [
        (json!({ "columns": ["nope"] }), "columns"),
        (json!({ "filter": "name > 1" }), "cannot be compared"),
    ] {
        let s = lake.source("chk", extra).await;
        match status(s.check(&ctx).await.unwrap()) {
            ProbeStatus::Fail { reason } => assert!(reason.contains(needle), "{reason}"),
            other => panic!("expected fail, got {other:?}"),
        }
    }

    let missing = lake.source("absent", json!({})).await;
    assert!(matches!(
        status(missing.check(&ctx).await.unwrap()),
        ProbeStatus::Fail { .. }
    ));

    let timeout = CheckContext {
        timeout: std::time::Duration::from_nanos(1),
    };
    let slow = lake.source("chk", json!({})).await;
    let _ = slow.check(&timeout).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn with_catalog_reuses_a_connected_catalog() {
    let lake = Lake::new();
    lake.append("wc", 0..2).await;
    let catalog: Arc<dyn Catalog> = Arc::new(lake.catalog().await);
    let src = IcebergSource::with_catalog(lake.source_config("wc", json!({})), catalog).unwrap();
    assert_eq!(ids(&pages(&src).await.unwrap()), vec![0, 1]);
    assert!(src.config_schema()["properties"]["table"].is_object());

    let empty = lake.source("never_written", json!({})).await;
    assert!(pages(&empty).await.is_err(), "missing table errors");
}

#[cfg(feature = "arrow")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn columnar_path_yields_record_batches_and_bookmarks() {
    let lake = Lake::new();
    lake.append("col", 0..5).await;
    let src = lake.source("col", json!({ "mode": "incremental" })).await;
    assert!(src.supports_columnar());
    let ctx = HashMap::new();
    let mut s = src.stream_batches(&ctx, 0);
    let mut rows = 0;
    let mut marks = Vec::new();
    while let Some(p) = s.next().await {
        let p = p.unwrap();
        rows += p.num_rows();
        if let Some(b) = p.bookmark {
            marks.push(b);
        }
    }
    assert_eq!(rows, 5);
    assert_eq!(
        marks,
        vec![json!({ "snapshot_id": lake.snapshots("col").await[0].0 })]
    );
}
