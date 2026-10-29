//! Integration test: `IcebergSink` on a Hive Metastore catalog (Hive 3.1.3
//! standalone metastore, warehouse in MinIO), #977. Commits go through
//! `faucet_common_iceberg::HmsCommitCatalog` (metastore lock, optimistic
//! `metadata_location` check, `alter_table`).
//!
//! Requires Docker and the `catalog-hms` feature; the Hive image is amd64-only:
//!   cargo test -p faucet-sink-iceberg --all-features --test integration_hms

#![cfg(feature = "catalog-hms")]

mod catalog_support;

use catalog_support::{assert_round_trip, create_s3_bucket, s3_props};
use faucet_common_iceberg::{CatalogConfig, build_catalog};
use faucet_conformance::stores;
use faucet_core::Sink;
use faucet_sink_iceberg::{IcebergSink, IcebergSinkConfig};
use futures::TryStreamExt;
use iceberg::spec::Operation;
use iceberg::{NamespaceIdent, TableIdent};
use serde_json::{Value, json};
use testcontainers::ImageExt;

struct Lake {
    _minio: stores::Store,
    _hms: stores::Metastore,
    catalog: Value,
    warehouse_dir: String,
}

async fn lake(tag: &str, bucket: &str) -> Lake {
    let suffix = format!("{}-{tag}", std::process::id());
    let network = format!("faucet-iceberg-hms-sink-{suffix}");
    let minio_name = format!("faucet-hms-sink-minio-{suffix}");
    let minio = stores::minio_with(|r| {
        r.with_network(network.clone())
            .with_container_name(minio_name.clone())
    })
    .await;
    create_s3_bucket(
        &minio.endpoint,
        stores::MINIO_ACCESS_KEY,
        stores::MINIO_SECRET_KEY,
        bucket,
    )
    .await;
    let warehouse_dir = format!("s3a://{bucket}/warehouse");
    let hms = stores::hive_metastore(&network, &minio_name, &warehouse_dir).await;
    let catalog = json!({
        "type": "hms",
        "uri": hms.uri,
        "warehouse": format!("s3://{bucket}/warehouse"),
        "properties": s3_props(&minio.endpoint, stores::MINIO_ACCESS_KEY, stores::MINIO_SECRET_KEY)
    });
    Lake {
        _minio: minio,
        _hms: hms,
        catalog,
        warehouse_dir,
    }
}

fn sink_config(catalog: &Value, table: &str) -> IcebergSinkConfig {
    serde_json::from_value(json!({
        "catalog": catalog,
        "namespace": ["db"],
        "table": table,
        "create_if_missing": true,
        "batch_size": 0
    }))
    .expect("sink config")
}

fn rows(ids: std::ops::Range<u64>) -> Vec<Value> {
    ids.map(|i| json!({ "id": i, "name": format!("n{i}") }))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hms_catalog_creates_and_appends_two_snapshots() {
    let lake = lake("append", "faucet-iceberg-hms-append").await;
    assert_round_trip(
        lake.catalog.clone(),
        "events",
        &format!("{}/db.db/events", lake.warehouse_dir),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_hms_writers_both_commit() {
    let lake = lake("concurrent", "faucet-iceberg-hms-concurrent").await;

    let first = IcebergSink::new(sink_config(&lake.catalog, "shared"))
        .await
        .expect("first sink");
    first.write_batch(&rows(0..5)).await.expect("seed write");
    first.flush().await.expect("seed flush");

    let a = IcebergSink::new(sink_config(&lake.catalog, "shared"))
        .await
        .expect("sink a");
    let b = IcebergSink::new(sink_config(&lake.catalog, "shared"))
        .await
        .expect("sink b");
    a.write_batch(&rows(100..120)).await.expect("a write");
    b.write_batch(&rows(200..230)).await.expect("b write");
    let (fa, fb) = tokio::join!(a.flush(), b.flush());
    fa.expect("writer a commits (or retries and commits)");
    fb.expect("writer b commits (or retries and commits)");

    let reader_cfg: CatalogConfig =
        serde_json::from_value(lake.catalog.clone()).expect("catalog config");
    let reader = build_catalog(&reader_cfg).await.expect("reader catalog");
    let table = reader
        .load_table(&TableIdent::new(
            NamespaceIdent::from_strs(["db"]).unwrap(),
            "shared".to_string(),
        ))
        .await
        .expect("load_table");
    let appends = table
        .metadata()
        .snapshots()
        .filter(|s| s.summary().operation == Operation::Append)
        .count();
    assert_eq!(appends, 3, "seed + both concurrent appends");

    let batches: Vec<_> = table
        .scan()
        .select_all()
        .build()
        .expect("scan")
        .to_arrow()
        .await
        .expect("arrow stream")
        .try_collect()
        .await
        .expect("scan rows");
    let mut ids: Vec<i64> = batches
        .iter()
        .flat_map(|batch| {
            let col = batch
                .column_by_name("id")
                .expect("id column")
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .expect("id is bigint")
                .clone();
            (0..col.len()).map(move |i| col.value(i))
        })
        .collect();
    ids.sort_unstable();
    let expected: Vec<i64> = (0..5).chain(100..120).chain(200..230).collect();
    assert_eq!(ids, expected, "every row of every writer, once");
}
