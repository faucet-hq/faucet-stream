//! Format-v3 tables written by another engine (Spark 4 + Apache Iceberg; see
//! `fixtures/v3/generate.sh`): deletion vectors are applied, and a variant
//! column is refused before the first page (#1091).
#![cfg(unix)]

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use faucet_core::Source;
use faucet_source_iceberg::{IcebergSource, IcebergSourceConfig};
use futures::StreamExt;
use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};
use serde_json::{Value, json};

/// The path the fixture tables were written at; their metadata stores it.
const WAREHOUSE: &str = "/tmp/faucet-fixtures/iceberg-v3";

fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/v3")
}

fn copy_tree(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_tree(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}

/// Put the fixture warehouse at [`WAREHOUSE`], once per content version: the
/// copy lands under a temporary name and is renamed into place, so tests and
/// test processes running side by side never see half of it.
fn install_fixture() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let sums = std::fs::read_to_string(fixture_dir().join("SHA256SUMS")).unwrap();
        let marker = Path::new(WAREHOUSE).join("SHA256SUMS");
        let current = || std::fs::read_to_string(&marker).is_ok_and(|have| have == sums);
        if current() {
            return;
        }
        let parent = Path::new(WAREHOUSE).parent().unwrap();
        std::fs::create_dir_all(parent).unwrap();
        let staged = tempfile::tempdir_in(parent).unwrap();
        copy_tree(&fixture_dir().join("warehouse"), staged.path());
        std::fs::write(staged.path().join("SHA256SUMS"), &sums).unwrap();
        // Another process may have installed it meanwhile: only an older
        // version is ever removed.
        if std::fs::rename(staged.path(), WAREHOUSE).is_err() && !current() {
            let _ = std::fs::remove_dir_all(WAREHOUSE);
            let _ = std::fs::rename(staged.path(), WAREHOUSE);
        }
        assert!(current(), "the fixture warehouse is not at {WAREHOUSE}");
    });
}

/// The newest metadata file of a fixture table.
fn metadata_location(table: &str) -> String {
    let dir = Path::new(WAREHOUSE).join("db").join(table).join("metadata");
    let mut files: Vec<String> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .filter(|n| n.ends_with(".metadata.json"))
        .collect();
    files.sort();
    dir.join(files.last().unwrap()).display().to_string()
}

async fn source(table: &str, extra: Value) -> IcebergSource {
    install_fixture();
    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load(
            "fixtures",
            HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), WAREHOUSE.to_string())]),
        )
        .await
        .unwrap();
    let ns = NamespaceIdent::new("db".to_string());
    catalog.create_namespace(&ns, HashMap::new()).await.unwrap();
    catalog
        .register_table(
            &TableIdent::new(ns, table.to_string()),
            metadata_location(table),
        )
        .await
        .unwrap();
    let mut config = json!({
        "catalog": { "type": "rest", "uri": "http://127.0.0.1:1" },
        "table": format!("db.{table}"),
    });
    config
        .as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    let config: IcebergSourceConfig = serde_json::from_value(config).unwrap();
    IcebergSource::with_catalog(config, Arc::new(catalog)).unwrap()
}

fn ids(rows: &[Value]) -> Vec<i64> {
    let mut ids: Vec<i64> = rows.iter().map(|r| r["id"].as_i64().unwrap()).collect();
    ids.sort_unstable();
    ids
}

/// What Spark reads back from `db.dv` after its two deletes.
const LIVE_IDS: [i64; 5] = [1, 4, 5, 6, 8];

#[tokio::test(flavor = "multi_thread")]
async fn rows_removed_by_deletion_vectors_are_not_read() {
    let rows = source("dv", json!({})).await.fetch_all().await.unwrap();
    assert_eq!(ids(&rows), LIVE_IDS);
    assert!(rows.contains(&json!({"id": 8, "name": "h"})), "{rows:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn each_snapshot_reads_with_the_vectors_it_had() {
    install_fixture();
    let meta: Value =
        serde_json::from_str(&std::fs::read_to_string(metadata_location("dv")).unwrap()).unwrap();
    let snapshots: Vec<i64> = meta["snapshots"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["snapshot-id"].as_i64().unwrap())
        .collect();
    let expected: [&[i64]; 4] = [
        &[1, 2, 3, 4],
        &[1, 2, 3, 4, 5, 6, 7, 8],
        &[1, 3, 4, 5, 6, 8],
        &LIVE_IDS,
    ];
    for (snapshot, want) in snapshots.iter().zip(expected) {
        let rows = source("dv", json!({ "snapshot_id": snapshot }))
            .await
            .fetch_all()
            .await
            .unwrap();
        assert_eq!(ids(&rows), want, "snapshot {snapshot}");
    }
}

#[cfg(feature = "arrow")]
#[tokio::test(flavor = "multi_thread")]
async fn the_columnar_path_applies_deletion_vectors_too() {
    let src = source("dv", json!({})).await;
    let context = HashMap::new();
    let mut pages = src.stream_batches(&context, 0);
    let mut got = Vec::new();
    while let Some(page) = pages.next().await {
        let page = page.unwrap();
        got.extend(faucet_core::record_batch_to_values(&page.batch).unwrap());
    }
    assert_eq!(ids(&got), LIVE_IDS);
}

/// `db.upgraded` was written as format v2 with Parquet position deletes and
/// then upgraded to v3: one data file keeps its Parquet position delete, the
/// other has a deletion vector that took over the earlier delete.
#[tokio::test(flavor = "multi_thread")]
async fn a_table_with_both_kinds_of_position_delete_reads_what_spark_reads() {
    let rows = source("upgraded", json!({}))
        .await
        .fetch_all()
        .await
        .unwrap();
    assert_eq!(ids(&rows), [1, 4, 5, 7, 8]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_filter_and_a_shard_keep_the_vectors_applied() {
    let rows = source("dv", json!({ "filter": "id >= 2 and id <= 7" }))
        .await
        .fetch_all()
        .await
        .unwrap();
    assert_eq!(ids(&rows), [4, 5, 6]);

    let mut all = Vec::new();
    let shards = source("dv", json!({}))
        .await
        .enumerate_shards(2)
        .await
        .unwrap();
    assert_eq!(shards.len(), 2);
    for shard in &shards {
        let src = source("dv", json!({})).await;
        src.apply_shard(shard).await.unwrap();
        all.extend(src.fetch_all().await.unwrap());
    }
    assert_eq!(ids(&all), LIVE_IDS);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_incremental_run_meets_the_delete_snapshots_through_on_rewrite() {
    let fail = source("dv", json!({ "mode": "incremental" })).await;
    let rows = fail.fetch_all().await.unwrap();
    assert_eq!(
        ids(&rows),
        LIVE_IDS,
        "the first run reads the table as it is"
    );

    let meta: Value =
        serde_json::from_str(&std::fs::read_to_string(metadata_location("dv")).unwrap()).unwrap();
    let second = meta["snapshots"][1]["snapshot-id"].as_i64().unwrap();
    let from_second = |on_rewrite: &'static str| async move {
        let src = source(
            "dv",
            json!({ "mode": "incremental", "on_rewrite": on_rewrite }),
        )
        .await;
        src.apply_start_bookmark(json!({ "snapshot_id": second }))
            .await
            .unwrap();
        src.fetch_all().await
    };
    let err = from_second("fail").await.unwrap_err().to_string();
    assert!(err.contains("on_rewrite"), "{err}");
    assert_eq!(ids(&from_second("full_refresh").await.unwrap()), LIVE_IDS);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_variant_column_is_refused_before_anything_is_read() {
    for extra in [json!({}), json!({ "columns": ["id", "payload"] })] {
        let src = source("variant", extra).await;
        let err = src.fetch_all().await.unwrap_err().to_string();
        assert!(err.contains(r#"["payload"]"#), "{err}");
        assert!(err.contains(r#"readable: ["id"]"#), "{err}");
        assert!(err.contains("nothing was read"), "{err}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_other_columns_of_a_variant_table_read_with_deletes_and_a_filter() {
    let rows = source("variant", json!({ "columns": ["id"] }))
        .await
        .fetch_all()
        .await
        .unwrap();
    assert_eq!(ids(&rows), [1, 2], "id 3 was deleted");
    assert!(
        rows.iter().all(|r| r.as_object().unwrap().len() == 1),
        "{rows:?}"
    );
    let rows = source("variant", json!({ "columns": ["id"], "filter": "id > 1" }))
        .await
        .fetch_all()
        .await
        .unwrap();
    assert_eq!(rows, [json!({"id": 2})]);
}

#[tokio::test(flavor = "multi_thread")]
async fn doctor_reports_the_variant_column_and_passes_without_it() {
    use faucet_core::check::{CheckContext, ProbeStatus};
    let report = source("variant", json!({}))
        .await
        .check(&CheckContext::default())
        .await
        .unwrap();
    match &report.probes[0].status {
        ProbeStatus::Fail { reason } => assert!(reason.contains(r#"["payload"]"#), "{reason}"),
        other => panic!("{other:?}"),
    }
    let report = source("variant", json!({ "columns": ["id"] }))
        .await
        .check(&CheckContext::default())
        .await
        .unwrap();
    assert!(
        matches!(report.probes[0].status, ProbeStatus::Pass),
        "{report:?}"
    );
}

/// A storage refresh starts a new reader, which loads the delete files of its
/// own data files again: deleted rows stay deleted across the boundary, and
/// no row is read twice or left out (#1117).
#[tokio::test(flavor = "multi_thread")]
async fn deletes_stay_applied_when_the_storage_is_refreshed_between_data_files() {
    for (table, want) in [("dv", LIVE_IDS), ("upgraded", [1, 4, 5, 7, 8])] {
        for concurrency in [1, 4] {
            let src = source(
                table,
                json!({ "credential_refresh_secs": 0, "concurrency": concurrency }),
            )
            .await
            .with_storage_refresh_forced();
            let rows = src.fetch_all().await.unwrap();
            assert_eq!(ids(&rows), want, "{table}, concurrency {concurrency}");
        }
    }
}
