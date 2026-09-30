//! End-to-end round-trip tests against a real Delta table on the local
//! filesystem: write with `DeltaSink`, read back with `DeltaSource`, assert
//! row-count and content parity. No Docker / object store required.

use std::collections::HashMap;

use faucet_core::{Sink, Source};
use faucet_sink_delta::{DeltaSink, DeltaSinkConfig};
use faucet_source_delta::{DeltaSource, DeltaSourceConfig};
use serde_json::{Value, json};

fn table_uri(dir: &tempfile::TempDir, name: &str) -> String {
    // Bare absolute path — `ensure_table_uri` promotes it to file://.
    dir.path().join(name).to_string_lossy().into_owned()
}

async fn read_all(uri: &str, cfg: impl FnOnce(&mut DeltaSourceConfig)) -> Vec<Value> {
    let mut sc = DeltaSourceConfig::new(uri);
    cfg(&mut sc);
    let source = DeltaSource::new(sc).await.expect("source");
    source
        .fetch_with_context(&HashMap::new())
        .await
        .expect("read")
}

#[tokio::test]
async fn append_new_table_then_read_back() {
    let dir = tempfile::tempdir().unwrap();
    let uri = table_uri(&dir, "events");

    let sink = DeltaSink::new(DeltaSinkConfig::new(&uri)).await.unwrap();
    let batch = vec![
        json!({"id": 1, "name": "alice"}),
        json!({"id": 2, "name": "bob"}),
    ];
    let n = sink.write_batch(&batch).await.unwrap();
    assert_eq!(n, 2);
    sink.flush().await.unwrap();

    let rows = read_all(&uri, |_| {}).await;
    assert_eq!(rows.len(), 2, "row-count parity");
    let ids: Vec<i64> = rows.iter().map(|r| r["id"].as_i64().unwrap()).collect();
    assert!(ids.contains(&1) && ids.contains(&2));
}

#[tokio::test]
async fn two_flushes_accumulate_across_commits() {
    let dir = tempfile::tempdir().unwrap();
    let uri = table_uri(&dir, "multi");

    let sink = DeltaSink::new(DeltaSinkConfig::new(&uri)).await.unwrap();
    sink.write_batch(&[json!({"id": 1})]).await.unwrap();
    sink.flush().await.unwrap();
    sink.write_batch(&[json!({"id": 2}), json!({"id": 3})])
        .await
        .unwrap();
    sink.flush().await.unwrap();

    let rows = read_all(&uri, |_| {}).await;
    assert_eq!(rows.len(), 3, "both commits visible");
}

#[tokio::test]
async fn append_to_existing_table_across_sink_instances() {
    let dir = tempfile::tempdir().unwrap();
    let uri = table_uri(&dir, "existing");

    {
        let sink = DeltaSink::new(DeltaSinkConfig::new(&uri)).await.unwrap();
        sink.write_batch(&[json!({"id": 1})]).await.unwrap();
        sink.flush().await.unwrap();
    }
    // A brand-new sink instance opens the existing table and appends.
    {
        let mut cfg = DeltaSinkConfig::new(&uri);
        cfg.create_if_not_missing = false;
        let sink = DeltaSink::new(cfg).await.unwrap();
        sink.write_batch(&[json!({"id": 2})]).await.unwrap();
        sink.flush().await.unwrap();
    }

    let rows = read_all(&uri, |_| {}).await;
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn partitioned_table_round_trips_partition_column() {
    let dir = tempfile::tempdir().unwrap();
    let uri = table_uri(&dir, "part");

    let mut cfg = DeltaSinkConfig::new(&uri);
    cfg.partition_by = vec!["region".into()];
    let sink = DeltaSink::new(cfg).await.unwrap();
    sink.write_batch(&[
        json!({"id": 1, "region": "us"}),
        json!({"id": 2, "region": "eu"}),
        json!({"id": 3, "region": "us"}),
    ])
    .await
    .unwrap();
    sink.flush().await.unwrap();

    let rows = read_all(&uri, |_| {}).await;
    assert_eq!(rows.len(), 3);
    // Partition column (stored in the path, not the file) is reconstructed.
    let us = rows.iter().filter(|r| r["region"] == json!("us")).count();
    let eu = rows.iter().filter(|r| r["region"] == json!("eu")).count();
    assert_eq!(us, 2);
    assert_eq!(eu, 1);
}

#[tokio::test]
async fn projection_limits_columns() {
    let dir = tempfile::tempdir().unwrap();
    let uri = table_uri(&dir, "proj");

    let sink = DeltaSink::new(DeltaSinkConfig::new(&uri)).await.unwrap();
    sink.write_batch(&[json!({"id": 1, "name": "a", "extra": "x"})])
        .await
        .unwrap();
    sink.flush().await.unwrap();

    let rows = read_all(&uri, |c| c.columns = vec!["id".into()]).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["id"], json!(1));
    assert!(rows[0].get("name").is_none());
    assert!(rows[0].get("extra").is_none());
}

#[tokio::test]
async fn time_travel_by_version() {
    let dir = tempfile::tempdir().unwrap();
    let uri = table_uri(&dir, "tt");

    let sink = DeltaSink::new(DeltaSinkConfig::new(&uri)).await.unwrap();
    sink.write_batch(&[json!({"id": 1})]).await.unwrap();
    sink.flush().await.unwrap(); // version 1 (create = 0)
    sink.write_batch(&[json!({"id": 2})]).await.unwrap();
    sink.flush().await.unwrap(); // version 2

    // Latest sees both rows.
    let latest = read_all(&uri, |_| {}).await;
    assert_eq!(latest.len(), 2);

    // Version 1 (first data commit) sees only the first row.
    let v1 = read_all(&uri, |c| c.version = Some(1)).await;
    assert_eq!(v1.len(), 1);
    assert_eq!(v1[0]["id"], json!(1));
}

#[tokio::test]
async fn source_check_pass_and_fail() {
    use faucet_core::check::{CheckContext, ProbeStatus};
    let dir = tempfile::tempdir().unwrap();
    let uri = table_uri(&dir, "srcchk");

    // Missing table → probe fails.
    let missing = DeltaSource::new(DeltaSourceConfig::new(&uri))
        .await
        .unwrap();
    let report = missing.check(&CheckContext::default()).await.unwrap();
    assert!(
        report
            .probes
            .iter()
            .any(|p| matches!(p.status, ProbeStatus::Fail { .. })),
        "source check should fail on a missing table"
    );

    // After a write the table exists → probe passes.
    let sink = DeltaSink::new(DeltaSinkConfig::new(&uri)).await.unwrap();
    sink.write_batch(&[json!({"id": 1})]).await.unwrap();
    sink.flush().await.unwrap();
    let source = DeltaSource::new(DeltaSourceConfig::new(&uri))
        .await
        .unwrap();
    let report = source.check(&CheckContext::default()).await.unwrap();
    assert!(
        report
            .probes
            .iter()
            .all(|p| matches!(p.status, ProbeStatus::Pass)),
        "source check should pass on an existing table: {report:?}"
    );
}

#[tokio::test]
async fn time_travel_by_timestamp() {
    let dir = tempfile::tempdir().unwrap();
    let uri = table_uri(&dir, "ts");
    let sink = DeltaSink::new(DeltaSinkConfig::new(&uri)).await.unwrap();
    sink.write_batch(&[json!({"id": 1})]).await.unwrap();
    sink.flush().await.unwrap();

    // A far-future timestamp resolves to the latest version (all rows).
    let rows = read_all(&uri, |c| c.timestamp = Some("2999-01-01T00:00:00Z".into())).await;
    assert_eq!(rows.len(), 1);
}

#[tokio::test]
async fn invalid_timestamp_errors() {
    let dir = tempfile::tempdir().unwrap();
    let uri = table_uri(&dir, "badts");
    let sink = DeltaSink::new(DeltaSinkConfig::new(&uri)).await.unwrap();
    sink.write_batch(&[json!({"id": 1})]).await.unwrap();
    sink.flush().await.unwrap();

    let mut sc = DeltaSourceConfig::new(&uri);
    sc.timestamp = Some("not-a-timestamp".into());
    let source = DeltaSource::new(sc).await.unwrap();
    let err = source
        .fetch_with_context(&HashMap::new())
        .await
        .unwrap_err();
    assert!(
        err.to_string().to_lowercase().contains("timestamp"),
        "{err}"
    );
}

/// Read a table back through the source's **columnar** (`stream_batches`) path,
/// converting each `RecordBatch` to JSON for assertions.
#[cfg(feature = "arrow")]
async fn read_all_columnar(uri: &str, cfg: impl FnOnce(&mut DeltaSourceConfig)) -> Vec<Value> {
    use futures::StreamExt;
    let mut sc = DeltaSourceConfig::new(uri);
    cfg(&mut sc);
    let source = DeltaSource::new(sc).await.expect("source");
    assert!(
        source.supports_columnar(),
        "delta source is columnar-capable"
    );
    let ctx = HashMap::new();
    let mut stream = source.stream_batches(&ctx, 0);
    let mut out = Vec::new();
    while let Some(page) = stream.next().await {
        let page = page.expect("columnar page");
        out.extend(
            faucet_core::columnar::record_batch_to_values(&page.batch).expect("batch to values"),
        );
    }
    out
}

/// Arrow end-to-end: write a `RecordBatch` via `write_batch_columnar` and read
/// it back via `stream_batches` — no `Value` materialization on either side.
#[cfg(feature = "arrow")]
#[tokio::test]
async fn columnar_round_trip_unpartitioned() {
    let dir = tempfile::tempdir().unwrap();
    let uri = table_uri(&dir, "colz");

    let sink = DeltaSink::new(DeltaSinkConfig::new(&uri)).await.unwrap();
    assert!(sink.supports_columnar(), "delta sink is columnar-capable");
    let batch = faucet_core::columnar::values_to_record_batch_inferred(&[
        json!({"id": 1, "name": "alice"}),
        json!({"id": 2, "name": "bob"}),
    ])
    .unwrap();
    let n = sink.write_batch_columnar(&batch).await.unwrap();
    assert_eq!(n, 2);
    sink.flush().await.unwrap();

    let rows = read_all_columnar(&uri, |_| {}).await;
    assert_eq!(rows.len(), 2);
    let ids: Vec<i64> = rows.iter().map(|r| r["id"].as_i64().unwrap()).collect();
    assert!(ids.contains(&1) && ids.contains(&2));
    assert_eq!(
        rows.iter().find(|r| r["id"] == json!(1)).unwrap()["name"],
        json!("alice")
    );
}

/// Columnar path over a partitioned table: the partition column lives in the
/// file path, so the source must re-append it as a constant Arrow column
/// (`append_partition_columns`) for the columnar output to match the row path.
#[cfg(feature = "arrow")]
#[tokio::test]
async fn columnar_round_trip_partitioned() {
    let dir = tempfile::tempdir().unwrap();
    let uri = table_uri(&dir, "colzpart");

    let mut cfg = DeltaSinkConfig::new(&uri);
    cfg.partition_by = vec!["region".into()];
    let sink = DeltaSink::new(cfg).await.unwrap();
    let batch = faucet_core::columnar::values_to_record_batch_inferred(&[
        json!({"id": 1, "region": "us"}),
        json!({"id": 2, "region": "eu"}),
        json!({"id": 3, "region": "us"}),
    ])
    .unwrap();
    sink.write_batch_columnar(&batch).await.unwrap();
    sink.flush().await.unwrap();

    let rows = read_all_columnar(&uri, |_| {}).await;
    assert_eq!(rows.len(), 3);
    // Partition column reconstructed into the columnar batch.
    assert_eq!(
        rows.iter().filter(|r| r["region"] == json!("us")).count(),
        2
    );
    assert_eq!(
        rows.iter().filter(|r| r["region"] == json!("eu")).count(),
        1
    );
    // And a projection that includes the partition column still works.
    let only = read_all_columnar(&uri, |c| c.columns = vec!["region".into()]).await;
    assert!(
        only.iter()
            .all(|r| r.get("id").is_none() && r.get("region").is_some())
    );
}

#[tokio::test]
async fn unknown_field_is_dropped_not_fatal() {
    let dir = tempfile::tempdir().unwrap();
    let uri = table_uri(&dir, "drift");
    let sink = DeltaSink::new(DeltaSinkConfig::new(&uri)).await.unwrap();
    // Schema locked from the first record ({id}); a later record with an
    // extra field has that field dropped, not erroring the batch.
    sink.write_batch(&[json!({"id": 1})]).await.unwrap();
    sink.write_batch(&[json!({"id": 2, "surprise": "x"})])
        .await
        .unwrap();
    sink.flush().await.unwrap();

    let rows = read_all(&uri, |_| {}).await;
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|r| r.get("surprise").is_none()));
}
