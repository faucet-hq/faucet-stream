//! Delta sink tests against a real table on the local filesystem. The
//! round trips that read the table back with the Delta source live in
//! `crates/interop-tests/tests/delta_roundtrip.rs`.

use faucet_core::Sink;
use faucet_sink_delta::{DeltaSink, DeltaSinkConfig};
use serde_json::json;

fn table_uri(dir: &tempfile::TempDir, name: &str) -> String {
    // Bare absolute path — `ensure_table_uri` promotes it to file://.
    dir.path().join(name).to_string_lossy().into_owned()
}

#[tokio::test]
async fn missing_table_without_create_errors() {
    let dir = tempfile::tempdir().unwrap();
    let uri = table_uri(&dir, "nope");

    let mut cfg = DeltaSinkConfig::new(&uri);
    cfg.create_if_not_missing = false;
    let sink = DeltaSink::new(cfg).await.unwrap();
    let err = sink.write_batch(&[json!({"id": 1})]).await.unwrap_err();
    assert!(
        err.to_string().contains("does not exist"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn empty_batch_is_noop() {
    let dir = tempfile::tempdir().unwrap();
    let uri = table_uri(&dir, "empty");
    let sink = DeltaSink::new(DeltaSinkConfig::new(&uri)).await.unwrap();
    assert_eq!(sink.write_batch(&[]).await.unwrap(), 0);
    // Nothing buffered → flush is a clean no-op.
    sink.flush().await.unwrap();
}

#[tokio::test]
async fn sink_check_passes_on_reachable_store() {
    use faucet_core::check::{CheckContext, ProbeStatus};
    let dir = tempfile::tempdir().unwrap();
    let uri = table_uri(&dir, "chk");
    // Table need not exist — create_if_not_missing handles that at write time,
    // so a reachable (local) store passes.
    let sink = DeltaSink::new(DeltaSinkConfig::new(&uri)).await.unwrap();
    let report = sink.check(&CheckContext::default()).await.unwrap();
    assert!(
        report
            .probes
            .iter()
            .all(|p| matches!(p.status, ProbeStatus::Pass)),
        "sink check should pass: {report:?}"
    );
}

/// #789 FILE-13: a run against an existing table decodes with the table's
/// schema, so whole numbers into a `double` column are accepted.
#[tokio::test]
async fn an_existing_table_keeps_its_own_column_types() {
    let dir = tempfile::tempdir().unwrap();
    let uri = table_uri(&dir, "typed");
    let first = DeltaSink::new(DeltaSinkConfig::new(&uri)).await.unwrap();
    first.write_batch(&[json!({"x": 1.5})]).await.unwrap();
    first.flush().await.unwrap();

    let second = DeltaSink::new(DeltaSinkConfig::new(&uri)).await.unwrap();
    assert_eq!(second.write_batch(&[json!({"x": 2})]).await.unwrap(), 1);
    second.flush().await.unwrap();
}

/// #789 FILE-12: a fractional value bound for an integer column fails the
/// write instead of being truncated.
#[tokio::test]
async fn a_fractional_value_in_an_integer_column_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let uri = table_uri(&dir, "ints");
    let sink = DeltaSink::new(DeltaSinkConfig::new(&uri)).await.unwrap();
    sink.write_batch(&[json!({"n": 1})]).await.unwrap();
    let err = sink.write_batch(&[json!({"n": 10.5})]).await.unwrap_err();
    assert!(err.to_string().contains("'n'"), "{err}");
}

fn committed_rows(dir: &std::path::Path) -> i64 {
    use deltalake::parquet::file::reader::{FileReader, SerializedFileReader};
    let mut rows = 0;
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_some_and(|e| e == "parquet") {
            let reader = SerializedFileReader::new(std::fs::File::open(&path).unwrap()).unwrap();
            rows += reader.metadata().file_metadata().num_rows();
        }
    }
    rows
}

/// #789 FILE-40: a page that fails in a later chunk leaves nothing of itself
/// buffered, so the next flush cannot commit its earlier chunks.
#[tokio::test]
async fn a_page_failing_in_a_later_chunk_buffers_none_of_it() {
    let dir = tempfile::tempdir().unwrap();
    let uri = table_uri(&dir, "chunks");
    let mut cfg = DeltaSinkConfig::new(&uri);
    cfg.batch_size = 2;
    let sink = DeltaSink::new(cfg).await.unwrap();
    sink.write_batch(&[json!({"id": 1})]).await.unwrap();
    let page = [
        json!({"id": 2}),
        json!({"id": 3}),
        json!({"id": 4}),
        json!({"id": "not-a-number"}),
    ];
    assert!(sink.write_batch(&page).await.is_err());
    sink.flush().await.unwrap();
    assert_eq!(committed_rows(&dir.path().join("chunks")), 1);
}

/// #789 FILE-41: a column null in every sampled record creates the table as
/// a nullable string column instead of failing the create.
#[tokio::test]
async fn a_column_null_in_the_sample_creates_the_table() {
    let dir = tempfile::tempdir().unwrap();
    let uri = table_uri(&dir, "sparse");
    let sink = DeltaSink::new(DeltaSinkConfig::new(&uri)).await.unwrap();
    sink.write_batch(&[json!({"id": 1, "deleted_at": null})])
        .await
        .unwrap();
    sink.write_batch(&[json!({"id": 2, "deleted_at": "2026-10-01"})])
        .await
        .unwrap();
    sink.flush().await.unwrap();
    assert_eq!(committed_rows(&dir.path().join("sparse")), 2);
}
