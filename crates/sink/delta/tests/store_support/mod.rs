//! Shared by the object-store round trips: write two committed pages, then
//! reopen the table and read every data file back.

use deltalake::arrow::array::{Array, Int64Array};
use deltalake::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use faucet_common_delta::DeltaConnection;
use faucet_core::Sink;
use faucet_sink_delta::{DeltaSink, DeltaSinkConfig};
use object_store::ObjectStoreExt;
use serde_json::{Value, json};

pub fn sink_config(table_uri: &str, credentials: Value, storage_options: Value) -> DeltaSinkConfig {
    serde_json::from_value(json!({
        "table_uri": table_uri,
        "credentials": credentials,
        "storage_options": storage_options,
    }))
    .expect("sink config")
}

pub async fn write_two_commits(cfg: DeltaSinkConfig) {
    let sink = DeltaSink::new(cfg).await.expect("sink");
    let first: Vec<Value> = (0..30)
        .map(|i| json!({ "id": i, "name": format!("n{i}") }))
        .collect();
    let second: Vec<Value> = (30..50)
        .map(|i| json!({ "id": i, "name": format!("n{i}") }))
        .collect();
    assert_eq!(sink.write_batch(&first).await.expect("write 1"), 30);
    sink.flush().await.expect("flush 1");
    assert_eq!(sink.write_batch(&second).await.expect("write 2"), 20);
    sink.flush().await.expect("flush 2");
}

pub async fn read_back(connection: &DeltaConnection) -> (u64, Vec<i64>) {
    let table = connection.open().await.expect("reopen the table");
    let version = table.version().expect("a committed version") as u64;
    let root = table.table_url().to_string();
    let root = root.trim_end_matches('/').to_string();
    let store = table.object_store();
    let mut ids = Vec::new();
    let uris: Vec<String> = table.get_file_uris().expect("file uris").collect();
    assert!(!uris.is_empty(), "no data files committed");
    for uri in uris {
        let relative = uri
            .strip_prefix(&root)
            .unwrap_or_else(|| panic!("{uri} outside {root}"))
            .trim_start_matches('/')
            .to_string();
        let bytes = store
            .get(&deltalake::Path::from(relative.as_str()))
            .await
            .unwrap_or_else(|e| panic!("get {relative}: {e}"))
            .bytes()
            .await
            .expect("object body");
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .expect("parquet footer")
            .build()
            .expect("parquet reader");
        for batch in reader {
            let batch = batch.expect("row group");
            let col = batch
                .column_by_name("id")
                .expect("id column")
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id is int64");
            ids.extend((0..col.len()).map(|i| col.value(i)));
        }
    }
    ids.sort_unstable();
    (version, ids)
}

pub async fn assert_round_trip(cfg: DeltaSinkConfig) {
    let connection = cfg.connection.clone();
    write_two_commits(cfg).await;
    let (version, ids) = read_back(&connection).await;
    assert_eq!(version, 2, "create + two appends");
    assert_eq!(ids, (0..50).collect::<Vec<i64>>());
}
