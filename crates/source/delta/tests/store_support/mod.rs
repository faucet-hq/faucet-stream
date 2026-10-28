//! Shared by the object-store reads: commit a table with delta-rs (create,
//! then two appends), read it back with the source at the latest version and
//! at version 1.

use std::sync::Arc;

use deltalake::arrow::array::{Int64Array, StringArray};
use deltalake::arrow::datatypes::{DataType, Field, Schema};
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::kernel::StructType;
use deltalake::kernel::engine::arrow_conversion::TryIntoKernel;
use deltalake::operations::create::CreateBuilder;
use deltalake::writer::{DeltaWriter, RecordBatchWriter};
use faucet_common_delta::DeltaConnection;
use faucet_core::Source;
use faucet_source_delta::{DeltaSource, DeltaSourceConfig};
use serde_json::{Value, json};

pub fn connection(table_uri: &str, credentials: Value, storage_options: Value) -> DeltaConnection {
    serde_json::from_value(json!({
        "table_uri": table_uri,
        "credentials": credentials,
        "storage_options": storage_options,
    }))
    .expect("connection")
}

fn batch(schema: &Arc<Schema>, ids: std::ops::Range<i64>) -> RecordBatch {
    let names: Vec<String> = ids.clone().map(|i| format!("n{i}")).collect();
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids.collect::<Vec<_>>())),
            Arc::new(StringArray::from(names)),
        ],
    )
    .expect("batch")
}

async fn write_table(connection: &DeltaConnection) {
    connection.register_handlers();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
    ]));
    let kernel: StructType = schema.as_ref().try_into_kernel().expect("kernel schema");
    let mut table = CreateBuilder::new()
        .with_location(connection.location_string().expect("location"))
        .with_storage_options(connection.merged_storage_options())
        .with_columns(kernel.fields().cloned())
        .await
        .expect("create table");
    for ids in [0..30, 30..50] {
        let mut writer = RecordBatchWriter::for_table(&table).expect("writer");
        writer.write(batch(&schema, ids)).await.expect("write");
        writer.flush_and_commit(&mut table).await.expect("commit");
    }
    assert_eq!(table.version(), Some(2));
}

async fn read_ids(connection: &DeltaConnection, version: Option<u64>) -> Vec<i64> {
    let mut cfg = DeltaSourceConfig::new(connection.table_uri.clone());
    cfg.connection = connection.clone();
    cfg.version = version;
    let rows = DeltaSource::new(cfg)
        .await
        .expect("source")
        .fetch_all()
        .await
        .expect("read");
    let mut ids: Vec<i64> = rows
        .iter()
        .map(|r| {
            assert_eq!(r["name"], json!(format!("n{}", r["id"])), "{r}");
            r["id"].as_i64().expect("id")
        })
        .collect();
    ids.sort_unstable();
    ids
}

pub async fn assert_round_trip(connection: DeltaConnection) {
    write_table(&connection).await;
    assert_eq!(
        read_ids(&connection, None).await,
        (0..50).collect::<Vec<i64>>()
    );
    assert_eq!(
        read_ids(&connection, Some(1)).await,
        (0..30).collect::<Vec<i64>>()
    );
}
