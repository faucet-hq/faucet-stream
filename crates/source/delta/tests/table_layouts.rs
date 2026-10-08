//! #789 FILE-17 / FILE-18 / FILE-39 / FILE-49: partition values come from the
//! log, column-mapped tables are refused, nested columns project, and
//! `batch_size: 0` reads one page per file.

use std::sync::Arc;

use arrow::array::{Int64Array, StringArray, StructArray};
use arrow::datatypes::{DataType, Field, Fields, Schema};
use arrow::record_batch::RecordBatch;
use faucet_core::Source as _;
use faucet_source_delta::{DeltaSource, DeltaSourceConfig};
use futures::StreamExt;
use serde_json::{Value, json};

fn write_parquet(path: &std::path::Path, batch: &RecordBatch, row_group: usize) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let props = parquet::file::properties::WriterProperties::builder()
        .set_max_row_group_row_count(Some(row_group))
        .build();
    let file = std::fs::File::create(path).unwrap();
    let mut w = parquet::arrow::ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();
    w.write(batch).unwrap();
    w.close().unwrap();
}

fn write_log(
    dir: &std::path::Path,
    fields: Value,
    partitions: &[&str],
    config: Value,
    adds: &[(String, Value)],
    protocol: Value,
) {
    let log = dir.join("_delta_log");
    std::fs::create_dir_all(&log).unwrap();
    let schema_string = json!({"type": "struct", "fields": fields}).to_string();
    let mut lines = vec![
        json!({"protocol": protocol}),
        json!({"metaData": {
            "id": "5fba94ed-9794-4965-ba6e-6ee3c0d22af9",
            "format": {"provider": "parquet", "options": {}},
            "schemaString": schema_string,
            "partitionColumns": partitions,
            "configuration": config,
            "createdTime": 0
        }}),
    ];
    for (path, pv) in adds {
        let size = std::fs::metadata(dir.join(path)).unwrap().len();
        lines.push(json!({"add": {
            "path": path, "partitionValues": pv, "size": size,
            "modificationTime": 0, "dataChange": true
        }}));
    }
    let body: String = lines.iter().map(|l| format!("{l}\n")).collect();
    std::fs::write(log.join("00000000000000000000.json"), body).unwrap();
}

fn ids(n: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from((0..n).collect::<Vec<_>>()))],
    )
    .unwrap()
}

fn plain_protocol() -> Value {
    json!({"minReaderVersion": 1, "minWriterVersion": 2})
}

fn source(dir: &std::path::Path) -> DeltaSourceConfig {
    DeltaSourceConfig::new(dir.to_string_lossy())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partition_values_come_from_the_add_action_not_the_path() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(&dir.path().join("ab/random-prefix.parquet"), &ids(2), 1_000);
    write_log(
        dir.path(),
        json!([
            {"name": "id", "type": "long", "nullable": true, "metadata": {}},
            {"name": "region", "type": "string", "nullable": true, "metadata": {}},
            {"name": "n", "type": "long", "nullable": true, "metadata": {}}
        ]),
        &["region", "n"],
        json!({}),
        &[(
            "ab/random-prefix.parquet".into(),
            json!({"region": "eu/west", "n": "7"}),
        )],
        plain_protocol(),
    );
    let rows = DeltaSource::new(source(dir.path()))
        .await
        .unwrap()
        .fetch_all()
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec![
            json!({"id": 0, "region": "eu/west", "n": 7}),
            json!({"id": 1, "region": "eu/west", "n": 7})
        ]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_column_mapped_table_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(&dir.path().join("part-0.parquet"), &ids(1), 1_000);
    write_log(
        dir.path(),
        json!([{"name": "id", "type": "long", "nullable": true, "metadata": {
            "delta.columnMapping.id": 1, "delta.columnMapping.physicalName": "id"
        }}]),
        &[],
        json!({"delta.columnMapping.mode": "name", "delta.columnMapping.maxColumnId": "1"}),
        &[("part-0.parquet".into(), json!({}))],
        json!({"minReaderVersion": 2, "minWriterVersion": 5}),
    );
    let err = DeltaSource::new(source(dir.path()))
        .await
        .unwrap()
        .fetch_all()
        .await
        .unwrap_err();
    assert!(err.to_string().contains("column mapping"), "{err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nested_columns_project_and_unknown_names_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    let inner = Fields::from(vec![Field::new("city", DataType::Utf8, true)]);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("addr", DataType::Struct(inner.clone()), true),
    ]));
    let addr = StructArray::new(inner, vec![Arc::new(StringArray::from(vec!["Oslo"]))], None);
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![1])), Arc::new(addr)],
    )
    .unwrap();
    write_parquet(&dir.path().join("part-0.parquet"), &batch, 1_000);
    write_log(
        dir.path(),
        json!([
            {"name": "id", "type": "long", "nullable": true, "metadata": {}},
            {"name": "addr", "type": {"type": "struct", "fields": [
                {"name": "city", "type": "string", "nullable": true, "metadata": {}}
            ]}, "nullable": true, "metadata": {}}
        ]),
        &[],
        json!({}),
        &[("part-0.parquet".into(), json!({}))],
        plain_protocol(),
    );
    let mut cfg = source(dir.path());
    cfg.columns = vec!["addr".into()];
    let rows = DeltaSource::new(cfg)
        .await
        .unwrap()
        .fetch_all()
        .await
        .unwrap();
    assert_eq!(rows, vec![json!({"addr": {"city": "Oslo"}})]);

    let mut cfg = source(dir.path());
    cfg.columns = vec!["adress".into()];
    let err = DeltaSource::new(cfg)
        .await
        .unwrap()
        .fetch_all()
        .await
        .unwrap_err();
    assert!(err.to_string().contains("adress"), "{err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_size_zero_reads_one_page_per_file() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(&dir.path().join("part-0.parquet"), &ids(3_000), 1_000);
    write_log(
        dir.path(),
        json!([{"name": "id", "type": "long", "nullable": true, "metadata": {}}]),
        &[],
        json!({}),
        &[("part-0.parquet".into(), json!({}))],
        plain_protocol(),
    );
    let mut cfg = source(dir.path());
    cfg.batch_size = 0;
    let src = DeltaSource::new(cfg).await.unwrap();
    let ctx = std::collections::HashMap::new();
    let pages: Vec<usize> = src
        .stream_pages(&ctx, 0)
        .map(|p| p.unwrap().records.len())
        .collect()
        .await;
    assert_eq!(pages, vec![3_000]);
    #[cfg(feature = "arrow")]
    {
        let batches: Vec<usize> = src
            .stream_batches(&ctx, 0)
            .map(|p| p.unwrap().batch.num_rows())
            .collect()
            .await;
        assert_eq!(batches, vec![3_000]);
    }
}
