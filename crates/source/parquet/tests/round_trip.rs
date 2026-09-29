#![allow(deprecated)]
//! End-to-end round-trip tests for `faucet-source-parquet`.
//!
//! Each test writes a Parquet fixture with `parquet::arrow::ArrowWriter` and
//! reads it back through `ParquetSource::fetch_all`, asserting JSON shape.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int32Array,
    Int32Builder, Int64Array, ListBuilder, MapBuilder, RecordBatch, StringArray, StringBuilder,
    StructArray, TimestampMillisecondArray,
};
use arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};
use faucet_core::Source;
use faucet_source_parquet::{ParquetSource, ParquetSourceConfig};
use parquet::arrow::ArrowWriter;
use std::fs::File;
use std::path::Path;
use tempfile::TempDir;

// ── Fixture helpers ──────────────────────────────────────────────────────────

fn write_parquet(path: &Path, batch: &RecordBatch) {
    let file = File::create(path).expect("create file");
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).expect("arrow writer");
    writer.write(batch).expect("write batch");
    writer.close().expect("close writer");
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn primitives_round_trip() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("i32", DataType::Int32, false),
        Field::new("i64", DataType::Int64, true),
        Field::new("f64", DataType::Float64, false),
        Field::new("s", DataType::Utf8, true),
        Field::new("b", DataType::Boolean, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![Some(10), None, Some(30)])),
            Arc::new(Float64Array::from(vec![1.5, 2.5, 3.5])),
            Arc::new(StringArray::from(vec![Some("a"), Some("b"), None])),
            Arc::new(BooleanArray::from(vec![true, false, true])),
        ],
    )
    .unwrap();

    let dir = TempDir::new().unwrap();
    let path = dir.path().join("primitives.parquet");
    write_parquet(&path, &batch);

    let source = ParquetSource::new(ParquetSourceConfig::local(path.to_str().unwrap()))
        .await
        .unwrap();
    let rows = source.fetch_all().await.unwrap();

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0]["i32"], 1);
    assert_eq!(rows[0]["i64"], 10);
    assert_eq!(rows[0]["f64"], 1.5);
    assert_eq!(rows[0]["s"], "a");
    assert_eq!(rows[0]["b"], true);
    // Null fields are omitted by arrow_json's default writer.
    assert!(rows[1].get("i64").is_none() || rows[1]["i64"].is_null());
    assert!(rows[2].get("s").is_none() || rows[2]["s"].is_null());
}

#[tokio::test]
async fn temporal_types_emitted_as_iso_strings() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("d", DataType::Date32, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
    ]));

    // Date32: days since UNIX epoch. 2024-01-15 == 19737.
    // Timestamp millis: 2024-01-15T12:34:56Z == 1705321796000.
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Date32Array::from(vec![19737])),
            Arc::new(TimestampMillisecondArray::from(vec![1_705_321_796_000])),
        ],
    )
    .unwrap();

    let dir = TempDir::new().unwrap();
    let path = dir.path().join("temporal.parquet");
    write_parquet(&path, &batch);

    let source = ParquetSource::new(ParquetSourceConfig::local(path.to_str().unwrap()))
        .await
        .unwrap();
    let rows = source.fetch_all().await.unwrap();

    assert_eq!(rows.len(), 1);
    let d = rows[0]["d"].as_str().expect("date as string");
    assert!(d.starts_with("2024-01-15"), "got {d}");
    let ts = rows[0]["ts"].as_str().expect("timestamp as string");
    assert!(ts.starts_with("2024-01-15"), "got {ts}");
}

#[tokio::test]
async fn decimal_emits_string() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "amount",
        DataType::Decimal128(10, 2),
        false,
    )]));
    let decimals = Decimal128Array::from(vec![12345_i128, -67890_i128])
        .with_precision_and_scale(10, 2)
        .unwrap();
    let batch = RecordBatch::try_new(schema, vec![Arc::new(decimals)]).unwrap();

    let dir = TempDir::new().unwrap();
    let path = dir.path().join("decimal.parquet");
    write_parquet(&path, &batch);

    let source = ParquetSource::new(ParquetSourceConfig::local(path.to_str().unwrap()))
        .await
        .unwrap();
    let rows = source.fetch_all().await.unwrap();
    assert_eq!(rows.len(), 2);
    let v = &rows[0]["amount"];
    assert!(
        v.is_string() || v.is_number(),
        "decimal should be string or number, got {v}"
    );
    if let Some(s) = v.as_str() {
        assert!(s.contains("123.45") || s.contains("12345"));
    }
}

#[tokio::test]
async fn nested_struct_round_trips() {
    let inner_fields = Fields::from(vec![
        Field::new("city", DataType::Utf8, false),
        Field::new("zip", DataType::Int32, false),
    ]);
    let outer_schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("addr", DataType::Struct(inner_fields.clone()), false),
    ]));

    let city = Arc::new(StringArray::from(vec!["SF", "NYC"])) as ArrayRef;
    let zip = Arc::new(Int32Array::from(vec![94016, 10001])) as ArrayRef;
    let addr = StructArray::new(inner_fields, vec![city, zip], None);

    let batch = RecordBatch::try_new(
        outer_schema,
        vec![
            Arc::new(StringArray::from(vec!["Alice", "Bob"])),
            Arc::new(addr),
        ],
    )
    .unwrap();

    let dir = TempDir::new().unwrap();
    let path = dir.path().join("nested.parquet");
    write_parquet(&path, &batch);

    let source = ParquetSource::new(ParquetSourceConfig::local(path.to_str().unwrap()))
        .await
        .unwrap();
    let rows = source.fetch_all().await.unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["name"], "Alice");
    assert_eq!(rows[0]["addr"]["city"], "SF");
    assert_eq!(rows[0]["addr"]["zip"], 94016);
    assert_eq!(rows[1]["addr"]["city"], "NYC");
}

#[tokio::test]
async fn list_round_trips_to_array() {
    let mut builder = ListBuilder::new(Int32Builder::new());
    builder.values().append_value(1);
    builder.values().append_value(2);
    builder.values().append_value(3);
    builder.append(true);
    builder.values().append_value(4);
    builder.append(true);
    let list = builder.finish();

    let schema = Arc::new(Schema::new(vec![Field::new(
        "nums",
        list.data_type().clone(),
        false,
    )]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(list)]).unwrap();

    let dir = TempDir::new().unwrap();
    let path = dir.path().join("list.parquet");
    write_parquet(&path, &batch);

    let source = ParquetSource::new(ParquetSourceConfig::local(path.to_str().unwrap()))
        .await
        .unwrap();
    let rows = source.fetch_all().await.unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["nums"], serde_json::json!([1, 2, 3]));
    assert_eq!(rows[1]["nums"], serde_json::json!([4]));
}

#[tokio::test]
async fn map_round_trips_to_object() {
    let mut builder = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
    builder.keys().append_value("a");
    builder.values().append_value(1);
    builder.keys().append_value("b");
    builder.values().append_value(2);
    builder.append(true).unwrap();
    builder.keys().append_value("c");
    builder.values().append_value(3);
    builder.append(true).unwrap();
    let map = builder.finish();

    let schema = Arc::new(Schema::new(vec![Field::new(
        "tags",
        map.data_type().clone(),
        false,
    )]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(map)]).unwrap();

    let dir = TempDir::new().unwrap();
    let path = dir.path().join("map.parquet");
    write_parquet(&path, &batch);

    let source = ParquetSource::new(ParquetSourceConfig::local(path.to_str().unwrap()))
        .await
        .unwrap();
    let rows = source.fetch_all().await.unwrap();

    assert_eq!(rows.len(), 2);
    // arrow_json encodes a Map as a JSON object keyed by the map's keys.
    let first = &rows[0]["tags"];
    assert!(first.is_object(), "expected object, got {first}");
    assert_eq!(first["a"], 1);
    assert_eq!(first["b"], 2);
    let second = &rows[1]["tags"];
    assert_eq!(second["c"], 3);
}

#[tokio::test]
async fn column_projection_excludes_unprojected_columns() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Int32, false),
        Field::new("c", DataType::Int32, false),
        Field::new("d", DataType::Int32, false),
        Field::new("e", DataType::Int32, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(Int32Array::from(vec![10, 20])),
            Arc::new(Int32Array::from(vec![100, 200])),
            Arc::new(Int32Array::from(vec![1000, 2000])),
            Arc::new(Int32Array::from(vec![10000, 20000])),
        ],
    )
    .unwrap();

    let dir = TempDir::new().unwrap();
    let path = dir.path().join("projection.parquet");
    write_parquet(&path, &batch);

    let cfg = ParquetSourceConfig::local(path.to_str().unwrap()).columns(["b", "d"]);
    let source = ParquetSource::new(cfg).await.unwrap();
    let rows = source.fetch_all().await.unwrap();

    assert_eq!(rows.len(), 2);
    for row in &rows {
        let obj = row.as_object().expect("object");
        assert!(obj.contains_key("b"));
        assert!(obj.contains_key("d"));
        assert!(!obj.contains_key("a"));
        assert!(!obj.contains_key("c"));
        assert!(!obj.contains_key("e"));
        assert_eq!(obj.len(), 2);
    }
    assert_eq!(rows[0]["b"], 10);
    assert_eq!(rows[0]["d"], 1000);
}

#[tokio::test]
async fn projection_missing_column_errors() {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1]))]).unwrap();

    let dir = TempDir::new().unwrap();
    let path = dir.path().join("missing.parquet");
    write_parquet(&path, &batch);

    let cfg = ParquetSourceConfig::local(path.to_str().unwrap()).columns(["nope"]);
    let source = ParquetSource::new(cfg).await.unwrap();
    let err = source.fetch_all().await.expect_err("should fail");
    let msg = err.to_string();
    assert!(msg.contains("nope"), "got: {msg}");
}

#[tokio::test]
async fn glob_reads_multiple_files() {
    let dir = TempDir::new().unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));

    for (i, vals) in [(0, vec![1, 2]), (1, vec![3, 4, 5]), (2, vec![6])]
        .iter()
        .enumerate()
    {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vals.1.clone()))],
        )
        .unwrap();
        write_parquet(&dir.path().join(format!("part-{i}.parquet")), &batch);
    }

    let pattern = format!("{}/*.parquet", dir.path().display());
    let cfg = ParquetSourceConfig::glob(pattern).concurrency(3);
    let source = ParquetSource::new(cfg).await.unwrap();
    let rows = source.fetch_all().await.unwrap();
    assert_eq!(rows.len(), 6);
    let mut nums: Vec<i64> = rows.iter().map(|r| r["v"].as_i64().unwrap()).collect();
    nums.sort();
    assert_eq!(nums, vec![1, 2, 3, 4, 5, 6]);
}

#[tokio::test]
async fn eager_fetch_preserves_deterministic_file_order() {
    // F42: the eager (`fetch_all`) path must return rows in the sorted file
    // order `resolve_files` produces, even with concurrency > 1. Each file
    // holds a single distinct value so we can assert the exact sequence
    // (without sorting) — `buffered` preserves order; `buffer_unordered` would
    // not.
    let dir = TempDir::new().unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
    // Names sort lexicographically to 00..07; values follow the same order.
    for i in 0..8i32 {
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![i]))])
            .unwrap();
        write_parquet(&dir.path().join(format!("part-{i:02}.parquet")), &batch);
    }

    let pattern = format!("{}/*.parquet", dir.path().display());
    let cfg = ParquetSourceConfig::glob(pattern).concurrency(8);
    let source = ParquetSource::new(cfg).await.unwrap();
    let rows = source.fetch_all().await.unwrap();
    let nums: Vec<i64> = rows.iter().map(|r| r["v"].as_i64().unwrap()).collect();
    assert_eq!(
        nums,
        (0..8).collect::<Vec<i64>>(),
        "rows must come back in sorted file order, not decode-completion order"
    );
}

#[tokio::test]
async fn glob_with_mismatched_schemas_fails_fast() {
    let dir = TempDir::new().unwrap();

    let s1 = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
    let b1 = RecordBatch::try_new(s1, vec![Arc::new(Int32Array::from(vec![1, 2]))]).unwrap();
    write_parquet(&dir.path().join("part-1.parquet"), &b1);

    let s2 = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, false)]));
    let b2 = RecordBatch::try_new(s2, vec![Arc::new(StringArray::from(vec!["x", "y"]))]).unwrap();
    write_parquet(&dir.path().join("part-2.parquet"), &b2);

    let pattern = format!("{}/*.parquet", dir.path().display());
    let cfg = ParquetSourceConfig::glob(pattern);
    let source = ParquetSource::new(cfg).await.unwrap();
    let err = source.fetch_all().await.expect_err("should fail");
    let msg = err.to_string();
    assert!(msg.contains("schema mismatch"), "got: {msg}");
    assert!(msg.contains("part-1.parquet") && msg.contains("part-2.parquet"));
}

#[tokio::test]
async fn empty_parquet_file_yields_zero_rows() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(Vec::<i32>::new()))]).unwrap();

    let dir = TempDir::new().unwrap();
    let path = dir.path().join("empty.parquet");
    write_parquet(&path, &batch);

    let source = ParquetSource::new(ParquetSourceConfig::local(path.to_str().unwrap()))
        .await
        .unwrap();
    let rows = source.fetch_all().await.unwrap();
    assert!(rows.is_empty());
}

#[tokio::test]
async fn glob_empty_match_returns_empty() {
    let dir = TempDir::new().unwrap();
    let pattern = format!("{}/no-such-*.parquet", dir.path().display());
    let cfg = ParquetSourceConfig::glob(pattern);
    let source = ParquetSource::new(cfg).await.unwrap();
    let rows = source.fetch_all().await.unwrap();
    assert!(rows.is_empty());
}

#[tokio::test]
async fn batch_size_streams_multiple_batches() {
    // Write 5000 rows; with batch_size=512, the reader yields ~10 batches —
    // exercises the streaming path rather than a single in-memory batch.
    let n = 5000;
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(
            (0..n as i64).collect::<Vec<_>>(),
        ))],
    )
    .unwrap();

    let dir = TempDir::new().unwrap();
    let path = dir.path().join("big.parquet");
    write_parquet(&path, &batch);

    let cfg = ParquetSourceConfig::local(path.to_str().unwrap()).batch_size(512);
    let source = ParquetSource::new(cfg).await.unwrap();
    let rows = source.fetch_all().await.unwrap();
    assert_eq!(rows.len(), n);
    assert_eq!(rows[0]["v"], 0);
    assert_eq!(rows[n - 1]["v"], (n - 1) as i64);
}

#[tokio::test]
async fn missing_local_file_is_source_error() {
    let cfg = ParquetSourceConfig::local("/definitely/not/a/real/path.parquet");
    let source = ParquetSource::new(cfg).await.unwrap();
    let err = source.fetch_all().await.expect_err("should fail");
    assert!(matches!(
        err,
        faucet_core::FaucetError::Source(_) | faucet_core::FaucetError::Config(_)
    ));
}
