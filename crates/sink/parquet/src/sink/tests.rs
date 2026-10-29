
use super::*;
use crate::config::ParquetCompression;
use faucet_core::Sink;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::json;

fn cfg(path: &std::path::Path) -> ParquetSinkConfig {
    ParquetSinkConfig::local(path.to_string_lossy().to_string())
}

/// Read every row group back from a Parquet file and collect the `id`
/// column (Int64) into a Vec. Panics on any read error — tests want the
/// loud failure.
fn read_ids(path: &std::path::Path) -> Vec<i64> {
    let file = std::fs::File::open(path).expect("open parquet file");
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .expect("parquet reader builder")
        .build()
        .expect("parquet reader");
    let mut ids = Vec::new();
    for batch in reader {
        let batch = batch.expect("record batch");
        let col = batch
            .column(batch.schema().index_of("id").expect("id column"))
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("id is Int64");
        for i in 0..col.len() {
            ids.push(col.value(i));
        }
    }
    ids
}

#[tokio::test]
async fn dataset_uri_local_path() {
    let tmp = tempfile::tempdir().unwrap();
    let sink = ParquetSink::new(cfg(tmp.path())).await.unwrap();
    let uri = sink.dataset_uri();
    assert!(
        uri.starts_with("file://"),
        "expected file:// URI, got: {uri}"
    );
    assert!(
        uri.contains(tmp.path().to_str().unwrap()),
        "URI should contain the path"
    );
}

#[tokio::test]
async fn a_column_null_in_the_sample_takes_later_values() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("sparse.parquet");
    let sink = ParquetSink::new(cfg(&path)).await.unwrap();
    sink.write_batch(&[json!({"id": 1, "deleted_at": null, "amount": null})])
        .await
        .unwrap();
    sink.write_batch(&[json!({"id": 2, "deleted_at": "2026-10-01", "amount": "12.5"})])
        .await
        .unwrap();
    sink.flush().await.unwrap();
    let file = std::fs::File::open(&path).unwrap();
    let batch = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    let text = |name: &str| {
        batch
            .column(batch.schema().index_of(name).unwrap())
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("a string column")
            .clone()
    };
    assert_eq!(text("deleted_at").value(1), "2026-10-01");
    assert_eq!(text("amount").value(1), "12.5");
    assert!(arrow::array::Array::is_null(&text("amount"), 0));
}

/// #862 L-159: a column null throughout the sample is a string column, and
/// later numbers, booleans or objects in it are written as their text
/// instead of failing the batch.
#[tokio::test]
async fn non_string_values_in_a_null_sampled_column_are_written_as_text() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("sparse.parquet");
    let sink = ParquetSink::new(cfg(&path)).await.unwrap();
    sink.write_batch(&[json!({"id": 1, "v": null})])
        .await
        .unwrap();
    sink.write_batch(&[
        json!({"id": 2, "v": 5}),
        json!({"id": 3, "v": true}),
        json!({"id": 4, "v": {"b": 1, "a": 2}}),
        json!({"id": 5, "v": "s"}),
        json!({"id": 6}),
    ])
    .await
    .unwrap();
    sink.flush().await.unwrap();
    let file = std::fs::File::open(&path).unwrap();
    let batch = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    let v = batch
        .column(batch.schema().index_of("v").unwrap())
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap()
        .clone();
    assert_eq!(v.value(1), "5");
    assert_eq!(v.value(2), "true");
    assert_eq!(v.value(3), r#"{"a":2,"b":1}"#);
    assert_eq!(v.value(4), "s");
    assert!(arrow::array::Array::is_null(&v, 5));
}

/// A store whose multipart uploads never complete and count their aborts.
#[derive(Debug, Default)]
struct FailingUploads {
    inner: object_store::memory::InMemory,
    aborts: Arc<std::sync::atomic::AtomicUsize>,
    fail_parts: bool,
}

impl std::fmt::Display for FailingUploads {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("FailingUploads")
    }
}

#[derive(Debug)]
struct FailingUpload(Arc<std::sync::atomic::AtomicUsize>, bool);

#[faucet_core::async_trait]
impl object_store::MultipartUpload for FailingUpload {
    fn put_part(&mut self, _data: object_store::PutPayload) -> object_store::UploadPart {
        let fail = self.1;
        Box::pin(async move {
            if fail {
                Err(object_store::Error::NotImplemented {
                    operation: "put_part".into(),
                    implementer: "FailingUploads".into(),
                })
            } else {
                Ok(())
            }
        })
    }
    async fn complete(&mut self) -> object_store::Result<object_store::PutResult> {
        Err(object_store::Error::NotImplemented {
            operation: "complete".into(),
            implementer: "FailingUploads".into(),
        })
    }
    async fn abort(&mut self) -> object_store::Result<()> {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
}

#[faucet_core::async_trait]
impl object_store::ObjectStore for FailingUploads {
    async fn put_opts(
        &self,
        location: &ObjPath,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }
    async fn put_multipart_opts(
        &self,
        _location: &ObjPath,
        _opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        Ok(Box::new(FailingUpload(
            self.aborts.clone(),
            self.fail_parts,
        )))
    }
    async fn get_opts(
        &self,
        location: &ObjPath,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.inner.get_opts(location, options).await
    }
    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<'static, object_store::Result<ObjPath>>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<ObjPath>> {
        self.inner.delete_stream(locations)
    }
    fn list(
        &self,
        prefix: Option<&ObjPath>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjPath>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &ObjPath,
        to: &ObjPath,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[tokio::test]
async fn a_failed_close_aborts_the_multipart_upload() {
    for fail_parts in [false, true] {
        let store = Arc::new(FailingUploads {
            fail_parts,
            ..Default::default()
        });
        let aborts = store.aborts.clone();
        let upload = AbortableUpload::with_part_size(store, ObjPath::from("out.parquet"), 16);
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
        ]));
        let mut writer = AsyncArrowWriter::try_new(upload, schema.clone(), None).unwrap();
        let batch = arrow::array::RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow::array::Int64Array::from(
                (0..1_000).collect::<Vec<i64>>(),
            ))],
        )
        .unwrap();
        writer.write(&batch).await.unwrap();
        let err = finish_or_abort(writer).await.unwrap_err();
        assert!(err.to_string().contains("could not close"), "{err}");
        assert_eq!(
            aborts.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "fail_parts={fail_parts}"
        );
    }
}

#[tokio::test]
async fn small_and_large_uploads_complete() {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    for (name, threshold) in [("small", 1 << 20), ("large", 8)] {
        let path = ObjPath::from(name);
        let mut upload = AbortableUpload::with_part_size(store.clone(), path.clone(), threshold);
        use parquet::arrow::async_writer::AsyncFileWriter as _;
        upload
            .write(bytes::Bytes::from_static(b"0123456789"))
            .await
            .unwrap();
        upload
            .write(bytes::Bytes::from_static(b"abc"))
            .await
            .unwrap();
        upload.complete().await.unwrap();
        assert!(upload.write(bytes::Bytes::from_static(b"x")).await.is_err());
        upload.abort().await.unwrap();
        let got = object_store::ObjectStoreExt::get(store.as_ref(), &path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(&got[..], b"0123456789abc");
    }
}

#[tokio::test]
async fn new_validates_config() {
    let cfg = ParquetSinkConfig::local("");
    match ParquetSink::new(cfg).await {
        Ok(_) => panic!("expected Config error"),
        Err(err) => assert!(matches!(err, FaucetError::Config(_))),
    }
}

#[tokio::test]
async fn empty_batch_is_noop() {
    let tmp = tempfile::tempdir().unwrap();
    let sink = ParquetSink::new(cfg(tmp.path())).await.unwrap();
    let count = sink.write_batch(&[]).await.unwrap();
    assert_eq!(count, 0);
    sink.flush().await.unwrap();
}

#[tokio::test]
async fn flush_without_write_is_noop() {
    let tmp = tempfile::tempdir().unwrap();
    let sink = ParquetSink::new(cfg(tmp.path())).await.unwrap();
    assert!(sink.flush().await.is_ok());
}

#[tokio::test]
async fn type_drift_returns_sink_error() {
    let tmp = tempfile::tempdir().unwrap();
    let sink = ParquetSink::new(cfg(tmp.path())).await.unwrap();
    sink.write_batch(&[json!({"x": 1})]).await.unwrap();
    let err = sink
        .write_batch(&[json!({"x": "not an int"})])
        .await
        .unwrap_err();
    match err {
        FaucetError::Sink(msg) => {
            assert!(
                msg.contains("'x'") || msg.contains("x"),
                "error must name the drifting field: {msg}"
            );
        }
        other => panic!("expected Sink error, got {other:?}"),
    }
    sink.flush().await.unwrap();
}

#[tokio::test]
async fn unknown_fields_are_silently_dropped() {
    let tmp = tempfile::tempdir().unwrap();
    let sink = ParquetSink::new(cfg(tmp.path())).await.unwrap();
    sink.write_batch(&[json!({"id": 1})]).await.unwrap();
    let count = sink
        .write_batch(&[json!({"id": 2, "ghost": "value"})])
        .await
        .unwrap();
    assert_eq!(count, 1);
    sink.flush().await.unwrap();
}

#[tokio::test]
async fn writer_properties_apply_compression() {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = cfg(tmp.path()).compression(ParquetCompression::Zstd);
    let sink = ParquetSink::new(cfg).await.unwrap();
    let props = sink.d().writer_properties();
    assert!(matches!(
        props.compression(&parquet::schema::types::ColumnPath::new(vec![
            "any".to_string(),
        ])),
        parquet::basic::Compression::ZSTD(_)
    ));
}

#[test]
fn should_rollover_rows_threshold() {
    let cfg = ParquetSinkConfig::local("/tmp/x").max_rows_per_file(10);
    let mut state = WriterState::new();
    state.rows_in_current_file = 9;
    assert!(!should_rollover(&cfg, &state, 0));
    state.rows_in_current_file = 10;
    assert!(should_rollover(&cfg, &state, 0));
    state.rows_in_current_file = 11;
    assert!(should_rollover(&cfg, &state, 0));
}

#[test]
fn should_rollover_bytes_threshold() {
    let cfg = ParquetSinkConfig::local("/tmp/x").max_bytes_per_file(1024);
    let state = WriterState::new();
    assert!(!should_rollover(&cfg, &state, 1023));
    assert!(should_rollover(&cfg, &state, 1024));
    assert!(should_rollover(&cfg, &state, 4096));
}

#[test]
fn should_rollover_no_thresholds_means_never() {
    let cfg = ParquetSinkConfig::local("/tmp/x");
    let mut state = WriterState::new();
    state.rows_in_current_file = 1_000_000;
    assert!(!should_rollover(&cfg, &state, usize::MAX / 2));
}

#[test]
fn matches_data_type_for_primitives() {
    use arrow::datatypes::DataType as DT;
    assert!(matches_data_type(&DT::Boolean, &json!(true)));
    assert!(matches_data_type(&DT::Int64, &json!(1)));
    assert!(!matches_data_type(&DT::Int64, &json!(1.5)));
    assert!(matches_data_type(&DT::Float64, &json!(1.5)));
    assert!(matches_data_type(&DT::Float64, &json!(1)));
    assert!(matches_data_type(&DT::Utf8, &json!("hi")));
    assert!(!matches_data_type(&DT::Utf8, &json!(1)));
    assert!(matches_data_type(&DT::Boolean, &Value::Null));
}

#[test]
fn json_value_type_name_covers_variants() {
    assert_eq!(json_value_type_name(&json!(null)), "null");
    assert_eq!(json_value_type_name(&json!(true)), "boolean");
    assert_eq!(json_value_type_name(&json!(1)), "integer");
    assert_eq!(json_value_type_name(&json!(1.5)), "float");
    assert_eq!(json_value_type_name(&json!("s")), "string");
    assert_eq!(json_value_type_name(&json!([1])), "array");
    assert_eq!(json_value_type_name(&json!({"a": 1})), "object");
}

#[test]
fn single_file_mode_only_for_fixed_parquet_path_without_rollover() {
    // Fixed *.parquet path, no rollover → single-file.
    assert!(compute_single_file_mode(&ParquetSinkConfig::local(
        "/tmp/out.parquet"
    )));
    // Rollover thresholds disable single-file mode.
    assert!(!compute_single_file_mode(
        &ParquetSinkConfig::local("/tmp/out.parquet").max_rows_per_file(10)
    ));
    assert!(!compute_single_file_mode(
        &ParquetSinkConfig::local("/tmp/out.parquet").max_bytes_per_file(1024)
    ));
    // A directory path is never single-file.
    assert!(!compute_single_file_mode(&ParquetSinkConfig::local(
        "/tmp/outdir"
    )));
}

/// Regression test for F2 (audit #264): single-file mode must accumulate
/// ALL pages across the per-page `flush()` calls the pipeline issues — the
/// file must not be reopened/truncated mid-stream, so only the final page
/// would survive.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_file_accumulates_all_pages_across_flushes() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("out.parquet");
    let cfg = ParquetSinkConfig::local(path.to_string_lossy().to_string());
    assert!(
        compute_single_file_mode(&cfg),
        "test setup must be single-file"
    );

    {
        let sink = ParquetSink::new(cfg).await.unwrap();

        // Page 1 + bookmark flush.
        sink.write_batch(&[json!({"id": 1}), json!({"id": 2})])
            .await
            .unwrap();
        sink.flush().await.unwrap();

        // Page 2 + bookmark flush — under the bug this would truncate the
        // file written by page 1.
        sink.write_batch(&[json!({"id": 3})]).await.unwrap();
        sink.flush().await.unwrap();

        // Page 3 + final flush (pipeline calls flush once more at end).
        sink.write_batch(&[json!({"id": 4}), json!({"id": 5})])
            .await
            .unwrap();
        sink.flush().await.unwrap();
    }

    let mut ids = read_ids(&path);
    ids.sort_unstable();
    assert_eq!(
        ids,
        vec![1, 2, 3, 4, 5],
        "all rows from every page must be present in the single file"
    );
}

/// #789 FILE-01: each `flush()` publishes a complete, readable file while
/// the sink is still open, and rows written after the last flush never
/// reach the file — `Drop` finalises nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_file_publishes_on_flush_and_discards_unflushed_rows() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("out.parquet");
    {
        let sink = ParquetSink::new(cfg(&path)).await.unwrap();
        sink.write_batch(&[json!({"id": 10}), json!({"id": 11})])
            .await
            .unwrap();
        sink.flush().await.unwrap();
        assert_eq!(read_ids(&path), vec![10, 11], "readable right after flush");

        sink.write_batch(&[json!({"id": 12})]).await.unwrap();
        sink.flush().await.unwrap();
        let mut ids = read_ids(&path);
        ids.sort_unstable();
        assert_eq!(
            ids,
            vec![10, 11, 12],
            "the second publish keeps the first page"
        );

        sink.write_batch(&[json!({"id": 13})]).await.unwrap();
    }
    let mut ids = read_ids(&path);
    ids.sort_unstable();
    assert_eq!(
        ids,
        vec![10, 11, 12],
        "unflushed rows are not finalised on drop"
    );
    let stray: Vec<_> = std::fs::read_dir(tmp.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name())
        .filter(|n| n != "out.parquet")
        .collect();
    assert!(stray.is_empty(), "no staging files left: {stray:?}");
}

#[tokio::test(flavor = "current_thread")]
async fn a_published_file_that_vanished_fails_the_next_write() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("out.parquet");
    let sink = ParquetSink::new(cfg(&path)).await.unwrap();
    sink.write_batch(&[json!({"id": 1})]).await.unwrap();
    sink.flush().await.unwrap();
    std::fs::remove_file(&path).unwrap();
    let err = sink.write_batch(&[json!({"id": 2})]).await.unwrap_err();
    assert!(err.to_string().contains("could not reread"), "{err}");
}

#[test]
fn sync_file_and_dir_reports_a_missing_file() {
    let tmp = tempfile::tempdir().unwrap();
    let err = sync_file_and_dir(&tmp.path().join("nope.parquet")).unwrap_err();
    assert!(err.to_string().contains("could not fsync"), "{err}");
    let f = tmp.path().join("ok");
    std::fs::write(&f, b"x").unwrap();
    sync_file_and_dir(&f).unwrap();
    sync_file_and_dir(std::path::Path::new("/")).unwrap();
}

/// Columnar fast path (feature `arrow`): a `RecordBatch` written via
/// `write_batch_columnar` produces the same readable single-file Parquet as
/// the `Value` path — it goes through the shared `write_record_batch` tail,
/// so schema inference (from the batch), lazy writer open, and publishing
/// on flush all behave identically. Proves the parquet sink's half of the
/// `parquet -> parquet` columnar chain (RFC 0002 / #375).
#[cfg(feature = "arrow")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_batch_columnar_round_trips() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("out.parquet");
    let rows = vec![json!({"id": 1}), json!({"id": 2}), json!({"id": 3})];
    let batch = faucet_core::columnar::values_to_record_batch_inferred(&rows).unwrap();
    {
        let sink = ParquetSink::new(cfg(&path)).await.unwrap();
        assert!(sink.supports_columnar(), "parquet sink is columnar-capable");
        let n = sink.write_batch_columnar(&batch).await.unwrap();
        assert_eq!(n, 3, "all rows written via the columnar path");
        sink.flush().await.unwrap();
    }
    let mut ids = read_ids(&path);
    ids.sort_unstable();
    assert_eq!(ids, vec![1, 2, 3], "columnar-written rows read back intact");
}

/// Rollover (directory) mode must keep its per-page file-rolling behavior:
/// each `flush()` closes a file and the next page opens a new one. Assert
/// the expected file count and total rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rollover_mode_still_rolls_files_per_page() {
    let tmp = tempfile::tempdir().unwrap();
    // Directory destination + a row threshold → rollover/multi-file mode.
    let cfg =
        ParquetSinkConfig::local(tmp.path().to_string_lossy().to_string()).max_rows_per_file(2);
    assert!(!compute_single_file_mode(&cfg));

    let mut all_ids = Vec::new();
    {
        let sink = ParquetSink::new(cfg).await.unwrap();
        // Two pages; max_rows_per_file=2 rolls within page 1.
        sink.write_batch(&[json!({"id": 1}), json!({"id": 2}), json!({"id": 3})])
            .await
            .unwrap();
        sink.flush().await.unwrap();
        sink.write_batch(&[json!({"id": 4})]).await.unwrap();
        sink.flush().await.unwrap();
    }

    // Read back every .parquet file in the directory.
    let mut files: Vec<_> = std::fs::read_dir(tmp.path())
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("parquet"))
        .collect();
    files.sort();
    assert!(
        files.len() >= 2,
        "rollover mode should produce multiple files, got {}",
        files.len()
    );
    for f in &files {
        all_ids.extend(read_ids(f));
    }
    all_ids.sort_unstable();
    assert_eq!(
        all_ids,
        vec![1, 2, 3, 4],
        "no rows lost across rolled files"
    );
}

/// Read the column names of a Parquet file's schema.
fn read_columns(path: &std::path::Path) -> Vec<String> {
    let file = std::fs::File::open(path).expect("open parquet file");
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).expect("parquet reader builder");
    builder
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().to_string())
        .collect()
}

/// Regression test for F34 (audit #264): the Parquet schema must be
/// re-inferred per file. When a later file's first batch carries a *wider*
/// schema (a new column added mid-run, e.g. CDC after ALTER TABLE ADD
/// COLUMN), the new column must be written in that later file rather than
/// silently dropped because the schema was locked from the very first batch
/// of the run.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rollover_reinfers_widened_schema_in_later_file() {
    let tmp = tempfile::tempdir().unwrap();
    // Directory destination + row threshold → multi-file rollover mode.
    let cfg =
        ParquetSinkConfig::local(tmp.path().to_string_lossy().to_string()).max_rows_per_file(2);
    assert!(!compute_single_file_mode(&cfg));

    {
        let sink = ParquetSink::new(cfg).await.unwrap();
        // File 1: narrow schema {id}. Two rows hits the rollover threshold,
        // closing file 1 and clearing the locked schema.
        sink.write_batch(&[json!({"id": 1}), json!({"id": 2})])
            .await
            .unwrap();
        sink.flush().await.unwrap();
        // File 2: widened schema {id, extra}. Under the bug the schema
        // stayed locked to {id} and `extra` would be silently dropped.
        sink.write_batch(&[json!({"id": 3, "extra": "new-column"})])
            .await
            .unwrap();
        sink.flush().await.unwrap();
    }

    let mut files: Vec<_> = std::fs::read_dir(tmp.path())
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("parquet"))
        .collect();
    files.sort();
    assert_eq!(files.len(), 2, "expected exactly two rolled files");

    // The file holding id=3 must include the widened `extra` column.
    let mut found_extra = false;
    for f in &files {
        let cols = read_columns(f);
        if cols.iter().any(|c| c == "extra") {
            found_extra = true;
            assert!(
                cols.iter().any(|c| c == "id"),
                "widened file must still carry the original `id` column: {cols:?}"
            );
        }
    }
    assert!(
        found_extra,
        "a later file must re-infer and write the widened `extra` column"
    );
}

/// `close_current` must reset the per-file state so the next file
/// re-infers its own schema: `state.schema` and `warned_fields` cleared,
/// `rows_in_current_file` reset, `files_written` incremented.
/// A failed write discards the current file and fails every later write
/// and flush, so a file missing bytes is never published and no bookmark
/// moves past the rows it held.
#[tokio::test]
async fn a_failed_write_poisons_the_sink() {
    let tmp = tempfile::tempdir().unwrap();
    let sink = ParquetSink::new(cfg(tmp.path())).await.unwrap();
    sink.write_batch(&[json!({"id": 1})]).await.unwrap();
    {
        let mut state = sink.d().state.lock().await;
        state.poison("parquet write failed: disk full").await;
        assert!(state.writer.is_none(), "the broken file is discarded");
    }
    let err = sink.write_batch(&[json!({"id": 2})]).await.unwrap_err();
    assert!(err.to_string().contains("disk full"), "{err}");
    let err = sink.flush().await.unwrap_err();
    assert!(err.to_string().contains("the run cannot complete"), "{err}");
    let published: Vec<_> = std::fs::read_dir(tmp.path())
        .unwrap()
        .filter_map(Result::ok)
        .filter(|e| e.path().extension().is_some_and(|x| x == "parquet"))
        .collect();
    assert!(published.is_empty(), "{published:?}");
}

#[tokio::test]
async fn close_current_resets_per_file_state() {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = ParquetSinkConfig::local(tmp.path().to_string_lossy().to_string());
    let sink = ParquetSink::new(cfg).await.unwrap();

    {
        let mut state = sink.d().state.lock().await;
        // Simulate a written-into file: schema locked, a row counted, and a
        // dropped field warned-about.
        sink.d()
            .write_chunk(&mut state, &[json!({"id": 1})])
            .await
            .unwrap();
        assert!(
            state.schema.is_some(),
            "schema should be locked after write"
        );
        state.warned_fields.insert("ghost".to_string());
        assert_eq!(state.rows_in_current_file, 1);

        sink.d().close_current(&mut state).await.unwrap();

        assert!(
            state.schema.is_none(),
            "schema must be cleared on rollover so the next file re-infers"
        );
        assert!(
            state.warned_fields.is_empty(),
            "warned_fields must be cleared so a new file re-warns"
        );
        assert_eq!(state.rows_in_current_file, 0);
        assert_eq!(state.files_written, 1);
    }
}

/// The unknown-field warning de-dupe set lives in `WriterState` and is
/// cleared on rollover, so a field dropped within one file warns once, and
/// a fresh file re-warns. We assert the de-dupe set behavior directly
/// (tracing output is not asserted here) since the warn path itself is
/// exercised by `unknown_fields_are_silently_dropped`.
#[test]
fn warn_on_unknown_fields_dedupes_per_file() {
    let records = vec![json!({"id": 1, "ghost": "x"})];
    let schema = infer_schema(&[json!({"id": 1})], 10).unwrap();
    let mut warned = std::collections::HashSet::new();

    warn_on_unknown_fields(&mut warned, &schema, &records);
    assert!(warned.contains("ghost"), "dropped field must be recorded");

    // Second call with the same set must not re-record (one warn per file).
    let before = warned.clone();
    warn_on_unknown_fields(&mut warned, &schema, &records);
    assert_eq!(warned, before, "already-warned field must not re-record");

    // Clearing (as close_current does) lets a new file re-warn.
    warned.clear();
    warn_on_unknown_fields(&mut warned, &schema, &records);
    assert!(warned.contains("ghost"), "a fresh file must re-warn");
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_outputs_records_every_rolled_part_individually() {
    // Retention GC provenance (#587). Rollover names each part with a fresh
    // UUID, so nothing outside the sink could enumerate them without
    // globbing the directory — which the GC is forbidden to do. Every part
    // must therefore arrive as its own recorded entry.
    let tmp = tempfile::tempdir().unwrap();
    let cfg =
        ParquetSinkConfig::local(tmp.path().to_string_lossy().to_string()).max_rows_per_file(2);
    let sink = ParquetSink::new(cfg).await.unwrap();
    assert!(sink.local_outputs().await.is_empty());

    sink.write_batch(&[json!({"id": 1}), json!({"id": 2}), json!({"id": 3})])
        .await
        .unwrap();
    sink.flush().await.unwrap();
    sink.write_batch(&[json!({"id": 4})]).await.unwrap();
    sink.flush().await.unwrap();

    let outs = sink.local_outputs().await;
    let on_disk: std::collections::BTreeSet<PathBuf> = std::fs::read_dir(tmp.path())
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("parquet"))
        .collect();
    assert!(
        on_disk.len() >= 2,
        "expected rollover to write several parts"
    );
    let recorded: std::collections::BTreeSet<PathBuf> =
        outs.iter().map(|o| o.path.clone()).collect();
    assert_eq!(
        recorded, on_disk,
        "every part faucet wrote must be recorded, and nothing else"
    );
    assert!(
        outs.iter().all(|o| !o.pre_existing),
        "UUID-named parts are always faucet's own files"
    );
    assert!(
        !recorded.contains(&PathBuf::from(tmp.path())),
        "the directory itself is never an output — only the files in it"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_outputs_reports_the_single_file_destination() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("out.parquet");
    let sink = ParquetSink::new(cfg(&path)).await.unwrap();
    assert!(compute_single_file_mode(&cfg(&path)));
    sink.write_batch(&[json!({"id": 1})]).await.unwrap();
    sink.flush().await.unwrap();

    let outs = sink.local_outputs().await;
    assert_eq!(outs.len(), 1);
    assert_eq!(outs[0].path, path);
    assert!(!outs[0].pre_existing);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_outputs_flags_a_pre_existing_single_file_destination() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("out.parquet");
    std::fs::write(&path, b"not really parquet").unwrap();
    let sink = ParquetSink::new(cfg(&path)).await.unwrap();
    sink.write_batch(&[json!({"id": 1})]).await.unwrap();
    sink.flush().await.unwrap();

    let outs = sink.local_outputs().await;
    assert_eq!(outs.len(), 1);
    assert!(
        outs[0].pre_existing,
        "faucet overwrote a file it did not create — never collectable"
    );
}

/// One `id: Int64` column — the locked file schema the drifting batch below
/// contradicts.
fn int_schema() -> SchemaRef {
    use arrow::datatypes::{DataType, Field, Schema};
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]))
}

#[test]
fn classify_decoder_error_reshapes_a_type_conflict_variant() {
    use arrow::error::ArrowError;
    let records = vec![json!({"id": "not-an-int"})];
    let err = classify_decoder_error(
        &int_schema(),
        &records,
        // The arrow-json decoder's real shape for a value that does not fit
        // the locked field type.
        ArrowError::JsonError("whilst decoding field 'id': expected i64 got string".into()),
    );
    let msg = err.to_string();
    assert!(
        msg.contains("parquet type drift for field 'id'")
            && msg.contains("schema declares Int64")
            && msg.contains("batch contains string"),
        "both sides of the drift must be named: {msg}"
    );
}

#[test]
fn classify_decoder_error_leaves_a_non_type_variant_alone() {
    use arrow::error::ArrowError;
    // Same drifting batch, so the field-guessing half would happily find a
    // mismatch — only the typed variant keeps an unrelated allocation
    // failure from being reported as type drift (#654 L28).
    let records = vec![json!({"id": "not-an-int"})];
    let err = classify_decoder_error(
        &int_schema(),
        &records,
        ArrowError::MemoryError("allocation of 8 bytes failed".into()),
    );
    let msg = err.to_string();
    assert!(
        msg.contains("parquet encode failed") && msg.contains("allocation of 8 bytes failed"),
        "a non-type failure must keep its raw message: {msg}"
    );
    assert!(
        !msg.contains("type drift"),
        "an unrelated failure must not be reported as type drift: {msg}"
    );
}

#[test]
fn classify_decoder_error_falls_back_when_no_field_drifts() {
    use arrow::error::ArrowError;
    // A type-conflict variant whose batch contains nothing contradicting the
    // schema (e.g. the conflict is inside a nested value we don't inspect):
    // there is no field to name, so the raw message is preserved.
    let records = vec![json!({"id": 7})];
    let err = classify_decoder_error(
        &int_schema(),
        &records,
        ArrowError::JsonError("offset overflow decoding ListArray".into()),
    );
    assert_eq!(
        err.to_string(),
        "Sink error: parquet encode failed: Json error: offset overflow decoding ListArray"
    );
}

#[test]
fn is_type_conflict_covers_the_decoder_variants_only() {
    use arrow::error::ArrowError as E;
    for err in [
        E::JsonError("x".into()),
        E::SchemaError("x".into()),
        E::CastError("x".into()),
        E::ParseError("x".into()),
    ] {
        assert!(is_type_conflict(&err), "{err} must classify as a conflict");
    }
    for err in [
        E::MemoryError("x".into()),
        E::ComputeError("x".into()),
        E::NotYetImplemented("x".into()),
        E::InvalidArgumentError("x".into()),
        E::IoError("x".into(), std::io::Error::other("x")),
        E::DivideByZero,
    ] {
        assert!(!is_type_conflict(&err), "{err} must not be a conflict");
    }
}
