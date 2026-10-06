#![allow(deprecated)]
//! Additional coverage tests for `faucet-sink-parquet`.
//!
//! These exercise branches not covered by `roundtrip.rs`: single-file mode
//! (a fixed `.parquet` path), the doctor `check()` probe paths, the explicit
//! schema rejection, the S3 `dataset_uri` / config-build branches, and the
//! unsigned-integer / float JSON type-name reporting on a type-drift error.
//! As in `roundtrip.rs`, we read the written Parquet back through the raw
//! `parquet` + `arrow` APIs (no dependency on `faucet-source-parquet`).

use arrow::record_batch::RecordBatch;
use faucet_core::Sink;
use faucet_core::check::{CheckContext, ProbeStatus};
use faucet_sink_parquet::{
    ParquetDestination, ParquetS3Destination, ParquetSink, ParquetSinkConfig,
};
use futures::TryStreamExt;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use serde_json::{Value, json};
use tempfile::TempDir;

fn rows_in(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

/// Read a single concrete `.parquet` file path back into record batches.
async fn read_file(path: &std::path::Path) -> Vec<RecordBatch> {
    let file = tokio::fs::File::open(path).await.unwrap();
    let builder = ParquetRecordBatchStreamBuilder::new(file).await.unwrap();
    let stream = builder.build().unwrap();
    stream.try_collect().await.unwrap()
}

#[tokio::test]
async fn single_file_mode_writes_to_the_exact_fixed_path() {
    // A `.parquet` path with no rollover thresholds is single-file mode:
    // `next_object_path` must return that exact path (not a UUID name in the
    // parent dir), so the file lands at precisely the configured location.
    let tmp = TempDir::new().unwrap();
    let target = tmp.path().join("out.parquet");
    let cfg = ParquetSinkConfig::local(target.to_string_lossy().to_string());

    {
        let sink = ParquetSink::new(cfg).await.unwrap();

        sink.write_batch(&[
            json!({"id": 1, "name": "alice"}),
            json!({"id": 2, "name": "bob"}),
        ])
        .await
        .unwrap();
        // In single-file mode `flush()` publishes the complete file.
        sink.flush().await.unwrap();
    }

    assert!(
        target.is_file(),
        "single-file mode must write the exact path"
    );
    // No stray UUID-named files should appear alongside it.
    let parquet_files: Vec<_> = std::fs::read_dir(tmp.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|x| x.to_str()) == Some("parquet"))
        .collect();
    assert_eq!(
        parquet_files.len(),
        1,
        "exactly one file: {parquet_files:?}"
    );
    assert_eq!(parquet_files[0], target);

    let batches = read_file(&target).await;
    assert_eq!(rows_in(&batches), 2);
}

#[tokio::test]
async fn fixed_parquet_path_with_rollover_falls_back_to_uuid_files() {
    // A fixed `.parquet` path combined with a rollover threshold is the
    // contradictory case that emits a one-shot warn and falls back to
    // UUID-named files in the *parent* directory (the configured filename is
    // ignored). Exercises both the constructor warn branch and the
    // `single_file_mode() == false` path of `next_object_path`.
    let tmp = TempDir::new().unwrap();
    let target = tmp.path().join("ignored.parquet");
    let cfg = ParquetSinkConfig::local(target.to_string_lossy().to_string()).max_rows_per_file(2);
    let sink = ParquetSink::new(cfg).await.unwrap();

    // Rollover fires after each `write_chunk` whose counter reaches the cap, so
    // write the rows in 3 separate batches of 2 to roll over three times.
    for chunk in 0..3 {
        let records: Vec<Value> = (0..2)
            .map(|j| json!({"i": (chunk * 2 + j) as i64}))
            .collect();
        sink.write_batch(&records).await.unwrap();
    }
    sink.flush().await.unwrap();

    // The fixed `ignored.parquet` name must NOT be the file that was written.
    assert!(
        !target.is_file(),
        "the fixed filename must be ignored under rollover"
    );
    let files: Vec<_> = std::fs::read_dir(tmp.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|x| x.to_str()) == Some("parquet"))
        .collect();
    // 3 batches of 2 rows with max_rows=2 → 3 rolled-over UUID files.
    assert_eq!(
        files.len(),
        3,
        "rollover should produce 3 UUID-named files, got {files:?}"
    );
    for f in &files {
        let stem = f.file_stem().and_then(|s| s.to_str()).unwrap();
        assert_ne!(stem, "ignored", "UUID name expected, not the fixed stem");
    }
    let total: usize = {
        let mut t = 0;
        for f in &files {
            t += rows_in(&read_file(f).await);
        }
        t
    };
    assert_eq!(total, 6);
}

#[tokio::test]
async fn explicit_schema_is_rejected_on_first_write() {
    // The `SchemaSource::Explicit {}` branch is reserved for a future revision
    // and must surface as a `FaucetError::Config` the first time a batch needs
    // a schema (i.e. on the first non-empty write_chunk).
    use faucet_sink_parquet::SchemaSource;

    let tmp = TempDir::new().unwrap();
    let cfg = ParquetSinkConfig::local(tmp.path().to_string_lossy().to_string())
        .schema(SchemaSource::Explicit {});
    let sink = ParquetSink::new(cfg).await.unwrap();

    let err = sink
        .write_batch(&[json!({"id": 1})])
        .await
        .expect_err("explicit schema must be rejected");
    match err {
        faucet_core::FaucetError::Config(msg) => {
            assert!(msg.contains("explicit"), "got: {msg}");
        }
        other => panic!("expected Config error, got {other:?}"),
    }
}

#[tokio::test]
async fn doctor_check_passes_for_writable_directory() {
    // The local `check()` probe creates and removes a temp file in the target
    // directory. A fresh temp dir is writable, so the probe must Pass.
    let tmp = TempDir::new().unwrap();
    let cfg = ParquetSinkConfig::local(tmp.path().to_string_lossy().to_string());
    let sink = ParquetSink::new(cfg).await.unwrap();

    let report = sink.check(&CheckContext::default()).await.unwrap();
    assert_eq!(report.probes.len(), 1);
    assert_eq!(report.probes[0].name, "io");
    assert!(
        matches!(report.probes[0].status, ProbeStatus::Pass),
        "writable dir must pass, got {:?}",
        report.probes[0].status
    );
    // No temp probe file should be left behind.
    let leftovers: Vec<_> = std::fs::read_dir(tmp.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_str()
                .map(|n| n.starts_with(".faucet_doctor_probe"))
                .unwrap_or(false)
        })
        .collect();
    assert!(leftovers.is_empty(), "probe file must be removed");
}

#[tokio::test]
async fn doctor_check_fails_when_parent_directory_missing() {
    // A `.parquet` single-file path whose parent dir does not exist must Fail
    // with a hint to create the directory. We point at a nonexistent subdir.
    let tmp = TempDir::new().unwrap();
    let missing = tmp.path().join("no_such_dir").join("out.parquet");
    let cfg = ParquetSinkConfig::local(missing.to_string_lossy().to_string());
    // `new()` calls build_store which create_dir_all's the parent — so to keep
    // the parent missing for the probe we build the sink, then remove the dir.
    let sink = ParquetSink::new(cfg).await.unwrap();
    std::fs::remove_dir_all(tmp.path().join("no_such_dir")).unwrap();

    let report = sink.check(&CheckContext::default()).await.unwrap();
    assert_eq!(report.probes.len(), 1);
    match &report.probes[0].status {
        ProbeStatus::Fail { reason } => {
            assert!(reason.contains("does not exist"), "got: {reason}");
            assert!(report.probes[0].hint.is_some(), "fail must carry a hint");
        }
        other => panic!("expected Fail, got {other:?}"),
    }
}

#[tokio::test]
async fn doctor_check_skips_s3_destination() {
    // For an S3 destination the doctor probe is skipped — object-store targets
    // are not probed.
    let cfg = ParquetSinkConfig::new(ParquetDestination::S3(ParquetS3Destination {
        bucket: "b".to_string(),
        prefix: "p/".to_string(),
        region: Some("us-east-1".to_string()),
        endpoint_url: Some("http://localhost:4566".to_string()),
        allow_http: true,
    }));
    // SAFETY: constants; concurrent writes converge to the same value (mirrors
    // the existing `s3_destination_builds_*` test in roundtrip.rs).
    unsafe {
        std::env::set_var("AWS_ACCESS_KEY_ID", "test");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
    }
    let sink = ParquetSink::new(cfg).await.unwrap();

    let report = sink.check(&CheckContext::default()).await.unwrap();
    assert_eq!(report.probes.len(), 1);
    assert!(
        matches!(report.probes[0].status, ProbeStatus::Skip { .. }),
        "S3 target must be skipped, got {:?}",
        report.probes[0].status
    );

    // The S3 dataset_uri branch.
    assert_eq!(sink.dataset_uri(), "s3://b/p/");
    // config_schema serialises without panicking.
    assert!(sink.config_schema().is_object());
}

#[tokio::test]
async fn doctor_check_skips_object_store_url_in_local_path() {
    // An `s3://` URL that slipped into a LocalPath destination is treated as an
    // object-store target and skipped, not mis-probed on the local FS.
    let cfg = ParquetSinkConfig::local("s3://bucket/key.parquet");
    let sink = ParquetSink::new(cfg).await.unwrap();
    let report = sink.check(&CheckContext::default()).await.unwrap();
    assert!(
        matches!(report.probes[0].status, ProbeStatus::Skip { .. }),
        "s3:// in a local path must be skipped, got {:?}",
        report.probes[0].status
    );
}

#[tokio::test]
async fn type_drift_reports_float_json_type_name() {
    // The float branch of `json_value_type_name` / `sample_field_type` is hit
    // when a field locked in as Utf8 (string) later receives a JSON float:
    // `matches_data_type(Utf8, Number)` is false, so `guess_drifting_field`
    // flags the field and `sample_field_type` reports its type as a float.
    let tmp = TempDir::new().unwrap();
    let cfg = ParquetSinkConfig::local(tmp.path().to_string_lossy().to_string());
    let sink = ParquetSink::new(cfg).await.unwrap();

    // First batch locks `n` in as a string column.
    sink.write_batch(&[json!({"n": "label"})]).await.unwrap();
    // A subsequent float for the same field drifts.
    let err = sink
        .write_batch(&[json!({"n": 2.5})])
        .await
        .expect_err("string→float drift must error");
    match err {
        faucet_core::FaucetError::Sink(msg) => {
            assert!(
                msg.contains("'n'") || msg.contains("n"),
                "field named: {msg}"
            );
            assert!(
                msg.contains("float") || msg.contains("Utf8"),
                "drift message should describe the float record type / utf8 schema: {msg}"
            );
        }
        other => panic!("expected Sink error, got {other:?}"),
    }
    sink.flush().await.unwrap();
}

#[tokio::test]
async fn lazy_writer_opens_on_first_batch_inferring_schema_from_records() {
    // Before any write there is no file; after the first batch + flush the
    // schema is inferred from the real records (id:Int64, name:Utf8). This
    // documents the lazy-open contract end-to-end.
    let tmp = TempDir::new().unwrap();
    let target = tmp.path().join("lazy.parquet");
    let cfg = ParquetSinkConfig::local(target.to_string_lossy().to_string());

    {
        let sink = ParquetSink::new(cfg).await.unwrap();

        // No batch written yet → no file.
        assert!(!target.is_file());

        sink.write_batch(&[json!({"id": 1, "name": "x"})])
            .await
            .unwrap();
        // Single-file mode finalizes the footer on drop, not on flush; drop the
        // sink (end of scope) before reading the file back.
        sink.flush().await.unwrap();
    }

    let batches = read_file(&target).await;
    assert_eq!(rows_in(&batches), 1);
    let schema = batches[0].schema();
    assert_eq!(
        schema.field_with_name("id").unwrap().data_type(),
        &arrow::datatypes::DataType::Int64
    );
    assert_eq!(
        schema.field_with_name("name").unwrap().data_type(),
        &arrow::datatypes::DataType::Utf8
    );
}

/// #789 FILE-01: a flush publishes a readable file while the run is still
/// going, later pages are appended to it, and unflushed rows never land.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_file_flush_publishes_and_drop_discards() {
    let tmp = TempDir::new().unwrap();
    let target = tmp.path().join("pub.parquet");
    let cfg = ParquetSinkConfig::local(target.to_string_lossy().to_string());
    {
        let sink = ParquetSink::new(cfg).await.unwrap();
        sink.write_batch(&[json!({"id": 1})]).await.unwrap();
        sink.flush().await.unwrap();
        assert_eq!(rows_in(&read_file(&target).await), 1);
        sink.write_batch(&[json!({"id": 2}), json!({"id": 3})])
            .await
            .unwrap();
        sink.flush().await.unwrap();
        assert_eq!(rows_in(&read_file(&target).await), 3);
        sink.write_batch(&[json!({"id": 4})]).await.unwrap();
    }
    assert_eq!(rows_in(&read_file(&target).await), 3);
}
