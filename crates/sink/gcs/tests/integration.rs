//! Integration tests for `faucet-sink-gcs` against `fake-gcs-server`.
//!
//! Requires Docker. Skips automatically when Docker is unavailable.

#![cfg(not(target_os = "windows"))]

use faucet_core::Sink;
use faucet_sink_gcs::{GcsCredentials, GcsSink, GcsSinkConfig};
use serde_json::json;
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};

async fn spawn_fake_gcs() -> Option<(ContainerAsync<GenericImage>, String, String)> {
    let image = GenericImage::new("fsouza/fake-gcs-server", "latest")
        .with_exposed_port(4443.tcp())
        .with_wait_for(WaitFor::message_on_stderr("server started at"))
        .with_cmd(vec![
            "-scheme=http".to_string(),
            "-public-host=0.0.0.0:4443".to_string(),
        ]);
    let container = match image.start().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Skipping: Docker not available ({e})");
            return None;
        }
    };
    let port = container.get_host_port_ipv4(4443).await.ok()?;
    let host = format!("http://127.0.0.1:{port}");
    let bucket = "faucet-sink-test".to_string();

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{host}/storage/v1/b"))
        .json(&json!({"name": bucket}))
        .send()
        .await
        .ok()?;
    if !resp.status().is_success() && resp.status() != reqwest::StatusCode::CONFLICT {
        eprintln!("Skipping: could not create bucket ({})", resp.status());
        return None;
    }
    // The handle is returned so the container lives exactly as long as the
    // test that owns it: dropping it stops and removes the container.
    // testcontainers-rs has no reaper — a forgotten handle is a leaked
    // container, one per test, forever.
    Some((container, host, bucket))
}

/// The preflight probe lists the bucket, so against the emulator it passes, and
/// against a bucket that does not exist it reports a failed probe, not an error.
#[tokio::test]
async fn preflight_check_passes_and_fails_against_the_emulator() {
    use faucet_core::Sink as _;
    let Some((_gcs, host, bucket)) = spawn_fake_gcs().await else {
        return;
    };
    let ctx = faucet_core::check::CheckContext::default();
    for (name, want_ok) in [(bucket.as_str(), true), ("no-such-bucket", false)] {
        let c = GcsSink::new(
            GcsSinkConfig::new(name)
                .auth(GcsCredentials::Anonymous)
                .storage_host(&host),
        )
        .await
        .unwrap();
        let report = c.check(&ctx).await.unwrap();
        assert_eq!(report.failed_count() == 0, want_ok, "{name}: {report:?}");
    }
}

async fn object_names(host: &str, bucket: &str, prefix: &str) -> Vec<String> {
    let body: serde_json::Value = reqwest::get(format!(
        "{host}/storage/v1/b/{bucket}/o?prefix={}",
        urlencoding::encode(prefix)
    ))
    .await
    .unwrap()
    .json()
    .await
    .unwrap();
    let mut names: Vec<String> = body["items"]
        .as_array()
        .map(|items| {
            items
                .iter()
                .map(|o| o["name"].as_str().unwrap().to_string())
                .collect()
        })
        .unwrap_or_default();
    names.sort();
    names
}

async fn download(host: &str, bucket: &str, name: &str) -> Vec<u8> {
    reqwest::get(format!(
        "{host}/storage/v1/b/{bucket}/o/{}?alt=media",
        urlencoding::encode(name)
    ))
    .await
    .unwrap()
    .bytes()
    .await
    .unwrap()
    .to_vec()
}

/// A whole-object format buffers the page and writes one encoded object on
/// flush, instead of appending per record.
#[tokio::test]
async fn sink_writes_a_whole_object_format_on_flush() {
    let Some((_gcs, host, bucket)) = spawn_fake_gcs().await else {
        return;
    };
    let sink = GcsSink::new(
        GcsSinkConfig::new(&bucket)
            .prefix("arr/")
            .format(faucet_sink_gcs::GcsSinkFormat::JsonArray)
            .file_extension(".json")
            .auth(GcsCredentials::Anonymous)
            .storage_host(&host),
    )
    .await
    .unwrap();
    sink.write_batch(&[json!({"i": 1}), json!({"i": 2})])
        .await
        .unwrap();
    assert!(object_names(&host, &bucket, "arr/").await.is_empty());
    sink.flush().await.unwrap();

    let names = object_names(&host, &bucket, "arr/").await;
    assert_eq!(names.len(), 1);
    assert!(names[0].ends_with(".json"), "{names:?}");
    let body: serde_json::Value =
        serde_json::from_slice(&download(&host, &bucket, &names[0]).await).unwrap();
    assert_eq!(body, json!([{"i": 1}, {"i": 2}]));
}

/// An upload the emulator rejects (the bucket does not exist) is an error from
/// the write, never a silent success.
#[tokio::test]
async fn sink_upload_to_a_missing_bucket_is_an_error() {
    let Some((_gcs, host, _bucket)) = spawn_fake_gcs().await else {
        return;
    };
    let sink = GcsSink::new(
        GcsSinkConfig::new("no-such-bucket")
            .max_records_per_file(1)
            .auth(GcsCredentials::Anonymous)
            .storage_host(&host),
    )
    .await
    .unwrap();
    let err = sink
        .write_batch(&[json!({"i": 1}), json!({"i": 2})])
        .await
        .unwrap_err();
    assert!(err.to_string().contains("GCS put object error"), "{err}");
}

#[cfg(feature = "compression")]
#[tokio::test]
async fn sink_compresses_objects_by_extension() {
    let Some((_gcs, host, bucket)) = spawn_fake_gcs().await else {
        return;
    };
    let sink = GcsSink::new(
        GcsSinkConfig::new(&bucket)
            .prefix("gz/")
            .file_extension(".jsonl.gz")
            .auth(GcsCredentials::Anonymous)
            .storage_host(&host),
    )
    .await
    .unwrap();
    sink.write_batch(&[json!({"i": 1})]).await.unwrap();
    sink.flush().await.unwrap();
    let names = object_names(&host, &bucket, "gz/").await;
    assert_eq!(names.len(), 1);
    assert_eq!(
        &download(&host, &bucket, &names[0]).await[..2],
        &[0x1f, 0x8b]
    );
}

#[cfg(feature = "arrow")]
#[tokio::test]
async fn sink_writes_parquet_on_the_row_and_columnar_paths() {
    let Some((_gcs, host, bucket)) = spawn_fake_gcs().await else {
        return;
    };
    let config = GcsSinkConfig::new(&bucket)
        .prefix("pq/")
        .format(faucet_sink_gcs::GcsSinkFormat::Parquet)
        .file_extension(".parquet")
        .with_batch_size(2)
        .auth(GcsCredentials::Anonymous)
        .storage_host(&host);
    let sink = GcsSink::new(config).await.unwrap();
    let rows: Vec<_> = (0..3).map(|i| json!({"i": i})).collect();
    assert_eq!(sink.write_batch(&rows).await.unwrap(), 3);
    let batch = faucet_core::columnar::values_to_record_batch_inferred(&rows).unwrap();
    assert!(sink.supports_columnar());
    assert_eq!(sink.write_batch_columnar(&batch).await.unwrap(), 3);
    sink.flush().await.unwrap();

    // Objects close on the cap across pages: six rows in objects of two.
    let names = object_names(&host, &bucket, "pq/").await;
    assert_eq!(names.len(), 3, "{names:?}");
    for name in names {
        let body = download(&host, &bucket, &name).await;
        assert_eq!(&body[..4], b"PAR1");
        assert_eq!(&body[body.len() - 4..], b"PAR1");
    }
}

#[cfg(feature = "arrow")]
#[tokio::test]
async fn sink_columnar_batch_falls_back_to_rows_for_jsonl() {
    let Some((_gcs, host, bucket)) = spawn_fake_gcs().await else {
        return;
    };
    let sink = GcsSink::new(
        GcsSinkConfig::new(&bucket)
            .prefix("colj/")
            .auth(GcsCredentials::Anonymous)
            .storage_host(&host),
    )
    .await
    .unwrap();
    assert!(!sink.supports_columnar());
    let rows = vec![json!({"i": 1}), json!({"i": 2})];
    let batch = faucet_core::columnar::values_to_record_batch_inferred(&rows).unwrap();
    assert_eq!(sink.write_batch_columnar(&batch).await.unwrap(), 2);
    sink.flush().await.unwrap();
    let names = object_names(&host, &bucket, "colj/").await;
    let text = String::from_utf8(download(&host, &bucket, &names[0]).await).unwrap();
    assert_eq!(text.lines().count(), 2);
}

/// Avro objects (#719) are written whole on flush with the configured codec
/// and read back with their inferred schema.
#[cfg(feature = "file-format-avro")]
#[tokio::test]
async fn sink_writes_avro_objects() {
    let Some((_gcs, host, bucket)) = spawn_fake_gcs().await else {
        return;
    };
    let sink = GcsSink::new(
        GcsSinkConfig::new(&bucket)
            .prefix("avro/")
            .format(faucet_sink_gcs::GcsSinkFormat::Avro)
            .avro(faucet_core::AvroOptions {
                schema: None,
                codec: faucet_core::AvroCodec::Deflate,
            })
            .file_extension(".avro")
            .auth(GcsCredentials::Anonymous)
            .storage_host(&host),
    )
    .await
    .unwrap();
    let rows = [json!({"i": 1, "s": "a"}), json!({"i": 2, "s": null})];
    sink.write_batch(&rows).await.unwrap();
    sink.flush().await.unwrap();
    let names = object_names(&host, &bucket, "avro/").await;
    assert_eq!(names.len(), 1);
    let body = download(&host, &bucket, &names[0]).await;
    let back = faucet_core::file_format::avro::decode(&body, &faucet_core::AvroOptions::default())
        .unwrap();
    assert_eq!(back, rows.to_vec());
}
