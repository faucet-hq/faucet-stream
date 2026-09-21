//! Round-trip every shared file format through a real S3-compatible endpoint
//! (#604): the sink encodes a whole-object format, and the object that lands
//! in the bucket decodes back to the records that went in.
//!
//! The point is the *pair*. Unit tests already pin `faucet_core::file_format`
//! in isolation; what only an integration test can show is that the sink's
//! buffered-group path — the one every format except JSON Lines takes —
//! actually produces bytes the shared decoder reads back.
//!
//! Requires Docker. Each test boots its own MinIO container and bucket.
#![cfg(feature = "file-format-csv")]

use aws_config::BehaviorVersion;
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::{Client, Config as S3Config};
use faucet_core::Sink;
use faucet_core::file_format::{FileFormat, FormatOptions, decode};
use faucet_sink_s3::{S3Sink, S3SinkConfig, S3SinkFormat};
use serde_json::{Value, json};
use testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};
use testcontainers_modules::minio::MinIO;

const MINIO_IMAGE_NAME: &str = "quay.io/minio/minio";
const ACCESS_KEY: &str = "minioadmin";
const SECRET_KEY: &str = "minioadmin";
const REGION: &str = "us-east-1";
const TEST_BUCKET: &str = "faucet-sink-s3-formats";

async fn start_minio() -> (ContainerAsync<MinIO>, String) {
    let container: ContainerAsync<MinIO> = MinIO::default()
        .with_name(MINIO_IMAGE_NAME)
        .start()
        .await
        .expect("minio container start");
    let port = container
        .get_host_port_ipv4(9000)
        .await
        .expect("minio port");
    (container, format!("http://127.0.0.1:{port}"))
}

async fn admin(endpoint: &str) -> Client {
    let creds = Credentials::new(ACCESS_KEY, SECRET_KEY, None, None, "test");
    let sdk_config = aws_config::defaults(BehaviorVersion::latest())
        .region(aws_config::Region::new(REGION))
        .endpoint_url(endpoint)
        .credentials_provider(creds)
        .load()
        .await;
    Client::from_conf(
        S3Config::from(&sdk_config)
            .to_builder()
            .force_path_style(true)
            .build(),
    )
}

async fn seed(endpoint: &str) -> Client {
    let client = admin(endpoint).await;
    client
        .create_bucket()
        .bucket(TEST_BUCKET)
        .send()
        .await
        .expect("create bucket");
    client
}

async fn build_sink(endpoint: &str, config: S3SinkConfig) -> S3Sink {
    // SAFETY: every test boots its own container with the same default MinIO
    // credentials, so overlapping writes set an identical value.
    unsafe {
        std::env::set_var("AWS_ACCESS_KEY_ID", ACCESS_KEY);
        std::env::set_var("AWS_SECRET_ACCESS_KEY", SECRET_KEY);
        std::env::set_var("AWS_DEFAULT_REGION", REGION);
    }
    S3Sink::new(
        config
            .endpoint_url(endpoint.to_string())
            .region(REGION.to_string()),
    )
    .await
    .expect("S3Sink::new")
}

/// The single object written under `prefix`, as raw bytes.
async fn sole_object(client: &Client, prefix: &str) -> Vec<u8> {
    let listed = client
        .list_objects_v2()
        .bucket(TEST_BUCKET)
        .prefix(prefix)
        .send()
        .await
        .expect("list");
    let keys: Vec<String> = listed
        .contents()
        .iter()
        .filter_map(|o| o.key().map(str::to_string))
        .collect();
    assert_eq!(keys.len(), 1, "expected exactly one object, got {keys:?}");
    let body = client
        .get_object()
        .bucket(TEST_BUCKET)
        .key(&keys[0])
        .send()
        .await
        .expect("get object");
    body.body
        .collect()
        .await
        .expect("read body")
        .into_bytes()
        .to_vec()
}

fn records() -> Vec<Value> {
    vec![
        json!({"id": "1", "name": "ada"}),
        json!({"id": "2", "name": "grace"}),
    ]
}

/// Write one whole-object format and decode the landed object back.
async fn round_trip(format: S3SinkFormat, shared: FileFormat, ext: &str) {
    let (_c, endpoint) = start_minio().await;
    let client = seed(&endpoint).await;
    let prefix = format!("{}/", shared.as_str());

    let sink = build_sink(
        &endpoint,
        S3SinkConfig::new(TEST_BUCKET)
            .prefix(&prefix)
            .format(format)
            .file_extension(ext),
    )
    .await;

    let expected = records();
    assert_eq!(
        sink.write_batch(&expected).await.expect("write"),
        expected.len()
    );
    // Nothing is uploaded until flush: a whole-object format cannot know the
    // group is complete before then.
    sink.flush().await.expect("flush");

    let bytes = sole_object(&client, &prefix).await;
    let back = decode(&bytes, shared, &FormatOptions::default())
        .await
        .expect("decode the landed object");
    assert_eq!(back, expected, "{shared:?} did not round-trip through S3");
}

#[tokio::test]
async fn csv_objects_round_trip() {
    round_trip(S3SinkFormat::Csv, FileFormat::Csv, ".csv").await;
}

#[tokio::test]
async fn json_array_objects_round_trip() {
    round_trip(S3SinkFormat::JsonArray, FileFormat::JsonArray, ".json").await;
}

#[cfg(feature = "file-format-xml")]
#[tokio::test]
async fn xml_objects_round_trip() {
    round_trip(S3SinkFormat::Xml, FileFormat::Xml, ".xml").await;
}

#[cfg(feature = "file-format-excel")]
#[tokio::test]
async fn xlsx_objects_round_trip() {
    round_trip(S3SinkFormat::Xlsx, FileFormat::Xlsx, ".xlsx").await;
}

/// The record cap rolls a whole-object format exactly as it rolls JSON Lines,
/// so object sizing means the same thing whatever the format.
#[tokio::test]
async fn the_record_cap_rolls_a_whole_object_format() {
    let (_c, endpoint) = start_minio().await;
    let client = seed(&endpoint).await;
    let sink = build_sink(
        &endpoint,
        S3SinkConfig::new(TEST_BUCKET)
            .prefix("rolled/")
            .format(S3SinkFormat::Csv)
            .file_extension(".csv")
            .max_records_per_file(2),
    )
    .await;

    for _ in 0..3 {
        sink.write_batch(&[json!({"id": "x"}), json!({"id": "y"})])
            .await
            .expect("write");
    }
    sink.flush().await.expect("flush");

    let listed = client
        .list_objects_v2()
        .bucket(TEST_BUCKET)
        .prefix("rolled/")
        .send()
        .await
        .expect("list");
    assert_eq!(
        listed.contents().len(),
        3,
        "six records at a cap of two is three objects"
    );
}

/// An empty page must not leave a stray zero-record object behind.
#[tokio::test]
async fn an_empty_write_leaves_no_object() {
    let (_c, endpoint) = start_minio().await;
    let client = seed(&endpoint).await;
    let sink = build_sink(
        &endpoint,
        S3SinkConfig::new(TEST_BUCKET)
            .prefix("empty/")
            .format(S3SinkFormat::Csv)
            .file_extension(".csv"),
    )
    .await;

    assert_eq!(sink.write_batch(&[]).await.expect("write"), 0);
    sink.flush().await.expect("flush");

    let listed = client
        .list_objects_v2()
        .bucket(TEST_BUCKET)
        .prefix("empty/")
        .send()
        .await
        .expect("list");
    assert!(listed.contents().is_empty(), "no object for an empty page");
}
