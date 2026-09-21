//! Read the shared file formats out of a real S3-compatible endpoint (#604).
//!
//! `fetch_decoded` is the whole-object path: CSV, XML and Excel are read in
//! one piece and decoded through `faucet_core::file_format` before the page
//! loop chunks them. A unit test of the decoder says nothing about whether
//! the object body reaches it, which is what these cover.
#![cfg(feature = "file-format-csv")]

use std::collections::HashMap;

use aws_config::BehaviorVersion;
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::{Client, Config as S3Config};
use faucet_core::Source;
use faucet_core::file_format::{FileFormat, FormatOptions, encode};
use faucet_source_s3::{S3FileFormat, S3Source, S3SourceConfig};
use futures::StreamExt;
use serde_json::{Value, json};
use testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};
use testcontainers_modules::minio::MinIO;

const MINIO_IMAGE_NAME: &str = "quay.io/minio/minio";
const ACCESS_KEY: &str = "minioadmin";
const SECRET_KEY: &str = "minioadmin";
const REGION: &str = "us-east-1";
const TEST_BUCKET: &str = "faucet-source-s3-formats";

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

async fn seed(endpoint: &str, objects: &[(String, Vec<u8>)]) {
    let creds = Credentials::new(ACCESS_KEY, SECRET_KEY, None, None, "test");
    let sdk_config = aws_config::defaults(BehaviorVersion::latest())
        .region(aws_config::Region::new(REGION))
        .endpoint_url(endpoint)
        .credentials_provider(creds)
        .load()
        .await;
    let client = Client::from_conf(
        S3Config::from(&sdk_config)
            .to_builder()
            .force_path_style(true)
            .build(),
    );
    client
        .create_bucket()
        .bucket(TEST_BUCKET)
        .send()
        .await
        .expect("create bucket");
    for (key, body) in objects {
        client
            .put_object()
            .bucket(TEST_BUCKET)
            .key(key)
            .body(ByteStream::from(body.clone()))
            .send()
            .await
            .expect("put object");
    }
}

async fn build_source(endpoint: &str, config: S3SourceConfig) -> S3Source {
    // SAFETY: every test boots its own container with the same default MinIO
    // credentials, so overlapping writes set an identical value.
    unsafe {
        std::env::set_var("AWS_ACCESS_KEY_ID", ACCESS_KEY);
        std::env::set_var("AWS_SECRET_ACCESS_KEY", SECRET_KEY);
        std::env::set_var("AWS_DEFAULT_REGION", REGION);
    }
    S3Source::new(
        config
            .endpoint_url(endpoint.to_string())
            .region(REGION.to_string()),
    )
    .await
    .expect("S3Source::new")
}

async fn drain(src: &S3Source) -> Vec<Value> {
    let ctx: HashMap<String, Value> = HashMap::new();
    let mut pages = src.stream_pages(&ctx, 0);
    let mut out = Vec::new();
    while let Some(page) = pages.next().await {
        out.extend(page.expect("page").records);
    }
    out
}

fn records() -> Vec<Value> {
    vec![
        json!({"id": "1", "name": "ada"}),
        json!({"id": "2", "name": "grace"}),
    ]
}

/// Seed one object encoded with the shared writer — the bytes a faucet sink
/// would have produced — then read it back through the source.
async fn round_trip(fmt: S3FileFormat, shared: FileFormat, key: &str) {
    let body = encode(&records(), shared, &FormatOptions::default()).expect("encode fixture");
    let (_c, endpoint) = start_minio().await;
    seed(&endpoint, &[(key.to_string(), body)]).await;
    let src = build_source(
        &endpoint,
        S3SourceConfig::new(TEST_BUCKET)
            .file_format(fmt)
            .with_batch_size(0),
    )
    .await;
    assert_eq!(drain(&src).await, records(), "{shared:?} did not read back");
}

#[tokio::test(flavor = "multi_thread")]
async fn csv_objects_are_decoded() {
    round_trip(S3FileFormat::Csv, FileFormat::Csv, "rows.csv").await;
}

#[cfg(feature = "file-format-xml")]
#[tokio::test(flavor = "multi_thread")]
async fn xml_objects_are_decoded() {
    round_trip(S3FileFormat::Xml, FileFormat::Xml, "rows.xml").await;
}

#[cfg(feature = "file-format-excel")]
#[tokio::test(flavor = "multi_thread")]
async fn xlsx_objects_are_decoded() {
    round_trip(S3FileFormat::Xlsx, FileFormat::Xlsx, "rows.xlsx").await;
}

/// A non-default dialect has to reach the decoder, or every row parses as a
/// single column and the run looks successful.
#[tokio::test(flavor = "multi_thread")]
async fn the_configured_csv_dialect_is_honoured() {
    let (_c, endpoint) = start_minio().await;
    seed(
        &endpoint,
        &[("semis.csv".to_string(), b"id;name\n1;ada\n".to_vec())],
    )
    .await;
    let mut cfg = S3SourceConfig::new(TEST_BUCKET)
        .file_format(S3FileFormat::Csv)
        .with_batch_size(0);
    cfg.csv = faucet_core::CsvOptions {
        delimiter: ";".into(),
        has_headers: true,
    };
    let src = build_source(&endpoint, cfg).await;
    assert_eq!(drain(&src).await, vec![json!({"id": "1", "name": "ada"})]);
}

/// Decoded records chunk into pages exactly as a JSON array's do.
#[tokio::test(flavor = "multi_thread")]
async fn decoded_records_chunk_at_the_batch_size() {
    let rows: Vec<Value> = (1..=5).map(|i| json!({"id": i.to_string()})).collect();
    let body = encode(&rows, FileFormat::Csv, &FormatOptions::default()).expect("encode");
    let (_c, endpoint) = start_minio().await;
    seed(&endpoint, &[("many.csv".to_string(), body)]).await;
    let src = build_source(
        &endpoint,
        S3SourceConfig::new(TEST_BUCKET)
            .file_format(S3FileFormat::Csv)
            .with_batch_size(2),
    )
    .await;

    let ctx: HashMap<String, Value> = HashMap::new();
    let mut pages = src.stream_pages(&ctx, 2);
    let mut sizes = Vec::new();
    let mut total = 0;
    while let Some(page) = pages.next().await {
        let n = page.expect("page").records.len();
        sizes.push(n);
        total += n;
    }
    assert_eq!(total, 5);
    assert_eq!(
        sizes,
        vec![2, 2, 1],
        "a trailing partial page is still emitted"
    );
}
