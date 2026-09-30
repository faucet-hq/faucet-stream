//! Format × compression composition through a real S3-compatible endpoint
//! (#604 × the `compression` feature).
//!
//! Both crate READMEs state "format composes with compression", and nothing
//! tested it. The claim spans four steps in two crates — the sink encodes a
//! whole-object format and *then* compresses (`write_encoded_object` →
//! `upload_file` → `encode_body`); the source decompresses and *then* decodes
//! (`read_object_all` → `open_object_reader` → `fetch_decoded`). Each half
//! has unit tests in isolation, and each would still pass if the other
//! silently dropped its step: a sink that skipped compression writes a plain
//! CSV under a `.csv.gz` key, which a source that skipped decompression reads
//! back perfectly. Only an end-to-end pair with an assertion on the *stored
//! bytes* can tell the composed path from the two no-ops.
//!
//! Requires Docker. Each test boots its own MinIO container and bucket.
#![cfg(all(feature = "compression", feature = "file-format-csv"))]

use aws_config::BehaviorVersion;
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::{Client, Config as S3Config};
use faucet_core::file_format::{FileFormat, FormatOptions, encode};
use faucet_core::{CompressionConfig, Sink, Source};
use faucet_sink_s3::{S3Sink, S3SinkConfig, S3SinkFormat};
use faucet_source_s3::{S3FileFormat, S3Source, S3SourceConfig};
use futures::StreamExt;
use serde_json::{Value, json};
use testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};
use testcontainers_modules::minio::MinIO;

/// Chainguard's maintained MinIO build: the upstream images stopped being
/// pullable (#694). Same server binary and CLI.
const MINIO_IMAGE_NAME: &str = "cgr.dev/chainguard/minio";
const MINIO_IMAGE_TAG: &str = "latest";
const ACCESS_KEY: &str = "minioadmin";
const SECRET_KEY: &str = "minioadmin";
const REGION: &str = "us-east-1";
const TEST_BUCKET: &str = "faucet-s3-format-compression";

const GZIP_MAGIC: &[u8] = &[0x1f, 0x8b];
const ZSTD_MAGIC: &[u8] = &[0x28, 0xb5, 0x2f, 0xfd];

async fn start_minio() -> (ContainerAsync<MinIO>, String) {
    let container: ContainerAsync<MinIO> = MinIO::default()
        .with_name(MINIO_IMAGE_NAME)
        .with_tag(MINIO_IMAGE_TAG)
        .with_mapped_port(0, testcontainers::core::IntoContainerPort::tcp(9000))
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

fn creds() {
    // SAFETY: every test boots its own container with the same default MinIO
    // credentials, so overlapping writes set an identical value.
    unsafe {
        std::env::set_var("AWS_ACCESS_KEY_ID", ACCESS_KEY);
        std::env::set_var("AWS_SECRET_ACCESS_KEY", SECRET_KEY);
        std::env::set_var("AWS_DEFAULT_REGION", REGION);
    }
}

async fn build_sink(endpoint: &str, config: S3SinkConfig) -> S3Sink {
    creds();
    S3Sink::new(
        config
            .endpoint_url(endpoint.to_string())
            .region(REGION.to_string()),
    )
    .await
    .expect("S3Sink::new")
}

async fn build_source(endpoint: &str, config: S3SourceConfig) -> S3Source {
    creds();
    S3Source::new(
        config
            .endpoint_url(endpoint.to_string())
            .region(REGION.to_string()),
    )
    .await
    .expect("S3Source::new")
}

/// The raw stored bytes of the single object under `prefix` — never passed
/// through the source, so decompression cannot hide from this assertion.
async fn sole_object_raw(client: &Client, prefix: &str) -> Vec<u8> {
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
    client
        .get_object()
        .bucket(TEST_BUCKET)
        .key(&keys[0])
        .send()
        .await
        .expect("get object")
        .body
        .collect()
        .await
        .expect("read body")
        .into_bytes()
        .to_vec()
}

async fn drain(src: &S3Source) -> Vec<Value> {
    let ctx: std::collections::HashMap<String, Value> = std::collections::HashMap::new();
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

/// Write one whole-object format compressed, assert the *stored* bytes really
/// are that codec, then read it back through the source.
async fn round_trip(
    sink_fmt: S3SinkFormat,
    src_fmt: S3FileFormat,
    shared: FileFormat,
    ext: &str,
    magic: &[u8],
    label: &str,
) {
    let (_c, endpoint) = start_minio().await;
    let client = seed(&endpoint).await;
    let prefix = format!("{label}/");

    let sink = build_sink(
        &endpoint,
        S3SinkConfig::new(TEST_BUCKET)
            .prefix(&prefix)
            .format(sink_fmt)
            .file_extension(ext),
    )
    .await;
    let expected = records();
    assert_eq!(
        sink.write_batch(&expected).await.expect("write"),
        expected.len()
    );
    // A whole-object format cannot know the group is complete before flush.
    sink.flush().await.expect("flush");

    // The object as stored: compressed, and therefore NOT the plain encoding.
    let raw = sole_object_raw(&client, &prefix).await;
    assert!(
        raw.starts_with(magic),
        "{label}: stored object is not {ext} data — the sink encoded the \
         format but skipped compression; first bytes: {:02x?}",
        &raw[..magic.len().min(raw.len())]
    );
    // Codec-agnostic: the stored body must differ from the plain encoding.
    // (A magic prefix alone could in principle be a framed no-op, and zstd
    // legitimately stores a small payload's literals verbatim, so a
    // plaintext-substring check would be a false failure here.)
    let plain = encode(&expected, shared, &FormatOptions::default()).expect("plain encode");
    assert_ne!(
        raw, plain,
        "{label}: the stored object is byte-identical to the uncompressed \
         encoding — the sink encoded the format but skipped compression"
    );

    // And the source pulls the records back out of it.
    let src = build_source(
        &endpoint,
        S3SourceConfig::new(TEST_BUCKET)
            .prefix(&prefix)
            .file_format(src_fmt)
            .with_batch_size(0),
    )
    .await;
    assert_eq!(
        drain(&src).await,
        expected,
        "{label}: the compressed object did not decode back"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn gzipped_csv_objects_round_trip() {
    round_trip(
        S3SinkFormat::Csv,
        S3FileFormat::Csv,
        FileFormat::Csv,
        ".csv.gz",
        GZIP_MAGIC,
        "csv-gz",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn zstd_csv_objects_round_trip() {
    round_trip(
        S3SinkFormat::Csv,
        S3FileFormat::Csv,
        FileFormat::Csv,
        ".csv.zst",
        ZSTD_MAGIC,
        "csv-zst",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn gzipped_json_array_objects_round_trip() {
    round_trip(
        S3SinkFormat::JsonArray,
        S3FileFormat::JsonArray,
        FileFormat::JsonArray,
        ".json.gz",
        GZIP_MAGIC,
        "jsonarray-gz",
    )
    .await;
}

#[cfg(feature = "file-format-xml")]
#[tokio::test(flavor = "multi_thread")]
async fn gzipped_xml_objects_round_trip() {
    round_trip(
        S3SinkFormat::Xml,
        S3FileFormat::Xml,
        FileFormat::Xml,
        ".xml.gz",
        GZIP_MAGIC,
        "xml-gz",
    )
    .await;
}

/// `compression: Auto` reads the codec off the extension, but an explicit
/// codec must win — otherwise a bucket convention that does not spell the
/// suffix (`.csv` served gzipped) silently stores plaintext, and the reader
/// that *does* set the codec then fails on the first byte.
#[tokio::test(flavor = "multi_thread")]
async fn an_explicit_codec_beats_the_extension_on_both_sides() {
    let (_c, endpoint) = start_minio().await;
    let client = seed(&endpoint).await;
    let prefix = "explicit/";

    let sink = build_sink(
        &endpoint,
        S3SinkConfig::new(TEST_BUCKET)
            .prefix(prefix)
            .format(S3SinkFormat::Csv)
            // No `.gz` suffix — only the explicit codec asks for gzip.
            .file_extension(".csv")
            .compression(CompressionConfig::Gzip),
    )
    .await;
    let expected = records();
    sink.write_batch(&expected).await.expect("write");
    sink.flush().await.expect("flush");

    let raw = sole_object_raw(&client, prefix).await;
    assert!(
        raw.starts_with(GZIP_MAGIC),
        "an explicit codec must apply even when the extension does not ask \
         for it; first bytes: {:02x?}",
        &raw[..2.min(raw.len())]
    );

    // Auto would resolve `.csv` to None and hand gzip bytes to the CSV
    // decoder; the explicit codec is what makes this readable.
    let src = build_source(
        &endpoint,
        S3SourceConfig::new(TEST_BUCKET)
            .prefix(prefix)
            .file_format(S3FileFormat::Csv)
            .compression(CompressionConfig::Gzip)
            .with_batch_size(0),
    )
    .await;
    assert_eq!(drain(&src).await, expected);
}
