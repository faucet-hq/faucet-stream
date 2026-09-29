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

/// Chainguard's maintained MinIO build: the upstream images stopped being
/// pullable (#694). Same server binary and CLI.
const MINIO_IMAGE_NAME: &str = "cgr.dev/chainguard/minio";
const MINIO_IMAGE_TAG: &str = "latest";
const ACCESS_KEY: &str = "minioadmin";
const SECRET_KEY: &str = "minioadmin";
const REGION: &str = "us-east-1";
const TEST_BUCKET: &str = "faucet-source-s3-formats";

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
        ..Default::default()
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

/// Avro and ORC objects (#719): every object in the prefix is resolved against
/// the first one's schema, on the row path and the columnar path alike.
#[cfg(all(feature = "file-format-avro", feature = "file-format-orc"))]
mod containers {
    use super::*;
    use faucet_core::{AvroCodec, AvroOptions, OrcOptions};

    const ORC: &[u8] = include_bytes!("../../../core/tests/fixtures/orc/people.orc");

    fn avro(records: &[Value], codec: AvroCodec) -> Vec<u8> {
        faucet_core::file_format::avro::encode(
            records,
            &AvroOptions {
                schema: None,
                codec,
            },
        )
        .expect("encode avro")
    }

    async fn columnar_rows(src: &S3Source) -> usize {
        let ctx = HashMap::new();
        assert!(src.supports_columnar());
        let mut batches = src.stream_batches(&ctx, 0);
        let mut n = 0;
        while let Some(page) = batches.next().await {
            n += page.expect("columnar page").num_rows();
        }
        n
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_mixed_avro_prefix_reads_as_one_shape() {
        let (_c, endpoint) = start_minio().await;
        seed(
            &endpoint,
            &[
                (
                    "avro/a.avro".into(),
                    avro(&[json!({"id": 1, "name": "a"})], AvroCodec::Null),
                ),
                (
                    "avro/b.avro".into(),
                    avro(&[json!({"id": 2, "name": "b"})], AvroCodec::Snappy),
                ),
                (
                    "avro/c.avro".into(),
                    avro(
                        &[json!({"id": 3, "name": "c", "extra": true})],
                        AvroCodec::Zstd,
                    ),
                ),
                (
                    "avro/d.avro".into(),
                    avro(&[json!({"id": 4, "name": "d"})], AvroCodec::Deflate),
                ),
                (
                    "bad/a.avro".into(),
                    avro(&[json!({"id": 1})], AvroCodec::Null),
                ),
                (
                    "bad/b.avro".into(),
                    avro(&[json!({"id": "x"})], AvroCodec::Null),
                ),
            ],
        )
        .await;
        let cfg = S3SourceConfig::new(TEST_BUCKET)
            .prefix("avro/")
            .file_format(S3FileFormat::Avro)
            .with_batch_size(2);
        let src = build_source(&endpoint, cfg).await;
        let want: Vec<Value> = ["a", "b", "c", "d"]
            .iter()
            .enumerate()
            .map(|(i, n)| json!({"id": i + 1, "name": n}))
            .collect();
        assert_eq!(drain(&src).await, want);
        assert_eq!(
            src.fetch_with_context(&HashMap::new())
                .await
                .expect("fetch"),
            want
        );
        assert_eq!(columnar_rows(&src).await, 4);

        let bad = build_source(
            &endpoint,
            S3SourceConfig::new(TEST_BUCKET)
                .prefix("bad/")
                .file_format(S3FileFormat::Avro),
        )
        .await;
        let ctx = HashMap::new();
        let mut pages = bad.stream_pages(&ctx, 0);
        let mut err = None;
        while let Some(page) = pages.next().await {
            if let Err(e) = page {
                err = Some(e.to_string());
            }
        }
        let err = err.expect("conflicting schemas fail");
        assert!(
            err.contains("bad/a.avro") && err.contains("bad/b.avro"),
            "{err}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn orc_objects_are_projected() {
        let (_c, endpoint) = start_minio().await;
        seed(&endpoint, &[("orc/p.orc".into(), ORC.to_vec())]).await;
        let mut cfg = S3SourceConfig::new(TEST_BUCKET)
            .prefix("orc/")
            .file_format(S3FileFormat::Orc)
            .with_batch_size(0);
        cfg.orc = OrcOptions {
            columns: Some(vec!["id".into(), "name".into()]),
        };
        let src = build_source(&endpoint, cfg).await;
        let rows = drain(&src).await;
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[2], json!({"id": 3, "name": "grace"}));
        assert_eq!(columnar_rows(&src).await, 3);
    }
}
