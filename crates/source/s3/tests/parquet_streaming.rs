//! #619 — Parquet objects are read row group at a time, not buffered whole.
//!
//! The row and columnar paths used to pull an entire Parquet object into
//! `Bytes` before decoding, so peak memory was the size of the largest object.
//! These tests pin the replacement through its two observable consequences:
//! records are identical to the buffered path (the ranged reader is correct),
//! and the first page arrives long before the whole object could have been
//! downloaded (it is genuinely incremental).
//!
//! Requires Docker.

#![cfg(feature = "arrow")]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use aws_config::BehaviorVersion;
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::{Client, Config as S3Config};
use faucet_core::Source;
use faucet_source_s3::{S3FileFormat, S3Source, S3SourceConfig};
use futures::StreamExt;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};
use testcontainers_modules::minio::MinIO;

/// See the note in `streaming.rs`: MinIO's Docker Hub repository was withdrawn,
/// so the identical pinned release is pulled from Quay.
/// Chainguard's maintained MinIO build: the upstream images stopped being
/// pullable (#694). Same server binary and CLI.
const MINIO_IMAGE_NAME: &str = "cgr.dev/chainguard/minio";
const MINIO_IMAGE_TAG: &str = "latest";
const ACCESS_KEY: &str = "minioadmin";
const SECRET_KEY: &str = "minioadmin";
const REGION: &str = "us-east-1";
const BUCKET: &str = "faucet-parquet-tests";

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

async fn admin_client(endpoint: &str) -> Client {
    let creds = Credentials::new(ACCESS_KEY, SECRET_KEY, None, None, "test");
    let sdk = aws_config::defaults(BehaviorVersion::latest())
        .region(aws_config::Region::new(REGION))
        .endpoint_url(endpoint)
        .credentials_provider(creds)
        .load()
        .await;
    Client::from_conf(
        S3Config::from(&sdk)
            .to_builder()
            .force_path_style(true)
            .build(),
    )
}

/// A Parquet object with `rows` rows split into row groups of `group` rows, so
/// there is something to stream *within* one object.
fn parquet_bytes(rows: i64, group: usize, payload_len: usize) -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(group))
        .build();
    let mut out = Vec::new();
    {
        let mut writer =
            ArrowWriter::try_new(&mut out, schema.clone(), Some(props)).expect("writer");
        // Write in chunks so the test itself does not build one giant array.
        let chunk = group.max(1);
        let mut written = 0i64;
        while written < rows {
            let n = std::cmp::min(chunk as i64, rows - written);
            let ids: Vec<i64> = (written..written + n).collect();
            // Vary the payload so it does not dictionary-compress to nothing —
            // the object has to be big enough for "downloaded it all" and
            // "downloaded a row group" to be distinguishable.
            let payloads: Vec<String> = ids
                .iter()
                .map(|i| format!("{i:0width$}", width = payload_len))
                .collect();
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(ids.clone())),
                    Arc::new(StringArray::from(payloads)),
                ],
            )
            .expect("batch");
            writer.write(&batch).expect("write");
            written += n;
        }
        writer.close().expect("close");
    }
    out
}

async fn seed(endpoint: &str, key: &str, body: Vec<u8>) {
    let client = admin_client(endpoint).await;
    // The bucket may already exist when a test seeds twice.
    let _ = client.create_bucket().bucket(BUCKET).send().await;
    client
        .put_object()
        .bucket(BUCKET)
        .key(key)
        .body(ByteStream::from(body))
        .send()
        .await
        .expect("put object");
}

async fn build_source(endpoint: &str, config: S3SourceConfig) -> S3Source {
    // SAFETY: as in `streaming.rs` — every test sets the same constants.
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

fn config(batch: usize) -> S3SourceConfig {
    S3SourceConfig::new(BUCKET)
        .file_format(S3FileFormat::Parquet)
        .with_batch_size(batch)
}

async fn collect_ids(source: &S3Source, batch: usize) -> Vec<i64> {
    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, batch);
    let mut ids = Vec::new();
    while let Some(page) = pages.next().await {
        for record in page.expect("page ok").records {
            ids.push(record["id"].as_i64().expect("id"));
        }
    }
    ids
}

#[tokio::test(flavor = "multi_thread")]
async fn the_ranged_reader_yields_exactly_what_the_buffered_reader_did() {
    // Correctness first: a byte-range reader that got an offset wrong would
    // produce plausible-looking but wrong rows, which is the worst outcome
    // available here. `verify_checksum` forces the old whole-object path, so
    // the two can be compared directly on the same object.
    let (_c, endpoint) = start_minio().await;
    seed(&endpoint, "cmp.parquet", parquet_bytes(20_000, 1_000, 40)).await;

    let ranged = build_source(&endpoint, config(500)).await;
    let meter = Arc::new(faucet_core::UsageMeter::new());
    ranged.set_roundtrip_recorder(Arc::new(
        faucet_core::observability::RoundtripRecorder::new(
            faucet_core::observability::RoundtripSide::Source,
            "p",
            "r",
            "s3",
        )
        .with_meter(meter.clone()),
    ));
    let streamed = collect_ids(&ranged, 500).await;
    let ops = meter.snapshot().source_roundtrips;
    assert!(ops.get("list").is_some_and(|n| *n >= 1), "{ops:?}");
    assert!(ops.get("head").is_some_and(|n| *n >= 1), "{ops:?}");
    assert!(ops.get("get").is_some_and(|n| *n >= 1), "{ops:?}");
    let buffered = collect_ids(
        &build_source(&endpoint, config(500).verify_checksum(true)).await,
        500,
    )
    .await;

    assert_eq!(streamed.len(), 20_000, "every row must arrive");
    assert_eq!(
        streamed,
        (0..20_000).collect::<Vec<i64>>(),
        "rows must stay in file order"
    );
    assert_eq!(
        streamed, buffered,
        "the ranged reader disagrees with the whole-object reader"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_first_page_arrives_without_downloading_the_whole_object() {
    // The memory claim, measured through the consequence a test can see: the
    // buffered path cannot produce a record until the entire body has arrived,
    // so on a large object its first page is dramatically later. If this ever
    // reverts to whole-object buffering, the two become indistinguishable.
    let (_c, endpoint) = start_minio().await;
    // ~200k rows x a 200-char payload: large enough that downloading all of it
    // is clearly separable from downloading one 5k-row group.
    seed(&endpoint, "big.parquet", parquet_bytes(200_000, 5_000, 200)).await;

    async fn time_to_first_page(source: &S3Source, batch: usize) -> Duration {
        let ctx: HashMap<String, serde_json::Value> = HashMap::new();
        let started = Instant::now();
        let mut pages = source.stream_pages(&ctx, batch);
        let first = pages.next().await.expect("a first page");
        first.expect("the first page reads");
        started.elapsed()
    }

    let streaming = build_source(&endpoint, config(1_000)).await;
    let buffering = build_source(&endpoint, config(1_000).verify_checksum(true)).await;
    // Untimed warm-up so neither measurement pays credential resolution or
    // connection setup.
    time_to_first_page(&streaming, 1_000).await;
    time_to_first_page(&buffering, 1_000).await;

    let streamed = time_to_first_page(&streaming, 1_000).await;
    let buffered = time_to_first_page(&buffering, 1_000).await;

    assert!(
        streamed < buffered,
        "the first page took {streamed:?} streaming and {buffered:?} buffering the whole \
         object — no faster, so the read is not incremental (#619)"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_columnar_path_streams_row_groups_and_bounds_the_batch() {
    // `stream_batches` is the Arrow-native path a parquet -> parquet/delta
    // chain uses. It must stream the same way, and honour `batch_size` so one
    // huge row group is still decoded in steps.
    let (_c, endpoint) = start_minio().await;
    seed(&endpoint, "col.parquet", parquet_bytes(12_000, 6_000, 40)).await;

    let source = build_source(&endpoint, config(1_000)).await;
    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_batches(&ctx, 1_000);

    let mut rows = 0usize;
    let mut batches = 0usize;
    while let Some(page) = pages.next().await {
        let page = page.expect("columnar page ok");
        assert!(
            page.batch.num_rows() <= 1_000,
            "a batch of {} rows ignores batch_size, so a single row group would be decoded \
             whole",
            page.batch.num_rows()
        );
        rows += page.batch.num_rows();
        batches += 1;
    }
    assert_eq!(rows, 12_000);
    assert!(
        batches >= 12,
        "12_000 rows at batch_size 1_000 must arrive in at least 12 batches, got {batches}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_schema_mismatch_between_objects_is_still_caught() {
    // The streaming path gets its schema from the footer rather than from a
    // decoded object, so the cross-object consistency check had to be re-wired.
    let (_c, endpoint) = start_minio().await;
    seed(&endpoint, "m-a.parquet", parquet_bytes(100, 50, 10)).await;

    // A second object with a different schema.
    let other = {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "other",
            DataType::Int64,
            false,
        )]));
        let mut out = Vec::new();
        {
            let mut w = ArrowWriter::try_new(&mut out, schema.clone(), None).expect("writer");
            w.write(
                &RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1i64, 2]))])
                    .expect("batch"),
            )
            .expect("write");
            w.close().expect("close");
        }
        out
    };
    seed(&endpoint, "m-b.parquet", other).await;

    let source = build_source(&endpoint, config(100)).await;
    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_batches(&ctx, 100);

    let mut err = None;
    while let Some(page) = pages.next().await {
        if let Err(e) = page {
            err = Some(e);
            break;
        }
    }
    let err = err.expect("a divergent schema must fail the columnar stream");
    assert!(
        err.to_string().contains("schema mismatch"),
        "the failure must name the problem: {err}"
    );
}
