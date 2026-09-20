//! Integration tests for `S3Sink::write_batch` write-side re-chunking,
//! exercised against a real S3-compatible endpoint (MinIO) via testcontainers.
//!
//! These tests require Docker. Each test boots its own container and seeds
//! its own bucket so they are fully isolated and safe to run in parallel.

use aws_config::BehaviorVersion;
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::{Client, Config as S3Config};
use faucet_core::Sink;
use faucet_sink_s3::{S3Sink, S3SinkConfig};
use serde_json::{Value, json};
use testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};
use testcontainers_modules::minio::MinIO;

/// MinIO's Docker Hub repository was withdrawn (September 2026): pulling
/// `minio/minio` fails with "repository does not exist / access denied". The
/// identical pinned release remains published on Quay, so only the module's
/// default image *name* is overridden — tag, cmd, and wait behavior stay
/// those of `testcontainers_modules::minio`.
const MINIO_IMAGE_NAME: &str = "quay.io/minio/minio";

const ACCESS_KEY: &str = "minioadmin";
const SECRET_KEY: &str = "minioadmin";
const REGION: &str = "us-east-1";
const TEST_BUCKET: &str = "faucet-sink-s3-tests";

/// Start a MinIO container and return the container handle plus the
/// `http://host:port` endpoint URL. The container is kept alive by the
/// returned handle; drop it to stop the container.
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
    let endpoint = format!("http://127.0.0.1:{port}");
    (container, endpoint)
}

/// Build a path-style aws-sdk-s3 admin client pointed at the MinIO endpoint.
async fn build_admin_client(endpoint: &str) -> Client {
    let creds = Credentials::new(ACCESS_KEY, SECRET_KEY, None, None, "test");
    let sdk_config = aws_config::defaults(BehaviorVersion::latest())
        .region(aws_config::Region::new(REGION))
        .endpoint_url(endpoint)
        .credentials_provider(creds)
        .load()
        .await;
    let s3_config = S3Config::from(&sdk_config)
        .to_builder()
        .force_path_style(true)
        .build();
    Client::from_conf(s3_config)
}

/// Create the test bucket.
async fn create_bucket(endpoint: &str) {
    let client = build_admin_client(endpoint).await;
    client
        .create_bucket()
        .bucket(TEST_BUCKET)
        .send()
        .await
        .expect("create bucket");
}

/// Build an `S3Sink` configured against MinIO. Credentials are passed via env
/// vars because the sink's `build_client` honours the standard AWS credential
/// chain and has no field for inline credentials.
async fn build_sink(endpoint: &str, config: S3SinkConfig) -> S3Sink {
    // SAFETY: tests are serialised on these env vars only loosely — each test
    // boots its own container with the same default MinIO credentials, so the
    // value written is the same across overlapping tests.
    unsafe {
        std::env::set_var("AWS_ACCESS_KEY_ID", ACCESS_KEY);
        std::env::set_var("AWS_SECRET_ACCESS_KEY", SECRET_KEY);
        std::env::set_var("AWS_DEFAULT_REGION", REGION);
    }
    let config = config
        .endpoint_url(endpoint.to_string())
        .region(REGION.to_string());
    S3Sink::new(config).await.expect("S3Sink::new")
}

/// Path-style admin client for assertions against the seeded bucket.
async fn assertion_client(endpoint: &str) -> Client {
    build_admin_client(endpoint).await
}

/// List every key under the given prefix in the test bucket.
async fn list_keys(client: &Client, prefix: &str) -> Vec<String> {
    let resp = client
        .list_objects_v2()
        .bucket(TEST_BUCKET)
        .prefix(prefix)
        .send()
        .await
        .expect("list objects");
    resp.contents()
        .iter()
        .filter_map(|o| o.key().map(|k| k.to_string()))
        .collect()
}

/// Fetch the body of a single object and parse it as a JSONL stream of
/// `serde_json::Value`s.
async fn fetch_jsonl(client: &Client, key: &str) -> Vec<Value> {
    let resp = client
        .get_object()
        .bucket(TEST_BUCKET)
        .key(key)
        .send()
        .await
        .expect("get object");
    let bytes = resp
        .body
        .collect()
        .await
        .expect("collect body")
        .into_bytes();
    let body = String::from_utf8(bytes.to_vec()).expect("utf-8");
    body.lines()
        .filter(|l| !l.is_empty())
        .map(|l| serde_json::from_str(l).expect("valid json line"))
        .collect()
}

/// Build `n` records of `{"id": i}` for `i = 1..=n`.
fn records(n: usize) -> Vec<Value> {
    (1..=n as i64).map(|i| json!({ "id": i })).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn write_batch_rechunks_into_batch_size_objects() {
    let (_container, endpoint) = start_minio().await;
    create_bucket(&endpoint).await;

    let prefix = "rechunk/";
    let config = S3SinkConfig::new(TEST_BUCKET)
        .prefix(prefix)
        .with_batch_size(500);
    let sink = build_sink(&endpoint, config).await;

    let written = sink.write_batch(&records(1_500)).await.expect("write");
    assert_eq!(written, 1_500, "all records reported written");
    // Since #618 the remainder of the open object is closed at `flush`, which
    // the pipeline calls at every bookmark-carrying page and at the end.
    sink.flush().await.expect("flush");

    let admin = assertion_client(&endpoint).await;
    let keys = list_keys(&admin, prefix).await;
    assert_eq!(
        keys.len(),
        3,
        "1500 records with batch_size 500 must produce 3 objects, got {:?}",
        keys
    );

    let mut all_ids: Vec<i64> = Vec::new();
    for key in &keys {
        let recs = fetch_jsonl(&admin, key).await;
        assert_eq!(
            recs.len(),
            500,
            "each of the 3 objects must contain exactly 500 records; key={key}"
        );
        for r in recs {
            all_ids.push(r["id"].as_i64().expect("id is integer"));
        }
    }
    all_ids.sort_unstable();
    let expected: Vec<i64> = (1..=1500).collect();
    assert_eq!(
        all_ids, expected,
        "every record id round-trips through the 3 objects exactly once"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn write_batch_sentinel_writes_one_object_for_the_whole_run() {
    let (_container, endpoint) = start_minio().await;
    create_bucket(&endpoint).await;

    let prefix = "sentinel/";
    let config = S3SinkConfig::new(TEST_BUCKET)
        .prefix(prefix)
        .with_batch_size(0);
    let sink = build_sink(&endpoint, config).await;

    let written = sink.write_batch(&records(1_500)).await.expect("write");
    assert_eq!(written, 1_500);
    sink.flush().await.expect("flush");

    let admin = assertion_client(&endpoint).await;
    let keys = list_keys(&admin, prefix).await;
    assert_eq!(
        keys.len(),
        1,
        "batch_size = 0 must collapse the whole run into a single object, got {:?}",
        keys
    );

    let recs = fetch_jsonl(&admin, &keys[0]).await;
    assert_eq!(recs.len(), 1_500, "single object holds the full run");
}

#[tokio::test(flavor = "multi_thread")]
async fn write_batch_partial_final_object() {
    let (_container, endpoint) = start_minio().await;
    create_bucket(&endpoint).await;

    let prefix = "partial/";
    let config = S3SinkConfig::new(TEST_BUCKET)
        .prefix(prefix)
        .with_batch_size(400);
    let sink = build_sink(&endpoint, config).await;

    let written = sink.write_batch(&records(1_000)).await.expect("write");
    assert_eq!(written, 1_000);
    sink.flush().await.expect("flush");

    let admin = assertion_client(&endpoint).await;
    let keys = list_keys(&admin, prefix).await;
    assert_eq!(
        keys.len(),
        3,
        "1000 records with batch_size 400 must produce 3 objects (400, 400, 200)"
    );

    let mut sizes: Vec<usize> = Vec::new();
    for key in &keys {
        sizes.push(fetch_jsonl(&admin, key).await.len());
    }
    sizes.sort_unstable();
    assert_eq!(
        sizes,
        vec![200, 400, 400],
        "the final object holds the 200-record remainder"
    );
}

/// #618 — the headline fix: several small pages must coalesce into one object,
/// not become one object each.
///
/// Before this, every `write_batch` call minted its own key, so a source with
/// a small `batch_size` produced a swarm of tiny objects — the small-files
/// problem that dominates read time on S3/Athena/Spark, where per-object
/// overhead outweighs the bytes.
#[tokio::test(flavor = "multi_thread")]
async fn small_pages_coalesce_into_one_object() {
    let (_container, endpoint) = start_minio().await;
    create_bucket(&endpoint).await;

    let prefix = "coalesce/";
    let config = S3SinkConfig::new(TEST_BUCKET)
        .prefix(prefix)
        .with_batch_size(0)
        .max_records_per_file(1_000);
    let sink = build_sink(&endpoint, config).await;

    // Ten pages of 100 — exactly the shape that used to produce ten objects.
    for _ in 0..10 {
        sink.write_batch(&records(100)).await.expect("write");
    }
    sink.flush().await.expect("flush");

    let admin = assertion_client(&endpoint).await;
    let keys = list_keys(&admin, prefix).await;
    assert_eq!(
        keys.len(),
        1,
        "ten 100-record pages under a 1000-record cap must be ONE object, got {:?}",
        keys
    );
    assert_eq!(
        fetch_jsonl(&admin, &keys[0]).await.len(),
        1_000,
        "and it must hold every record"
    );
}

/// The byte cap rolls independently of rows — the axis that actually bounds
/// object size for wide data (#618).
#[tokio::test(flavor = "multi_thread")]
async fn the_byte_cap_rolls_objects() {
    let (_container, endpoint) = start_minio().await;
    create_bucket(&endpoint).await;

    let prefix = "bytes/";
    // No record cap at all: only the byte cap may roll.
    let config = S3SinkConfig::new(TEST_BUCKET)
        .prefix(prefix)
        .with_batch_size(0)
        .max_bytes_per_file(1_000);
    let sink = build_sink(&endpoint, config).await;

    sink.write_batch(&records(2_000)).await.expect("write");
    sink.flush().await.expect("flush");

    let admin = assertion_client(&endpoint).await;
    let keys = list_keys(&admin, prefix).await;
    assert!(
        keys.len() > 1,
        "a 1000-byte cap over 2000 records must roll, got {:?}",
        keys
    );

    let mut total = 0usize;
    for key in &keys {
        total += fetch_jsonl(&admin, key).await.len();
    }
    assert_eq!(total, 2_000, "no record lost across byte rollovers");
}

/// An object larger than the 5 MiB multipart floor must stream through
/// multipart and still read back intact (#618).
///
/// This is the memory bound the issue is really about: without multipart the
/// whole object sits in RAM before a single-shot PUT, so output size is capped
/// by the process's memory.
#[tokio::test(flavor = "multi_thread")]
async fn a_large_object_streams_through_multipart_and_round_trips() {
    let (_container, endpoint) = start_minio().await;
    create_bucket(&endpoint).await;

    let prefix = "multipart/";
    let config = S3SinkConfig::new(TEST_BUCKET)
        .prefix(prefix)
        .with_batch_size(0);
    let sink = build_sink(&endpoint, config).await;

    // ~200 bytes per record × 60k ≈ 12 MiB → at least two 5 MiB parts plus a
    // tail, so the completion path is exercised rather than the single-shot
    // fallback.
    let wide: Vec<Value> = (0..60_000)
        .map(|i| json!({ "id": i, "pad": "x".repeat(180) }))
        .collect();
    for page in wide.chunks(5_000) {
        sink.write_batch(page).await.expect("write");
    }
    sink.flush().await.expect("flush");

    let admin = assertion_client(&endpoint).await;
    let keys = list_keys(&admin, prefix).await;
    assert_eq!(keys.len(), 1, "one object for the run, got {:?}", keys);
    let recs = fetch_jsonl(&admin, &keys[0]).await;
    assert_eq!(
        recs.len(),
        60_000,
        "every record must survive the multipart assembly"
    );
    assert_eq!(recs[0]["id"], 0);
    assert_eq!(recs[59_999]["id"], 59_999);
}
