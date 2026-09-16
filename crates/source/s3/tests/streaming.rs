//! Integration tests for `S3Source::stream_pages` against a real
//! S3-compatible endpoint (MinIO) via testcontainers.
//!
//! These tests require Docker. Each test boots its own container and seeds
//! its own bucket so they are fully isolated and safe to run in parallel.

use aws_config::BehaviorVersion;
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::{Client, Config as S3Config};
use faucet_core::{DEFAULT_BATCH_SIZE, Source};
use faucet_source_s3::{S3FileFormat, S3Source, S3SourceConfig};
use futures::StreamExt;
use std::collections::HashMap;
use std::time::Instant;
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
const TEST_BUCKET: &str = "faucet-stream-tests";

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

/// Build a path-style aws-sdk-s3 client pointed at the MinIO endpoint.
/// MinIO does not implement virtual-host-style addressing, so all callers
/// must force path style.
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

/// Create the test bucket and upload every `(key, body)` pair to it.
async fn seed_bucket(endpoint: &str, objects: &[(String, String)]) {
    // Point the SDK at the MinIO endpoint with admin credentials.
    let client = build_admin_client(endpoint).await;
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
            .body(ByteStream::from(body.clone().into_bytes()))
            .send()
            .await
            .expect("put object");
    }
}

/// Build an `S3Source` configured against MinIO. Credentials are passed via
/// env vars because the source's `build_client` honours the standard AWS
/// credential chain and has no field for inline credentials.
async fn build_source(endpoint: &str, config: S3SourceConfig) -> S3Source {
    // SAFETY: tests are serialised on these env vars because each test
    // creates its own container/bucket and the credentials are identical
    // for every MinIO run, so the value is the same across overlapping
    // tests. The thread-unsafe set is fine here because each test
    // re-applies the same constants on entry.
    unsafe {
        std::env::set_var("AWS_ACCESS_KEY_ID", ACCESS_KEY);
        std::env::set_var("AWS_SECRET_ACCESS_KEY", SECRET_KEY);
        std::env::set_var("AWS_DEFAULT_REGION", REGION);
    }
    let config = config
        .endpoint_url(endpoint.to_string())
        .region(REGION.to_string());
    S3Source::new(config).await.expect("S3Source::new")
}

/// Build a JSONL body with `n` records of `{"id": i}` for `i = 1..=n`.
fn jsonl_body(start: i64, end_inclusive: i64) -> String {
    let mut out = String::new();
    for i in start..=end_inclusive {
        out.push_str(&format!("{{\"id\":{i}}}\n"));
    }
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_chunks_jsonl_lines_into_batch_sized_pages() {
    let (_container, endpoint) = start_minio().await;
    seed_bucket(
        &endpoint,
        &[("data.jsonl".to_string(), jsonl_body(1, 10_000))],
    )
    .await;

    let config = S3SourceConfig::new(TEST_BUCKET).with_batch_size(1000);
    let source = build_source(&endpoint, config).await;

    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 1000);

    let mut page_count = 0;
    let mut total = 0;
    while let Some(page) = pages.next().await {
        let page = page.expect("page ok");
        page_count += 1;
        total += page.records.len();
        assert_eq!(
            page.records.len(),
            1000,
            "every page must be batch_size when total is a multiple"
        );
        assert!(
            page.bookmark.is_none(),
            "S3 source has no incremental mode; bookmark must be None"
        );
    }

    assert_eq!(page_count, 10, "10_000 / 1000 = 10 pages");
    assert_eq!(total, 10_000);
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_flattens_across_multiple_jsonl_objects() {
    let (_container, endpoint) = start_minio().await;
    seed_bucket(
        &endpoint,
        &[
            ("a.jsonl".to_string(), jsonl_body(1, 700)),
            ("b.jsonl".to_string(), jsonl_body(701, 1400)),
            ("c.jsonl".to_string(), jsonl_body(1401, 2100)),
        ],
    )
    .await;

    let config = S3SourceConfig::new(TEST_BUCKET).with_batch_size(500);
    let source = build_source(&endpoint, config).await;

    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 500);

    let mut sizes = Vec::new();
    let mut ids: Vec<i64> = Vec::new();
    while let Some(page) = pages.next().await {
        let page = page.expect("page ok");
        sizes.push(page.records.len());
        for r in &page.records {
            ids.push(r["id"].as_i64().expect("id"));
        }
    }

    // 2100 records at batch_size 500 → 4 full pages + 1 page of 100.
    // Pages are emitted as lines flow across object boundaries, so we
    // assert on the *shape* (size sequence and total).
    assert_eq!(sizes, vec![500, 500, 500, 500, 100], "flattened page sizes");
    assert_eq!(ids.len(), 2100);

    ids.sort_unstable();
    let expected: Vec<i64> = (1..=2100).collect();
    assert_eq!(ids, expected, "all ids preserved across object flattening");
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_partial_final_page() {
    let (_container, endpoint) = start_minio().await;
    seed_bucket(
        &endpoint,
        &[("data.jsonl".to_string(), jsonl_body(1, 2_500))],
    )
    .await;

    let config = S3SourceConfig::new(TEST_BUCKET).with_batch_size(1000);
    let source = build_source(&endpoint, config).await;

    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 1000);

    let mut sizes = Vec::new();
    while let Some(page) = pages.next().await {
        let page = page.expect("page ok");
        sizes.push(page.records.len());
    }
    assert_eq!(sizes, vec![1000, 1000, 500], "partial trailing page");
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_batch_size_zero_emits_one_page_per_object() {
    let (_container, endpoint) = start_minio().await;
    seed_bucket(
        &endpoint,
        &[
            ("a.jsonl".to_string(), jsonl_body(1, 100)),
            ("b.jsonl".to_string(), jsonl_body(101, 350)),
            ("c.jsonl".to_string(), jsonl_body(351, 351)),
        ],
    )
    .await;

    let config = S3SourceConfig::new(TEST_BUCKET).with_batch_size(0);
    let source = build_source(&endpoint, config).await;

    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 0);

    let mut sizes = Vec::new();
    while let Some(page) = pages.next().await {
        let page = page.expect("page ok");
        sizes.push(page.records.len());
    }
    sizes.sort_unstable();

    // batch_size = 0 → one page per object. Object key listing order is
    // alphabetical for MinIO/S3, but we sort so the assertion does not
    // depend on listing order.
    let mut expected = vec![100, 250, 1];
    expected.sort_unstable();
    assert_eq!(
        sizes, expected,
        "one page per object, no within-object chunking"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_preserves_row_contents() {
    let (_container, endpoint) = start_minio().await;
    let body = "{\"id\":1,\"name\":\"alpha\"}\n\
                {\"id\":2,\"name\":\"beta\"}\n\
                {\"id\":3,\"name\":\"gamma\"}\n";
    seed_bucket(&endpoint, &[("items.jsonl".to_string(), body.to_string())]).await;

    let config = S3SourceConfig::new(TEST_BUCKET).with_batch_size(2);
    let source = build_source(&endpoint, config).await;

    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 2);

    let mut all = Vec::new();
    while let Some(page) = pages.next().await {
        let page = page.expect("page ok");
        all.extend(page.records);
    }

    assert_eq!(all.len(), 3);
    assert_eq!(all[0]["id"], 1);
    assert_eq!(all[0]["name"], "alpha");
    assert_eq!(all[2]["name"], "gamma");
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_empty_result_yields_no_pages() {
    let (_container, endpoint) = start_minio().await;
    // Create an empty bucket.
    seed_bucket(&endpoint, &[]).await;

    let config = S3SourceConfig::new(TEST_BUCKET).with_batch_size(DEFAULT_BATCH_SIZE);
    let source = build_source(&endpoint, config).await;

    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, DEFAULT_BATCH_SIZE);

    let mut page_count = 0;
    while let Some(page) = pages.next().await {
        let _ = page.expect("page ok");
        page_count += 1;
    }
    assert_eq!(page_count, 0, "empty bucket must yield zero pages");
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_json_array_chunks_records() {
    let (_container, endpoint) = start_minio().await;
    let mut elements = Vec::new();
    for i in 1..=2500 {
        elements.push(serde_json::json!({"id": i}));
    }
    let body = serde_json::to_string(&serde_json::Value::Array(elements)).unwrap();
    seed_bucket(&endpoint, &[("data.json".to_string(), body)]).await;

    let config = S3SourceConfig::new(TEST_BUCKET)
        .file_format(S3FileFormat::JsonArray)
        .with_batch_size(1000);
    let source = build_source(&endpoint, config).await;

    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 1000);

    let mut sizes = Vec::new();
    while let Some(page) = pages.next().await {
        let page = page.expect("page ok");
        sizes.push(page.records.len());
    }
    assert_eq!(
        sizes,
        vec![1000, 1000, 500],
        "JSON-array source chunked into batch_size pages with a partial tail"
    );
}

/// Catches the "buffered-then-chunked" anti-pattern on the JSONL path.
///
/// The default trait `stream_pages` impl materialises the full result via
/// `fetch_with_context_incremental` before any page is yielded; the true
/// streaming impl emits the first page as soon as `batch_size` lines have
/// been parsed off the wire.
///
/// For a large JSONL object on MinIO (running on the same host), the
/// difference is observable: stopping after the first page should arrive
/// in well under the full-drain time.
#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_first_page_completes_without_parsing_full_object() {
    let (_container, endpoint) = start_minio().await;
    seed_bucket(
        &endpoint,
        &[("big.jsonl".to_string(), jsonl_body(1, 200_000))],
    )
    .await;

    // Reference: full drain time.
    let config_full = S3SourceConfig::new(TEST_BUCKET).with_batch_size(1000);
    let source = build_source(&endpoint, config_full).await;
    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let start = Instant::now();
    let mut full_pages = source.stream_pages(&ctx, 1000);
    while let Some(page) = full_pages.next().await {
        let _ = page.expect("page ok");
    }
    let full_elapsed = start.elapsed();
    drop(full_pages);
    drop(source);

    // First-page time: a true streaming impl yields after parsing 1000
    // lines, not all 200k.
    let config_first = S3SourceConfig::new(TEST_BUCKET).with_batch_size(1000);
    let source = build_source(&endpoint, config_first).await;
    let start = Instant::now();
    let mut first_pages = source.stream_pages(&ctx, 1000);
    let first_page = first_pages
        .next()
        .await
        .expect("first page exists")
        .expect("page ok");
    let first_elapsed = start.elapsed();
    drop(first_pages);
    assert_eq!(first_page.records.len(), 1000);

    assert!(
        first_elapsed * 2 < full_elapsed,
        "first page should arrive without parsing the full object; \
         first page took {first_elapsed:?}, full drain took {full_elapsed:?}"
    );
}

// ── Dataset discovery (#211) ────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn discover_enumerates_common_prefixes() {
    let (_container, endpoint) = start_minio().await;
    seed_bucket(
        &endpoint,
        &[
            ("raw/orders/part-1.jsonl".to_string(), jsonl_body(1, 3)),
            ("raw/orders/part-2.jsonl".to_string(), jsonl_body(4, 6)),
            ("raw/users/part-1.jsonl".to_string(), jsonl_body(1, 3)),
        ],
    )
    .await;

    let config = S3SourceConfig::new(TEST_BUCKET).prefix("raw/");
    let source = build_source(&endpoint, config).await;

    assert!(source.supports_discover());
    let datasets = source.discover().await.expect("discover");
    let names: Vec<&str> = datasets.iter().map(|d| d.name.as_str()).collect();
    assert_eq!(names, vec!["raw/orders/", "raw/users/"]);
    for d in &datasets {
        assert_eq!(d.kind, "prefix");
        assert_eq!(d.config_patch["prefix"], d.name.as_str());
        assert!(d.schema.is_none());
        assert!(d.estimated_rows.is_none());
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn discover_falls_back_to_objects_under_leaf_prefix() {
    let (_container, endpoint) = start_minio().await;
    seed_bucket(
        &endpoint,
        &[
            ("raw/orders/part-1.jsonl".to_string(), jsonl_body(1, 3)),
            ("raw/orders/part-2.jsonl".to_string(), jsonl_body(4, 6)),
        ],
    )
    .await;

    let config = S3SourceConfig::new(TEST_BUCKET).prefix("raw/orders/");
    let source = build_source(&endpoint, config).await;

    let datasets = source.discover().await.expect("discover");
    let names: Vec<&str> = datasets.iter().map(|d| d.name.as_str()).collect();
    assert_eq!(
        names,
        vec!["raw/orders/part-1.jsonl", "raw/orders/part-2.jsonl"]
    );
    for d in &datasets {
        assert_eq!(d.kind, "object");
        assert_eq!(d.config_patch["prefix"], d.name.as_str());
    }
}
