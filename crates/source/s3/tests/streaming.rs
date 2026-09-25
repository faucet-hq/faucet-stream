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
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};
use testcontainers_modules::minio::MinIO;
use tokio::net::TcpListener;

/// MinIO's Docker Hub repository was withdrawn (September 2026): pulling
/// `minio/minio` fails with "repository does not exist / access denied". The
/// identical pinned release remains published on Quay, so only the module's
/// default image *name* is overridden — tag, cmd, and wait behavior stay
/// those of `testcontainers_modules::minio`.
/// Chainguard's maintained MinIO build: the upstream images stopped being
/// pullable (#694). Same server binary and CLI.
const MINIO_IMAGE_NAME: &str = "cgr.dev/chainguard/minio";
const MINIO_IMAGE_TAG: &str = "latest";

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
        .with_tag(MINIO_IMAGE_TAG)
        .with_mapped_port(0, testcontainers::core::IntoContainerPort::tcp(9000))
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

// ---------------------------------------------------------------------------
// #619 — `concurrency` on the streaming path.
//
// Before this, `buffer_unordered(concurrency)` appeared only in the eager
// `fetch_with_context` batch path; `stream_pages` — what every real run
// drives — read objects strictly one at a time, so the knob was documented
// but inert. These tests pin both halves of the fix: that the reads actually
// overlap, and that overlapping them changed nothing an operator can observe
// (order, contents, and which object an error is blamed on).
// ---------------------------------------------------------------------------

/// Collect every record a source streams, flattened in page order.
async fn collect_all(source: &S3Source, batch_size: usize) -> Vec<serde_json::Value> {
    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, batch_size);
    let mut out = Vec::new();
    while let Some(page) = pages.next().await {
        out.extend(page.expect("page ok").records);
    }
    out
}

/// `n` single-record objects, one record per object, ids ascending with the
/// (zero-padded, so lexicographic = numeric) listing order.
fn one_record_objects(n: i64) -> Vec<(String, String)> {
    (1..=n)
        .map(|i| (format!("part-{i:04}.jsonl"), format!("{{\"id\":{i}}}\n")))
        .collect()
}

/// A TCP proxy that records the high-water mark of *simultaneously open*
/// connections passing through it.
///
/// Overlap was originally asserted by wall clock — concurrent must beat
/// serial by 2x — and that was the wrong instrument: on a CI runner the
/// per-request latency this fix hides is small next to the fixed decode cost,
/// so a genuinely-overlapping read only reached ~1.6x and the test failed on
/// working code. How *fast* the overlap makes a run is a property of the
/// machine; *that* the reads overlap is a property of the source, and this
/// measures exactly that: HTTP/1.1 cannot multiplex, so N requests in flight
/// need N sockets.
struct CountingProxy {
    /// `http://127.0.0.1:<port>` to point the source at.
    endpoint: String,
    peak: Arc<AtomicUsize>,
}

impl CountingProxy {
    async fn start(upstream: &str) -> Self {
        let target = upstream.trim_start_matches("http://").to_string();
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind proxy");
        let port = listener.local_addr().expect("proxy addr").port();
        let peak = Arc::new(AtomicUsize::new(0));
        let live = Arc::new(AtomicUsize::new(0));

        let (peak_task, live_task) = (peak.clone(), live.clone());
        tokio::spawn(async move {
            loop {
                let Ok((mut inbound, _)) = listener.accept().await else {
                    return;
                };
                let now = live_task.fetch_add(1, Ordering::SeqCst) + 1;
                peak_task.fetch_max(now, Ordering::SeqCst);
                let (live_conn, target) = (live_task.clone(), target.clone());
                tokio::spawn(async move {
                    if let Ok(mut outbound) = tokio::net::TcpStream::connect(&target).await {
                        let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
                    }
                    live_conn.fetch_sub(1, Ordering::SeqCst);
                });
            }
        });

        Self {
            endpoint: format!("http://127.0.0.1:{port}"),
            peak,
        }
    }

    fn peak(&self) -> usize {
        self.peak.load(Ordering::SeqCst)
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_overlaps_object_reads() {
    // The regression this file exists to catch: before #619 the streaming path
    // read objects strictly one at a time, so `concurrency` was accepted and
    // ignored. Measured by how many connections are open at once rather than
    // by elapsed time — see `CountingProxy` for why.
    let (_container, endpoint) = start_minio().await;
    seed_bucket(&endpoint, &one_record_objects(60)).await;

    let serial_proxy = CountingProxy::start(&endpoint).await;
    let serial = build_source(
        &serial_proxy.endpoint,
        S3SourceConfig::new(TEST_BUCKET)
            .with_batch_size(1000)
            .concurrency(1),
    )
    .await;
    let serial_records = collect_all(&serial, 1000).await;

    let concurrent_proxy = CountingProxy::start(&endpoint).await;
    let concurrent = build_source(
        &concurrent_proxy.endpoint,
        S3SourceConfig::new(TEST_BUCKET)
            .with_batch_size(1000)
            .concurrency(15),
    )
    .await;
    let concurrent_records = collect_all(&concurrent, 1000).await;

    assert_eq!(serial_records.len(), 60);
    assert_eq!(
        serial_records, concurrent_records,
        "concurrency must not change what the source yields, only how fast"
    );

    // The proxy notices a closed connection only after its copy loop ends, so
    // a client that drops a pooled socket and immediately opens the next one
    // can briefly count as two. A serial reader therefore peaks at one or two;
    // an overlapping one at well above that.
    let (serial_peak, concurrent_peak) = (serial_proxy.peak(), concurrent_proxy.peak());
    assert!(
        serial_peak <= 2,
        "concurrency = 1 must not overlap reads (peak {serial_peak} connections)"
    );
    assert!(
        concurrent_peak >= 4 && concurrent_peak > serial_peak,
        "reading 60 objects with concurrency=15 peaked at {concurrent_peak} open \
         connections (serial: {serial_peak}) — the reads are not overlapping, \
         which is exactly the #619 defect: the knob is accepted and ignored"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_preserves_listing_order_under_concurrency() {
    // `buffered` (ordered) rather than `buffer_unordered` is a deliberate
    // choice: pages must arrive in listing order so a downstream sink writes
    // the same file sequence it would have serially.
    let (_container, endpoint) = start_minio().await;
    seed_bucket(&endpoint, &one_record_objects(20)).await;

    let source = build_source(
        &endpoint,
        S3SourceConfig::new(TEST_BUCKET)
            .with_batch_size(1000)
            .concurrency(8),
    )
    .await;

    let ids: Vec<i64> = collect_all(&source, 1000)
        .await
        .iter()
        .map(|r| r["id"].as_i64().expect("id"))
        .collect();
    assert_eq!(
        ids,
        (1..=20).collect::<Vec<i64>>(),
        "records must stay in listing order; an unordered prefetch would \
         interleave objects by completion time"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_concurrency_zero_reads_serially_rather_than_stalling() {
    // A `buffered(0)` stream yields nothing forever, so a config of `0` has to
    // be clamped. Asserting it here keeps the clamp from being refactored away
    // into a silent hang.
    let (_container, endpoint) = start_minio().await;
    seed_bucket(&endpoint, &one_record_objects(5)).await;

    let source = build_source(
        &endpoint,
        S3SourceConfig::new(TEST_BUCKET)
            .with_batch_size(1000)
            .concurrency(0),
    )
    .await;

    let records = tokio::time::timeout(
        std::time::Duration::from_secs(60),
        collect_all(&source, 1000),
    )
    .await
    .expect("concurrency = 0 must read serially, not hang");
    assert_eq!(records.len(), 5);
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_blames_the_failing_object_even_when_prefetched() {
    // With a look-ahead, several objects are in flight when one fails. The
    // error must still name the offending key — otherwise concurrency makes
    // diagnostics worse than the serial path it replaced.
    let (_container, endpoint) = start_minio().await;
    seed_bucket(
        &endpoint,
        &[
            ("a-ok.jsonl".to_string(), jsonl_body(1, 3)),
            ("b-bad.jsonl".to_string(), "not json at all\n".to_string()),
            ("c-ok.jsonl".to_string(), jsonl_body(4, 6)),
        ],
    )
    .await;

    let source = build_source(
        &endpoint,
        S3SourceConfig::new(TEST_BUCKET)
            .with_batch_size(1000)
            .concurrency(8),
    )
    .await;

    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 1000);
    let mut err = None;
    while let Some(page) = pages.next().await {
        if let Err(e) = page {
            err = Some(e);
            break;
        }
    }
    let err = err.expect("the malformed object must fail the stream");
    assert!(
        err.to_string().contains("b-bad.jsonl"),
        "the failure must name the object it came from: {err}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_json_array_objects_read_concurrently_and_stay_ordered() {
    // The whole-object format: unlike JSONL, `concurrency` bodies really are
    // resident at once here, which is the memory bound the knob advertises.
    let (_container, endpoint) = start_minio().await;
    let objects: Vec<(String, String)> = (0..12)
        .map(|i| {
            let base = i * 10;
            let body: Vec<String> = (1..=10)
                .map(|j| format!("{{\"id\":{}}}", base + j))
                .collect();
            (format!("arr-{i:04}.json"), format!("[{}]", body.join(",")))
        })
        .collect();
    seed_bucket(&endpoint, &objects).await;

    let source = build_source(
        &endpoint,
        S3SourceConfig::new(TEST_BUCKET)
            .file_format(S3FileFormat::JsonArray)
            .with_batch_size(DEFAULT_BATCH_SIZE)
            .concurrency(6),
    )
    .await;

    let ids: Vec<i64> = collect_all(&source, DEFAULT_BATCH_SIZE)
        .await
        .iter()
        .map(|r| r["id"].as_i64().expect("id"))
        .collect();
    assert_eq!(ids, (1..=120).collect::<Vec<i64>>());
}
