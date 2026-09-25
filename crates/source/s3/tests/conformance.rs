//! `faucet-conformance` Tier-1 battery for the S3 source.
//!
//! Check 1 (config-schema validity) is pure and offline. Check 2
//! (bounded-memory streaming) boots MinIO via testcontainers and so requires
//! Docker — it runs in CI alongside the other integration tests. It matches
//! the existing streaming integration test's MinIO + bucket setup verbatim.

use aws_config::BehaviorVersion;
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::{Client, Config as S3Config};
use faucet_conformance::{assert_config_schema_valid_value, assert_errors_not_panics};
use faucet_core::Source;
use faucet_source_s3::{S3Source, S3SourceConfig};
use testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};
use testcontainers_modules::minio::MinIO;

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
const TEST_BUCKET: &str = "faucet-stream-conformance";

// ── Check 1: config schema ──────────────────────────────────────────────────

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(S3SourceConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "faucet-source-s3");
}

// ── Check 10: connector_name is non-empty (offline, lazy build) ──────────────
#[tokio::test(flavor = "multi_thread")]
async fn conformance_connector_name_nonempty() {
    let config = S3SourceConfig::new("does-not-exist")
        .endpoint_url("http://127.0.0.1:1".to_string())
        .region(REGION.to_string());
    let source = S3Source::new(config).await.expect("source builds lazily");
    faucet_conformance::assert_connector_name_nonempty(&source);
    assert_eq!(source.connector_name(), "s3");
}

// ── Check 2: bounded-memory streaming (Docker) ──────────────────────────────

/// Start a MinIO container and return the container handle plus the
/// `http://host:port` endpoint URL.
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
    // SAFETY: the credentials are identical for every MinIO run, so the value
    // is the same across overlapping tests; re-applying the same constants on
    // entry keeps the thread-unsafe set benign.
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

/// Build a JSONL body with records `{"id": i}` for `i = start..=end_inclusive`.
fn jsonl_body(start: i64, end_inclusive: i64) -> String {
    let mut out = String::new();
    for i in start..=end_inclusive {
        out.push_str(&format!("{{\"id\":{i}}}\n"));
    }
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_bounded_memory() {
    let (_container, endpoint) = start_minio().await;
    seed_bucket(
        &endpoint,
        &[("data.jsonl".to_string(), jsonl_body(1, 5_000))],
    )
    .await;

    let config = S3SourceConfig::new(TEST_BUCKET).with_batch_size(250);
    let source = build_source(&endpoint, config).await;

    faucet_conformance::assert_bounded_memory(&source, 250, 5_000).await;

    // Check 9: reuse the same MinIO instance + seeded bucket — a source built
    // with `batch_size = 0` must yield the whole object as a single page.
    let zero_config = S3SourceConfig::new(TEST_BUCKET).with_batch_size(0);
    let zero_source = build_source(&endpoint, zero_config).await;
    faucet_conformance::assert_batch_size_zero_single_page(&zero_source).await;
    // _container stays alive to here
}

// ── Check 12: discovery round-trips (Docker) ────────────────────────────────

/// Every dataset `discover()` reports must be genuinely selectable: deep-merge
/// its config_patch (a `{"prefix": …}` override) onto the base config, rebuild
/// the source, and read it. Objects are seeded under two "directory" prefixes
/// so the delimiter listing yields prefix datasets.
#[tokio::test(flavor = "multi_thread")]
async fn conformance_discover_roundtrips() {
    let (_container, endpoint) = start_minio().await;
    seed_bucket(
        &endpoint,
        &[
            ("orders/data.jsonl".to_string(), jsonl_body(1, 3)),
            ("customers/data.jsonl".to_string(), jsonl_body(4, 6)),
        ],
    )
    .await;

    let source = build_source(&endpoint, S3SourceConfig::new(TEST_BUCKET)).await;
    faucet_conformance::assert_discover_roundtrips(&source, |patch| {
        let endpoint = endpoint.clone();
        async move {
            let base = serde_json::to_value(S3SourceConfig::new(TEST_BUCKET))
                .expect("serialize base config");
            let merged = faucet_conformance::merge_config_patch(base, &patch);
            let cfg: S3SourceConfig =
                serde_json::from_value(merged).expect("deserialize merged config");
            Box::new(build_source(&endpoint, cfg).await) as Box<dyn faucet_core::Source>
        }
    })
    .await;
    // _container stays alive to here
}

// ── Check 6: errors, not panics (no container) ──────────────────────────────

/// Point the source at an unreachable S3 endpoint (`http://127.0.0.1:1`, which
/// refuses connections immediately) with bogus credentials. `new()` stays lazy
/// — no MinIO container needed — and the first `ListObjectsV2` / `GetObject`
/// call fails with a typed `FaucetError` on both the `fetch_all` and
/// `stream_pages` paths, never a panic. Credentials are supplied via env vars
/// (the source honours the standard AWS credential chain), matching the
/// bounded-memory check's approach.
#[tokio::test(flavor = "multi_thread")]
async fn conformance_errors_not_panics() {
    // SAFETY: identical constant credentials across all runs in this test
    // binary; re-applying the same values keeps the thread-unsafe set benign.
    unsafe {
        std::env::set_var("AWS_ACCESS_KEY_ID", "bogus");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "bogus");
        std::env::set_var("AWS_DEFAULT_REGION", REGION);
    }
    let config = S3SourceConfig::new("does-not-exist")
        .endpoint_url("http://127.0.0.1:1".to_string())
        .region(REGION.to_string());
    let source = S3Source::new(config).await.expect("source builds lazily");
    assert_errors_not_panics(&source).await;
}
