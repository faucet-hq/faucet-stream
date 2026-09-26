//! Integration tests against a real Google Cloud Pub/Sub **emulator** started
//! automatically in Docker via testcontainers — no external infra, no env
//! gating, not `#[ignore]`d. Mirrors the DB testcontainer template
//! (`crates/source/mssql/tests/integration.rs`).
//!
//! Requires Docker (the `google/cloud-sdk:*-emulators` image). Run with:
//! `cargo test -p faucet-source-pubsub --test integration`.
//!
//! Each test starts its own emulator container and holds the handle until it
//! returns, so the container is stopped and removed when the test ends —
//! testcontainers-rs has no reaper, so a forgotten handle is a leaked
//! container. `PUBSUB_EMULATOR_HOST` is process-global (the SDK's
//! `ClientConfig::default()` — used both by the setup client here and by the
//! connector's `build_client` — reads it and switches to the emulator
//! environment, no auth), so the tests are serialized by [`EMULATOR_LOCK`] and
//! the variable is rewritten under that lock for each test's own port.

use faucet_common_pubsub::PubsubMessage;
use faucet_core::{CheckContext, Source};
use faucet_source_pubsub::{
    PubsubConnection, PubsubCredentials, PubsubSource, PubsubSourceConfig, ValueFormat,
};
use gcloud_pubsub::client::{Client, ClientConfig};
use tokio::sync::{Mutex, MutexGuard};

use testcontainers_modules::google_cloud_sdk_emulators::{CloudSdk, PUBSUB_PORT};
use testcontainers_modules::testcontainers::{ContainerAsync, runners::AsyncRunner};

/// Every topic/subscription is created under this project id; the emulator
/// accepts any project name. The connector and the setup client must agree, or
/// they would address different `projects/<id>/…` namespaces.
const PROJECT: &str = "faucet-test";

/// Serializes the tests: `PUBSUB_EMULATOR_HOST` is one process-wide variable
/// and each test's emulator listens on its own mapped port.
static EMULATOR_LOCK: Mutex<()> = Mutex::const_new(());

/// One test's Pub/Sub emulator: the container (stopped + removed on drop),
/// its `host:port`, and the serialization guard released with it.
struct Emulator {
    _container: ContainerAsync<CloudSdk>,
    host: String,
    _guard: MutexGuard<'static, ()>,
}

/// Start an emulator for the calling test and point `PUBSUB_EMULATOR_HOST` at
/// it. Held for the test's lifetime; dropping it stops the container and lets
/// the next test start.
async fn emulator() -> Emulator {
    let guard = EMULATOR_LOCK.lock().await;
    let container = CloudSdk::pubsub()
        .start()
        .await
        .expect("start pubsub emulator container");
    let port = container
        .get_host_port_ipv4(PUBSUB_PORT)
        .await
        .expect("pubsub emulator host port");
    let host = format!("127.0.0.1:{port}");
    // SAFETY: every reader of this variable in the process is one of these
    // tests, and they hold `EMULATOR_LOCK` while running; nothing reads it
    // concurrently with this write.
    unsafe {
        std::env::set_var("PUBSUB_EMULATOR_HOST", &host);
    }
    Emulator {
        _container: container,
        host,
        _guard: guard,
    }
}

/// A setup/admin client pointed at the emulator, scoped to `PROJECT`.
async fn setup_client() -> Client {
    // `ClientConfig` is `#[non_exhaustive]`, so struct-update syntax is
    // unavailable — reassign the one field we need after `default()`.
    #[allow(clippy::field_reassign_with_default)]
    let config = {
        let mut config = ClientConfig::default(); // reads PUBSUB_EMULATOR_HOST
        config.project_id = Some(PROJECT.to_string());
        config
    };
    Client::new(config).await.expect("emulator setup client")
}

fn conn(host: &str) -> PubsubConnection {
    PubsubConnection {
        project_id: Some(PROJECT.into()),
        emulator_host: Some(host.to_string()),
        credentials: PubsubCredentials::Anonymous,
        ..Default::default()
    }
}

async fn create_topic_sub(client: &Client, topic_id: &str, sub_id: &str) {
    let topic = client
        .create_topic(topic_id, None, None)
        .await
        .expect("create topic");
    client
        .create_subscription(
            sub_id,
            topic.fully_qualified_name(),
            Default::default(),
            None,
        )
        .await
        .expect("create subscription");
}

fn msg(data: &[u8], attrs: &[(&str, &str)], ordering_key: &str) -> PubsubMessage {
    PubsubMessage {
        data: data.to_vec(),
        attributes: attrs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect(),
        ordering_key: ordering_key.to_string(),
        ..Default::default()
    }
}

async fn publish(client: &Client, topic_id: &str, messages: Vec<PubsubMessage>) {
    let topic = client.topic(topic_id);
    let publisher = topic.new_publisher(None);
    let mut awaiters = Vec::with_capacity(messages.len());
    for m in messages {
        awaiters.push(publisher.publish(m).await);
    }
    for a in awaiters {
        a.get().await.expect("publish message");
    }
}

/// JSON value_format: attributes surfaced under `__attributes`, `message_id` /
/// `publish_time_millis` populated by the server, an ordering key round-tripped,
/// multi-page draining (`batch_size = 2` over 3 messages), and the `check()`
/// subscription-exists probe. Exercises `stream.rs` streaming-pull + ack path
/// and `convert.rs` JSON decoding + attribute mapping.
#[tokio::test(flavor = "multi_thread")]
async fn source_json_multipage_attributes_and_check() {
    let emu = emulator().await;
    let host = emu.host.as_str();
    let client = setup_client().await;
    create_topic_sub(&client, "src-json-t", "src-json-s").await;
    publish(
        &client,
        "src-json-t",
        vec![
            msg(br#"{"n":1}"#, &[("origin", "eu")], "ok-1"),
            msg(br#"{"n":2}"#, &[], ""),
            msg(br#"{"n":3}"#, &[], ""),
        ],
    )
    .await;

    let mut cfg = PubsubSourceConfig::new("src-json-s");
    cfg.connection = conn(host);
    cfg.value_format = ValueFormat::Json;
    cfg.idle_termination_secs = Some(5);
    cfg.max_messages = Some(3);
    cfg.batch_size = 2; // 3 messages → pages of 2 + 1: exercises ack-at-page-boundary
    let source = PubsubSource::new(cfg).await.expect("source builds");

    // Side-effect-free preflight probe (subscription exists).
    let report = source
        .check(&CheckContext::default())
        .await
        .expect("check runs");
    assert_eq!(report.failed_count(), 0, "subscription-exists probe passes");

    let mut records = source.fetch_all().await.expect("drain");
    assert_eq!(records.len(), 3, "all published messages delivered");
    records.sort_by_key(|r| r["data"]["n"].as_i64().unwrap());
    assert_eq!(records[0]["data"]["n"], 1);
    assert_eq!(records[2]["data"]["n"], 3);
    assert!(
        records.iter().all(|r| r["message_id"].is_string()),
        "server-assigned message_id present"
    );
    assert!(
        records.iter().all(|r| r["publish_time_millis"].is_i64()),
        "server-assigned publish_time_millis present: {records:?}"
    );
    // Attribute map surfaced under the default `__attributes` key.
    assert!(
        records.iter().any(|r| r["__attributes"]["origin"] == "eu"),
        "attribute mapping: {records:?}"
    );
    // Ordering key round-tripped onto the record.
    assert!(
        records.iter().any(|r| r["ordering_key"] == "ok-1"),
        "ordering key surfaced: {records:?}"
    );
}

/// `value_format: string` decodes the raw payload as UTF-8.
#[tokio::test(flavor = "multi_thread")]
async fn source_value_format_string() {
    let emu = emulator().await;
    let host = emu.host.as_str();
    let client = setup_client().await;
    create_topic_sub(&client, "src-str-t", "src-str-s").await;
    publish(
        &client,
        "src-str-t",
        vec![
            msg(b"hello world", &[], ""),
            msg("h\u{e9}llo".as_bytes(), &[], ""),
        ],
    )
    .await;

    let mut cfg = PubsubSourceConfig::new("src-str-s");
    cfg.connection = conn(host);
    cfg.value_format = ValueFormat::String;
    cfg.idle_termination_secs = Some(5);
    cfg.max_messages = Some(2);
    let source = PubsubSource::new(cfg).await.expect("source builds");

    let records = source.fetch_all().await.expect("drain");
    let mut datas: Vec<String> = records
        .iter()
        .map(|r| r["data"].as_str().unwrap().to_string())
        .collect();
    datas.sort();
    assert_eq!(
        datas,
        vec!["hello world".to_string(), "h\u{e9}llo".to_string()]
    );
}

/// `value_format: bytes` base64-encodes the raw payload bytes.
#[tokio::test(flavor = "multi_thread")]
async fn source_value_format_bytes() {
    let emu = emulator().await;
    let host = emu.host.as_str();
    let client = setup_client().await;
    create_topic_sub(&client, "src-bytes-t", "src-bytes-s").await;
    publish(&client, "src-bytes-t", vec![msg(&[1, 2, 3], &[], "")]).await;

    let mut cfg = PubsubSourceConfig::new("src-bytes-s");
    cfg.connection = conn(host);
    cfg.value_format = ValueFormat::Bytes;
    cfg.idle_termination_secs = Some(5);
    cfg.max_messages = Some(1);
    let source = PubsubSource::new(cfg).await.expect("source builds");

    let records = source.fetch_all().await.expect("drain");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["data"], "AQID", "0x010203 → base64");
}
