//! Integration tests against a real Google Cloud Pub/Sub **emulator** started
//! automatically in Docker via testcontainers — no external infra, no env
//! gating, not `#[ignore]`d. Mirrors the DB testcontainer template
//! (`crates/source/mssql/tests/integration.rs`).
//!
//! Requires Docker (the `google/cloud-sdk:*-emulators` image). Run with:
//! `cargo test -p faucet-sink-pubsub --test integration`.
//!
//! Each test starts its own emulator container and holds the handle until it
//! returns, so the container is stopped and removed when the test ends —
//! testcontainers-rs has no reaper, so a forgotten handle is a leaked
//! container. `PUBSUB_EMULATOR_HOST` is process-global (the SDK's
//! `ClientConfig::default()` — used both by the setup client here and by the
//! connector's `build_client` — reads it and switches to the emulator
//! environment, no auth), so the tests are serialized by [`EMULATOR_LOCK`] and
//! the variable is rewritten under that lock for each test's own port.

use faucet_core::{CheckContext, Sink};
use faucet_sink_pubsub::{
    OrderingKey, PubsubConnection, PubsubCredentials, PubsubSink, PubsubSinkConfig, ValueFormat,
};
use gcloud_pubsub::client::{Client, ClientConfig};
use gcloud_pubsub::subscriber::ReceivedMessage;
use gcloud_pubsub::subscription::Subscription;
use serde_json::{Value, json};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, MutexGuard};

use testcontainers_modules::google_cloud_sdk_emulators::{CloudSdk, PUBSUB_PORT};
use testcontainers_modules::testcontainers::{ContainerAsync, runners::AsyncRunner};

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

/// Create the topic + a subscription and return a handle to the subscription so
/// the test can read the published messages back.
async fn create_topic_sub(client: &Client, topic_id: &str, sub_id: &str) -> Subscription {
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
    client.subscription(sub_id)
}

/// Pull (and ack) until `want` messages are collected or a deadline passes.
/// Pub/Sub `pull` may return the messages across several calls.
async fn drain(sub: &Subscription, want: usize) -> Vec<ReceivedMessage> {
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut out = Vec::new();
    while out.len() < want && Instant::now() < deadline {
        let msgs = sub.pull(want as i32, None).await.expect("pull");
        if msgs.is_empty() {
            tokio::time::sleep(Duration::from_millis(200)).await;
            continue;
        }
        for m in &msgs {
            let _ = m.ack().await;
        }
        out.extend(msgs);
    }
    out
}

fn payload_json(m: &ReceivedMessage) -> Value {
    serde_json::from_slice(&m.message.data).expect("payload is JSON")
}

/// JSON value_format with an attributes field: records publish, are pullable,
/// the attribute map arrives on the message, and the attributes field is
/// stripped from the payload. Also exercises the `check()` topic-exists probe.
/// Covers `sink.rs` `write_batch` → `publish_all` → `publish_chunk`.
#[tokio::test(flavor = "multi_thread")]
async fn sink_json_publishes_with_attributes_and_check() {
    let emu = emulator().await;
    let host = emu.host.as_str();
    let client = setup_client().await;
    let sub = create_topic_sub(&client, "sink-json-t", "sink-json-s").await;

    let mut cfg = PubsubSinkConfig::new("sink-json-t");
    cfg.connection = conn(host);
    cfg.value_format = ValueFormat::Json;
    cfg.attributes_field = Some("__attributes".into());
    let sink = PubsubSink::new(cfg).await.expect("sink builds");

    // Side-effect-free preflight probe (topic exists).
    let report = sink
        .check(&CheckContext::default())
        .await
        .expect("check runs");
    assert_eq!(report.failed_count(), 0, "topic-exists probe passes");

    let written = sink
        .write_batch(&[
            json!({"id": "a", "n": 1, "__attributes": {"src": "test"}}),
            json!({"id": "b", "n": 2}),
        ])
        .await
        .expect("write");
    assert_eq!(written, 2);

    let got = drain(&sub, 2).await;
    assert!(got.len() >= 2, "published messages are pullable");

    let mut ns: Vec<i64> = got
        .iter()
        .map(|m| payload_json(m)["n"].as_i64().unwrap())
        .collect();
    ns.sort();
    assert_eq!(ns, vec![1, 2]);
    // The record with `__attributes` carries `src=test` as a message attribute.
    assert!(
        got.iter()
            .any(|m| m.message.attributes.get("src").map(String::as_str) == Some("test")),
        "attribute mapping onto the message"
    );
    // The attributes field is stripped from every payload.
    assert!(
        got.iter()
            .all(|m| payload_json(m).get("__attributes").is_none()),
        "attributes field stripped from payload"
    );
}

/// An `ordering_key: field` strategy stamps each message's ordering key and
/// drives the publisher's ordered send path. The ordering key round-trips onto
/// the received messages. Covers the ordered branch of `publish_chunk`.
#[tokio::test(flavor = "multi_thread")]
async fn sink_ordering_key_field_sets_message_key() {
    let emu = emulator().await;
    let host = emu.host.as_str();
    let client = setup_client().await;
    let sub = create_topic_sub(&client, "sink-ord-t", "sink-ord-s").await;

    let mut cfg = PubsubSinkConfig::new("sink-ord-t");
    cfg.connection = conn(host);
    cfg.value_format = ValueFormat::Json;
    cfg.ordering_key = OrderingKey::Field { name: "id".into() };
    let sink = PubsubSink::new(cfg).await.expect("sink builds");

    let written = sink
        .write_batch(&[json!({"id": "o-1", "n": 1}), json!({"id": "o-2", "n": 2})])
        .await
        .expect("write");
    assert_eq!(written, 2);

    let got = drain(&sub, 2).await;
    assert!(got.len() >= 2, "ordered messages are pullable");
    let keys: std::collections::BTreeSet<&str> = got
        .iter()
        .map(|m| m.message.ordering_key.as_str())
        .collect();
    assert!(keys.contains("o-1"), "ordering key o-1 present: {keys:?}");
    assert!(keys.contains("o-2"), "ordering key o-2 present: {keys:?}");
}

/// `write_batch_partial` returns per-row outcomes in input order: an
/// unresolvable ordering key fails just that row (DLQ-routable) while the valid
/// rows publish. Covers `encode_records` failure partitioning +
/// `assemble_row_outcomes` + `write_batch_partial`.
#[tokio::test(flavor = "multi_thread")]
async fn sink_write_batch_partial_reports_row_failures() {
    let emu = emulator().await;
    let host = emu.host.as_str();
    let client = setup_client().await;
    let sub = create_topic_sub(&client, "sink-partial-t", "sink-partial-s").await;

    let mut cfg = PubsubSinkConfig::new("sink-partial-t");
    cfg.connection = conn(host);
    cfg.ordering_key = OrderingKey::Field { name: "id".into() };
    let sink = PubsubSink::new(cfg).await.expect("sink builds");

    let records = [
        json!({"id": "ok1", "n": 1}),
        json!({"n": 2}), // missing the `id` ordering-key field → per-row failure
        json!({"id": "ok2", "n": 3}),
    ];
    let outcomes = sink
        .write_batch_partial(&records)
        .await
        .expect("partial write");
    assert_eq!(outcomes.len(), 3);
    assert!(outcomes[0].is_ok(), "row 0 published");
    assert!(outcomes[1].is_err(), "row 1 has no ordering key");
    assert!(
        outcomes[1].as_ref().unwrap_err().to_string().contains("id"),
        "error names the missing field: {:?}",
        outcomes[1]
    );
    assert!(outcomes[2].is_ok(), "row 2 published");

    let got = drain(&sub, 2).await;
    assert!(got.len() >= 2, "the two valid rows are pullable");
}

/// When every row fails to encode, `write_batch` returns an aggregate error
/// (rather than a partial success). Covers the `failed > 0` branch of
/// `write_batch` and its first-error extraction.
#[tokio::test(flavor = "multi_thread")]
async fn sink_write_batch_surfaces_encode_failure() {
    let emu = emulator().await;
    let host = emu.host.as_str();
    let client = setup_client().await;
    let _sub = create_topic_sub(&client, "sink-fail-t", "sink-fail-s").await;

    let mut cfg = PubsubSinkConfig::new("sink-fail-t");
    cfg.connection = conn(host);
    cfg.ordering_key = OrderingKey::Field { name: "id".into() };
    let sink = PubsubSink::new(cfg).await.expect("sink builds");

    // The single record has no `id` field → encode fails before any publish.
    let err = match sink.write_batch(&[json!({"n": 1})]).await {
        Ok(_) => panic!("expected an aggregate publish failure"),
        Err(e) => e,
    };
    assert!(
        err.to_string().contains("failed to publish"),
        "aggregate error: {err}"
    );
}
