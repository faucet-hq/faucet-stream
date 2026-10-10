//! `faucet-conformance` Tier-1 battery for the NATS source.
//!
//! - **Check 1** (`conformance_config_schema_valid`) — pure/offline, MUST pass.
//! - **Check 6** (`conformance_errors_not_panics`) — points at an unreachable
//!   server (`nats://127.0.0.1:1`); connect fails on the first poll, so this
//!   runs offline and MUST pass.
//! - **Check 2** (`conformance_bounded_memory`) — boots a real NATS server via
//!   `testcontainers-modules` (Docker); publishes N messages, then drains with
//!   `max_messages = N` so the bounded-memory check can assert `seen == total`.
//!
//! Bookmark/idempotency checks (3/4/5) do not apply: core NATS is
//! fire-and-forget (no bookmark) and 4/5 are sink-only.

use faucet_conformance::{assert_config_schema_valid_value, assert_errors_not_panics};
use faucet_core::Source as _;
use faucet_source_nats::{NatsSource, NatsSourceConfig};

// ── Check 1: config schema (offline) ────────────────────────────────────────

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(NatsSourceConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "faucet-source-nats");
}

// ── Check 6: errors, not panics (offline — unreachable server) ───────────────

#[tokio::test]
async fn conformance_errors_not_panics() {
    let mut cfg = NatsSourceConfig::new("events.>");
    cfg.connection.servers = vec!["nats://127.0.0.1:1".into()];
    // A short terminator keeps the run bounded even on the (unreachable) happy
    // path; the connect failure surfaces before it ever matters.
    cfg.idle_timeout_secs = Some(1);
    let source = NatsSource::new(cfg)
        .await
        .expect("lazy construction succeeds");
    // Check 10: connector_name is non-empty (metric-cardinality contract).
    faucet_conformance::assert_connector_name_nonempty(&source);
    assert!(!source.consumes_destructively(), "core NATS acks nothing");
    assert_errors_not_panics(&source).await;

    let mut js = NatsSourceConfig::new("events.>");
    js.connection.servers = vec!["nats://127.0.0.1:1".into()];
    js.idle_timeout_secs = Some(1);
    js.jetstream_stream = Some("EVENTS".into());
    js.jetstream_consumer = Some("faucet".into());
    let js = NatsSource::new(js)
        .await
        .expect("lazy construction succeeds");
    assert!(js.consumes_destructively(), "JetStream acks what it reads");
}

// ── Check 2: bounded-memory streaming (Docker) ───────────────────────────────

#[cfg(test)]
mod docker {
    use super::*;
    use faucet_source_nats::Source;
    use futures::StreamExt;
    use testcontainers_modules::nats::Nats;

    async fn start_nats() -> (testcontainers::ContainerAsync<Nats>, String) {
        let container = faucet_conformance::containers::start(Nats::default).await;
        let host = container.get_host().await.expect("nats host");
        let port = container.get_host_port_ipv4(4222).await.expect("nats port");
        let url = format!("nats://{host}:{port}");
        // The mapped port can refuse connections for a moment after start.
        for _ in 0..50 {
            if async_nats::connect(&url).await.is_ok() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
        (container, url)
    }

    async fn publish_json(server: &str, subject: &str, count: usize) {
        let client = async_nats::connect(server).await.expect("connect");
        for i in 1..=count {
            let payload = format!(r#"{{"id":{i}}}"#);
            client
                .publish(subject.to_string(), payload.into_bytes().into())
                .await
                .expect("publish");
        }
        client.flush().await.expect("flush");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn conformance_bounded_memory() {
        let (_container, server) = start_nats().await;
        let subject = "conformance.bounded";
        const N: usize = 5_000;

        // Subscribe first (core NATS drops messages published before a
        // subscription exists), then publish, then drain.
        let mut cfg = NatsSourceConfig::new(subject);
        cfg.connection.servers = vec![server.clone()];
        cfg.max_messages = Some(N);
        cfg.idle_timeout_secs = Some(30);
        cfg.batch_size = 250;
        let source = NatsSource::new(cfg).await.expect("source new");

        // Drive the drain concurrently with the publisher: start streaming so the
        // subscription is live, then publish the N messages.
        let ctx = std::collections::HashMap::new();
        let publisher = {
            let server = server.clone();
            tokio::spawn(async move {
                // Small delay so the subscription is established first.
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                publish_json(&server, subject, N).await;
            })
        };

        let mut stream = source.stream_pages(&ctx, 250);
        let mut seen = 0usize;
        let mut peak = 0usize;
        while let Some(page) = stream.next().await {
            let page = page.expect("page");
            peak = peak.max(page.records.len());
            seen += page.records.len();
        }
        publisher.await.expect("publisher");

        assert_eq!(seen, N, "streamed {seen}, expected {N}");
        assert!(peak <= 250, "peak page {peak} exceeds batch_size");
        assert!(peak < N, "buffered everything into one page");
    }

    /// #789 MSG-39: a binary payload arrives intact under `value_format:
    /// bytes`, fails loudly under the default instead of turning into U+FFFD
    /// text, and the dataset URI never carries URL credentials.
    #[tokio::test(flavor = "multi_thread")]
    async fn binary_payloads_survive_and_credentials_stay_out_of_lineage() {
        let (_container, server) = start_nats().await;
        let run = |format: faucet_source_nats::NatsValueFormat, subject: &'static str| {
            let server = server.clone();
            async move {
                let mut cfg = NatsSourceConfig::new(subject);
                cfg.connection.servers = vec![server.clone()];
                cfg.max_messages = Some(1);
                cfg.idle_timeout_secs = Some(10);
                cfg.value_format = format;
                let source = NatsSource::new(cfg).await.expect("source new");
                let publisher = tokio::spawn(async move {
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    let client = async_nats::connect(&server).await.expect("connect");
                    client
                        .publish(subject.to_string(), vec![0xff_u8, 0xfe, 0x00].into())
                        .await
                        .expect("publish");
                    client.flush().await.expect("flush");
                });
                let ctx = std::collections::HashMap::new();
                let mut stream = source.stream_pages(&ctx, 10);
                let mut out = Vec::new();
                while let Some(page) = stream.next().await {
                    match page {
                        Ok(p) => out.extend(p.records),
                        Err(e) => {
                            publisher.await.expect("publisher");
                            return Err(e.to_string());
                        }
                    }
                }
                publisher.await.expect("publisher");
                Ok(out)
            }
        };
        let got = run(faucet_source_nats::NatsValueFormat::Bytes, "bin.bytes")
            .await
            .unwrap();
        assert_eq!(got, vec![serde_json::json!("//4A")]);
        let err = run(faucet_source_nats::NatsValueFormat::Auto, "bin.auto")
            .await
            .unwrap_err();
        assert!(err.contains("value_format: bytes"), "{err}");

        let mut cfg = NatsSourceConfig::new("s");
        cfg.connection.servers = vec![server.replace("nats://", "nats://user:pw@")];
        let source = NatsSource::new(cfg).await.expect("source new");
        let uri = source.dataset_uri();
        assert!(
            !uri.contains("pw") && uri.starts_with("nats://") && !uri.contains("nats://nats"),
            "{uri}"
        );
    }

    async fn start_jetstream() -> (testcontainers::ContainerAsync<Nats>, String) {
        use testcontainers::ImageExt;
        use testcontainers_modules::nats::NatsServerCmd;
        let cmd = NatsServerCmd::default().with_jetstream();
        let container =
            faucet_conformance::containers::start(|| Nats::default().with_cmd(&cmd)).await;
        let host = container.get_host().await.expect("nats host");
        let port = container.get_host_port_ipv4(4222).await.expect("nats port");
        let url = format!("nats://{host}:{port}");
        // The mapped port can refuse connections for a moment after start.
        for _ in 0..50 {
            if async_nats::connect(&url).await.is_ok() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
        (container, url)
    }

    async fn drain(source: &NatsSource) -> Vec<serde_json::Value> {
        let ctx = std::collections::HashMap::new();
        let mut pages = source.stream_pages(&ctx, 4);
        let mut out = Vec::new();
        while let Some(page) = pages.next().await {
            let page = page.expect("page");
            assert!(
                page.bookmark.is_some(),
                "every JetStream page carries a bookmark (MSG-07)"
            );
            out.extend(page.records);
        }
        out
    }

    /// MSG-07 / MSG-09: JetStream pages carry a bookmark (so the pipeline
    /// flushes before the page is acked), and messages held while a page is
    /// assembled get in-progress acks, so a page that outlives the consumer's
    /// `ack_wait` is not redelivered into the same run. Without them it is.
    #[tokio::test(flavor = "multi_thread")]
    async fn held_jetstream_messages_are_not_redelivered_while_a_page_is_assembled() {
        use async_nats::jetstream::consumer::pull::Config as PullConfig;
        let (_container, server) = start_jetstream().await;
        let client = async_nats::connect(&server).await.expect("connect");
        let js = async_nats::jetstream::new(client);
        let stream = js
            .create_stream(async_nats::jetstream::stream::Config {
                name: "ORDERS".into(),
                subjects: vec!["orders.>".into()],
                ..Default::default()
            })
            .await
            .expect("stream");
        for name in ["renewed", "unrenewed"] {
            stream
                .create_consumer(PullConfig {
                    durable_name: Some(name.into()),
                    ack_wait: std::time::Duration::from_secs(3),
                    ..Default::default()
                })
                .await
                .expect("consumer");
        }
        for i in 0..2 {
            js.publish("orders.new", format!(r#"{{"i":{i}}}"#).into())
                .await
                .expect("publish")
                .await
                .expect("ack");
        }

        let config = |consumer: &str, progress: u64| {
            let mut cfg = NatsSourceConfig::new("orders.>");
            cfg.connection.servers = vec![server.clone()];
            cfg.jetstream_stream = Some("ORDERS".into());
            cfg.jetstream_consumer = Some(consumer.into());
            cfg.idle_timeout_secs = Some(8);
            cfg.batch_size = 4;
            cfg.progress_interval_secs = progress;
            cfg
        };

        let renewed = NatsSource::new(config("renewed", 1)).await.unwrap();
        assert_eq!(
            drain(&renewed).await.len(),
            2,
            "no redelivery while renewed"
        );
        let again = NatsSource::new(config("renewed", 1)).await.unwrap();
        assert!(drain(&again).await.is_empty(), "the page was acked");

        let unrenewed = NatsSource::new(config("unrenewed", 0)).await.unwrap();
        assert!(
            drain(&unrenewed).await.len() > 2,
            "without in-progress acks the held messages come back (proves the test can fail)"
        );
    }

    async fn jetstream_with(server: &str, max_ack_pending: i64, n: usize) {
        use async_nats::jetstream::consumer::pull::Config as PullConfig;
        let client = async_nats::connect(server).await.expect("connect");
        let js = async_nats::jetstream::new(client);
        let stream = js
            .create_stream(async_nats::jetstream::stream::Config {
                name: "EVENTS".into(),
                subjects: vec!["events.>".into()],
                ..Default::default()
            })
            .await
            .expect("stream");
        stream
            .create_consumer(PullConfig {
                durable_name: Some("faucet".into()),
                ack_wait: std::time::Duration::from_secs(60),
                max_ack_pending,
                ..Default::default()
            })
            .await
            .expect("consumer");
        for i in 0..n {
            js.publish("events.x", format!(r#"{{"i":{i}}}"#).into())
                .await
                .expect("publish")
                .await
                .expect("ack");
        }
    }

    fn js_config(server: &str, max: usize) -> NatsSourceConfig {
        let mut cfg = NatsSourceConfig::new("events.>");
        cfg.connection.servers = vec![server.to_string()];
        cfg.jetstream_stream = Some("EVENTS".into());
        cfg.jetstream_consumer = Some("faucet".into());
        cfg.max_messages = Some(max);
        cfg.idle_timeout_secs = None;
        cfg.batch_size = 4;
        cfg.include_metadata = true;
        cfg
    }

    /// `max_messages` pulls only what it needs, so the next run gets the rest
    /// immediately instead of after `ack_wait` (MSG-45); every record carries
    /// its stream sequence for downstream dedupe (MSG-91).
    #[tokio::test(flavor = "multi_thread")]
    async fn max_messages_leaves_the_backlog_unleased() {
        let (_container, server) = start_jetstream().await;
        jetstream_with(&server, -1, 10).await;
        let first = drain(&NatsSource::new(js_config(&server, 3)).await.unwrap()).await;
        assert_eq!(first.len(), 3);
        assert_eq!(first[0]["sequence"], 1);
        assert_eq!(first[0]["payload"], serde_json::json!({"i": 0}));
        let rest = tokio::time::timeout(
            std::time::Duration::from_secs(20),
            drain(&NatsSource::new(js_config(&server, 7)).await.unwrap()),
        )
        .await
        .expect("the rest is deliverable at once, not after ack_wait");
        let seqs: Vec<_> = rest
            .iter()
            .map(|r| r["sequence"].as_u64().unwrap())
            .collect();
        assert_eq!(seqs, (4..=10).collect::<Vec<u64>>());
    }

    /// Pages are capped at the consumer's `max_ack_pending`, so a run with
    /// only `max_messages` never stalls waiting on its own unacked page
    /// (MSG-47).
    #[tokio::test(flavor = "multi_thread")]
    async fn pages_never_exceed_max_ack_pending() {
        let (_container, server) = start_jetstream().await;
        jetstream_with(&server, 2, 6).await;
        let got = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            drain(&NatsSource::new(js_config(&server, 6)).await.unwrap()),
        )
        .await
        .expect("the run completes");
        let mut seqs: Vec<_> = got
            .iter()
            .map(|r| r["sequence"].as_u64().unwrap())
            .collect();
        seqs.dedup();
        assert_eq!(seqs, (1..=6).collect::<Vec<u64>>(), "no duplicates");
    }
}
