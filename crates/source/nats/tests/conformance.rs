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
    assert_errors_not_panics(&source).await;
}

// ── Check 2: bounded-memory streaming (Docker) ───────────────────────────────

#[cfg(test)]
mod docker {
    use super::*;
    use faucet_source_nats::Source;
    use futures::StreamExt;
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::nats::Nats;

    async fn start_nats() -> (testcontainers::ContainerAsync<Nats>, String) {
        let container = Nats::default().start().await.expect("nats container start");
        let host = container.get_host().await.expect("nats host");
        let port = container.get_host_port_ipv4(4222).await.expect("nats port");
        (container, format!("nats://{host}:{port}"))
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

    async fn start_jetstream() -> (testcontainers::ContainerAsync<Nats>, String) {
        use testcontainers::ImageExt;
        use testcontainers_modules::nats::NatsServerCmd;
        let cmd = NatsServerCmd::default().with_jetstream();
        let container = Nats::default()
            .with_cmd(&cmd)
            .start()
            .await
            .expect("nats jetstream container start");
        let host = container.get_host().await.expect("nats host");
        let port = container.get_host_port_ipv4(4222).await.expect("nats port");
        (container, format!("nats://{host}:{port}"))
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
}
