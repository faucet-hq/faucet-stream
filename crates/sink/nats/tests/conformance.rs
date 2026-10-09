//! `faucet-conformance` Tier-1 battery for the NATS sink.
//!
//! - **Check 1** (`conformance_config_schema_valid`) — pure/offline, MUST pass.
//! - **Check 5** (`conformance_capabilities_truthful`) — boots a real NATS
//!   server via `testcontainers-modules` (Docker) and verifies the append-only
//!   capability surface against real behaviour. Docker-gated; runs only where a
//!   Docker daemon is available.
//!
//! Idempotency checks (3/4) do not apply — the NATS sink is append-only and
//! advertises no idempotency/keyed-upsert mechanism.

use faucet_conformance::assert_config_schema_valid_value;
use faucet_sink_nats::NatsSinkConfig;

// ── Check 1: config schema (offline) ────────────────────────────────────────

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(NatsSinkConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "faucet-sink-nats");
}

// ── Check 5: capabilities truthful (Docker) ──────────────────────────────────

#[cfg(test)]
mod docker {
    use super::*;
    use faucet_core::Sink as _;
    use faucet_sink_nats::NatsSink;
    use futures::StreamExt;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
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

    #[tokio::test(flavor = "multi_thread")]
    async fn conformance_capabilities_truthful() {
        let (_container, server) = start_nats().await;
        let subject = "conformance.sink";

        // A background subscriber counts every message the sink publishes; the
        // distinct_count closure settles briefly then reports the running total.
        let counter = Arc::new(AtomicUsize::new(0));
        let sub_client = async_nats::connect(&server).await.expect("sub connect");
        let mut subscriber = sub_client
            .subscribe(subject.to_string())
            .await
            .expect("subscribe");
        let counter_bg = counter.clone();
        tokio::spawn(async move {
            while subscriber.next().await.is_some() {
                counter_bg.fetch_add(1, Ordering::SeqCst);
            }
        });

        let mut cfg = NatsSinkConfig::new(subject);
        cfg.connection.servers = vec![server.clone()];
        let sink = NatsSink::new(cfg).await.expect("sink new");
        faucet_conformance::assert_batch_atomicity_declared(&sink);
        // #789 MSG-39: lineage never sees URL credentials.
        let mut with_creds = NatsSinkConfig::new(subject);
        with_creds.connection.servers = vec![server.replace("nats://", "nats://user:pw@")];
        let uri = NatsSink::new(with_creds)
            .await
            .expect("sink new")
            .dataset_uri();
        assert!(!uri.contains("pw") && !uri.contains("nats://nats"), "{uri}");

        // Check 10: connector_name is non-empty (metric-cardinality contract).
        faucet_conformance::assert_connector_name_nonempty_value(
            sink.connector_name(),
            sink.connector_name(),
        );
        // Check 11: the append-only NATS sink implements no custom check(), so
        // the core default returns a well-formed single Skip probe inside
        // Ok(report) — never an Err.
        faucet_conformance::assert_sink_preflight_check_wellformed(
            &sink,
            &faucet_core::check::CheckContext::default(),
        )
        .await;

        let counter_cl = counter.clone();
        faucet_conformance::assert_capabilities_truthful(&sink, move || {
            let c = counter_cl.clone();
            async move {
                // Let in-flight deliveries settle before reading the count.
                tokio::time::sleep(std::time::Duration::from_millis(300)).await;
                c.load(Ordering::SeqCst)
            }
        })
        .await;
    }

    /// JetStream mode awaits every publish acknowledgement: a row whose
    /// subject no stream stores fails alone instead of vanishing behind a
    /// green run (#789 MSG-31), and the batch is bounded by
    /// `publish_timeout_secs` (#789 MSG-59).
    #[tokio::test(flavor = "multi_thread")]
    async fn jetstream_publishes_are_acknowledged_per_row() {
        use testcontainers::ImageExt;
        use testcontainers_modules::nats::NatsServerCmd;
        let cmd = NatsServerCmd::default().with_jetstream();
        let container =
            faucet_conformance::containers::start(|| Nats::default().with_cmd(&cmd)).await;
        let host = container.get_host().await.unwrap();
        let port = container.get_host_port_ipv4(4222).await.unwrap();
        let server = format!("nats://{host}:{port}");
        let mut client = None;
        for _ in 0..50 {
            if let Ok(c) = async_nats::connect(&server).await {
                client = Some(c);
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
        let js = async_nats::jetstream::new(client.expect("connect"));
        let stream = js
            .create_stream(async_nats::jetstream::stream::Config {
                name: "OUT".into(),
                subjects: vec!["out.>".into()],
                ..Default::default()
            })
            .await
            .expect("stream");

        let mut cfg = NatsSinkConfig::new("out.ok");
        cfg.connection.servers = vec![server.clone()];
        cfg.subject_field = Some("to".into());
        cfg.jetstream = true;
        cfg.batch_size = 2;
        let sink = NatsSink::new(cfg).await.unwrap();
        let rows = vec![
            serde_json::json!({"to": "out.a"}),
            serde_json::json!({"to": "nowhere.b"}),
            serde_json::json!({"to": "out.c"}),
            serde_json::json!({"no_subject": 1}),
        ];
        let outcomes = sink.write_batch_partial(&rows).await.unwrap();
        assert!(outcomes[0].is_ok() && outcomes[2].is_ok(), "{outcomes:?}");
        assert!(outcomes[1].is_err() && outcomes[3].is_err());
        let info = stream.get_info().await.unwrap();
        assert_eq!(info.state.messages, 2);
        assert!(sink.write_batch(&rows).await.is_err());

        drop(container);
        let mut cfg = NatsSinkConfig::new("out.x");
        cfg.connection.servers = vec![server];
        cfg.publish_timeout_secs = 1;
        let started = std::time::Instant::now();
        let sink = NatsSink::new(cfg).await.unwrap();
        assert!(
            sink.write_batch(&[serde_json::json!({"a": 1})])
                .await
                .is_err()
        );
        assert!(started.elapsed() < std::time::Duration::from_secs(20));
    }
}
