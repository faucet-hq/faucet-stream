//! `faucet-conformance` battery for the Redis sink.
//! Passing this battery in CI is the Tier-1 (supported) criterion.
//!
//! - check 1 `assert_config_schema_valid_value`
//! - check 4 `assert_idempotent_replay` — the atomic-watermark path
//!   (`write_batch_idempotent` + `last_committed_token`, one `MULTI`/`EXEC`
//!   transaction with a `_faucet_commit_token:<scope>` key).
//! - check 5 `assert_capabilities_truthful` — Append plus the advertised
//!   idempotency mechanism actually hold.
//!
//! Checks 4 and 5 boot a real Redis container (reusing `exactly_once.rs`'s
//! setup), so they require Docker.
use faucet_conformance::assert_config_schema_valid_value;

#[test]
fn conformance_config_schema_valid() {
    let schema =
        serde_json::to_value(schemars::schema_for!(faucet_sink_redis::RedisSinkConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "redis");
}

mod idempotent {
    use faucet_core::Sink as _;
    use faucet_sink_redis::{RedisSink, RedisSinkConfig, RedisSinkType};
    use redis::AsyncCommands;
    use testcontainers::ContainerAsync;
    use testcontainers_modules::redis::{REDIS_PORT, Redis};

    /// Boot a Redis container — mirrors `exactly_once.rs::start_redis`.
    async fn start_redis() -> (ContainerAsync<Redis>, String) {
        let container: ContainerAsync<Redis> =
            faucet_conformance::containers::start(Redis::default).await;
        let host = container.get_host().await.expect("redis host");
        let port = container
            .get_host_port_ipv4(REDIS_PORT)
            .await
            .expect("redis port");
        let url = format!("redis://{host}:{port}");
        let _ = open_conn(&url).await;
        (container, url)
    }

    /// Multiplexed connection with short retry — mirrors `exactly_once.rs::open_conn`.
    async fn open_conn(url: &str) -> redis::aio::MultiplexedConnection {
        let client = redis::Client::open(url).expect("redis client open");
        let mut last_err: Option<redis::RedisError> = None;
        for _ in 0..30 {
            match client.get_multiplexed_async_connection().await {
                Ok(conn) => return conn,
                Err(e) => {
                    last_err = Some(e);
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                }
            }
        }
        panic!("redis connect: {:?}", last_err);
    }

    /// A fresh container + a List-mode sink pointed at the key `q`. The List
    /// sink appends one entry per record, so the distinct-row count is `llen q`
    /// (the `_faucet_commit_token:<scope>` watermark lives in a separate key and
    /// does not affect the list length).
    async fn fresh_sink() -> (ContainerAsync<Redis>, String, RedisSink) {
        let (container, url) = start_redis().await;
        let sink = RedisSink::new(RedisSinkConfig::new(
            &url,
            RedisSinkType::List { key: "q".into() },
        ))
        .await
        .expect("sink build");
        faucet_conformance::assert_batch_atomicity_declared(&sink);
        (container, url, sink)
    }

    async fn count_entries(url: &str) -> usize {
        let mut conn = open_conn(url).await;
        conn.llen("q").await.expect("llen")
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn conformance_idempotent_replay() {
        let (_container, url, sink) = fresh_sink().await;
        faucet_conformance::assert_idempotent_replay(&sink, || {
            let url = url.clone();
            async move { count_entries(&url).await }
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn conformance_capabilities_truthful() {
        let (_container, url, sink) = fresh_sink().await;
        // Check 10: connector_name is non-empty (metric-cardinality contract).
        faucet_conformance::assert_connector_name_nonempty_value(
            sink.connector_name(),
            sink.connector_name(),
        );
        assert_eq!(sink.connector_name(), "redis");
        // Check 11: preflight check() is well-formed against the live server
        // (Redis `PING` → a Pass probe inside Ok(report); nothing written).
        faucet_conformance::assert_sink_preflight_check_wellformed(
            &sink,
            &faucet_core::check::CheckContext::default(),
        )
        .await;
        faucet_conformance::assert_capabilities_truthful(&sink, || {
            let url = url.clone();
            async move { count_entries(&url).await }
        })
        .await;
    }
}
