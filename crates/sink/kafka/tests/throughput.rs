//! Manual throughput measurement against an already-running broker.
//!
//! `FAUCET_KAFKA_BENCH_BROKERS=127.0.0.1:9092 cargo test -p faucet-sink-kafka
//! --release --test throughput -- --ignored --nocapture`. Optional:
//! `FAUCET_KAFKA_BENCH_MESSAGES` (default 200000),
//! `FAUCET_KAFKA_BENCH_MAX_IN_FLIGHT` (default: the sink's default).

use faucet_core::Sink;
use faucet_sink_kafka::{KafkaSink, KafkaSinkConfig};
use serde_json::{Value, json};
use std::time::Instant;

fn record(i: usize) -> Value {
    json!({
        "id": i,
        "name": format!("customer-{i:08}"),
        "email": format!("customer-{i:08}@example.test"),
        "status": if i.is_multiple_of(3) { "active" } else { "inactive" },
        "amount": (i as f64) * 1.25,
        "created_at": "2026-01-01T00:00:00Z",
        "notes": "x".repeat(420),
    })
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "manual benchmark; needs FAUCET_KAFKA_BENCH_BROKERS"]
async fn at_least_once_throughput() {
    let Ok(brokers) = std::env::var("FAUCET_KAFKA_BENCH_BROKERS") else {
        eprintln!("FAUCET_KAFKA_BENCH_BROKERS not set; skipping");
        return;
    };
    let total: usize = std::env::var("FAUCET_KAFKA_BENCH_MESSAGES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(200_000);
    let topic = format!(
        "faucet_bench_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or_default()
    );
    let mut config = json!({
        "brokers": brokers,
        "topic": {"type": "fixed", "name": topic},
    });
    if let Ok(window) = std::env::var("FAUCET_KAFKA_BENCH_MAX_IN_FLIGHT") {
        config["max_in_flight"] = json!(window.parse::<usize>().expect("window"));
    }
    let config: KafkaSinkConfig = serde_json::from_value(config).expect("config");
    let window = config.max_in_flight;
    let page_size = config.batch_size;
    let sink = KafkaSink::new(config).await.expect("sink");

    let pages: Vec<Vec<Value>> = (0..total)
        .collect::<Vec<_>>()
        .chunks(page_size)
        .map(|c| c.iter().map(|i| record(*i)).collect())
        .collect();
    let bytes = serde_json::to_vec(&pages[0][0]).expect("encode").len();

    sink.write_batch(&pages[0][..1]).await.expect("warm up");
    let started = Instant::now();
    let mut written = 0usize;
    for page in &pages {
        written += sink.write_batch(page).await.expect("write");
    }
    sink.flush().await.expect("flush");
    let elapsed = started.elapsed();
    assert_eq!(written, total);
    eprintln!(
        "kafka sink at-least-once: {total} messages (~{bytes} B) in {:.2}s = {:.0} msg/s (max_in_flight {window}, page {page_size})",
        elapsed.as_secs_f64(),
        total as f64 / elapsed.as_secs_f64()
    );
}
