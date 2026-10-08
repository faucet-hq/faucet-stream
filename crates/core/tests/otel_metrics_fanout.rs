#![cfg(all(feature = "otel", feature = "observability-install"))]
//! A host that renders `/metrics` from its own Prometheus recorder (`faucet
//! serve`) still exports OTLP metrics: the fanout feeds both.

use axum::{Router, body::Bytes, extract::State, routing::post};
use faucet_core::observability::install_prometheus_with_otel_metrics;
use faucet_core::observability::otel::{OtelConfig, OtelProtocol, OtelSignal, shutdown_otel};
use metrics_exporter_prometheus::PrometheusBuilder;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

async fn metrics_handler(State(h): State<Arc<AtomicUsize>>, body: Bytes) {
    if !body.is_empty() {
        h.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn metrics_reach_prometheus_and_the_collector() {
    let hits = Arc::new(AtomicUsize::new(0));
    let app = Router::new()
        .route("/v1/metrics", post(metrics_handler))
        .with_state(Arc::clone(&hits));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let otel = OtelConfig {
        endpoint: format!("http://{addr}"),
        protocol: OtelProtocol::Http,
        export: vec![OtelSignal::Metrics],
        ..Default::default()
    };
    let recorder = PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    assert!(install_prometheus_with_otel_metrics(recorder, &otel).unwrap());
    let second = PrometheusBuilder::new().build_recorder();
    assert!(
        install_prometheus_with_otel_metrics(second, &otel).is_err(),
        "a recorder is already installed"
    );

    metrics::counter!("fanout_probe_total").increment(3);
    assert!(handle.render().contains("fanout_probe_total 3"));
    shutdown_otel();
    for _ in 0..50 {
        if hits.load(Ordering::SeqCst) > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        hits.load(Ordering::SeqCst) > 0,
        "no metrics export received"
    );
}
