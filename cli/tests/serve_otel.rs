#![cfg(all(feature = "serve", feature = "otel"))]
//! `faucet serve --otel-config` exports OTLP traces through the trace-layer
//! slot at the bottom of the serve subscriber, and OTLP metrics alongside the
//! Prometheus recorder `/metrics` renders.

use axum::{Router, body::Bytes, extract::State, routing::post};
use faucet_cli::cli::LogFormat;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

#[derive(Clone, Default)]
struct Hits {
    traces: Arc<AtomicUsize>,
    metrics: Arc<AtomicUsize>,
}

async fn traces(State(h): State<Hits>, body: Bytes) {
    if !body.is_empty() {
        h.traces.fetch_add(1, Ordering::SeqCst);
    }
}

async fn metrics_route(State(h): State<Hits>, body: Bytes) {
    if !body.is_empty() {
        h.metrics.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn serve_exports_traces_and_metrics_over_otlp() {
    let hits = Hits::default();
    let app = Router::new()
        .route("/v1/traces", post(traces))
        .route("/v1/metrics", post(metrics_route))
        .with_state(hits.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("otel.yaml");
    std::fs::write(
        &path,
        format!(
            "endpoint: http://{addr}\nprotocol: http\nexport: [traces, metrics]\n\
             service_name: faucet-serve-test\n"
        ),
    )
    .unwrap();
    let otel = faucet_cli::serve::config::load_otel_config(&path).unwrap();

    let (handle, _hub) =
        faucet_cli::serve::observability::install("info", LogFormat::Text, Some(&otel));
    let handle = handle.expect("serve installed the Prometheus recorder");

    tracing::info_span!("serve-span").in_scope(|| tracing::info!("inside"));
    metrics::counter!("serve_otel_probe_total").increment(2);
    assert!(handle.render().contains("serve_otel_probe_total 2"));

    faucet_core::shutdown_otel();
    for _ in 0..50 {
        if hits.traces.load(Ordering::SeqCst) > 0 && hits.metrics.load(Ordering::SeqCst) > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        hits.traces.load(Ordering::SeqCst) > 0,
        "no trace export received"
    );
    assert!(
        hits.metrics.load(Ordering::SeqCst) > 0,
        "no metrics export received"
    );
}
