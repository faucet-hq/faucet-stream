#![cfg(feature = "otel")]
//! Integration test: drive the isolated OTLP provider builders against an
//! in-process axum mock OTLP/HTTP collector and assert both signals arrive —
//! WITHOUT touching the global recorder/subscriber.
//!
//! Note on endpoint handling: opentelemetry-otlp 0.31's HTTP exporter uses a
//! programmatically supplied `.with_endpoint(...)` value **verbatim** — unlike
//! the `OTEL_EXPORTER_OTLP_ENDPOINT` env var, it does NOT append the standard
//! `/v1/traces` / `/v1/metrics` signal path. `http_signal_endpoint` in `otel.rs`
//! corrects this by appending the per-signal path when it is absent, so a
//! conventional bare base endpoint (e.g. `http://collector:4318`) reaches the
//! right route on the collector. This test uses the bare base to prove that.
#![allow(clippy::disallowed_methods)] // tests drive time and raw clients directly

use axum::{Router, body::Bytes, extract::State, routing::post};
use faucet_core::observability::otel::{
    OtelConfig, OtelProtocol, OtelSignal, build_meter_provider, build_trace_provider,
};
use opentelemetry::trace::{Tracer, TracerProvider as _};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

#[derive(Clone, Default)]
struct Hits {
    traces: Arc<AtomicUsize>,
    metrics: Arc<AtomicUsize>,
}

async fn traces_handler(State(h): State<Hits>, body: Bytes) {
    if !body.is_empty() {
        h.traces.fetch_add(1, Ordering::SeqCst);
    }
}
async fn metrics_handler(State(h): State<Hits>, body: Bytes) {
    if !body.is_empty() {
        h.metrics.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn spans_and_metrics_reach_collector() {
    let hits = Hits::default();
    let app = Router::new()
        .route("/v1/traces", post(traces_handler))
        .route("/v1/metrics", post(metrics_handler))
        .with_state(hits.clone());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Both providers use the bare base endpoint — the fix appends /v1/traces and
    // /v1/metrics respectively so requests reach the correct mock routes.
    let cfg = OtelConfig {
        endpoint: format!("http://{addr}"),
        protocol: OtelProtocol::Http,
        export: vec![OtelSignal::Traces, OtelSignal::Metrics],
        metric_interval_secs: 1,
        ..Default::default()
    };

    // Traces: emit a span, force-flush.
    let tp = build_trace_provider(&cfg).unwrap();
    let tracer = tp.tracer("test");
    tracer.in_span("unit-span", |_cx| {});
    let _ = tp.force_flush();

    // Metrics: record through the bridged recorder locally (not global), flush.
    let (mp, recorder) = build_meter_provider(&cfg).unwrap();
    metrics::with_local_recorder(&recorder, || {
        metrics::counter!("faucet_test_counter").increment(1);
    });
    let _ = mp.force_flush();

    // Allow the async HTTP exports to land.
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
        "no metric export received"
    );

    let _ = tp.shutdown();
    let _ = mp.shutdown();
}
