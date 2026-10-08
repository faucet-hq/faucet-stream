#![cfg(all(feature = "otel", feature = "observability-install"))]
//! A host that installs its own global subscriber before any config is loaded
//! (the CLI) still exports OTLP traces: `install_observability` hands its trace
//! layer to the reload slot the host registered.

use axum::{Router, body::Bytes, extract::State, routing::post};
use faucet_core::observability::otel::{
    OtelConfig, OtelProtocol, OtelSignal, TraceLayer, register_trace_layer_slot, shutdown_otel,
};
use faucet_core::{ObservabilityConfig, install_observability};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

async fn traces_handler(State(h): State<Arc<AtomicUsize>>, body: Bytes) {
    if !body.is_empty() {
        h.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn traces_reach_the_collector_through_a_host_subscriber() {
    let hits = Arc::new(AtomicUsize::new(0));
    let app = Router::new()
        .route("/v1/traces", post(traces_handler))
        .with_state(Arc::clone(&hits));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let (slot, handle) = tracing_subscriber::reload::Layer::<
        Option<TraceLayer>,
        tracing_subscriber::Registry,
    >::new(None);
    tracing_subscriber::registry()
        .with(slot)
        .with(tracing_subscriber::EnvFilter::new("info"))
        .try_init()
        .unwrap();
    assert!(register_trace_layer_slot(move |l| {
        let mut l = Some(l);
        handle
            .modify(|s| *s = l.take())
            .map_err(|_| l.take().unwrap())
    }));
    assert!(!register_trace_layer_slot(Err), "one slot per process");

    let cfg = ObservabilityConfig {
        otel: Some(OtelConfig {
            endpoint: format!("http://{addr}"),
            protocol: OtelProtocol::Http,
            export: vec![OtelSignal::Traces],
            ..Default::default()
        }),
        ..Default::default()
    };
    let report = install_observability(&cfg).unwrap();
    assert_eq!(report.otel_signals, vec!["traces"]);
    assert!(!report.tracing_already_installed);
    let again = install_observability(&cfg).unwrap();
    assert!(
        again.otel_signals.is_empty(),
        "a second install keeps the first"
    );

    tracing::info_span!("exported-span").in_scope(|| tracing::info!("inside"));
    shutdown_otel();
    for _ in 0..50 {
        if hits.load(Ordering::SeqCst) > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(hits.load(Ordering::SeqCst) > 0, "no trace export received");
}
