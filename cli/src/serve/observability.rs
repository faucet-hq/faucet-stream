//! serve-owned observability: install the Prometheus recorder (returning a
//! render handle for the `/metrics` route) and a tracing subscriber whose fmt
//! layer routes through the secret-redacting writer and whose `RunLogLayer`
//! feeds the per-run SSE log buffers. Both are process-global and set-once; a
//! second install in the same process is tolerated (returns no handle / leaves
//! the existing subscriber). The returned [`LogHub`] is shared with
//! `ServerState` so the `/logs` handler reads the same buffers the layer writes.

use crate::serve::logs::LogHub;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use std::sync::OnceLock;

/// The tracing subscriber is process-global and set-once, so the [`LogHub`] wired
/// into it is too. A second `serve` in the same process (e.g. multiple tests)
/// reuses this hub rather than getting a detached one whose lines are never
/// captured by the live subscriber.
static LOG_HUB: OnceLock<LogHub> = OnceLock::new();

/// Install the recorder + tracing subscriber, and OTLP export when `otel` is
/// set (`--otel-config`). Returns the Prometheus render handle (when this
/// call installed the recorder; `None` if one was already present) and the
/// process-global [`LogHub`] wired into the subscriber's `RunLogLayer`.
pub fn install(
    level: &str,
    format: crate::cli::LogFormat,
    otel: Option<&faucet_core::OtelConfig>,
) -> (Option<PrometheusHandle>, LogHub) {
    crate::set_build_version();
    let handle = install_recorder(otel);
    // Register faucet_build_info into whatever recorder is now global.
    faucet_core::register_build_info();

    let hub = LOG_HUB.get_or_init(LogHub::new).clone();
    // Only the first call's `try_init` succeeds; subsequent calls leave the
    // already-installed subscriber (which holds this same hub) in place.
    install_subscriber(level, format, hub.clone());
    if let Some(otel) = otel {
        install_otel_traces(otel);
    }
    (handle, hub)
}

/// The Prometheus recorder `/metrics` renders — fanned out to the OTLP
/// exporter when `otel` exports metrics.
fn install_recorder(otel: Option<&faucet_core::OtelConfig>) -> Option<PrometheusHandle> {
    #[cfg(feature = "otel")]
    if let Some(otel) = otel.filter(|o| o.exports(faucet_core::OtelSignal::Metrics)) {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        return match faucet_core::observability::install_prometheus_with_otel_metrics(
            recorder, otel,
        ) {
            Ok(exported) => {
                if exported {
                    tracing::info!("OTLP export enabled: metrics");
                }
                Some(handle)
            }
            Err(e) => {
                tracing::warn!("Prometheus recorder already installed or failed: {e}");
                None
            }
        };
    }
    let _ = otel;
    match PrometheusBuilder::new().install_recorder() {
        Ok(h) => Some(h),
        Err(e) => {
            tracing::warn!("Prometheus recorder already installed or failed: {e}");
            None
        }
    }
}

/// OTLP traces through the trace-layer slot at the bottom of the serve
/// subscriber.
#[cfg(feature = "otel")]
fn install_otel_traces(otel: &faucet_core::OtelConfig) {
    if !otel.exports(faucet_core::OtelSignal::Traces) {
        return;
    }
    let cfg = faucet_core::ObservabilityConfig {
        otel: Some(faucet_core::OtelConfig {
            export: vec![faucet_core::OtelSignal::Traces],
            ..otel.clone()
        }),
        ..Default::default()
    };
    match faucet_core::install_observability(&cfg) {
        Ok(report) if report.otel_signals.contains(&"traces") => {
            tracing::info!("OTLP export enabled: traces");
        }
        Ok(_) => {}
        Err(e) => tracing::warn!("OTLP trace export not installed: {e}"),
    }
}

#[cfg(not(feature = "otel"))]
fn install_otel_traces(_otel: &faucet_core::OtelConfig) {
    tracing::warn!(
        "--otel-config is set but this binary was built without --features otel; OTLP export is disabled"
    );
}

#[cfg(feature = "observability")]
fn install_subscriber(level: &str, format: crate::cli::LogFormat, hub: LogHub) {
    use crate::secrets::registry::RedactingMakeWriter;
    use crate::serve::logs::RunLogLayer;
    use tracing_subscriber::EnvFilter;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    let filter = EnvFilter::try_new(level).unwrap_or_else(|_| EnvFilter::new("info"));
    // `RunLogLayer` is unaffected by the format: it captures the run's records
    // into the SSE ring with its own structured columns, which the console
    // renders. This flag governs the *stderr* stream an orchestrator ingests.
    let stderr_layer = tracing_subscriber::fmt::layer().with_writer(RedactingMakeWriter);
    let initialised = match format {
        crate::cli::LogFormat::Text => tracing_subscriber::registry()
            .with(crate::trace_layer_slot())
            .with(filter)
            .with(stderr_layer)
            .with(RunLogLayer::new(hub))
            .try_init(),
        crate::cli::LogFormat::Json => tracing_subscriber::registry()
            .with(crate::trace_layer_slot())
            .with(filter)
            .with(
                stderr_layer
                    .json()
                    .flatten_event(true)
                    .with_current_span(true)
                    .with_span_list(false),
            )
            .with(RunLogLayer::new(hub))
            .try_init(),
    };
    if initialised.is_err() {
        tracing::warn!("tracing subscriber already installed; continuing");
    }
}

#[cfg(not(feature = "observability"))]
fn install_subscriber(_level: &str, _format: crate::cli::LogFormat, _hub: LogHub) {}
