//! OpenTelemetry (OTLP) export of traces + metrics (#201).
//!
//! `OtelConfig` and its enums are pure data (no opentelemetry types) so they
//! compile unconditionally. Everything that touches the OTel SDK is gated on
//! `#[cfg(feature = "otel")]` further down this file.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// OTLP transport protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum OtelProtocol {
    /// gRPC over tonic (default OTLP port 4317). Must be initialised inside a
    /// tokio runtime.
    #[default]
    Grpc,
    /// HTTP/protobuf (default OTLP port 4318).
    Http,
}

/// A telemetry signal that can be exported over OTLP.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum OtelSignal {
    Traces,
    Metrics,
}

fn default_protocol() -> OtelProtocol {
    OtelProtocol::Grpc
}
fn default_sample_ratio() -> f64 {
    1.0
}
fn default_export() -> Vec<OtelSignal> {
    vec![OtelSignal::Traces, OtelSignal::Metrics]
}
fn default_service_name() -> String {
    "faucet".to_string()
}
fn default_timeout_secs() -> u64 {
    10
}
fn default_metric_interval_secs() -> u64 {
    60
}

/// Pure-data OTLP export configuration. Contains no opentelemetry types.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OtelConfig {
    /// Collector endpoint. When empty, resolved per protocol
    /// (`http://localhost:4317` grpc / `:4318` http) by [`OtelConfig::resolve_endpoint`].
    #[serde(default)]
    pub endpoint: String,
    #[serde(default = "default_protocol")]
    pub protocol: OtelProtocol,
    /// Extra headers sent on every export (e.g. backend auth tokens).
    #[serde(default)]
    pub headers: std::collections::HashMap<String, String>,
    /// Head-based trace sampling ratio, 0.0..=1.0.
    #[serde(default = "default_sample_ratio")]
    pub sample_ratio: f64,
    /// Which signals to export.
    #[serde(default = "default_export")]
    pub export: Vec<OtelSignal>,
    /// OTel resource `service.name`.
    #[serde(default = "default_service_name")]
    pub service_name: String,
    /// Per-export timeout (seconds).
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,
    /// Metric push interval (seconds).
    #[serde(default = "default_metric_interval_secs")]
    pub metric_interval_secs: u64,
}

impl Default for OtelConfig {
    fn default() -> Self {
        Self {
            endpoint: String::new(),
            protocol: default_protocol(),
            headers: std::collections::HashMap::new(),
            sample_ratio: default_sample_ratio(),
            export: default_export(),
            service_name: default_service_name(),
            timeout_secs: default_timeout_secs(),
            metric_interval_secs: default_metric_interval_secs(),
        }
    }
}

impl OtelConfig {
    /// Whether the given signal is in the export list.
    pub fn exports(&self, signal: OtelSignal) -> bool {
        self.export.contains(&signal)
    }

    /// The collector endpoint, filling in the protocol-specific default when
    /// `endpoint` is empty.
    pub fn resolve_endpoint(&self) -> String {
        if !self.endpoint.is_empty() {
            return self.endpoint.clone();
        }
        match self.protocol {
            OtelProtocol::Grpc => "http://localhost:4317".to_string(),
            OtelProtocol::Http => "http://localhost:4318".to_string(),
        }
    }

    /// Validate ranges + endpoint URL at config-load time. Returns a message on
    /// the first problem.
    pub fn validate(&self) -> Result<(), String> {
        if !(0.0..=1.0).contains(&self.sample_ratio) {
            return Err(format!(
                "otel.sample_ratio must be in 0.0..=1.0, got {}",
                self.sample_ratio
            ));
        }
        if self.timeout_secs == 0 {
            return Err("otel.timeout_secs must be > 0".to_string());
        }
        if self.metric_interval_secs == 0 {
            return Err("otel.metric_interval_secs must be > 0".to_string());
        }
        // resolve_endpoint() always yields a value; validate the effective URL.
        let ep = self.resolve_endpoint();
        url::Url::parse(&ep)
            .map_err(|e| format!("otel.endpoint is not a valid URL ({ep}): {e}"))?;
        Ok(())
    }
}

/// opentelemetry-otlp uses a programmatic HTTP endpoint verbatim (it only
/// appends the per-signal path for the `OTEL_EXPORTER_OTLP_ENDPOINT` env-var
/// form). Users naturally configure the base endpoint, so for HTTP we append the
/// conventional per-signal path ourselves when it is not already present.
// Always compiled (unit-testable without the feature); only *called* from the
// otel-gated `mod sdk`, so it reads as dead code in a no-otel non-test build.
#[cfg_attr(not(feature = "otel"), allow(dead_code))]
pub(crate) fn http_signal_endpoint(base: &str, signal_path: &str) -> String {
    let trimmed = base.trim_end_matches('/');
    if trimmed.ends_with(signal_path) {
        trimmed.to_string()
    } else {
        format!("{trimmed}{signal_path}")
    }
}

/// Map an `opentelemetry*` tracing event target to a `signal` label for the
/// `faucet_otel_export_failures_total` counter. Pure — unit-testable without a
/// tracing `Context`.
// Always compiled (unit-testable without the feature); only *called* from the
// otel-gated `mod layer`, so it reads as dead code in a no-otel non-test build.
#[cfg_attr(not(feature = "otel"), allow(dead_code))]
pub(crate) fn otel_signal_label(target: &str) -> &'static str {
    if target.contains("metric") {
        "metrics"
    } else if target.contains("trace") || target.contains("span") {
        "traces"
    } else {
        "export"
    }
}

/// Register HELP text for the OTLP export metric. Called from
/// `install_observability` (a `describe!` into a not-yet-installed recorder is a
/// no-op, so ordering is forgiving).
pub fn describe() {
    metrics::describe_counter!(
        "faucet_otel_export_failures_total",
        "OTLP export attempts that failed (collector unreachable, serialization error, etc.)."
    );
}

#[cfg(feature = "otel")]
mod layer {
    use super::otel_signal_label;
    use tracing::{Event, Level, Subscriber};
    use tracing_subscriber::layer::{Context, Layer};

    /// A `tracing` layer that counts ERROR/WARN events emitted by the
    /// opentelemetry SDK (its only error channel in the 0.31 line, since
    /// `global::set_error_handler` was removed) into
    /// `faucet_otel_export_failures_total{signal}`.
    pub struct OtelErrorCountLayer;

    impl<S: Subscriber> Layer<S> for OtelErrorCountLayer {
        fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
            let meta = event.metadata();
            let target = meta.target();
            if target.starts_with("opentelemetry")
                && matches!(*meta.level(), Level::ERROR | Level::WARN)
            {
                metrics::counter!(
                    "faucet_otel_export_failures_total",
                    "signal" => otel_signal_label(target),
                )
                .increment(1);
            }
        }
    }
}

#[cfg(feature = "otel")]
pub use layer::OtelErrorCountLayer;

#[cfg(feature = "otel")]
mod sdk {
    use super::{OtelConfig, OtelProtocol};
    use opentelemetry_otlp::{WithExportConfig, WithHttpConfig, WithTonicConfig};
    use opentelemetry_sdk::Resource;
    use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
    use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};
    use std::sync::OnceLock;
    use std::time::Duration;

    pub type OtelError = Box<dyn std::error::Error + Send + Sync>;

    fn resource(cfg: &OtelConfig) -> Resource {
        Resource::builder()
            .with_service_name(cfg.service_name.clone())
            .build()
    }

    fn header_map(cfg: &OtelConfig) -> std::collections::HashMap<String, String> {
        cfg.headers.clone()
    }

    /// The configured headers as gRPC metadata, for the tonic exporters.
    pub(super) fn header_metadata(
        cfg: &OtelConfig,
    ) -> Result<opentelemetry_otlp::tonic_types::metadata::MetadataMap, OtelError> {
        use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
        let mut headers = HeaderMap::new();
        for (k, v) in &cfg.headers {
            let name = HeaderName::from_bytes(k.to_ascii_lowercase().as_bytes())
                .map_err(|e| format!("otel.headers: invalid header name '{k}': {e}"))?;
            let value = HeaderValue::from_str(v)
                .map_err(|e| format!("otel.headers: invalid value for header '{k}': {e}"))?;
            headers.insert(name, value);
        }
        Ok(opentelemetry_otlp::tonic_types::metadata::MetadataMap::from_headers(headers))
    }

    /// Build a batch-exporting tracer provider. NOTE: with grpc-tonic this MUST
    /// be called inside a tokio runtime (the CLI is `#[tokio::main]`).
    pub fn build_trace_provider(cfg: &OtelConfig) -> Result<SdkTracerProvider, OtelError> {
        let endpoint = cfg.resolve_endpoint();
        let timeout = Duration::from_secs(cfg.timeout_secs);
        let exporter = match cfg.protocol {
            OtelProtocol::Grpc => opentelemetry_otlp::SpanExporter::builder()
                .with_tonic()
                .with_endpoint(&endpoint)
                .with_metadata(header_metadata(cfg)?)
                .with_timeout(timeout)
                .build()?,
            OtelProtocol::Http => opentelemetry_otlp::SpanExporter::builder()
                .with_http()
                .with_endpoint(super::http_signal_endpoint(&endpoint, "/v1/traces"))
                .with_headers(header_map(cfg))
                .with_timeout(timeout)
                .build()?,
        };
        let provider = SdkTracerProvider::builder()
            .with_batch_exporter(exporter)
            .with_sampler(Sampler::TraceIdRatioBased(cfg.sample_ratio))
            .with_resource(resource(cfg))
            .build();
        Ok(provider)
    }

    /// Build a NON-global metrics recorder bridging the `metrics` facade to an
    /// OTLP PeriodicReader, plus its meter provider (kept alive by the caller).
    pub fn build_meter_provider(
        cfg: &OtelConfig,
    ) -> Result<(SdkMeterProvider, metrics_exporter_opentelemetry::Recorder), OtelError> {
        let endpoint = cfg.resolve_endpoint();
        let timeout = Duration::from_secs(cfg.timeout_secs);
        let exporter = match cfg.protocol {
            OtelProtocol::Grpc => opentelemetry_otlp::MetricExporter::builder()
                .with_tonic()
                .with_endpoint(&endpoint)
                .with_metadata(header_metadata(cfg)?)
                .with_timeout(timeout)
                .build()?,
            OtelProtocol::Http => opentelemetry_otlp::MetricExporter::builder()
                .with_http()
                .with_endpoint(super::http_signal_endpoint(&endpoint, "/v1/metrics"))
                .with_headers(header_map(cfg))
                .with_timeout(timeout)
                .build()?,
        };
        let reader = PeriodicReader::builder(exporter)
            .with_interval(Duration::from_secs(cfg.metric_interval_secs))
            .build();
        let res = resource(cfg);
        let (provider, recorder) =
            metrics_exporter_opentelemetry::Recorder::builder(cfg.service_name.clone())
                .with_meter_provider(move |mb| mb.with_reader(reader).with_resource(res))
                .build();
        Ok((provider, recorder))
    }

    pub struct OtelGuard {
        pub tracer: Option<SdkTracerProvider>,
        pub meter: Option<SdkMeterProvider>,
    }

    static GUARD: OnceLock<OtelGuard> = OnceLock::new();

    /// Store providers for the process lifetime. Returns false if already set.
    pub fn set_guard(guard: OtelGuard) -> bool {
        GUARD.set(guard).is_ok()
    }

    /// Flush + shut down installed providers. Idempotent.
    pub fn shutdown_otel() {
        if let Some(g) = GUARD.get() {
            if let Some(t) = g.tracer.as_ref() {
                let _ = t.force_flush();
                let _ = t.shutdown();
            }
            if let Some(m) = g.meter.as_ref() {
                let _ = m.force_flush();
                let _ = m.shutdown();
            }
        }
    }

    /// The OTLP trace layer [`install_observability`](crate::install_observability)
    /// builds, typed for the bottom of a [`tracing_subscriber::Registry`] stack.
    pub type TraceLayer =
        Box<dyn tracing_subscriber::Layer<tracing_subscriber::Registry> + Send + Sync>;

    type TraceSlot = Box<dyn Fn(TraceLayer) -> Result<(), TraceLayer> + Send + Sync>;
    static TRACE_SLOT: OnceLock<TraceSlot> = OnceLock::new();

    /// Register where the OTLP trace layer goes when the host installed its own
    /// global subscriber first (the CLI does, before any config is loaded): a
    /// `tracing_subscriber::reload` slot at the bottom of that subscriber. The
    /// closure returns the layer back when it cannot take it. Returns `false`
    /// when a slot was already registered.
    pub fn register_trace_layer_slot(
        slot: impl Fn(TraceLayer) -> Result<(), TraceLayer> + Send + Sync + 'static,
    ) -> bool {
        TRACE_SLOT.set(Box::new(slot)).is_ok()
    }

    /// Hand `layer` to the registered slot; gives it back when there is none
    /// or the slot refused it.
    pub(crate) fn fill_trace_slot(layer: TraceLayer) -> Result<(), TraceLayer> {
        match TRACE_SLOT.get() {
            Some(slot) => slot(layer),
            None => Err(layer),
        }
    }

    /// Whether a trace provider is already installed for this process.
    pub(crate) fn tracer_installed() -> bool {
        GUARD.get().is_some_and(|g| g.tracer.is_some())
    }

    /// Install the W3C trace-context propagator globally (#230 groundwork).
    pub fn install_propagator() {
        use opentelemetry_sdk::propagation::TraceContextPropagator;
        opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
    }
}

#[cfg(feature = "otel")]
pub use sdk::{
    OtelError, OtelGuard, TraceLayer, build_meter_provider, build_trace_provider,
    install_propagator, register_trace_layer_slot, set_guard, shutdown_otel,
};
#[cfg(feature = "otel")]
pub(crate) use sdk::{fill_trace_slot, tracer_installed};

/// No-op `shutdown_otel` when the `otel` feature is disabled, so CLI call sites
/// compile in every build.
#[cfg(not(feature = "otel"))]
pub fn shutdown_otel() {}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "otel")]
    #[test]
    fn grpc_exports_carry_the_configured_headers() {
        let mut cfg: OtelConfig = serde_json::from_value(serde_json::json!({
            "headers": {"Authorization": "Bearer t0k", "x-tenant": "acme"}
        }))
        .unwrap();
        let md = sdk::header_metadata(&cfg).unwrap();
        assert_eq!(md.get("authorization").unwrap(), "Bearer t0k");
        assert_eq!(md.get("x-tenant").unwrap(), "acme");
        cfg.headers.insert("bad header".into(), "v".into());
        assert!(sdk::header_metadata(&cfg).is_err());
        cfg.headers.clear();
        cfg.headers.insert("ok".into(), "line\nbreak".into());
        assert!(sdk::header_metadata(&cfg).is_err());
    }

    #[cfg(feature = "otel")]
    #[test]
    fn error_layer_increments_export_failures_counter() {
        // Validates the failure-isolation acceptance criterion end-to-end: an
        // ERROR event from an `opentelemetry*` target, routed through
        // `OtelErrorCountLayer`, bumps `faucet_otel_export_failures_total` with
        // the classified `signal` label. Reuses the crate-wide shared debugging
        // recorder + LOCK (same pattern as the decorator metric tests).
        use crate::observability::decorator::source_tests::{LOCK, snapshotter};
        use metrics_util::debugging::DebugValue;
        use tracing_subscriber::layer::SubscriberExt;

        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let snap = snapshotter();

        let subscriber = tracing_subscriber::registry().with(OtelErrorCountLayer);
        tracing::subscriber::with_default(subscriber, || {
            tracing::error!(
                target: "opentelemetry_sdk::metrics::periodic_reader",
                "simulated export failure"
            );
        });

        let found = snap.snapshot().into_vec().into_iter().any(
            |(key, _u, _d, v): (metrics_util::CompositeKey, _, _, _)| {
                key.key().name() == "faucet_otel_export_failures_total"
                    && key
                        .key()
                        .labels()
                        .any(|l: &metrics::Label| l.key() == "signal" && l.value() == "metrics")
                    && matches!(v, DebugValue::Counter(c) if c >= 1)
            },
        );
        assert!(
            found,
            "OtelErrorCountLayer must increment faucet_otel_export_failures_total{{signal=metrics}}"
        );
    }

    #[cfg(feature = "otel")]
    #[tokio::test]
    async fn build_providers_construct_without_a_live_collector() {
        let cfg = OtelConfig {
            endpoint: "http://localhost:4317".into(),
            protocol: OtelProtocol::Grpc,
            ..Default::default()
        };
        let tp = build_trace_provider(&cfg).expect("trace provider builds");
        let (mp, _recorder) = build_meter_provider(&cfg).expect("meter provider builds");
        let _ = tp.force_flush();
        let _ = mp.force_flush();
        let _ = tp.shutdown();
        let _ = mp.shutdown();
    }

    #[test]
    fn http_signal_endpoint_appends_path_when_absent() {
        assert_eq!(
            http_signal_endpoint("http://c:4318", "/v1/traces"),
            "http://c:4318/v1/traces"
        );
        assert_eq!(
            http_signal_endpoint("http://c:4318/", "/v1/traces"),
            "http://c:4318/v1/traces"
        );
        // idempotent: already has the path
        assert_eq!(
            http_signal_endpoint("http://c:4318/v1/traces", "/v1/traces"),
            "http://c:4318/v1/traces"
        );
        assert_eq!(
            http_signal_endpoint("http://c:4318", "/v1/metrics"),
            "http://c:4318/v1/metrics"
        );
    }

    #[test]
    fn record_otel_error_classifies_signal_by_target() {
        assert_eq!(
            otel_signal_label("opentelemetry_sdk::metrics::periodic_reader"),
            "metrics"
        );
        assert_eq!(
            otel_signal_label("opentelemetry_sdk::trace::span_processor"),
            "traces"
        );
        assert_eq!(otel_signal_label("opentelemetry_otlp::exporter"), "export");
        assert_eq!(otel_signal_label("opentelemetry"), "export");
    }

    #[test]
    fn config_defaults_are_applied_from_empty_yaml() {
        let cfg: OtelConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(cfg.protocol, OtelProtocol::Grpc);
        assert_eq!(cfg.sample_ratio, 1.0);
        assert_eq!(cfg.export, vec![OtelSignal::Traces, OtelSignal::Metrics]);
        assert_eq!(cfg.service_name, "faucet");
        assert_eq!(cfg.timeout_secs, 10);
        assert_eq!(cfg.metric_interval_secs, 60);
        assert!(cfg.exports(OtelSignal::Traces));
        assert!(cfg.exports(OtelSignal::Metrics));
    }

    #[test]
    fn resolve_endpoint_defaults_per_protocol() {
        let mut cfg = OtelConfig::default();
        assert_eq!(cfg.resolve_endpoint(), "http://localhost:4317");
        cfg.protocol = OtelProtocol::Http;
        assert_eq!(cfg.resolve_endpoint(), "http://localhost:4318");
        cfg.endpoint = "http://collector:4317".into();
        assert_eq!(cfg.resolve_endpoint(), "http://collector:4317");
    }

    #[test]
    fn validate_rejects_bad_values() {
        let mut cfg = OtelConfig::default();
        assert!(cfg.validate().is_ok());

        cfg.sample_ratio = 1.5;
        assert!(cfg.validate().is_err());
        cfg.sample_ratio = -0.1;
        assert!(cfg.validate().is_err());
        cfg.sample_ratio = 0.5;
        assert!(cfg.validate().is_ok());

        cfg.timeout_secs = 0;
        assert!(cfg.validate().is_err());
        cfg.timeout_secs = 10;

        cfg.metric_interval_secs = 0;
        assert!(cfg.validate().is_err());
        cfg.metric_interval_secs = 60;

        cfg.endpoint = "not a url".into();
        assert!(cfg.validate().is_err());
    }
}
