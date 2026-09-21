//! Idempotent global installer for the Prometheus recorder and a
//! `tracing-subscriber`. Safe to call more than once; subsequent calls warn
//! and continue rather than panicking. Port-in-use becomes a typed error.

use thiserror::Error;

/// Configuration for `install_observability`. Either or both sections may be
/// `None`; unset sections install nothing.
#[derive(Debug, Clone, Default)]
pub struct ObservabilityConfig {
    pub prometheus: Option<PrometheusConfig>,
    pub tracing: Option<TracingConfig>,
    pub otel: Option<crate::observability::otel::OtelConfig>,
}

/// Which metrics recorder to install given which exporters are requested.
/// Only consulted by `install_observability`, which itself requires the recorder
/// machinery, so it's gated on the same feature.
#[cfg(feature = "observability-install")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MetricsMode {
    None,
    PrometheusOnly,
    OtelOnly,
    Fanout,
}

#[cfg(feature = "observability-install")]
impl MetricsMode {
    pub(crate) fn select(prometheus: bool, otel_metrics: bool) -> Self {
        match (prometheus, otel_metrics) {
            (true, true) => MetricsMode::Fanout,
            (true, false) => MetricsMode::PrometheusOnly,
            (false, true) => MetricsMode::OtelOnly,
            (false, false) => MetricsMode::None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PrometheusConfig {
    /// `host:port` to bind a `/metrics` HTTP endpoint. Recommended:
    /// `127.0.0.1:9464`.
    pub listen: String,
    /// Histogram bucket overrides (in seconds). When `None`, sensible defaults
    /// apply (0.001..300s spanning sub-ms through five-minute durations).
    pub buckets: Option<Vec<f64>>,
}

#[derive(Debug, Clone)]
pub struct TracingConfig {
    /// `EnvFilter`-style directive, e.g. `"info"` or `"faucet_core=debug,info"`.
    pub level: String,
}

/// Report from `install_observability` so callers can log what actually
/// happened (recorder installed vs. already-installed vs. disabled).
#[derive(Debug, Clone, Default)]
pub struct InstallReport {
    pub prometheus_listen: Option<String>,
    pub prometheus_already_installed: bool,
    pub tracing_already_installed: bool,
    pub otel_installed: bool,
    pub otel_signals: Vec<&'static str>,
}

#[derive(Debug, Error)]
pub enum InstallError {
    #[error("failed to bind Prometheus listener at {listen}: {source}")]
    PrometheusBind {
        listen: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to install Prometheus recorder: {0}")]
    PrometheusInstall(String),
}

/// Install observability if requested. Always returns; never panics.
///
/// Behavior:
/// - If `prometheus` is set, builds a `PrometheusBuilder` and installs the
///   recorder + HTTP `/metrics` endpoint at the configured listen address.
///   Already-installed recorder (typed `BuildError::FailedToSetGlobalRecorder`)
///   is logged via `tracing::warn!` and continues. Listen-address parse failures
///   and HTTP-listener bind failures (e.g. port-in-use, typed
///   `BuildError::FailedToCreateHTTPListener`) return `InstallError::PrometheusBind`.
/// - If `tracing` is set, installs a `tracing-subscriber` registry with the
///   given env-filter directive as the default subscriber. Already-set-default
///   is logged via `tracing::warn!` and continues.
#[cfg(feature = "observability-install")]
pub fn install_observability(cfg: &ObservabilityConfig) -> Result<InstallReport, InstallError> {
    let mut report = InstallReport::default();

    // Provider holders moved into the guard after both arms run; only populated
    // (and only referenced) when the `otel` feature is enabled.
    #[cfg(feature = "otel")]
    let mut otel_tracer: Option<opentelemetry_sdk::trace::SdkTracerProvider> = None;

    // --- Metrics ---
    #[cfg(feature = "otel")]
    let otel_metrics = cfg
        .otel
        .as_ref()
        .map(|o| o.exports(crate::observability::otel::OtelSignal::Metrics))
        .unwrap_or(false);
    #[cfg(not(feature = "otel"))]
    let otel_metrics = false;

    let mode = MetricsMode::select(cfg.prometheus.is_some(), otel_metrics);

    // Declared so the trace arm + guard step can move them; only used under otel.
    #[cfg(feature = "otel")]
    let mut otel_meter: Option<opentelemetry_sdk::metrics::SdkMeterProvider> = None;

    match mode {
        MetricsMode::None => {}
        MetricsMode::PrometheusOnly => {
            install_prometheus_only(cfg.prometheus.as_ref().unwrap(), &mut report)?;
        }
        #[cfg(feature = "otel")]
        MetricsMode::OtelOnly => {
            if let Some(otel) = cfg.otel.as_ref() {
                match crate::observability::otel::build_meter_provider(otel) {
                    Ok((mp, recorder)) => {
                        if metrics::set_global_recorder(recorder).is_err() {
                            tracing::warn!("metrics recorder already installed; continuing");
                            report.prometheus_already_installed = true;
                        } else {
                            otel_meter = Some(mp);
                            report.otel_signals.push("metrics");
                        }
                    }
                    Err(e) => tracing::warn!("OTLP metrics exporter init failed; skipping: {e}"),
                }
            }
        }
        #[cfg(feature = "otel")]
        MetricsMode::Fanout => {
            install_fanout(
                cfg.prometheus.as_ref().unwrap(),
                cfg.otel.as_ref().unwrap(),
                &mut report,
                &mut otel_meter,
            )?;
        }
        #[cfg(not(feature = "otel"))]
        MetricsMode::OtelOnly | MetricsMode::Fanout => {
            unreachable!("otel metrics mode selected without the otel feature")
        }
    }

    // --- Traces ---
    if let Some(t) = cfg.tracing.as_ref() {
        use tracing_subscriber::EnvFilter;
        use tracing_subscriber::layer::SubscriberExt;
        use tracing_subscriber::util::SubscriberInitExt;

        let make_filter =
            || EnvFilter::try_new(&t.level).unwrap_or_else(|_| EnvFilter::new("info"));

        // Reassigned only in the `otel` branch below; without that feature the
        // `mut` is genuinely unused.
        #[cfg_attr(not(feature = "otel"), allow(unused_mut))]
        let mut installed = false;

        #[cfg(feature = "otel")]
        {
            let otel_traces = cfg
                .otel
                .as_ref()
                .map(|o| o.exports(crate::observability::otel::OtelSignal::Traces))
                .unwrap_or(false);
            if let (true, Some(otel)) = (otel_traces, cfg.otel.as_ref()) {
                match crate::observability::otel::build_trace_provider(otel) {
                    Ok(tp) => {
                        use opentelemetry::trace::TracerProvider as _;
                        let tracer = tp.tracer("faucet");
                        crate::observability::otel::install_propagator();
                        let reg = tracing_subscriber::registry()
                            .with(make_filter())
                            .with(tracing_subscriber::fmt::layer())
                            .with(tracing_opentelemetry::layer().with_tracer(tracer))
                            .with(crate::observability::otel::OtelErrorCountLayer);
                        if reg.try_init().is_err() {
                            tracing::warn!("tracing subscriber already installed; continuing");
                            report.tracing_already_installed = true;
                            // try_init failed: no otel layer was installed, so DON'T store the
                            // provider — let `tp` drop here to shut its exporter down.
                        } else {
                            report.otel_signals.push("traces");
                            otel_tracer = Some(tp);
                        }
                        installed = true;
                    }
                    Err(e) => tracing::warn!("OTLP trace exporter init failed; logs-only: {e}"),
                }
            }
        }

        if !installed {
            // Install the OTLP export-error counter layer whenever ANY otel
            // signal is exported — not only when a trace layer was installed —
            // so `faucet_otel_export_failures_total` still fires under
            // `otel.export: [metrics]` (audit #321 L7). `Option<Layer>` is a
            // no-op when `None`, so this composes for the non-otel build too.
            #[cfg(feature = "otel")]
            let otel_error_layer = cfg
                .otel
                .as_ref()
                .map(|o| {
                    o.exports(crate::observability::otel::OtelSignal::Traces)
                        || o.exports(crate::observability::otel::OtelSignal::Metrics)
                })
                .unwrap_or(false)
                .then_some(crate::observability::otel::OtelErrorCountLayer);
            #[cfg(not(feature = "otel"))]
            let otel_error_layer: Option<tracing_subscriber::layer::Identity> = None;

            let reg = tracing_subscriber::registry()
                .with(make_filter())
                .with(tracing_subscriber::fmt::layer())
                .with(otel_error_layer);
            if reg.try_init().is_err() {
                // Some other code path has already set a global default. Log and
                // continue — observability still works through the previously-
                // installed subscriber.
                tracing::warn!("tracing subscriber already installed; continuing");
                report.tracing_already_installed = true;
            }
        }
    }

    // --- OTel guard + describe ---
    #[cfg(feature = "otel")]
    {
        if otel_tracer.is_some() || otel_meter.is_some() {
            crate::observability::otel::describe();
            let _ = crate::observability::otel::set_guard(crate::observability::otel::OtelGuard {
                tracer: otel_tracer,
                meter: otel_meter,
            });
            report.otel_installed = true;
        }
    }

    // Register metric HELP text + build_info after any Prometheus install
    // attempt — describe!()/set!() into a not-yet-installed recorder is a no-op,
    // so we order them last.
    crate::observability::resilience::describe();
    crate::observability::cleanup::describe();
    crate::observability::drift::describe();
    crate::observability::describe_roundtrip_metrics();
    register_build_info();

    Ok(report)
}

/// Install the Prometheus recorder + `/metrics` HTTP endpoint as the sole
/// global recorder. Extracted verbatim from the original metrics arm so the
/// fanout / otel-only paths can choose a different recorder.
#[cfg(feature = "observability-install")]
fn install_prometheus_only(
    p: &PrometheusConfig,
    report: &mut InstallReport,
) -> Result<(), InstallError> {
    use metrics_exporter_prometheus::{BuildError, PrometheusBuilder};

    let listen: std::net::SocketAddr =
        p.listen
            .parse()
            .map_err(|e: std::net::AddrParseError| InstallError::PrometheusBind {
                listen: p.listen.clone(),
                source: std::io::Error::new(std::io::ErrorKind::InvalidInput, e.to_string()),
            })?;

    const DEFAULT_BUCKETS: &[f64] = &[
        0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0,
    ];
    let buckets = p.buckets.as_deref().unwrap_or(DEFAULT_BUCKETS);

    let builder = PrometheusBuilder::new()
        .with_http_listener(listen)
        .set_buckets(buckets)
        .map_err(|e| InstallError::PrometheusInstall(e.to_string()))?;

    match builder.install() {
        Ok(()) => report.prometheus_listen = Some(p.listen.clone()),
        // Match the TYPED `BuildError` variant rather than scraping its
        // Display string — the latter breaks silently if the upstream
        // wording changes.
        Err(e) => match e {
            // Recorder already installed (e.g. a prior `install` call or a
            // test harness). Idempotent: warn and continue.
            BuildError::FailedToSetGlobalRecorder(_) => {
                tracing::warn!("Prometheus recorder already installed; continuing");
                report.prometheus_already_installed = true;
            }
            // The HTTP `/metrics` listener could not bind. This is where a
            // genuine bind failure (e.g. EADDRINUSE / port-in-use) lands,
            // since the real `TcpListener::bind` happens inside `install()`,
            // not in the address parse above. Surface it as the dedicated
            // bind error so port-in-use is reported correctly.
            BuildError::FailedToCreateHTTPListener(msg) => {
                return Err(InstallError::PrometheusBind {
                    listen: p.listen.clone(),
                    source: std::io::Error::other(msg),
                });
            }
            other => return Err(InstallError::PrometheusInstall(other.to_string())),
        },
    }
    Ok(())
}

/// Install a `metrics-util` fanout recorder dispatching to BOTH a Prometheus
/// recorder (with its `/metrics` HTTP endpoint) and an OTLP metrics recorder,
/// so both coexist. If the OTLP exporter fails to build, falls back to a
/// Prometheus-only recorder so `/metrics` still works — export never fails the
/// process.
#[cfg(all(feature = "observability-install", feature = "otel"))]
fn install_fanout(
    p: &PrometheusConfig,
    otel: &crate::observability::otel::OtelConfig,
    report: &mut InstallReport,
    otel_meter: &mut Option<opentelemetry_sdk::metrics::SdkMeterProvider>,
) -> Result<(), InstallError> {
    use metrics_exporter_prometheus::{BuildError, PrometheusBuilder};
    use metrics_util::layers::FanoutBuilder;

    let listen: std::net::SocketAddr =
        p.listen
            .parse()
            .map_err(|e: std::net::AddrParseError| InstallError::PrometheusBind {
                listen: p.listen.clone(),
                source: std::io::Error::new(std::io::ErrorKind::InvalidInput, e.to_string()),
            })?;
    const DEFAULT_BUCKETS: &[f64] = &[
        0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0,
    ];
    let buckets = p.buckets.as_deref().unwrap_or(DEFAULT_BUCKETS);

    // build() returns (recorder, /metrics exporter future) WITHOUT setting the
    // global recorder, so we can fan it out.
    let (prom_recorder, prom_exporter) = PrometheusBuilder::new()
        .with_http_listener(listen)
        .set_buckets(buckets)
        .map_err(|e| InstallError::PrometheusInstall(e.to_string()))?
        .build()
        .map_err(|e| match e {
            BuildError::FailedToCreateHTTPListener(msg) => InstallError::PrometheusBind {
                listen: p.listen.clone(),
                source: std::io::Error::other(msg),
            },
            other => InstallError::PrometheusInstall(other.to_string()),
        })?;

    match crate::observability::otel::build_meter_provider(otel) {
        Ok((mp, otel_recorder)) => {
            let fanout = FanoutBuilder::default()
                .add_recorder(prom_recorder)
                .add_recorder(otel_recorder)
                .build();
            if metrics::set_global_recorder(fanout).is_err() {
                tracing::warn!("metrics recorder already installed; continuing");
                report.prometheus_already_installed = true;
            } else {
                report.prometheus_listen = Some(p.listen.clone());
                report.otel_signals.push("metrics");
                *otel_meter = Some(mp);
                tokio::spawn(prom_exporter);
            }
        }
        Err(e) => {
            // OTLP metrics init failed — fall back to Prometheus-only so /metrics
            // still works; export never fails the process.
            tracing::warn!("OTLP metrics exporter init failed; Prometheus-only: {e}");
            if metrics::set_global_recorder(prom_recorder).is_err() {
                report.prometheus_already_installed = true;
            } else {
                report.prometheus_listen = Some(p.listen.clone());
                tokio::spawn(prom_exporter);
            }
        }
    }
    Ok(())
}

/// Non-`observability-install` stub. Returns an empty report, never panics.
#[cfg(not(feature = "observability-install"))]
pub fn install_observability(_cfg: &ObservabilityConfig) -> Result<InstallReport, InstallError> {
    crate::observability::resilience::describe();
    crate::observability::cleanup::describe();
    crate::observability::drift::describe();
    crate::observability::describe_roundtrip_metrics();
    crate::observability::otel::describe();
    register_build_info();
    Ok(InstallReport::default())
}

/// Register the `faucet_build_info{version}` gauge (set to 1) under the
/// currently-installed `metrics` recorder. Safe to call from any code path
/// that wants to ensure the gauge is set; `install_observability` invokes
/// this automatically. Gauges are naturally idempotent under the `metrics`
/// model — repeat calls just re-set the same value.
///
/// The version label is `CARGO_PKG_VERSION` of `faucet-core` — matches the
/// crate that owns the observability layer. Dashboards `group_left` the gauge
/// onto every other metric to annotate panels with the running version.
pub fn register_build_info() {
    metrics::gauge!(
        "faucet_build_info",
        "version" => env!("CARGO_PKG_VERSION"),
    )
    .set(1.0);
}

#[cfg(all(test, feature = "observability-install"))]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn metrics_mode_selection() {
        use super::MetricsMode;
        assert_eq!(MetricsMode::select(true, true), MetricsMode::Fanout);
        assert_eq!(
            MetricsMode::select(true, false),
            MetricsMode::PrometheusOnly
        );
        assert_eq!(MetricsMode::select(false, true), MetricsMode::OtelOnly);
        assert_eq!(MetricsMode::select(false, false), MetricsMode::None);
    }

    #[test]
    fn no_config_returns_empty_report() {
        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let r = install_observability(&ObservabilityConfig::default()).unwrap();
        assert!(r.prometheus_listen.is_none());
        assert!(!r.prometheus_already_installed);
        assert!(!r.tracing_already_installed);
    }

    #[test]
    fn malformed_listen_returns_bind_error() {
        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let cfg = ObservabilityConfig {
            prometheus: Some(PrometheusConfig {
                listen: "not-a-socket".into(),
                buckets: None,
            }),
            tracing: None,
            otel: None,
        };
        match install_observability(&cfg) {
            Err(InstallError::PrometheusBind { .. }) => {}
            other => panic!("expected PrometheusBind error, got {other:?}"),
        }
    }

    #[test]
    fn register_build_info_is_callable_and_idempotent() {
        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        // Gauges are idempotent under the metrics model; repeat calls must not
        // panic regardless of which recorder (if any) is installed.
        register_build_info();
        register_build_info();
    }

    #[test]
    fn install_prometheus_and_tracing_returns_ok() {
        // Drive the full prometheus + tracing install path on an ephemeral
        // port. The recorder + subscriber are process-global: depending on
        // test ordering, the recorder either installs fresh (Ok path) or is
        // already installed (idempotent warn path). Either way the call must
        // return Ok and never panic, and the report must reflect what happened.
        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let cfg = ObservabilityConfig {
            prometheus: Some(PrometheusConfig {
                listen: "127.0.0.1:0".into(),
                // Exercise the explicit-buckets branch.
                buckets: Some(vec![0.01, 0.1, 1.0]),
            }),
            tracing: Some(TracingConfig {
                level: "info".into(),
            }),
            otel: None,
        };
        let report = install_observability(&cfg).expect("install must return Ok");
        // Exactly one of: recorder installed (listen set) OR already installed.
        assert!(
            report.prometheus_listen.is_some() || report.prometheus_already_installed,
            "prometheus install must either bind or report already-installed"
        );
    }

    #[test]
    fn install_tracing_with_invalid_directive_falls_back_to_info() {
        // An unparseable EnvFilter directive must not error — it silently falls
        // back to "info". The call returns Ok regardless of subscriber state.
        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let cfg = ObservabilityConfig {
            prometheus: None,
            tracing: Some(TracingConfig {
                // Garbage directive → try_new errors → fallback to info.
                level: "this is !!! not a valid filter".into(),
            }),
            otel: None,
        };
        install_observability(&cfg).expect("invalid tracing directive must not fail install");
    }
}
