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
    /// Histogram bucket overrides (in seconds), applied to every histogram.
    /// When `None`, 0.001..300 s applies, and the histograms that time whole
    /// runs (run / invocation duration, schedule lateness, sink flush, verify)
    /// get 0.1 s..24 h.
    pub buckets: Option<Vec<f64>>,
    /// Drop counter and histogram series not updated for this long from the
    /// recorder (and so from `/metrics`) until they are emitted again. Gauges
    /// never expire. `None` keeps every series for the life of the process.
    pub idle_timeout: Option<std::time::Duration>,
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
/// - OTLP export (feature `otel`) needs a tokio runtime; called outside one it
///   is skipped with a warning.
#[cfg(feature = "observability-install")]
pub fn install_observability(cfg: &ObservabilityConfig) -> Result<InstallReport, InstallError> {
    let mut report = InstallReport::default();

    // The OTLP exporters (and the fanout's `/metrics` task) need a tokio
    // runtime; outside one, skip OTLP rather than panic.
    #[cfg(feature = "otel")]
    let without_otel;
    #[cfg(feature = "otel")]
    let cfg = if cfg.otel.is_some() && tokio::runtime::Handle::try_current().is_err() {
        tracing::warn!("OTLP export needs a tokio runtime; install_observability skips it");
        without_otel = ObservabilityConfig {
            otel: None,
            ..cfg.clone()
        };
        &without_otel
    } else {
        cfg
    };

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
                            tracing::warn!(
                                "metrics recorder already installed; OTLP metrics are not exported"
                            );
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
    #[cfg(feature = "otel")]
    let otel_traces = cfg
        .otel
        .as_ref()
        .is_some_and(|o| o.exports(crate::observability::otel::OtelSignal::Traces));
    #[cfg(not(feature = "otel"))]
    let otel_traces = false;
    if cfg.tracing.is_some() || otel_traces {
        use tracing_subscriber::EnvFilter;
        use tracing_subscriber::layer::SubscriberExt;
        use tracing_subscriber::util::SubscriberInitExt;

        let level = cfg.tracing.as_ref().map_or("info", |t| t.level.as_str());
        let make_filter = || EnvFilter::try_new(level).unwrap_or_else(|_| EnvFilter::new("info"));

        // Reassigned only in the `otel` branch below; without that feature the
        // `mut` is genuinely unused.
        #[cfg_attr(not(feature = "otel"), allow(unused_mut))]
        let mut installed = false;

        #[cfg(feature = "otel")]
        if let (true, Some(otel)) = (otel_traces, cfg.otel.as_ref()) {
            if crate::observability::otel::tracer_installed() {
                installed = true;
            } else {
                match crate::observability::otel::build_trace_provider(otel) {
                    Ok(tp) => {
                        use opentelemetry::trace::TracerProvider as _;
                        use tracing_subscriber::Layer as _;
                        let tracer = tp.tracer("faucet");
                        crate::observability::otel::install_propagator();
                        let layer: crate::observability::otel::TraceLayer = Box::new(
                            tracing_opentelemetry::layer()
                                .with_tracer(tracer)
                                .and_then(crate::observability::otel::OtelErrorCountLayer),
                        );
                        // A host that installed its own subscriber first (the CLI)
                        // registered a slot for exactly this layer.
                        match crate::observability::otel::fill_trace_slot(layer) {
                            Ok(()) => {
                                report.otel_signals.push("traces");
                                otel_tracer = Some(tp);
                            }
                            Err(layer) => {
                                let reg = tracing_subscriber::registry()
                                    .with(layer)
                                    .with(make_filter())
                                    .with(
                                        cfg.tracing.is_some().then(tracing_subscriber::fmt::layer),
                                    );
                                if reg.try_init().is_err() {
                                    tracing::warn!(
                                        "tracing subscriber already installed and no trace \
                                         layer slot registered; OTLP traces are not exported"
                                    );
                                    report.tracing_already_installed = true;
                                    // No otel layer was installed, so DON'T store the
                                    // provider — let `tp` drop to shut its exporter down.
                                } else {
                                    report.otel_signals.push("traces");
                                    otel_tracer = Some(tp);
                                }
                            }
                        }
                        installed = true;
                    }
                    Err(e) => tracing::warn!(
                        "OTLP trace exporter init failed; traces are not exported: {e}"
                    ),
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
                .with(cfg.tracing.is_some().then(tracing_subscriber::fmt::layer))
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
/// Histogram buckets (seconds) for calls, pages and writes.
#[cfg(feature = "observability-install")]
const DEFAULT_BUCKETS: &[f64] = &[
    0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0,
];

/// Buckets for histograms that time whole runs, which last up to hours.
#[cfg(feature = "observability-install")]
const LONG_BUCKETS: &[f64] = &[
    0.1, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0, 900.0, 1800.0, 3600.0, 7200.0, 21600.0, 86400.0,
];

/// Histograms that get [`LONG_BUCKETS`] unless `prometheus.buckets` is set.
#[cfg(feature = "observability-install")]
const LONG_RUNNING_HISTOGRAMS: &[&str] = &[
    "faucet_pipeline_run_duration_seconds",
    "faucet_pipeline_invocation_duration_seconds",
    "faucet_schedule_run_lateness_seconds",
    "faucet_sink_flush_duration_seconds",
    "faucet_verify_duration_seconds",
];

#[cfg(feature = "observability-install")]
fn bucket_error(e: metrics_exporter_prometheus::BuildError) -> InstallError {
    InstallError::PrometheusInstall(e.to_string())
}

/// Apply `p.buckets` to every histogram, or the defaults with longer buckets
/// for the run-length histograms.
#[cfg(feature = "observability-install")]
fn with_buckets(
    builder: metrics_exporter_prometheus::PrometheusBuilder,
    p: &PrometheusConfig,
) -> Result<metrics_exporter_prometheus::PrometheusBuilder, InstallError> {
    if let Some(buckets) = p.buckets.as_deref() {
        return builder.set_buckets(buckets).map_err(bucket_error);
    }
    with_default_buckets(builder)
}

/// Give a Prometheus builder faucet's default histogram buckets: the buckets
/// [`install_observability`] uses when `prometheus.buckets` is unset, with
/// longer ones for the histograms that time whole runs. Without buckets the
/// exporter renders every histogram as a summary, which `histogram_quantile`
/// cannot read. For a process that installs its own recorder (`faucet serve`).
#[cfg(feature = "observability-install")]
pub fn with_default_buckets(
    builder: metrics_exporter_prometheus::PrometheusBuilder,
) -> Result<metrics_exporter_prometheus::PrometheusBuilder, InstallError> {
    use metrics_exporter_prometheus::Matcher;
    let mut builder = builder.set_buckets(DEFAULT_BUCKETS).map_err(bucket_error)?;
    for name in LONG_RUNNING_HISTOGRAMS {
        builder = builder
            .set_buckets_for_metric(Matcher::Full((*name).to_string()), LONG_BUCKETS)
            .map_err(bucket_error)?;
    }
    Ok(builder)
}

/// Expire counter and histogram series that saw no update for `timeout`
/// (`None` keeps them). Gauges are left out on purpose: staleness alerts read
/// gauges (`faucet_pipeline_last_bookmark_unix_seconds`, heartbeats) that must
/// stay while a pipeline is idle. An expired counter that is emitted again
/// restarts at zero, which `rate` / `increase` read as a reset.
///
/// The exporter prunes on render, so emit through the `metrics` macros on each
/// update: a metric handle kept across an expiry keeps writing to a series
/// that is never rendered again.
#[cfg(feature = "observability-install")]
pub fn with_idle_timeout(
    builder: metrics_exporter_prometheus::PrometheusBuilder,
    timeout: Option<std::time::Duration>,
) -> metrics_exporter_prometheus::PrometheusBuilder {
    use metrics_util::MetricKindMask;
    builder.idle_timeout(MetricKindMask::COUNTER | MetricKindMask::HISTOGRAM, timeout)
}

/// How often [`prune_idle_series`] renders: a quarter of the idle timeout,
/// never more often than once a minute.
#[cfg(feature = "observability-install")]
pub fn prune_interval(idle: std::time::Duration) -> std::time::Duration {
    (idle / 4).max(std::time::Duration::from_secs(60))
}

/// Render `handle` every [`prune_interval`] and discard the output, so expired
/// series are dropped from memory even when nothing scrapes `/metrics`. Runs
/// until the future is dropped.
#[cfg(feature = "observability-install")]
pub async fn prune_idle_series(
    handle: metrics_exporter_prometheus::PrometheusHandle,
    idle: std::time::Duration,
) {
    let mut tick = tokio::time::interval(prune_interval(idle));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    tick.tick().await;
    loop {
        tick.tick().await;
        drop(handle.render());
    }
}

/// Install `recorder` globally and run `exporter` (plus the pruning task when
/// `idle` is set) on the ambient runtime, or on a background thread's runtime
/// when there is none. Returns `false` when a recorder was already installed.
#[cfg(feature = "observability-install")]
fn install_built(
    recorder: metrics_exporter_prometheus::PrometheusRecorder,
    exporter: metrics_exporter_prometheus::ExporterFuture,
    idle: Option<std::time::Duration>,
    runtime: Option<tokio::runtime::Runtime>,
) -> Result<bool, InstallError> {
    let handle = recorder.handle();
    if metrics::set_global_recorder(recorder).is_err() {
        return Ok(false);
    }
    let work = async move {
        if let Some(idle) = idle {
            tokio::spawn(prune_idle_series(handle, idle));
        }
        let _ = exporter.await;
    };
    match runtime {
        None => {
            tokio::spawn(work);
        }
        Some(rt) => {
            std::thread::Builder::new()
                .name("faucet-prometheus".into())
                .spawn(move || rt.block_on(work))
                .map_err(|e| InstallError::PrometheusInstall(e.to_string()))?;
        }
    }
    Ok(true)
}

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

    let builder = with_buckets(PrometheusBuilder::new().with_http_listener(listen), p)?;

    if p.idle_timeout.is_some() {
        let builder = with_idle_timeout(builder, p.idle_timeout);
        let build = |b: PrometheusBuilder| {
            b.build().map_err(|e| match e {
                BuildError::FailedToCreateHTTPListener(msg) => InstallError::PrometheusBind {
                    listen: p.listen.clone(),
                    source: std::io::Error::other(msg),
                },
                other => InstallError::PrometheusInstall(other.to_string()),
            })
        };
        let installed = match tokio::runtime::Handle::try_current() {
            Ok(rt) => {
                let (recorder, exporter) = {
                    let _g = rt.enter();
                    build(builder)?
                };
                install_built(recorder, exporter, p.idle_timeout, None)?
            }
            Err(_) => {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|e| InstallError::PrometheusInstall(e.to_string()))?;
                let (recorder, exporter) = {
                    let _g = rt.enter();
                    build(builder)?
                };
                install_built(recorder, exporter, p.idle_timeout, Some(rt))?
            }
        };
        if installed {
            report.prometheus_listen = Some(p.listen.clone());
        } else {
            tracing::warn!("Prometheus recorder already installed; continuing");
            report.prometheus_already_installed = true;
        }
        return Ok(());
    }

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
    // build() returns (recorder, /metrics exporter future) WITHOUT setting the
    // global recorder, so we can fan it out.
    let (prom_recorder, prom_exporter) = with_idle_timeout(
        with_buckets(PrometheusBuilder::new().with_http_listener(listen), p)?,
        p.idle_timeout,
    )
    .build()
    .map_err(|e| match e {
        BuildError::FailedToCreateHTTPListener(msg) => InstallError::PrometheusBind {
            listen: p.listen.clone(),
            source: std::io::Error::other(msg),
        },
        other => InstallError::PrometheusInstall(other.to_string()),
    })?;

    let prune = p
        .idle_timeout
        .map(|idle| prune_idle_series(prom_recorder.handle(), idle));
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
                if let Some(prune) = prune {
                    tokio::spawn(prune);
                }
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
                if let Some(prune) = prune {
                    tokio::spawn(prune);
                }
            }
        }
    }
    Ok(())
}

/// Install, as the global metrics recorder, a fanout to `prometheus` — a
/// recorder the host built (e.g. `PrometheusBuilder::build_recorder`) and
/// renders through its own handle — and an OTLP metrics exporter built from
/// `otel`. For a host such as an HTTP server that serves `/metrics` itself and
/// so cannot use [`install_observability`]'s listener.
///
/// Returns `Ok(true)` when OTLP metrics are exported, `Ok(false)` when the
/// exporter could not be built (the Prometheus recorder is installed alone, so
/// `/metrics` keeps working), and an error when a global recorder is already
/// installed. The meter provider lives for the process and is flushed by
/// [`shutdown_otel`](crate::observability::otel::shutdown_otel).
#[cfg(all(feature = "observability-install", feature = "otel"))]
pub fn install_prometheus_with_otel_metrics(
    prometheus: metrics_exporter_prometheus::PrometheusRecorder,
    otel: &crate::observability::otel::OtelConfig,
) -> Result<bool, InstallError> {
    use metrics_util::layers::FanoutBuilder;
    let already =
        || InstallError::PrometheusInstall("a metrics recorder is already installed".into());
    match crate::observability::otel::build_meter_provider(otel) {
        Ok((mp, otel_recorder)) => {
            let fanout = FanoutBuilder::default()
                .add_recorder(prometheus)
                .add_recorder(otel_recorder)
                .build();
            metrics::set_global_recorder(fanout).map_err(|_| already())?;
            crate::observability::otel::keep_meter_provider(mp);
            crate::observability::otel::describe();
            Ok(true)
        }
        Err(e) => {
            tracing::warn!("OTLP metrics exporter init failed; Prometheus-only: {e}");
            metrics::set_global_recorder(prometheus).map_err(|_| already())?;
            Ok(false)
        }
    }
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

/// The version [`register_build_info`] reports, when an application set one.
static BUILD_VERSION: std::sync::OnceLock<&'static str> = std::sync::OnceLock::new();

/// Set the version the `faucet_build_info{version}` gauge reports — call it
/// with the **binary's** own `env!("CARGO_PKG_VERSION")` before installing
/// observability. Without it the label is `faucet-core`'s version, which
/// differs from the binary's whenever only the binary was released (#832).
///
/// First call wins (a process has one build); returns `false` when a version
/// was already set. Re-registers the gauge so a recorder installed before
/// this call reports the new version too, and sets the series it reported
/// until now to 0, so the recorder never holds two versions at 1.
pub fn set_build_version(version: &'static str) -> bool {
    let first = BUILD_VERSION.set(version).is_ok();
    if first {
        if version != env!("CARGO_PKG_VERSION") {
            #[cfg(feature = "time-zones")]
            metrics::gauge!(
                "faucet_build_info",
                "version" => env!("CARGO_PKG_VERSION"),
                "tzdata" => crate::time::TZDATA_VERSION,
            )
            .set(0.0);
            #[cfg(not(feature = "time-zones"))]
            metrics::gauge!("faucet_build_info", "version" => env!("CARGO_PKG_VERSION")).set(0.0);
        }
        register_build_info();
    }
    first
}

/// The version `faucet_build_info` reports: the one passed to
/// [`set_build_version`], else `faucet-core`'s `CARGO_PKG_VERSION`.
pub fn build_version() -> &'static str {
    BUILD_VERSION
        .get()
        .copied()
        .unwrap_or(env!("CARGO_PKG_VERSION"))
}

/// Register the `faucet_build_info{version}` gauge (set to 1) under the
/// currently-installed `metrics` recorder. Safe to call from any code path
/// that wants to ensure the gauge is set; `install_observability` invokes
/// this automatically. Gauges are naturally idempotent under the `metrics`
/// model — repeat calls just re-set the same value.
///
/// With the `time-zones` feature it also carries `tzdata`, the IANA release
/// named zones resolve with (#1116).
///
/// The version label is [`build_version`]: the application's version when it
/// called [`set_build_version`] (the `faucet` CLI does), else `faucet-core`'s.
/// Dashboards `group_left` the gauge onto every other metric to annotate
/// panels with the running version.
pub fn register_build_info() {
    #[cfg(feature = "time-zones")]
    metrics::gauge!(
        "faucet_build_info",
        "version" => build_version(),
        "tzdata" => crate::time::TZDATA_VERSION,
    )
    .set(1.0);
    #[cfg(not(feature = "time-zones"))]
    metrics::gauge!(
        "faucet_build_info",
        "version" => build_version(),
    )
    .set(1.0);
}

#[cfg(all(test, feature = "observability-install"))]
mod tests {
    #[cfg(feature = "observability-install")]
    #[test]
    fn rejected_buckets_become_an_install_error() {
        let err = metrics_exporter_prometheus::PrometheusBuilder::new()
            .set_buckets(&[])
            .map_err(bucket_error)
            .unwrap_err();
        assert!(matches!(err, InstallError::PrometheusInstall(_)), "{err:?}");
    }

    use super::*;
    use std::sync::Mutex;

    static LOCK: Mutex<()> = Mutex::new(());

    fn rendered(buckets: Option<Vec<f64>>) -> String {
        let p = PrometheusConfig {
            listen: "127.0.0.1:0".into(),
            buckets,
            idle_timeout: None,
        };
        let recorder = with_buckets(metrics_exporter_prometheus::PrometheusBuilder::new(), &p)
            .unwrap()
            .build_recorder();
        let handle = recorder.handle();
        metrics::with_local_recorder(&recorder, || {
            metrics::histogram!("faucet_pipeline_run_duration_seconds").record(2700.0);
            metrics::histogram!("faucet_source_page_duration_seconds").record(0.2);
        });
        handle.render()
    }

    #[test]
    fn idle_counters_and_histograms_expire_but_gauges_stay() {
        let recorder = with_idle_timeout(
            metrics_exporter_prometheus::PrometheusBuilder::new(),
            Some(std::time::Duration::from_millis(200)),
        )
        .build_recorder();
        let handle = recorder.handle();
        metrics::with_local_recorder(&recorder, || {
            metrics::counter!("idle_test_total").increment(1);
            metrics::histogram!("idle_test_seconds").record(1.0);
            metrics::gauge!("idle_test_gauge").set(1.0);
        });
        let before = handle.render();
        assert!(before.contains("idle_test_total"), "{before}");
        #[allow(clippy::disallowed_methods)] // waits past the idle timeout; no runtime here
        std::thread::sleep(std::time::Duration::from_millis(400));
        let after = handle.render();
        assert!(!after.contains("idle_test_total"), "{after}");
        assert!(!after.contains("idle_test_seconds"), "{after}");
        assert!(after.contains("idle_test_gauge 1"), "{after}");
        metrics::with_local_recorder(&recorder, || {
            metrics::counter!("idle_test_total").increment(1);
        });
        assert!(handle.render().contains("idle_test_total 1"));
    }

    #[test]
    fn install_with_an_idle_timeout_and_no_runtime_returns_ok() {
        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let cfg = ObservabilityConfig {
            prometheus: Some(PrometheusConfig {
                listen: "127.0.0.1:0".into(),
                buckets: None,
                idle_timeout: Some(std::time::Duration::from_secs(3600)),
            }),
            tracing: None,
            otel: None,
        };
        let report = install_observability(&cfg).expect("install");
        assert!(report.prometheus_listen.is_some() || report.prometheus_already_installed);
    }

    #[test]
    fn prune_interval_is_a_quarter_of_the_timeout_and_at_least_a_minute() {
        use std::time::Duration;
        assert_eq!(
            prune_interval(Duration::from_secs(86_400)),
            Duration::from_secs(21_600)
        );
        assert_eq!(
            prune_interval(Duration::from_secs(10)),
            Duration::from_secs(60)
        );
    }

    #[test]
    fn run_length_histograms_get_buckets_past_five_minutes() {
        let out = rendered(None);
        assert!(
            out.contains("faucet_pipeline_run_duration_seconds_bucket{le=\"3600\"} 1"),
            "{out}"
        );
        assert!(!out.contains("faucet_source_page_duration_seconds_bucket{le=\"3600\"}"));
        assert!(out.contains("faucet_source_page_duration_seconds_bucket{le=\"300\"} 1"));
        let custom = rendered(Some(vec![1.0, 10.0]));
        assert!(!custom.contains("le=\"3600\""), "{custom}");
        assert!(custom.contains("faucet_pipeline_run_duration_seconds_bucket{le=\"10\"} 0"));
    }

    #[cfg(feature = "otel")]
    #[test]
    fn otlp_outside_a_runtime_is_skipped_not_a_panic() {
        let cfg = ObservabilityConfig {
            otel: Some(crate::observability::otel::OtelConfig::default()),
            ..Default::default()
        };
        let report = install_observability(&cfg).expect("no runtime is not an error");
        assert!(report.otel_signals.is_empty());
        assert!(!report.prometheus_already_installed);
    }

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
                idle_timeout: None,
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
                idle_timeout: None,
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
