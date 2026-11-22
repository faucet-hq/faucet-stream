//! Shared observability setup (Prometheus + tracing) used by `run` and
//! `schedule`. `install_observability` is idempotent, so calling this once per
//! process is safe even though `main.rs` already installed a basic subscriber.

use crate::config::PipelineConfig;
use crate::error::CliResult;
use faucet_core::{ObservabilityConfig, PrometheusConfig, TracingConfig, install_observability};

/// Resolve the effective tracing level using the documented precedence:
/// `FAUCET_LOG` > `RUST_LOG` > YAML `observability.tracing.level` > `None`.
/// (`cli_flag` is reserved for a future call-site that forwards `--log-level`.)
pub fn resolve_tracing_level(cli_flag: Option<&str>, yaml_level: Option<&str>) -> Option<String> {
    if let Some(l) = cli_flag {
        return Some(l.to_string());
    }
    if let Ok(l) = std::env::var("FAUCET_LOG")
        && !l.is_empty()
    {
        return Some(l);
    }
    if let Ok(l) = std::env::var("RUST_LOG")
        && !l.is_empty()
    {
        return Some(l);
    }
    yaml_level.map(|s| s.to_string())
}

/// Build the core `ObservabilityConfig` from the CLI config, including OTLP when
/// the `otel` feature is compiled in. Logs a one-shot warning if an `otel:` block
/// is present but the binary was built without `--features otel`.
pub fn build_observability_config(cfg: &PipelineConfig) -> ObservabilityConfig {
    build_observability_config_for(cfg, false)
}

/// The idle timeout long-running commands give counters and histograms when
/// the config does not set `observability.prometheus.idle_timeout_secs`.
pub const LONG_RUNNING_IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(86_400);

/// The series idle timeout from `idle_timeout_secs` (`0` = never), else the
/// long-running default.
pub fn idle_timeout(secs: Option<u64>, long_running: bool) -> Option<std::time::Duration> {
    match secs {
        Some(0) => None,
        Some(n) => Some(std::time::Duration::from_secs(n)),
        None => long_running.then_some(LONG_RUNNING_IDLE_TIMEOUT),
    }
}

/// [`build_observability_config`] for a command that runs for a long time
/// (`long_running`: `faucet schedule`, `faucet mirror`), which expires idle
/// counter and histogram series by default.
pub fn build_observability_config_for(
    cfg: &PipelineConfig,
    long_running: bool,
) -> ObservabilityConfig {
    let level = resolve_tracing_level(
        None,
        cfg.observability
            .as_ref()
            .and_then(|o| o.tracing.as_ref())
            .and_then(|t| t.level.as_deref()),
    );
    // `mut` is only needed when the `otel` feature populates `obs.otel` below.
    #[cfg_attr(not(feature = "otel"), allow(unused_mut))]
    let mut obs = ObservabilityConfig {
        prometheus: cfg
            .observability
            .as_ref()
            .and_then(|o| o.prometheus.as_ref())
            .map(|p| PrometheusConfig {
                listen: p.listen.clone(),
                buckets: p.buckets.clone(),
                idle_timeout: idle_timeout(p.idle_timeout_secs, long_running),
            }),
        tracing: level.map(|l| TracingConfig { level: l }),
        ..Default::default()
    };

    let otel_present = cfg
        .observability
        .as_ref()
        .and_then(|o| o.otel.as_ref())
        .is_some();
    #[cfg(feature = "otel")]
    {
        if let Some(spec) = cfg.observability.as_ref().and_then(|o| o.otel.as_ref()) {
            match spec.to_core() {
                Ok(c) => obs.otel = Some(c),
                Err(e) => tracing::warn!("ignoring invalid otel config: {e}"),
            }
        }
    }
    #[cfg(not(feature = "otel"))]
    {
        if otel_present {
            tracing::warn!(
                "observability.otel is configured but this binary was built without --features otel; OTLP export is disabled"
            );
        }
    }
    let _ = otel_present;
    obs
}

/// Install Prometheus + tracing from the config's `observability:` block. Logs
/// (does not fail) when a recorder/subscriber is already installed.
pub fn install(cfg: &PipelineConfig) -> CliResult<()> {
    install_for(cfg, false)
}

/// [`install`] for a long-running command (see
/// [`build_observability_config_for`]).
pub fn install_for(cfg: &PipelineConfig, long_running: bool) -> CliResult<()> {
    crate::set_build_version();
    let obs_cfg = build_observability_config_for(cfg, long_running);
    let report = install_observability(&obs_cfg)?;
    if let Some(addr) = report.prometheus_listen.as_deref() {
        tracing::info!("Prometheus /metrics listening on {addr}");
    }
    if report.prometheus_already_installed {
        tracing::warn!(
            "Prometheus recorder already installed; metrics route through the existing recorder"
        );
    }
    if report.tracing_already_installed {
        tracing::warn!(
            "tracing subscriber already installed; logs route through the existing subscriber"
        );
    }
    if report.otel_installed {
        tracing::info!("OTLP export enabled: {}", report.otel_signals.join(", "));
    }
    Ok(())
}

/// Flushes buffered OTLP telemetry when dropped, so a command that returns
/// early with an error still exports the traces and metrics of the run that
/// failed (no-op without the `otel` feature). Hold it for the command's whole
/// body: `let _otel = obs::flush_on_exit();`.
#[must_use = "telemetry is flushed when the guard drops; bind it to a variable"]
pub struct FlushOnExit(());

/// A [`FlushOnExit`] guard.
pub fn flush_on_exit() -> FlushOnExit {
    FlushOnExit(())
}

#[cfg(test)]
thread_local! {
    /// Guards dropped on this thread (tests count their own flushes).
    pub(crate) static FLUSHES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

impl Drop for FlushOnExit {
    fn drop(&mut self) {
        #[cfg(test)]
        FLUSHES.with(|n| n.set(n.get() + 1));
        faucet_core::shutdown_otel();
    }
}

#[cfg(test)]
#[allow(unsafe_code)] // tests set process environment variables
mod tests {
    use super::*;
    use std::sync::Mutex;
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn with_clean_env<F: FnOnce()>(f: F) {
        let _g = ENV_LOCK.lock().unwrap();
        unsafe {
            std::env::remove_var("FAUCET_LOG");
            std::env::remove_var("RUST_LOG");
        }
        f();
    }

    #[test]
    #[serial_test::serial(env)]
    fn cli_flag_beats_env_and_yaml() {
        with_clean_env(|| {
            unsafe {
                std::env::set_var("FAUCET_LOG", "debug");
                std::env::set_var("RUST_LOG", "trace");
            }
            assert_eq!(
                resolve_tracing_level(Some("error"), Some("info")).as_deref(),
                Some("error")
            );
        });
    }

    #[test]
    #[serial_test::serial(env)]
    fn faucet_log_beats_rust_log_and_yaml() {
        with_clean_env(|| {
            unsafe {
                std::env::set_var("FAUCET_LOG", "debug");
                std::env::set_var("RUST_LOG", "trace");
            }
            assert_eq!(
                resolve_tracing_level(None, Some("info")).as_deref(),
                Some("debug")
            );
        });
    }

    #[test]
    #[serial_test::serial(env)]
    fn rust_log_beats_yaml() {
        with_clean_env(|| {
            unsafe {
                std::env::set_var("RUST_LOG", "trace");
            }
            assert_eq!(
                resolve_tracing_level(None, Some("info")).as_deref(),
                Some("trace")
            );
        });
    }

    #[test]
    fn yaml_used_when_no_flag_or_env() {
        with_clean_env(|| {
            assert_eq!(
                resolve_tracing_level(None, Some("info")).as_deref(),
                Some("info")
            );
        });
    }

    #[test]
    fn none_returned_when_nothing_set() {
        with_clean_env(|| {
            assert_eq!(resolve_tracing_level(None, None), None);
        });
    }

    #[test]
    fn maps_otel_spec_into_observability_config() {
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: { base_url: "http://x" } }
  sink: { type: stdout, config: {} }
observability:
  otel: { endpoint: "http://c:4317" }
"#;
        let cfg = crate::config::parse_with_extension(yaml, "yaml").unwrap();
        let obs = build_observability_config(&cfg);
        #[cfg(feature = "otel")]
        assert!(obs.otel.is_some());
        let _ = obs;
    }

    #[test]
    fn idle_timeout_defaults_only_for_long_running_commands() {
        use std::time::Duration;
        assert_eq!(idle_timeout(None, false), None);
        assert_eq!(idle_timeout(None, true), Some(LONG_RUNNING_IDLE_TIMEOUT));
        assert_eq!(idle_timeout(Some(0), true), None);
        assert_eq!(idle_timeout(Some(60), false), Some(Duration::from_secs(60)));
    }

    #[test]
    fn maps_the_prometheus_idle_timeout() {
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: { base_url: "http://x" } }
  sink: { type: stdout, config: {} }
observability:
  prometheus: { listen: "127.0.0.1:0", idle_timeout_secs: 120 }
"#;
        let cfg = crate::config::parse_with_extension(yaml, "yaml").unwrap();
        let p = build_observability_config(&cfg).prometheus.unwrap();
        assert_eq!(p.idle_timeout, Some(std::time::Duration::from_secs(120)));
        let bare = yaml.replace(", idle_timeout_secs: 120", "");
        let cfg = crate::config::parse_with_extension(&bare, "yaml").unwrap();
        assert_eq!(
            build_observability_config(&cfg)
                .prometheus
                .unwrap()
                .idle_timeout,
            None
        );
        assert_eq!(
            build_observability_config_for(&cfg, true)
                .prometheus
                .unwrap()
                .idle_timeout,
            Some(LONG_RUNNING_IDLE_TIMEOUT)
        );
    }
}
