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
    crate::set_build_version();
    let obs_cfg = build_observability_config(cfg);
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

#[cfg(test)]
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
}
