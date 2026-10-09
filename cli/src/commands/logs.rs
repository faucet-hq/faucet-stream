//! `faucet logs ship` (#806): deliver the run logs `faucet run` / `faucet
//! schedule` left in a local spool — a cron job or sidecar for when the
//! collector was down while those processes ran.

use crate::cli::{LogsArgs, LogsCommand, LogsShipArgs};
use crate::config::PipelineConfig;
use crate::error::{CliError, CliResult};
use crate::logship::LogsSpec;
use crate::logship::otlp::OtlpLogExporter;
use crate::logship::spool::{PassReport, Spool};
use std::time::Duration;

pub async fn run(args: LogsArgs) -> CliResult<()> {
    match args.command {
        LogsCommand::Ship(a) => ship(a).await,
    }
}

/// The collector + buffer settings `faucet logs ship` uses: the config's
/// blocks, overridden by the flags.
pub fn resolve(
    cfg: Option<&PipelineConfig>,
    endpoint: Option<&str>,
    protocol: Option<&str>,
    spool: Option<&std::path::Path>,
) -> CliResult<(faucet_core::OtelConfig, LogsSpec)> {
    let obs = cfg.and_then(|c| c.observability.as_ref());
    let mut otel = match obs.and_then(|o| o.otel.as_ref()) {
        Some(o) => o.to_core().map_err(CliError::Config)?,
        None if endpoint.is_some() => faucet_core::OtelConfig::default(),
        None => {
            return Err(CliError::Config(
                "no collector: pass --endpoint, or a config with an `observability.otel` block"
                    .into(),
            ));
        }
    };
    if let Some(e) = endpoint {
        otel.endpoint = e.to_string();
    }
    if let Some(p) = protocol {
        otel.protocol = if p == "http" {
            faucet_core::OtelProtocol::Http
        } else {
            faucet_core::OtelProtocol::Grpc
        };
    }
    otel.validate().map_err(CliError::Config)?;
    let mut spec = obs.and_then(|o| o.logs.clone()).unwrap_or_default();
    if let Some(d) = spool {
        spec.spool_dir = Some(d.to_path_buf());
    }
    Ok((otel, spec))
}

async fn ship(args: LogsShipArgs) -> CliResult<()> {
    let cwd = std::env::current_dir()?;
    let env_path =
        crate::env_loader::resolve_env_file(args.env_file.as_deref(), args.no_env_file, &cwd)?;
    crate::env_loader::load_env_file_if_present(env_path.as_deref())?;
    let path = args
        .config
        .clone()
        .or_else(|| crate::env_loader::discover_config_path(&cwd));
    let cfg = match &path {
        Some(p) => Some(PipelineConfig::from_path_async(p, args.profile.as_deref()).await?),
        None => None,
    };
    let (otel, spec) = resolve(
        cfg.as_ref(),
        args.endpoint.as_deref(),
        args.protocol.as_deref(),
        args.spool.as_deref(),
    )?;
    let exporter = OtlpLogExporter::new(&otel).map_err(CliError::Config)?;
    let dir = spec.resolved_spool_dir();
    let spool = Spool::open(&dir)
        .map_err(|e| CliError::Config(format!("cannot open the spool {}: {e}", dir.display())))?;
    crate::logship::metrics::describe();
    let report = crate::logship::session::ship_spool(
        &spool,
        &exporter,
        &spec,
        Duration::from_secs(args.timeout_secs.max(1)),
    )
    .await;
    #[cfg(feature = "notify")]
    if let Some(c) = &cfg
        && let Ok(Some(n)) = crate::notify::Notifier::from_specs(&c.notifications)
    {
        let pipeline = c.name.clone().unwrap_or_default();
        crate::logship::session::notify_report(&n, &pipeline, &report).await;
    }
    if args.json {
        println!("{}", render_json(&report));
    } else {
        eprint!("{}", render_human(&dir, &report));
    }
    match report.undelivered() {
        0 => Ok(()),
        runs => Err(CliError::LogsUndelivered { runs }),
    }
}

/// The JSON report: one object per run.
pub fn render_json(report: &PassReport) -> String {
    let runs: Vec<serde_json::Value> = report
        .runs
        .iter()
        .map(|r| {
            serde_json::json!({
                "run_id": r.run_id,
                "pipeline": r.meta.attrs.get("pipeline"),
                "log_export": r.view,
                "locked_elsewhere": r.locked_elsewhere,
                "removed": r.removed,
            })
        })
        .collect();
    serde_json::json!({
        "shipped_lines": report.shipped,
        "undelivered_runs": report.undelivered(),
        "runs": runs,
    })
    .to_string()
}

/// The human report.
pub fn render_human(dir: &std::path::Path, report: &PassReport) -> String {
    let mut out = format!(
        "spool {}: shipped {} line{}, {} run{} still undelivered\n",
        dir.display(),
        crate::logship::record::group(report.shipped),
        if report.shipped == 1 { "" } else { "s" },
        report.undelivered(),
        if report.undelivered() == 1 { "" } else { "s" },
    );
    for r in report.runs.iter().filter(|r| !r.removed) {
        let pipeline = r.meta.attrs.get("pipeline").map(String::as_str).unwrap_or("-");
        let mut line = format!("  {} {:<20} {}", r.run_id, pipeline, r.view.status.as_str());
        if r.view.pending_lines > 0 {
            line.push_str(&format!(" ({} pending)", r.view.pending_lines));
        }
        if r.locked_elsewhere {
            line.push_str(" [being shipped by another process]");
        }
        if let Some(e) = &r.view.last_error {
            line.push_str(&format!(" — {e}"));
        }
        out.push_str(&line);
        out.push('\n');
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_needs_a_collector_and_applies_overrides() {
        assert!(resolve(None, None, None, None).is_err());
        let (o, s) = resolve(
            None,
            Some("http://c:4318"),
            Some("http"),
            Some(std::path::Path::new("/tmp/spool")),
        )
        .unwrap();
        assert_eq!(o.endpoint, "http://c:4318");
        assert_eq!(o.protocol, faucet_core::OtelProtocol::Http);
        assert_eq!(s.spool_dir.as_deref(), Some(std::path::Path::new("/tmp/spool")));
        let (o, _) = resolve(None, Some("http://c:4317"), Some("grpc"), None).unwrap();
        assert_eq!(o.protocol, faucet_core::OtelProtocol::Grpc);
        assert!(resolve(None, Some("not a url"), None, None).is_err());
        let yaml = "version: 1\npipeline:\n  source: { type: rest, config: { base_url: \"http://x\" } }\n  sink: { type: stdout, config: {} }\nobservability:\n  otel: { endpoint: \"http://col:4317\", export: [logs] }\n  logs: { spool_dir: /var/spool }\n";
        let cfg = crate::config::parse_with_extension(yaml, "yaml").unwrap();
        let (o, s) = resolve(Some(&cfg), None, None, None).unwrap();
        assert_eq!(o.endpoint, "http://col:4317");
        assert_eq!(s.spool_dir.as_deref(), Some(std::path::Path::new("/var/spool")));
    }

    #[test]
    fn reports_render() {
        let mut r = PassReport {
            shipped: 1,
            ..Default::default()
        };
        r.runs.push(crate::logship::spool::RunReport {
            run_id: "r1".into(),
            meta: Default::default(),
            view: crate::logship::LogExportView {
                pending_lines: 2,
                last_error: Some("down".into()),
                ..crate::logship::derive_view(true, &Default::default())
            },
            locked_elsewhere: true,
            removed: false,
            notify_failure: false,
            notify_drop: false,
        });
        let h = render_human(std::path::Path::new("/s"), &r);
        assert!(h.contains("shipped 1 line,"));
        assert!(h.contains("(2 pending)"));
        assert!(h.contains("another process"));
        assert!(h.contains("— down"));
        let j: serde_json::Value = serde_json::from_str(&render_json(&r)).unwrap();
        assert_eq!(j["undelivered_runs"], 1);
        assert_eq!(j["runs"][0]["run_id"], "r1");
    }
}
