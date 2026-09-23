//! `faucet run` — load a pipeline config, expand the matrix, execute every
//! invocation under bounded concurrency.

use crate::cli::{RunArgs, RunOutput};
use crate::config::PipelineConfig;
use crate::error::{CliError, CliResult};
use crate::executor::{ExecuteOptions, RunSummary, run_expanded};
use crate::expand::expand;
use chrono::{DateTime, Utc};
use serde::Serialize;

/// Parse the optional `--clock` override (RFC3339 or `YYYY-MM-DD`), or default
/// to process start. Returned as a UTC fixed-offset clock for `${now.*}`.
/// Shared with `faucet test` (the `--clock` flag and per-case `clock:` field).
pub(crate) fn resolve_run_clock(
    flag: Option<&str>,
) -> CliResult<chrono::DateTime<chrono::FixedOffset>> {
    use chrono::{DateTime, NaiveDate, TimeZone, Utc};
    match flag {
        None => Ok(Utc::now().fixed_offset()),
        Some(s) => {
            if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
                return Ok(dt);
            }
            if let Ok(d) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                let ndt = d.and_hms_opt(0, 0, 0).expect("00:00:00 is valid");
                return Ok(Utc.from_utc_datetime(&ndt).fixed_offset());
            }
            Err(CliError::Config(format!(
                "--clock '{s}' is not RFC3339 (2026-01-31T00:00:00Z) or a date (2026-01-31)"
            )))
        }
    }
}

/// Drive the run future under the inline progress line when a recorder handle
/// is present (interactive terminal, not `--quiet`/`--tui`), else await it
/// plainly. Keeps the two summary call sites in `run` free of nested cfgs.
#[cfg(feature = "cli-progress")]
async fn drive_progress_or_plain<T>(
    run: impl Future<Output = T>,
    pipeline: &str,
    handle: Option<metrics_exporter_prometheus::PrometheusHandle>,
) -> T {
    match handle {
        Some(h) => crate::progress::drive(run, pipeline, h).await,
        None => run.await,
    }
}

/// Execute the `run` subcommand.
pub async fn run(args: RunArgs) -> CliResult<()> {
    // Rejected up front rather than silently clamped: `--concurrency 0` reads
    // as "unlimited" under the house `0` sentinel but would mean "no
    // connections" here, and the two readings are far enough apart that
    // guessing would be wrong either way (#610).
    if args.concurrency == Some(0) {
        return Err(CliError::Config(
            "--concurrency must be greater than 0 (it is a connection/fetch count,              not a `0 = unlimited` sentinel)"
                .into(),
        ));
    }
    let cwd = std::env::current_dir()?;
    let env_path =
        crate::env_loader::resolve_env_file(args.env_file.as_deref(), args.no_env_file, &cwd)?;
    crate::env_loader::load_env_file_if_present(env_path.as_deref())?;

    // Template Hub (#571): `--source X --sink Y` composes two hub templates
    // into a config document and runs it through the identical path a file
    // would take — params binding, secret resolution, expand, execute.
    // (One `execute` await below, not an early return here: the run future is
    // large, and awaiting it twice doubles the frame — enough to overflow a
    // test thread's stack.)
    let hub_pair = match (&args.source, &args.sink) {
        (Some(s), Some(k)) => Some((s.clone(), k.clone())),
        _ => None,
    };

    let resolved_config_path: Option<std::path::PathBuf> = if args.from_env || hub_pair.is_some() {
        None
    } else {
        Some(match args.config.as_ref() {
            Some(p) => p.clone(),
            None => {
                crate::env_loader::discover_config_path(&cwd).ok_or(CliError::NoConfigOrFromEnv)?
            }
        })
    };

    let cfg = if let Some((source, sink)) = hub_pair {
        let hub_dir = crate::hub::resolve_hub(args.hub.as_deref()).await?;
        let composition = crate::hub::compose_locators(&source, &sink, &hub_dir)?;
        let inputs = crate::config::RunInputs {
            params: crate::params::collect_cli_params(&args.param)?,
            env: crate::params::collect_env_overrides(&args.param_env)?
                .into_iter()
                .collect(),
            mode: crate::params::BindMode::Strict,
        };
        // Heap-pin the secret-resolving load so the hub branch adds no stack
        // to the run future.
        Box::pin(crate::hub::load_composed_async(&composition, &inputs)).await?
    } else if args.from_env {
        if args.profile.is_some() {
            tracing::warn!(
                "--profile / FAUCET_PROFILE has no effect in --from-env mode (no config file to compose); ignoring"
            );
        }
        if !args.param.is_empty() || !args.param_env.is_empty() {
            return Err(CliError::Config(
                "--param / --param-env have no effect in --from-env mode: the `params:` block \
                 lives in a config file, and every value already comes from the environment"
                    .into(),
            ));
        }
        crate::env_config::from_process_env()?
    } else {
        // Typed run params (#444): `--param name=value` / `--param-env NAME[=V]`
        // are bound before the typed parse, so `${param.*}` never reaches a
        // connector. A config with no `params:` block is unaffected.
        let inputs = crate::config::RunInputs {
            params: crate::params::collect_cli_params(&args.param)?,
            env: crate::params::collect_env_overrides(&args.param_env)?
                .into_iter()
                .collect(),
            mode: crate::params::BindMode::Strict,
        };
        let path = resolved_config_path
            .as_ref()
            .expect("YAML mode always resolves a path above");
        // A hub template handed to `run` directly would fail on `kind:` as an
        // unknown field; say what it is and how to run it instead.
        if let Some(kind) = crate::hub::detect_kind_in_file(path).filter(|k| k.is_hub()) {
            return Err(CliError::Config(format!(
                "{} is a hub {} — compose it: `faucet run --source <source-template> --sink <sink-template>`",
                path.display(),
                kind.as_str()
            )));
        }
        PipelineConfig::from_path_async_with(path, args.profile.as_deref(), &inputs).await?
    };

    execute(cfg, args, resolved_config_path).await
}

/// Execute an already-loaded config: install observability, build the auth
/// catalog, expand + select rows, run, and report.
///
/// Split out of [`run`] so a caller that obtains its config some other way runs
/// through the *identical* path — `faucet template run` materializes a
/// registered template and hands it straight here, rather than re-implementing
/// (and inevitably under-implementing) the lineage / notification / catalog /
/// SLA / progress wiring below.
pub(crate) async fn execute(
    cfg: PipelineConfig,
    args: RunArgs,
    resolved_config_path: Option<std::path::PathBuf>,
) -> CliResult<()> {
    #[cfg(not(feature = "cli-tui"))]
    if args.tui {
        return Err(CliError::Config(
            "--tui requires a binary built with the `cli-tui` feature \
             (e.g. `cargo install faucet-cli --features cli-tui`)"
                .into(),
        ));
    }
    #[cfg(feature = "cli-tui")]
    let tui_active = crate::tui::is_tui_session(args.tui);
    #[cfg(not(feature = "cli-tui"))]
    let tui_active = false;

    // Exactly one observability install. A live view (the full-screen `--tui`
    // or the inline `--progress` line) owns the Prometheus recorder so it can
    // render the recorder's output; otherwise the standard install runs. The
    // TUI supersedes the inline line when both are eligible.
    #[cfg_attr(
        not(any(feature = "cli-tui", feature = "cli-progress")),
        allow(unused_mut)
    )]
    let mut live_view_owns_recorder = false;

    #[cfg(feature = "cli-tui")]
    let tui_handle = if tui_active {
        let h = crate::tui::setup_observability(&cfg)?;
        live_view_owns_recorder = true;
        Some(h)
    } else {
        if args.tui {
            tracing::info!("--tui: stdout is not a terminal; running without the TUI");
        }
        None
    };

    // Inline progress line (#385): only when the TUI isn't taking over, stdout
    // is a terminal, and the operator did not pass `--quiet`. On a non-TTY /
    // `--quiet` this is `None` and the run falls back to periodic log lines.
    #[cfg(feature = "cli-progress")]
    let progress_handle =
        if !tui_active && crate::progress::is_progress_session(args.quiet, args.tui) {
            let h = crate::livemetrics::setup_observability(&cfg)?;
            live_view_owns_recorder = true;
            Some(h)
        } else {
            None
        };

    if !live_view_owns_recorder {
        crate::obs::install(&cfg)?;
    }

    let pipeline_name = cfg.name.clone().unwrap_or_else(|| {
        resolved_config_path
            .as_ref()
            .and_then(|p| p.file_stem())
            .and_then(|s| s.to_str())
            .unwrap_or("pipeline")
            .to_owned()
    });

    let auth = crate::auth_catalog::build_auth_catalog(cfg.auth.as_ref())?;

    // Topology mode (#71/#72): an explicit `pipeline.nodes` graph replaces the
    // matrix entirely. Run it directly and report through the same summary
    // surfaces (`--output text|json|ndjson`), then return.
    if crate::topology::is_topology(&cfg) {
        let started_at = Utc::now();
        let summary = crate::topology::run_topology(
            &cfg,
            &auth,
            crate::topology::TopologyRunOptions {
                cancel: None,
                dry_run: args.dry_run,
                limit: args.limit,
                clock: Some(resolve_run_clock(args.clock.as_deref())?),
            },
        )
        .await?;
        let finished_at = Utc::now();
        return finish_topology_run(
            &pipeline_name,
            started_at,
            finished_at,
            &summary,
            args.output,
        );
    }

    #[cfg(feature = "lineage")]
    let lineage = crate::lineage_glue::build_emitter(cfg.lineage.as_ref())
        .map_err(|e| CliError::Config(format!("lineage: {e}")))?;
    let resilience = match &cfg.resilience {
        Some(spec) => Some(spec.to_policy()?),
        None => None,
    };
    #[cfg(feature = "notify")]
    let notifier = crate::notify::Notifier::from_specs(&cfg.notifications)?;
    #[cfg(feature = "catalog")]
    let catalog = match cfg.catalog.as_ref() {
        Some(spec) => Some(crate::catalog::connect_from_spec(spec).await?),
        None => None,
    };
    // Resolve discoverable partition bounds before planning (#479): `expand` is
    // synchronous and has no registry access, and it needs concrete bounds. A
    // config with no probes does no I/O here.
    let mut cfg = cfg;
    crate::partition::resolve_config_bounds(&mut cfg, &auth).await?;
    // Discovery-driven matrix fan-out (#647): a source with a discovery block
    // (`discovery`/`odata`) + `fan_out` discovers its datasets live and generates
    // the matrix before planning.
    crate::dynamic_fanout::resolve_dynamic_fanout(&mut cfg, &auth).await?;
    let nodes = expand(&cfg)?;
    // Capture the config-snapshot inputs (#374) before `nodes` / `catalog` are
    // moved into the executor; recorded after a fully-successful run below. The
    // snapshot represents the fully-resolved config (all rows) — runtime row
    // selection is a per-invocation concern, so it is captured pre-selection.
    #[cfg(feature = "catalog")]
    let snapshot_inputs = catalog
        .as_ref()
        .map(|handle| (handle.clone(), nodes.clone(), pipeline_name.clone()));
    // Runtime matrix-row selection (#370/#371/#376/#377): status gate → tag
    // narrowing → parent policy → skip. A plain config (no `status`/`tags`, no
    // selection flags) returns every row unchanged.
    let selection =
        crate::select::RunSelection::from_args(&args.selection, cfg.selection.as_ref())?;
    let nodes = crate::select::select_nodes(nodes, &selection, !cfg.matrix.is_empty())?;
    // The TUI wires `q` / Ctrl-C to this token: in-flight invocations stop at
    // their next page boundary and flush (#146 H16). Plain runs keep `None`.
    #[cfg(feature = "cli-tui")]
    let tui_cancel = tui_active.then(faucet_core::CancellationToken::new);
    #[cfg(not(feature = "cli-tui"))]
    let tui_cancel: Option<faucet_core::CancellationToken> = None;
    let started_at = Utc::now();
    let run_fut = run_expanded(
        nodes,
        ExecuteOptions {
            pipeline_name: pipeline_name.clone(),
            run_id: None,
            execution: cfg.execution.clone(),
            concurrency: args.concurrency,
            dry_run: args.dry_run,
            limit: args.limit,
            state_path_override: args.state_path.clone(),
            shard: None,
            auth,
            clock: resolve_run_clock(args.clock.as_deref())?,
            // Plain runs have no external cancel signal (the executor still
            // cooperatively cancels in-flight rows on `on_error: stop`); a
            // TUI session cancels via `q` / Ctrl-C.
            cancel: tui_cancel.clone(),
            resilience,
            sla: cfg.sla.clone(),
            reconcile: cfg.reconcile.clone(),
            #[cfg(feature = "lineage")]
            lineage,
            #[cfg(feature = "lineage")]
            lineage_cfg: cfg.lineage.clone(),
            #[cfg(feature = "notify")]
            notifier,
            #[cfg(feature = "catalog")]
            catalog,
        },
    );
    #[cfg(feature = "cli-tui")]
    let summary = match (tui_handle, tui_cancel) {
        (Some(handle), Some(cancel)) => {
            let result = crate::tui::drive(run_fut, &pipeline_name, handle, cancel).await;
            if result
                .as_ref()
                .map(|s| s.failure_count() > 0)
                .unwrap_or(true)
            {
                // The failure context lived on the alternate screen — replay
                // the tail of the log ring to stderr now that it's gone.
                crate::tui::flush_logs_to_stderr(25);
            }
            result?
        }
        _ => {
            #[cfg(feature = "cli-progress")]
            let s = drive_progress_or_plain(run_fut, &pipeline_name, progress_handle).await?;
            #[cfg(not(feature = "cli-progress"))]
            let s = run_fut.await?;
            s
        }
    };
    #[cfg(not(feature = "cli-tui"))]
    let summary = {
        #[cfg(feature = "cli-progress")]
        let s = drive_progress_or_plain(run_fut, &pipeline_name, progress_handle).await?;
        #[cfg(not(feature = "cli-progress"))]
        let s = run_fut.await?;
        s
    };

    let finished_at = Utc::now();
    let total_written: usize = summary.invocations.iter().map(|i| i.records_written).sum();
    let success = summary
        .invocations
        .iter()
        .filter(|i| i.error.is_none())
        .count();
    let failed = summary.failure_count();

    // Record the resolved config snapshot for `faucet plan --diff` (best-effort;
    // #374 / #279) — only on a fully-successful run.
    #[cfg(feature = "catalog")]
    if let Some((handle, snap_nodes, name)) = snapshot_inputs {
        crate::catalog::snapshot::record_if_ok(
            Some(&handle),
            &name,
            crate::catalog::snapshot::on_error_str(&cfg.execution),
            &snap_nodes,
            failed == 0,
            chrono::Utc::now(),
        )
        .await;
    }

    tracing::info!(
        pipeline = %pipeline_name,
        invocations = summary.invocations.len(),
        succeeded = success,
        failed,
        records_written = total_written,
        "pipeline completed"
    );
    // Reliable process-level peak RSS (#631). For a one-shot `faucet run` this
    // process runs exactly this sync, so it is the sync's true memory high-water
    // mark (not the in-flight serialized proxy). Emitted as a metric always;
    // printed after the summary for text output.
    let peak_rss = crate::memstat::process_peak_rss_bytes();
    if let Some(bytes) = peak_rss {
        metrics::describe_gauge!(
            "faucet_process_peak_rss_bytes",
            "Peak resident set size of the faucet process (getrusage high-water mark, bytes). \
             Process-scoped: equals the run's peak only for one-shot `faucet run`."
        );
        metrics::gauge!("faucet_process_peak_rss_bytes").set(bytes as f64);
    }
    // End-of-run summary. `text` is the human line (default); `json` / `ndjson`
    // emit a machine-readable summary and keep stdout otherwise clean so
    // `faucet run` is scriptable in CI / cron / Slack (#390). Logs are on stderr.
    match args.output {
        // Human status → stderr, so stdout belongs exclusively to the sink /
        // the machine-readable json|ndjson contract (#424). Piping a
        // stdout-sink run stays clean.
        // Under `--log-format json` every stderr line must be one JSON object,
        // or a strict consumer chokes on the human status block. The same
        // numbers already left as the structured `pipeline completed` event
        // above, so nothing is lost — and text mode is untouched (#634).
        RunOutput::Text if crate::cli::log_format() == crate::cli::LogFormat::Json => {
            for i in &summary.invocations {
                tracing::info!(
                    pipeline = %pipeline_name,
                    row = %i.row_id,
                    records_written = i.records_written,
                    duration_ms = i.metrics.as_ref().map(|m| m.duration_ms).unwrap_or(0),
                    failed = i.error.is_some(),
                    "row completed"
                );
            }
        }
        RunOutput::Text => {
            eprintln!(
                "{}: {} invocation{}, {} ok, {} failed, wrote {} record{}",
                pipeline_name,
                summary.invocations.len(),
                if summary.invocations.len() == 1 {
                    ""
                } else {
                    "s"
                },
                success,
                failed,
                total_written,
                if total_written == 1 { "" } else { "s" }
            );
            // Per-row timing breakdown (slowest first) — the wall-clock each
            // matrix row's work took. Only for multi-row runs, where the
            // scheduling tail matters; a single invocation adds no signal.
            if summary.invocations.len() > 1 {
                let mut rows: Vec<(u64, &str, usize)> = summary
                    .invocations
                    .iter()
                    .map(|i| {
                        (
                            i.metrics.as_ref().map(|m| m.duration_ms).unwrap_or(0),
                            i.row_id.as_str(),
                            i.records_written,
                        )
                    })
                    .collect();
                rows.sort_by_key(|r| std::cmp::Reverse(r.0));
                eprintln!("  per-row timing (slowest first):");
                for (ms, row, recs) in rows {
                    eprintln!(
                        "    {:<30} {:>8.1}s  {:>12} record{}",
                        row,
                        ms as f64 / 1000.0,
                        recs,
                        if recs == 1 { "" } else { "s" }
                    );
                }
            }
        }
        RunOutput::Json => {
            let doc = summary_document(&pipeline_name, started_at, finished_at, &summary);
            let rendered = serde_json::to_string_pretty(&doc).unwrap_or_else(|_| "{}".to_string());
            // Belt-and-suspenders: scrub any resolved secret that reached an
            // error string before it hits stdout (#390 secret-redaction AC).
            println!("{}", crate::secrets::registry::redact(&rendered));
        }
        RunOutput::Ndjson => {
            for row in summary_rows(&summary) {
                let line = serde_json::to_string(&row).unwrap_or_else(|_| "{}".to_string());
                println!("{}", crate::secrets::registry::redact(&line));
            }
        }
    }

    if matches!(args.output, RunOutput::Text)
        && crate::cli::log_format() == crate::cli::LogFormat::Text
        && let Some(bytes) = peak_rss
    {
        eprintln!(
            "  peak process RSS {} (whole process; exact for a one-shot run)",
            crate::memstat::fmt_mb(bytes)
        );
    }

    // Under JSON the same number travels as a field rather than a prose line,
    // so nothing is lost and every stderr line stays one object (#634).
    if crate::cli::log_format() == crate::cli::LogFormat::Json
        && let Some(bytes) = peak_rss
    {
        tracing::info!(
            pipeline = %pipeline_name,
            peak_rss_bytes = bytes,
            "process peak rss"
        );
    }

    // Flush any buffered OTLP telemetry before the process exits (no-op without
    // the `otel` feature). Done on both the success and failure exit paths.
    faucet_core::shutdown_otel();

    if summary.had_failures() {
        return Err(CliError::PipelineHadFailures { count: failed });
    }
    Ok(())
}

/// One matrix row's line in a `--output json`/`ndjson` summary (#390). Every
/// field is a counter the pipeline already maintains; `rows_in` is `null` when
/// input sampling was not active (no `lineage:` / `catalog:` block).
#[derive(Debug, Serialize)]
pub(crate) struct RunRowSummary {
    pub row_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_key: Option<String>,
    pub source: String,
    pub sink: String,
    pub status: &'static str,
    pub rows_in: Option<u64>,
    pub rows_out: u64,
    pub duration_ms: u64,
    pub dlq_count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bookmark: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Aggregate counters across every row.
#[derive(Debug, Serialize)]
pub(crate) struct RunTotals {
    pub rows: usize,
    pub rows_out: u64,
    pub dlq_count: u64,
    pub ok: usize,
    pub failed: usize,
}

/// The full `--output json` document.
#[derive(Debug, Serialize)]
pub(crate) struct RunSummaryDocument {
    pub pipeline: String,
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    pub status: &'static str,
    pub totals: RunTotals,
    pub rows: Vec<RunRowSummary>,
}

/// Project one invocation outcome into its summary row.
pub(crate) fn summary_rows(summary: &RunSummary) -> Vec<RunRowSummary> {
    summary
        .invocations
        .iter()
        .map(|o| {
            let m = o.metrics.clone().unwrap_or_default();
            RunRowSummary {
                row_id: o.row_id.clone(),
                parent_key: o.parent_record_key.clone(),
                source: m.source_kind,
                sink: m.sink_kind,
                status: if o.error.is_some() { "failed" } else { "ok" },
                rows_in: m.records_read,
                rows_out: o.records_written as u64,
                duration_ms: m.duration_ms,
                dlq_count: m.dlq_count,
                bookmark: m.bookmark,
                error: o.error.clone(),
            }
        })
        .collect()
}

/// Build the top-level `--output json` document from a run summary.
pub(crate) fn summary_document(
    pipeline: &str,
    started_at: DateTime<Utc>,
    finished_at: DateTime<Utc>,
    summary: &RunSummary,
) -> RunSummaryDocument {
    let rows = summary_rows(summary);
    let failed = rows.iter().filter(|r| r.status == "failed").count();
    let totals = RunTotals {
        rows: rows.len(),
        rows_out: rows.iter().map(|r| r.rows_out).sum(),
        dlq_count: rows.iter().map(|r| r.dlq_count).sum(),
        ok: rows.len() - failed,
        failed,
    };
    RunSummaryDocument {
        pipeline: pipeline.to_string(),
        started_at,
        finished_at,
        status: if failed > 0 { "failed" } else { "ok" },
        totals,
        rows,
    }
}

/// Report a topology-mode run through the same `--output` surfaces as a matrix
/// run, then map any node failures to the process exit code (#71/#72).
fn finish_topology_run(
    pipeline_name: &str,
    started_at: DateTime<Utc>,
    finished_at: DateTime<Utc>,
    summary: &RunSummary,
    output: RunOutput,
) -> CliResult<()> {
    let total_written: usize = summary.invocations.iter().map(|i| i.records_written).sum();
    let failed = summary.failure_count();
    let success = summary.invocations.len() - failed;

    tracing::info!(
        pipeline = %pipeline_name,
        nodes = summary.invocations.len(),
        succeeded = success,
        failed,
        records_written = total_written,
        "topology completed"
    );

    match output {
        // Under JSON the `topology completed` event above already carries these
        // numbers as fields; emitting the prose line too would break a strict
        // stderr consumer (#634).
        RunOutput::Text if crate::cli::log_format() == crate::cli::LogFormat::Json => {}
        // Human status → stderr; stdout stays clean for the sink / json|ndjson (#424).
        RunOutput::Text => eprintln!(
            "{}: {} sink node{}, {} ok, {} failed, wrote {} record{}",
            pipeline_name,
            summary.invocations.len(),
            if summary.invocations.len() == 1 {
                ""
            } else {
                "s"
            },
            success,
            failed,
            total_written,
            if total_written == 1 { "" } else { "s" }
        ),
        RunOutput::Json => {
            let doc = summary_document(pipeline_name, started_at, finished_at, summary);
            let rendered = serde_json::to_string_pretty(&doc).unwrap_or_else(|_| "{}".to_string());
            println!("{}", crate::secrets::registry::redact(&rendered));
        }
        RunOutput::Ndjson => {
            for row in summary_rows(summary) {
                let line = serde_json::to_string(&row).unwrap_or_else(|_| "{}".to_string());
                println!("{}", crate::secrets::registry::redact(&line));
            }
        }
    }

    faucet_core::shutdown_otel();

    if summary.had_failures() {
        return Err(CliError::TopologyHadFailures { count: failed });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::{InvocationMetrics, InvocationOutcome};

    fn outcome(id: &str, written: usize, err: Option<&str>) -> InvocationOutcome {
        InvocationOutcome {
            row_id: id.into(),
            parent_record_key: None,
            records_written: written,
            error: err.map(|s| s.to_string()),
            error_kind: None,
            metrics: Some(InvocationMetrics {
                source_kind: "rest".into(),
                sink_kind: "jsonl".into(),
                duration_ms: 12,
                records_read: Some(written as u64),
                dlq_count: 0,
                bookmark: None,
            }),
        }
    }

    #[test]
    fn summary_document_aggregates_rows_and_status() {
        let summary = RunSummary {
            invocations: vec![outcome("a", 3, None), outcome("b", 0, Some("boom"))],
        };
        let now = Utc::now();
        let doc = summary_document("demo", now, now, &summary);
        assert_eq!(doc.status, "failed");
        assert_eq!(doc.totals.rows, 2);
        assert_eq!(doc.totals.rows_out, 3);
        assert_eq!(doc.totals.ok, 1);
        assert_eq!(doc.totals.failed, 1);
        assert_eq!(doc.rows[0].source, "rest");
        assert_eq!(doc.rows[0].rows_in, Some(3));
        assert_eq!(doc.rows[1].status, "failed");
        assert_eq!(doc.rows[1].error.as_deref(), Some("boom"));
        // Serializes cleanly.
        let json = serde_json::to_string(&doc).unwrap();
        assert!(json.contains("\"pipeline\":\"demo\""), "{json}");
    }

    #[test]
    fn all_ok_run_reports_ok_status() {
        let summary = RunSummary {
            invocations: vec![outcome("only", 5, None)],
        };
        let now = Utc::now();
        let doc = summary_document("p", now, now, &summary);
        assert_eq!(doc.status, "ok");
        assert_eq!(doc.totals.failed, 0);
    }

    #[cfg(feature = "cli-progress")]
    #[tokio::test]
    async fn drive_progress_or_plain_without_handle_just_awaits() {
        // No recorder handle (non-TTY / --quiet) → the future is awaited plainly.
        let out = super::drive_progress_or_plain(async { 7_usize }, "p", None).await;
        assert_eq!(out, 7);
    }

    #[test]
    fn run_clock_parses_rfc3339_date_and_defaults() {
        // RFC3339
        let c = resolve_run_clock(Some("2026-01-31T12:00:00Z")).unwrap();
        assert_eq!(c.format("%Y-%m-%d %H:%M").to_string(), "2026-01-31 12:00");
        // date-only → midnight UTC
        let c = resolve_run_clock(Some("2026-01-31")).unwrap();
        assert_eq!(c.format("%Y-%m-%d %H:%M").to_string(), "2026-01-31 00:00");
        // default = now (just assert it's Ok / recent year)
        let c = resolve_run_clock(None).unwrap();
        assert!(c.format("%Y").to_string().parse::<i32>().unwrap() >= 2025);
        // bad input errors
        assert!(resolve_run_clock(Some("not-a-date")).is_err());
    }
}
