#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-cli
//!
//! A config-driven runner for [`faucet-stream`](https://docs.rs/faucet-stream)
//! pipelines.  Define a source, optional transforms, a sink, and (optionally)
//! a state store in a YAML or JSON file, then run it with the `faucet` binary —
//! no Rust code required.
//!
//! The library half of this crate exposes the same building blocks the binary
//! uses (config parsing, env interpolation, the connector registry) so that
//! integrations and tests can reuse them.

pub mod auth_catalog;
pub mod backfill;
#[cfg(feature = "catalog")]
pub mod catalog;
pub mod chunking;
pub mod cli;
pub mod commands;
pub mod compose;
pub mod config;
pub mod conformance;
pub mod discovery_matrix;
pub mod dlq_replay;
pub mod dynamic_fanout;
pub mod env_config;
pub mod env_loader;
pub mod error;
pub mod exec_metrics;
pub mod executor;
pub mod expand;
pub mod hub;
pub mod init_template;
pub mod interpolate;
#[cfg(feature = "lineage")]
pub mod lineage_glue;
/// Shared live-view metrics plumbing (recorder install + Prometheus-text
/// sampler), compiled when either live-view feature is on.
#[cfg(any(feature = "cli-tui", feature = "cli-progress"))]
pub mod livemetrics;
#[cfg(feature = "serve")]
pub mod local_outputs;
#[cfg(feature = "mcp")]
pub mod mcp;
pub mod memstat;
pub mod merge;
#[cfg(feature = "notify")]
pub mod notify;
pub mod obs;
pub mod params;
pub mod partition;
pub mod pipeline_test;
#[cfg(feature = "cli-progress")]
pub mod progress;
pub mod reconcile;
pub mod registry;
pub mod registry_index;
pub mod replication;
pub mod scaffold;
#[cfg(feature = "schedule")]
pub mod schedule;
pub mod schema_compose;
pub mod secrets;
pub mod select;
#[cfg(feature = "serve")]
pub mod serve;
pub mod sla;
pub mod state;
#[cfg(feature = "templates")]
pub mod templates;
pub mod topology;
pub mod transforms;
#[cfg(feature = "cli-tui")]
pub mod tui;

pub use error::{CliError, CliResult};

use crate::cli::{Cli, Command};
use crate::registry::PluginRegistry;

/// Entry point for a custom `faucet` binary that bundles third-party
/// connectors.
///
/// A custom-CLI author writes a tiny `main.rs` that builds a [`PluginRegistry`]
/// with their connectors registered on top of the built-ins and hands it here:
///
/// ```no_run
/// use faucet_cli::registry::PluginRegistry;
/// fn main() -> std::process::ExitCode {
///     faucet_cli::run_main(PluginRegistry::with_builtins())
/// }
/// ```
///
/// This installs `registry` as the process-global connector registry (so every
/// command — `run`, `validate`, `schema`, `list`, `preview`, `serve`, … — sees
/// the custom connectors), parses argv, installs the tracing subscriber, and
/// dispatches. The return value is the process exit code (the failed-probe /
/// failed-case / failed-unit count for `doctor` / `test` / `backfill`, `1` for
/// any other error, `0` on success). The stock `faucet` binary calls this with
/// `PluginRegistry::with_builtins()`.
pub fn run_main(registry: PluginRegistry) -> std::process::ExitCode {
    use clap::Parser;
    use std::process::ExitCode;

    if let Err(err) = registry.install() {
        commands::report(&err);
        return ExitCode::from(1);
    }

    // Hand `faucet-core` the secret scrubber before anything can produce output.
    // Core builds two things that leave the process and that it cannot redact on
    // its own: the DLQ envelope's `error.message`, and error text a caller
    // forwards on. Installed here, at the single entry point, so every subcommand
    // and runtime gets it (#456 H5).
    faucet_core::redact::install(Box::new(|s: &str| {
        secrets::registry::redact(s).into_owned()
    }));

    // Dynamic shell completion (#383): when the shell invokes us with the
    // `COMPLETE` env var set, compute and print candidates, then exit — before
    // any normal parsing/tracing/runtime setup. A no-op otherwise. The registry
    // is installed above so connector-kind candidates reflect the live binary.
    clap_complete::env::CompleteEnv::with_factory(<Cli as clap::CommandFactory>::command)
        .complete();

    let cli = Cli::parse();
    #[cfg(feature = "serve")]
    let is_serve = matches!(cli.command, Command::Serve(_));
    #[cfg(not(feature = "serve"))]
    let is_serve = false;
    // A `--tui` run on a real terminal routes logs into the TUI's in-memory
    // ring (the stdout subscriber would corrupt the alternate screen).
    #[cfg(feature = "cli-tui")]
    let is_tui = matches!(&cli.command, Command::Run(a) if tui::is_tui_session(a.tui));
    #[cfg(not(feature = "cli-tui"))]
    let is_tui = false;
    // `faucet mcp` (stdio) writes JSON-RPC on stdout, so its logs must go to
    // stderr — never the default subscriber.
    #[cfg(feature = "mcp")]
    let is_mcp = matches!(cli.command, Command::Mcp(_));
    #[cfg(not(feature = "mcp"))]
    let is_mcp = false;
    // `serve` installs its own (redacting, run-scoped) subscriber; every other
    // command uses the plain redacting fmt subscriber.
    if !is_serve && !is_tui && !is_mcp {
        crate::cli::set_log_format(cli.log_format);
        install_tracing(&cli.log_level, cli.log_format);
    }
    #[cfg(feature = "cli-tui")]
    if is_tui {
        tui::install_tui_tracing(&cli.log_level);
    }
    #[cfg(feature = "mcp")]
    if is_mcp {
        crate::cli::set_log_format(cli.log_format);
        mcp::install_stderr_tracing(&cli.log_level, cli.log_format);
    }

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("error: failed to start async runtime: {e}");
            return ExitCode::from(1);
        }
    };

    runtime.block_on(async move {
        match run_command(cli).await {
            Ok(()) => ExitCode::SUCCESS,
            // `doctor` / `test` / `backfill` already printed their report; the
            // exit code is the failed count (clamped to 255).
            Err(CliError::DoctorFailed { failed }) => ExitCode::from(failed.min(255) as u8),
            Err(CliError::TestsFailed { failed }) => ExitCode::from(failed.min(255) as u8),
            Err(CliError::BackfillFailed { failed }) => ExitCode::from(failed.min(255) as u8),
            Err(err) => {
                commands::report(&err);
                ExitCode::from(1)
            }
        }
    })
}

/// Dispatch a parsed [`Cli`] to the matching command. Public so custom hosts and
/// integration tests can drive the exact same code path as [`run_main`] with a
/// programmatically-built `Cli` (and a registry installed via
/// [`PluginRegistry::install`]).
pub async fn run_command(cli: Cli) -> CliResult<()> {
    #[cfg(feature = "serve")]
    let serve_log_level = cli.log_level.clone();
    let log_format = cli.log_format;
    match cli.command {
        Command::Run(args) => commands::run::run(args).await,
        Command::Backfill(args) => commands::backfill::run(args).await,
        Command::Replicate(args) => commands::replicate::run(args).await,
        Command::Discover(args) => commands::discover::run(args).await,
        Command::Validate(args) => commands::validate::run(args).await,
        Command::Schema(args) => commands::schema::run(args).await,
        Command::List(args) => commands::list::run(args).await,
        Command::Search(args) => commands::search::run(args).await,
        Command::Conformance(args) => commands::conformance::run(args).await,
        Command::Install(args) => commands::install::run(args).await,
        Command::Preview(args) => commands::preview::run(args).await,
        Command::Plan(args) => commands::plan::run(args).await,
        #[cfg(feature = "cli-dev")]
        Command::Dev(args) => commands::dev::run(args).await,
        Command::Init(args) => commands::init::run(args).await,
        Command::New(args) => commands::new::run(args).await,
        Command::Doctor(args) => commands::doctor::run(args).await,
        Command::Test(args) => commands::test::run(args).await,
        Command::Dlq(args) => commands::dlq::run(args).await,
        Command::Hub(args) => commands::hub::run(args).await,
        #[cfg(feature = "contract")]
        Command::Contract(args) => commands::contract::run(args).await,
        #[cfg(feature = "masking")]
        Command::Masking(args) => commands::masking::run(args).await,
        #[cfg(feature = "schedule")]
        Command::Schedule(args) => commands::schedule::run(args).await,
        #[cfg(feature = "serve")]
        Command::Serve(args) => commands::serve::run(args, serve_log_level, log_format).await,
        #[cfg(feature = "mcp")]
        Command::Mcp(args) => commands::mcp::run(args).await,
        #[cfg(feature = "notify")]
        Command::Notify(args) => commands::notify::run(args).await,
        #[cfg(feature = "catalog")]
        Command::Catalog(args) => commands::catalog::run(args).await,
        #[cfg(feature = "templates")]
        Command::Template(args) => commands::template::run(args).await,
        Command::Completions(args) => commands::completions::run(args.shell),
        Command::Migrate(args) => commands::migrate::run(args).await,
        Command::Fmt(args) => commands::fmt::run(args).await,
        Command::Explain(args) => commands::explain::run(args).await,
        #[cfg(feature = "catalog")]
        Command::History(args) => commands::history::run(args).await,
        #[cfg(feature = "catalog")]
        Command::Cleanup(args) => commands::cleanup::run(args).await,
    }
}

#[cfg(feature = "observability")]
fn install_tracing(level: &str, format: crate::cli::LogFormat) {
    use crate::secrets::registry::RedactingMakeWriter;
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_new(level).unwrap_or_else(|_| EnvFilter::new("info"));
    let builder = tracing_subscriber::fmt()
        .with_env_filter(filter)
        // Redaction wraps the *serialized bytes*, so it keeps working under
        // JSON: a resolved secret appearing in a field value is still scrubbed.
        .with_writer(RedactingMakeWriter);
    match format {
        crate::cli::LogFormat::Text => {
            let _ = builder.try_init();
        }
        crate::cli::LogFormat::Json => {
            let _ = builder
                .json()
                // Event fields at the top level rather than nested under
                // `fields`, which is what log pipelines index on.
                .flatten_event(true)
                // Span context (`pipeline`, `row`, `run_id`, …) as fields; the
                // full ancestor list is noise once the current span's fields
                // are present.
                .with_current_span(true)
                .with_span_list(false)
                .try_init();
        }
    }
}

/// Stub used when the `observability` feature is disabled. Logging falls back to
/// whatever the host environment has wired (or nothing).
#[cfg(not(feature = "observability"))]
fn install_tracing(_level: &str, _format: crate::cli::LogFormat) {}

/// Convenience entry point for integration tests and custom hosts: parse a
/// YAML config string, expand the matrix, and run all rows.
///
/// This skips the `install_observability` call that [`commands::run::run`]
/// performs — callers can wire their own `metrics` recorder / tracing
/// subscriber before calling this function (or not at all).
pub async fn run_from_yaml_str(yaml: &str) -> CliResult<executor::RunSummary> {
    // Parse first, then resolve ${env}/${file}/${secret} INTO the parsed tree
    // (post-parse) so a resolved value can never alter the document's structure
    // (F43) — mirroring the binary's `from_path` path.
    let mut value: serde_json::Value =
        serde_yaml::from_str(yaml).map_err(|e| CliError::ParseConfig {
            path: std::path::PathBuf::from("<yaml-string>"),
            message: e.to_string(),
        })?;
    interpolate::interpolate_value(&mut value)?;
    // Bind `${param.*}` from the config's own `params:` defaults (#444). No
    // caller-supplied values on this convenience path, so a required param is a
    // clear error rather than a token leaking into a connector config.
    params::bind_document(&mut value, &Default::default(), params::BindMode::Strict)?;
    let interpolated = serde_yaml::to_string(&value).map_err(|e| CliError::ParseConfig {
        path: std::path::PathBuf::from("<yaml-string>"),
        message: e.to_string(),
    })?;
    let mut cfg: config::PipelineConfig =
        serde_yaml::from_str(&interpolated).map_err(|e| CliError::ParseConfig {
            path: std::path::PathBuf::from("<yaml-string>"),
            message: e.to_string(),
        })?;
    if cfg.version != 1 {
        return Err(CliError::ParseConfig {
            path: std::path::PathBuf::from("<yaml-string>"),
            message: format!(
                "unsupported pipeline version {}, only version 1 is recognised",
                cfg.version
            ),
        });
    }
    crate::secrets::resolve_secrets(&mut cfg).await?;
    let pipeline_name = cfg.name.clone().unwrap_or_else(|| "unnamed".to_string());
    let auth = auth_catalog::build_auth_catalog(cfg.auth.as_ref())?;
    let resilience = match &cfg.resilience {
        Some(spec) => Some(spec.to_policy()?),
        None => None,
    };
    #[cfg(feature = "catalog")]
    let catalog = match cfg.catalog.as_ref() {
        Some(spec) => Some(catalog::connect_from_spec(spec).await?),
        None => None,
    };
    let nodes = expand::expand(&cfg)?;
    executor::run_expanded(
        nodes,
        executor::ExecuteOptions {
            pipeline_name,
            run_id: None,
            execution: cfg.execution.clone(),
            concurrency: None,
            dry_run: false,
            limit: None,
            state_path_override: None,
            shard: None,
            auth,
            clock: chrono::Utc::now().fixed_offset(),
            cancel: None,
            resilience,
            sla: cfg.sla.clone(),
            reconcile: cfg.reconcile.clone(),
            #[cfg(feature = "lineage")]
            lineage: None,
            #[cfg(feature = "lineage")]
            lineage_cfg: None,
            #[cfg(feature = "notify")]
            notifier: None,
            #[cfg(feature = "catalog")]
            catalog,
        },
    )
    .await
}
