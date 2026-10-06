//! `faucet preview` — run only the source side of the first root row and emit
//! the first N records to stdout as JSON Lines.
//!
//! Child rows can't be previewed in isolation in v1: they need parent records
//! to resolve `${parent.path}` tokens. Preview the parent first, then point
//! the child at a `${file:...}` fixture if you need to drive it standalone.

use crate::cli::PreviewArgs;
use crate::config::PipelineConfig;
use crate::error::{CliError, CliResult};
use crate::expand::{NodeRole, expand};
use crate::registry::build_source;
use crate::transforms::compile_transforms;
use faucet_core::stage::compile_stage;

#[cfg(feature = "sink-stdout")]
use faucet_core::{Pipeline, Sink};

/// Execute the `preview` subcommand.
#[cfg(feature = "sink-stdout")]
pub async fn run(args: PreviewArgs) -> CliResult<()> {
    use faucet_sink_stdout::{StdoutFormat, StdoutSink, StdoutSinkConfig};
    let cwd = std::env::current_dir()?;
    let env_path =
        crate::env_loader::resolve_env_file(args.env_file.as_deref(), args.no_env_file, &cwd)?;
    crate::env_loader::load_env_file_if_present(env_path.as_deref())?;
    let path = match args.config {
        Some(p) => p,
        None => crate::env_loader::discover_config_path(&cwd).ok_or(CliError::NoConfigOrFromEnv)?,
    };
    let cfg = PipelineConfig::from_path_async(&path, args.profile.as_deref()).await?;
    let auth = crate::auth_catalog::build_auth_catalog(cfg.auth.as_ref())?;

    // Topology mode (#71/#72): preview the source side of each source node.
    if crate::topology::is_topology(&cfg) {
        return crate::topology::preview(&cfg, &auth, args.limit).await;
    }

    let nodes = expand(&cfg)?;
    // Apply runtime row selection so `preview` previews the first root of the
    // selected run set (#370/#371/#376/#377).
    let selection =
        crate::select::RunSelection::from_args(&args.selection, cfg.selection.as_ref())?;
    let nodes = crate::select::select_nodes(nodes, &selection, !cfg.matrix.is_empty())?;
    let first_root = nodes
        .iter()
        .find(|n| matches!(n.role, NodeRole::Root))
        .ok_or_else(|| CliError::ParseConfig {
            path: std::path::PathBuf::from("(preview)"),
            message: "no root rows in matrix to preview".to_owned(),
        })?;
    tracing::info!(row = %first_root.id, "previewing first root row");

    let source = build_source(
        &first_root.source.kind,
        first_root.source.config.clone(),
        &auth,
        None,
    )
    .await?;
    let compiled = compile_transforms(&first_root.transforms)?
        .iter()
        .map(compile_stage)
        .collect::<Result<Vec<_>, _>>()?;
    let sample = crate::preview_sample::sample(
        source.as_ref(),
        &compiled,
        args.limit,
        crate::preview_sample::PREVIEW_TIMEOUT,
    )
    .await?;
    if sample.timed_out {
        tracing::warn!(
            records = sample.records.len(),
            "preview stopped after {}s before reaching --limit",
            crate::preview_sample::PREVIEW_TIMEOUT.as_secs()
        );
    }
    let limited = sample.records;
    let sink = StdoutSink::new(
        StdoutSinkConfig::new()
            .format(StdoutFormat::JsonLines)
            .flush_per_record(true),
    );
    sink.write_batch(&limited).await?;
    sink.flush().await?;

    let _ = std::marker::PhantomData::<Pipeline<'_, dyn faucet_core::Source, dyn Sink>>;
    Ok(())
}

#[cfg(not(feature = "sink-stdout"))]
pub async fn run(_args: PreviewArgs) -> CliResult<()> {
    Err(CliError::UnknownConnector {
        kind: "sink",
        name: "stdout".into(),
        available: "(preview requires faucet-cli to be built with the 'sink-stdout' feature)"
            .into(),
    })
}
