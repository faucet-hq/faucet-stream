//! `faucet history` — terminal view of the run history recorded in a config's
//! `catalog:` store (#391).
//!
//! The catalog store (the same backend `faucet serve` history and `faucet plan
//! --diff` use) records a [`RunRecord`] per run. This command reads the most
//! recent N of them and prints a table — status, duration, throughput — without
//! standing up `faucet serve`. Read-only; requires the `catalog` build feature.
//!
//! Run records are written by the control plane (`faucet serve`); a plain
//! `faucet run` with a `catalog:` block records dataset observations (see
//! `faucet catalog`) but not run records. Point `faucet history` at the same
//! store your `serve` instance writes to, to see its runs here.

use crate::catalog::CatalogHandle;
use crate::cli::HistoryArgs;
use crate::config::PipelineConfig;
use crate::error::{CliError, CliResult};
use crate::serve::history::{ListFilter, RunRecord};

/// Execute the `history` subcommand.
pub async fn run(args: HistoryArgs) -> CliResult<()> {
    let json = args.json;
    let runs = load(args).await?;

    if json {
        // The RunRecord's `Serialize` already omits secret-bearing fields
        // (config bodies are only present for cluster runs); scrub as a backstop.
        let json = serde_json::to_string_pretty(&runs)
            .map_err(|e| CliError::Internal(format!("rendering history JSON: {e}")))?;
        println!("{}", crate::secrets::registry::redact(&json));
        return Ok(());
    }

    if runs.is_empty() {
        println!(
            "no runs recorded yet in this catalog store \
             (run history is written by `faucet serve`)"
        );
        return Ok(());
    }

    print!("{}", render_table(&runs));
    Ok(())
}

/// The runs `faucet history` shows for `args`, newest first.
pub async fn load(args: HistoryArgs) -> CliResult<Vec<RunRecord>> {
    let labels =
        crate::serve::history::labels::parse_cli_filters(&args.label).map_err(CliError::Config)?;
    let (handle, pipeline_name) = connect(&args).await?;

    // Over-fetch when filtering by row so the post-filter still returns a full
    // page; otherwise fetch exactly the requested limit. Newest-first ordering
    // is guaranteed by the backend.
    let fetch = if args.row.is_some() {
        args.limit.max(200)
    } else {
        args.limit.max(1)
    };
    let page = handle
        .store
        .list(&ListFilter {
            name: pipeline_name,
            limit: fetch,
            labels,
            ..Default::default()
        })
        .await
        .map_err(|e| CliError::Internal(format!("catalog run-history read: {e}")))?;

    Ok(select_runs(page.runs, args.row.as_deref(), args.limit))
}

/// Load the config named by the flags and connect its `catalog:` store,
/// returning the handle + the pipeline name to filter run records by.
async fn connect(args: &HistoryArgs) -> CliResult<(CatalogHandle, Option<String>)> {
    let cwd = std::env::current_dir()?;
    let env_path =
        crate::env_loader::resolve_env_file(args.env_file.as_deref(), args.no_env_file, &cwd)?;
    crate::env_loader::load_env_file_if_present(env_path.as_deref())?;
    let path = match &args.config {
        Some(p) => p.clone(),
        None => crate::env_loader::discover_config_path(&cwd).ok_or(CliError::NoConfigOrFromEnv)?,
    };
    let cfg = PipelineConfig::from_path_async(&path, args.profile.as_deref()).await?;
    let spec = cfg.catalog.as_ref().ok_or_else(|| {
        CliError::Config(
            "no `catalog:` block in this config — add one naming the store (e.g. \
             `catalog: { url: sqlite:./faucet-catalog.db }`), or run \
             `faucet schema catalog` for the block's JSON Schema. `faucet history` \
             requires the `catalog` build feature."
                .to_string(),
        )
    })?;
    let handle = crate::catalog::connect_from_spec(spec).await?;
    Ok((handle, cfg.name.clone()))
}

/// Apply the `--row` filter and `--limit` truncation to a fetched page
/// (newest-first order preserved by the backend). Pure — unit-testable without
/// a catalog store.
pub(crate) fn select_runs(
    mut runs: Vec<RunRecord>,
    row: Option<&str>,
    limit: usize,
) -> Vec<RunRecord> {
    if let Some(row) = row {
        runs.retain(|r| r.invocations.iter().any(|i| i.row_id == row));
    }
    runs.truncate(limit);
    runs
}

/// Render the run table (newest first). Pure so it is unit-testable.
pub(crate) fn render_table(runs: &[RunRecord]) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "{:<20}  {:<10}  {:<19}  {:>10}  {:>12}  {:>10}  ROWS\n",
        "RUN ID", "STATUS", "STARTED", "DURATION", "ROWS OUT", "ROWS/S"
    ));
    for r in runs {
        let started = r
            .started_at
            .or(Some(r.submitted_at))
            .map(|t| t.format("%Y-%m-%d %H:%M:%S").to_string())
            .unwrap_or_else(|| "-".to_string());
        let duration = match r.elapsed_secs {
            Some(s) => format!("{s:.1}s"),
            None => "-".to_string(),
        };
        let rate = match r.elapsed_secs {
            Some(s) if s > 0.0 => format!("{:.0}", r.records_written as f64 / s),
            _ => "-".to_string(),
        };
        // Truncate long run ids (UUIDs) so the table stays aligned.
        let id = if r.run_id.chars().count() > 20 {
            format!("{}…", r.run_id.chars().take(19).collect::<String>())
        } else {
            r.run_id.clone()
        };
        out.push_str(&format!(
            "{:<20}  {:<10}  {:<19}  {:>10}  {:>12}  {:>10}  {}\n",
            id,
            r.status.as_str(),
            started,
            duration,
            r.records_written,
            rate,
            r.invocations.len(),
        ));
        if let Some(err) = &r.error {
            out.push_str(&format!(
                "  └─ error: {}\n",
                crate::secrets::registry::redact(err)
            ));
        }
    }
    out
}

#[cfg(test)]
mod tests;
