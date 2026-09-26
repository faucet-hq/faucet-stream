//! `faucet policy` — evaluate a data-flow policy against a config (#702) and
//! print, per row, the labelled columns heading into each sink and every
//! violated rule. Offline-safe: secrets are never fetched. Exits with the
//! violation count, so it doubles as a CI gate.

use crate::cli::PolicyArgs;
use crate::config::PipelineConfig;
use crate::error::{CliError, CliResult};

/// Execute the `policy` subcommand.
pub async fn run(args: PolicyArgs) -> CliResult<()> {
    let cwd = std::env::current_dir()?;
    let env_path =
        crate::env_loader::resolve_env_file(args.env_file.as_deref(), args.no_env_file, &cwd)?;
    crate::env_loader::load_env_file_if_present(env_path.as_deref())?;
    let path = match args.config {
        Some(p) => p,
        None => crate::env_loader::discover_config_path(&cwd).ok_or(CliError::NoConfigOrFromEnv)?,
    };
    let mut cfg = PipelineConfig::from_path_tolerating_secrets(&path, args.profile.as_deref())?;
    crate::policy::apply_to_config(&mut cfg, args.policy.as_deref())?;
    let spec = cfg.policy.as_ref().ok_or_else(|| {
        CliError::Config(
            "no policy: pass `--policy <file>` or add a top-level `policy:` block (see \
             `faucet schema policy`)"
                .to_string(),
        )
    })?;
    let nodes = crate::expand::expand(&cfg)?;
    let nodes: Vec<_> = match &args.row {
        Some(r) => {
            let selected: Vec<_> = nodes.into_iter().filter(|n| &n.id == r).collect();
            if selected.is_empty() {
                return Err(CliError::Config(format!(
                    "policy: row '{r}' is not a row of this config"
                )));
            }
            selected
        }
        None => nodes,
    };
    let report = crate::policy::evaluate_nodes(spec, &nodes, &Default::default())?;
    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&report)
                .map_err(|e| CliError::Internal(format!("serializing JSON output: {e}")))?
        );
    } else {
        print!("{}", crate::policy::render_human(&report));
    }
    if report.violated() {
        return Err(report.error());
    }
    Ok(())
}
