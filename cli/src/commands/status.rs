//! `faucet status` (#732) — load the config, read every row's state, run
//! history and DLQ, print one screen (or `--json`), and exit 0 healthy /
//! 1 degraded or unknown / 2 failed.

use crate::cli::StatusArgs;
use crate::error::{CliError, CliResult};
use crate::pipeline_state::ops::Stores;
use crate::status::{StatusInputs, StatusReport, assemble, render};
use chrono::Utc;

/// Execute the `status` subcommand.
pub async fn run(args: StatusArgs) -> CliResult<()> {
    let report = build(&args).await?;
    let text = if args.load.json {
        serde_json::to_string_pretty(&report)
            .map_err(|e| CliError::Internal(format!("serializing JSON output: {e}")))?
            + "\n"
    } else {
        render::render(&report)
            + &report
                .notes
                .iter()
                .map(|n| format!("  note: {n}\n"))
                .collect::<String>()
    };
    print!("{}", crate::secrets::registry::redact(&text));
    match report.exit_code {
        0 => Ok(()),
        code => Err(CliError::StatusUnhealthy {
            code,
            health: report.health.as_str().to_string(),
        }),
    }
}

/// Assemble the report `faucet status` prints.
pub async fn build(args: &StatusArgs) -> CliResult<StatusReport> {
    let (cfg, target, _) = super::state::load(args.config.as_deref(), &args.load).await?;
    let auth = crate::auth_catalog::build_auth_catalog(cfg.auth.as_ref())?;
    let stores = Stores::build(&target, None)
        .await
        .map_err(|e| crate::secrets::registry::redact(&e.to_string()).into_owned());
    #[allow(unused_mut)]
    let mut notes: Vec<String> = Vec::new();
    #[allow(unused_mut)]
    let (mut history, mut active_runs) = (Vec::new(), Vec::new());
    #[cfg(feature = "catalog")]
    if let Some(spec) = &cfg.catalog {
        match crate::catalog::connect_from_spec(spec).await {
            Ok(handle) => {
                match crate::status::history::read(handle.store.as_ref(), &target.pipeline).await {
                    Ok((h, a)) => {
                        history = h;
                        active_runs = a;
                    }
                    Err(e) => notes.push(format!("run history unreadable: {e}")),
                }
            }
            Err(e) => notes.push(format!("run history unreachable: {e}")),
        }
    }
    let inputs = StatusInputs {
        now: Utc::now(),
        row: args.row.as_deref(),
        probe: args.probe,
        auth: &auth,
        history,
        active_runs,
    };
    let mut report = assemble(&target, stores.as_ref().map_err(Clone::clone), &inputs).await?;
    report.notes.extend(notes);
    Ok(report)
}
