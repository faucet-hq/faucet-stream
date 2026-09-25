//! `faucet rollback` — undo a run (#706).
//!
//! Thin command layer over [`crate::rollback`]: load the config, find the
//! run's marker in the row's state, ask the sink to undo it, rewind the
//! bookmark, and render the report (human or `--json`). `--list` prints the
//! undoable runs instead.

use crate::cli::RollbackArgs;
use crate::config::PipelineConfig;
use crate::error::{CliError, CliResult};
use crate::rollback::{RollbackInputs, RollbackReport, RunMarker};

/// Execute the `rollback` subcommand.
pub async fn run(args: RollbackArgs) -> CliResult<()> {
    let cwd = std::env::current_dir()?;
    let env_path =
        crate::env_loader::resolve_env_file(args.env_file.as_deref(), args.no_env_file, &cwd)?;
    crate::env_loader::load_env_file_if_present(env_path.as_deref())?;
    let path = match args.config {
        Some(p) => p,
        None => crate::env_loader::discover_config_path(&cwd).ok_or(CliError::NoConfigOrFromEnv)?,
    };
    let cfg = PipelineConfig::from_path_async(&path, args.profile.as_deref()).await?;
    crate::obs::install(&cfg)?;
    let pipeline_name = cfg.name.clone().unwrap_or_else(|| {
        path.file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("pipeline")
            .to_owned()
    });

    if args.list {
        let runs = crate::rollback::list(&cfg, &pipeline_name, args.row.as_deref()).await?;
        if args.json {
            println!(
                "{}",
                serde_json::to_string_pretty(&runs)
                    .map_err(|e| CliError::Internal(format!("serializing JSON output: {e}")))?
            );
        } else {
            print!("{}", render_list(&runs));
        }
        return Ok(());
    }

    let run_id = args
        .run
        .ok_or_else(|| CliError::Config("rollback: --run <id> is required (or --list)".into()))?;
    let auth = crate::auth_catalog::build_auth_catalog(cfg.auth.as_ref())?;
    let report = crate::rollback::rollback(
        &cfg,
        RollbackInputs {
            run_id,
            row: args.row,
            dry_run: args.dry_run,
            force: args.force,
            pipeline_name,
            auth,
        },
    )
    .await?;

    faucet_core::shutdown_otel();

    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&report)
                .map_err(|e| CliError::Internal(format!("serializing JSON output: {e}")))?
        );
    } else {
        print!("{}", render_report(&report));
    }
    if report.blocked() {
        return Err(CliError::RollbackBlocked {
            conflicts: report.outcome.conflicts,
        });
    }
    Ok(())
}

/// The human report of one rollback.
pub fn render_report(r: &RollbackReport) -> String {
    let o = &r.outcome;
    let head = if r.dry_run {
        "rollback (dry-run)"
    } else if o.applied {
        "rollback"
    } else {
        "rollback BLOCKED"
    };
    let mut out = format!(
        "{head}: run {} on row '{}' ({} {}, {} mode)\n",
        r.run_id,
        r.row,
        r.sink_kind,
        r.dataset,
        o_mode(r)
    );
    let verb = if r.dry_run { "would " } else { "" };
    match r.mode {
        faucet_core::rollback::RollbackMode::Append => {
            out.push_str(&format!(
                "  {verb}delete {} row(s) the run appended\n",
                o.deleted
            ));
        }
        faucet_core::rollback::RollbackMode::Upsert => {
            out.push_str(&format!(
                "  {verb}delete {} key(s) the run created, {verb}restore {} before-image(s)\n",
                o.deleted, o.restored
            ));
        }
        faucet_core::rollback::RollbackMode::Overwrite => {
            out.push_str(&format!(
                "  {verb}swap back the kept previous table ({} row(s))\n",
                o.restored
            ));
        }
    }
    if o.conflicts > 0 {
        out.push_str(&format!(
            "  {} key(s) were changed by a later run{}\n",
            o.conflicts,
            if o.applied {
                " (restored anyway: --force)"
            } else {
                ""
            }
        ));
    }
    if let Some(note) = &o.note {
        out.push_str(&format!("  note: {note}\n"));
    }
    if o.applied && !r.dry_run {
        out.push_str(&format!(
            "  bookmark {}; exactly-once watermark {}\n",
            if r.bookmark_rewound {
                "rewound to its pre-run value"
            } else {
                "unchanged"
            },
            if r.token_rewound {
                "rewound"
            } else {
                "not applicable"
            }
        ));
    }
    out
}

fn o_mode(r: &RollbackReport) -> &'static str {
    r.mode.as_str()
}

/// The `--list` table: newest first per row.
pub fn render_list(runs: &[RunMarker]) -> String {
    if runs.is_empty() {
        return "no undoable runs recorded (runs need a `rollback:` block and a durable `state:`)\n"
            .to_string();
    }
    let mut out = format!(
        "{:<38} {:<20} {:<10} {:<20} {}\n",
        "run id", "row", "mode", "started", "dataset"
    );
    for m in runs {
        out.push_str(&format!(
            "{:<38} {:<20} {:<10} {:<20} {}\n",
            m.run_id,
            m.row,
            m.mode.as_str(),
            m.started_at.format("%Y-%m-%d %H:%M:%SZ"),
            m.sink_uri
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use faucet_core::DeliveryMode;
    use faucet_core::rollback::{RollbackMode, RollbackOutcome};

    fn report(mode: RollbackMode, outcome: RollbackOutcome, dry_run: bool) -> RollbackReport {
        RollbackReport {
            run_id: "r1".into(),
            row: "default".into(),
            sink_kind: "sqlite".into(),
            dataset: "sqlite:///x#t".into(),
            mode,
            dry_run,
            outcome,
            bookmark_rewound: true,
            token_rewound: false,
        }
    }

    #[test]
    fn renders_each_mode() {
        let applied = RollbackOutcome {
            deleted: 3,
            restored: 0,
            conflicts: 0,
            applied: true,
            note: None,
        };
        let t = render_report(&report(RollbackMode::Append, applied.clone(), false));
        assert!(t.contains("delete 3 row(s) the run appended"), "{t}");
        assert!(t.contains("bookmark rewound"), "{t}");
        let t = render_report(&report(
            RollbackMode::Upsert,
            RollbackOutcome {
                deleted: 1,
                restored: 2,
                conflicts: 1,
                applied: true,
                note: None,
            },
            true,
        ));
        assert!(
            t.contains("(dry-run)") && t.contains("would delete 1 key(s)"),
            "{t}"
        );
        assert!(t.contains("restored anyway"));
        let t = render_report(&report(
            RollbackMode::Overwrite,
            RollbackOutcome::blocked(4),
            false,
        ));
        assert!(
            t.contains("BLOCKED") && t.contains("4 key(s) were changed"),
            "{t}"
        );
        assert!(t.contains("note: "));
    }

    #[test]
    fn renders_list() {
        assert!(render_list(&[]).contains("no undoable runs"));
        let m = RunMarker {
            run_id: "abc".into(),
            pipeline: "p".into(),
            row: "default".into(),
            state_key: "p::default".into(),
            started_at: Utc::now(),
            clock: Utc::now().fixed_offset(),
            sink_kind: "sqlite".into(),
            sink_uri: "sqlite:///x#t".into(),
            mode: RollbackMode::Upsert,
            delivery: DeliveryMode::AtLeastOnce,
            run_id_column: "_faucet_run_id".into(),
            bookmark_before: None,
            token_before: None,
        };
        let t = render_list(&[m]);
        assert!(t.contains("abc") && t.contains("upsert") && t.contains("sqlite:///x#t"));
    }
}
