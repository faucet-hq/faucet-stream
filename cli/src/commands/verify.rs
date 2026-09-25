//! `faucet verify` — compare a destination to its source by content (#701).
//!
//! Thin command layer over [`crate::verify`]: load the config, run the
//! verification for one root row, render the report (human or `--json`), and
//! exit with the differing-key count.

use crate::cli::VerifyArgs;
use crate::config::PipelineConfig;
use crate::error::{CliError, CliResult};
use crate::verify::{VerifyInputs, VerifyOutcome};
use chrono::Utc;
use faucet_core::diff::DifferenceKind;

/// Execute the `verify` subcommand.
pub async fn run(args: VerifyArgs) -> CliResult<()> {
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
    let auth = crate::auth_catalog::build_auth_catalog(cfg.auth.as_ref())?;
    // The `verify:` block is optional for the command: without one, defaults
    // apply (the sink's key, every column, `_faucet_*` excluded).
    let mut spec = cfg.verify.clone().unwrap_or_default();
    if let Some(n) = args.max_differences {
        spec.max_differences = n;
    }

    let outcome = crate::verify::verify(
        &cfg,
        &spec,
        VerifyInputs {
            row: args.row.clone(),
            repair: args.repair,
            allow_delete: args.allow_delete,
            dry_run: args.dry_run,
            pipeline_name,
            execution: cfg.execution.clone(),
            auth,
            clock: Utc::now().fixed_offset(),
        },
    )
    .await?;

    faucet_core::shutdown_otel();

    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&outcome)
                .map_err(|e| CliError::Internal(format!("serializing JSON output: {e}")))?
        );
    } else {
        print!("{}", render_human(&outcome));
    }
    let differences = outcome.differing_keys();
    if differences > 0 {
        return Err(CliError::VerifyFailed { differences });
    }
    Ok(())
}

/// The human report: one summary line, the tally, then the first differences.
pub fn render_human(outcome: &VerifyOutcome) -> String {
    let r = &outcome.report;
    let mut out = String::new();
    let (missing, extra, changed, dup) = r.tally();
    let verdict = if r.equal() { "MATCH" } else { "DIFFERENT" };
    out.push_str(&format!(
        "verify [{}]: {} — {} vs {} (key {}, {} mode, {} range(s) compared, {} differing)\n",
        outcome.row,
        verdict,
        outcome.source,
        outcome.destination,
        outcome.key.join(","),
        outcome.strategy,
        r.ranges_compared,
        r.ranges_differing,
    ));
    out.push_str(&format!(
        "  digests: {}   rows fetched: {} source / {} destination\n",
        if r.server_digests {
            "server-side (matching ranges shipped no rows)"
        } else {
            "client-side"
        },
        r.rows_fetched_source,
        r.rows_fetched_dest,
    ));
    if !r.equal() {
        out.push_str(&format!(
            "  {} differing key(s): {missing} missing in destination, {extra} extra in destination, \
             {changed} changed, {dup} duplicated{}\n",
            r.differences.len(),
            if r.truncated {
                " (report truncated)"
            } else {
                ""
            }
        ));
        for d in r.differences.iter().take(25) {
            let what = match &d.kind {
                DifferenceKind::MissingInDest => "missing in destination".to_string(),
                DifferenceKind::ExtraInDest => "extra in destination".to_string(),
                DifferenceKind::Changed { columns } => format!("changed: {}", columns.join(", ")),
                DifferenceKind::Duplicate { side, count } => {
                    format!("duplicated {count}× on the {side:?} side").to_lowercase()
                }
            };
            out.push_str(&format!("    {} → {what}\n", d.key));
        }
        if r.differences.len() > 25 {
            out.push_str(&format!(
                "    … {} more (use --json for all)\n",
                r.differences.len() - 25
            ));
        }
    }
    match (r.repaired_upserts, r.repaired_deletes) {
        (Some(u), Some(d)) if outcome.dry_run => out.push_str(&format!(
            "  repair (dry-run): {u} row(s) would be upserted, {d} deleted\n"
        )),
        (Some(u), Some(d)) => out.push_str(&format!(
            "  repaired: {u} row(s) upserted, {d} deleted — re-run `faucet verify` to confirm\n"
        )),
        _ => {}
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::diff::{Difference, Side, VerifyReport};
    use serde_json::json;

    fn outcome(report: VerifyReport, dry_run: bool) -> VerifyOutcome {
        VerifyOutcome {
            row: "default".into(),
            source: "sqlite:///a#t".into(),
            destination: "sqlite:///b#t".into(),
            key: vec!["id".into()],
            strategy: "range",
            report,
            dry_run,
        }
    }

    #[test]
    fn human_report_match() {
        let text = render_human(&outcome(
            VerifyReport {
                ranges_compared: 4,
                server_digests: true,
                ..Default::default()
            },
            false,
        ));
        assert!(text.contains("MATCH"), "{text}");
        assert!(text.contains("server-side"));
        assert!(!text.contains("differing key(s)"));
    }

    #[test]
    fn human_report_lists_differences_and_repair() {
        let mut report = VerifyReport {
            ranges_compared: 4,
            ranges_differing: 1,
            truncated: true,
            repaired_upserts: Some(2),
            repaired_deletes: Some(1),
            ..Default::default()
        };
        report.differences = vec![
            Difference {
                key: json!({"id": 1}),
                kind: DifferenceKind::MissingInDest,
            },
            Difference {
                key: json!({"id": 2}),
                kind: DifferenceKind::ExtraInDest,
            },
            Difference {
                key: json!({"id": 3}),
                kind: DifferenceKind::Changed {
                    columns: vec!["v".into()],
                },
            },
            Difference {
                key: json!({"id": 4}),
                kind: DifferenceKind::Duplicate {
                    side: Side::Source,
                    count: 2,
                },
            },
        ];
        let text = render_human(&outcome(report.clone(), false));
        assert!(text.contains("DIFFERENT"), "{text}");
        assert!(text.contains("1 missing in destination, 1 extra in destination, 1 changed, 1 duplicated (report truncated)"), "{text}");
        assert!(text.contains("changed: v"));
        assert!(text.contains("duplicated 2× on the source side"));
        assert!(text.contains("repaired: 2 row(s) upserted, 1 deleted"));
        let dry = render_human(&outcome(report, true));
        assert!(dry.contains("repair (dry-run): 2 row(s) would be upserted"));
    }

    #[test]
    fn human_report_elides_beyond_25() {
        let report = VerifyReport {
            differences: (0..30)
                .map(|i| Difference {
                    key: json!({"id": i}),
                    kind: DifferenceKind::MissingInDest,
                })
                .collect(),
            ..Default::default()
        };
        let text = render_human(&outcome(report, false));
        assert!(text.contains("… 5 more"), "{text}");
    }
}
