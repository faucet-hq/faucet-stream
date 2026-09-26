//! `faucet cleanup` — reclaim the local files a pipeline's sinks wrote (#587).
//!
//! The CLI half of the retention GC that `faucet serve` runs on a timer. Same
//! engine, same guardrail: it deletes **only** paths recorded in the ledger as
//! faucet's own local sink outputs, never a glob or a directory, and never a
//! file faucet wrote to but did not create.
//!
//! ```text
//! faucet cleanup                        # outputs past their retention window
//! faucet cleanup --older-than-days 3    # regardless of per-pipeline overrides
//! faucet cleanup --dataset <id>         # one dataset's outputs
//! faucet cleanup --all --yes            # every tracked output (confirmed)
//! faucet cleanup --all --dry-run        # show what --all would remove
//! ```
//!
//! Run history, catalog entries, and lineage are untouched — this removes data
//! files. A cleaned output keeps its ledger row, marked expired, which is what
//! the console renders instead of a dangling path.

use crate::cli::CleanupArgs;
use crate::config::PipelineConfig;
use crate::error::{CliError, CliResult};
use crate::local_outputs::{
    DEFAULT_RETENTION_DAYS, SkipReason, SweepReport, SweepScope,
    sweep::{self, SweepOptions},
};
use crate::serve::history::RunHistory;
use std::sync::Arc;

/// Execute the `cleanup` subcommand.
pub async fn run(args: CleanupArgs) -> CliResult<()> {
    let scope = resolve_scope(&args)?;
    // "Delete everything, including files still inside their retention window"
    // is not something to do because a flag was mistyped in a script.
    if scope.requires_confirmation() && !args.yes && !args.dry_run {
        return Err(CliError::Config(format!(
            "cleanup: scope `{}` deletes tracked local outputs that are still inside \
             their retention window (`--older-than-days 0` matches every output, same \
             as `--all`). Re-run with --yes to confirm, or with --dry-run to see what \
             it would remove.",
            scope.label()
        )));
    }

    let (store, retention_days) = connect(&args).await?;
    // `in_flight` stays empty here — this process runs no pipelines, and a
    // `--store` pointed at a live server's ledger cannot see that server's runs
    // at all. The mtime grace is what protects those files: it asks the
    // filesystem whether the path was touched recently rather than asking about
    // runs it cannot enumerate.
    let opts = SweepOptions::new(retention_days)
        .dry_run(args.dry_run)
        .in_flight_grace(std::time::Duration::from_secs(args.in_flight_grace_secs));
    let report = sweep::run(store.as_ref(), &scope, &opts)
        .await
        .map_err(|e| CliError::Internal(format!("local-output ledger: {e}")))?;

    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&report)
                .map_err(|e| CliError::Internal(format!("rendering cleanup JSON: {e}")))?
        );
    } else {
        print_report(&report, retention_days);
    }
    Ok(())
}

/// Resolve the mutually-exclusive scope flags. Clap enforces the exclusivity;
/// this maps whichever was given (defaulting to the retention sweep) onto a
/// [`SweepScope`].
fn resolve_scope(args: &CleanupArgs) -> CliResult<SweepScope> {
    if let Some(days) = args.older_than_days {
        return Ok(SweepScope::OlderThanDays(days));
    }
    if let Some(id) = &args.dataset {
        return Ok(SweepScope::Dataset(id.clone()));
    }
    if let Some(id) = &args.run {
        return Ok(SweepScope::Run(id.clone()));
    }
    if let Some(id) = &args.output {
        return Ok(SweepScope::Output(id.clone()));
    }
    if args.all {
        return Ok(SweepScope::All);
    }
    // The bare invocation is the safe one: exactly what the background sweeper
    // would do.
    Ok(SweepScope::Expired)
}

/// Load the config named by the flags and connect its ledger store, returning it
/// alongside the retention window to apply.
///
/// The ledger lives in the `catalog:` store — the same one `faucet run` /
/// `schedule` / `replicate` record into, and the one `faucet serve --history`
/// browses. `--store` overrides it for the case where the config is not to hand.
async fn connect(args: &CleanupArgs) -> CliResult<(Arc<dyn RunHistory>, u32)> {
    // An explicit --store needs no config at all.
    if let Some(url) = &args.store {
        let spec = crate::catalog::CatalogSpec {
            url: url.clone(),
            sample_records: crate::catalog::DEFAULT_SAMPLE_RECORDS,
            datasets: Vec::new(),
        };
        let handle = crate::catalog::connect_from_spec(&spec).await?;
        return Ok((
            handle.store,
            args.retention_days.unwrap_or(DEFAULT_RETENTION_DAYS),
        ));
    }

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
            "no `catalog:` block in this config, so there is no ledger of local outputs to \
             clean. Add one naming the store (e.g. `catalog: { url: sqlite:./faucet-catalog.db }`) \
             and re-run the pipeline, or pass --store <url> to point at an existing store."
                .to_string(),
        )
    })?;
    let handle = crate::catalog::connect_from_spec(spec).await?;
    // Precedence: the flag, then the config's own window, then the default —
    // so `faucet cleanup` and the serve sweeper agree about a given config.
    let retention_days = args
        .retention_days
        .or_else(|| {
            cfg.local_outputs
                .as_ref()
                .and_then(|spec| spec.retention_days)
        })
        .unwrap_or(DEFAULT_RETENTION_DAYS);
    Ok((handle.store, retention_days))
}

/// Human-readable report. Deliberately explicit about what was *not* deleted:
/// a silent "0 files" leaves the user wondering whether the GC is broken, when
/// the real answer is usually "those files are not faucet's to delete".
fn print_report(report: &SweepReport, retention_days: u32) {
    let verb = if report.dry_run {
        "would delete"
    } else {
        "deleted"
    };
    if report.outputs.is_empty() {
        match report.scope.as_str() {
            "expired" => println!(
                "nothing to clean — no tracked local output is older than {retention_days} day(s)"
            ),
            _ => println!("nothing to clean — no tracked local output matched"),
        }
        return;
    }
    for o in &report.outputs {
        match o.skipped {
            None => println!("  {} {} ({})", verb, o.path, human_bytes(o.bytes)),
            Some(reason) => println!("  skipped {} — {}", o.path, explain(reason, o)),
        }
    }
    println!(
        "{} {} file(s), {}{}",
        verb,
        report.deleted,
        human_bytes(report.bytes),
        if report.skipped > 0 {
            format!("; {} skipped", report.skipped)
        } else {
            String::new()
        }
    );
    if report.dry_run {
        println!("(dry run — nothing was removed)");
    }
}

/// One-line reason a file was left alone.
fn explain(reason: SkipReason, outcome: &crate::local_outputs::SweepOutcome) -> String {
    match reason {
        SkipReason::PreExisting => {
            "faucet wrote this file but did not create it, so it is never deleted".to_string()
        }
        SkipReason::AlreadyDeleted => "already cleaned (the record is kept as expired)".to_string(),
        SkipReason::NotOnDisk => "already gone from disk; marked expired".to_string(),
        SkipReason::InFlight => "a run is still writing it; will be retried".to_string(),
        SkipReason::DeleteFailed => format!(
            "could not delete: {}",
            outcome.error.as_deref().unwrap_or("unknown error")
        ),
    }
}

/// Bytes in a compact human form (the report is read by a human at a terminal).
fn human_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    match bytes {
        b if b >= GB => format!("{:.1} GiB", b as f64 / GB as f64),
        b if b >= MB => format!("{:.1} MiB", b as f64 / MB as f64),
        b if b >= KB => format!("{:.1} KiB", b as f64 / KB as f64),
        b => format!("{b} B"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::local_outputs::SweepOutcome;

    fn args() -> CleanupArgs {
        CleanupArgs {
            config: None,
            env_file: None,
            no_env_file: false,
            profile: None,
            json: false,
            store: None,
            older_than_days: None,
            dataset: None,
            run: None,
            output: None,
            in_flight_grace_secs: 60,
            all: false,
            retention_days: None,
            dry_run: false,
            yes: false,
        }
    }

    #[test]
    fn the_bare_invocation_is_the_retention_sweep() {
        // The default must be the conservative scope, not "everything".
        assert_eq!(resolve_scope(&args()).unwrap(), SweepScope::Expired);
    }

    #[test]
    fn each_flag_resolves_to_its_scope() {
        let mut a = args();
        a.older_than_days = Some(3);
        assert_eq!(resolve_scope(&a).unwrap(), SweepScope::OlderThanDays(3));

        let mut a = args();
        a.dataset = Some("ds1".into());
        assert_eq!(
            resolve_scope(&a).unwrap(),
            SweepScope::Dataset("ds1".into())
        );

        let mut a = args();
        a.run = Some("run-1".into());
        assert_eq!(resolve_scope(&a).unwrap(), SweepScope::Run("run-1".into()));

        let mut a = args();
        a.output = Some("out1".into());
        assert_eq!(
            resolve_scope(&a).unwrap(),
            SweepScope::Output("out1".into())
        );

        let mut a = args();
        a.all = true;
        assert_eq!(resolve_scope(&a).unwrap(), SweepScope::All);
    }

    #[tokio::test]
    async fn a_zero_day_window_needs_the_same_confirmation_as_clean_all() {
        // `--older-than-days 0` matches every row, so a script that computes the
        // window and lands on 0 must not delete everything unconfirmed.
        let mut a = args();
        a.older_than_days = Some(0);
        let err = run(a).await.unwrap_err();
        match err {
            CliError::Config(m) => assert!(m.contains("--yes"), "{m}"),
            other => panic!("expected a Config error, got {other:?}"),
        }

        // A real window is the operator's own bound and needs no second ask.
        let mut a = args();
        a.older_than_days = Some(1);
        let err = run(a).await.unwrap_err();
        assert!(
            !matches!(&err, CliError::Config(m) if m.contains("--yes")),
            "should have passed the confirmation gate, got {err:?}"
        );
    }

    #[tokio::test]
    async fn clean_all_without_confirmation_is_refused_before_touching_a_store() {
        // The refusal must happen before `connect`, so a mistyped `--all` in a
        // directory with no config still fails safe rather than by accident.
        let mut a = args();
        a.all = true;
        let err = run(a).await.unwrap_err();
        match err {
            CliError::Config(m) => {
                assert!(m.contains("--yes"), "{m}");
                assert!(m.contains("--dry-run"), "{m}");
            }
            other => panic!("expected a Config error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn clean_all_is_allowed_to_proceed_with_dry_run() {
        // `--dry-run` deletes nothing, so it needs no confirmation. It should get
        // past the gate and fail later, on config discovery.
        let mut a = args();
        a.all = true;
        a.dry_run = true;
        let err = run(a).await.unwrap_err();
        assert!(
            !matches!(&err, CliError::Config(m) if m.contains("--yes")),
            "should have passed the confirmation gate, got {err:?}"
        );
    }

    #[test]
    fn bytes_render_in_human_units() {
        assert_eq!(human_bytes(0), "0 B");
        assert_eq!(human_bytes(512), "512 B");
        assert_eq!(human_bytes(2048), "2.0 KiB");
        assert_eq!(human_bytes(5 * 1024 * 1024), "5.0 MiB");
        assert_eq!(human_bytes(3 * 1024 * 1024 * 1024), "3.0 GiB");
    }

    #[test]
    fn every_skip_reason_has_a_human_explanation() {
        let outcome = SweepOutcome {
            id: "i".into(),
            path: "/tmp/a".into(),
            dataset_uri: "file:///tmp/a".into(),
            deleted: false,
            bytes: 0,
            skipped: None,
            error: Some("permission denied".into()),
        };
        for r in [
            SkipReason::PreExisting,
            SkipReason::AlreadyDeleted,
            SkipReason::NotOnDisk,
            SkipReason::InFlight,
            SkipReason::DeleteFailed,
        ] {
            let text = explain(r, &outcome);
            assert!(!text.is_empty(), "{}", r.as_str());
        }
        assert!(explain(SkipReason::DeleteFailed, &outcome).contains("permission denied"));
        // The guardrail's explanation must actually explain it.
        assert!(explain(SkipReason::PreExisting, &outcome).contains("did not create"));
    }

    #[test]
    fn an_empty_retention_sweep_says_so_without_panicking() {
        let report = SweepReport {
            scope: "expired".into(),
            ..Default::default()
        };
        print_report(&report, 7);
        let report = SweepReport {
            scope: "all".into(),
            ..Default::default()
        };
        print_report(&report, 7);
    }
}
