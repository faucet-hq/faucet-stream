//! `faucet backfill` — replay a bounded historical window of a pipeline
//! (#282): plan window units, run them with bounded parallelism, record
//! durable progress, and report the per-unit outcome.

use crate::backfill::plan::{parse_boundary, parse_window};
use crate::backfill::spec::parse_timezone;
use crate::backfill::{BackfillOptions, BackfillOutcome, BackfillRange, run_backfill};
use crate::cli::BackfillArgs;
use crate::config::PipelineConfig;
use crate::error::{CliError, CliResult};
use serde_json::Value;

/// Execute the `backfill` subcommand.
pub async fn run(args: BackfillArgs) -> CliResult<()> {
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

    let spec = cfg.backfill.clone().unwrap_or_default();
    let tz = match args.timezone.as_deref().or(spec.timezone.as_deref()) {
        Some(name) => parse_timezone(name)?,
        None => chrono_tz::Tz::UTC,
    };
    let window = match args.window.as_deref().or(spec.window.as_deref()) {
        Some(w) => Some(parse_window(w)?),
        None => None,
    };
    let concurrency = args.concurrency.or(spec.concurrency).unwrap_or(1).max(1);

    let range = build_range(
        &args.from,
        &args.to,
        &args.from_bookmark,
        &args.to_bookmark,
        args.bookmark_field.clone(),
        window,
        tz,
    )?;

    let pipeline_name = cfg.name.clone().unwrap_or_else(|| {
        path.file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("pipeline")
            .to_owned()
    });
    let auth = crate::auth_catalog::build_auth_catalog(cfg.auth.as_ref())?;
    let resilience = match &cfg.resilience {
        Some(spec) => Some(spec.to_policy()?),
        None => None,
    };

    let outcome = run_backfill(
        &cfg,
        BackfillOptions {
            pipeline_name,
            execution: cfg.execution.clone(),
            auth,
            resilience,
            usage: crate::usage::UsageOptions::from_spec(cfg.usage.as_ref(), None)
                .map_err(CliError::Config)?,
            range,
            concurrency,
            row: args.row,
            into_sink: args.into,
            dry_run: args.dry_run,
            resume: args.resume,
            restart: args.restart,
            cancel: None,
        },
    )
    .await?;

    faucet_core::shutdown_otel();
    report(&outcome, args.json)?;

    if outcome.failed > 0 {
        return Err(CliError::BackfillFailed {
            failed: outcome.failed,
        });
    }
    Ok(())
}

/// Resolve the mutually-exclusive range flags into a [`BackfillRange`].
#[allow(clippy::too_many_arguments)]
fn build_range(
    from: &Option<String>,
    to: &Option<String>,
    from_bookmark: &Option<String>,
    to_bookmark: &Option<String>,
    bookmark_field: Option<String>,
    window: Option<crate::backfill::plan::WindowStep>,
    tz: chrono_tz::Tz,
) -> CliResult<BackfillRange> {
    match (from, to, from_bookmark) {
        (Some(f), Some(t), None) => Ok(BackfillRange::Time {
            from: parse_boundary(f, tz)?,
            to: parse_boundary(t, tz)?,
            window,
            tz,
        }),
        (None, None, Some(fb)) => Ok(BackfillRange::Bookmark {
            from: parse_bookmark_value(fb),
            to: to_bookmark.as_deref().map(parse_bookmark_value),
            field: bookmark_field,
        }),
        (None, None, None) => Err(CliError::Config(
            "specify a range: --from/--to (wall-clock) or --from-bookmark (bookmark value)".into(),
        )),
        _ => Err(CliError::Config(
            "--from/--to and --from-bookmark are mutually exclusive, and --from requires --to"
                .into(),
        )),
    }
}

/// A bookmark flag value: JSON if it parses (numbers, quoted strings, null),
/// else the raw string (so `--from-bookmark 2026-01-01` needs no quoting).
fn parse_bookmark_value(s: &str) -> Value {
    serde_json::from_str(s).unwrap_or_else(|_| Value::String(s.to_string()))
}

/// Print the outcome (human table or `--json`).
fn report(outcome: &BackfillOutcome, json: bool) -> CliResult<()> {
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(outcome)
                .map_err(|e| CliError::Internal(format!("json render: {e}")))?
        );
        return Ok(());
    }
    if outcome.dry_run {
        println!(
            "backfill plan — {} unit{} ({} already done):",
            outcome.planned,
            if outcome.planned == 1 { "" } else { "s" },
            outcome.skipped
        );
        for u in &outcome.units {
            println!("  {:9}  {}  {} → {}", u.outcome, u.unit, u.start, u.end);
        }
        println!("dry run — nothing executed");
        return Ok(());
    }
    for u in &outcome.units {
        match &u.error {
            Some(e) => println!("  failed   {}  {} → {}: {e}", u.unit, u.start, u.end),
            None => println!("  done     {}  {} → {}", u.unit, u.start, u.end),
        }
    }
    println!(
        "backfill: {} done, {} failed, {} skipped (of {} planned){}",
        outcome.succeeded,
        outcome.failed,
        outcome.skipped,
        outcome.planned,
        if outcome.failed > 0 {
            " — re-run with --resume to retry the failed units"
        } else {
            ""
        }
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn utc() -> chrono_tz::Tz {
        "UTC".parse().unwrap()
    }

    #[test]
    fn range_flag_combinations() {
        // Time mode.
        let r = build_range(
            &Some("2026-06-01".into()),
            &Some("2026-07-01".into()),
            &None,
            &None,
            None,
            None,
            utc(),
        )
        .unwrap();
        assert!(matches!(r, BackfillRange::Time { .. }));

        // Bookmark mode with typed values.
        let r = build_range(
            &None,
            &None,
            &Some("42".into()),
            &Some("2026-01-01".into()),
            Some("updated_at".into()),
            None,
            utc(),
        )
        .unwrap();
        match r {
            BackfillRange::Bookmark { from, to, field } => {
                assert_eq!(from, serde_json::json!(42), "JSON number parsed");
                assert_eq!(
                    to,
                    Some(serde_json::json!("2026-01-01")),
                    "unquoted date stays a string"
                );
                assert_eq!(field.as_deref(), Some("updated_at"));
            }
            other => panic!("expected bookmark range, got {other:?}"),
        }

        // Missing / conflicting flags.
        assert!(build_range(&None, &None, &None, &None, None, None, utc()).is_err());
        assert!(
            build_range(
                &Some("2026-06-01".into()),
                &None,
                &None,
                &None,
                None,
                None,
                utc()
            )
            .is_err()
        );
        assert!(
            build_range(
                &Some("2026-06-01".into()),
                &Some("2026-07-01".into()),
                &Some("42".into()),
                &None,
                None,
                None,
                utc()
            )
            .is_err()
        );
    }

    #[test]
    fn report_renders_human_and_json() {
        let outcome = BackfillOutcome {
            descriptor: "time|a|b|1d|default".into(),
            planned: 2,
            skipped: 1,
            succeeded: 1,
            failed: 0,
            dry_run: false,
            units: vec![crate::backfill::orchestrator::UnitReport {
                unit: "20260601T000000Z".into(),
                start: "2026-06-01T00:00:00+00:00".into(),
                end: "2026-06-02T00:00:00+00:00".into(),
                outcome: "done".into(),
                error: None,
            }],
        };
        report(&outcome, false).unwrap();
        report(&outcome, true).unwrap();
        let dry = BackfillOutcome {
            dry_run: true,
            ..outcome
        };
        report(&dry, false).unwrap();
    }
}

#[cfg(all(test, feature = "source-sqlite", feature = "sink-jsonl"))]
mod run_tests {
    //! Command-level tests driving `run()` end-to-end (offline).
    use super::run;
    use crate::cli::BackfillArgs;

    fn args(config: std::path::PathBuf) -> BackfillArgs {
        BackfillArgs {
            config: Some(config),
            from: Some("2026-06-01".into()),
            to: Some("2026-06-04".into()),
            window: Some("1d".into()),
            from_bookmark: None,
            to_bookmark: None,
            bookmark_field: None,
            concurrency: None,
            timezone: None,
            row: None,
            into: None,
            dry_run: true,
            resume: false,
            restart: false,
            json: false,
            env_file: None,
            no_env_file: true,
            profile: None,
        }
    }

    fn write_config(dir: &std::path::Path) -> std::path::PathBuf {
        let cfg = dir.join("bf.yaml");
        std::fs::write(
            &cfg,
            r#"
version: 1
name: bf
backfill:
  window: 1d
  concurrency: 1
pipeline:
  source:
    type: sqlite
    config:
      database_url: "sqlite::memory:"
      query: "SELECT '${backfill.start}' AS s"
  sink:
    type: jsonl
    config: { path: ./out.jsonl }
"#,
        )
        .unwrap();
        cfg
    }

    #[tokio::test]
    async fn dry_run_plans_without_executing() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = write_config(dir.path());
        run(args(cfg.clone())).await.expect("dry run succeeds");

        // JSON output path.
        let mut a = args(cfg);
        a.json = true;
        run(a).await.expect("json dry run succeeds");
    }

    #[tokio::test]
    async fn missing_range_is_a_typed_error() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = write_config(dir.path());
        let mut a = args(cfg);
        a.from = None;
        a.to = None;
        let err = run(a).await.unwrap_err();
        assert!(err.to_string().contains("--from"), "{err}");
    }

    #[tokio::test]
    async fn config_window_default_applies_when_flag_omitted() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = write_config(dir.path());
        let mut a = args(cfg);
        a.window = None; // falls back to backfill.window (1d) from the config
        run(a).await.expect("config default window used");
    }
}
