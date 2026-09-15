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
            ..Default::default()
        })
        .await
        .map_err(|e| CliError::Internal(format!("catalog run-history read: {e}")))?;

    let runs = select_runs(page.runs, args.row.as_deref(), args.limit);

    if args.json {
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
        let id = if r.run_id.len() > 20 {
            format!("{}…", &r.run_id[..19])
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
            out.push_str(&format!("  └─ error: {err}\n"));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serve::history::{InvocationRecord, RunStatus};
    use chrono::{TimeZone, Utc};

    fn record(id: &str, status: RunStatus, rows: u64, elapsed: Option<f64>) -> RunRecord {
        let t = Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 0).unwrap();
        RunRecord {
            run_id: id.into(),
            name: Some("demo".into()),
            labels: Default::default(),
            status,
            submitted_at: t,
            started_at: Some(t),
            finished_at: Some(t),
            elapsed_secs: elapsed,
            records_written: rows,
            invocations: vec![InvocationRecord {
                row_id: "us".into(),
                parent_record_key: None,
                records_written: rows as usize,
                duration_ms: 0,
                error: None,
            }],
            error: (status == RunStatus::Failed).then(|| "boom".to_string()),
            idempotency_key: None,
            doctor_report: None,
            config_body: None,
            config_format: None,
            timeout_secs: None,
            clock: None,
            attempt: 0,
            replay_of: None,
            callback: None,
        }
    }

    #[test]
    fn table_lists_runs_with_status_and_throughput() {
        let runs = vec![
            record("run-1", RunStatus::Completed, 1000, Some(2.0)),
            record("run-2", RunStatus::Failed, 0, Some(0.5)),
        ];
        let t = render_table(&runs);
        assert!(t.contains("RUN ID"), "{t}");
        assert!(t.contains("run-1"), "{t}");
        assert!(t.contains("completed"), "{t}");
        assert!(t.contains("500"), "rows/s = 1000/2 = 500: {t}");
        assert!(t.contains("failed"), "{t}");
        assert!(t.contains("error: boom"), "failed run shows its error: {t}");
    }

    #[test]
    fn missing_elapsed_renders_dashes_not_a_panic() {
        let runs = vec![record("r", RunStatus::Running, 5, None)];
        let t = render_table(&runs);
        assert!(t.contains("running"), "{t}");
        // No division by zero / no NaN in the rate column.
        assert!(!t.contains("NaN") && !t.contains("inf"), "{t}");
    }

    #[tokio::test]
    async fn reads_seeded_in_memory_catalog() {
        use crate::serve::history::RunHistory;
        // Seed a memory backend with two runs, then read them back via list().
        let store =
            crate::serve::history::memory::MemoryHistory::new(std::time::Duration::from_secs(3600));
        store
            .upsert(&record("a", RunStatus::Completed, 10, Some(1.0)))
            .await
            .unwrap();
        store
            .upsert(&record("b", RunStatus::Completed, 20, Some(1.0)))
            .await
            .unwrap();
        let page = store
            .list(&ListFilter {
                name: Some("demo".into()),
                limit: 10,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(page.runs.len(), 2);
        let t = render_table(&page.runs);
        assert!(t.contains("a") && t.contains("b"), "{t}");
    }

    #[test]
    fn select_runs_filters_by_row_and_truncates() {
        let mut a = record("a", RunStatus::Completed, 1, Some(1.0));
        a.invocations[0].row_id = "us".into();
        let mut b = record("b", RunStatus::Completed, 1, Some(1.0));
        b.invocations[0].row_id = "eu".into();
        let all = vec![a.clone(), b.clone()];

        // --row keeps only matching invocations.
        let only_eu = select_runs(all.clone(), Some("eu"), 20);
        assert_eq!(only_eu.len(), 1);
        assert_eq!(only_eu[0].run_id, "b");
        // No filter, but --limit truncates.
        let capped = select_runs(all.clone(), None, 1);
        assert_eq!(capped.len(), 1);
        // A row nobody has → empty.
        assert!(select_runs(all, Some("apac"), 20).is_empty());
    }

    #[tokio::test]
    async fn run_errors_without_a_catalog_block() {
        use crate::cli::HistoryArgs;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("faucet.yaml");
        std::fs::write(
            &path,
            "version: 1\nname: demo\npipeline:\n  source: { type: rest, config: { path: /x } }\n  sink: { type: jsonl, config: { path: o } }\n",
        )
        .unwrap();
        let err = super::run(HistoryArgs {
            config: Some(path),
            env_file: None,
            no_env_file: true,
            profile: None,
            limit: 20,
            row: None,
            json: false,
        })
        .await;
        match err {
            Err(CliError::Config(m)) => assert!(m.contains("catalog"), "got: {m}"),
            other => panic!("expected a no-catalog Config error, got {other:?}"),
        }
    }
}
