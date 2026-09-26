//! `faucet profiling show|reset` — inspect or re-baseline the learned column
//! profiles a config's `profiling:` block keeps in its state store (#708).
//!
//! Thin command layer over [`crate::profiling`]: load the config, walk the
//! root rows, render (human or `--json`).

use crate::cli::{ProfilingArgs, ProfilingCommand, ProfilingConfigArgs};
use crate::config::PipelineConfig;
use crate::error::{CliError, CliResult};
use crate::profiling::{ResetOutcome, RowHistory};
use faucet_core::ColumnProfile;

/// Execute the `profiling` subcommand.
pub async fn run(args: ProfilingArgs) -> CliResult<()> {
    match args.command {
        ProfilingCommand::Show(a) => {
            let (cfg, name) = load(&a.common).await?;
            let rows = crate::profiling::show(&cfg, &name, a.common.row.as_deref()).await?;
            if a.common.json {
                println!("{}", to_pretty(&rows)?);
            } else {
                print!("{}", render_show(&rows, a.full));
            }
            Ok(())
        }
        ProfilingCommand::Reset(a) => {
            let (cfg, name) = load(&a.common).await?;
            let out =
                crate::profiling::reset(&cfg, &name, a.common.row.as_deref(), a.column.as_deref())
                    .await?;
            if a.common.json {
                println!("{}", to_pretty(&out)?);
            } else {
                print!("{}", render_reset(&out, a.column.as_deref()));
            }
            Ok(())
        }
    }
}

async fn load(common: &ProfilingConfigArgs) -> CliResult<(PipelineConfig, String)> {
    let cwd = std::env::current_dir()?;
    let env_path =
        crate::env_loader::resolve_env_file(common.env_file.as_deref(), common.no_env_file, &cwd)?;
    crate::env_loader::load_env_file_if_present(env_path.as_deref())?;
    let path = match &common.config {
        Some(p) => p.clone(),
        None => crate::env_loader::discover_config_path(&cwd).ok_or(CliError::NoConfigOrFromEnv)?,
    };
    let cfg = PipelineConfig::from_path_async(&path, common.profile.as_deref()).await?;
    if cfg.profiling.is_none() && !cfg.matrix.iter().any(|r| r.profiling.is_some()) {
        return Err(CliError::Config(
            "this config has no `profiling:` block — add one (see `faucet schema profiling`) \
             and run the pipeline to start a baseline"
                .into(),
        ));
    }
    let name = cfg.name.clone().unwrap_or_else(|| {
        path.file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("pipeline")
            .to_owned()
    });
    Ok((cfg, name))
}

fn to_pretty<T: serde::Serialize>(v: &T) -> CliResult<String> {
    serde_json::to_string_pretty(v)
        .map_err(|e| CliError::Internal(format!("serializing JSON output: {e}")))
}

/// One summary line per column: type mix, null rate, distinct, the numeric or
/// string summary, and the top values when they exist.
pub fn column_line(name: &str, c: &ColumnProfile) -> String {
    let types = c
        .types
        .present()
        .into_iter()
        .filter(|(t, _)| *t != "null")
        .map(|(t, n)| format!("{t}:{n}"))
        .collect::<Vec<_>>()
        .join("/");
    let mut line = format!(
        "  {name:<28} null {:>5.1}%  distinct {:>8}  [{}]",
        c.null_rate * 100.0,
        c.distinct,
        if types.is_empty() {
            "null".to_string()
        } else {
            types
        }
    );
    if let Some(n) = &c.numeric {
        line.push_str(&format!(
            "  min {} max {} mean {:.4} p50 {:.4} p95 {:.4}",
            n.min, n.max, n.mean, n.p50, n.p95
        ));
    }
    if let Some(s) = &c.string {
        line.push_str(&format!(
            "  len {}..{} mean {:.1}",
            s.len_min, s.len_max, s.len_mean
        ));
    }
    if c.high_cardinality {
        line.push_str("  (high cardinality)");
    } else if let Some(top) = &c.top_values {
        let shown: Vec<String> = top
            .iter()
            .take(5)
            .map(|t| format!("{:?} {:.1}%", t.value, t.share * 100.0))
            .collect();
        if !shown.is_empty() {
            line.push_str(&format!("  top {}", shown.join(", ")));
        }
    }
    line
}

/// The human report for `faucet profiling show`.
pub fn render_show(rows: &[RowHistory], full: bool) -> String {
    let mut out = String::new();
    for r in rows {
        out.push_str(&format!(
            "row {} — {} run(s) in the baseline (state {})\n",
            r.row,
            r.history.runs.len(),
            r.state_key
        ));
        let Some(latest) = r.history.latest() else {
            out.push_str("  no profile recorded yet — run the pipeline to start a baseline\n\n");
            continue;
        };
        out.push_str(&format!(
            "  latest run {} at {}: {} row(s), {} column(s){}\n",
            latest.run_id,
            latest.recorded_at.format("%Y-%m-%dT%H:%M:%SZ"),
            latest.profile.rows,
            latest.profile.columns.len(),
            if latest.profile.truncated() {
                format!(" (+{} beyond max_columns)", latest.profile.skipped_columns)
            } else {
                String::new()
            }
        ));
        if latest.drift.is_empty() {
            out.push_str("  drift: none\n");
        } else {
            out.push_str(&format!("  drift: {} finding(s)\n", latest.drift.len()));
            for d in &latest.drift {
                out.push_str(&format!("    ! {d}\n"));
            }
        }
        out.push_str("  columns:\n");
        for (name, c) in &latest.profile.columns {
            out.push_str(&column_line(name, c));
            out.push('\n');
            if full {
                out.push_str(&format!(
                    "{}\n",
                    serde_json::to_string_pretty(c)
                        .unwrap_or_default()
                        .lines()
                        .map(|l| format!("      {l}"))
                        .collect::<Vec<_>>()
                        .join("\n")
                ));
            }
        }
        out.push('\n');
    }
    out
}

/// The human report for `faucet profiling reset`.
pub fn render_reset(out: &[ResetOutcome], column: Option<&str>) -> String {
    let mut s = String::new();
    for o in out {
        match (column, o.column_runs) {
            (Some(c), Some(n)) => s.push_str(&format!(
                "row {}: column {c:?} reset in {n} of {} run(s) — its baseline restarts\n",
                o.row, o.runs
            )),
            _ => s.push_str(&format!(
                "row {}: {} run(s) of profile history dropped — the baseline restarts\n",
                o.row, o.runs
            )),
        }
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::profiling::{ProfileHistory, ProfileRecord};
    use faucet_core::{DriftMetric, ProfileDrift, Profiler, ProfilingSpec};
    use serde_json::json;

    fn history() -> RowHistory {
        let mut p = Profiler::new(ProfilingSpec::default());
        p.observe_page(&[
            json!({"amount": 1.5, "region": "eu", "note": "abc", "id": "x"}),
            json!({"amount": null, "region": "us", "note": "de", "id": "y"}),
        ]);
        let mut h = ProfileHistory::default();
        h.record(
            ProfileRecord {
                run_id: "run-1".into(),
                recorded_at: chrono::Utc::now(),
                profile: p.finish(),
                drift: vec![ProfileDrift {
                    column: "amount".into(),
                    metric: DriftMetric::NullRate,
                    observed: 0.5,
                    baseline: Some(0.0),
                    value: None,
                    detail: "jumped".into(),
                }],
            },
            5,
        );
        RowHistory {
            row: "default".into(),
            state_key: "p::default".into(),
            history: h,
        }
    }

    #[test]
    fn show_renders_summary_drift_and_columns() {
        let text = render_show(&[history()], false);
        assert!(text.contains("row default — 1 run(s)"), "{text}");
        assert!(text.contains("drift: 1 finding(s)"));
        assert!(text.contains("! amount.null_rate: jumped"));
        assert!(text.contains("null  50.0%"), "{text}");
        assert!(text.contains("min 1.5 max 1.5"));
        assert!(text.contains("len 2..3"));
        assert!(text.contains("top \"eu\" 50.0%"));
        let full = render_show(&[history()], true);
        assert!(full.contains("\"null_rate\""), "{full}");
    }

    #[test]
    fn show_handles_an_empty_history() {
        let empty = RowHistory {
            row: "r".into(),
            state_key: "k".into(),
            history: ProfileHistory::default(),
        };
        let text = render_show(&[empty], false);
        assert!(text.contains("no profile recorded yet"));
    }

    #[test]
    fn reset_wording_for_whole_and_column() {
        let whole = render_reset(
            &[ResetOutcome {
                row: "r".into(),
                runs: 3,
                column_runs: None,
            }],
            None,
        );
        assert!(whole.contains("3 run(s) of profile history dropped"));
        let col = render_reset(
            &[ResetOutcome {
                row: "r".into(),
                runs: 3,
                column_runs: Some(2),
            }],
            Some("amount"),
        );
        assert!(col.contains("column \"amount\" reset in 2 of 3 run(s)"));
    }

    #[test]
    fn column_line_marks_high_cardinality_and_all_null() {
        let mut p = Profiler::new(ProfilingSpec {
            categorical_max_distinct: 1,
            ..Default::default()
        });
        p.observe_page(&[json!({"id": "a", "n": null}), json!({"id": "b", "n": null})]);
        let rp = p.finish();
        assert!(column_line("id", &rp.columns["id"]).contains("(high cardinality)"));
        assert!(column_line("n", &rp.columns["n"]).contains("[null]"));
    }
}
