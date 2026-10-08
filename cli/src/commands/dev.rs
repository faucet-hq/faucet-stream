//! `faucet dev` — a watch-and-diff authoring loop (#283, `cli-dev` feature).
//!
//! Re-runs a bounded sample through the offline pipeline harness
//! (`pipeline_test::run_case` — transforms → quality → contract, zero real sink
//! writes) on every config save and prints the resulting schema, quality/DLQ
//! counts, errors, and a **diff vs the previous run**. Fast and offline by
//! default (`--sample <fixture>`); `--live --limit N` pulls a capped, read-only
//! sample from the real source instead.
//!
//! Only the filesystem-watch glue touches the OS; the decision logic
//! (`diff_records`, `referenced_paths`, `should_refire`) is pure and unit-tested.

use crate::cli::DevArgs;
use crate::error::{CliError, CliResult};
use crate::pipeline_test::runner::run_case;
use serde_json::Value;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// A record-level diff between two runs' output.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct RecordDiff {
    pub added: Vec<String>,
    pub removed: Vec<String>,
    pub kept: usize,
}

/// Diff two output record sets by their canonical JSON (order-insensitive).
pub fn diff_records(prev: &[Value], curr: &[Value]) -> RecordDiff {
    let ser = |v: &Value| serde_json::to_string(v).unwrap_or_default();
    let prev_set: BTreeSet<String> = prev.iter().map(ser).collect();
    let curr_set: BTreeSet<String> = curr.iter().map(ser).collect();
    RecordDiff {
        added: curr_set.difference(&prev_set).cloned().collect(),
        removed: prev_set.difference(&curr_set).cloned().collect(),
        kept: prev_set.intersection(&curr_set).count(),
    }
}

/// Shallowly extract the paths a config references so we can watch them too:
/// `extends:` targets (string or list) and `!include <path>` tags. Relative
/// paths resolve against `dir`. Pure over `(dir, text)`.
pub fn referenced_paths(dir: &Path, text: &str) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut in_extends_list = false;
    for line in text.lines() {
        let trimmed = line.trim();
        // `!include <path>` anywhere on a line.
        if let Some(idx) = trimmed.find("!include") {
            let rest = trimmed[idx + "!include".len()..]
                .trim()
                .trim_matches(['"', '\'']);
            if !rest.is_empty() {
                out.push(dir.join(rest));
            }
        }
        // `extends: <path>` or an `extends:` list.
        if let Some(rest) = trimmed.strip_prefix("extends:") {
            in_extends_list = false;
            let rest = rest.trim().trim_matches(['"', '\'']);
            if rest.is_empty() {
                in_extends_list = true;
            } else if !rest.starts_with('[') {
                out.push(dir.join(rest));
            } else {
                for item in rest.trim_matches(['[', ']']).split(',') {
                    let p = item.trim().trim_matches(['"', '\'']);
                    if !p.is_empty() {
                        out.push(dir.join(p));
                    }
                }
            }
            continue;
        }
        if in_extends_list {
            if let Some(item) = trimmed.strip_prefix('-') {
                let p = item.trim().trim_matches(['"', '\'']);
                if !p.is_empty() {
                    out.push(dir.join(p));
                }
            } else if !trimmed.is_empty() {
                in_extends_list = false;
            }
        }
    }
    out
}

/// Leading-edge debounce: fire only if at least `min_gap` has elapsed since the
/// last fire. Pure.
pub fn should_refire(last: Option<Instant>, now: Instant, min_gap: Duration) -> bool {
    match last {
        None => true,
        Some(t) => now.duration_since(t) >= min_gap,
    }
}

/// Wait until no event has arrived for `quiet`. `false` when the watcher
/// channel closed.
async fn wait_quiet(rx: &mut tokio::sync::mpsc::UnboundedReceiver<()>, quiet: Duration) -> bool {
    loop {
        match tokio::time::timeout(quiet, rx.recv()).await {
            Ok(Some(())) => continue,
            Ok(None) => return false,
            Err(_) => return true,
        }
    }
}

/// Load an offline sample fixture (`.jsonl` or `.json` array).
fn read_sample(path: &Path) -> CliResult<Vec<Value>> {
    let text = std::fs::read_to_string(path)?;
    let t = text.trim_start();
    if t.starts_with('[') {
        serde_json::from_str(t)
            .map_err(|e| CliError::Config(format!("invalid --sample `{}`: {e}", path.display())))
    } else {
        text.lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| serde_json::from_str(l).map_err(|e| CliError::Config(e.to_string())))
            .collect()
    }
}

/// Run the selected row once through the offline harness and return its output
/// records (+ any run error string). Reused for the initial run and each save.
async fn run_once(
    args: &DevArgs,
    sample: &[Value],
) -> CliResult<(Vec<Value>, Option<String>, usize)> {
    let cfg = crate::config::PipelineConfig::from_path_tolerating_secrets(
        &args.config,
        args.profile.as_deref(),
    )?;
    let nodes = crate::expand::expand(&cfg)?;
    let node = crate::commands::plan::select_root(&nodes, args.row.as_deref())?;
    let clock = chrono::Utc::now().fixed_offset();
    let case = crate::commands::plan::resolved_case_from_node(node, sample.to_vec(), clock);
    let run = run_case(&case).await?;
    Ok((run.written, run.error, run.dlq_payloads.len()))
}

fn render(prev: Option<&[Value]>, curr: &[Value], error: &Option<String>, dlq: usize) {
    let schema = faucet_core::schema::infer_schema(curr);
    let cols = schema
        .get("properties")
        .and_then(Value::as_object)
        .map(|o| o.keys().cloned().collect::<Vec<_>>().join(", "))
        .unwrap_or_default();
    println!("── run @ {} ─────────────", short_now());
    println!("  {} record(s) out, {} to DLQ", curr.len(), dlq);
    println!("  schema: {{ {cols} }}");
    if let Some(err) = error {
        println!("  ⚠ run error: {err}");
    }
    if let Some(prev) = prev {
        let d = diff_records(prev, curr);
        println!(
            "  diff vs previous: +{} -{} ={}",
            d.added.len(),
            d.removed.len(),
            d.kept
        );
        for a in d.added.iter().take(3) {
            println!("    + {a}");
        }
        for r in d.removed.iter().take(3) {
            println!("    - {r}");
        }
    }
}

fn short_now() -> String {
    chrono::Utc::now().format("%H:%M:%S").to_string()
}

/// Execute the `dev` subcommand.
pub async fn run(args: DevArgs) -> CliResult<()> {
    use std::io::IsTerminal;

    let sample = match &args.sample {
        Some(p) => read_sample(p)?,
        None => {
            return Err(CliError::Config(
                "faucet dev needs an offline sample: pass --sample <fixture.jsonl>".to_owned(),
            ));
        }
    };

    // Initial run.
    let (mut prev, err, dlq) = run_once(&args, &sample).await?;
    render(None, &prev, &err, dlq);

    // Non-interactive or --once: single shot.
    if args.once || !std::io::stdin().is_terminal() {
        println!("(single run — not watching: pass a TTY and omit --once to watch)");
        return Ok(());
    }

    watch_loop(args, sample, &mut prev).await
}

/// The filesystem-watch loop (the only OS-touching part).
async fn watch_loop(args: DevArgs, sample: Vec<Value>, prev: &mut Vec<Value>) -> CliResult<()> {
    use notify_fs::{RecursiveMode, Watcher};

    // Watch the config file's directory plus the directories of any referenced
    // (`extends:` / `!include`) fragments, so editing an include re-triggers.
    let cfg_dir = args
        .config
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    let text = std::fs::read_to_string(&args.config).unwrap_or_default();
    let mut dirs: BTreeSet<PathBuf> = BTreeSet::new();
    dirs.insert(cfg_dir.clone());
    for p in referenced_paths(&cfg_dir, &text) {
        if let Some(d) = p.parent() {
            dirs.insert(d.to_path_buf());
        }
    }

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<()>();
    let mut watcher =
        notify_fs::recommended_watcher(move |res: notify_fs::Result<notify_fs::Event>| {
            if res.is_ok() {
                let _ = tx.send(());
            }
        })
        .map_err(|e| CliError::Config(format!("failed to start file watcher: {e}")))?;
    for d in &dirs {
        watcher
            .watch(d, RecursiveMode::NonRecursive)
            .map_err(|e| CliError::Config(format!("failed to watch {}: {e}", d.display())))?;
    }

    println!(
        "\nwatching {} director{} — edit the config to re-run (Ctrl-C to stop)",
        dirs.len(),
        if dirs.len() == 1 { "y" } else { "ies" }
    );

    let debounce = Duration::from_millis(args.debounce_ms);
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                println!("\nstopping.");
                return Ok(());
            }
            recv = rx.recv() => {
                if recv.is_none() {
                    return Ok(());
                }
                // Trailing-edge debounce: run once the files have been quiet
                // for `debounce`, so the last save of a burst is what runs.
                // Events during the run stay queued and trigger another.
                if !wait_quiet(&mut rx, debounce).await {
                    return Ok(());
                }
                match run_once(&args, &sample).await {
                    Ok((curr, err, dlq)) => {
                        render(Some(prev), &curr, &err, dlq);
                        *prev = curr;
                    }
                    Err(e) => println!("  ⚠ reload failed: {e}"),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// The run waits for the burst to end, so a save inside the debounce
    /// window is never dropped (#789 CLI-156).
    #[tokio::test]
    async fn the_debounce_waits_for_the_last_save() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<()>();
        let sender = tokio::spawn(async move {
            for _ in 0..3 {
                tx.send(()).unwrap();
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            tx
        });
        let start = Instant::now();
        assert!(wait_quiet(&mut rx, Duration::from_millis(80)).await);
        assert!(start.elapsed() >= Duration::from_millis(100), "{:?}", start.elapsed());
        let tx = sender.await.unwrap();
        drop(tx);
        assert!(!wait_quiet(&mut rx, Duration::from_millis(80)).await);
    }

    #[test]
    fn diff_records_detects_add_remove_keep() {
        let prev = vec![json!({"a": 1}), json!({"a": 2})];
        let curr = vec![json!({"a": 2}), json!({"a": 3})];
        let d = diff_records(&prev, &curr);
        assert_eq!(d.kept, 1);
        assert_eq!(d.added.len(), 1);
        assert_eq!(d.removed.len(), 1);
        assert!(d.added[0].contains("\"a\":3"));
        assert!(d.removed[0].contains("\"a\":1"));
    }

    #[test]
    fn referenced_paths_extracts_extends_and_include() {
        let dir = Path::new("/cfg");
        let text = "extends: base.yaml\npipeline:\n  source: !include src.yaml\n";
        let paths = referenced_paths(dir, text);
        assert!(paths.contains(&PathBuf::from("/cfg/base.yaml")));
        assert!(paths.contains(&PathBuf::from("/cfg/src.yaml")));
    }

    #[test]
    fn referenced_paths_handles_extends_list() {
        let dir = Path::new("/cfg");
        let text = "extends:\n  - base1.yaml\n  - base2.yaml\nname: x\n";
        let paths = referenced_paths(dir, text);
        assert!(paths.contains(&PathBuf::from("/cfg/base1.yaml")));
        assert!(paths.contains(&PathBuf::from("/cfg/base2.yaml")));
    }

    #[test]
    fn should_refire_respects_min_gap() {
        let now = Instant::now();
        assert!(should_refire(None, now, Duration::from_millis(100)));
        assert!(!should_refire(Some(now), now, Duration::from_millis(100)));
        assert!(should_refire(
            Some(now - Duration::from_millis(200)),
            now,
            Duration::from_millis(100)
        ));
    }

    #[tokio::test]
    async fn run_once_produces_output_offline() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = dir.path().join("p.yaml");
        std::fs::write(
            &cfg,
            "version: 1\npipeline:\n  source:\n    type: csv\n    config:\n      path: in.csv\n  sink:\n    type: jsonl\n    config:\n      path: out.jsonl\n",
        )
        .unwrap();
        let args = DevArgs {
            config: cfg,
            row: None,
            sample: None,
            live: false,
            limit: 10,
            once: true,
            debounce_ms: 300,
            profile: None,
        };
        let (out, err, _dlq) = run_once(&args, &[json!({"a": 1})]).await.unwrap();
        assert_eq!(out.len(), 1);
        assert!(err.is_none());
    }
}
