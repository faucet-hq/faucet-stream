//! `faucet fmt` — canonicalize a pipeline config (#387).
//!
//! A deterministic, idempotent config formatter — the config analogue of
//! `cargo fmt` / `terraform fmt`. It parses the file, rewrites every object with
//! a stable key order (a curated priority order for the well-known blocks, then
//! alphabetical for everything else), and re-serializes it. Running `fmt` twice
//! is always a no-op, so `--check` is a cheap CI gate for "is this config
//! normalized?".
//!
//! Canonicalization is a **pure** `serde_json::Value → serde_json::Value`
//! transform ([`canonicalize`]); the command layer only reads/writes files and
//! renders the `--check` diff.
//!
//! **Scope:** `fmt` formats the *literal* file — it does not resolve
//! `${env:…}` / `${file:…}` interpolation, apply `!include` / `extends`
//! composition, or drop schema defaults. Use `faucet validate --show-composed`
//! to see the composed result.
//!
//! **Comments are not preserved** — the config is parsed and re-serialized, the
//! same limitation `faucet migrate` documents.

use crate::cli::FmtArgs;
use crate::error::{CliError, CliResult};
use serde_json::{Map, Value};
use std::path::{Path, PathBuf};

/// Execute the `fmt` subcommand.
pub async fn run(args: FmtArgs) -> CliResult<()> {
    let cwd = std::env::current_dir()?;
    let paths: Vec<PathBuf> = if args.configs.is_empty() {
        vec![
            crate::env_loader::discover_config_path(&cwd)
                .ok_or_else(|| CliError::Config("no config file found to format".into()))?,
        ]
    } else {
        args.configs.clone()
    };

    let mut not_canonical = 0usize;
    for path in &paths {
        let text = std::fs::read_to_string(path)
            .map_err(|e| CliError::Config(format!("cannot read '{}': {e}", path.display())))?;
        let format = ConfigFormat::from_path(path)?;
        let value = format.parse(&text, path)?;
        let formatted = format.render(&canonicalize(value), path)?;

        if args.check {
            if formatted != text {
                not_canonical += 1;
                eprintln!("{}: not canonical", path.display());
                eprint!("{}", unified_diff(&text, &formatted));
            }
            continue;
        }

        if args.stdout {
            print!("{formatted}");
            continue;
        }

        if formatted == text {
            println!("{}: already formatted", path.display());
        } else {
            std::fs::write(path, &formatted)
                .map_err(|e| CliError::Config(format!("cannot write '{}': {e}", path.display())))?;
            println!("{}: formatted", path.display());
        }
    }

    if not_canonical > 0 {
        return Err(CliError::Config(format!(
            "{not_canonical} file{} not canonical; run `faucet fmt` to format",
            if not_canonical == 1 { "" } else { "s" }
        )));
    }
    Ok(())
}

/// The on-disk serialization of a config file.
#[derive(Clone, Copy)]
enum ConfigFormat {
    Yaml,
    Json,
}

impl ConfigFormat {
    fn from_path(path: &Path) -> CliResult<Self> {
        match path.extension().and_then(|e| e.to_str()) {
            Some("yaml") | Some("yml") => Ok(ConfigFormat::Yaml),
            Some("json") => Ok(ConfigFormat::Json),
            _ => Err(CliError::Config(format!(
                "unsupported config extension for '{}' (expected .yaml/.yml/.json)",
                path.display()
            ))),
        }
    }

    fn parse(self, text: &str, path: &Path) -> CliResult<Value> {
        let err = |e: String| CliError::Config(format!("cannot parse '{}': {e}", path.display()));
        match self {
            ConfigFormat::Yaml => serde_yaml::from_str(text).map_err(|e| err(e.to_string())),
            ConfigFormat::Json => serde_json::from_str(text).map_err(|e| err(e.to_string())),
        }
    }

    fn render(self, value: &Value, path: &Path) -> CliResult<String> {
        let err =
            |e: String| CliError::Config(format!("cannot serialize '{}': {e}", path.display()));
        match self {
            ConfigFormat::Yaml => serde_yaml::to_string(value).map_err(|e| err(e.to_string())),
            ConfigFormat::Json => {
                let mut s = serde_json::to_string_pretty(value).map_err(|e| err(e.to_string()))?;
                s.push('\n');
                Ok(s)
            }
        }
    }
}

/// The canonical key order for the well-known config blocks. Keys appear in this
/// order at the front of their object; every other key follows alphabetically.
/// A single flat list works because these names are unambiguous across the nesting
/// levels they appear at (there is no top-level `type`, no connector-level
/// `version`, etc.).
const KEY_ORDER: &[&str] = &[
    // ── top level ────────────────────────────────────────────────────────────
    "kind",
    "version",
    "name",
    "vars",
    "params",
    "auth",
    "pipeline",
    "matrix",
    "execution",
    "selection",
    // ── pipeline children ──────────────────────────────────────────────────────
    "sources",
    "source",
    "sinks",
    "sink",
    "transforms",
    "state",
    // ── connector / matrix-row block children ──────────────────────────────────
    "id",
    "parent",
    "parent_key",
    "depends_on",
    "type",
    "ref",
    "status",
    "tags",
    "inherit_transforms",
    "config",
    // ── remaining top-level blocks (kept deterministic, after the canonical set) ─
    "delivery",
    "resilience",
    "sla",
    "reconcile",
    "verify",
    "rollback",
    "backfill",
    "mirror",
    "replication",
    "schedule",
    "notifications",
    "lineage",
    "catalog",
    "local_outputs",
    "metadata_columns",
    "profiles",
];

/// Rank of `key` in the canonical order, or `KEY_ORDER.len()` for any key not in
/// the list (which then sorts alphabetically after the ranked keys).
fn key_rank(key: &str) -> usize {
    KEY_ORDER
        .iter()
        .position(|k| *k == key)
        .unwrap_or(KEY_ORDER.len())
}

/// Rewrite `value` into canonical form: every object's keys are reordered by
/// (canonical rank, then name), recursively. Pure and idempotent — running it on
/// its own output is a no-op. Relies on `serde_json`'s `preserve_order` feature
/// (enabled workspace-wide) so the rebuilt insertion order survives serialization.
pub fn canonicalize(value: Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut entries: Vec<(String, Value)> =
                map.into_iter().map(|(k, v)| (k, canonicalize(v))).collect();
            entries.sort_by(|(a, _), (b, _)| key_rank(a).cmp(&key_rank(b)).then_with(|| a.cmp(b)));
            Value::Object(entries.into_iter().collect::<Map<String, Value>>())
        }
        Value::Array(items) => Value::Array(items.into_iter().map(canonicalize).collect()),
        scalar => scalar,
    }
}

/// A minimal LCS-based unified-ish line diff, used only to show what `--check`
/// would change. Lines present in both are context; removed lines are `-` and
/// added lines are `+`. Not a git-grade diff, but deterministic and enough to
/// point a reviewer at the reordering.
fn unified_diff(old: &str, new: &str) -> String {
    let a: Vec<&str> = old.lines().collect();
    let b: Vec<&str> = new.lines().collect();
    // LCS table.
    let (n, m) = (a.len(), b.len());
    let mut lcs = vec![vec![0usize; m + 1]; n + 1];
    for i in (0..n).rev() {
        for j in (0..m).rev() {
            lcs[i][j] = if a[i] == b[j] {
                lcs[i + 1][j + 1] + 1
            } else {
                lcs[i + 1][j].max(lcs[i][j + 1])
            };
        }
    }
    let mut out = String::new();
    let (mut i, mut j) = (0, 0);
    while i < n && j < m {
        if a[i] == b[j] {
            out.push_str(&format!("  {}\n", a[i]));
            i += 1;
            j += 1;
        } else if lcs[i + 1][j] >= lcs[i][j + 1] {
            out.push_str(&format!("- {}\n", a[i]));
            i += 1;
        } else {
            out.push_str(&format!("+ {}\n", b[j]));
            j += 1;
        }
    }
    while i < n {
        out.push_str(&format!("- {}\n", a[i]));
        i += 1;
    }
    while j < m {
        out.push_str(&format!("+ {}\n", b[j]));
        j += 1;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn reorders_top_level_keys_canonically() {
        let v = json!({
            "matrix": [],
            "pipeline": { "sink": {}, "source": {} },
            "name": "demo",
            "version": 1,
        });
        let out = canonicalize(v);
        let keys: Vec<&String> = out.as_object().unwrap().keys().collect();
        assert_eq!(keys, ["version", "name", "pipeline", "matrix"]);
        // Nested pipeline is reordered too: source before sink.
        let pk: Vec<&String> = out["pipeline"].as_object().unwrap().keys().collect();
        assert_eq!(pk, ["source", "sink"]);
    }

    #[test]
    fn connector_block_puts_type_before_config_and_sorts_rest() {
        let v = json!({
            "source": { "config": { "url": "x", "auth": {} }, "type": "rest", "status": "active" }
        });
        let out = canonicalize(v);
        let sk: Vec<&String> = out["source"].as_object().unwrap().keys().collect();
        assert_eq!(sk, ["type", "status", "config"]);
    }

    #[test]
    fn unknown_keys_sort_alphabetically_after_ranked_keys() {
        let v = json!({ "config": { "zebra": 1, "alpha": 2, "mango": 3 } });
        let out = canonicalize(v);
        let ck: Vec<&String> = out["config"].as_object().unwrap().keys().collect();
        assert_eq!(ck, ["alpha", "mango", "zebra"]);
    }

    #[test]
    fn canonicalize_is_idempotent() {
        let v = json!({
            "version": 1,
            "pipeline": { "sink": { "type": "jsonl", "config": { "b": 1, "a": 2 } },
                          "source": { "config": {}, "type": "rest" } },
            "name": "x",
        });
        let once = canonicalize(v);
        let twice = canonicalize(once.clone());
        assert_eq!(once, twice);
    }

    #[test]
    fn config_format_from_path() {
        assert!(matches!(
            ConfigFormat::from_path(Path::new("a.yaml")),
            Ok(ConfigFormat::Yaml)
        ));
        assert!(matches!(
            ConfigFormat::from_path(Path::new("a.json")),
            Ok(ConfigFormat::Json)
        ));
        assert!(ConfigFormat::from_path(Path::new("a.toml")).is_err());
    }

    #[test]
    fn render_yaml_is_byte_stable_across_two_passes() {
        let p = Path::new("f.yaml");
        let v = ConfigFormat::Yaml
            .parse(
                "pipeline:\n  sink: {}\n  source: {}\nname: d\nversion: 1\n",
                p,
            )
            .unwrap();
        let once = ConfigFormat::Yaml.render(&canonicalize(v), p).unwrap();
        let reparsed = ConfigFormat::Yaml.parse(&once, p).unwrap();
        let twice = ConfigFormat::Yaml
            .render(&canonicalize(reparsed), p)
            .unwrap();
        assert_eq!(once, twice, "fmt must be idempotent at the byte level");
        assert!(once.starts_with("version: 1"), "{once}");
    }

    #[test]
    fn unified_diff_marks_added_and_removed_lines() {
        let d = unified_diff("a\nb\nc\n", "a\nx\nc\n");
        assert!(d.contains("- b"), "{d}");
        assert!(d.contains("+ x"), "{d}");
        assert!(d.contains("  a"), "{d}");
    }

    fn write_tmp(name: &str, body: &str) -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join(name);
        std::fs::write(&path, body).expect("write");
        (dir, path)
    }

    const UNSORTED: &str = "name: demo\nversion: 1\npipeline:\n  sink: { type: jsonl, config: { path: o } }\n  source: { type: rest, config: {} }\n";

    #[tokio::test]
    async fn run_rewrites_file_in_place_and_is_idempotent() {
        let (_d, path) = write_tmp("f.yaml", UNSORTED);
        run(FmtArgs {
            configs: vec![path.clone()],
            check: false,
            stdout: false,
        })
        .await
        .unwrap();
        let after = std::fs::read_to_string(&path).unwrap();
        assert!(after.starts_with("version: 1"), "{after}");
        // Second pass leaves it byte-identical.
        run(FmtArgs {
            configs: vec![path.clone()],
            check: false,
            stdout: false,
        })
        .await
        .unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), after);
    }

    #[tokio::test]
    async fn run_check_fails_on_unsorted_passes_on_canonical() {
        let (_d, path) = write_tmp("f.yaml", UNSORTED);
        assert!(
            run(FmtArgs {
                configs: vec![path.clone()],
                check: true,
                stdout: false,
            })
            .await
            .is_err(),
            "--check must fail on a non-canonical file"
        );
        // Format it, then --check passes.
        run(FmtArgs {
            configs: vec![path.clone()],
            check: false,
            stdout: false,
        })
        .await
        .unwrap();
        run(FmtArgs {
            configs: vec![path],
            check: true,
            stdout: false,
        })
        .await
        .expect("--check passes on a canonical file");
    }

    #[tokio::test]
    async fn run_stdout_leaves_file_untouched() {
        let (_d, path) = write_tmp("f.yaml", UNSORTED);
        run(FmtArgs {
            configs: vec![path.clone()],
            check: false,
            stdout: true,
        })
        .await
        .unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), UNSORTED);
    }
}
