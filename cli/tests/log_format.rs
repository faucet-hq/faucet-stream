//! #634 — `--log-format json` emits one parseable JSON object per line.
//!
//! The point of the flag is that a log pipeline can ingest faucet's output
//! without regex parsing, so the test parses it the way that pipeline would:
//! every stderr line must be a JSON object carrying the fields an operator
//! filters on. Asserting on substrings would pass for text that merely
//! contains braces.

use assert_cmd::Command;
use serde_json::Value;
use std::fs;
use tempfile::TempDir;

/// A minimal pipeline that actually moves a row, so the run emits real
/// records-written logs rather than only startup chatter.
fn config(dir: &std::path::Path) -> String {
    let csv = dir.join("in.csv");
    fs::write(&csv, "id,name\n1,ada\n2,grace\n").unwrap();
    let out = dir.join("out.jsonl");
    format!(
        r#"version: 1
name: log-format-test
pipeline:
  source:
    type: csv
    config: {{ path: "{}", has_header: true }}
  sink:
    type: jsonl
    config: {{ path: "{}" }}
"#,
        csv.display(),
        out.display()
    )
}

fn run_with(args: &[&str], cfg: &std::path::Path) -> String {
    let out = Command::cargo_bin("faucet")
        .unwrap()
        .args(args)
        .arg(cfg)
        .output()
        .expect("faucet runs");
    assert!(
        out.status.success(),
        "run failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stderr).into_owned()
}

#[test]
fn json_format_emits_one_parseable_object_per_line() {
    let dir = TempDir::new().unwrap();
    let cfg = dir.path().join("p.yaml");
    fs::write(&cfg, config(dir.path())).unwrap();

    let stderr = run_with(
        &["run", "--log-format", "json", "--log-level", "info"],
        &cfg,
    );

    let lines: Vec<&str> = stderr.lines().filter(|l| !l.trim().is_empty()).collect();
    assert!(!lines.is_empty(), "the run must log something");

    for line in &lines {
        let v: Value = serde_json::from_str(line)
            .unwrap_or_else(|e| panic!("every line must be one JSON object: {e}\nline: {line}"));
        assert!(v.is_object(), "not an object: {line}");
        // The three fields any log pipeline keys on. `flatten_event` puts the
        // message at the top level rather than under `fields`.
        for key in ["timestamp", "level", "message"] {
            assert!(
                v.get(key).is_some(),
                "missing `{key}`, which is what makes this ingestible: {line}"
            );
        }
    }
}

#[test]
fn text_is_the_default_and_is_not_json() {
    // The default must stay human-readable — a silent switch would break every
    // existing log consumer.
    let dir = TempDir::new().unwrap();
    let cfg = dir.path().join("p.yaml");
    fs::write(&cfg, config(dir.path())).unwrap();

    let stderr = run_with(&["run", "--log-level", "info"], &cfg);
    let parsed_as_json = stderr
        .lines()
        .filter(|l| !l.trim().is_empty())
        .any(|l| serde_json::from_str::<Value>(l).is_ok());
    assert!(
        !parsed_as_json,
        "default output must be text, got JSON-parseable lines:\n{stderr}"
    );
}

#[test]
fn the_env_var_selects_the_format_too() {
    // Orchestrators set this once in the environment rather than editing every
    // command line.
    let dir = TempDir::new().unwrap();
    let cfg = dir.path().join("p.yaml");
    fs::write(&cfg, config(dir.path())).unwrap();

    let out = Command::cargo_bin("faucet")
        .unwrap()
        .env("FAUCET_LOG_FORMAT", "json")
        .args(["run", "--log-level", "info"])
        .arg(&cfg)
        .output()
        .expect("faucet runs");
    assert!(out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    let first = stderr
        .lines()
        .find(|l| !l.trim().is_empty())
        .expect("some output");
    serde_json::from_str::<Value>(first)
        .unwrap_or_else(|e| panic!("FAUCET_LOG_FORMAT=json must switch the format: {e}\n{first}"));
}
