//! Integration tests for `faucet run --from-env` (pure-env mode, issue #42).
#![allow(unsafe_code)] // tests set process environment variables

use assert_cmd::Command;
use predicates::prelude::*;
use serial_test::serial;
use std::fs;

/// Clear every `FAUCET_*` env var in this process before a test runs so leftover
/// state from earlier tests can't cross-contaminate.
fn clear_faucet_env() {
    let to_clear: Vec<String> = std::env::vars()
        .map(|(k, _)| k)
        .filter(|k| k.starts_with("FAUCET_"))
        .collect();
    for k in to_clear {
        unsafe { std::env::remove_var(&k) };
    }
}

#[test]
#[serial]
fn csv_to_jsonl_via_env() {
    clear_faucet_env();
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.csv");
    let output = dir.path().join("out.jsonl");
    fs::write(&input, "id,name\n1,alice\n2,bob\n").unwrap();

    Command::cargo_bin("faucet")
        .unwrap()
        .env_clear()
        .env("PATH", std::env::var("PATH").unwrap_or_default())
        .env("FAUCET_SOURCE", "csv")
        .env("FAUCET_SOURCE_CSV_PATH", &input)
        .env("FAUCET_SINK", "jsonl")
        .env("FAUCET_SINK_JSONL_PATH", &output)
        .args(["run", "--from-env"])
        .assert()
        .success();

    let written = fs::read_to_string(&output).unwrap();
    let records: Vec<serde_json::Value> = written
        .lines()
        .filter(|l| !l.is_empty())
        .map(|l| serde_json::from_str::<serde_json::Value>(l).expect("line is valid JSON"))
        .collect();
    assert_eq!(records.len(), 2, "expected 2 records, got {records:?}");
    assert_eq!(records[0]["name"], "alice");
    assert_eq!(records[0]["id"], "1");
    assert_eq!(records[1]["name"], "bob");
    assert_eq!(records[1]["id"], "2");
}

#[test]
#[serial]
fn env_file_loads_before_run() {
    clear_faucet_env();
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.csv");
    let output = dir.path().join("out.jsonl");
    let envfile = dir.path().join("pipeline.env");
    fs::write(&input, "id\n1\n").unwrap();
    fs::write(
        &envfile,
        format!(
            "FAUCET_SOURCE=csv\nFAUCET_SOURCE_CSV_PATH={}\nFAUCET_SINK=jsonl\nFAUCET_SINK_JSONL_PATH={}\n",
            input.display(),
            output.display(),
        ),
    )
    .unwrap();

    Command::cargo_bin("faucet")
        .unwrap()
        .env_clear()
        .env("PATH", std::env::var("PATH").unwrap_or_default())
        .args(["run", "--from-env", "--env-file"])
        .arg(&envfile)
        .assert()
        .success();

    assert!(output.exists());
}

#[test]
#[serial]
fn from_env_and_config_path_are_mutually_exclusive() {
    clear_faucet_env();
    let dir = tempfile::tempdir().unwrap();
    let yaml = dir.path().join("pipe.yaml");
    fs::write(&yaml, "version: 1\npipeline:\n  source: {type: csv, config: {path: x}}\n  sink: {type: jsonl, config: {path: y}}\n").unwrap();

    Command::cargo_bin("faucet")
        .unwrap()
        .env_clear()
        .env("PATH", std::env::var("PATH").unwrap_or_default())
        .args(["run", "--from-env"])
        .arg(&yaml)
        .assert()
        .failure()
        .stderr(predicate::str::contains("cannot be used with"));
}

#[test]
#[serial]
fn missing_selector_errors_clearly() {
    clear_faucet_env();
    Command::cargo_bin("faucet")
        .unwrap()
        .env_clear()
        .env("PATH", std::env::var("PATH").unwrap_or_default())
        .args(["run", "--from-env"])
        .assert()
        .failure()
        .stderr(predicates::str::contains("FAUCET_SOURCE"));
}

#[test]
#[serial]
fn env_file_works_in_yaml_mode() {
    // Lifting the old `--env-file` ↔ `--from-env` gate (#55): an explicit
    // .env file must be loaded for `${env:VAR}` interpolation in YAML mode.
    clear_faucet_env();
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.csv");
    let output = dir.path().join("out.jsonl");
    let envfile = dir.path().join("pipe.env");
    fs::write(&input, "name\nalice\n").unwrap();
    fs::write(
        &envfile,
        format!("FAUCET_TEST_OUT_PATH={}\n", output.display()),
    )
    .unwrap();
    let yaml = dir.path().join("pipe.yaml");
    fs::write(
        &yaml,
        format!(
            r#"version: 1
pipeline:
  source:
    type: csv
    config:
      path: {input}
  sink:
    type: jsonl
    config:
      path: ${{env:FAUCET_TEST_OUT_PATH}}
"#,
            input = input.display(),
        ),
    )
    .unwrap();

    Command::cargo_bin("faucet")
        .unwrap()
        .env_clear()
        .env("PATH", std::env::var("PATH").unwrap_or_default())
        .args(["run", "--env-file"])
        .arg(&envfile)
        .arg(&yaml)
        .assert()
        .success();

    assert!(output.exists());
}

#[test]
#[serial]
fn missing_env_file_path_errors() {
    clear_faucet_env();
    Command::cargo_bin("faucet")
        .unwrap()
        .env_clear()
        .env("PATH", std::env::var("PATH").unwrap_or_default())
        .args([
            "run",
            "--env-file",
            "/path/does/not/exist.env",
            "--from-env",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("does not exist"));
}
