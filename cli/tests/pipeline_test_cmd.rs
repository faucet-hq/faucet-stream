//! `faucet test` end-to-end: passing and failing specs report correctly with
//! proper exit codes, DLQ/quality/contract outcomes are assertable, `--json`
//! emits a parseable report, and `faucet schema test` prints the spec schema.

use assert_cmd::Command;
use predicates::str::contains;
use std::fs;
use std::path::Path;
use tempfile::TempDir;

fn write(dir: &Path, name: &str, body: &str) -> std::path::PathBuf {
    let p = dir.join(name);
    fs::write(&p, body).unwrap();
    p
}

fn faucet() -> Command {
    let mut cmd = Command::cargo_bin("faucet").unwrap();
    // Never pick up a developer's .env from the test cwd.
    cmd.arg("test").arg("--no-env-file");
    cmd
}

#[test]
fn passing_inline_spec_exits_zero() {
    let dir = TempDir::new().unwrap();
    let spec = write(
        dir.path(),
        "spec.yaml",
        r#"
version: 1
tests:
  - name: flatten and stamp
    pipeline:
      transforms:
        - type: flatten
          config: { separator: "_" }
        - type: set
          config: { values: { day: "${now.date}" } }
    clock: 2026-02-01T00:00:00Z
    input:
      - { user: { name: Ada } }
    expect:
      records:
        - { user_name: Ada, day: "2026-02-01" }
"#,
    );
    faucet()
        .arg(&spec)
        .assert()
        .success()
        .stdout(contains("✓ flatten and stamp"))
        .stdout(contains("1 test, 1 passed, 0 failed"));
}

#[test]
fn failing_case_exits_with_failure_count_and_diff() {
    let dir = TempDir::new().unwrap();
    let spec = write(
        dir.path(),
        "spec.yaml",
        r#"
version: 1
tests:
  - name: ok
    pipeline: {}
    input: [ { a: 1 } ]
    expect: { records_written: 1 }
  - name: wrong value
    pipeline: {}
    input: [ { a: 1 } ]
    expect: { records: [ { a: 2 } ] }
  - name: wrong count
    pipeline: {}
    input: [ { a: 1 } ]
    expect: { records_written: 5 }
"#,
    );
    faucet()
        .arg(&spec)
        .assert()
        .code(2) // two failed cases → exit 2
        .stdout(contains("✓ ok"))
        .stdout(contains("✗ wrong value"))
        .stdout(contains("records[0].a: expected 2, got 1"))
        .stdout(contains("records_written: expected 5, got 1"))
        .stdout(contains("3 tests, 1 passed, 2 failed"));
}

#[test]
fn json_report_is_machine_readable() {
    let dir = TempDir::new().unwrap();
    let spec = write(
        dir.path(),
        "spec.yaml",
        r#"
version: 1
tests:
  - name: pass case
    pipeline: {}
    input: [ { a: 1 } ]
    expect: { records_written: 1 }
  - name: fail case
    pipeline: {}
    input: [ { a: 1 } ]
    expect: { records_written: 0 }
"#,
    );
    let output = faucet().arg(&spec).arg("--json").output().unwrap();
    assert_eq!(output.status.code(), Some(1));
    let v: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(v["total"], 2);
    assert_eq!(v["passed"], 1);
    assert_eq!(v["failed"], 1);
    assert_eq!(v["tests"][0]["status"], "pass");
    assert_eq!(v["tests"][1]["status"], "fail");
    assert!(
        v["tests"][1]["failures"][0]
            .as_str()
            .unwrap()
            .contains("records_written")
    );
}

#[test]
fn config_file_quality_quarantine_routes_to_dlq() {
    let dir = TempDir::new().unwrap();
    // A realistic config: the csv source / jsonl sink are never touched by
    // `faucet test` — only transforms + quality run against the fixtures.
    write(
        dir.path(),
        "pipeline.yaml",
        r#"
version: 1
name: orders
pipeline:
  source: { type: csv, config: { path: ./never-read.csv } }
  sink:   { type: jsonl, config: { path: ./never-written.jsonl } }
  transforms:
    - type: keys_case
      config: { mode: snake }
  quality:
    record:
      - { type: not_null, field: order_id, on_failure: quarantine }
  dlq:
    sink: { type: stdout, config: {} }
"#,
    );
    let spec = write(
        dir.path(),
        "spec.yaml",
        r#"
version: 1
tests:
  - name: null ids quarantined
    config: pipeline.yaml
    input:
      - { OrderId: 1 }
      - { OrderId: null }
    expect:
      records: [ { order_id: 1 } ]
      dlq: [ { order_id: null } ]
      dlq_count: 1
"#,
    );
    faucet()
        .arg(&spec)
        .assert()
        .success()
        .stdout(contains("✓ null ids quarantined"));
}

#[test]
fn contract_fail_is_assertable_via_expect_error() {
    let dir = TempDir::new().unwrap();
    let spec = write(
        dir.path(),
        "spec.yaml",
        r#"
version: 1
tests:
  - name: breach aborts
    pipeline:
      contract:
        version: "2.0.0"
        fields:
          - { name: id, type: integer, required: true }
    input: [ { id: not-an-int } ]
    expect:
      error: "Contract v2.0.0 violated"
      records_written: 0
"#,
    );
    faucet().arg(&spec).assert().success();
}

#[test]
fn fixture_file_input_and_subset_unordered_matching() {
    let dir = TempDir::new().unwrap();
    fs::create_dir(dir.path().join("fixtures")).unwrap();
    write(
        &dir.path().join("fixtures"),
        "records.jsonl",
        "{\"id\":1,\"noise\":\"x\"}\n{\"id\":2,\"noise\":\"y\"}\n",
    );
    let spec = write(
        dir.path(),
        "spec.yaml",
        r#"
version: 1
tests:
  - name: subset unordered
    pipeline: {}
    input: fixtures/records.jsonl
    expect:
      match: subset
      unordered: true
      records:
        - { id: 2 }
        - { id: 1 }
"#,
    );
    faucet().arg(&spec).assert().success();
}

#[test]
fn matrix_config_requires_row_and_selects_it() {
    let dir = TempDir::new().unwrap();
    write(
        dir.path(),
        "pipeline.yaml",
        r#"
version: 1
pipeline:
  source: { type: csv, config: { path: ./in.csv } }
  sink:   { type: jsonl, config: { path: ./out.jsonl } }
matrix:
  - id: plain
  - id: shaped
    transforms:
      - type: set
        config: { values: { shaped: true } }
"#,
    );
    let no_row = write(
        dir.path(),
        "no_row.yaml",
        r#"
version: 1
tests:
  - name: ambiguous
    config: pipeline.yaml
    input: [ { a: 1 } ]
    expect: { records_written: 1 }
"#,
    );
    faucet()
        .arg(&no_row)
        .assert()
        .code(1)
        .stderr(contains("set `row` to one of"))
        .stderr(contains("plain"))
        .stderr(contains("shaped"));

    let with_row = write(
        dir.path(),
        "with_row.yaml",
        r#"
version: 1
tests:
  - name: shaped row applies row transforms
    config: pipeline.yaml
    row: shaped
    input: [ { a: 1 } ]
    expect:
      records: [ { a: 1, shaped: true } ]
  - name: unknown row errors
    config: pipeline.yaml
    row: nope
    input: [ { a: 1 } ]
    expect: { records_written: 1 }
"#,
    );
    // The unknown-row case is a setup error (not a test failure) — the whole
    // command errors out before reporting.
    faucet()
        .arg(&with_row)
        .assert()
        .code(1)
        .stderr(contains("row 'nope' not found"));

    faucet()
        .arg(&with_row)
        .arg("--filter")
        .arg("shaped row")
        .assert()
        .success()
        .stdout(contains("✓ shaped row applies row transforms"));
}

#[test]
fn filter_with_no_matches_errors() {
    let dir = TempDir::new().unwrap();
    let spec = write(
        dir.path(),
        "spec.yaml",
        r#"
version: 1
tests:
  - name: only case
    pipeline: {}
    input: []
    expect: { records_written: 0 }
"#,
    );
    faucet()
        .arg(&spec)
        .arg("--filter")
        .arg("does-not-exist")
        .assert()
        .code(1)
        .stderr(contains("no test cases match --filter"));
}

#[test]
fn multiple_spec_files_aggregate_into_one_report() {
    let dir = TempDir::new().unwrap();
    let a = write(
        dir.path(),
        "a.yaml",
        "version: 1\ntests: [ { name: a1, pipeline: {}, input: [], expect: { records_written: 0 } } ]\n",
    );
    let b = write(
        dir.path(),
        "b.yaml",
        "version: 1\ntests: [ { name: b1, pipeline: {}, input: [], expect: { records_written: 0 } } ]\n",
    );
    faucet()
        .arg(&a)
        .arg(&b)
        .assert()
        .success()
        .stdout(contains("a.yaml"))
        .stdout(contains("b.yaml"))
        .stdout(contains("2 tests, 2 passed, 0 failed"));
}

#[test]
fn command_clock_applies_when_case_has_none() {
    let dir = TempDir::new().unwrap();
    let spec = write(
        dir.path(),
        "spec.yaml",
        r#"
version: 1
tests:
  - name: stamped by flag clock
    pipeline:
      transforms:
        - type: set
          config: { values: { day: "${now.date}" } }
    input: [ {} ]
    expect: { records: [ { day: "2026-03-01" } ] }
"#,
    );
    faucet()
        .arg(&spec)
        .arg("--clock")
        .arg("2026-03-01")
        .assert()
        .success();
}

#[test]
fn invalid_spec_reports_config_error() {
    let dir = TempDir::new().unwrap();
    let spec = write(
        dir.path(),
        "spec.yaml",
        "version: 1\ntests: [ { name: x, input: [], expect: { records_written: 0 } } ]\n",
    );
    faucet()
        .arg(&spec)
        .assert()
        .code(1)
        .stderr(contains("one of `config`"));
}

#[test]
fn schema_test_target_prints_spec_schema() {
    Command::cargo_bin("faucet")
        .unwrap()
        .args(["schema", "test"])
        .assert()
        .success()
        .stdout(contains("\"tests\""))
        .stdout(contains("TestCase"));
}

#[test]
fn shipped_example_spec_passes() {
    // Keeps cli/examples/tests/ (and the docs that quote it) grounded: the
    // shipped example spec must always pass against its pipeline config.
    let spec = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/tests/pipeline_tests.yaml");
    faucet()
        .arg(&spec)
        .assert()
        .success()
        .stdout(contains("5 tests, 5 passed, 0 failed"));
}

#[cfg(feature = "masking")]
#[test]
fn shipped_masking_example_spec_passes() {
    // Keeps cli/examples/tests/masking_tests.yaml (and the masking cookbook
    // that quotes it) grounded.
    let spec = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/tests/masking_tests.yaml");
    faucet()
        .arg(&spec)
        .assert()
        .success()
        .stdout(contains("3 tests, 3 passed, 0 failed"));
}

#[cfg(feature = "transform-zip-columns")]
#[test]
fn shipped_zip_columns_groups_spec_passes() {
    // #746: the grouped runReport fixture (quoted by the transforms cookbook).
    let spec =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/tests/zip_columns_groups_tests.yaml");
    faucet()
        .arg(&spec)
        .assert()
        .success()
        .stdout(contains("3 tests, 3 passed, 0 failed"));
}

#[cfg(feature = "transforms")]
#[test]
fn shipped_tree_flatten_spec_passes() {
    // #530 / #746: the tree_flatten fixtures, including column groups.
    let spec = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/tests/tree_flatten_tests.yaml");
    faucet()
        .arg(&spec)
        .assert()
        .success()
        .stdout(contains("3 tests, 3 passed, 0 failed"));
}

#[test]
fn resolve_secrets_flag_loads_plain_config() {
    // --resolve-secrets on a config with no secret directives goes through the
    // async load path and behaves identically.
    let dir = TempDir::new().unwrap();
    write(
        dir.path(),
        "pipeline.yaml",
        r#"
version: 1
pipeline:
  source: { type: csv, config: { path: ./in.csv } }
  sink:   { type: jsonl, config: { path: ./out.jsonl } }
  transforms:
    - type: set
      config: { values: { ok: true } }
"#,
    );
    let spec = write(
        dir.path(),
        "spec.yaml",
        r#"
version: 1
tests:
  - name: async load path
    config: pipeline.yaml
    input: [ {} ]
    expect: { records: [ { ok: true } ] }
"#,
    );
    faucet()
        .arg(&spec)
        .arg("--resolve-secrets")
        .assert()
        .success();
}
