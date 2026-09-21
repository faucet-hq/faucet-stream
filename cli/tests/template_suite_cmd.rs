//! `faucet template test` end-to-end (#648).
//!
//! Two gaps this closes. First, `cli/examples/tests/template_suite.yaml` ships
//! in the repo and is quoted verbatim by the templates cookbook, and nothing
//! ran it — so the example could rot against the template it points at and
//! only a user would find out. Second, the command's contract is its **exit
//! code**: it is documented as the failed-case count so CI can gate on it
//! without parsing output, and a unit test of the runner cannot observe what
//! the process actually exits with.
#![cfg(feature = "templates")]

use assert_cmd::Command;
use predicates::prelude::PredicateBooleanExt;
use predicates::str::contains;
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

/// The workspace root — the shipped suite's `template:` is a repo-relative
/// path, resolved against the process cwd.
fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("cli/ has a parent")
        .to_path_buf()
}

fn faucet() -> Command {
    let mut cmd = Command::cargo_bin("faucet").unwrap();
    cmd.current_dir(repo_root());
    // Never pick up a developer's .env.
    cmd.args(["template", "test"]).arg("--no-env-file");
    cmd
}

#[test]
fn the_shipped_example_suite_passes() {
    // Keeps cli/examples/tests/template_suite.yaml — and the cookbook that
    // quotes it — grounded against cli/examples/rest_to_jsonl_templated.yaml.
    faucet()
        .arg("cli/examples/tests/template_suite.yaml")
        .assert()
        .success()
        .stdout(contains("13 case(s): 13 passed, 0 failed"))
        // Every origin the suite exercises must actually have produced cases;
        // an `auto:` block that silently generated nothing would still print
        // "0 failed".
        .stdout(contains("[explicit] eu-small-pages"))
        .stdout(contains("[combine] "))
        .stdout(contains("[auto] auto:defaults"))
        .stdout(contains("[auto] auto:missing-tenant_id"));
}

#[test]
fn the_filter_narrows_the_run() {
    faucet()
        .arg("cli/examples/tests/template_suite.yaml")
        .args(["--filter", "auto:missing-*"])
        .assert()
        .success()
        .stdout(contains("2 case(s): 2 passed, 0 failed"))
        .stdout(contains("auto:missing-api_token"))
        .stdout(contains("auto:missing-tenant_id"));
}

#[test]
fn json_output_is_parseable_and_agrees_with_the_checklist() {
    let out = faucet()
        .arg("cli/examples/tests/template_suite.yaml")
        .arg("--json")
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let v: serde_json::Value = serde_json::from_slice(&out).expect("--json emits parseable JSON");
    assert_eq!(v["total"], 13);
    assert_eq!(v["passed"], 13);
    assert_eq!(v["failed"], 0);
    assert_eq!(v["template"], "cli/examples/rest_to_jsonl_templated.yaml");
    let cases = v["cases"].as_array().expect("cases array");
    assert_eq!(cases.len(), 13);
    assert!(
        cases.iter().all(|c| c["status"] == "pass"),
        "every case must report its own status: {v}"
    );
    // The params a generated case ran under have to be in the report, or a red
    // case cannot be reproduced from CI output alone.
    let defaults = cases
        .iter()
        .find(|c| c["name"] == "auto:defaults")
        .expect("the defaults baseline case");
    assert_eq!(defaults["origin"], "auto");
    assert!(defaults["params"].is_object(), "{defaults}");
}

/// The documented contract: the exit code **is** the failed-case count, so a
/// CI job gates on it without parsing stdout. One failure must not just be
/// "non-zero" — two failures must exit 2.
#[test]
fn the_exit_code_is_the_failed_case_count() {
    let dir = TempDir::new().unwrap();
    let template = dir.path().join("tpl.yaml");
    fs::write(
        &template,
        r#"
version: 1
params:
  region: { type: string, required: true, values: [us, eu] }
pipeline:
  source: { type: csv, config: { path: "./in-${param.region}.csv" } }
  sink:   { type: jsonl, config: { path: ./out.jsonl } }
"#,
    )
    .unwrap();

    let suite = dir.path().join("suite.yaml");
    fs::write(
        &suite,
        format!(
            r#"
version: 1
template: {}
suite:
  cases:
    - name: good
      params: {{ region: us }}
    # Two cases that must fail: each asserts an error the run will not produce,
    # because the combination is in fact valid.
    - name: bad-one
      params: {{ region: us }}
      expect: {{ error: "no such failure" }}
    - name: bad-two
      params: {{ region: eu }}
      expect: {{ error: "no such failure either" }}
"#,
            template.display()
        ),
    )
    .unwrap();

    faucet()
        .arg(&suite)
        .assert()
        .failure()
        .code(2)
        .stdout(contains("3 case(s): 1 passed, 2 failed"))
        .stdout(contains("FAIL [explicit] bad-one"))
        .stdout(contains("FAIL [explicit] bad-two"));
}

#[test]
fn a_suite_naming_an_unregistered_template_without_a_store_is_refused() {
    // The `template:` is neither a readable path nor resolvable without a
    // registry: the refusal must say so rather than reporting zero cases.
    let dir = TempDir::new().unwrap();
    let suite = dir.path().join("suite.yaml");
    fs::write(
        &suite,
        r#"
version: 1
template: not-a-file-and-not-registered
suite:
  cases:
    - name: any
      params: {}
"#,
    )
    .unwrap();

    faucet()
        .arg(&suite)
        .assert()
        .failure()
        .stderr(contains("not-a-file-and-not-registered").or(contains("store")));
}

#[test]
fn schema_template_test_prints_the_suite_schema() {
    Command::cargo_bin("faucet")
        .unwrap()
        .current_dir(repo_root())
        .args(["schema", "template-test"])
        .assert()
        .success()
        .stdout(contains("\"suite\""));
}
