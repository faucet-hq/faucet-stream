//! End-to-end tests for data-flow policies (#702): the static gate in
//! `validate` / `policy` / `plan` / `doctor` / `run`, label propagation through
//! renames and opaque transforms, masking satisfying a rule, and the runtime
//! backstop (`fail` / `quarantine`) on real records.
#![cfg(all(
    feature = "policy",
    feature = "source-csv",
    feature = "sink-jsonl",
    feature = "contract"
))]

use assert_cmd::Command;
use predicates::str::contains;
use std::fs;
use std::path::Path;
use tempfile::TempDir;

const POLICY: &str = r#"
version: 1
classifications:
  - label: pii
    fields: [email]
    value_detector: email
  - label: finance
    field_pattern: "^amount"
rules:
  - name: pii-eu
    when: { label: pii }
    require: { residency: [eu] }
    mask: [hash]
  - name: finance-no-files
    when: { label: finance, sink_kind: [jsonl] }
    deny: true
    on_runtime: quarantine
"#;

fn config(dir: &Path, attributes: &str, extra_pipeline: &str) -> String {
    format!(
        r#"version: 1
name: poltest
pipeline:
  source:
    type: csv
    config: {{ path: "{in}" }}
  contract:
    version: "1"
    fields:
      - {{ name: id, type: string }}
      - {{ name: email, type: string }}
  sink:
    type: jsonl
{attributes}
    config: {{ path: "{out}" }}
{extra_pipeline}
"#,
        r#in = dir.join("in.csv").display(),
        out = dir.join("out.jsonl").display(),
    )
}

fn write(dir: &Path, name: &str, body: &str) -> std::path::PathBuf {
    let p = dir.join(name);
    fs::write(&p, body).unwrap();
    p
}

fn faucet() -> Command {
    Command::cargo_bin("faucet").unwrap()
}

#[test]
fn validate_refuses_a_violation_with_exit_code_equal_to_the_count() {
    let dir = TempDir::new().unwrap();
    let cfg = write(dir.path(), "p.yaml", &config(dir.path(), "", ""));
    let pol = write(dir.path(), "policy.yaml", POLICY);
    faucet()
        .args(["validate"])
        .arg(&cfg)
        .arg("--policy")
        .arg(&pol)
        .assert()
        .code(1)
        .stdout(contains("rule `pii-eu`"))
        .stdout(contains("policy violations"));

    // JSON: `valid: false` + the report under `policy`.
    let out = faucet()
        .args(["validate", "--json"])
        .arg(&cfg)
        .arg("--policy")
        .arg(&pol)
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(1));
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(v["valid"], false);
    assert_eq!(v["policy"]["violations"], 1);
    assert_eq!(v["policy"]["rows"][0]["violations"][0]["column"], "email");
    assert_eq!(v["policy"]["rows"][0]["column_source"], "contract");
}

#[test]
fn compliant_attributes_or_masking_satisfy_the_rule() {
    let dir = TempDir::new().unwrap();
    let pol = write(dir.path(), "policy.yaml", POLICY);
    // residency: eu satisfies `require`.
    let cfg = write(
        dir.path(),
        "eu.yaml",
        &config(dir.path(), "    attributes: { residency: eu }", ""),
    );
    faucet()
        .args(["validate"])
        .arg(&cfg)
        .arg("--policy")
        .arg(&pol)
        .assert()
        .success()
        .stdout(contains("no violations"));

    // A hash mask on `email` satisfies `mask: [hash]` without the attribute.
    let masked = write(
        dir.path(),
        "masked.yaml",
        &config(
            dir.path(),
            "",
            "  masking:\n    rules:\n      - name: m\n        match: { fields: [email] }\n        action: { type: hash }\n",
        ),
    );
    faucet()
        .args(["policy"])
        .arg(&masked)
        .arg("--policy")
        .arg(&pol)
        .assert()
        .success()
        .stdout(contains("masked:hash"))
        .stdout(contains("no violations"));
}

#[test]
fn labels_follow_a_rename_and_an_opaque_chain_is_conservative() {
    let dir = TempDir::new().unwrap();
    let pol = write(dir.path(), "policy.yaml", POLICY);
    let renamed = write(
        dir.path(),
        "renamed.yaml",
        &config(
            dir.path(),
            "",
            "  transforms:\n    - type: rename_field\n      config: { fields: { email: contact } }\n",
        ),
    );
    let out = faucet()
        .args(["policy", "--json"])
        .arg(&renamed)
        .arg("--policy")
        .arg(&pol)
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(1));
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    let col = &v["rows"][0]["columns"][0];
    assert_eq!(col["name"], "contact", "{v}");
    assert_eq!(col["via"], "lineage");
    assert_eq!(v["rows"][0]["violations"][0]["column"], "contact");

    let opaque = write(
        dir.path(),
        "opaque.yaml",
        &config(
            dir.path(),
            "",
            "  transforms:\n    - type: flatten\n      config: {}\n",
        ),
    );
    let out = faucet()
        .args(["policy", "--json"])
        .arg(&opaque)
        .arg("--policy")
        .arg(&pol)
        .output()
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(v["rows"][0]["opaque"], true, "{v}");
    assert_eq!(v["rows"][0]["violations"][0]["conservative"], true);
}

#[test]
fn run_refuses_before_any_connector_is_built_and_plan_doctor_report() {
    let dir = TempDir::new().unwrap();
    fs::write(dir.path().join("in.csv"), "id,email\n1,a@x.io\n").unwrap();
    let cfg = write(dir.path(), "p.yaml", &config(dir.path(), "", ""));
    let pol = write(dir.path(), "policy.yaml", POLICY);
    faucet()
        .args(["run"])
        .arg(&cfg)
        .arg("--policy")
        .arg(&pol)
        .assert()
        .code(1)
        .stderr(contains("rule `pii-eu`"));
    assert!(
        !dir.path().join("out.jsonl").exists(),
        "a refused run must write nothing"
    );

    // `plan` reports the verdict (and does not fail — it is a preview).
    let out = faucet()
        .args(["plan", "--json"])
        .arg(&cfg)
        .arg("--policy")
        .arg(&pol)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(v["policy"]["violations"].as_array().unwrap().len(), 1);
    faucet()
        .args(["plan"])
        .arg(&cfg)
        .arg("--policy")
        .arg(&pol)
        .assert()
        .success()
        .stdout(contains("policy: 1 labelled column(s)"))
        .stdout(contains("rule `pii-eu`"));

    // `doctor` adds a `policy` probe per root row; the violation fails it.
    faucet()
        .args(["doctor"])
        .arg(&cfg)
        .arg("--policy")
        .arg(&pol)
        .args(["--timeout-secs", "10"])
        .assert()
        .code(1)
        .stdout(contains("policy"))
        .stdout(contains("pii-eu"));
}

#[test]
fn runtime_backstop_fails_on_a_detected_value_and_quarantines_with_a_dlq() {
    let dir = TempDir::new().unwrap();
    // No contract → nothing is known statically; the row `mail` (not a
    // classified name) carries an email address the detector catches.
    fs::write(
        dir.path().join("in.csv"),
        "id,mail\n1,alice@example.com\n2,not-an-email\n",
    )
    .unwrap();
    let base = |name: &str, extra: &str| {
        format!(
            r#"version: 1
name: rt
policy:
  classifications:
    - {{ label: pii, value_detector: email }}
  rules:
    - name: pii-eu
      when: {{ label: pii }}
      require: {{ residency: [eu] }}
      on_runtime: {name}
pipeline:
  source: {{ type: csv, config: {{ path: "{in}" }} }}
  sink: {{ type: jsonl, config: {{ path: "{out}" }} }}
{extra}
"#,
            r#in = dir.path().join("in.csv").display(),
            out = dir.path().join(format!("{name}.jsonl")).display(),
        )
    };
    // fail: the page is refused, nothing is written.
    let cfg = write(dir.path(), "fail.yaml", &base("fail", ""));
    faucet()
        .args(["run"])
        .arg(&cfg)
        .assert()
        .failure()
        .stderr(contains("Policy `pii-eu` violated"));
    assert!(!dir.path().join("fail.jsonl").exists());

    // quarantine without a DLQ is refused at load time.
    let no_dlq = write(dir.path(), "q.yaml", &base("quarantine", ""));
    faucet()
        .args(["run"])
        .arg(&no_dlq)
        .assert()
        .failure()
        .stderr(contains("needs a `dlq:` block"));

    // quarantine with a DLQ: the offending row lands in the DLQ, the rest is
    // written, and the run succeeds.
    let dlq = dir.path().join("dlq.jsonl");
    let cfg = write(
        dir.path(),
        "q2.yaml",
        &base(
            "quarantine",
            &format!(
                "  dlq:\n    sink: {{ type: jsonl, config: {{ path: \"{}\" }} }}\n",
                dlq.display()
            ),
        ),
    );
    faucet().args(["run"]).arg(&cfg).assert().success();
    let out = fs::read_to_string(dir.path().join("quarantine.jsonl")).unwrap();
    assert_eq!(out.lines().count(), 1, "{out}");
    assert!(out.contains("not-an-email"));
    let dlq_text = fs::read_to_string(&dlq).unwrap();
    assert_eq!(dlq_text.lines().count(), 1, "{dlq_text}");
    assert!(dlq_text.contains("alice@example.com") && dlq_text.contains("pii-eu"));
}

#[test]
fn schema_policy_and_a_malformed_policy_file_fail_closed() {
    faucet()
        .args(["schema", "policy"])
        .assert()
        .success()
        .stdout(contains("classifications"))
        .stdout(contains("on_runtime"));
    let dir = TempDir::new().unwrap();
    let cfg = write(dir.path(), "p.yaml", &config(dir.path(), "", ""));
    let bad = write(
        dir.path(),
        "bad.yaml",
        "classifications: [{ label: x, field_pattern: '(' }]\nrules: []\n",
    );
    faucet()
        .args(["validate"])
        .arg(&cfg)
        .arg("--policy")
        .arg(&bad)
        .assert()
        .failure()
        .stderr(contains("policy"));
    // No policy anywhere: `faucet policy` says so instead of printing nothing.
    faucet()
        .args(["policy"])
        .arg(&cfg)
        .assert()
        .failure()
        .stderr(contains("no policy"));
}
