//! #789 CLI-100: a `$${…}` escape stays literal through every interpolation
//! pass — load-time, params, `${now.*}`, `${vars.*}` — and reaches the
//! connector / transform as a plain `${…}`.
use assert_cmd::Command;

#[test]
fn escaped_tokens_reach_the_record_literally() {
    let dir = tempfile::tempdir().unwrap();
    let csv = dir.path().join("in.csv");
    std::fs::write(&csv, "name\nalice\n").unwrap();
    let out = dir.path().join("out.jsonl");
    let cfg = dir.path().join("pipeline.yaml");
    std::fs::write(
        &cfg,
        format!(
            r#"version: 1
name: "esc"
params:
  p: {{ default: "bound" }}
pipeline:
  source: {{ type: csv, config: {{ path: "{csv}" }} }}
  sink: {{ type: jsonl, config: {{ path: "{out}" }} }}
  transforms:
    - type: set
      config:
        values:
          lit: "$${{now.date}}|$${{param.p}}|$${{env:HOME}}|${{param.p}}"
"#,
            csv = csv.display(),
            out = out.display(),
        ),
    )
    .unwrap();
    Command::cargo_bin("faucet")
        .unwrap()
        .arg("run")
        .arg(&cfg)
        .arg("--no-env-file")
        .assert()
        .success();
    let text = std::fs::read_to_string(&out).unwrap();
    let row: serde_json::Value = serde_json::from_str(text.lines().next().unwrap()).unwrap();
    assert_eq!(row["lit"], "${now.date}|${param.p}|${env:HOME}|bound");
}
