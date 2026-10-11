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

/// #862 M-023: an escape stays literal in a topology node's transform, in a
/// `vars:` value, and in a typed block the secrets pass walks.
#[test]
fn escapes_stay_literal_in_topology_nodes_and_vars() {
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
vars:
  v: "$${{now.date}}"
pipeline:
  sources:
    s: {{ type: csv, config: {{ path: "{csv}" }} }}
  sinks:
    o: {{ type: jsonl, config: {{ path: "{out}" }} }}
  nodes:
    src: {{ kind: source, ref: s }}
    t:
      kind: transform
      transforms:
        - type: set
          config: {{ values: {{ a: "$${{now.date}}", b: "${{vars.v}}", c: "$${{aws-sm:prod/db#password}}" }} }}
    out: {{ kind: sink, ref: o }}
  edges:
    - {{ from: src, to: t }}
    - {{ from: t, to: out }}
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
    assert_eq!(row["a"], "${now.date}");
    assert_eq!(row["b"], "${now.date}");
    assert_eq!(row["c"], "${aws-sm:prod/db#password}");
}

#[cfg(feature = "masking")]
#[tokio::test]
async fn an_escaped_secret_in_a_typed_block_is_not_resolved() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = dir.path().join("p.yaml");
    std::fs::write(
        &cfg,
        r#"version: 1
pipeline:
  source: { type: csv, config: { path: in.csv } }
  sink: { type: jsonl, config: { path: out.jsonl } }
  masking:
    rules:
      - match: { fields: [email] }
        action: { type: redact, mask: "$${vault:secret/app#key}" }
"#,
    )
    .unwrap();
    let loaded = faucet_cli::config::PipelineConfig::from_path_async(&cfg, None)
        .await
        .expect("an escaped directive is text, not a secret to resolve");
    let masking = serde_json::to_value(loaded.pipeline.masking.unwrap()).unwrap();
    assert_eq!(
        masking["rules"][0]["action"]["mask"],
        "${vault:secret/app#key}"
    );
}
