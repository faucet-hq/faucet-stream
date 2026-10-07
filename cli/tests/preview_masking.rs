//! `faucet preview` applies the config's `masking:` policy to the records it
//! prints, in matrix and topology mode (CLI-154).
#![cfg(all(
    feature = "source-csv",
    feature = "sink-jsonl",
    feature = "sink-stdout",
    feature = "masking"
))]

use assert_cmd::Command;
use std::fs;
use tempfile::TempDir;

const MASKING: &str = r#"  masking:
    rules:
      - name: emails
        match: { value_detector: email }
        action: { type: redact }
      - name: jsonl-only
        match: { fields: [name] }
        action: { type: redact }
        applies_to: [jsonl]
"#;

fn preview(cfg: &str) -> String {
    let dir = TempDir::new().unwrap();
    let csv = dir.path().join("people.csv");
    fs::write(&csv, "id,name,email\n1,Ada,ada@example.com\n").unwrap();
    let out = dir.path().join("o.jsonl");
    let path = dir.path().join("faucet.yaml");
    let body = cfg
        .replace("{csv}", &csv.display().to_string())
        .replace("{out}", &out.display().to_string());
    fs::write(&path, format!("{body}{MASKING}")).unwrap();
    let assert = Command::cargo_bin("faucet")
        .unwrap()
        .args(["preview", "--limit", "5"])
        .arg(&path)
        .assert()
        .success();
    String::from_utf8(assert.get_output().stdout.clone()).unwrap()
}

#[test]
fn preview_masks_records_of_a_matrix_config() {
    let stdout = preview(
        "version: 1\nname: p\npipeline:\n  source: { type: csv, config: { path: {csv} } }\n  sink: { type: jsonl, config: { path: {out} } }\n",
    );
    assert!(stdout.contains("\"id\""), "{stdout}");
    assert!(!stdout.contains("ada@example.com"), "{stdout}");
    assert!(!stdout.contains("Ada"), "every rule applies, applies_to aside: {stdout}");
}

#[test]
fn preview_masks_records_of_a_topology_config() {
    let stdout = preview(
        "version: 1\nname: p\npipeline:\n  sources:\n    o: { type: csv, config: { path: {csv} } }\n  sinks:\n    out: { type: jsonl, config: { path: {out} } }\n  nodes:\n    s: { kind: source, ref: o }\n    w: { kind: sink, ref: out }\n  edges:\n    - { from: s, to: w }\n",
    );
    assert!(stdout.contains("\"id\""), "{stdout}");
    assert!(!stdout.contains("ada@example.com"), "{stdout}");
}
