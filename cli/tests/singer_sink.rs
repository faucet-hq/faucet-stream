//! `faucet run` → Singer target sink (#722): the CLI fills the stream name from
//! the row id, the SCHEMA from the pipeline contract, and one ACTIVATE_VERSION
//! per run; a crashing target fails the row with its stderr.
#![cfg(all(feature = "sink-singer", feature = "source-csv", feature = "contract"))]

use std::path::{Path, PathBuf};

use serde_json::Value;

fn fake_target() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../crates/sink/singer/tests/fake_targets/fake_target.py")
        .canonicalize()
        .unwrap()
}

fn read_jsonl(path: &Path) -> Vec<Value> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

fn events(log: &Path, kind: &str) -> Vec<Value> {
    read_jsonl(log)
        .into_iter()
        .filter(|e| e["event"] == kind)
        .collect()
}

#[tokio::test]
async fn csv_to_singer_rows_use_row_ids_contract_schema_and_one_version() {
    let dir = tempfile::tempdir().unwrap();
    let csv = dir.path().join("in.csv");
    std::fs::write(&csv, "id,name\n1,ada\n2,grace\n").unwrap();
    let out = dir.path().join("out.jsonl");
    let log = dir.path().join("log.jsonl");
    let yaml = format!(
        r#"
version: 1
name: people
pipeline:
  source: {{ type: csv, config: {{ path: "{csv}" }} }}
  sink:
    type: singer
    config:
      target_command: "{target}"
      target_config: {{ path: "{out}", log: "{log}" }}
      write_mode: overwrite
  contract:
    version: "1"
    fields:
      - {{ name: id, type: string, required: true }}
      - {{ name: name, type: string }}
matrix:
  - id: contacts
  - id: members
"#,
        csv = csv.display(),
        target = fake_target().display(),
        out = out.display(),
        log = log.display(),
    );
    let summary = faucet_cli::run_from_yaml_str(&yaml).await.unwrap();
    assert!(!summary.had_failures(), "{:?}", summary.invocations);

    let records = read_jsonl(&out);
    let mut streams: Vec<&str> = records
        .iter()
        .map(|r| r["_stream"].as_str().unwrap())
        .collect();
    streams.sort();
    assert_eq!(streams, ["contacts", "contacts", "members", "members"]);
    let versions: std::collections::HashSet<i64> = records
        .iter()
        .map(|r| r["_version"].as_i64().unwrap())
        .collect();
    assert_eq!(
        versions.len(),
        1,
        "every writer of a run shares one version"
    );

    let schemas = events(&log, "schema");
    assert!(!schemas.is_empty());
    assert!(
        schemas
            .iter()
            .all(|s| s["schema"]["x-faucet-contract-version"] == "1"),
        "{schemas:?}"
    );
    let activates = events(&log, "activate");
    assert_eq!(activates.len(), 2, "one ACTIVATE_VERSION per stream");
    assert!(
        activates
            .iter()
            .all(|a| a["version"].as_i64() == versions.iter().next().copied())
    );
}

#[tokio::test]
async fn single_row_config_streams_under_the_pipeline_name_and_crash_fails_the_row() {
    let dir = tempfile::tempdir().unwrap();
    let csv = dir.path().join("in.csv");
    std::fs::write(&csv, "id\n1\n2\n3\n").unwrap();
    let out = dir.path().join("out.jsonl");
    let ok = format!(
        r#"
version: 1
name: people
pipeline:
  source: {{ type: csv, config: {{ path: "{csv}" }} }}
  sink:
    type: singer
    config:
      target_command: "{target}"
      target_config: {{ path: "{out}" }}
"#,
        csv = csv.display(),
        target = fake_target().display(),
        out = out.display(),
    );
    let summary = faucet_cli::run_from_yaml_str(&ok).await.unwrap();
    assert!(!summary.had_failures(), "{:?}", summary.invocations);
    assert!(read_jsonl(&out).iter().all(|r| r["_stream"] == "people"));

    let crash = ok.replace(
        "target_config: {",
        "target_config: { mode: crash, crash_after: 1, secret: cli-secret-value-42, ",
    );
    let summary = faucet_cli::run_from_yaml_str(&crash).await.unwrap();
    assert!(summary.had_failures());
    let err = format!("{:?}", summary.invocations[0].error);
    assert!(err.contains("fatal: cannot load record"), "{err}");
    assert!(!err.contains("cli-secret-value-42"), "{err}");
}
