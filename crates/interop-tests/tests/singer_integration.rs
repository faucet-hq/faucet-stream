#![allow(deprecated)]

//! End-to-end tests for `faucet-sink-singer` against real target subprocesses
//! (a dependency-free fake Singer target written in Python).

use std::path::{Path, PathBuf};

use faucet_core::{Pipeline, Value};
use faucet_sink_singer::{SingerSink, SingerSinkConfig};
use faucet_source_csv::{CsvSource, CsvSourceConfig};
use serde_json::json;

fn fake_target() -> String {
    format!(
        "{}/../sink/singer/tests/fake_targets/fake_target.py",
        env!("CARGO_MANIFEST_DIR")
    )
}

struct Fixture {
    dir: tempfile::TempDir,
}

impl Fixture {
    fn new() -> Self {
        Self {
            dir: tempfile::tempdir().unwrap(),
        }
    }
    fn out(&self) -> PathBuf {
        self.dir.path().join("out.jsonl")
    }
    fn log(&self) -> PathBuf {
        self.dir.path().join("log.jsonl")
    }
    fn config(&self, extra: Value) -> SingerSinkConfig {
        let mut target_config = json!({
            "path": self.out(),
            "log": self.log(),
        });
        for (k, v) in extra.as_object().unwrap() {
            target_config[k] = v.clone();
        }
        let mut cfg = SingerSinkConfig::new(fake_target());
        cfg.target_config = target_config;
        cfg.stream = Some("orders".into());
        cfg
    }
    fn records(&self) -> Vec<Value> {
        read_jsonl(&self.out())
    }
    fn events(&self, kind: &str) -> Vec<Value> {
        read_jsonl(&self.log())
            .into_iter()
            .filter(|e| e["event"] == kind)
            .collect()
    }
}

fn read_jsonl(path: &Path) -> Vec<Value> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

#[tokio::test]
async fn csv_source_to_singer_target_writes_expected_records() {
    let fx = Fixture::new();
    let csv = fx.dir.path().join("in.csv");
    std::fs::write(&csv, "id,name,amount\n1,ada,10\n2,grace,20\n3,linus,30\n").unwrap();
    let source = CsvSource::new(CsvSourceConfig::new(csv.to_string_lossy()));
    let sink = SingerSink::new(fx.config(json!({"mode": "echo"}))).unwrap();
    let result = Pipeline::new(&source, &sink).run().await.unwrap();
    assert_eq!(result.records_written, 3);

    let got = fx.records();
    let names: Vec<&str> = got.iter().map(|r| r["name"].as_str().unwrap()).collect();
    assert_eq!(names, ["ada", "grace", "linus"]);
    assert!(got.iter().all(|r| r["_stream"] == "orders"));
    assert!(got.iter().all(|r| r.get("_version").is_none()));
    let schemas = fx.events("schema");
    assert_eq!(schemas.len(), 1);
    assert!(schemas[0]["schema"]["properties"]["name"].is_object());
    assert_eq!(
        fx.events("exit").len(),
        1,
        "flush_on: exit closes the target"
    );
}

/// Opt-in: the real `target-jsonl` (`pip install target-jsonl`), located by
/// `FAUCET_TARGET_JSONL`. Never run in CI — it needs a network install.
#[tokio::test]
#[ignore = "needs the real target-jsonl; set FAUCET_TARGET_JSONL to its path"]
async fn real_target_jsonl() {
    let Ok(target) = std::env::var("FAUCET_TARGET_JSONL") else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let csv = dir.path().join("in.csv");
    std::fs::write(&csv, "id,name\n1,ada\n2,grace\n").unwrap();
    let source = CsvSource::new(CsvSourceConfig::new(csv.to_string_lossy()));
    let mut cfg = SingerSinkConfig::new(target);
    cfg.stream = Some("people".into());
    cfg.target_config = json!({"destination_path": dir.path(), "do_timestamp_file": false});
    let sink = SingerSink::new(cfg).unwrap();
    let result = Pipeline::new(&source, &sink).run().await.unwrap();
    assert_eq!(result.records_written, 2);
    let out = read_jsonl(&dir.path().join("people.jsonl"));
    assert_eq!(out.len(), 2);
    assert_eq!(out[1]["name"], "grace");
}
