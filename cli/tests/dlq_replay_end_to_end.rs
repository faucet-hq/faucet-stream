//! End-to-end tests for `faucet dlq replay` (#281): drive the real
//! `dlq_replay::replay` orchestration through the executor + a real JSONL
//! sink, and assert the now-valid rows land while rows that fail *again* are
//! routed to a fresh DLQ (never back to the source).

use faucet_cli::auth_catalog::build_auth_catalog;
use faucet_cli::config::parse_with_extension;
use faucet_cli::dlq_replay::{self, ReplayInputs};
use serde_json::{Value, json};
use std::io::Write;
use std::path::Path;

/// Build a DLQ envelope line the way `faucet_core::dlq::build_envelope` does.
fn envelope(reason: &str, kind: &str, payload: Value) -> String {
    json!({
        "error": { "kind": kind, "message": "boom" },
        "reason": reason,
        "payload": payload,
        "ts_ms": 1_751_760_000_000i64,
        "sink": "jsonl",
        "pipeline": "orig",
        "row": "",
        "record_index": 0,
    })
    .to_string()
}

fn write_file(path: &Path, body: &str) {
    let mut f = std::fs::File::create(path).unwrap();
    f.write_all(body.as_bytes()).unwrap();
    f.flush().unwrap();
}

fn read_lines(path: &Path) -> Vec<Value> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

async fn inputs(pipeline: &str, dry_run: bool) -> ReplayInputs<'static> {
    ReplayInputs {
        decryptor: Default::default(),
        reason: None,
        failed_dlq: None,
        row: None,
        dry_run,
        pipeline_name: pipeline.to_string(),
        execution: None,
        auth: build_auth_catalog(None).unwrap(),
        clock: chrono::Utc::now().fixed_offset(),
        budget: None,
        cancel: None,
    }
}

#[tokio::test]
async fn replay_lands_payloads_in_the_sink() {
    let dir = tempfile::tempdir().unwrap();
    let dlq = dir.path().join("dlq.jsonl");
    let out = dir.path().join("out.jsonl");
    write_file(
        &dlq,
        &format!(
            "{}\n{}\n",
            envelope("quality", "QualityFailure", json!({"id": 1, "name": "a"})),
            envelope("quality", "QualityFailure", json!({"id": 2, "name": "b"})),
        ),
    );

    // Source kind is irrelevant — the replay overrides it with a DLQ reader —
    // but must be a valid connector kind for the config to parse/expand.
    let cfg_yaml = format!(
        "version: 1\nname: replay\npipeline:\n  source: {{ type: csv, config: {{ path: /dev/null }} }}\n  sink: {{ type: jsonl, config: {{ path: {} }} }}\n",
        out.display()
    );
    let cfg = parse_with_extension(&cfg_yaml, "yaml").unwrap();

    let outcome = dlq_replay::replay(&cfg, dlq.to_str().unwrap(), inputs("replay", false).await)
        .await
        .expect("replay succeeds");

    assert_eq!(outcome.candidates, 2);
    assert_eq!(outcome.records_written, 2);
    assert!(!outcome.dry_run);

    let landed = read_lines(&out);
    assert_eq!(landed.len(), 2);
    assert_eq!(landed[0], json!({"id": 1, "name": "a"}));
    assert_eq!(landed[1], json!({"id": 2, "name": "b"}));
}

#[tokio::test]
async fn replay_dry_run_writes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let dlq = dir.path().join("dlq.jsonl");
    let out = dir.path().join("out.jsonl");
    write_file(
        &dlq,
        &format!(
            "{}\n",
            envelope("quality", "QualityFailure", json!({"id": 1}))
        ),
    );
    let cfg_yaml = format!(
        "version: 1\nname: replay\npipeline:\n  source: {{ type: csv, config: {{ path: /dev/null }} }}\n  sink: {{ type: jsonl, config: {{ path: {} }} }}\n",
        out.display()
    );
    let cfg = parse_with_extension(&cfg_yaml, "yaml").unwrap();

    let outcome = dlq_replay::replay(&cfg, dlq.to_str().unwrap(), inputs("replay", true).await)
        .await
        .unwrap();
    assert_eq!(outcome.candidates, 1);
    assert!(outcome.dry_run);
    // The real sink was never touched under --dry-run.
    assert!(!out.exists() || read_lines(&out).is_empty());
}

#[cfg(feature = "contract")]
#[tokio::test]
async fn replay_refailures_go_to_fresh_dlq_not_the_source() {
    let dir = tempfile::tempdir().unwrap();
    let dlq = dir.path().join("dlq.jsonl");
    let out = dir.path().join("out.jsonl");
    // One payload that now satisfies the contract, one that still breaches it.
    write_file(
        &dlq,
        &format!(
            "{}\n{}\n",
            envelope("contract", "ContractViolation", json!({"status": "ok"})),
            envelope(
                "contract",
                "ContractViolation",
                json!({"status": "still-bad"})
            ),
        ),
    );

    // The replay config re-applies a contract that quarantines a bad `status`.
    // Its original DLQ is the source location; the replay swaps in a fresh one.
    let cfg_yaml = format!(
        r#"
version: 1
name: replay
pipeline:
  source: {{ type: csv, config: {{ path: /dev/null }} }}
  contract:
    version: "1.0.0"
    on_breach: quarantine
    fields:
      - name: status
        type: string
        enum: [ok]
  dlq:
    sink: {{ type: jsonl, config: {{ path: {dlq} }} }}
  sink: {{ type: jsonl, config: {{ path: {out} }} }}
"#,
        dlq = dlq.display(),
        out = out.display()
    );
    let cfg = parse_with_extension(&cfg_yaml, "yaml").unwrap();

    let outcome = dlq_replay::replay(&cfg, dlq.to_str().unwrap(), inputs("replay", false).await)
        .await
        .expect("replay succeeds");
    assert_eq!(outcome.candidates, 2);
    // Only the now-valid row reached the sink.
    assert_eq!(outcome.records_written, 1);
    let landed = read_lines(&out);
    assert_eq!(landed, vec![json!({"status": "ok"})]);

    // The still-breaching row was quarantined to the FRESH replay-failed DLQ…
    let fresh = Path::new(&outcome.failed_dlq);
    assert!(
        fresh.exists(),
        "fresh DLQ should exist at {}",
        outcome.failed_dlq
    );
    let refailed = read_lines(fresh);
    assert_eq!(refailed.len(), 1);
    assert_eq!(refailed[0]["payload"], json!({"status": "still-bad"}));

    // …and NOT re-appended to the source DLQ (no infinite loop). The source
    // still holds exactly its original two envelope lines.
    assert_eq!(read_lines(&dlq).len(), 2);
    assert_ne!(
        std::fs::canonicalize(fresh).unwrap(),
        std::fs::canonicalize(&dlq).unwrap()
    );
}
