#![allow(deprecated)]
//! End-to-end tests for encrypted DLQ files (#207): envelopes written by the
//! jsonl sink's `encryption:` block are readable by `faucet dlq
//! inspect/replay/discard` — with an explicit key, or (for replay) picked up
//! automatically from the config's own `dlq:` block.
#![cfg(all(feature = "encryption", feature = "sink-jsonl"))]

use faucet_cli::auth_catalog::build_auth_catalog;
use faucet_cli::config::parse_with_extension;
use faucet_cli::dlq_replay::{self, ReplayInputs, reader::DlqDecryptor};
use faucet_core::Sink;
use serde_json::{Value, json};
use std::path::Path;

const KEY: &str = "dlq-at-rest-key";

fn envelope(reason: &str, payload: Value) -> Value {
    json!({
        "error": { "kind": "QualityFailure", "message": "boom" },
        "reason": reason,
        "payload": payload,
        "ts_ms": 1_751_760_000_000i64,
        "sink": "jsonl",
        "pipeline": "orig",
        "row": "",
        "record_index": 0,
    })
}

/// Write envelope lines the way a real encrypted DLQ does: through the jsonl
/// sink with an `encryption:` block.
async fn write_encrypted_dlq(path: &Path, envelopes: &[Value]) {
    let cfg =
        faucet_sink_jsonl::JsonlSinkConfig::new(path).encryption(faucet_core::EncryptionSpec {
            key: KEY.into(),
            previous_keys: vec![],
            algorithm: Default::default(),
        });
    let sink = faucet_sink_jsonl::JsonlSink::new(cfg);
    sink.write_batch(envelopes).await.unwrap();
    sink.flush().await.unwrap();
}

fn read_lines(path: &Path) -> Vec<Value> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

#[tokio::test]
async fn inspect_reads_sealed_envelopes_with_key_and_reports_without() {
    let dir = tempfile::tempdir().unwrap();
    let dlq = dir.path().join("dlq.jsonl");
    write_encrypted_dlq(
        &dlq,
        &[
            envelope("quality", json!({"id": 1})),
            envelope("contract", json!({"id": 2})),
        ],
    )
    .await;

    // No key: nothing readable, but honestly reported as encrypted.
    let blind =
        dlq_replay::inspect(dlq.to_str().unwrap(), None, 5, &DlqDecryptor::default()).unwrap();
    assert_eq!(blind.total_envelopes, 0);
    assert_eq!(blind.undecryptable, 2);
    assert_eq!(blind.malformed, 0, "sealed lines are not 'malformed'");

    // With the key: fully readable.
    let dec = DlqDecryptor::from_keys(&[KEY.to_string()]).unwrap();
    let seen = dlq_replay::inspect(dlq.to_str().unwrap(), None, 5, &dec).unwrap();
    assert_eq!(seen.total_envelopes, 2);
    assert_eq!(seen.undecryptable, 0);
    assert_eq!(seen.by_reason.get("quality"), Some(&1));

    // Wrong key: still counted as undecryptable, never a silent skip.
    let wrong = DlqDecryptor::from_keys(&["nope".to_string()]).unwrap();
    let blind = dlq_replay::inspect(dlq.to_str().unwrap(), None, 5, &wrong).unwrap();
    assert_eq!(blind.undecryptable, 2);
}

#[tokio::test]
async fn replay_picks_up_the_configs_dlq_encryption_block() {
    let dir = tempfile::tempdir().unwrap();
    let dlq = dir.path().join("dlq.jsonl");
    let out = dir.path().join("out.jsonl");
    write_encrypted_dlq(&dlq, &[envelope("quality", json!({"id": 7, "ok": true}))]).await;

    // The config declares the same encrypted jsonl DLQ it originally wrote —
    // replay derives the key from it, no --encryption-key needed.
    let cfg_yaml = format!(
        concat!(
            "version: 1\nname: replay\npipeline:\n",
            "  source: {{ type: csv, config: {{ path: /dev/null }} }}\n",
            "  sink: {{ type: jsonl, config: {{ path: {out} }} }}\n",
            "  dlq:\n    sink:\n      type: jsonl\n",
            "      config: {{ path: {dlq}, encryption: {{ key: \"{key}\" }} }}\n",
        ),
        out = out.display(),
        dlq = dlq.display(),
        key = KEY,
    );
    let cfg = parse_with_extension(&cfg_yaml, "yaml").unwrap();

    let outcome = dlq_replay::replay(
        &cfg,
        dlq.to_str().unwrap(),
        ReplayInputs {
            decryptor: Default::default(), // no explicit key — config-derived
            reason: None,
            failed_dlq: None,
            row: None,
            dry_run: false,
            pipeline_name: "replay".into(),
            execution: None,
            auth: build_auth_catalog(None).unwrap(),
            clock: chrono::Utc::now().fixed_offset(),
        },
    )
    .await
    .unwrap();
    assert_eq!(outcome.candidates, 1);
    assert_eq!(outcome.records_written, 1);
    let rows = read_lines(&out);
    assert_eq!(rows, vec![json!({"id": 7, "ok": true})]);
}

/// A `type: file` DLQ writing JSON Lines seals each line the way the jsonl
/// sink did, and replay picks its key up from the config the same way.
#[cfg(feature = "sink-file")]
#[tokio::test]
async fn replay_picks_up_a_file_dlqs_encryption_block() {
    let dir = tempfile::tempdir().unwrap();
    let dlq = dir.path().join("dlq.jsonl");
    let out = dir.path().join("out.jsonl");
    let sink = faucet_sink_file::FileSink::new(
        faucet_sink_file::FileSinkConfig::new(dlq.to_str().unwrap()).encryption(
            faucet_core::EncryptionSpec {
                key: KEY.into(),
                previous_keys: vec![],
                algorithm: Default::default(),
            },
        ),
    )
    .unwrap();
    sink.write_batch(&[envelope("quality", json!({"id": 9}))])
        .await
        .unwrap();
    sink.flush().await.unwrap();

    let cfg_yaml = format!(
        concat!(
            "version: 1\nname: replay\npipeline:\n",
            "  source: {{ type: csv, config: {{ path: /dev/null }} }}\n",
            "  sink: {{ type: file, config: {{ path: {out} }} }}\n",
            "  dlq:\n    sink:\n      type: file\n",
            "      config: {{ path: {dlq}, encryption: {{ key: \"{key}\" }} }}\n",
        ),
        out = out.display(),
        dlq = dlq.display(),
        key = KEY,
    );
    let cfg = parse_with_extension(&cfg_yaml, "yaml").unwrap();
    let outcome = dlq_replay::replay(
        &cfg,
        dlq.to_str().unwrap(),
        ReplayInputs {
            decryptor: Default::default(),
            reason: None,
            failed_dlq: None,
            row: None,
            dry_run: false,
            pipeline_name: "replay".into(),
            execution: None,
            auth: build_auth_catalog(None).unwrap(),
            clock: chrono::Utc::now().fixed_offset(),
        },
    )
    .await
    .unwrap();
    assert_eq!(outcome.candidates, 1);
    assert_eq!(outcome.records_written, 1);
    assert_eq!(read_lines(&out), vec![json!({"id": 9})]);
}

#[tokio::test]
async fn discard_filters_sealed_envelopes_and_preserves_lines_verbatim() {
    let dir = tempfile::tempdir().unwrap();
    let dlq = dir.path().join("dlq.jsonl");
    write_encrypted_dlq(
        &dlq,
        &[
            envelope("quality", json!({"id": 1})),
            envelope("contract", json!({"id": 2})),
        ],
    )
    .await;
    let before = std::fs::read_to_string(&dlq).unwrap();
    let kept_line = before.lines().nth(1).unwrap().to_string();

    let dec = DlqDecryptor::from_keys(&[KEY.to_string()]).unwrap();
    let outcome =
        dlq_replay::discard(dlq.to_str().unwrap(), Some("quality"), None, false, &dec).unwrap();
    assert_eq!(outcome.discarded, 1);

    // The live file is never rewritten (#789 CLI-48): it is byte-identical
    // (still sealed), the reader sees only the kept line, and the archive
    // holds the removed sealed line — nothing was re-encrypted or exposed.
    let after = std::fs::read_to_string(&dlq).unwrap();
    assert_eq!(after, before);
    let scan = dlq_replay::reader::scan_files(&[dlq.clone()], &dec).unwrap();
    assert_eq!(scan.envelopes.len(), 1);
    assert!(before.lines().nth(1) == Some(kept_line.as_str()));
    let archive = std::fs::read_to_string(dir.path().join("dlq.jsonl.archived")).unwrap();
    assert!(archive.contains(before.lines().next().unwrap()));
    assert!(!archive.contains("\"id\":1"), "archive stays sealed");

    // Without a key, a filtered discard removes nothing (undecryptable lines
    // are preserved).
    let blind = dlq_replay::discard(
        dlq.to_str().unwrap(),
        Some("contract"),
        None,
        false,
        &DlqDecryptor::default(),
    )
    .unwrap();
    assert_eq!(blind.discarded, 0);
}

/// Rows that fail again during a replay land in the fresh failure DLQ sealed
/// with the same keys as the DLQ they came from, never in clear (CLI-47).
#[cfg(feature = "quality")]
#[tokio::test]
async fn replay_seals_the_refailed_rows_like_the_source_dlq() {
    let dir = tempfile::tempdir().unwrap();
    let dlq = dir.path().join("dlq.jsonl");
    let out = dir.path().join("out.jsonl");
    write_encrypted_dlq(
        &dlq,
        &[
            envelope("quality", json!({"id": 1, "ssn": "123-45-6789"})),
            envelope("quality", json!({"ssn": "987-65-4321"})),
        ],
    )
    .await;

    let cfg_yaml = format!(
        concat!(
            "version: 1\nname: replay\npipeline:\n",
            "  source: {{ type: csv, config: {{ path: /dev/null }} }}\n",
            "  sink: {{ type: jsonl, config: {{ path: {out} }} }}\n",
            "  quality:\n    record:\n",
            "      - {{ type: not_null, field: id, on_failure: quarantine }}\n",
            "  dlq:\n    sink:\n      type: jsonl\n",
            "      config: {{ path: {dlq}, encryption: {{ key: \"{key}\" }} }}\n",
        ),
        out = out.display(),
        dlq = dlq.display(),
        key = KEY,
    );
    let cfg = parse_with_extension(&cfg_yaml, "yaml").unwrap();
    for decryptor in [
        DlqDecryptor::default(),
        DlqDecryptor::from_keys(&[KEY.to_string()]).unwrap(),
    ] {
        let failed = dir.path().join("failed.jsonl");
        let _ = std::fs::remove_file(&failed);
        dlq_replay::replay(
            &cfg,
            dlq.to_str().unwrap(),
            ReplayInputs {
                decryptor,
                reason: None,
                failed_dlq: Some(failed.to_str().unwrap()),
                row: None,
                dry_run: false,
                pipeline_name: "replay".into(),
                execution: None,
                auth: build_auth_catalog(None).unwrap(),
                clock: chrono::Utc::now().fixed_offset(),
            },
        )
        .await
        .unwrap();
        let raw = std::fs::read_to_string(&failed).unwrap();
        assert!(!raw.trim().is_empty(), "the id-less row fails again");
        assert!(
            !raw.contains("987-65-4321"),
            "re-failed row written in clear"
        );
        let dec = DlqDecryptor::from_keys(&[KEY.to_string()]).unwrap();
        let seen = dlq_replay::inspect(failed.to_str().unwrap(), None, 5, &dec).unwrap();
        assert_eq!(seen.total_envelopes, 1);
        assert_eq!(seen.undecryptable, 0);
    }
}

/// The key can come from `FAUCET_DLQ_ENCRYPTION_KEY` or a key file instead of
/// argv (CLI-152).
#[tokio::test]
async fn inspect_takes_the_key_from_the_environment_or_a_file() {
    let dir = tempfile::tempdir().unwrap();
    let dlq = dir.path().join("dlq.jsonl");
    write_encrypted_dlq(&dlq, &[envelope("quality", json!({"id": 1}))]).await;
    let key_file = dir.path().join("dlq.key");
    std::fs::write(&key_file, format!("{KEY}\n")).unwrap();
    let count = |out: std::process::Output| -> u64 {
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        let v: Value = serde_json::from_slice(&out.stdout).unwrap();
        v["total_envelopes"].as_u64().unwrap()
    };
    let faucet = || {
        let mut c = std::process::Command::new(env!("CARGO_BIN_EXE_faucet"));
        c.env_remove("FAUCET_DLQ_ENCRYPTION_KEY").args([
            "dlq",
            "inspect",
            dlq.to_str().unwrap(),
            "--json",
        ]);
        c
    };
    assert_eq!(count(faucet().output().unwrap()), 0);
    assert_eq!(
        count(
            faucet()
                .env("FAUCET_DLQ_ENCRYPTION_KEY", KEY)
                .output()
                .unwrap()
        ),
        1
    );
    assert_eq!(
        count(
            faucet()
                .arg("--encryption-key-file")
                .arg(&key_file)
                .output()
                .unwrap()
        ),
        1
    );
    let out = faucet()
        .arg("--encryption-key-file")
        .arg(dir.path().join("missing.key"))
        .output()
        .unwrap();
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("--encryption-key-file"));
}
