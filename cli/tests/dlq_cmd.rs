//! Command-layer tests for `faucet dlq` (#281): drive `commands::dlq::run`
//! for each subcommand so the human/JSON render branches and dispatch execute.

use faucet_cli::cli::{DlqArgs, DlqCommand, DlqDiscardArgs, DlqInspectArgs, DlqReplayArgs};
use faucet_cli::commands::dlq;
use serde_json::json;
use std::path::{Path, PathBuf};

fn envelope(reason: &str, payload: serde_json::Value) -> String {
    json!({
        "error": { "kind": "QualityFailure", "message": "boom" },
        "reason": reason,
        "payload": payload,
        "ts_ms": 1_751_760_000_000i64,
        "sink": "jsonl", "pipeline": "replay", "row": "", "record_index": 0,
    })
    .to_string()
}

fn write(dir: &Path, name: &str, body: &str) -> PathBuf {
    let p = dir.join(name);
    std::fs::write(&p, body).unwrap();
    p
}

fn dlq_file(dir: &Path) -> PathBuf {
    write(
        dir,
        "dlq.jsonl",
        &format!(
            "{}\n{}\n",
            envelope("quality", json!({"id": 1})),
            envelope("contract", json!({"id": 2})),
        ),
    )
}

fn inspect_args(location: &Path, reason: Option<&str>, json: bool) -> DlqArgs {
    DlqArgs {
        command: DlqCommand::Inspect(DlqInspectArgs {
            encryption_key: vec![],
            encryption_key_file: vec![],
            location: location.to_string_lossy().into_owned(),
            reason: reason.map(str::to_owned),
            limit: 5,
            json,
        }),
    }
}

#[tokio::test]
async fn inspect_human_and_json_render() {
    let dir = tempfile::tempdir().unwrap();
    let dlq = dlq_file(dir.path());
    // Human render (all reasons) + JSON render + reason filter all succeed.
    dlq::run(inspect_args(&dlq, None, false)).await.unwrap();
    dlq::run(inspect_args(&dlq, None, true)).await.unwrap();
    dlq::run(inspect_args(&dlq, Some("quality"), false))
        .await
        .unwrap();
    // A bad reason is rejected.
    assert!(
        dlq::run(inspect_args(&dlq, Some("nope"), false))
            .await
            .is_err()
    );
    // A missing location errors.
    assert!(
        dlq::run(inspect_args(&dir.path().join("missing.jsonl"), None, false))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn discard_human_and_json_render() {
    let dir = tempfile::tempdir().unwrap();
    let dlq = dlq_file(dir.path());
    // Archive one reason (human render), then delete the rest (JSON render).
    let archive = DlqArgs {
        command: DlqCommand::Discard(DlqDiscardArgs {
            encryption_key: vec![],
            encryption_key_file: vec![],
            location: dlq.to_string_lossy().into_owned(),
            reason: Some("quality".into()),
            before: Some("1s".into()),
            delete: false,
            purge: false,
            json: false,
        }),
    };
    dlq::run(archive).await.unwrap();
    let delete = DlqArgs {
        command: DlqCommand::Discard(DlqDiscardArgs {
            encryption_key: vec![],
            encryption_key_file: vec![],
            location: dlq.to_string_lossy().into_owned(),
            reason: None,
            before: None,
            delete: true,
            purge: false,
            json: true,
        }),
    };
    dlq::run(delete).await.unwrap();
    // A bad --before is rejected.
    let bad = DlqArgs {
        command: DlqCommand::Discard(DlqDiscardArgs {
            encryption_key: vec![],
            encryption_key_file: vec![],
            location: dlq.to_string_lossy().into_owned(),
            reason: None,
            before: Some("soon".into()),
            delete: false,
            purge: false,
            json: false,
        }),
    };
    assert!(dlq::run(bad).await.is_err());
}

/// #862 M-024: `--purge` removes discarded envelopes from the file itself.
#[tokio::test]
async fn discard_purge_removes_the_envelopes_from_disk() {
    let dir = tempfile::tempdir().unwrap();
    let dlq = dlq_file(dir.path());
    let lines_before = std::fs::read_to_string(&dlq).unwrap().lines().count();
    for json in [false, true] {
        let purge = DlqArgs {
            command: DlqCommand::Discard(DlqDiscardArgs {
                encryption_key: vec![],
                encryption_key_file: vec![],
                location: dlq.to_string_lossy().into_owned(),
                reason: Some("quality".into()),
                before: None,
                delete: false,
                purge: true,
                json,
            }),
        };
        dlq::run(purge).await.unwrap();
    }
    let after = std::fs::read_to_string(&dlq).unwrap();
    assert!(after.lines().count() < lines_before, "{after}");
    assert!(!after.contains("\"quality\""), "{after}");
    assert!(!dir.path().join("dlq.jsonl.archived").exists());
}

fn replay_args(config: PathBuf, from: &Path, dry_run: bool, json: bool) -> DlqArgs {
    DlqArgs {
        command: DlqCommand::Replay(DlqReplayArgs {
            encryption_key: vec![],
            encryption_key_file: vec![],
            config: Some(config),
            from: from.to_string_lossy().into_owned(),
            reason: None,
            failed_dlq: None,
            row: None,
            all_rows: false,
            dry_run,
            json,
            env_file: None,
            no_env_file: true,
            profile: None,
        }),
    }
}

fn replay_config(dir: &Path, out: &Path) -> PathBuf {
    write(
        dir,
        "replay.yaml",
        &format!(
            "version: 1\nname: replay\npipeline:\n  source: {{ type: csv, config: {{ path: /dev/null }} }}\n  sink: {{ type: jsonl, config: {{ path: \"{}\" }} }}\n",
            out.display()
        ),
    )
}

#[tokio::test]
async fn replay_dry_run_via_command() {
    let dir = tempfile::tempdir().unwrap();
    let dlq = dlq_file(dir.path());
    let out = dir.path().join("out.jsonl");
    let cfg = replay_config(dir.path(), &out);
    // JSON render + human render of the dry-run path.
    dlq::run(replay_args(cfg.clone(), &dlq, true, true))
        .await
        .unwrap();
    dlq::run(replay_args(cfg, &dlq, true, false)).await.unwrap();
    // Dry-run never writes the sink.
    assert!(!out.exists() || std::fs::read_to_string(&out).unwrap().trim().is_empty());
}

#[tokio::test]
async fn replay_real_via_command_writes_and_renders_human() {
    let dir = tempfile::tempdir().unwrap();
    let dlq = dlq_file(dir.path());
    let out = dir.path().join("out.jsonl");
    let cfg = replay_config(dir.path(), &out);
    // A real (non-dry-run) replay exercises the human summary render + the
    // sink-write path; both payloads land.
    dlq::run(replay_args(cfg, &dlq, false, false))
        .await
        .unwrap();
    let landed = std::fs::read_to_string(&out).unwrap();
    assert!(landed.contains("\"id\":1") || landed.contains("\"id\": 1"));
    assert!(landed.contains("\"id\":2") || landed.contains("\"id\": 2"));
}

/// A replay runs under the config's `resilience:` block (#871).
#[tokio::test]
async fn replay_runs_under_the_configs_resilience_block() {
    let dir = tempfile::tempdir().unwrap();
    let dlq = dlq_file(dir.path());
    let out = dir.path().join("out.jsonl");
    let cfg = write(
        dir.path(),
        "resilient.yaml",
        &format!(
            "version: 1\nname: replay\nresilience: {{ retry: {{ max_attempts: 2 }} }}\npipeline:\n  source: {{ type: csv, config: {{ path: /dev/null }} }}\n  sink: {{ type: jsonl, config: {{ path: \"{}\" }} }}\n",
            out.display()
        ),
    );
    dlq::run(replay_args(cfg, &dlq, false, true)).await.unwrap();
    let landed = std::fs::read_to_string(&out).unwrap();
    assert_eq!(landed.lines().count(), 2, "{landed}");
}

/// A shared DLQ holds another row's envelopes: the default replay skips them
/// and says so; `--all-rows` replays them too.
#[tokio::test]
async fn replay_skips_other_rows_envelopes_unless_all_rows() {
    let dir = tempfile::tempdir().unwrap();
    let other = json!({
        "error": { "kind": "QualityFailure", "message": "boom" },
        "reason": "quality",
        "payload": {"id": 9},
        "ts_ms": 1_751_760_000_000i64,
        "sink": "jsonl", "pipeline": "replay", "row": "eu", "record_index": 0,
    })
    .to_string();
    let dlq = write(
        dir.path(),
        "shared.jsonl",
        &format!("{}\n{other}\n", envelope("quality", json!({"id": 1}))),
    );
    let out = dir.path().join("out.jsonl");
    let cfg = replay_config(dir.path(), &out);
    dlq::run(replay_args(cfg.clone(), &dlq, false, false))
        .await
        .unwrap();
    let landed = std::fs::read_to_string(&out).unwrap();
    assert_eq!(landed.lines().count(), 1, "{landed}");
    assert!(!landed.contains("\"id\":9"), "{landed}");

    let mut args = replay_args(cfg, &dlq, false, true);
    if let DlqCommand::Replay(r) = &mut args.command {
        r.all_rows = true;
    }
    dlq::run(args).await.unwrap();
    let landed = std::fs::read_to_string(&out).unwrap();
    assert!(landed.contains("\"id\":9"), "{landed}");
}
