//! A replay a caller starts writes a dead-lettered payload without masking it
//! again only when the envelope carries a valid signed record of the masking
//! in force today; a local `faucet dlq replay` keeps trusting the envelope's
//! writer.
#![cfg(all(
    feature = "masking",
    feature = "serve",
    feature = "sink-jsonl",
    feature = "source-csv"
))]

use faucet_cli::auth_catalog::build_auth_catalog;
use faucet_cli::config::parse_with_extension;
use faucet_cli::dlq_replay::ReplayInputs;
use faucet_cli::runtime::{Origin, Principal};
use serde_json::{Value, json};

fn inputs() -> ReplayInputs<'static> {
    ReplayInputs {
        decryptor: Default::default(),
        reason: None,
        failed_dlq: None,
        row: None,
        all_rows: false,
        dry_run: false,
        pipeline_name: "replay".into(),
        execution: None,
        auth: build_auth_catalog(None).unwrap(),
        clock: chrono::Utc::now().fixed_offset(),
        budget: None,
        cancel: None,
    }
}

async fn replay_as(principal: &Principal) -> Value {
    let dir = tempfile::tempdir().unwrap();
    let dlq = dir.path().join("dlq.jsonl");
    let out = dir.path().join("out.jsonl");
    let cfg_yaml = format!(
        "version: 1\nname: replay\npipeline:\n  source: {{ type: csv, config: {{ path: /dev/null }} }}\n  sink: {{ type: jsonl, config: {{ path: {} }} }}\n  masking:\n    rules:\n      - match: {{ fields: [email] }}\n        action: {{ type: redact }}\n",
        out.display()
    );
    let cfg = parse_with_extension(&cfg_yaml, "yaml").unwrap();
    let row = faucet_cli::expand::expand(&cfg).unwrap().remove(0).id;
    let envelope = json!({
        "error": {"kind": "Sink", "message": "boom"}, "reason": "sink_rejected",
        "payload": {"email": "a@b.example"}, "ts_ms": 1, "sink": "jsonl",
        "pipeline": "replay", "row": row, "record_index": 0,
    });
    std::fs::write(&dlq, format!("{envelope}\n")).unwrap();
    faucet_cli::runtime::local::replay_dlq(principal, &cfg, dlq.to_str().unwrap(), inputs())
        .await
        .expect("replay succeeds");
    let text = std::fs::read_to_string(&out).unwrap();
    serde_json::from_str(text.lines().next().unwrap()).unwrap()
}

#[tokio::test]
async fn a_caller_replay_masks_payloads_that_carry_no_proof() {
    let local = replay_as(&Principal::local()).await;
    assert_eq!(
        local["email"], "a@b.example",
        "the local CLI trusts the writer"
    );
    let caller = Principal::new(
        faucet_cli::serve::rbac::AuthContext::runtime(),
        Origin::Caller {
            allow_subprocess: false,
        },
    );
    assert_eq!(replay_as(&caller).await["email"], "***");
}

#[tokio::test]
async fn a_caller_replay_leaves_out_payloads_a_hash_rule_would_mask_again() {
    let dir = tempfile::tempdir().unwrap();
    let dlq = dir.path().join("dlq.jsonl");
    let out = dir.path().join("out.jsonl");
    let cfg_yaml = format!(
        "version: 1\nname: replay\npipeline:\n  source: {{ type: csv, config: {{ path: /dev/null }} }}\n  sink: {{ type: jsonl, config: {{ path: {} }} }}\n  masking:\n    rules:\n      - match: {{ fields: [email] }}\n        action: {{ type: hash }}\n",
        out.display()
    );
    let cfg = parse_with_extension(&cfg_yaml, "yaml").unwrap();
    let row = faucet_cli::expand::expand(&cfg).unwrap().remove(0).id;
    let line = |payload: Value| {
        json!({
            "error": {"kind": "Sink", "message": "boom"}, "reason": "sink_rejected",
            "payload": payload, "ts_ms": 1, "sink": "jsonl",
            "pipeline": "replay", "row": row, "record_index": 0,
        })
        .to_string()
    };
    std::fs::write(
        &dlq,
        format!(
            "{}\n{}\n",
            line(json!({"email": "5f0c-hashed"})),
            line(json!({"id": 7}))
        ),
    )
    .unwrap();
    let caller = Principal::new(
        faucet_cli::serve::rbac::AuthContext::runtime(),
        Origin::Caller {
            allow_subprocess: false,
        },
    );
    let outcome =
        faucet_cli::runtime::local::replay_dlq(&caller, &cfg, dlq.to_str().unwrap(), inputs())
            .await
            .expect("replay succeeds");
    assert_eq!(outcome.candidates, 2);
    assert_eq!(outcome.skipped_unproven, 1);
    assert_eq!(outcome.records_written, 1);
    let text = std::fs::read_to_string(&out).unwrap();
    assert!(!text.contains("5f0c-hashed"), "{text}");
    assert_eq!(text.lines().count(), 1, "{text}");
}

#[test]
fn installed_keys_sign_and_verify() {
    use faucet_cli::dlq_replay::proof;
    let spec: faucet_core::MaskingSpec = serde_json::from_value(json!({
        "rules": [{"match": {"fields": ["a"]}, "action": {"type": "redact"}}]
    }))
    .unwrap();
    proof::install_keys(vec![b"current".to_vec(), b"previous".to_vec()]);
    let mut e = json!({"pipeline": "p", "row": "r", "payload": {"a": "***"}});
    proof::signer(&spec, &["x"], Some("t"))
        .unwrap()
        .sign(&mut e);
    let env = faucet_core::unwrap_envelope(&e).unwrap();
    assert!(proof::verified(&env, &spec, &["x"], Some("t")));
    assert!(proof::ProofCheck::new(spec.clone(), &["x"], Some("t".into())).holds(&env));
    assert!(!proof::verified(&env, &spec, &["x"], Some("u")));
    proof::install_keys(Vec::new());
    assert!(proof::signer(&spec, &["x"], None).is_none());
    assert!(!proof::verified(&env, &spec, &["x"], Some("t")));
}
