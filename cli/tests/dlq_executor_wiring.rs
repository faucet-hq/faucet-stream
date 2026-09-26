//! Unit-ish integration test confirming the CLI executor's `build_dlq_config`
//! correctly bridges the YAML-shaped DlqSpec into a runtime DlqConfig.

use faucet_cli::config::{ConnectorSpec, DlqSpec, OnBatchErrorSpec};
use faucet_cli::executor::build_dlq_config;
use serde_json::json;
use tempfile::tempdir;

#[tokio::test]
async fn build_dlq_config_constructs_runtime_config_from_spec() {
    let dir = tempdir().unwrap();
    let dlq_path = dir.path().join("dlq.jsonl");
    let spec = DlqSpec {
        sink: ConnectorSpec {
            kind: "jsonl".into(),
            config: json!({ "path": dlq_path }),
            transforms: None,
            inherit_transforms: true,
            status: None,
            tags: Vec::new(),
            complete_for: None,
            attributes: Default::default(),
        },
        on_batch_error: OnBatchErrorSpec::DlqAll,
        max_failures_per_page: Some(100),
        max_failures_total: Some(10000),
        include_original_payload: true,
    };
    let cfg = build_dlq_config(&spec).await.expect("build_dlq_config");
    assert!(cfg.include_original_payload);
    assert_eq!(cfg.max_failures_per_page, Some(100));
    assert_eq!(cfg.max_failures_total, Some(10000));
    // on_batch_error mapping verified by enum equality:
    assert!(matches!(
        cfg.on_batch_error,
        faucet_core::OnBatchError::DlqAll
    ));
}

#[tokio::test]
async fn build_dlq_config_defaults_propagate_policy() {
    let dir = tempdir().unwrap();
    let dlq_path = dir.path().join("dlq.jsonl");
    let spec = DlqSpec {
        sink: ConnectorSpec {
            kind: "jsonl".into(),
            config: json!({ "path": dlq_path }),
            transforms: None,
            inherit_transforms: true,
            status: None,
            tags: Vec::new(),
            complete_for: None,
            attributes: Default::default(),
        },
        on_batch_error: OnBatchErrorSpec::Propagate,
        max_failures_per_page: None,
        max_failures_total: None,
        include_original_payload: true,
    };
    let cfg = build_dlq_config(&spec).await.expect("build_dlq_config");
    assert!(matches!(
        cfg.on_batch_error,
        faucet_core::OnBatchError::Propagate
    ));
    assert!(cfg.max_failures_per_page.is_none());
}
