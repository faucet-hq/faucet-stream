
use super::*;
use crate::serve::history::catalog::ConnectorSnapshot;
use serde_json::json;

fn conn(kind: &str, cfg: Value) -> ConnectorSnapshot {
    ConnectorSnapshot {
        kind: kind.into(),
        config: cfg,
    }
}

fn row(source_cfg: Value) -> RowSnapshot {
    RowSnapshot {
        source: conn("rest", source_cfg),
        sink: conn("jsonl", json!({"path": "out.jsonl"})),
        transforms: vec![],
        state_key: None,
        delivery_guarantee: "AtLeastOnce".into(),
        on_error: "stop".into(),
        dlq: false,
        contract: None,
    }
}

fn snap(rows: Vec<(&str, RowSnapshot)>) -> ConfigSnapshot {
    ConfigSnapshot {
        pipeline: "p".into(),
        recorded_at: DateTime::parse_from_rfc3339("2026-07-18T00:00:00Z")
            .unwrap()
            .to_utc(),
        faucet_version: "0.0.0".into(),
        rows: rows.into_iter().map(|(k, v)| (k.to_owned(), v)).collect(),
    }
}

#[test]
fn first_run_marks_every_row_new() {
    let curr = snap(vec![("a", row(json!({"path": "/v1"})))]);
    let d = diff(None, &curr);
    assert!(d.first_run);
    assert_eq!(d.summary.create, 1);
    assert_eq!(d.rows[0].status, RowStatus::New);
}

#[test]
fn detects_added_removed_changed_and_unchanged() {
    let prev = snap(vec![
        ("payroll", row(json!({"path": "/v1/pay", "page_size": 100}))),
        ("benefits", row(json!({"path": "/v1/benefits"}))),
        ("employees", row(json!({"path": "/v1/emp"}))),
    ]);
    let curr = snap(vec![
        ("people", row(json!({"path": "/v1/people"}))), // NEW
        (
            "payroll",
            row(json!({"path": "/v1/payroll", "page_size": 500})),
        ), // CHANGED
        ("employees", row(json!({"path": "/v1/emp"}))), // unchanged
                                                        // benefits REMOVED
    ]);
    let d = diff(Some(&prev), &curr);
    assert_eq!(d.summary.create, 1);
    assert_eq!(d.summary.change, 1);
    assert_eq!(d.summary.remove, 1);
    assert_eq!(d.summary.unchanged, 1);

    let payroll = d.rows.iter().find(|r| r.id == "payroll").unwrap();
    assert_eq!(payroll.status, RowStatus::Changed);
    let paths: Vec<&str> = payroll.changes.iter().map(|c| c.path.as_str()).collect();
    assert!(paths.contains(&"source.config.path"));
    assert!(paths.contains(&"source.config.page_size"));
    let ps = payroll
        .changes
        .iter()
        .find(|c| c.path == "source.config.page_size")
        .unwrap();
    assert_eq!(ps.before.as_deref(), Some("100"));
    assert_eq!(ps.after.as_deref(), Some("500"));
}

#[test]
fn secret_rotation_is_surfaced_not_printed() {
    let prev = snap(vec![(
        "r",
        row(json!({"token": "<secret:sha256:aaaaaaaaaaaa>"})),
    )]);
    let curr = snap(vec![(
        "r",
        row(json!({"token": "<secret:sha256:bbbbbbbbbbbbb>"})),
    )]);
    let d = diff(Some(&prev), &curr);
    let r = &d.rows[0];
    assert_eq!(r.status, RowStatus::Changed);
    assert!(r.changes[0].secret_rotated);
    let text = render_human(&d);
    assert!(text.contains("secret rotated"), "{text}");
    assert!(!text.contains("bbbbbbbbbbbb"), "hash should not be printed");
}

#[test]
fn the_snapshot_key_file_is_created_once_owner_only() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("faucet/snapshot.key");
    let first = load_or_create_key(&path).unwrap();
    assert_eq!(first.len(), 32);
    assert_eq!(load_or_create_key(&path).unwrap(), first);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
    }
    let blocked = dir.path().join("file");
    std::fs::write(&blocked, b"x").unwrap();
    assert!(load_or_create_key(&blocked.join("k")).is_err());
    assert!(is_secret_token("<secret:sha256:abc>"));
    assert_ne!(random_key(), random_key());
}

#[test]
fn redact_value_replaces_registered_secret_with_stable_token() {
    crate::secrets::registry::register("supersecrettoken");
    let redacted = redact_value(&json!({"auth": "supersecrettoken", "path": "/v1"}));
    let token = redacted["auth"].as_str().unwrap();
    assert!(token.starts_with("<secret:hmac:"), "{token}");
    assert_eq!(token.len(), "<secret:hmac:>".len() + 32);
    assert_eq!(redacted["path"], json!("/v1"));
    // Stable: same value → same token.
    let again = redact_value(&json!("supersecrettoken"));
    assert_eq!(again.as_str().unwrap(), token);
}

#[test]
fn redact_value_hides_inline_credentials() {
    let redacted = redact_value(&json!({
        "connection_url": "postgres://app:S3cretPw@db:5432/prod",
        "password": "hunter22",
        "client_secret": 1234,
        "auth": {"type": "bearer", "config": {"token": "tok-literal-value"}},
        "api_key": "${param.api_key}",
        "next_token": "$.paging.next",
        "key": ["id"],
        "table": "orders",
    }));
    let text = redacted.to_string();
    for secret in ["S3cretPw", "hunter22", "tok-literal-value"] {
        assert!(!text.contains(secret), "{secret} leaked: {text}");
    }
    let url = redacted["connection_url"].as_str().unwrap();
    assert!(
        url.starts_with("postgres://db:5432/prod <secret:hmac:"),
        "{url}"
    );
    assert!(is_secret_token(redacted["password"].as_str().unwrap()));
    assert!(is_secret_token(redacted["client_secret"].as_str().unwrap()));
    assert!(is_secret_token(
        redacted["auth"]["config"]["token"].as_str().unwrap()
    ));
    assert_eq!(redacted["auth"]["type"], json!("bearer"));
    assert_eq!(redacted["api_key"], json!("${param.api_key}"));
    assert_eq!(redacted["next_token"], json!("$.paging.next"));
    assert_eq!(redacted["key"], json!(["id"]));
    assert_eq!(redacted["table"], json!("orders"));
    assert_eq!(redact_value(&redacted), redacted, "redaction is idempotent");
}

/// Write a config to a temp file and return (config, expanded nodes, path).
fn expand_config(
    yaml: &str,
) -> (
    crate::config::PipelineConfig,
    Vec<ExpandedNode>,
    std::path::PathBuf,
) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("p.yaml");
    std::fs::write(&path, yaml).unwrap();
    let cfg = crate::config::PipelineConfig::from_path_tolerating_secrets(&path, None).unwrap();
    let nodes = crate::expand::expand(&cfg).unwrap();
    std::mem::forget(dir); // keep the temp file alive for the returned path
    (cfg, nodes, path)
}

const REST_TO_JSONL: &str = r#"
version: 1
name: mypipe
pipeline:
  source:
    type: rest
    config:
      url: https://api.example.com/v1
      auth: { type: bearer, config: { token: topsecretvalue12345 } }
  sink:
    type: jsonl
    config:
      path: out.jsonl
  transforms:
    - type: flatten
      config: {}
"#;

#[test]
fn build_snapshot_shapes_rows_and_redacts_secrets() {
    crate::secrets::registry::register("topsecretvalue12345");
    let (cfg, nodes, path) = expand_config(REST_TO_JSONL);
    assert_eq!(resolve_name(&cfg, Some(&path)), "mypipe");
    assert_eq!(on_error_str(&cfg.execution), "continue"); // default

    let snap = build_snapshot(
        resolve_name(&cfg, Some(&path)),
        on_error_str(&cfg.execution),
        &nodes,
        Utc::now(),
    );
    assert_eq!(snap.pipeline, "mypipe");
    let r = snap.rows.values().next().unwrap();
    assert_eq!(r.source.kind, "rest");
    assert_eq!(r.sink.kind, "jsonl");
    assert_eq!(r.transforms.len(), 1);
    let src = serde_json::to_string(&r.source.config).unwrap();
    assert!(!src.contains("topsecretvalue12345"), "secret leaked: {src}");
    assert!(src.contains("api.example.com"), "non-secret url must show");
}

#[test]
fn resolve_name_falls_back_to_file_stem_then_default() {
    // No `name:` → file stem.
    let (cfg, _n, path) = expand_config(
        "version: 1\npipeline:\n  source: { type: rest, config: { url: https://x/y } }\n  sink: { type: jsonl, config: { path: o.jsonl } }\n",
    );
    assert_eq!(resolve_name(&cfg, Some(&path)), "p"); // p.yaml
    assert_eq!(resolve_name(&cfg, None), "pipeline"); // nothing → default
}

#[tokio::test]
async fn record_if_ok_records_only_on_success_with_a_catalog() {
    let (_cfg, nodes, _p) = expand_config(REST_TO_JSONL);
    let handle = crate::catalog::connect_from_spec(&crate::catalog::CatalogSpec {
        url: "memory".into(),
        sample_records: 10,
        datasets: Vec::new(),
    })
    .await
    .unwrap();

    // Failure → nothing recorded.
    record_if_ok(Some(&handle), "mypipe", "stop", &nodes, false, Utc::now()).await;
    assert!(
        handle
            .store
            .catalog_last_config_snapshot("mypipe")
            .await
            .unwrap()
            .is_none()
    );
    // No catalog → no-op (must not panic).
    record_if_ok(None, "mypipe", "stop", &nodes, true, Utc::now()).await;
    // Success + catalog → recorded.
    record_if_ok(Some(&handle), "mypipe", "stop", &nodes, true, Utc::now()).await;
    let got = handle
        .store
        .catalog_last_config_snapshot("mypipe")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.pipeline, "mypipe");
    assert!(!got.rows.is_empty());
}
