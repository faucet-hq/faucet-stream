//! An embedder's use of the registry, end to end and through this crate only:
//! build a source and a sink by `type:` name, move rows, resume from state.

#![cfg(all(feature = "source-file", feature = "sink-jsonl"))]

use std::sync::Arc;

use faucet_core::{FileStateStore, Pipeline};
use faucet_registry::{
    BuildPolicy, RegistryError, auth_catalog, build_sink, build_source, build_source_with,
    sink_exists, sink_kinds, source_exists, source_kinds, source_resumes_from_bookmark,
    validate_sink_config, validate_source_config,
};
use serde_json::{Value, json};

fn read_jsonl(path: &std::path::Path) -> Vec<Value> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

async fn run_once(source_cfg: &Value, out: &std::path::Path, state: &std::path::Path) -> usize {
    let auth = auth_catalog::build_auth_catalog(None).unwrap();
    let source = build_source("file", source_cfg.clone(), &auth, None)
        .await
        .unwrap();
    let sink = build_sink(
        "jsonl",
        json!({ "path": out.to_str().unwrap(), "append": true }),
        &auth,
    )
    .await
    .unwrap();
    Pipeline::new(source.as_ref(), sink.as_ref())
        .with_state_store(Arc::new(FileStateStore::new(state)))
        .run()
        .await
        .unwrap()
        .records_written
}

#[tokio::test]
async fn rows_move_by_name_and_a_rerun_resumes_from_state() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in");
    std::fs::create_dir(&input).unwrap();
    std::fs::write(input.join("a.jsonl"), "{\"id\":1}\n{\"id\":2}\n").unwrap();
    let out = dir.path().join("out.jsonl");
    let state = dir.path().join("state");

    assert!(source_kinds().contains(&"file") && source_exists("file"));
    assert!(sink_kinds().contains(&"jsonl") && sink_exists("jsonl"));
    let source_cfg = json!({
        "path": input.to_str().unwrap(),
        "incremental": { "by": "name" },
    });
    assert!(source_resumes_from_bookmark("file", &source_cfg));
    validate_source_config("file", "in", source_cfg.clone()).unwrap();
    validate_sink_config("jsonl", "out", json!({ "path": "x.jsonl" })).unwrap();

    assert_eq!(run_once(&source_cfg, &out, &state).await, 2);
    assert_eq!(read_jsonl(&out), vec![json!({"id": 1}), json!({"id": 2})]);

    // Nothing new: the stored bookmark makes the second run a no-op.
    assert_eq!(run_once(&source_cfg, &out, &state).await, 0);

    std::fs::write(input.join("b.jsonl"), "{\"id\":3}\n").unwrap();
    assert_eq!(run_once(&source_cfg, &out, &state).await, 1);
    assert_eq!(read_jsonl(&out).len(), 3);
}

#[tokio::test]
async fn configs_are_used_as_given_and_errors_are_typed() {
    let dir = tempfile::tempdir().unwrap();
    let literal = dir.path().join("a$${x}.jsonl");
    std::fs::write(&literal, "{\"id\":1}\n").unwrap();
    let auth = auth_catalog::AuthCatalog::new();
    let source = build_source(
        "file",
        json!({ "path": literal.to_str().unwrap() }),
        &auth,
        None,
    )
    .await
    .unwrap();
    assert_eq!(source.fetch_all().await.unwrap(), vec![json!({"id": 1})]);

    match build_source("no-such-kind", json!({}), &auth, None).await {
        Err(RegistryError::UnknownConnector { kind, name, .. }) => {
            assert_eq!((kind, name.as_str()), ("source", "no-such-kind"))
        }
        other => panic!("expected UnknownConnector, got {:?}", other.map(|_| ())),
    }
    match build_sink("jsonl", json!({ "path": 5 }), &auth).await {
        Err(RegistryError::InvalidConnectorConfig { path, message, .. }) => {
            assert_eq!(path.as_deref(), Some("path"), "{message}");
        }
        other => panic!(
            "expected InvalidConnectorConfig, got {:?}",
            other.map(|_| ())
        ),
    }
    let err = build_source("file", json!({ "path": "x", "nope": true }), &auth, None)
        .await
        .err()
        .unwrap();
    assert!(err.to_string().contains("`nope`"), "{err}");
    let trusted = build_source_with(
        "file",
        json!({ "path": literal.to_str().unwrap() }),
        &auth,
        None,
        &BuildPolicy::trusted(),
    )
    .await;
    assert!(trusted.is_ok());
}
