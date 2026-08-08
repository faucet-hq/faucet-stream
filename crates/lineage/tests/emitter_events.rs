//! Emitter behaviour tests — no live services.
//!
//! Builds `RunEvent`s via the public `LineageEmitter` API and asserts the
//! emitted OpenLineage JSON for every lifecycle event (START/RUNNING/COMPLETE/
//! ABORT/FAIL), the schema / column-lineage / source-code / parent facets, and
//! that a failing transport is dropped (never propagated). The file transport
//! is pointed at a tempfile; the HTTP transport is exercised with wiremock.

use faucet_lineage::*;
use std::path::PathBuf;
use std::time::Duration;

fn file_cfg(path: PathBuf) -> LineageConfig {
    LineageConfig {
        kind: Default::default(),
        namespace: "ns".into(),
        transport: Transport::File { path },
        job_name: "j".into(),
        parent_job: None,
        include_column_lineage: false,
        include_schema_facet: false,
        include_source_code_facet: false,
        emit_on: EmitOn::default(),
        sample_records: 100,
        heartbeat_interval: Duration::from_secs(30),
    }
}

fn lifecycle() -> RunLifecycle {
    RunLifecycle {
        job_namespace: "ns".into(),
        job_name: "j".into(),
        run_id: "r1".into(),
        parent: None,
        inputs: vec![DatasetRef {
            namespace: "ns".into(),
            name: "postgres://h/db".into(),
        }],
        output: DatasetRef {
            namespace: "ns".into(),
            name: "bigquery://p.d.t".into(),
        },
        started_at: chrono::Utc::now(),
        finished_at: None,
        records: 0,
        error: None,
        input_schemas: Vec::new(),
        output_schema: None,
        column_lineage: None,
        source_code: None,
    }
}

/// Read every JSON line emitted to `path`.
fn read_lines(path: &PathBuf) -> Vec<serde_json::Value> {
    std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

#[tokio::test]
async fn complete_event_emits_terminal_event_type() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ol.jsonl");
    let em = LineageEmitter::new(file_cfg(path.clone())).unwrap();
    let mut done = lifecycle();
    done.finished_at = Some(chrono::Utc::now());
    done.records = 42;
    em.emit(EventType::Complete, &done).await;
    let events = read_lines(&path);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["eventType"], "COMPLETE");
    assert_eq!(events[0]["job"]["name"], "j");
    assert_eq!(events[0]["job"]["namespace"], "ns");
    assert_eq!(events[0]["run"]["runId"], "r1");
    assert_eq!(events[0]["outputs"][0]["name"], "bigquery://p.d.t");
    // finished_at present → eventTime is the finish time, nominalEndTime set.
    assert!(events[0]["run"]["facets"]["nominalTime"]["nominalEndTime"].is_string());
}

#[tokio::test]
async fn abort_event_emits_abort_type() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ol.jsonl");
    let em = LineageEmitter::new(file_cfg(path.clone())).unwrap();
    em.emit(EventType::Abort, &lifecycle()).await;
    let events = read_lines(&path);
    assert_eq!(events[0]["eventType"], "ABORT");
}

#[tokio::test]
async fn fail_event_emits_fail_type() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ol.jsonl");
    let em = LineageEmitter::new(file_cfg(path.clone())).unwrap();
    em.emit(EventType::Fail, &lifecycle()).await;
    let events = read_lines(&path);
    assert_eq!(events[0]["eventType"], "FAIL");
}

#[tokio::test]
async fn running_event_emits_when_enabled() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ol.jsonl");
    let mut cfg = file_cfg(path.clone());
    cfg.emit_on.running = true; // default is false
    let em = LineageEmitter::new(cfg).unwrap();
    em.emit(EventType::Running, &lifecycle()).await;
    let events = read_lines(&path);
    assert_eq!(events[0]["eventType"], "RUNNING");
}

#[tokio::test]
async fn schema_facet_emitted_on_terminal_event_when_enabled() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ol.jsonl");
    let mut cfg = file_cfg(path.clone());
    cfg.include_schema_facet = true;
    let em = LineageEmitter::new(cfg).unwrap();
    let mut done = lifecycle();
    done.finished_at = Some(chrono::Utc::now());
    done.input_schemas = vec![Some(InferredSchema {
        fields: vec![
            ("id".into(), "integer".into()),
            ("name".into(), "string".into()),
        ],
    })];
    done.output_schema = Some(InferredSchema {
        fields: vec![("id".into(), "integer".into())],
    });
    em.emit(EventType::Complete, &done).await;
    let events = read_lines(&path);
    let inp = &events[0]["inputs"][0]["facets"]["schema"]["fields"];
    assert_eq!(inp[0]["name"], "id");
    assert_eq!(inp[0]["type"], "integer");
    assert_eq!(inp[1]["name"], "name");
    assert_eq!(inp[1]["type"], "string");
    let out = &events[0]["outputs"][0]["facets"]["schema"]["fields"];
    assert_eq!(out[0]["name"], "id");
}

#[tokio::test]
async fn schema_facet_suppressed_on_start_even_when_enabled() {
    // Schema facets only attach on terminal events (Complete/Abort/Fail).
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ol.jsonl");
    let mut cfg = file_cfg(path.clone());
    cfg.include_schema_facet = true;
    let em = LineageEmitter::new(cfg).unwrap();
    let mut start = lifecycle();
    start.input_schemas = vec![Some(InferredSchema {
        fields: vec![("id".into(), "integer".into())],
    })];
    em.emit(EventType::Start, &start).await;
    let events = read_lines(&path);
    assert_eq!(events[0]["eventType"], "START");
    // No facets object on the input dataset (empty facets are skipped).
    assert!(events[0]["inputs"][0].get("facets").is_none());
}

#[tokio::test]
async fn column_lineage_facet_emitted_for_supported_ops() {
    // rename + select chain: a supported (non-opaque) op chain.
    let inputs = vec!["id".to_string(), "email".to_string(), "name".to_string()];
    let ops = [
        ColumnOp::Rename(vec![("email".into(), "contact".into())]),
        ColumnOp::Select(vec!["id".into(), "contact".into()]),
    ];
    let cl = derive_column_lineage(&inputs, &ops).expect("supported ops derive lineage");

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ol.jsonl");
    let mut cfg = file_cfg(path.clone());
    cfg.include_column_lineage = true;
    let em = LineageEmitter::new(cfg).unwrap();
    let mut done = lifecycle();
    done.finished_at = Some(chrono::Utc::now());
    done.column_lineage = Some(cl);
    em.emit(EventType::Complete, &done).await;

    let events = read_lines(&path);
    let fields = &events[0]["outputs"][0]["facets"]["columnLineage"]["fields"];
    // `contact` derives from input field `email` in the input dataset.
    // (`ColumnLineageFieldEntry` serializes `input_fields` as-is, not camelCase.)
    assert_eq!(fields["contact"]["input_fields"][0]["field"], "email");
    assert_eq!(
        fields["contact"]["input_fields"][0]["name"],
        "postgres://h/db"
    );
    assert_eq!(fields["contact"]["input_fields"][0]["namespace"], "ns");
    // `id` survives select, maps to itself.
    assert_eq!(fields["id"]["input_fields"][0]["field"], "id");
    // `name` was dropped by select → not present.
    assert!(fields.get("name").is_none());
}

#[tokio::test]
async fn column_lineage_set_literal_has_no_input_edge() {
    // A `set` field is a literal — no upstream edge, so it is omitted from the
    // emitted facet (column_facet skips empty source lists).
    let inputs = vec!["id".to_string()];
    let ops = [ColumnOp::Set(vec!["created_at".into()])];
    let cl = derive_column_lineage(&inputs, &ops).unwrap();
    // The derived lineage records the literal field with an empty source list.
    assert!(cl.edges.get("created_at").unwrap().is_empty());

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ol.jsonl");
    let mut cfg = file_cfg(path.clone());
    cfg.include_column_lineage = true;
    let em = LineageEmitter::new(cfg).unwrap();
    let mut done = lifecycle();
    done.finished_at = Some(chrono::Utc::now());
    done.column_lineage = Some(cl);
    em.emit(EventType::Complete, &done).await;

    let events = read_lines(&path);
    let fields = &events[0]["outputs"][0]["facets"]["columnLineage"]["fields"];
    // The literal `created_at` has no upstream edge → suppressed in the facet.
    assert!(fields.get("created_at").is_none());
    // `id` (identity passthrough) is present.
    assert_eq!(fields["id"]["input_fields"][0]["field"], "id");
}

#[tokio::test]
async fn identity_ops_preserve_all_columns() {
    // cast / redact / value_case / spell_symbols all map to ColumnOp::Identity.
    let inputs = vec!["a".to_string(), "b".to_string()];
    let cl = derive_column_lineage(
        &inputs,
        &[ColumnOp::Identity, ColumnOp::Identity, ColumnOp::Identity],
    )
    .unwrap();
    assert_eq!(cl.edges.get("a").unwrap(), &vec!["a".to_string()]);
    assert_eq!(cl.edges.get("b").unwrap(), &vec!["b".to_string()]);
    assert_eq!(cl.edges.len(), 2);
}

#[tokio::test]
async fn drop_op_removes_column_from_lineage() {
    let inputs = vec!["a".to_string(), "b".to_string(), "c".to_string()];
    let cl = derive_column_lineage(&inputs, &[ColumnOp::Drop(vec!["b".into()])]).unwrap();
    assert!(!cl.edges.contains_key("b"));
    assert_eq!(cl.edges.len(), 2);
}

#[tokio::test]
async fn opaque_op_suppresses_column_lineage_facet() {
    // An opaque op (flatten/explode/keys_case/rename_keys/custom) → derive None.
    let inputs = vec!["id".to_string()];
    assert!(derive_column_lineage(&inputs, &[ColumnOp::Opaque]).is_none());
    assert!(
        derive_column_lineage(&inputs, &[ColumnOp::Rename(vec![]), ColumnOp::Opaque]).is_none()
    );

    // With include_column_lineage on but no column_lineage on the context (the
    // CLI sets None when the chain is opaque), no columnLineage facet appears.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ol.jsonl");
    let mut cfg = file_cfg(path.clone());
    cfg.include_column_lineage = true;
    let em = LineageEmitter::new(cfg).unwrap();
    let mut done = lifecycle();
    done.finished_at = Some(chrono::Utc::now());
    done.column_lineage = None; // opaque chain → no lineage
    em.emit(EventType::Complete, &done).await;

    let events = read_lines(&path);
    // No facets object at all (schema disabled, column-lineage None → empty).
    assert!(events[0]["outputs"][0].get("facets").is_none());
}

#[tokio::test]
async fn source_code_facet_emitted_when_enabled() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ol.jsonl");
    let mut cfg = file_cfg(path.clone());
    cfg.include_source_code_facet = true; // triggers the constructor warn branch
    let em = LineageEmitter::new(cfg).unwrap();
    let mut ctx = lifecycle();
    ctx.source_code = Some("source: { type: rest }".into());
    em.emit(EventType::Start, &ctx).await;
    let events = read_lines(&path);
    let sc = &events[0]["job"]["facets"]["sourceCode"];
    assert_eq!(sc["language"], "yaml");
    assert_eq!(sc["sourceCode"], "source: { type: rest }");
}

#[tokio::test]
async fn parent_run_facet_built_from_parent_job() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ol.jsonl");
    let em = LineageEmitter::new(file_cfg(path.clone())).unwrap();
    let mut ctx = lifecycle();
    ctx.parent = Some(ParentJob {
        namespace: "airflow".into(),
        name: "dag.task".into(),
        run_id: Some("parent-run-99".into()),
    });
    em.emit(EventType::Start, &ctx).await;
    let events = read_lines(&path);
    let parent = &events[0]["run"]["facets"]["parent"];
    assert_eq!(parent["job"]["namespace"], "airflow");
    assert_eq!(parent["job"]["name"], "dag.task");
    assert_eq!(parent["run"]["runId"], "parent-run-99");
}

#[tokio::test]
async fn parent_run_facet_falls_back_to_run_id_when_parent_run_id_absent() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ol.jsonl");
    let em = LineageEmitter::new(file_cfg(path.clone())).unwrap();
    let mut ctx = lifecycle();
    ctx.parent = Some(ParentJob {
        namespace: "airflow".into(),
        name: "dag.task".into(),
        run_id: None, // → falls back to ctx.run_id ("r1")
    });
    em.emit(EventType::Start, &ctx).await;
    let events = read_lines(&path);
    assert_eq!(events[0]["run"]["facets"]["parent"]["run"]["runId"], "r1");
}

#[tokio::test]
async fn disabled_event_is_dropped_not_emitted() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ol.jsonl");
    let mut cfg = file_cfg(path.clone());
    cfg.emit_on.complete = false;
    let em = LineageEmitter::new(cfg).unwrap();
    em.emit(EventType::Complete, &lifecycle()).await;
    // Disabled → nothing written, no panic.
    assert!(!path.exists() || std::fs::read_to_string(&path).unwrap().is_empty());
}

// ---- HTTP transport (wiremock) ----

#[tokio::test]
async fn http_transport_posts_event_body() {
    use wiremock::matchers::{header, method, path as wm_path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(wm_path("/api/v1/lineage"))
        .and(header("content-type", "application/json"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    let cfg = LineageConfig {
        kind: Default::default(),
        namespace: "ns".into(),
        transport: Transport::Http {
            url: format!("{}/api/v1/lineage", server.uri()),
            timeout_secs: Duration::from_secs(5),
            auth: None,
        },
        job_name: "j".into(),
        parent_job: None,
        include_column_lineage: false,
        include_schema_facet: false,
        include_source_code_facet: false,
        emit_on: EmitOn::default(),
        sample_records: 100,
        heartbeat_interval: Duration::from_secs(30),
    };
    let em = LineageEmitter::new(cfg).unwrap();
    em.emit(EventType::Start, &lifecycle()).await;

    // Inspect the actually-received request body.
    let received = server.received_requests().await.unwrap();
    assert_eq!(received.len(), 1);
    let body: serde_json::Value = serde_json::from_slice(&received[0].body).unwrap();
    assert_eq!(body["eventType"], "START");
    assert_eq!(body["job"]["name"], "j");
    assert_eq!(body["inputs"][0]["name"], "postgres://h/db");
}

#[tokio::test]
async fn http_transport_sends_bearer_auth_header() {
    use wiremock::matchers::{header, method};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(header("authorization", "Bearer s3cr3t"))
        .respond_with(ResponseTemplate::new(201))
        .expect(1)
        .mount(&server)
        .await;

    let cfg = LineageConfig {
        kind: Default::default(),
        namespace: "ns".into(),
        transport: Transport::Http {
            url: server.uri(),
            timeout_secs: Duration::from_secs(5),
            auth: Some(HttpAuth::Bearer {
                token: "s3cr3t".into(),
            }),
        },
        job_name: "j".into(),
        parent_job: None,
        include_column_lineage: false,
        include_schema_facet: false,
        include_source_code_facet: false,
        emit_on: EmitOn::default(),
        sample_records: 100,
        heartbeat_interval: Duration::from_secs(30),
    };
    let em = LineageEmitter::new(cfg).unwrap();
    em.emit(EventType::Start, &lifecycle()).await;
    // The .expect(1) on the mock asserts exactly one matching (authed) request
    // on server drop.
}

#[tokio::test]
async fn http_transport_error_is_dropped_not_propagated() {
    // A 500 from the endpoint makes the transport return Err; the emitter must
    // log + count it (faucet_lineage_dropped_total) and return without
    // propagating — emit() has no Result and must not panic.
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;

    let cfg = LineageConfig {
        kind: Default::default(),
        namespace: "ns".into(),
        transport: Transport::Http {
            url: server.uri(),
            timeout_secs: Duration::from_secs(5),
            auth: None,
        },
        job_name: "j".into(),
        parent_job: None,
        include_column_lineage: false,
        include_schema_facet: false,
        include_source_code_facet: false,
        emit_on: EmitOn::default(),
        sample_records: 100,
        heartbeat_interval: Duration::from_secs(30),
    };
    let em = LineageEmitter::new(cfg).unwrap();
    // Returns (), must not panic, despite the 500.
    em.emit(EventType::Start, &lifecycle()).await;
}

#[tokio::test]
async fn http_transport_connection_failure_is_dropped() {
    // Point at an unroutable / closed address: the request fails at the network
    // layer (reqwest error). The emitter must still swallow it.
    let cfg = LineageConfig {
        kind: Default::default(),
        namespace: "ns".into(),
        transport: Transport::Http {
            // 127.0.0.1:1 — nothing listens; connection refused.
            url: "http://127.0.0.1:1/api/v1/lineage".into(),
            timeout_secs: Duration::from_secs(2),
            auth: None,
        },
        job_name: "j".into(),
        parent_job: None,
        include_column_lineage: false,
        include_schema_facet: false,
        include_source_code_facet: false,
        emit_on: EmitOn::default(),
        sample_records: 100,
        heartbeat_interval: Duration::from_secs(30),
    };
    let em = LineageEmitter::new(cfg).unwrap();
    em.emit(EventType::Start, &lifecycle()).await; // must not panic
}
