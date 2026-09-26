use super::*;
use crate::config::PipelineConfig;
use crate::pipeline_state::outcome::OutcomeEvent;
use faucet_core::MemoryStateStore;
use faucet_core::idempotency::wrap_state;
use serde_json::json;
use std::path::Path;

fn target(extra_top: &str, rows: &str) -> PipelineTarget {
    let text = format!(
        r#"version: 1
name: orders
pipeline:
  source: {{ type: csv, config: {{ path: in.csv }} }}
  sink: {{ type: stdout, config: {{}} }}
  state: {{ type: file, config: {{ path: ./unused-state }} }}
{extra_top}
matrix:
  - id: a
{rows}"#
    );
    let cfg = PipelineConfig::from_text(&text, Path::new("t.yaml")).unwrap();
    PipelineTarget::resolve(&cfg, "orders").unwrap()
}

async fn stores(t: &PipelineTarget) -> (Stores, Arc<dyn StateStore>) {
    let store: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
    (
        Stores::build(t, Some(Arc::clone(&store))).await.unwrap(),
        store,
    )
}

fn inputs(auth: &AuthCatalog) -> StatusInputs<'_> {
    StatusInputs {
        now: Utc::now(),
        row: None,
        probe: false,
        auth,
        history: Vec::new(),
        active_runs: Vec::new(),
    }
}

fn ev(ago_secs: i64, err: Option<&str>) -> OutcomeEvent {
    OutcomeEvent {
        at: Utc::now() - chrono::Duration::seconds(ago_secs),
        run_id: format!("r{ago_secs}"),
        records: 10,
        duration_ms: 5,
        error_kind: err.map(|_| "Sink".into()),
        error: err.map(str::to_owned),
    }
}

async fn put_outcome(store: &dyn StateStore, base: &str, events: Vec<OutcomeEvent>) {
    for e in events {
        outcome::record(store, base, e).await;
    }
}

#[tokio::test]
async fn warming_ok_and_failed_rows() {
    let t = target("", "  - id: b\n  - id: c\n");
    let (s, store) = stores(&t).await;
    let auth = AuthCatalog::new();
    store
        .put("orders::b", &json!({"updated_at": "2026-09-26"}))
        .await
        .unwrap();
    put_outcome(store.as_ref(), "orders::b", vec![ev(7200, None)]).await;
    store
        .put("orders::c", &json!({"lsn": "0/1"}))
        .await
        .unwrap();
    put_outcome(
        store.as_ref(),
        "orders::c",
        vec![ev(9000, None), ev(100, Some("deadlock detected"))],
    )
    .await;
    let r = assemble(&t, Ok(&s), &inputs(&auth)).await.unwrap();
    let by = |id: &str| r.rows.iter().find(|x| x.row == id).unwrap();
    assert_eq!(by("a").health, Health::Warming);
    assert_eq!(by("a").resume, "full snapshot");
    assert_eq!(by("b").health, Health::Ok);
    assert_eq!(by("b").resume, "updated_at=2026-09-26");
    assert!(by("b").bookmark_age_secs.unwrap() >= 7200);
    let c = by("c");
    assert_eq!(c.health, Health::Failed);
    assert!(c.reasons[0].contains("deadlock"));
    assert_eq!(c.consecutive_failures, 1);
    assert_eq!(r.health, Health::Failed);
    assert_eq!(r.exit_code, 2);
    let text = render::render(&r);
    assert!(text.contains("FAILED"), "{text}");
    assert!(
        text.contains("last error: Sink: deadlock detected"),
        "{text}"
    );
    assert!(text.contains("never"), "{text}");
}

#[tokio::test]
async fn degraded_by_sla_dlq_drift_and_a_crashed_lease() {
    let dir = tempfile::tempdir().unwrap();
    let dlq = dir.path().join("dlq.jsonl");
    std::fs::write(
        &dlq,
        json!({"payload": {}, "pipeline": "orders", "row": "a", "ts_ms": 1000}).to_string(),
    )
    .unwrap();
    let t = target(
        &format!(
            "  dlq:\n    sink: {{ type: jsonl, config: {{ path: {} }} }}\nsla:\n  max_staleness_secs: 60\n  min_rows_per_run: 100\nprofiling:\n  min_history: 2\n",
            dlq.display()
        ),
        "",
    );
    let (s, store) = stores(&t).await;
    let auth = AuthCatalog::new();
    put_outcome(store.as_ref(), "orders::a", vec![ev(3600, None)]).await;
    store
        .put(
            "orders::a::__sla__",
            &json!({"last_success_unix": (Utc::now().timestamp() - 3600), "volumes": [10]}),
        )
        .await
        .unwrap();
    store
        .put(
            "orders::a::__profiling__",
            &json!({"runs": [{"run_id": "x", "recorded_at": Utc::now(), "profile": {"rows": 1, "columns": {}}, "drift": [{"column": "c", "metric": "null_rate", "observed": 1.0, "baseline": 0.0, "detail": "d"}]}]}),
        )
        .await
        .unwrap();
    store
        .put(
            "orders::a::__lease__",
            &json!({"run_id": "dead", "pid": 1, "acquired_at": Utc::now(), "expires_at": Utc::now() - chrono::Duration::seconds(5)}),
        )
        .await
        .unwrap();
    store
        .put("orders::a::__rollback__", &json!({"runs": ["r1", "r2"]}))
        .await
        .unwrap();
    let r = assemble(&t, Ok(&s), &inputs(&auth)).await.unwrap();
    let a = &r.rows[0];
    assert_eq!(a.health, Health::Degraded, "{:?}", a.reasons);
    assert_eq!(a.dlq.count, 1);
    assert_eq!(a.sla.len(), 2, "{:?}", a.sla);
    assert_eq!(a.rollback.undoable_runs, 2);
    assert!(a.reasons.iter().any(|x| x.contains("crashed")));
    assert_eq!(r.exit_code, 1);
    let text = render::render(&r);
    assert!(
        text.contains("DEGRADED") && text.contains("DLQ: 1 record"),
        "{text}"
    );
    assert!(text.contains("rollback: 2 undoable"), "{text}");
}

#[tokio::test]
async fn running_rows_and_history_merges() {
    let t = target("", "  - id: b\n");
    let (s, store) = stores(&t).await;
    let auth = AuthCatalog::new();
    let g = crate::pipeline_state::lease::acquire(Arc::clone(&store), "orders::a", "live")
        .await
        .unwrap();
    put_outcome(store.as_ref(), "orders::a", vec![ev(60, None)]).await;
    let mut inp = inputs(&auth);
    inp.history = vec![
        HistoryRun {
            run_id: "h2".into(),
            row: "b".into(),
            at: Utc::now() - chrono::Duration::seconds(10),
            records: 0,
            error: Some("boom".into()),
        },
        HistoryRun {
            run_id: "h1".into(),
            row: "b".into(),
            at: Utc::now() - chrono::Duration::seconds(100),
            records: 5,
            error: None,
        },
    ];
    let r = assemble(&t, Ok(&s), &inp).await.unwrap();
    assert_eq!(r.rows[0].health, Health::Running);
    assert_eq!(r.rows[1].health, Health::Failed);
    assert_eq!(r.rows[1].last_success.as_ref().unwrap().source, "history");
    let text = render::render(&r);
    assert!(text.contains("running: run live"), "{text}");
    g.release().await;

    let mut inp = inputs(&auth);
    inp.active_runs = vec!["srv-1".into()];
    inp.row = Some("a");
    let r = assemble(&t, Ok(&s), &inp).await.unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0].health, Health::Running);
    assert!(r.rows[0].reasons.iter().any(|x| x.contains("srv-1")));
}

#[tokio::test]
async fn memory_state_and_unreachable_backends_are_unknown() {
    let text = "version: 1\nname: m\npipeline:\n  source: { type: csv, config: { path: a } }\n  sink: { type: stdout, config: {} }\n  state: { type: memory, config: {} }\n";
    let cfg = PipelineConfig::from_text(text, Path::new("t.yaml")).unwrap();
    let t = PipelineTarget::resolve(&cfg, "m").unwrap();
    let s = Stores::build(&t, None).await.unwrap();
    let auth = AuthCatalog::new();
    let r = assemble(&t, Ok(&s), &inputs(&auth)).await.unwrap();
    assert_eq!(r.rows[0].health, Health::Unknown);
    assert!(!r.state.durable);
    assert!(r.state.note.as_ref().unwrap().contains("no durable state"));
    assert_eq!(r.exit_code, 1);
    assert!(render::render(&r).contains("unknown between runs"));

    let t = target("", "");
    let r = assemble(&t, Err("connection refused".into()), &inputs(&auth))
        .await
        .unwrap();
    assert_eq!(r.rows[0].health, Health::Unknown);
    assert!(r.rows[0].errors[0].contains("connection refused"));
    assert!(r.state.note.unwrap().contains("unreachable"));
}

#[tokio::test]
async fn children_aggregate_under_their_parent() {
    let t = target("", "  - id: kid\n    parent: a\n    parent_key: id\n");
    let (s, store) = stores(&t).await;
    let auth = AuthCatalog::new();
    put_outcome(store.as_ref(), "orders::a", vec![ev(60, None)]).await;
    store.put("orders::kid::1", &json!(1)).await.unwrap();
    store.put("orders::kid::2", &json!(2)).await.unwrap();
    put_outcome(store.as_ref(), "orders::kid::1", vec![ev(30, Some("bad"))]).await;
    put_outcome(store.as_ref(), "orders::kid::2", vec![ev(30, None)]).await;
    let r = assemble(&t, Ok(&s), &inputs(&auth)).await.unwrap();
    assert_eq!(r.rows.len(), 1, "children fold under the parent");
    let a = &r.rows[0];
    assert_eq!(a.children[0].bookmarks, 2);
    assert_eq!(a.children[0].failed, 1);
    assert_eq!(a.health, Health::Failed);
    assert!(render::render(&r).contains("children 'kid': 2 bookmark(s), 1 failed"));
    let mut inp = inputs(&auth);
    inp.row = Some("kid");
    let r = assemble(&t, Ok(&s), &inp).await.unwrap();
    assert_eq!(r.rows[0].row, "kid");
}

#[tokio::test]
async fn exactly_once_resume_and_probe_errors() {
    let t = target("", "");
    let (s, store) = stores(&t).await;
    let auth = AuthCatalog::new();
    store
        .put("orders::a", &wrap_state(Some(&json!({"lsn": "0/9"})), 4))
        .await
        .unwrap();
    put_outcome(store.as_ref(), "orders::a", vec![ev(10, None)]).await;
    let r = assemble(&t, Ok(&s), &inputs(&auth)).await.unwrap();
    let eo = r.rows[0].exactly_once.as_ref().unwrap();
    assert_eq!((eo.state_seq, eo.agreement), (4, Agreement::NotProbed));
    assert_eq!(r.rows[0].resume, "lsn=0/9");

    let mut inp = inputs(&auth);
    inp.probe = true;
    let r = assemble(&t, Ok(&s), &inp).await.unwrap();
    let eo = r.rows[0].exactly_once.as_ref().unwrap();
    assert_eq!(eo.agreement, Agreement::NoToken);
    assert!(r.rows[0].resume.contains("sink holds no watermark"));

    let mut broken = t.clone();
    broken.rows[0].sink_kind = "nope".into();
    let r = assemble(&broken, Ok(&s), &inp).await.unwrap();
    let eo = r.rows[0].exactly_once.as_ref().unwrap();
    assert!(eo.probe_error.is_some());
    assert!(render::render(&r).contains("watermark probe failed"));
}

#[tokio::test]
async fn overwrite_failures_flag_possible_staging() {
    let text = "version: 1\nname: o\npipeline:\n  source: { type: csv, config: { path: a } }\n  sink: { type: sqlite, config: { database_url: 'sqlite://x.db', table_name: t, write_mode: overwrite } }\n  state: { type: file, config: { path: ./x } }\n";
    let cfg = PipelineConfig::from_text(text, Path::new("t.yaml")).unwrap();
    let t = PipelineTarget::resolve(&cfg, "o").unwrap();
    let (s, store) = stores(&t).await;
    put_outcome(store.as_ref(), "o::row-0", vec![ev(10, Some("boom"))]).await;
    let auth = AuthCatalog::new();
    let r = assemble(&t, Ok(&s), &inputs(&auth)).await.unwrap();
    assert!(
        r.rows[0]
            .overwrite_staging
            .as_ref()
            .unwrap()
            .contains("t__faucet_ovw")
    );
}

#[tokio::test]
async fn topology_resume_needs_agreeing_sink_bookmarks() {
    let text = r#"version: 1
name: topo
pipeline:
  sources: { src: { type: csv, config: { path: in.csv } } }
  sinks:
    a: { type: stdout, config: {} }
  state: { type: file, config: { path: ./s } }
  nodes:
    read: { kind: source, ref: src }
    fan: { kind: tee, fanout: 2 }
    left: { kind: sink, ref: a }
    right: { kind: sink, ref: a }
  edges:
    - { from: read, to: fan }
    - { from: fan, to: left }
    - { from: fan, to: right }
"#;
    let cfg = PipelineConfig::from_text(text, Path::new("t.yaml")).unwrap();
    let t = PipelineTarget::resolve(&cfg, "topo").unwrap();
    let (s, store) = stores(&t).await;
    let auth = AuthCatalog::new();
    store.put("topo::left", &json!({"id": 1})).await.unwrap();
    store.put("topo::right", &json!({"id": 2})).await.unwrap();
    let r = assemble(&t, Ok(&s), &inputs(&auth)).await.unwrap();
    assert!(r.topology);
    assert!(
        r.rows[0].resume.contains("disagree"),
        "{}",
        r.rows[0].resume
    );
    store.put("topo::right", &json!({"id": 1})).await.unwrap();
    let r = assemble(&t, Ok(&s), &inputs(&auth)).await.unwrap();
    assert_eq!(r.rows[1].resume, "id=1");
    assert!(render::render(&r).contains("2 sink nodes"));
}

#[test]
fn health_codes_and_bookmark_text() {
    assert_eq!(Health::Ok.exit_code(), 0);
    assert_eq!(Health::Running.exit_code(), 0);
    assert_eq!(Health::Warming.exit_code(), 0);
    assert_eq!(Health::Unknown.exit_code(), 1);
    assert_eq!(Health::Degraded.exit_code(), 1);
    assert_eq!(Health::Failed.exit_code(), 2);
    assert!(Health::Failed > Health::Degraded && Health::Degraded > Health::Ok);
    assert_eq!(bookmark_text(&json!({"a": 1, "b": "x"})), "a=1 b=x");
    assert_eq!(bookmark_text(&json!("s")), "s");
    assert_eq!(bookmark_text(&json!(5)), "5");
}
