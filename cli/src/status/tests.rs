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
        batches: None,
        lag: None,
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
    // A child invocation in flight makes the children `running`.
    let g = crate::pipeline_state::lease::acquire(Arc::clone(&store), "orders::kid::2", "c")
        .await
        .unwrap();
    put_outcome(store.as_ref(), "orders::kid::1", vec![ev(1, None)]).await;
    let r = assemble(&t, Ok(&s), &inputs(&auth)).await.unwrap();
    assert_eq!(r.rows[0].children[0].worst, Health::Running);
    g.release().await;
    let r = assemble(&t, Ok(&s), &inputs(&auth)).await.unwrap();
    assert_eq!(r.rows[0].children[0].worst, Health::Ok);
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
    let sg = r.rows[0].overwrite_staging.as_ref().unwrap();
    assert_eq!((sg.state, sg.verified), ("unknown", false));
    assert_eq!(sg.object, "t__faucet_ovw");
    assert!(sg.note.contains("unverified"));
    assert!(render::render(&r).contains("overwrite staging: unknown"));

    // --probe asks the sink: a fresh database holds no staging.
    let dir = tempfile::tempdir().unwrap();
    let mut probed = t.clone();
    probed.rows[0].sink_config["database_url"] = serde_json::json!(format!(
        "sqlite://{}?mode=rwc",
        dir.path().join("x.db").display()
    ));
    probed.rows[0].sink_config["column_mapping"] = serde_json::json!("auto_map");
    let mut inp = inputs(&auth);
    inp.probe = true;
    let r = assemble(&probed, Ok(&s), &inp).await.unwrap();
    let sg = r.rows[0].overwrite_staging.as_ref().unwrap();
    assert_eq!((sg.state, sg.verified), ("absent", true), "{}", sg.note);
    // A sink that cannot be built, or cannot tell, is unknown.
    let mut broken = probed.clone();
    broken.rows[0].sink_kind = "nope".into();
    let r = assemble(&broken, Ok(&s), &inp).await.unwrap();
    let sg = r.rows[0].overwrite_staging.as_ref().unwrap();
    assert_eq!(sg.state, "unknown");
    assert!(sg.note.contains("probe failed"));
    let mut silent = probed.clone();
    silent.rows[0].sink_kind = "stdout".into();
    silent.rows[0].sink_config = serde_json::json!({});
    let r = assemble(&silent, Ok(&s), &inp).await.unwrap();
    let sg = r.rows[0].overwrite_staging.as_ref().unwrap();
    assert!(sg.note.contains("cannot report"), "{}", sg.note);
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

#[tokio::test]
async fn last_run_batches_and_lag_surface_and_breach_the_lag_sla() {
    let t = target("sla:\n  max_lag_bytes: 1000\n", "");
    let (s, store) = stores(&t).await;
    let auth = AuthCatalog::new();
    let mut e = ev(60, None);
    e.batches = Some(faucet_core::BatchOutcomes {
        attempted: 4,
        committed: 2,
        dlq_partial: 1,
        dlq_all: 1,
        failed: 0,
    });
    e.lag = Some(faucet_core::SourceLag::bytes(4096));
    put_outcome(store.as_ref(), "orders::a", vec![e]).await;
    let r = assemble(&t, Ok(&s), &inputs(&auth)).await.unwrap();
    let row = &r.rows[0];
    assert_eq!(row.batches.unwrap().dlq_all, 1);
    let lag = row.lag.as_ref().unwrap();
    assert_eq!(lag.measured, "last_run");
    assert_eq!(lag.lag.bytes, Some(4096));
    assert_eq!(lag.human, "4 KiB");
    assert_eq!(row.health, Health::Degraded);
    assert!(row.sla.iter().any(|v| v.kind == "lag"), "{:?}", row.sla);
    assert!(
        row.reasons
            .iter()
            .any(|r| r.contains("went to the DLQ whole")),
        "{:?}",
        row.reasons
    );
    let text = render::render(&r);
    assert!(text.contains("4 KiB"), "{text}");
    assert!(text.contains("at the end of the last run"), "{text}");
    assert!(text.contains("1 partly"), "{text}");
    let json = serde_json::to_value(&r).unwrap();
    assert_eq!(json["rows"][0]["lag"]["bytes"], 4096);
    assert_eq!(json["rows"][0]["batches"]["dlq_partial"], 1);
}

#[test]
fn batch_note_names_every_unclean_outcome() {
    let all = faucet_core::BatchOutcomes {
        attempted: 3,
        committed: 3,
        ..Default::default()
    };
    assert_eq!(
        batch_note(&all),
        "last run: of 3 sink write(s), all committed"
    );
    let failed = faucet_core::BatchOutcomes {
        attempted: 2,
        committed: 1,
        failed: 1,
        ..Default::default()
    };
    assert!(batch_note(&failed).ends_with("1 failed"));
}

#[tokio::test]
async fn probe_asks_a_lag_capable_source_and_reports_failure_as_unreadable() {
    let text = r#"version: 1
name: cdc
pipeline:
  source: { type: kafka, config: { brokers: "127.0.0.1:1", topics: [t], group_id: g } }
  sink: { type: stdout, config: {} }
  state: { type: file, config: { path: ./unused-state } }
"#;
    let cfg = PipelineConfig::from_text(text, Path::new("t.yaml")).unwrap();
    let t = PipelineTarget::resolve(&cfg, "cdc").unwrap();
    let (s, store) = stores(&t).await;
    let base = t.base_key(&t.rows[0].id);
    put_outcome(store.as_ref(), &base, vec![ev(10, None)]).await;
    let auth = AuthCatalog::new();
    let mut i = inputs(&auth);
    i.probe = true;
    let r = assemble(&t, Ok(&s), &i).await.unwrap();
    if crate::registry::source_kinds().contains(&"kafka") {
        assert!(
            r.rows[0].errors.iter().any(|e| e.starts_with("lag probe:")),
            "{:?}",
            r.rows[0].errors
        );
    }
}
