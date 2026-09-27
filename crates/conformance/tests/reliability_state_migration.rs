//! #735 — moving state between backends must not move the resume point.
//!
//! `faucet state export` → `faucet state import` onto a *different* backend is
//! the supported way to migrate state (file → Postgres, restore after losing a
//! volume). The guarantee: the next run on the target backend resumes exactly
//! where the last run on the source backend left off — nothing re-read, nothing
//! skipped — in both at-least-once and exactly-once delivery.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use faucet_conformance::scripted::{Boundary, EventLog, PagedSource, ScriptedSink};
use faucet_core::idempotency::DeliveryMode;
use faucet_core::state::{export_namespace, import_namespace};
use faucet_core::{
    FaucetError, FileStateStore, MemoryStateStore, Pipeline, Sink, Source, StateStore, StreamPage,
    Value, async_trait,
};

/// `PagedSource` under a pipeline-namespaced key, the way the CLI keys a row.
struct Keyed<'a>(&'a PagedSource);

#[async_trait]
impl Source for Keyed<'_> {
    async fn fetch_with_context(
        &self,
        ctx: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        self.0.fetch_with_context(ctx).await
    }
    fn stream_pages<'a>(
        &'a self,
        ctx: &'a HashMap<String, Value>,
        batch_size: usize,
    ) -> Pin<Box<dyn futures_core::Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>>
    {
        self.0.stream_pages(ctx, batch_size)
    }
    fn state_key(&self) -> Option<String> {
        Some("orders::row".into())
    }
    async fn apply_start_bookmark(&self, bookmark: Value) -> Result<(), FaucetError> {
        self.0.apply_start_bookmark(bookmark).await
    }
    fn supports_exactly_once(&self) -> bool {
        true
    }
    fn config_schema(&self) -> Value {
        self.0.config_schema()
    }
}

async fn migrate(from: &dyn StateStore, to: &dyn StateStore) {
    let export = export_namespace(from, "orders").await.expect("export");
    assert!(!export.keys.is_empty(), "the first run left a bookmark");
    let report = import_namespace(to, &export, true).await.expect("import");
    assert!(report.error.is_none(), "{report:?}");
}

#[tokio::test]
async fn at_least_once_resume_point_survives_a_file_to_memory_migration() {
    let dir = tempfile::tempdir().unwrap();
    let file: Arc<dyn StateStore> = Arc::new(FileStateStore::new(dir.path()));
    let log = EventLog::new();
    let sink = ScriptedSink::new(log.clone());

    let first = PagedSource::new(5, 4).failing_after(3);
    let _ = Pipeline::new(&Keyed(&first), &sink)
        .with_state_store(file.clone())
        .run()
        .await;
    assert_eq!(first.emitted_pages(), vec![0, 1, 2]);

    let memory: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
    migrate(file.as_ref(), memory.as_ref()).await;

    let second = PagedSource::new(5, 4);
    Pipeline::new(&Keyed(&second), &sink)
        .with_state_store(memory)
        .run()
        .await
        .expect("resumed run");
    assert_eq!(
        second.emitted_pages(),
        vec![3, 4],
        "the migrated bookmark must resume after the last confirmed page"
    );
    assert_eq!(log.records_written(), 20, "no page lost or written twice");
}

#[tokio::test]
async fn exactly_once_envelope_survives_a_memory_to_file_migration() {
    let log = EventLog::new();
    let sink = ScriptedSink::new(log.clone()).idempotent();
    let memory: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());

    let first = PagedSource::new(5, 4).failing_after(3);
    let _ = Pipeline::new(&Keyed(&first), &sink)
        .with_state_store(memory.clone())
        .with_delivery(DeliveryMode::ExactlyOnce)
        .run()
        .await;
    let token_before = sink.last_committed_token("orders::row").await.unwrap();
    assert!(token_before.is_some());

    let dir = tempfile::tempdir().unwrap();
    let file: Arc<dyn StateStore> = Arc::new(FileStateStore::new(dir.path()));
    migrate(memory.as_ref(), file.as_ref()).await;
    let (_, seq) = faucet_core::idempotency::unwrap_state(
        &file.get("orders::row").await.unwrap().expect("envelope"),
    );
    assert_eq!(seq, 3, "the committed sequence travels with the envelope");

    sink.rearm(Boundary::Never);
    let second = PagedSource::new(5, 4);
    Pipeline::new(&Keyed(&second), &sink)
        .with_state_store(file)
        .with_delivery(DeliveryMode::ExactlyOnce)
        .run()
        .await
        .expect("resumed run");
    assert_eq!(second.emitted_pages(), vec![3, 4]);
    let committed: usize = log
        .events()
        .iter()
        .filter_map(|e| match e {
            faucet_conformance::scripted::Event::IdempotentWrite { records, .. } => Some(*records),
            _ => None,
        })
        .sum();
    assert_eq!(committed, 20, "every page committed exactly once");
}

// ── #736: versioned state across releases ───────────────────────────────────

/// `PagedSource` at bookmark schema 1, whose release 0 stored `{"old_page": n}`.
struct Upgraded<'a>(&'a PagedSource);

#[async_trait]
impl Source for Upgraded<'_> {
    async fn fetch_with_context(
        &self,
        ctx: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        self.0.fetch_with_context(ctx).await
    }
    fn stream_pages<'a>(
        &'a self,
        ctx: &'a HashMap<String, Value>,
        batch_size: usize,
    ) -> Pin<Box<dyn futures_core::Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>>
    {
        self.0.stream_pages(ctx, batch_size)
    }
    fn state_key(&self) -> Option<String> {
        Some("orders::row".into())
    }
    async fn apply_start_bookmark(&self, bookmark: Value) -> Result<(), FaucetError> {
        assert!(
            bookmark.get("old_page").is_none(),
            "the source must only ever see its current bookmark shape: {bookmark}"
        );
        self.0.apply_start_bookmark(bookmark).await
    }
    fn supports_exactly_once(&self) -> bool {
        true
    }
    fn config_schema(&self) -> Value {
        self.0.config_schema()
    }
    fn connector_name(&self) -> &'static str {
        "paged"
    }
    fn state_schema(&self) -> u32 {
        1
    }
    fn migrate_state(&self, from: u32, data: Value) -> Result<Value, FaucetError> {
        match from {
            0 => Ok(serde_json::json!({ "page": data["old_page"].clone() })),
            _ => Err(FaucetError::State(format!("no migration from {from}"))),
        }
    }
}

#[tokio::test]
async fn a_bookmark_written_by_an_older_release_migrates_and_resumes_exactly() {
    let store: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
    // What release 0 stored: a bare value in its old shape.
    store
        .put("orders::row", &serde_json::json!({ "old_page": 2 }))
        .await
        .unwrap();
    let log = EventLog::new();
    let sink = ScriptedSink::new(log.clone());
    let source = PagedSource::new(5, 4);
    Pipeline::new(&Upgraded(&source), &sink)
        .with_state_store(store.clone())
        .run()
        .await
        .expect("the migrated bookmark resumes");
    assert_eq!(source.emitted_pages(), vec![3, 4], "no gap, no re-read");
    assert_eq!(log.records_written(), 8);
    assert_eq!(
        store.get("orders::row").await.unwrap(),
        Some(faucet_core::state_version::wrap_versioned(
            "paged",
            1,
            &serde_json::json!({ "page": 4 })
        )),
        "the next bookmark is written in the envelope at the current schema"
    );
}

#[tokio::test]
async fn an_exactly_once_envelope_from_an_older_release_migrates_its_bookmark_only() {
    let store: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
    store
        .put(
            "orders::row",
            &faucet_core::idempotency::wrap_state(Some(&serde_json::json!({ "old_page": 1 })), 2),
        )
        .await
        .unwrap();
    let sink = ScriptedSink::new(EventLog::new()).idempotent();
    let source = PagedSource::new(4, 2);
    Pipeline::new(&Upgraded(&source), &sink)
        .with_state_store(store.clone())
        .with_delivery(DeliveryMode::ExactlyOnce)
        .run()
        .await
        .expect("resumes");
    assert_eq!(source.emitted_pages(), vec![2, 3]);
    let stored = store.get("orders::row").await.unwrap().unwrap();
    let (bookmark, seq) = faucet_core::idempotency::unwrap_state(&stored);
    assert_eq!(bookmark, Some(serde_json::json!({ "page": 3 })));
    assert_eq!(
        seq, 4,
        "the committed sequence carries on from the migrated envelope"
    );
}

#[tokio::test]
async fn state_from_a_newer_release_or_another_source_is_refused_before_reading() {
    for stored in [
        faucet_core::state_version::wrap_versioned("paged", 2, &serde_json::json!({ "page": 1 })),
        faucet_core::state_version::wrap_versioned("kafka", 1, &serde_json::json!({ "page": 1 })),
        serde_json::json!({ "faucet_state": 99, "owner": "paged", "schema": 1, "data": {} }),
    ] {
        let store: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
        store.put("orders::row", &stored).await.unwrap();
        let log = EventLog::new();
        let sink = ScriptedSink::new(log.clone());
        let source = PagedSource::new(3, 2);
        let err = Pipeline::new(&Upgraded(&source), &sink)
            .with_state_store(store.clone())
            .run()
            .await
            .expect_err("unreadable state is refused");
        assert!(
            matches!(err, FaucetError::StateIncompatible { .. }),
            "{err:?}"
        );
        assert!(
            source.emitted_pages().is_empty(),
            "nothing read from the source"
        );
        assert!(log.events().is_empty(), "nothing written");
        assert_eq!(
            store.get("orders::row").await.unwrap(),
            Some(stored),
            "the stored value is left untouched"
        );
    }
}

#[tokio::test]
async fn legacy_writes_for_a_mixed_version_cluster_store_the_bare_bookmark() {
    let store: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
    let source = PagedSource::new(2, 1);
    Pipeline::new(&Keyed(&source), &ScriptedSink::new(EventLog::new()))
        .with_state_store(store.clone())
        .with_legacy_state_writes(true)
        .run()
        .await
        .expect("runs");
    assert_eq!(
        store.get("orders::row").await.unwrap(),
        Some(serde_json::json!({ "page": 1 })),
        "an older cluster member reads the bare value"
    );
    let upgraded = PagedSource::new(2, 1);
    let err = Pipeline::new(&Upgraded(&upgraded), &ScriptedSink::new(EventLog::new()))
        .with_state_store(Arc::new(MemoryStateStore::new()))
        .with_legacy_state_writes(true)
        .run()
        .await
        .expect_err("a schema-1 source cannot write for an older reader");
    assert!(
        matches!(err, FaucetError::StateIncompatible { .. }),
        "{err:?}"
    );
}
