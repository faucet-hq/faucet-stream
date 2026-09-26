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
