//! `on_gap: resnapshot` is a complete read: after it, the destination rows of
//! items it did not see (deleted while their stream records were trimmed) are
//! deleted, and only then is the new position saved.

mod common;

use aws_sdk_dynamodb::types::ScalarAttributeType;
use common::{config, create_table, item, n, s, start};
use faucet_core::observability::Labels;
use faucet_core::stage::{CdcUnwrapSpec, TransformStage};
use faucet_core::state::{MemoryStateStore, StateStore};
use faucet_core::state_version::{StoredState, wrap_versioned};
use faucet_core::{
    CleanupPolicy, FaucetError, Pipeline, SeenKeys, Sink, Source, TransformingSource, async_trait,
};
use faucet_source_dynamodb::{DynamoDbSource, OnGap, ReadMode, StreamBookmark};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

/// A keyed destination: rows by `pk`, `__op: d` deletes, cleanup removes
/// every row whose key was not written.
#[derive(Default)]
struct KeyedSink {
    rows: Mutex<BTreeMap<String, Value>>,
    cleanups: Mutex<usize>,
}

impl KeyedSink {
    fn keys(&self) -> BTreeSet<String> {
        self.rows.lock().unwrap().keys().cloned().collect()
    }
}

#[async_trait]
impl Sink for KeyedSink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        let mut rows = self.rows.lock().unwrap();
        for r in records {
            let pk = r["pk"].as_str().expect("pk").to_string();
            if r["__op"] == "d" {
                rows.remove(&pk);
            } else {
                rows.insert(pk, r.clone());
            }
        }
        Ok(records.len())
    }

    fn supports_cleanup(&self) -> bool {
        true
    }

    async fn cleanup_scope(
        &self,
        scope: &BTreeMap<String, Value>,
        seen: &SeenKeys,
    ) -> Result<u64, FaucetError> {
        assert!(scope.is_empty(), "a complete read covers the whole table");
        let written: BTreeSet<String> = seen
            .keys()
            .iter()
            .map(|k| k.0[0].1.as_str().unwrap().to_string())
            .collect();
        let mut rows = self.rows.lock().unwrap();
        let before = rows.len();
        rows.retain(|k, _| written.contains(k));
        *self.cleanups.lock().unwrap() += 1;
        Ok((before - rows.len()) as u64)
    }
}

fn unwrapped(source: DynamoDbSource) -> TransformingSource {
    TransformingSource::new(
        Box::new(source),
        vec![TransformStage::CdcUnwrap(CdcUnwrapSpec::default())],
        Labels::new("p", "", "r"),
    )
    .expect("stages")
}

async fn run(
    source: &TransformingSource,
    sink: &KeyedSink,
    store: &Arc<dyn StateStore>,
) -> Result<(), FaucetError> {
    let policy = CleanupPolicy::for_complete_read(vec!["pk".into()], 1000)?;
    Pipeline::new(source, sink)
        .with_state_store(Arc::clone(store))
        .with_cleanup(Arc::new(policy))
        .run()
        .await
        .map(drop)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_resnapshot_deletes_rows_whose_items_vanished_in_the_gap() {
    let Some((_c, endpoint, client)) = start().await else {
        return;
    };
    create_table(&client, "orders", ScalarAttributeType::S, false, true).await;
    for i in 0..3 {
        client
            .put_item()
            .table_name("orders")
            .set_item(Some(item(vec![("pk", s(&format!("o{i}"))), ("qty", n(i))])))
            .send()
            .await
            .unwrap();
    }
    let mut cfg = config(&endpoint, "orders");
    cfg.mode = ReadMode::Streams;
    cfg.on_gap = OnGap::Resnapshot;
    cfg.idle_termination_secs = Some(3);
    cfg.shard_concurrency = 1;

    let sink = KeyedSink::default();
    let store: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());

    // An ordinary run streams the three items and never deletes.
    let first = unwrapped(DynamoDbSource::new(cfg.clone()).await.unwrap());
    run(&first, &sink, &store).await.expect("first run");
    let key = first.state_key().expect("state key");
    assert_eq!(sink.keys(), ["o0", "o1", "o2"].map(String::from).into());

    // A row whose item was deleted while its stream record was trimmed: no
    // longer in the table, and no record of it left in the stream.
    sink.rows
        .lock()
        .unwrap()
        .insert("ghost".into(), serde_json::json!({"pk": "ghost"}));
    let second = unwrapped(DynamoDbSource::new(cfg.clone()).await.unwrap());
    run(&second, &sink, &store).await.expect("ordinary resume");
    assert!(
        sink.keys().contains("ghost"),
        "an ordinary run never deletes"
    );
    assert_eq!(*sink.cleanups.lock().unwrap(), 0);

    // Fake the gap: the stored bookmark names a shard the stream no longer has.
    let stored = StoredState::parse(&store.get(&key).await.unwrap().expect("bookmark"));
    let mut gapped = StreamBookmark::from_value(&stored.data);
    gapped.advance("shardId-00000000000000000000-deadbeef", "1");
    let gapped = wrap_versioned(
        stored.owner.as_deref().unwrap(),
        stored.schema,
        &gapped.to_value(),
    );
    store.put(&key, &gapped).await.unwrap();

    let third = unwrapped(DynamoDbSource::new(cfg.clone()).await.unwrap());
    run(&third, &sink, &store).await.expect("resnapshot run");
    assert_eq!(*sink.cleanups.lock().unwrap(), 1);
    assert_eq!(
        sink.keys(),
        ["o0", "o1", "o2"].map(String::from).into(),
        "the ghost row is deleted, live items stay"
    );
    let saved = StoredState::parse(&store.get(&key).await.unwrap().unwrap()).data;
    assert!(
        !StreamBookmark::from_value(&saved)
            .shards
            .contains_key("shardId-00000000000000000000-deadbeef"),
        "the position past the gap is saved once the cleanup succeeded"
    );

    // A resnapshot whose cleanup fails keeps the gapped position, so the next
    // run reads the table again rather than skipping the delete.
    store.put(&key, &gapped).await.unwrap();
    let failing = FailingCleanup(KeyedSink::default());
    let fourth = unwrapped(DynamoDbSource::new(cfg).await.unwrap());
    let policy = CleanupPolicy::for_complete_read(vec!["pk".into()], 1000).unwrap();
    let err = Pipeline::new(&fourth, &failing)
        .with_state_store(Arc::clone(&store))
        .with_cleanup(Arc::new(policy))
        .run()
        .await
        .expect_err("the failed delete fails the run");
    assert!(err.to_string().contains("delete refused"), "{err}");
    assert_eq!(store.get(&key).await.unwrap(), Some(gapped));
}

struct FailingCleanup(KeyedSink);

#[async_trait]
impl Sink for FailingCleanup {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        self.0.write_batch(records).await
    }
    fn supports_cleanup(&self) -> bool {
        true
    }
    async fn cleanup_scope(
        &self,
        _scope: &BTreeMap<String, Value>,
        _seen: &SeenKeys,
    ) -> Result<u64, FaucetError> {
        Err(FaucetError::Sink("delete refused".into()))
    }
}
