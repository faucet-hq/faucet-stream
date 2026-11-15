//! The value-fidelity corpus (#885) through the feed source: each corpus row
//! is stored as one JSON event, a real pipeline delivers the feed into the
//! file sink, and the file source reads it back unchanged. The feed source
//! refuses a buffered fetch, so the corpus runs through `Pipeline::run`.

use faucet_common_feed::{
    DeliveryUid, FeedStore, FeedUid, NewEvent, SqliteFeedStore, SqliteOptions, Start, StoreLimits,
};
use faucet_conformance::fidelity::{self, Tolerance};
use faucet_core::io::CallContext;
use faucet_core::{Pipeline, Source};
use faucet_source_feed::{FeedSource, FeedSourceConfig, FeedStoreSpec};
use serde_json::json;

#[tokio::test]
async fn the_feed_source_delivers_the_corpus_exactly() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("feeds.db").to_string_lossy().into_owned();
    let calls = CallContext::new();
    let store = SqliteFeedStore::open(&db, &SqliteOptions::new())
        .await
        .expect("open the feed store");
    let feed = FeedUid::new("corpus").unwrap();
    let delivery = DeliveryUid::new(feed.clone(), "files").unwrap();
    store
        .ensure_feed(&calls, &feed, None, &StoreLimits::new())
        .await
        .unwrap();
    store
        .ensure_delivery(&calls, &delivery, Start::Earliest)
        .await
        .unwrap();
    let mut sent = fidelity::corpus();
    sent.push(fidelity::flat_corpus());
    let events = sent
        .iter()
        .map(|row| NewEvent::new("application/json", serde_json::to_vec(row).unwrap()))
        .collect();
    store.append(&calls, &feed, events).await.unwrap();

    let out = dir
        .path()
        .join("corpus.jsonl")
        .to_string_lossy()
        .into_owned();
    let sink =
        faucet_sink_file::FileSink::new(serde_json::from_value(json!({"path": out})).unwrap())
            .unwrap();
    let source = FeedSource::new(
        FeedSourceConfig::standalone("corpus", "files", FeedStoreSpec::sqlite(db))
            .include_metadata(false),
    );
    let result = Pipeline::new(&source, &sink).run().await.unwrap();
    assert_eq!(result.records_written, sent.len());

    let landed =
        faucet_source_file::FileSource::new(serde_json::from_value(json!({"path": out})).unwrap())
            .unwrap()
            .fetch_all()
            .await
            .unwrap();
    fidelity::assert_round_trip(&sent, &landed, Tolerance::exact());

    let heads = store
        .heads(&calls, std::slice::from_ref(&feed))
        .await
        .unwrap();
    assert_eq!(heads[0].deliveries[0].cursor.get(), sent.len() as u64 + 1);
}
