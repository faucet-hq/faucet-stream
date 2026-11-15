//! The value-fidelity corpus through the feed source: every corpus row is
//! stored as one JSON event and must come out of a real pipeline unchanged.

mod support;

use faucet_common_feed::NewEvent;
use faucet_conformance::fidelity::{self, Tolerance};
use faucet_core::Pipeline;
use faucet_source_feed::FeedSource;
use support::{Fixture, RecordingSink};

#[tokio::test]
async fn the_corpus_is_delivered_exactly() {
    let mut fx = Fixture::empty().await;
    let mut sent = fidelity::corpus();
    sent.push(fidelity::flat_corpus());
    let events = sent
        .iter()
        .map(|row| NewEvent::new("application/json", serde_json::to_vec(row).unwrap()))
        .collect();
    fx.append(events).await;

    let sink = RecordingSink::new();
    let source = FeedSource::new(fx.config().include_metadata(false).page_events(3));
    let result = Pipeline::new(&source, &sink).run().await.unwrap();
    assert_eq!(result.records_written, sent.len());
    fidelity::assert_round_trip(&sent, &sink.rows(), Tolerance::exact());
    assert_eq!(fx.cursor().await, sent.len() as u64 + 1);
}
