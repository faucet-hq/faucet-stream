//! Every DynamoDB Streams bookmark shape a release wrote still loads. The
//! files under `tests/fixtures/state/` are frozen — never edit one to make
//! this pass.

use faucet_source_dynamodb::StreamBookmark;
use serde_json::Value;

fn fixture(name: &str) -> Value {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/state")
        .join(name);
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("golden fixture {} is missing: {e}", path.display()));
    serde_json::from_str(&text).expect("fixture is JSON")
}

const EMPTY: &str = "shardId-00000000000000000001-aaaaaaaa";
const READ: &str = "shardId-00000000000000000002-bbbbbbbb";

#[test]
fn a_bookmark_without_verified_at_still_loads() {
    let bm = StreamBookmark::from_value(&fixture("stream-bookmark-legacy.json"));
    assert!(bm.stream_arn.is_some());
    assert_eq!(bm.shards[EMPTY], "");
    assert!(bm.shards[READ].starts_with("4960"));
    assert_eq!(bm.finished.len(), 1);
    assert!(bm.verified_at.is_empty());
    assert_eq!(StreamBookmark::from_value(&bm.to_value()), bm);
}

#[test]
fn a_bookmark_with_verified_at_round_trips() {
    let raw = fixture("stream-bookmark-verified.json");
    let bm = StreamBookmark::from_value(&raw);
    assert_eq!(bm.verified_at[EMPTY], 1_767_225_600);
    assert_eq!(bm.to_value(), raw);
}
