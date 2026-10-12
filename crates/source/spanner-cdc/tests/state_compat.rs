//! The persisted bookmark is a public contract: a bookmark written by one
//! release must be readable by the next. These golden shapes must never change
//! without a `STATE_SCHEMA` bump and a migration.

use faucet_source_spanner_cdc::{Bookmark, STATE_SCHEMA, migrate_state};
use serde_json::json;

#[test]
fn schema_version_is_one() {
    assert_eq!(STATE_SCHEMA, 1);
}

#[test]
fn a_v1_bookmark_literal_parses_and_round_trips_byte_for_byte() {
    let stored = json!({
        "watermark": "2026-10-12T08:05:25Z",
        "partitions": [
            {"token": "tokA", "read_from": "2026-10-12T08:05:25Z"},
            {
                "token": "tokM",
                "read_from": "2026-10-12T08:05:20.123456Z",
                "waiting_on": ["tokA", "tokB"]
            }
        ]
    });
    let bm = Bookmark::from_value(stored.clone()).unwrap();
    assert_eq!(bm.partitions.len(), 2);
    assert_eq!(bm.partitions[0].waiting_on, Vec::<String>::new());
    assert_eq!(bm.partitions[1].waiting_on, vec!["tokA", "tokB"]);
    assert_eq!(bm.to_value().unwrap(), stored);
}

#[test]
fn the_unread_root_partition_is_a_null_token() {
    let stored = json!({
        "watermark": "2026-10-12T08:05:25Z",
        "partitions": [{"token": null, "read_from": "2026-10-12T08:05:25Z"}]
    });
    let bm = Bookmark::from_value(stored).unwrap();
    assert_eq!(bm.partitions[0].token, None);
}

#[test]
fn current_schema_migrates_to_itself_and_others_are_refused() {
    let v = json!({"watermark": "2026-10-12T08:05:25Z", "partitions": []});
    assert_eq!(migrate_state(STATE_SCHEMA, v.clone()).unwrap(), v);
    assert!(migrate_state(0, v.clone()).is_err());
    assert!(migrate_state(STATE_SCHEMA + 1, v).is_err());
}
