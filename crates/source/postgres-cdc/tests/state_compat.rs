//! #736 — every bookmark shape a release of this source wrote still loads.
//!
//! The files under `tests/fixtures/state/` are frozen: what a released faucet
//! stored (a bare pre-envelope bookmark, the versioned envelope, and the
//! exactly-once wrapper inside it). Never edit one to make this pass — add a
//! migration instead.

use faucet_core::idempotency::unwrap_state;
use faucet_core::state_version::resolve_with;
use serde_json::Value;

fn fixture(name: &str) -> Value {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/state")
        .join(name);
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("golden fixture {} is missing: {e}", path.display()));
    serde_json::from_str(&text).expect("fixture is JSON")
}

#[test]
fn every_released_bookmark_shape_still_loads() {
    for (name, seq) in [
        ("bookmark-bare.json", 0),
        ("bookmark-state-format-1.json", 0),
        ("eo-bookmark-state-format-1.json", 7),
    ] {
        let resolved = resolve_with("p::r", &fixture(name), "postgres-cdc", 0, |from, _| {
            panic!("{name}: schema 0 needs no migration from {from}")
        })
        .unwrap_or_else(|e| panic!("{name} no longer loads: {e}"));
        let (bookmark, got_seq) = unwrap_state(&resolved.data);
        assert_eq!(got_seq, seq, "{name}");
        let bm = bookmark.unwrap_or_else(|| panic!("{name} lost its bookmark"));
        assert_eq!(
            faucet_source_postgres_cdc::Bookmark::from_value(bm)
                .unwrap()
                .last_lsn,
            "0/16B3748",
            "{name}"
        );
    }
}

#[test]
fn a_bookmark_from_another_connector_or_a_newer_release_is_refused() {
    let mut foreign = fixture("bookmark-state-format-1.json");
    foreign["owner"] = Value::from("someone-else");
    assert!(resolve_with("p::r", &foreign, "postgres-cdc", 0, |_, d| Ok(d)).is_err());
    let mut newer = fixture("bookmark-state-format-1.json");
    newer["schema"] = Value::from(1);
    assert!(resolve_with("p::r", &newer, "postgres-cdc", 0, |_, d| Ok(d)).is_err());
}
