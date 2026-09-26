//! #736 — every bookmark shape a release of this source wrote still loads.
//!
//! The files under `tests/fixtures/state/` are frozen: what a released faucet
//! stored (bare schema-0 bookmarks from before versioned state, and the
//! schema-1 envelope with and without the exactly-once wrapper). Never edit one
//! to make this pass — add a migration instead.

use faucet_core::idempotency::unwrap_state;
use faucet_core::state_version::resolve_with;
use faucet_source_mongodb_cdc::{Bookmark, STATE_SCHEMA, migrate_state};
use serde_json::{Value, json};

const TOKEN: &str = "8266F1A2B3000000012B022C0100296E5A1004";

fn fixture(name: &str) -> Value {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/state")
        .join(name);
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("golden fixture {} is missing: {e}", path.display()));
    serde_json::from_str(&text).expect("fixture is JSON")
}

fn resolve(name: &str) -> faucet_core::state_version::ResolvedState {
    resolve_with(
        "p::r",
        &fixture(name),
        "mongodb-cdc",
        STATE_SCHEMA,
        migrate_state,
    )
    .unwrap_or_else(|e| panic!("{name} no longer loads: {e}"))
}

#[test]
fn a_schema_zero_bare_bookmark_migrates_to_an_explicit_flag() {
    let resolved = resolve("bookmark-bare.json");
    assert!(resolved.legacy);
    assert_eq!(resolved.migrated_from, Some(0));
    assert_eq!(
        resolved.data,
        json!({ "resume_token": { "_data": TOKEN }, "invalidate": false })
    );
    let inv = resolve("bookmark-invalidate-bare.json");
    assert!(Bookmark::from_value(inv.data).unwrap().invalidate);
}

#[test]
fn schema_one_envelopes_load_as_is() {
    for (name, seq) in [
        ("bookmark-state-format-1.json", 0),
        ("eo-bookmark-state-format-1.json", 7),
    ] {
        let resolved = resolve(name);
        assert_eq!(resolved.migrated_from, None, "{name}");
        let (bookmark, got_seq) = unwrap_state(&resolved.data);
        assert_eq!(got_seq, seq, "{name}");
        let bm = Bookmark::from_value(bookmark.expect("bookmark")).unwrap();
        assert_eq!(bm.resume_token, json!({ "_data": TOKEN }), "{name}");
        assert!(!bm.invalidate, "{name}");
    }
}

#[test]
fn a_newer_schema_or_another_owner_is_refused() {
    let mut newer = fixture("bookmark-state-format-1.json");
    newer["schema"] = Value::from(STATE_SCHEMA + 1);
    assert!(resolve_with("p::r", &newer, "mongodb-cdc", STATE_SCHEMA, migrate_state).is_err());
    let mut foreign = fixture("bookmark-state-format-1.json");
    foreign["owner"] = Value::from("postgres-cdc");
    assert!(resolve_with("p::r", &foreign, "mongodb-cdc", STATE_SCHEMA, migrate_state).is_err());
}
