//! #651 Category G1 — state-format compatibility.
//!
//! A resumable pipeline's bookmark outlives the process that wrote it, so every
//! upgrade crosses the state-format boundary. If release N+1 cannot read what N
//! wrote, the pipeline resumes from the wrong place — re-delivering everything
//! or losing everything — and **neither failure is loud**. The run is green
//! either way.
//!
//! A round-trip test cannot catch this, because it serializes and deserializes
//! with the same code: both halves move together and the test stays green
//! through a breaking change. So these tests read **frozen files** written by a
//! released version (`tests/fixtures/state/`) and assert the current code still
//! understands them.
//!
//! If one of these fails, the fixture is not what is wrong.

use faucet_core::Value;
use faucet_core::idempotency::{
    format_token, format_token_with_bookmark, parse_token, parse_token_parts, unwrap_state,
    wrap_state,
};

/// Load a frozen fixture. A missing file is a failure, not a skip — a silently
/// skipped compatibility gate is the same as not having one.
fn fixture(name: &str) -> Value {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/state")
        .join(name);
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!(
            "golden fixture {} is missing or unreadable: {e}",
            path.display()
        )
    });
    serde_json::from_str(&text)
        .unwrap_or_else(|e| panic!("golden fixture {} is not valid JSON: {e}", path.display()))
}

#[test]
fn an_exactly_once_envelope_written_by_a_release_still_unwraps() {
    let stored = fixture("eo-envelope-v1.json");
    let (bookmark, seq) = unwrap_state(&stored);

    assert_eq!(
        seq, 42,
        "the committed sequence must survive the upgrade — it is what decides which \
         pages are already durable"
    );
    let bookmark = bookmark.expect("the envelope carries a bookmark");
    assert_eq!(bookmark["page"], 7);
    assert_eq!(bookmark["updated_at"], "2026-05-01T00:00:00Z");
}

#[test]
fn an_envelope_with_no_bookmark_yet_unwraps_to_none_not_to_a_null_bookmark() {
    // The distinction matters: `Some(Value::Null)` would be handed to
    // `apply_start_bookmark` as if it were a real position.
    let (bookmark, seq) = unwrap_state(&fixture("eo-envelope-null-bookmark-v1.json"));
    assert_eq!(seq, 0);
    assert!(
        bookmark.is_none(),
        "a null bookmark must unwrap to None, got {bookmark:?}"
    );
}

#[test]
fn a_bare_at_least_once_bookmark_is_still_read_as_a_bookmark() {
    // Every pipeline that predates exactly-once wrote a bare bookmark. Reading
    // it as an envelope (and so as `None`) would restart it from scratch.
    let stored = fixture("bare-bookmark-v1.json");
    let (bookmark, seq) = unwrap_state(&stored);
    assert_eq!(seq, 0, "a bare bookmark carries no sequence");
    let bookmark = bookmark.expect("a bare bookmark must be returned as the bookmark");
    assert_eq!(bookmark["updated_at"], "2026-05-01T00:00:00Z");
    assert_eq!(
        bookmark["id"].as_i64(),
        Some(9_007_199_254_740_993),
        "an id past 2^53 must survive exactly — losing a digit silently re-reads or \
         skips rows"
    );
}

#[test]
fn a_scalar_bookmark_survives_unchanged() {
    let stored = fixture("bare-bookmark-scalar-v1.json");
    let (bookmark, seq) = unwrap_state(&stored);
    assert_eq!(seq, 0);
    assert_eq!(bookmark, Some(Value::String("2026-05-01T00:00:00Z".into())));
}

#[test]
fn commit_tokens_written_by_a_release_still_parse() {
    let tokens = fixture("commit-tokens-v1.json");

    assert_eq!(
        parse_token(tokens["bare"].as_str().expect("string")),
        Some(42),
        "a stored bare token must still yield its sequence"
    );
    assert_eq!(
        parse_token(tokens["zero"].as_str().expect("string")),
        Some(0)
    );
    assert_eq!(
        parse_token(tokens["max"].as_str().expect("string")),
        Some(u64::MAX),
        "the widest sequence a u64 can hold must round-trip"
    );

    let (seq, bookmark) = parse_token_parts(tokens["with_bookmark"].as_str().expect("string"))
        .expect("a bookmark-carrying token must parse");
    assert_eq!(seq, 42);
    assert_eq!(
        bookmark.expect("the token carries a bookmark")["page"],
        7,
        "the embedded bookmark is what re-anchors the source on resume"
    );
}

#[test]
fn the_token_encoding_is_still_fixed_width_and_lexicographically_ordered() {
    // Sinks compare tokens as strings (a SQL `MAX(token)`, a compacted Kafka
    // key), so the encoding must sort the same way the numbers do. A change to
    // the width would silently invert that ordering for existing data.
    let stored = fixture("commit-tokens-v1.json");
    let width = stored["bare"].as_str().expect("string").len();
    assert_eq!(
        format_token(42).len(),
        width,
        "the token width changed — every stored token now sorts against the new ones \
         incorrectly"
    );
    assert_eq!(format_token(42), stored["bare"].as_str().expect("string"));
    assert_eq!(format_token(0), stored["zero"].as_str().expect("string"));
    assert_eq!(
        format_token(u64::MAX),
        stored["max"].as_str().expect("string")
    );

    // The ordering property itself.
    let mut rendered: Vec<String> = [0u64, 1, 9, 10, 99, 100, u64::MAX]
        .iter()
        .map(|n| format_token(*n))
        .collect();
    let numeric_order = rendered.clone();
    rendered.sort();
    assert_eq!(
        rendered, numeric_order,
        "fixed-width tokens must sort lexicographically in numeric order"
    );
}

#[test]
fn the_bookmark_carrying_token_separator_is_unchanged() {
    let stored = fixture("commit-tokens-v1.json");
    let expected = stored["with_bookmark"].as_str().expect("string");
    let rendered = format_token_with_bookmark(42, Some(&serde_json::json!({ "page": 7 })));
    assert_eq!(
        rendered, expected,
        "the separator or the embedded-bookmark rendering changed, so tokens written by \
         a release can no longer be parsed back into a resume position"
    );
}

#[test]
fn garbage_is_rejected_rather_than_silently_read_as_zero() {
    // Reading an unparseable token as sequence 0 would make the engine believe
    // nothing had committed and replay the entire stream.
    for bad in [
        "",
        "abc",
        "#",
        "#{}",
        "not-a-number#{\"page\":1}",
        "4 2",
        "42x",
    ] {
        assert_eq!(
            parse_token(bad),
            None,
            "{bad:?} must not parse as a valid sequence"
        );
    }
}

#[test]
fn a_whitespace_padded_token_still_parses_because_char_columns_pad() {
    // Deliberate, and load-bearing: the watermark table's token column may be
    // a fixed-width `CHAR(20)`, which pads on read. Rejecting the padded form
    // would make every stored watermark on such a column unreadable — the
    // engine would see "nothing committed" and replay the whole stream.
    //
    // Pinned here so the leniency is understood as a compatibility requirement
    // rather than mistaken for sloppiness and "tidied up" later.
    assert_eq!(parse_token("  00000000000000000042  "), Some(42));
    assert_eq!(parse_token(" 42"), Some(42));

    let (seq, bookmark) = parse_token_parts(" 00000000000000000042 #{\"page\":7}")
        .expect("a padded bookmark-carrying token must parse too");
    assert_eq!(seq, 42);
    assert_eq!(bookmark.expect("bookmark")["page"], 7);
}

#[test]
fn a_current_envelope_still_matches_the_frozen_shape() {
    // The other direction: what today's code *writes* must still look like what
    // the fixture recorded, so a downgrade (or a peer instance still on the old
    // release, which is the normal state during a rolling deploy) can read it.
    let written = wrap_state(
        Some(&serde_json::json!({ "page": 7, "updated_at": "2026-05-01T00:00:00Z" })),
        42,
    );
    let frozen = fixture("eo-envelope-v1.json");
    assert_eq!(
        written, frozen,
        "the envelope this release writes no longer matches the frozen shape — a peer \
         instance on the previous release would misread it mid-deploy"
    );
}

#[test]
fn an_envelope_from_the_future_degrades_to_a_bare_bookmark_rather_than_panicking() {
    // Forward compatibility: a newer release may add fields. An older reader
    // must not panic on them, and must not mistake an unknown marker version
    // for a bookmark it understands.
    let future = serde_json::json!({
        "__faucet_eo": 2,
        "bookmark": { "page": 7 },
        "seq": 42,
        "something_new": true
    });
    let (bookmark, seq) = unwrap_state(&future);
    // Marker 2 is not recognised, so the whole object is treated as a bare
    // bookmark. That is conservative — it replays rather than skipping.
    assert_eq!(seq, 0);
    assert!(
        bookmark.is_some(),
        "an unrecognised envelope must still yield something to resume from"
    );

    // And an envelope with the right marker but extra fields is read normally.
    let extended = serde_json::json!({
        "__faucet_eo": 1,
        "bookmark": { "page": 7 },
        "seq": 42,
        "added_in_a_later_release": "ignored"
    });
    let (bookmark, seq) = unwrap_state(&extended);
    assert_eq!(seq, 42, "unknown sibling fields must not break the read");
    assert_eq!(bookmark.expect("bookmark")["page"], 7);
}
