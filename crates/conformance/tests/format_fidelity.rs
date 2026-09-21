//! Typed round-trip fidelity of the shared file-format layer (#604).
//!
//! `faucet_core::file_format` is the one "connector" every file source and
//! sink shares: what the s3/gcs/azure-blob/sftp **sinks** write is what those
//! **sources** read back. So the pair-level question the `fidelity` harness
//! asks of postgres↔postgres — *which values survive?* — has to be asked of
//! each format once, here, rather than separately in each of the eight
//! connector crates.
//!
//! Every allowance below is a real, named limitation of the format, asserted
//! rather than assumed. That is the point: a format's lossiness is part of its
//! contract, and a change that makes a format *quietly* lossier (a new
//! stringification, a dropped null) turns one of these green tolerances into a
//! red mismatch instead of shipping.

use faucet_conformance::fidelity::{self, Tolerance};
use faucet_core::file_format::{FileFormat, FormatOptions, decode, encode};
use serde_json::Value;

/// Encode the one-wide-row corpus and read it straight back.
async fn round_trip(format: FileFormat) -> (Vec<Value>, Vec<Value>) {
    let sent = vec![fidelity::flat_corpus()];
    let opts = FormatOptions::default();
    let bytes = encode(&sent, format, &opts).expect("encode the corpus");
    let landed = decode(&bytes, format, &opts).await.expect("decode it back");
    (sent, landed)
}

/// Fields whose value is a JSON object or array. Only the JSON formats have a
/// native representation; the tabular ones necessarily flatten or stringify,
/// which the per-format tests assert individually.
const NESTED: [&str; 4] = [
    "nested_object",
    "nested_array",
    "nested_empty_object",
    "nested_empty_array",
];

/// The JSON formats are the reference: **nothing** is allowed to change,
/// including the sign bit of `-0.0` and integers past 2^53. If either of these
/// ever needs a tolerance, the JSON encoder has grown a defect.
#[tokio::test]
async fn the_json_formats_are_exact() {
    for format in [FileFormat::JsonLines, FileFormat::JsonArray] {
        let (sent, landed) = round_trip(format).await;
        assert_eq!(landed.len(), 1, "{format:?} lost the row");
        fidelity::assert_round_trip(&sent, &landed, Tolerance::exact());
    }
}

/// CSV is text-only: every scalar comes back as its rendered string, and a
/// null is indistinguishable from an empty field. Values are preserved —
/// *types* are not, which is why a CSV round trip needs a cast downstream.
#[tokio::test]
async fn csv_preserves_every_value_but_no_type() {
    let (sent, landed) = round_trip(FileFormat::Csv).await;
    let mut tol = Tolerance::exact()
        // Every scalar renders to text and parses back as text.
        .lenient_scalar_kind()
        // A CSV field cannot distinguish "absent" from "empty".
        .empty_string_becomes_null();
    for f in NESTED {
        // Re-serialized as JSON text rather than dropped — asserted below.
        tol = tol.skipping(f);
    }
    // `null` renders to an empty field, which reads back as "" rather than
    // null, so it is the mirror of the allowance above.
    tol = tol.skipping("booleans_and_null_null_val");
    fidelity::assert_round_trip(&sent, &landed, tol);

    let row = &landed[0];
    assert_eq!(
        row["booleans_and_null_null_val"],
        Value::String(String::new()),
        "a null must become an empty field, not the text \"null\""
    );
    // Nested values survive as JSON text — recoverable, unlike being dropped.
    let nested: Value =
        serde_json::from_str(row["nested_object"].as_str().expect("json text")).expect("reparse");
    assert_eq!(nested, sent[0]["nested_object"]);
    // The exact digits of a bigint survive the text round trip.
    assert_eq!(row["integers_i64_max"], Value::String(i64::MAX.to_string()));
}

/// XML is text-only like CSV, but nests: an object stays an object, with its
/// scalars stringified. It also **trims leading and trailing whitespace** —
/// the one place a value is genuinely altered rather than retyped.
#[tokio::test]
async fn xml_nests_but_stringifies_and_trims() {
    let (sent, landed) = round_trip(FileFormat::Xml).await;
    let mut tol = Tolerance::exact()
        .lenient_scalar_kind()
        .empty_string_becomes_null();
    for f in NESTED {
        tol = tol.skipping(f);
    }
    tol = tol
        .skipping("booleans_and_null_null_val")
        // XML text nodes are whitespace-trimmed on read, so padding is lost.
        .skipping("strings_padded");
    fidelity::assert_round_trip(&sent, &landed, tol);

    let row = &landed[0];
    // The trim is real data loss; pin it so it cannot widen silently and so
    // the behaviour is discoverable from the test suite.
    assert_eq!(
        row["strings_padded"],
        Value::String("spaced".into()),
        "XML read trims surrounding whitespace — a padded string does not survive"
    );
    // Structure survives even though the leaf types do not.
    assert_eq!(
        row["nested_object"],
        serde_json::json!({"a": "1", "b": {"c": ["1", "2", "3"]}}),
        "XML keeps the shape and stringifies the leaves"
    );
}

/// xlsx keeps numbers and booleans as *typed* cells — the one tabular format
/// that does — at the cost of the double it stores them in.
#[tokio::test]
async fn xlsx_keeps_scalar_types_within_the_double_it_stores_them_in() {
    let (sent, landed) = round_trip(FileFormat::Xlsx).await;
    let mut tol = Tolerance::exact()
        // Beyond 2^53 an integer has no exact double, so it is written as
        // text (see `exact_f64`); the digits survive, the JSON kind does not.
        .lenient_scalar_kind()
        // An empty cell and an empty string are the same cell in xlsx.
        .empty_string_becomes_null();
    for f in NESTED {
        tol = tol.skipping(f);
    }
    tol = tol
        // IEEE -0.0 has no distinct cell representation.
        .skipping("floats_negative_zero");
    fidelity::assert_round_trip(&sent, &landed, tol);

    let row = &landed[0];
    // The types that *do* survive are what make xlsx worth the complexity.
    assert_eq!(row["integers_zero"], serde_json::json!(0));
    assert_eq!(row["floats_simple"], serde_json::json!(1.5));
    assert_eq!(row["booleans_and_null_true_val"], serde_json::json!(true));
    // A count written as 1 must not come back as 1.0.
    assert!(
        row["integers_negative"].is_i64(),
        "an integral cell must decode back to an integer, not a float: {}",
        row["integers_negative"]
    );
    // The regression this test exists for: past 2^53 the exact digits must
    // survive as text rather than being silently rounded to an even double.
    assert_eq!(
        row["integers_beyond_f64_exact"],
        Value::String("9007199254740993".into()),
        "an integer with no exact double must keep its digits, not round"
    );
    assert_eq!(row["integers_i64_max"], Value::String(i64::MAX.to_string()));
}

/// Whatever each format's tolerances are, no format may **invent or lose a
/// row**, and none may drop a column outright. A tolerance can excuse a
/// changed value; nothing excuses a missing field.
#[tokio::test]
async fn no_format_drops_a_row_or_a_column() {
    let expected: Vec<String> = fidelity::flat_corpus()
        .as_object()
        .expect("object")
        .keys()
        .cloned()
        .collect();
    for format in [
        FileFormat::JsonLines,
        FileFormat::JsonArray,
        FileFormat::Csv,
        FileFormat::Xml,
        FileFormat::Xlsx,
    ] {
        let (_sent, landed) = round_trip(format).await;
        assert_eq!(landed.len(), 1, "{format:?} changed the row count");
        let got = landed[0].as_object().expect("object");
        for key in &expected {
            // The single named exception: XML encodes a list as repeated
            // elements, so an *empty* list is zero elements — indistinguishable
            // from the field being absent. There is no framing that fixes it
            // short of a type attribute, so it is asserted rather than hidden.
            if format == FileFormat::Xml && key == "nested_empty_array" {
                assert!(
                    !got.contains_key(key),
                    "XML gained a representation for an empty array — update \
                     this exception rather than leaving it stale"
                );
                continue;
            }
            assert!(
                got.contains_key(key),
                "{format:?} dropped the column `{key}` entirely"
            );
        }
    }
}

/// Multi-record parity: the formats must agree on how many records they
/// carry, and each record must stay distinguishable. A format that merged or
/// reordered rows would still pass the single-row tests above.
#[tokio::test]
async fn every_format_round_trips_the_multi_record_corpus_row_for_row() {
    let sent = fidelity::corpus();
    for format in [
        FileFormat::JsonLines,
        FileFormat::JsonArray,
        FileFormat::Csv,
        FileFormat::Xml,
        FileFormat::Xlsx,
    ] {
        let opts = FormatOptions::default();
        let bytes = encode(&sent, format, &opts).expect("encode");
        let landed = decode(&bytes, format, &opts).await.expect("decode");
        assert_eq!(
            landed.len(),
            sent.len(),
            "{format:?} changed the record count"
        );
        let ids: Vec<&str> = landed
            .iter()
            .map(|r| r[fidelity::ROW_KEY].as_str().unwrap_or("<missing>"))
            .collect();
        let want: Vec<&str> = sent
            .iter()
            .map(|r| r[fidelity::ROW_KEY].as_str().expect("id"))
            .collect();
        assert_eq!(ids, want, "{format:?} reordered or renamed rows");
    }
}
