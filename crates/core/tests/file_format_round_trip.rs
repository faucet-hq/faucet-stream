//! Every file format round-trips through the shared encode/decode pair (#604).
//!
//! The point of `faucet_core::file_format` is that what one file connector
//! writes, another can read — so the contract worth pinning is not each
//! parser in isolation but the *pair*, exercised through the same public
//! entry points the connectors call.
#![cfg(feature = "file-formats")]

use faucet_core::file_format::{FileFormat, FormatOptions, decode, encode};
use serde_json::{Value, json};

/// Records whose values are all strings, so a format that cannot carry types
/// (CSV, XML) round-trips exactly rather than approximately.
fn text_records() -> Vec<Value> {
    vec![
        json!({"id": "1", "name": "ada", "team": "core"}),
        json!({"id": "2", "name": "grace", "team": "compilers"}),
    ]
}

async fn round_trip(format: FileFormat, records: &[Value], opts: &FormatOptions) -> Vec<Value> {
    let bytes = encode(records, format, opts).expect("encode");
    decode(&bytes, format, opts).await.expect("decode")
}

#[tokio::test]
async fn text_records_survive_every_format() {
    let opts = FormatOptions::default();
    let records = text_records();
    for format in [
        FileFormat::JsonLines,
        FileFormat::JsonArray,
        FileFormat::Csv,
        FileFormat::Xml,
        FileFormat::Xlsx,
    ] {
        assert_eq!(
            round_trip(format, &records, &opts).await,
            records,
            "{format:?} did not round-trip"
        );
    }
}

/// JSON Lines, JSON array and Excel carry types; CSV and XML are text formats
/// and are documented as such. Stating which is which here means a change to
/// either group is a test failure rather than a surprise in production.
#[tokio::test]
async fn typed_formats_keep_types_and_text_formats_stringify() {
    let opts = FormatOptions::default();
    let typed = vec![json!({"n": 42, "ok": true, "s": "x"})];

    for format in [
        FileFormat::JsonLines,
        FileFormat::JsonArray,
        FileFormat::Xlsx,
    ] {
        assert_eq!(
            round_trip(format, &typed, &opts).await,
            typed,
            "{format:?} must preserve numbers and booleans"
        );
    }

    for format in [FileFormat::Csv, FileFormat::Xml] {
        assert_eq!(
            round_trip(format, &typed, &opts).await,
            vec![json!({"n": "42", "ok": "true", "s": "x"})],
            "{format:?} is a text format: values come back as strings"
        );
    }
}

/// An empty page must produce an object that reads back as zero records, not
/// an error and not a malformed file.
#[tokio::test]
async fn an_empty_page_round_trips_to_nothing() {
    let opts = FormatOptions::default();
    for format in [
        FileFormat::JsonLines,
        FileFormat::JsonArray,
        FileFormat::Csv,
        FileFormat::Xml,
        FileFormat::Xlsx,
    ] {
        let out = round_trip(format, &[], &opts).await;
        assert!(out.is_empty(), "{format:?} produced {out:?}");
    }
}

/// A record that gains a field mid-page widens the file instead of losing the
/// field — the silent-data-loss failure the header union exists to prevent.
#[tokio::test]
async fn a_late_field_widens_the_tabular_formats() {
    let opts = FormatOptions::default();
    let records = vec![json!({"a": "1"}), json!({"a": "2", "b": "3"})];
    for format in [FileFormat::Csv, FileFormat::Xlsx] {
        let back = round_trip(format, &records, &opts).await;
        assert_eq!(back.len(), 2, "{format:?}");
        assert!(
            back[1].get("b").is_some(),
            "{format:?} lost a field only the second record had: {back:?}"
        );
    }
}

/// Parquet is columnar and belongs to each connector's Arrow path; routing it
/// through the generic helper would work and silently cost that path.
#[tokio::test]
async fn parquet_is_refused_by_the_generic_helper() {
    let opts = FormatOptions::default();
    assert!(encode(&text_records(), FileFormat::Parquet, &opts).is_err());
    assert!(decode(b"", FileFormat::Parquet, &opts).await.is_err());
}
