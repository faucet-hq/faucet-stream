//! #651 Category F — typed round-trip fidelity for the JSON Lines sink.
//!
//! The strictest fidelity pair in the program, and the one that isolates the
//! *pipeline* from every destination limitation. A JSONL file has no schema, no
//! column types and no affinity rules, so nothing here can be excused as "the
//! destination cannot represent that". Every value must land byte-identical, and
//! the assertion carries no tolerance at all.
//!
//! That makes this the control for the whole category: if a value survives here
//! but not in a typed destination, the loss belongs to that destination (and its
//! test says so via `Tolerance`); if it fails *here*, the loss is faucet's.

use faucet_conformance::fidelity::{self, Tolerance};
use faucet_core::Sink;
use faucet_sink_jsonl::{JsonlSink, JsonlSinkConfig};
use serde_json::Value;
use tempfile::TempDir;

/// Read the sink's output back as one `Value` per line.
async fn read_back(path: &std::path::Path) -> Vec<Value> {
    let text = tokio::fs::read_to_string(path)
        .await
        .expect("the sink must have created its output file");
    text.lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).expect("every emitted line must be valid JSON"))
        .collect()
}

#[tokio::test]
async fn the_shared_corpus_survives_byte_exactly_with_no_tolerance() {
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().join("out.jsonl");
    let sink = JsonlSink::new(JsonlSinkConfig::new(&path));

    let sent = fidelity::corpus();
    let written = sink.write_batch(&sent).await.expect("write");
    sink.flush().await.expect("flush");

    assert_eq!(written, sent.len());
    let landed = read_back(&path).await;
    assert_eq!(landed.len(), sent.len(), "one line per record");

    // No allowances whatsoever. A schemaless destination has no excuse, so any
    // difference here is a defect in the pipeline itself.
    fidelity::assert_round_trip(&sent, &landed, Tolerance::exact());
}

#[tokio::test]
async fn the_wide_flat_record_survives_byte_exactly_too() {
    // The single-wide-row shape, which is what a typed destination receives.
    // Proving it is lossless here is what lets a typed pair attribute a loss to
    // its own column types rather than to the pipeline.
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().join("flat.jsonl");
    let sink = JsonlSink::new(JsonlSinkConfig::new(&path));

    let flat = fidelity::flat_corpus();
    sink.write_batch(std::slice::from_ref(&flat))
        .await
        .expect("write");
    sink.flush().await.expect("flush");

    let landed = read_back(&path).await;
    assert_eq!(landed.len(), 1);
    fidelity::assert_round_trip(std::slice::from_ref(&flat), &landed, Tolerance::exact());
}

#[tokio::test]
async fn pretty_printing_changes_the_framing_but_not_the_values() {
    // `pretty: true` emits multi-line JSON, so the file is no longer one record
    // per line — but the *values* must be untouched. A formatter that quietly
    // re-rendered numbers would show up here.
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().join("pretty.jsonl");
    let sink = JsonlSink::new(JsonlSinkConfig::new(&path).pretty(true));

    let sent = fidelity::corpus();
    sink.write_batch(&sent).await.expect("write");
    sink.flush().await.expect("flush");

    // Parse the whole file as a stream of concatenated JSON values rather than
    // line-by-line, since pretty output spans lines.
    let text = tokio::fs::read_to_string(&path).await.expect("read");
    let landed: Vec<Value> = serde_json::Deserializer::from_str(&text)
        .into_iter::<Value>()
        .collect::<Result<_, _>>()
        .expect("pretty output must still be a valid JSON value stream");

    assert_eq!(landed.len(), sent.len());
    fidelity::assert_round_trip(&sent, &landed, Tolerance::exact());
}

#[tokio::test]
async fn a_second_write_appends_without_disturbing_the_first_batch() {
    // Multi-page fidelity: a later page must not rewrite or reorder an earlier
    // one. A sink that re-serialized its whole buffer per flush could corrupt
    // already-written rows, which a single-batch test never sees.
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().join("two.jsonl");
    let sink = JsonlSink::new(JsonlSinkConfig::new(&path));

    let sent = fidelity::corpus();
    let (first, second) = sent.split_at(3);
    sink.write_batch(first).await.expect("page 1");
    sink.write_batch(second).await.expect("page 2");
    sink.flush().await.expect("flush");

    let landed = read_back(&path).await;
    assert_eq!(landed.len(), sent.len(), "both pages must be present");
    fidelity::assert_round_trip(&sent, &landed, Tolerance::exact());
}

#[tokio::test]
async fn a_corrupted_read_back_is_caught() {
    // Failing-first: without this the four tests above could be asserting
    // nothing if the comparison were vacuous.
    let sent = fidelity::corpus();
    let mut landed = sent.clone();
    landed[2]["unicode"] = Value::String("mangled".into());

    let mismatches = fidelity::diff_round_trip(&sent, &landed, &Tolerance::exact());
    assert_eq!(mismatches.len(), 1, "{mismatches:?}");
    assert_eq!(mismatches[0].field, "unicode");
}
