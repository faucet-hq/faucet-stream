//! #651 Category F — typed round-trip fidelity for the MongoDB sink.
//!
//! MongoDB stores BSON, not JSON, and the conversion is where fidelity is won
//! or lost. BSON has *more* numeric types than JSON (Int32, Int64, Double,
//! Decimal128), so a JSON number has to be assigned one — and the wrong choice
//! is silent:
//!
//! - an `i64` past 2^53 assigned `Double` loses digits;
//! - a whole-valued float assigned `Int32`/`Int64` loses its floatness;
//! - `-0.0` assigned through a decimal type loses its sign.
//!
//! It is also the one destination with **structural** restrictions rather than
//! only type ones: a document is a map, so key order is not preserved, and
//! historically keys containing `.` or starting with `$` were rejected outright.
//!
//! Requires Docker. The Docker-free control is
//! `crates/sink/jsonl/tests/fidelity.rs`.

use faucet_conformance::fidelity::{self, Tolerance};
use faucet_core::Sink;
use faucet_sink_mongodb::{MongoSink, MongoSinkConfig};
use mongodb::Client;
use mongodb::bson::{Document, doc};
use serde_json::{Value, json};
use testcontainers::{ContainerAsync, runners::AsyncRunner};
use testcontainers_modules::mongo::Mongo;

async fn start_mongo() -> (ContainerAsync<Mongo>, String) {
    let container: ContainerAsync<Mongo> = Mongo::default()
        .start()
        .await
        .expect("mongo container start");
    let port = container
        .get_host_port_ipv4(27017)
        .await
        .expect("mongo port");
    (container, format!("mongodb://127.0.0.1:{port}"))
}

/// Read a collection back as JSON, dropping Mongo's own `_id` (which the sink
/// did not send and which is not part of the corpus).
async fn read_back(uri: &str, collection: &str) -> Vec<Value> {
    use futures::TryStreamExt;
    let client = Client::with_uri_str(uri).await.expect("client");
    let coll = client.database("testdb").collection::<Document>(collection);
    let docs: Vec<Document> = coll
        .find(doc! {})
        .await
        .expect("find")
        .try_collect()
        .await
        .expect("collect");
    client.shutdown().await;

    docs.into_iter()
        .map(|mut d| {
            d.remove("_id");
            // `into` on a BSON Document yields the canonical JSON mapping,
            // which is what a downstream consumer of this collection sees.
            let v: Value = mongodb::bson::Bson::Document(d).into_canonical_extjson();
            v
        })
        .collect()
}

#[tokio::test]
async fn the_shared_corpus_survives_the_bson_round_trip() {
    let (_c, uri) = start_mongo().await;
    let sink = MongoSink::new(MongoSinkConfig::new(&uri, "testdb", "corpus"))
        .await
        .expect("sink");

    let sent = fidelity::corpus();
    sink.write_batch(&sent).await.expect("write the corpus");
    sink.flush().await.expect("flush");

    let landed = read_relaxed(&uri, "corpus").await;
    assert_eq!(landed.len(), sent.len(), "every corpus row must land");

    // No tolerance: BSON can represent every hazard class in the corpus, so
    // anything lost here is the conversion's fault, not the store's.
    fidelity::assert_round_trip(&sent, &landed, Tolerance::exact());
}

/// The relaxed (rather than canonical) extended-JSON mapping, which renders a
/// BSON Int64/Double back as a plain JSON number — the shape a consumer reading
/// this collection as JSON actually gets.
async fn read_relaxed(uri: &str, collection: &str) -> Vec<Value> {
    use futures::TryStreamExt;
    let client = Client::with_uri_str(uri).await.expect("client");
    let coll = client.database("testdb").collection::<Document>(collection);
    let docs: Vec<Document> = coll
        .find(doc! {})
        .await
        .expect("find")
        .try_collect()
        .await
        .expect("collect");
    client.shutdown().await;

    docs.into_iter()
        .map(|mut d| {
            d.remove("_id");
            mongodb::bson::Bson::Document(d).into_relaxed_extjson()
        })
        .collect()
}

#[tokio::test]
async fn an_integer_past_2_pow_53_keeps_every_digit() {
    // The headline BSON risk: if the conversion picked `Double` for a large
    // integer, the value would come back rounded and no error would be raised.
    let (_c, uri) = start_mongo().await;
    let sink = MongoSink::new(MongoSinkConfig::new(&uri, "testdb", "bigints"))
        .await
        .expect("sink");

    let sent = vec![json!({
        "k": "big",
        "i64_max": i64::MAX,
        "i64_min": i64::MIN,
        "beyond": 9_007_199_254_740_993i64,
    })];
    sink.write_batch(&sent).await.expect("write");
    sink.flush().await.expect("flush");

    // Read as canonical extended JSON so the *chosen BSON type* is visible —
    // this is what distinguishes "the value is right" from "the value is right
    // and stored as an integer".
    let canonical = read_back(&uri, "bigints").await;
    let doc = &canonical[0];
    assert!(
        doc["beyond"].get("$numberLong").is_some(),
        "a large integer must be stored as BSON Int64, not Double — got {}",
        doc["beyond"]
    );

    let relaxed = read_relaxed(&uri, "bigints").await;
    assert_eq!(relaxed[0]["i64_max"].as_i64(), Some(i64::MAX));
    assert_eq!(relaxed[0]["i64_min"].as_i64(), Some(i64::MIN));
    assert_eq!(
        relaxed[0]["beyond"].as_i64(),
        Some(9_007_199_254_740_993),
        "the digit past 2^53 must survive"
    );
}

#[tokio::test]
async fn a_float_stays_a_float_even_when_whole_valued() {
    // The mirror risk: `1.0` narrowed to an integer type changes the schema a
    // downstream consumer infers, and `-0.0` losing its sign is unrecoverable.
    let (_c, uri) = start_mongo().await;
    let sink = MongoSink::new(MongoSinkConfig::new(&uri, "testdb", "floats"))
        .await
        .expect("sink");

    sink.write_batch(&[json!({
        "k": "f",
        "whole": 1.0,
        "negative_zero": -0.0,
        "very_large": 1e300,
        "repeating": 0.1,
    })])
    .await
    .expect("write");
    sink.flush().await.expect("flush");

    let relaxed = read_relaxed(&uri, "floats").await;
    let got = &relaxed[0];
    assert_eq!(got["very_large"].as_f64(), Some(1e300));
    assert_eq!(got["repeating"].as_f64(), Some(0.1));

    let nz = got["negative_zero"].as_f64().expect("a number");
    assert!(
        nz == 0.0 && nz.is_sign_negative(),
        "BSON Double is IEEE-754 and must keep the sign bit, got {nz}"
    );
}

#[tokio::test]
async fn strings_with_sql_and_bson_hazards_are_untouched() {
    let (_c, uri) = start_mongo().await;
    let sink = MongoSink::new(MongoSinkConfig::new(&uri, "testdb", "strings"))
        .await
        .expect("sink");

    let hazards = [
        ("empty", ""),
        ("quote", "it's"),
        ("backslash", "back\\slash"),
        ("newline", "line1\nline2"),
        ("padded", "  spaced  "),
        ("unicode", "héllo wörld — 日本語 🚰"),
        // A value that *looks* like a Mongo operator. Restrictions apply to
        // field names, not values, so this must pass through verbatim.
        ("operator_like", "$gt"),
        ("dotted_value", "a.b.c"),
    ];
    let records: Vec<Value> = hazards
        .iter()
        .map(|(k, v)| json!({ "k": k, "v": v }))
        .collect();
    sink.write_batch(&records).await.expect("write");
    sink.flush().await.expect("flush");

    let landed = read_relaxed(&uri, "strings").await;
    for (k, expected) in hazards {
        let got = landed
            .iter()
            .find(|d| d["k"] == json!(k))
            .unwrap_or_else(|| panic!("row {k} must be present"));
        assert_eq!(
            got["v"].as_str(),
            Some(expected),
            "{k} was altered in transit"
        );
    }
}

#[tokio::test]
async fn nested_structure_and_empty_containers_survive() {
    // An empty object and an empty array are the two shapes most often
    // normalised away (to null, or dropped entirely).
    let (_c, uri) = start_mongo().await;
    let sink = MongoSink::new(MongoSinkConfig::new(&uri, "testdb", "nested"))
        .await
        .expect("sink");

    let sent = json!({
        "k": "n",
        "object": { "a": 1, "b": { "c": [1, 2, 3] } },
        "array": [1, "two", null, true],
        "empty_object": {},
        "empty_array": [],
        "explicit_null": Value::Null,
    });
    sink.write_batch(std::slice::from_ref(&sent))
        .await
        .expect("write");
    sink.flush().await.expect("flush");

    let landed = read_relaxed(&uri, "nested").await;
    let got = &landed[0];
    assert_eq!(got["object"], sent["object"]);
    assert_eq!(got["array"], sent["array"]);
    assert_eq!(
        got["empty_object"],
        json!({}),
        "an empty object must survive"
    );
    assert_eq!(got["empty_array"], json!([]), "an empty array must survive");
    assert!(
        got.get("explicit_null").is_some(),
        "an explicit null must be stored, not dropped: {got}"
    );
    assert_eq!(got["explicit_null"], Value::Null);
}
