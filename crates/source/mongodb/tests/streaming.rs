//! Integration tests for `MongoSource::stream_pages` against a real MongoDB
//! instance via testcontainers.
//!
//! These tests require Docker. Each test boots its own container and seeds
//! its own collection so they are fully isolated and safe to run in
//! parallel.

use faucet_core::{DEFAULT_BATCH_SIZE, Source};
use faucet_source_mongodb::{MongoSource, MongoSourceConfig};
use futures::StreamExt;
use mongodb::Client;
use mongodb::bson::{Document, doc};
use std::collections::HashMap;
use std::time::Instant;
use testcontainers::ContainerAsync;
use testcontainers_modules::mongo::Mongo;

/// Start a MongoDB container and return both the container handle and a
/// connection URI. The container is kept alive by the returned handle.
async fn start_mongo() -> (ContainerAsync<Mongo>, String) {
    let container: ContainerAsync<Mongo> =
        faucet_conformance::containers::start(Mongo::default).await;
    let port = container
        .get_host_port_ipv4(27017)
        .await
        .expect("mongo port");
    let uri = format!("mongodb://127.0.0.1:{port}");
    (container, uri)
}

/// Insert `n` documents of the form `{ id: i }` for `i = 1..=n` into
/// `db.collection`.
async fn seed_docs(uri: &str, db: &str, collection: &str, n: i64) {
    let client = Client::with_uri_str(uri).await.expect("client");
    let coll = client.database(db).collection::<Document>(collection);
    let docs: Vec<Document> = (1..=n).map(|i| doc! { "id": i }).collect();
    coll.insert_many(docs).await.expect("insert_many");
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_chunks_documents_into_batch_sized_pages() {
    let (_container, uri) = start_mongo().await;
    seed_docs(&uri, "testdb", "events", 10_000).await;

    let config = MongoSourceConfig::new(uri, "testdb", "events").with_batch_size(1000);
    let source = MongoSource::new(config).await.expect("source new");

    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 1000);

    let mut page_count = 0;
    let mut total = 0;
    while let Some(page) = pages.next().await {
        let page = page.expect("page ok");
        page_count += 1;
        total += page.records.len();
        assert_eq!(
            page.records.len(),
            1000,
            "every page must hold exactly batch_size docs when total is a multiple"
        );
        assert!(
            page.bookmark.is_none(),
            "mongodb source has no incremental mode yet; bookmark must be None"
        );
    }
    assert_eq!(page_count, 10);
    assert_eq!(total, 10_000);
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_partial_final_page() {
    let (_container, uri) = start_mongo().await;
    seed_docs(&uri, "testdb", "events", 2_500).await;

    let config = MongoSourceConfig::new(uri, "testdb", "events").with_batch_size(1000);
    let source = MongoSource::new(config).await.expect("source new");

    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 1000);

    let mut sizes = Vec::new();
    while let Some(page) = pages.next().await {
        sizes.push(page.expect("page ok").records.len());
    }
    assert_eq!(sizes, vec![1000, 1000, 500]);
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_batch_size_zero_emits_single_page() {
    let (_container, uri) = start_mongo().await;
    seed_docs(&uri, "testdb", "events", 10_000).await;

    let config = MongoSourceConfig::new(uri, "testdb", "events").with_batch_size(0);
    let source = MongoSource::new(config).await.expect("source new");

    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 0);

    let mut collected = Vec::new();
    while let Some(page) = pages.next().await {
        collected.push(page.expect("page ok").records.len());
    }
    assert_eq!(
        collected,
        vec![10_000],
        "batch_size = 0 must drain the cursor and emit exactly one page"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_empty_result_yields_no_pages() {
    let (_container, uri) = start_mongo().await;
    // Don't seed — the collection is empty.

    let config = MongoSourceConfig::new(uri, "testdb", "empty").with_batch_size(DEFAULT_BATCH_SIZE);
    let source = MongoSource::new(config).await.expect("source new");

    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, DEFAULT_BATCH_SIZE);

    let mut page_count = 0;
    while let Some(page) = pages.next().await {
        let _ = page.expect("page ok");
        page_count += 1;
    }
    assert_eq!(page_count, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_preserves_document_contents() {
    let (_container, uri) = start_mongo().await;
    let client = Client::with_uri_str(&uri).await.expect("client");
    let coll = client.database("testdb").collection::<Document>("items");
    coll.insert_many(vec![
        doc! { "id": 1, "name": "alpha" },
        doc! { "id": 2, "name": "beta" },
        doc! { "id": 3, "name": "gamma" },
    ])
    .await
    .expect("insert_many");

    let config = MongoSourceConfig::new(uri, "testdb", "items")
        .sort(serde_json::json!({"id": 1}))
        .with_batch_size(2);
    let source = MongoSource::new(config).await.expect("source new");

    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 2);

    let mut all = Vec::new();
    while let Some(page) = pages.next().await {
        all.extend(page.expect("page ok").records);
    }
    assert_eq!(all.len(), 3);
    assert_eq!(all[0]["id"], 1);
    assert_eq!(all[0]["name"], "alpha");
    assert_eq!(all[2]["name"], "gamma");
}

/// Catches the "buffered-then-chunked" anti-pattern.
///
/// The MongoDB driver's cursor naturally streams documents in batches from
/// the server. The true-streaming impl yields a `StreamPage` after parsing
/// `batch_size` documents off the cursor; the default trait impl materialises
/// every document into a `Vec<Value>` before any page is yielded.
///
/// For a large result, the parse-and-buffer cost dominates and the
/// difference is observable: dropping the stream after the first page in the
/// streaming impl avoids parsing the remaining ~99% of documents.
#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_first_page_completes_without_parsing_full_result() {
    let (_container, uri) = start_mongo().await;
    seed_docs(&uri, "testdb", "events", 100_000).await;

    let ctx: HashMap<String, serde_json::Value> = HashMap::new();

    // Full drain for reference.
    let config_full = MongoSourceConfig::new(&uri, "testdb", "events").with_batch_size(1000);
    let source = MongoSource::new(config_full).await.expect("source new");
    let start = Instant::now();
    let mut pages = source.stream_pages(&ctx, 1000);
    while let Some(page) = pages.next().await {
        let _ = page.expect("page ok");
    }
    let full_elapsed = start.elapsed();
    drop(pages);
    drop(source);

    // First page only.
    let config_first = MongoSourceConfig::new(&uri, "testdb", "events").with_batch_size(1000);
    let source = MongoSource::new(config_first).await.expect("source new");
    let start = Instant::now();
    let mut pages = source.stream_pages(&ctx, 1000);
    let first = pages
        .next()
        .await
        .expect("first page exists")
        .expect("page ok");
    let first_elapsed = start.elapsed();
    drop(pages);
    assert_eq!(first.records.len(), 1000);

    assert!(
        first_elapsed * 2 < full_elapsed,
        "first page should arrive without parsing the full result; \
         first page took {first_elapsed:?}, full drain took {full_elapsed:?}"
    );
}

// --- fetch_all(): the convenience buffering path (distinct from stream_pages) ---

#[tokio::test(flavor = "multi_thread")]
async fn fetch_all_returns_all_matching_documents() {
    let (_container, uri) = start_mongo().await;
    seed_docs(&uri, "testdb", "events", 1_234).await;

    let config = MongoSourceConfig::new(uri, "testdb", "events");
    let source = MongoSource::new(config).await.expect("source new");

    let records = source.fetch_all().await.expect("fetch_all");
    assert_eq!(records.len(), 1_234);
    // Every record carries its `id` field and a server-assigned `_id`.
    assert!(records.iter().all(|r| r.get("id").is_some()));
    assert!(records.iter().all(|r| r["_id"]["$oid"].is_string()));
}

#[tokio::test(flavor = "multi_thread")]
async fn fetch_all_applies_filter_projection_sort_and_limit() {
    let (_container, uri) = start_mongo().await;
    let client = Client::with_uri_str(&uri).await.expect("client");
    let coll = client.database("testdb").collection::<Document>("people");
    coll.insert_many(vec![
        doc! { "id": 1, "name": "alice", "active": true },
        doc! { "id": 2, "name": "bob", "active": false },
        doc! { "id": 3, "name": "carol", "active": true },
        doc! { "id": 4, "name": "dave", "active": true },
    ])
    .await
    .expect("insert_many");

    let config = MongoSourceConfig::new(uri, "testdb", "people")
        .filter(serde_json::json!({"active": true}))
        .projection(serde_json::json!({"_id": 0, "name": 1, "id": 1}))
        .sort(serde_json::json!({"id": -1}))
        .limit(2);
    let source = MongoSource::new(config).await.expect("source new");

    let records = source.fetch_all().await.expect("fetch_all");
    // filter: only active (alice, carol, dave); sort id desc → dave(4), carol(3);
    // limit 2 → two records; projection drops _id.
    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["name"], "dave");
    assert_eq!(records[1]["name"], "carol");
    assert!(
        records[0].get("_id").is_none(),
        "projection must exclude _id: {:?}",
        records[0]
    );
    assert!(
        records[0].get("active").is_none(),
        "projection excludes active"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn fetch_all_with_cursor_batch_size_and_empty_result() {
    let (_container, uri) = start_mongo().await;
    // Empty collection — exercises the zero-document branch of fetch_all.
    let config = MongoSourceConfig::new(uri, "testdb", "nothing_here").cursor_batch_size(50);
    let source = MongoSource::new(config).await.expect("source new");

    let records = source.fetch_all().await.expect("fetch_all");
    assert!(records.is_empty(), "empty collection yields no records");
}

#[tokio::test(flavor = "multi_thread")]
async fn fetch_all_preserves_bson_types_as_relaxed_extjson() {
    use mongodb::bson::DateTime;
    use mongodb::bson::oid::ObjectId;
    let (_container, uri) = start_mongo().await;
    let client = Client::with_uri_str(&uri).await.expect("client");
    let coll = client.database("testdb").collection::<Document>("typed");
    let oid = ObjectId::parse_str("64ab00112233445566778899").unwrap();
    coll.insert_one(doc! {
        "_id": oid,
        "when": DateTime::from_millis(1_000_000),
        "count": 9_000_000_000i64,
        "nothing": mongodb::bson::Bson::Null,
        "tags": ["x", "y"],
        "nested": { "inner": 5 },
    })
    .await
    .expect("insert_one");

    let config = MongoSourceConfig::new(uri, "testdb", "typed");
    let source = MongoSource::new(config).await.expect("source new");
    let records = source.fetch_all().await.expect("fetch_all");
    assert_eq!(records.len(), 1);
    let r = &records[0];
    assert_eq!(r["_id"]["$oid"], "64ab00112233445566778899");
    assert!(r["when"]["$date"].is_string(), "datetime → $date: {r:?}");
    assert_eq!(r["count"], serde_json::json!(9_000_000_000i64));
    assert_eq!(r["nothing"], serde_json::Value::Null);
    assert_eq!(r["tags"], serde_json::json!(["x", "y"]));
    assert_eq!(r["nested"]["inner"], 5);
}

// --- fetch_with_context: empty ctx delegates to fetch_all; non-empty substitutes ---

#[tokio::test(flavor = "multi_thread")]
async fn fetch_with_context_empty_delegates_to_fetch_all() {
    let (_container, uri) = start_mongo().await;
    seed_docs(&uri, "testdb", "events", 5).await;

    let config = MongoSourceConfig::new(uri, "testdb", "events");
    let source = MongoSource::new(config).await.expect("source new");

    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let records = source
        .fetch_with_context(&ctx)
        .await
        .expect("fetch_with_context");
    assert_eq!(records.len(), 5);
}

#[tokio::test(flavor = "multi_thread")]
async fn fetch_with_context_substitutes_filter_placeholder() {
    let (_container, uri) = start_mongo().await;
    let client = Client::with_uri_str(&uri).await.expect("client");
    let coll = client.database("testdb").collection::<Document>("orders");
    coll.insert_many(vec![
        doc! { "id": 1, "region": "us" },
        doc! { "id": 2, "region": "eu" },
        doc! { "id": 3, "region": "us" },
    ])
    .await
    .expect("insert_many");

    // Placeholders use the flat `{key}` syntax over top-level context keys.
    let config = MongoSourceConfig::new(uri, "testdb", "orders")
        .filter(serde_json::json!({"region": "{region}"}))
        .sort(serde_json::json!({"id": 1}));
    let source = MongoSource::new(config).await.expect("source new");

    let mut ctx: HashMap<String, serde_json::Value> = HashMap::new();
    ctx.insert("region".into(), serde_json::json!("us"));

    let records = source
        .fetch_with_context(&ctx)
        .await
        .expect("fetch_with_context");
    // Only the two "us" docs match after substitution.
    let ids: Vec<i64> = records.iter().map(|r| r["id"].as_i64().unwrap()).collect();
    assert_eq!(ids, vec![1, 3]);
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_substitutes_context_into_filter() {
    use futures::StreamExt;
    let (_container, uri) = start_mongo().await;
    let client = Client::with_uri_str(&uri).await.expect("client");
    let coll = client.database("testdb").collection::<Document>("logs");
    coll.insert_many(vec![
        doc! { "id": 1, "level": "info" },
        doc! { "id": 2, "level": "error" },
        doc! { "id": 3, "level": "error" },
    ])
    .await
    .expect("insert_many");

    let config = MongoSourceConfig::new(uri, "testdb", "logs")
        .filter(serde_json::json!({"level": "{level}"}))
        .with_batch_size(10);
    let source = MongoSource::new(config).await.expect("source new");

    let mut ctx: HashMap<String, serde_json::Value> = HashMap::new();
    ctx.insert("level".into(), serde_json::json!("error"));

    let mut pages = source.stream_pages(&ctx, 10);
    let mut total = 0;
    while let Some(page) = pages.next().await {
        total += page.expect("page ok").records.len();
    }
    assert_eq!(
        total, 2,
        "only the two error docs match the substituted filter"
    );
}

// --- discover(): live catalog introspection (#211) ---

#[tokio::test(flavor = "multi_thread")]
async fn discover_enumerates_collections_with_schemas() {
    let (_container, uri) = start_mongo().await;
    let client = Client::with_uri_str(&uri).await.expect("client");
    let db = client.database("shop");
    db.collection::<Document>("orders")
        .insert_many(vec![
            doc! { "id": 1, "note": "a" },
            doc! { "id": 2, "note": "b" },
            doc! { "id": 3, "note": "c" },
        ])
        .await
        .expect("seed orders");
    db.collection::<Document>("carts")
        .insert_many(vec![doc! { "id": 1, "open": true }])
        .await
        .expect("seed carts");
    // faucet's own collections are not data (#789 MSG-92).
    for internal in ["_faucet_commit_token", "orders__faucet_ovw"] {
        db.collection::<Document>(internal)
            .insert_one(doc! { "x": 1 })
            .await
            .expect("seed internal");
    }

    let config = MongoSourceConfig::new(uri, "shop", "orders");
    let source = MongoSource::new(config).await.expect("source new");
    assert!(source.supports_discover());
    let datasets = source.discover().await.expect("discover");

    let names: Vec<&str> = datasets.iter().map(|d| d.name.as_str()).collect();
    assert!(names.contains(&"orders"), "got: {names:?}");
    assert!(names.contains(&"carts"), "got: {names:?}");
    assert!(
        !names.iter().any(|n| n.starts_with("system.")),
        "system collections must be excluded: {names:?}"
    );
    assert_eq!(
        names.len(),
        2,
        "faucet-internal collections excluded: {names:?}"
    );

    let orders = datasets
        .iter()
        .find(|d| d.name == "orders")
        .expect("orders dataset");
    assert_eq!(orders.kind, "collection");
    assert_eq!(
        orders.config_patch,
        serde_json::json!({ "collection": "orders" })
    );
    assert_eq!(orders.estimated_rows, Some(3));
    let schema = orders.schema.as_ref().expect("schema from sampled docs");
    assert_eq!(schema["type"], "object");
    assert_eq!(schema["properties"]["id"]["type"], "integer");
    assert_eq!(schema["properties"]["note"]["type"], "string");

    let carts = datasets
        .iter()
        .find(|d| d.name == "carts")
        .expect("carts dataset");
    assert_eq!(carts.estimated_rows, Some(1));
    assert_eq!(
        carts.schema.as_ref().expect("carts schema")["properties"]["open"]["type"],
        "boolean"
    );
}

// --- instance trait methods (require a live server via new()) ---

#[tokio::test(flavor = "multi_thread")]
async fn config_schema_and_dataset_uri_via_instance() {
    let (_container, uri) = start_mongo().await;
    let config = MongoSourceConfig::new(format!("{uri}/?appName=t"), "shop", "carts");
    let source = MongoSource::new(config).await.expect("source new");

    let schema = source.config_schema();
    let props = schema
        .get("properties")
        .and_then(|p| p.as_object())
        .expect("config_schema must be a JSON object schema");
    assert!(props.contains_key("connection_uri"), "schema: {schema}");
    assert!(props.contains_key("filter"), "schema documents `filter`");
    assert!(
        props.contains_key("batch_size"),
        "schema documents `batch_size`"
    );

    let ds = source.dataset_uri();
    assert!(ds.ends_with("/shop/carts"), "dataset_uri: {ds}");
    assert!(
        !ds.contains('?'),
        "query options are not part of the identity: {ds}"
    );
}
