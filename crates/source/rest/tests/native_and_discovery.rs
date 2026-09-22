//! Native byte-passthrough (#633), OData fan-out discovery + key-range
//! partitioning (#479/#512), and the generic `discovery:` recipe (#647) —
//! driven end-to-end against wiremock, so the streaming/discovery code paths
//! are exercised over real HTTP rather than only through their pure helpers.

use faucet_core::Source;
use faucet_source_rest::{RestStream, RestStreamConfig};
use futures::StreamExt;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, Respond, ResponseTemplate};

/// A CSV async job: submit → poll (complete) → fetch, two locator pages.
fn csv_job(server: &MockServer) -> Value {
    let _ = server;
    json!({
        "submit": { "method": "POST", "url": "/jobs", "json": { "query": "SELECT Id FROM Lead" } },
        "job_id": "$.id",
        "poll": { "url": "/jobs/${job_id}", "interval_secs": 0, "timeout_secs": 30 },
        "status": { "path": "$.state", "success": ["Complete"], "failure": ["Failed"] },
        "fetch": {
            "method": "GET",
            "url": "/jobs/${job_id}/result",
            "locator_header": "Sforce-Locator",
            "locator_param": "locator"
        }
    })
}

/// Fetch responder: page 1 carries a locator, page 2 is terminal.
struct TwoCsvPages(Arc<AtomicUsize>);
impl Respond for TwoCsvPages {
    fn respond(&self, _: &wiremock::Request) -> ResponseTemplate {
        if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
            ResponseTemplate::new(200)
                .insert_header("Sforce-Locator", "loc2")
                .set_body_string("id,name\n1,alice\n")
        } else {
            ResponseTemplate::new(200).set_body_string("id,name\n2,bob\n")
        }
    }
}

async fn mount_csv_job(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path("/jobs"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "id": "job-1" })))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/jobs/job-1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "state": "Complete" })))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/jobs/job-1/result"))
        .respond_with(TwoCsvPages(Arc::new(AtomicUsize::new(0))))
        .mount(server)
        .await;
}

fn csv_native_config(server: &MockServer) -> RestStreamConfig {
    let mut cfg = RestStreamConfig::new(&server.uri(), "");
    cfg.async_job = Some(serde_json::from_value(csv_job(server)).unwrap());
    cfg.response_format = faucet_source_rest::ResponseFormat::Csv;
    cfg
}

/// Drain a `NativeBatch` payload into bytes (both variants).
async fn drain(batch: faucet_core::NativeBatch) -> Vec<u8> {
    match batch.payload {
        faucet_core::NativePayload::Bytes(b) => b,
        faucet_core::NativePayload::Stream(mut s) => {
            let mut out = Vec::new();
            while let Some(chunk) = s.next().await {
                out.extend_from_slice(&chunk.unwrap());
            }
            out
        }
    }
}

#[tokio::test]
async fn stream_native_emits_ndjson_per_locator_page() {
    let server = MockServer::start().await;
    mount_csv_job(&server).await;
    let stream = RestStream::new(csv_native_config(&server)).unwrap();
    assert_eq!(
        stream.native_output_formats(),
        &[faucet_core::NativeFormat::NdJson]
    );

    let ctx: HashMap<String, Value> = HashMap::new();
    let mut batches = stream.stream_native(&ctx, faucet_core::NativeFormat::NdJson, 1000);
    let mut bodies = Vec::new();
    while let Some(b) = batches.next().await {
        let b = b.unwrap();
        assert_eq!(b.format, faucet_core::NativeFormat::NdJson);
        bodies.push(drain(b).await);
    }

    // One batch per locator page, each already NDJSON — byte-identical to what
    // the `Value` path would have produced (all-String fields, header keys).
    assert_eq!(bodies.len(), 2, "one native batch per locator page");
    assert_eq!(bodies[0], b"{\"id\":\"1\",\"name\":\"alice\"}\n".to_vec());
    assert_eq!(bodies[1], b"{\"id\":\"2\",\"name\":\"bob\"}\n".to_vec());
}

/// #635 — the Arrow-columnar twin of the native path, over the same job.
#[cfg(feature = "arrow")]
#[tokio::test]
async fn stream_batches_emits_record_batches_matching_the_value_path() {
    let server = MockServer::start().await;
    mount_csv_job(&server).await;
    let stream = RestStream::new(csv_native_config(&server)).unwrap();
    assert!(
        stream.supports_columnar(),
        "a CSV async job with a header locator and no custom decode is columnar"
    );

    let ctx: HashMap<String, Value> = HashMap::new();
    let mut batches = stream.stream_batches(&ctx, 1000);
    let mut rows: Vec<Value> = Vec::new();
    let mut pages = 0usize;
    while let Some(p) = batches.next().await {
        let p = p.unwrap();
        pages += 1;
        rows.extend(faucet_core::columnar::record_batch_to_values(&p.batch).unwrap());
    }
    assert_eq!(pages, 2, "one batch per locator page");

    // The guarantee that makes automatic path selection safe: identical data
    // to the `Value` path over the same job.
    let server2 = MockServer::start().await;
    mount_csv_job(&server2).await;
    let value_stream = RestStream::new(csv_native_config(&server2)).unwrap();
    let mut vpages = <RestStream as faucet_core::Source>::stream_pages(&value_stream, &ctx, 1000);
    let mut vrows: Vec<Value> = Vec::new();
    while let Some(p) = vpages.next().await {
        vrows.extend(p.unwrap().records);
    }
    assert_eq!(rows, vrows, "columnar and Value paths must agree exactly");
    assert_eq!(
        rows,
        vec![
            json!({ "id": "1", "name": "alice" }),
            json!({ "id": "2", "name": "bob" }),
        ]
    );
}

/// The gate must be closed for every shape the row-at-a-time decoder cannot
/// serve, or the pipeline silently selects a path that mis-decodes.
#[cfg(feature = "arrow")]
#[tokio::test]
async fn the_columnar_gate_is_closed_for_shapes_it_cannot_serve() {
    let server = MockServer::start().await;

    // No async job at all.
    let plain = RestStream::new(RestStreamConfig::new(&server.uri(), "/x")).unwrap();
    assert!(!plain.supports_columnar());

    // JSON async job — only CSV decodes row-at-a-time.
    let mut json_job = csv_native_config(&server);
    json_job.response_format = faucet_source_rest::ResponseFormat::Json;
    assert!(!RestStream::new(json_job).unwrap().supports_columnar());

    // A body locator needs the parsed document this path never materializes.
    let mut body_loc = csv_native_config(&server);
    let mut job = csv_job(&server);
    job["fetch"]["locator_body"] = json!("$.next");
    body_loc.async_job = Some(serde_json::from_value(job).unwrap());
    assert!(
        !RestStream::new(body_loc).unwrap().supports_columnar(),
        "a body locator must fall back to the Value path"
    );
}

#[tokio::test]
async fn stream_native_rejects_a_format_it_does_not_emit() {
    let server = MockServer::start().await;
    mount_csv_job(&server).await;
    let stream = RestStream::new(csv_native_config(&server)).unwrap();
    let ctx: HashMap<String, Value> = HashMap::new();
    let mut batches = stream.stream_native(&ctx, faucet_core::NativeFormat::Csv, 1000);
    let err = batches
        .next()
        .await
        .expect("a batch result")
        .expect_err("Csv is not emitted");
    assert!(err.to_string().contains("only emits NdJson"), "{err}");
}

#[tokio::test]
async fn stream_native_incremental_emits_trailing_bookmark_batch() {
    let server = MockServer::start().await;
    mount_csv_job(&server).await;
    let mut cfg = csv_native_config(&server);
    cfg.replication_method = faucet_core::ReplicationMethod::Incremental;
    cfg.replication_key = Some("SystemModstamp".into());
    let stream = RestStream::new(cfg)
        .unwrap()
        .with_now_override_rfc3339("2026-06-01T12:00:00Z");

    let ctx: HashMap<String, Value> = HashMap::new();
    let mut batches = stream.stream_native(&ctx, faucet_core::NativeFormat::NdJson, 1000);
    let mut bookmarks = Vec::new();
    let mut payloads = Vec::new();
    while let Some(b) = batches.next().await {
        let b = b.unwrap();
        bookmarks.push(b.bookmark.clone());
        payloads.push(drain(b).await);
    }
    // Two data batches (no bookmark) + a final empty batch carrying the
    // run-start-minus-lookback bookmark.
    assert_eq!(bookmarks.len(), 3);
    assert!(bookmarks[0].is_none() && bookmarks[1].is_none());
    assert_eq!(bookmarks[2], Some(json!("2026-06-01T11:55:00Z")));
    assert!(payloads[2].is_empty(), "trailing batch carries no rows");
}

// ── OData fan-out discovery (objects + $metadata) ────────────────────────────

const EDMX: &str = r#"<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="Fin" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Ledger">
        <Key><PropertyRef Name="RecId"/></Key>
        <Property Name="RecId" Type="Edm.Int64" Nullable="false"/>
        <Property Name="Amount" Type="Edm.Decimal"/>
      </EntityType>
      <EntityType Name="Dept">
        <Key><PropertyRef Name="Code"/></Key>
        <Property Name="Code" Type="Edm.String" Nullable="false"/>
      </EntityType>
      <EntityContainer Name="C">
        <EntitySet Name="LedgerEntries" EntityType="Fin.Ledger"/>
        <EntitySet Name="Departments" EntityType="Fin.Dept"/>
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>"#;

fn odata_fanout_config(server: &MockServer, partition: Option<Value>) -> RestStreamConfig {
    let mut odata = json!({
        "version": "v4",
        "fan_out": true,
        "objects": "LedgerEntries,Departments",
        "emit": { "table_id": "fno_${name_snake}" }
    });
    if let Some(p) = partition {
        odata["partition"] = p;
    }
    let mut cfg = RestStreamConfig::new(&server.uri(), "/");
    cfg.odata = Some(serde_json::from_value(odata).unwrap());
    cfg
}

#[tokio::test]
async fn odata_discover_types_objects_from_metadata_and_renders_table_ids() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/$metadata"))
        .respond_with(ResponseTemplate::new(200).set_body_string(EDMX))
        .expect(1) // fetched once per source instance, not once per entity
        .mount(&server)
        .await;

    let stream = RestStream::new(odata_fanout_config(&server, None)).unwrap();
    assert!(stream.supports_discover());
    let descs = stream.discover().await.unwrap();

    assert_eq!(descs.len(), 2);
    let ledger = descs.iter().find(|d| d.name == "LedgerEntries").unwrap();
    assert_eq!(ledger.config_patch["odata"]["entity"], "LedgerEntries");
    assert_eq!(
        ledger.sink_patch.as_ref().unwrap()["table_id"],
        "fno_ledger_entries"
    );
    // The EDM types ride on the sink patch, so the sink declares real column
    // types instead of autodetecting (and rejecting a row that doesn't fit).
    let schema = &ledger.sink_patch.as_ref().unwrap()["schema"];
    assert_eq!(schema["properties"]["RecId"]["type"], "integer");
    assert_eq!(schema["properties"]["Amount"]["type"][0], "number");
    server.verify().await;
}

#[tokio::test]
async fn odata_discover_falls_back_to_untyped_when_metadata_unavailable() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/$metadata"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;

    let stream = RestStream::new(odata_fanout_config(&server, None)).unwrap();
    let descs = stream.discover().await.unwrap();
    // Still one descriptor per requested object (the run proceeds; the sink
    // autodetects), just with no schema attached.
    assert_eq!(descs.len(), 2);
    assert!(
        descs
            .iter()
            .all(|d| d.sink_patch.as_ref().unwrap().get("schema").is_none()),
        "untyped fallback attaches no schema"
    );
    assert_eq!(
        descs[0].sink_patch.as_ref().unwrap()["table_id"],
        "fno_ledger_entries"
    );
}

#[tokio::test]
async fn odata_discover_stamps_partition_only_for_single_int_key_entities() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/$metadata"))
        .respond_with(ResponseTemplate::new(200).set_body_string(EDMX))
        .mount(&server)
        .await;

    let cfg = odata_fanout_config(
        &server,
        Some(json!({
            "objects": "LedgerEntries,Departments",
            "workers": 6,
            "count": 24
        })),
    );
    let descs = RestStream::new(cfg).unwrap().discover().await.unwrap();

    // Int64 key → resolved partition block with the configured knobs.
    let ledger = descs.iter().find(|d| d.name == "LedgerEntries").unwrap();
    let part = &ledger.config_patch["odata"]["partition"];
    assert_eq!(part["key"], "RecId");
    assert_eq!(part["workers"], 6);
    assert_eq!(part["count"], 24);
    // String key → left sequential (no partition block), logged not failed.
    let dept = descs.iter().find(|d| d.name == "Departments").unwrap();
    assert!(dept.config_patch["odata"].get("partition").is_none());
}

#[tokio::test]
async fn odata_discover_without_objects_lists_every_entity_set() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/$metadata"))
        .respond_with(ResponseTemplate::new(200).set_body_string(EDMX))
        .mount(&server)
        .await;
    let mut cfg = RestStreamConfig::new(&server.uri(), "/");
    cfg.odata = Some(serde_json::from_value(json!({ "version": "v4" })).unwrap());
    let descs = RestStream::new(cfg).unwrap().discover().await.unwrap();
    let names: Vec<&str> = descs.iter().map(|d| d.name.as_str()).collect();
    assert!(names.contains(&"LedgerEntries") && names.contains(&"Departments"));
}

#[tokio::test]
async fn discover_without_a_recipe_or_odata_block_is_a_typed_error() {
    let server = MockServer::start().await;
    let stream = RestStream::new(RestStreamConfig::new(&server.uri(), "/")).unwrap();
    assert!(!stream.supports_discover());
    let err = stream.discover().await.expect_err("nothing to discover");
    assert!(err.to_string().contains("`discovery:`"), "{err}");
}

// ── Key-range partitioned extraction ─────────────────────────────────────────

#[tokio::test]
async fn key_range_partitioning_tiles_the_entity_and_suppresses_bookmarks() {
    let server = MockServer::start().await;
    // Key bounds: asc → 1, desc → 8.
    Mock::given(method("GET"))
        .and(path("/Ledger"))
        .and(query_param("$orderby", "RecId asc"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({ "value": [{ "RecId": 1 }] })),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/Ledger"))
        .and(query_param("$orderby", "RecId desc"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({ "value": [{ "RecId": 8 }] })),
        )
        .mount(&server)
        .await;
    // Every range page returns one row (no nextLink → one page per range).
    Mock::given(method("GET"))
        .and(path("/Ledger"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({ "value": [{ "RecId": 5, "Amount": 1 }] })),
        )
        .mount(&server)
        .await;

    let mut cfg = RestStreamConfig::new(&server.uri(), "/Ledger");
    cfg.odata = Some(
        serde_json::from_value(json!({
            "version": "v4",
            "entity": "Ledger",
            "partition": { "objects": "Ledger", "key": "RecId", "workers": 2, "count": 4 }
        }))
        .unwrap(),
    );
    cfg.records_path = Some("$.value[*]".into());

    let stream = RestStream::new(cfg).unwrap();
    let ctx: HashMap<String, Value> = HashMap::new();
    let mut pages = <RestStream as Source>::stream_pages(&stream, &ctx, 1000);
    let mut records = 0usize;
    while let Some(p) = pages.next().await {
        let p = p.unwrap();
        records += p.records.len();
        // Partitioned reads are an intra-run full-table scan: no per-range
        // bookmark may be persisted (it would be a partial high-water mark).
        assert!(
            p.bookmark.is_none(),
            "per-range bookmark must be suppressed"
        );
    }
    assert_eq!(records, 4, "one page per planned range (count: 4)");
}

#[tokio::test]
async fn key_range_partitioning_on_an_empty_entity_yields_nothing() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/Ledger"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "value": [] })))
        .mount(&server)
        .await;
    let mut cfg = RestStreamConfig::new(&server.uri(), "/Ledger");
    cfg.odata = Some(
        serde_json::from_value(json!({
            "version": "v4",
            "entity": "Ledger",
            "partition": { "objects": "Ledger", "key": "RecId" }
        }))
        .unwrap(),
    );
    cfg.records_path = Some("$.value[*]".into());
    let stream = RestStream::new(cfg).unwrap();
    let ctx: HashMap<String, Value> = HashMap::new();
    let mut pages = <RestStream as Source>::stream_pages(&stream, &ctx, 1000);
    assert!(pages.next().await.is_none(), "empty entity → no pages");
}

#[tokio::test]
async fn key_range_partitioning_surfaces_a_non_integer_key() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/Ledger"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({ "value": [{ "RecId": "abc" }] })),
        )
        .mount(&server)
        .await;
    let mut cfg = RestStreamConfig::new(&server.uri(), "/Ledger");
    cfg.odata = Some(
        serde_json::from_value(json!({
            "version": "v4",
            "entity": "Ledger",
            "partition": { "objects": "Ledger", "key": "RecId" }
        }))
        .unwrap(),
    );
    cfg.records_path = Some("$.value[*]".into());
    let stream = RestStream::new(cfg).unwrap();
    let ctx: HashMap<String, Value> = HashMap::new();
    let mut pages = <RestStream as Source>::stream_pages(&stream, &ctx, 1000);
    let err = pages
        .next()
        .await
        .expect("a page result")
        .expect_err("non-integer key must fail loudly");
    assert!(err.to_string().contains("is not an integer"), "{err}");
}

#[tokio::test]
async fn key_bounds_http_failure_is_a_typed_source_error() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/Ledger"))
        .respond_with(ResponseTemplate::new(503))
        .mount(&server)
        .await;
    let mut cfg = RestStreamConfig::new(&server.uri(), "/Ledger");
    cfg.odata = Some(
        serde_json::from_value(json!({
            "version": "v4",
            "entity": "Ledger",
            "partition": { "objects": "Ledger", "key": "RecId" }
        }))
        .unwrap(),
    );
    let stream = RestStream::new(cfg).unwrap();
    let ctx: HashMap<String, Value> = HashMap::new();
    let mut pages = <RestStream as Source>::stream_pages(&stream, &ctx, 1000);
    let err = pages
        .next()
        .await
        .expect("a page result")
        .expect_err("503 must surface");
    assert!(
        err.to_string().contains("key-bounds returned HTTP 503"),
        "{err}"
    );
}

// ── Generic `discovery:` recipe ──────────────────────────────────────────────

fn recipe_config(server: &MockServer, recipe: Value) -> RestStreamConfig {
    let mut cfg = RestStreamConfig::new(&server.uri(), "/");
    cfg.discovery = Some(serde_json::from_value(recipe).unwrap());
    cfg
}

#[tokio::test]
async fn discovery_recipe_lists_describes_and_emits_descriptors() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/objects"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [
                { "name": "Account", "queryable": true },
                { "name": "AccountFeed", "queryable": true },
                { "name": "Secret", "queryable": false }
            ]
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/describe/Account"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "fields": [
                { "name": "Id", "type": "id", "nillable": false },
                { "name": "Name", "type": "string", "nillable": true },
                { "name": "Blob", "type": "base64", "nillable": true }
            ]
        })))
        .mount(&server)
        .await;

    let stream = RestStream::new(recipe_config(
        &server,
        json!({
            "list": {
                "get": "/objects",
                "items": "$.items[*]",
                "name": "$.name",
                "keep_if": { "path": "$.queryable", "equals": true },
                "exclude_name_suffixes": ["Feed"]
            },
            "describe": {
                "get": "/describe/${name}",
                "fields": "$.fields[*]",
                "field_name": "$.name",
                "field_type": "$.type",
                "field_nullable": "$.nillable",
                "skip_types": ["base64"],
                "type_map": { "id": "string", "string": "string", "*": "string" }
            },
            "emit": {
                "config": { "async_job": { "submit": { "json": { "query": "SELECT ${field_names} FROM ${name}" } } } },
                "table_id": "sf_${name_snake}"
            }
        }),
    ))
    .unwrap();

    let descs = stream.discover().await.unwrap();
    // `keep_if` dropped Secret; `exclude_name_suffixes` dropped AccountFeed.
    assert_eq!(descs.len(), 1);
    let d = &descs[0];
    assert_eq!(d.name, "Account");
    // `${field_names}` rendered from describe, with the skipped type excluded.
    assert_eq!(
        d.config_patch["async_job"]["submit"]["json"]["query"],
        "SELECT Id, Name FROM Account"
    );
    assert_eq!(d.sink_patch.as_ref().unwrap()["table_id"], "sf_account");
    // Typed schema: non-nillable stays a bare type, nillable gains null.
    let props = &d.schema.as_ref().unwrap()["properties"];
    assert_eq!(props["Id"]["type"], "string");
    assert!(props["Name"]["type"].is_array(), "nullable → [type, null]");
}

#[tokio::test]
async fn discovery_recipe_explicit_objects_skip_the_list_request() {
    let server = MockServer::start().await;
    // No /objects mock is mounted: an explicit `objects:` list must not call it.
    let stream = RestStream::new(recipe_config(
        &server,
        json!({
            "objects": "Lead,Case",
            "emit": { "config": { "path": "/${name_lower}" } }
        }),
    ))
    .unwrap();
    let descs = stream.discover().await.unwrap();
    assert_eq!(descs.len(), 2);
    assert_eq!(descs[0].config_patch["path"], "/lead");
    assert_eq!(descs[1].config_patch["path"], "/case");
    assert!(descs[0].sink_patch.is_none(), "no table_id → no sink patch");
}

#[tokio::test]
async fn discovery_recipe_skips_a_dataset_whose_describe_has_no_fields() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/describe/Empty"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "fields": [] })))
        .mount(&server)
        .await;
    let stream = RestStream::new(recipe_config(
        &server,
        json!({
            "objects": "Empty",
            "describe": { "get": "/describe/${name}", "fields": "$.fields[*]", "field_name": "$.name" },
            "emit": { "config": { "q": "SELECT ${field_names} FROM ${name}" } }
        }),
    ))
    .unwrap();
    assert!(
        stream.discover().await.unwrap().is_empty(),
        "nothing selectable → dataset skipped"
    );
}

#[tokio::test]
async fn discovery_recipe_list_http_failure_surfaces() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/objects"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;
    let stream = RestStream::new(recipe_config(
        &server,
        json!({
            "list": { "get": "/objects", "items": "$.items[*]", "name": "$.name" },
            "emit": { "config": { "path": "/${name}" } }
        }),
    ))
    .unwrap();
    let err = stream.discover().await.expect_err("500 must surface");
    assert!(err.to_string().contains("discovery list"), "{err}");
}

#[tokio::test]
async fn discovery_recipe_list_non_json_body_is_a_typed_error() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/objects"))
        .respond_with(ResponseTemplate::new(200).set_body_string("<html>nope</html>"))
        .mount(&server)
        .await;
    let stream = RestStream::new(recipe_config(
        &server,
        json!({
            "list": { "get": "/objects", "items": "$.items[*]", "name": "$.name" },
            "emit": { "config": { "path": "/${name}" } }
        }),
    ))
    .unwrap();
    let err = stream.discover().await.expect_err("non-JSON must surface");
    assert!(err.to_string().contains("invalid JSON"), "{err}");
}
