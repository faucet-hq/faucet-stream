//! Integration tests for `DynamoDbSource` against DynamoDB Local (Docker).
//! Each test boots its own container; skipped when Docker is unavailable.

mod common;

use aws_sdk_dynamodb::types::ScalarAttributeType;
use common::{config, create_table, item, n, put_items, s, start};
use faucet_core::Source;
use faucet_source_dynamodb::{DynamoDbSource, OnGap, ReadMode, StreamBookmark};
use futures::StreamExt;
use serde_json::{Value, json};
use std::collections::{BTreeSet, HashMap};

fn ids(records: &[Value]) -> Vec<String> {
    let mut v: Vec<String> = records
        .iter()
        .map(|r| r["pk"].as_str().expect("pk").to_string())
        .collect();
    v.sort();
    v
}

async fn drain(source: &DynamoDbSource) -> (Vec<Value>, Vec<Option<Value>>) {
    let ctx: HashMap<String, Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 0);
    let mut records = Vec::new();
    let mut bookmarks = Vec::new();
    while let Some(page) = pages.next().await {
        let page = page.expect("page");
        records.extend(page.records);
        bookmarks.push(page.bookmark);
    }
    (records, bookmarks)
}

#[tokio::test(flavor = "multi_thread")]
async fn parallel_scan_returns_every_item_once_and_resumes() {
    let Some((_c, endpoint, client)) = start().await else {
        return;
    };
    create_table(&client, "items", ScalarAttributeType::S, false, false).await;
    let mut items: Vec<_> = (0..500)
        .map(|i| item(vec![("pk", s(&format!("k{i:04}"))), ("v", n(i))]))
        .collect();
    items.push(item(vec![
        ("pk", s("precise")),
        ("v", n("12345678901234567890123")),
        (
            "bin",
            aws_sdk_dynamodb::types::AttributeValue::B(vec![1, 2].into()),
        ),
        (
            "tags",
            aws_sdk_dynamodb::types::AttributeValue::Ss(vec!["x".into()]),
        ),
    ]));
    put_items(&client, "items", items).await;

    let mut cfg = config(&endpoint, "items");
    cfg.segments = 4;
    cfg.batch_size = 50;
    cfg.page_limit = Some(40);
    let source = DynamoDbSource::new(cfg.clone()).await.unwrap();
    let (records, bookmarks) = drain(&source).await;
    let got = ids(&records);
    assert_eq!(got.len(), 501, "every item exactly once");
    assert_eq!(got.iter().collect::<BTreeSet<_>>().len(), 501);
    let precise = records.iter().find(|r| r["pk"] == "precise").unwrap();
    assert_eq!(precise["v"], json!("12345678901234567890123"));
    assert_eq!(precise["bin"], json!("AQI="));
    assert_eq!(precise["tags"], json!(["x"]));
    assert!(records.iter().any(|r| r["v"] == json!(7)));
    assert_eq!(
        bookmarks.last().unwrap().as_ref().unwrap()["segments"],
        json!({}),
        "the final page resets the cursors so the next run re-reads the table"
    );

    // Resume from an intermediate bookmark: nothing is lost.
    let mid = bookmarks
        .iter()
        .flatten()
        .find(|b| b["segments"].as_object().is_some_and(|m| !m.is_empty()))
        .cloned()
        .expect("an intermediate cursor bookmark");
    let resumed = DynamoDbSource::new(cfg.clone()).await.unwrap();
    resumed.apply_start_bookmark(mid).await.unwrap();
    let (again, _) = drain(&resumed).await;
    assert!(again.len() < 501, "resume skips finished work");

    // Cluster shards: each segment on its own instance, union = the table.
    let planner = DynamoDbSource::new(cfg.clone()).await.unwrap();
    let shards = planner.enumerate_shards(3).await.unwrap();
    assert_eq!(shards.len(), 4, "configured segments win over the target");
    let mut union = Vec::new();
    for shard in &shards {
        let worker = DynamoDbSource::new(cfg.clone()).await.unwrap();
        worker.apply_shard(shard).await.unwrap();
        union.extend(worker.fetch_all().await.unwrap());
    }
    let u = ids(&union);
    assert_eq!(u.len(), 501);
    assert_eq!(u.iter().collect::<BTreeSet<_>>().len(), 501);

    // Discovery + preflight.
    let found = source.discover().await.unwrap();
    assert!(found.iter().any(|d| d.name == "items"));
    let report = source
        .check(&faucet_core::CheckContext::default())
        .await
        .unwrap();
    assert_eq!(report.failed_count(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn query_by_key_condition_with_filter_and_projection() {
    let Some((_c, endpoint, client)) = start().await else {
        return;
    };
    create_table(&client, "events", ScalarAttributeType::S, true, false).await;
    let items: Vec<_> = (0..30)
        .map(|i| {
            item(vec![
                ("pk", s(if i % 2 == 0 { "a" } else { "b" })),
                ("sk", n(i)),
                ("status", s(if i % 4 == 0 { "open" } else { "closed" })),
                ("extra", s("x")),
            ])
        })
        .collect();
    put_items(&client, "events", items).await;

    let mut cfg = config(&endpoint, "events");
    cfg.mode = ReadMode::Query;
    cfg.key_condition_expression = Some("pk = :p".into());
    cfg.expression_attribute_values
        .insert(":p".into(), json!("a"));
    cfg.scan_index_forward = false;
    cfg.page_limit = Some(4);
    cfg.batch_size = 3;
    let source = DynamoDbSource::new(cfg.clone()).await.unwrap();
    let records = source.fetch_all().await.unwrap();
    let sks: Vec<i64> = records.iter().map(|r| r["sk"].as_i64().unwrap()).collect();
    assert_eq!(
        sks,
        (0..30).filter(|i| i % 2 == 0).rev().collect::<Vec<_>>()
    );

    cfg.filter_expression = Some("#s = :open".into());
    cfg.projection = Some("pk, sk, #s".into());
    cfg.expression_attribute_names
        .insert("#s".into(), "status".into());
    cfg.expression_attribute_values
        .insert(":open".into(), json!("open"));
    let source = DynamoDbSource::new(cfg).await.unwrap();
    let records = source.fetch_all().await.unwrap();
    assert_eq!(records.len(), 8);
    assert!(
        records
            .iter()
            .all(|r| r["status"] == "open" && r.get("extra").is_none())
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn streams_cdc_captures_changes_and_resumes() {
    let Some((_c, endpoint, client)) = start().await else {
        return;
    };
    create_table(&client, "orders", ScalarAttributeType::S, false, true).await;
    for i in 0..3 {
        client
            .put_item()
            .table_name("orders")
            .set_item(Some(item(vec![("pk", s(&format!("o{i}"))), ("qty", n(i))])))
            .send()
            .await
            .unwrap();
    }
    client
        .put_item()
        .table_name("orders")
        .set_item(Some(item(vec![("pk", s("o1")), ("qty", n(10))])))
        .send()
        .await
        .unwrap();
    client
        .delete_item()
        .table_name("orders")
        .key("pk", s("o2"))
        .send()
        .await
        .unwrap();

    let mut cfg = config(&endpoint, "orders");
    cfg.mode = ReadMode::Streams;
    cfg.idle_termination_secs = Some(3);
    cfg.batch_size = 2;
    let source = DynamoDbSource::new(cfg.clone()).await.unwrap();
    let capture = source.capture_resume_position().await.unwrap().unwrap();
    assert!(
        StreamBookmark::from_value(&capture)
            .shards
            .values()
            .all(String::is_empty)
    );
    let report = source
        .check(&faucet_core::CheckContext::default())
        .await
        .unwrap();
    assert_eq!(report.failed_count(), 0, "{report:?}");

    let (records, bookmarks) = drain(&source).await;
    let ops: Vec<&str> = records.iter().map(|r| r["op"].as_str().unwrap()).collect();
    assert_eq!(ops, vec!["c", "c", "c", "u", "d"]);
    assert_eq!(records[3]["before"]["qty"], 1);
    assert_eq!(records[3]["after"]["qty"], 10);
    assert_eq!(records[4]["before"]["pk"], "o2");
    assert_eq!(records[4]["after"], Value::Null);
    assert_eq!(records[4]["document_key"], json!({"pk": "o2"}));
    assert!(bookmarks.iter().all(Option::is_some));
    let bookmark = bookmarks.last().unwrap().clone().unwrap();

    // Resume: only changes after the bookmark.
    client
        .put_item()
        .table_name("orders")
        .set_item(Some(item(vec![("pk", s("o9")), ("qty", n(9))])))
        .send()
        .await
        .unwrap();
    let resumed = DynamoDbSource::new(cfg.clone()).await.unwrap();
    resumed
        .apply_start_bookmark(bookmark.clone())
        .await
        .unwrap();
    let (records, _) = drain(&resumed).await;
    assert_eq!(records.len(), 1, "{records:?}");
    assert_eq!(records[0]["after"]["pk"], "o9");

    // A bookmark naming a shard that no longer exists is a gap.
    let mut gapped = StreamBookmark::from_value(&bookmark);
    gapped.advance("shardId-00000000000000000000-deadbeef", "1");
    let failing = DynamoDbSource::new(cfg.clone()).await.unwrap();
    failing
        .apply_start_bookmark(gapped.to_value())
        .await
        .unwrap();
    let err = failing.fetch_all().await.unwrap_err().to_string();
    assert!(err.contains("expired") && err.contains("on_gap"), "{err}");

    // on_gap: resnapshot re-reads the table, then streams the retained window.
    cfg.on_gap = OnGap::Resnapshot;
    let resnap = DynamoDbSource::new(cfg).await.unwrap();
    resnap
        .apply_start_bookmark(gapped.to_value())
        .await
        .unwrap();
    let (records, bookmarks) = drain(&resnap).await;
    let snapshot: Vec<&Value> = records.iter().filter(|r| r["op"] == "r").collect();
    assert_eq!(snapshot.len(), 3, "o0, o1, o9 are live");
    assert!(snapshot.iter().all(|r| r["key"]["pk"].is_string()));
    assert!(records.iter().filter(|r| r["op"] != "r").count() >= 6);
    let last = StreamBookmark::from_value(bookmarks.last().unwrap().as_ref().unwrap());
    assert!(
        !last
            .shards
            .contains_key("shardId-00000000000000000000-deadbeef")
    );
}
