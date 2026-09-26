//! Integration tests for `DynamoDbSink` against DynamoDB Local (Docker).
//! Each test boots its own container; skipped when Docker is unavailable.

mod common;

use aws_sdk_dynamodb::types::ScalarAttributeType;
use common::{config, count, create_table, get, item, n, s, start};
use faucet_core::{DeleteMarker, Sink, WriteMode};
use faucet_sink_dynamodb::{DynamoDbSink, OnConditionFailure};
use serde_json::{Value, json};

#[tokio::test(flavor = "multi_thread")]
async fn writes_10k_items_and_routes_invalid_rows_to_the_dlq() {
    let Some((_c, endpoint, client)) = start().await else {
        return;
    };
    create_table(&client, "bulk", ScalarAttributeType::S, false, false).await;
    let mut cfg = config(&endpoint, "bulk");
    cfg.concurrency = 8;
    let sink = DynamoDbSink::new(cfg).await.unwrap();

    let mut records: Vec<Value> = (0..10_000)
        .map(|i| json!({"pk": format!("k{i:05}"), "v": i, "nested": {"a": [1, 2]}}))
        .collect();
    let bad = [
        (137usize, json!({"pk": 42})),
        (4_321, json!({"v": 1})),
        (9_000, json!({"pk": ""})),
    ];
    for (at, rec) in &bad {
        records.insert(*at, rec.clone());
    }
    let mut failed = Vec::new();
    let mut offset = 0;
    for page in records.chunks(1_000) {
        let outcomes = sink.write_batch_partial(page).await.unwrap();
        assert_eq!(outcomes.len(), page.len());
        for (i, o) in outcomes.iter().enumerate() {
            if o.is_err() {
                failed.push(offset + i);
            }
        }
        offset += page.len();
    }
    assert_eq!(failed, bad.iter().map(|(i, _)| *i).collect::<Vec<_>>());
    assert_eq!(count(&client, "bulk").await, 10_000);
    let one = get(&client, "bulk", item(vec![("pk", s("k00007"))]))
        .await
        .unwrap();
    assert_eq!(one["v"], n(7));

    let err = sink
        .write_batch(&[json!({"pk": "ok"}), json!({"pk": 1})])
        .await
        .unwrap_err();
    assert!(err.to_string().contains("rejected"), "{err}");
    let err = sink.write_batch(&[json!({"nope": 1})]).await.unwrap_err();
    assert!(err.to_string().contains("missing key attribute"), "{err}");
    assert_eq!(sink.write_batch(&[json!({"pk": "fine"})]).await.unwrap(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn upsert_delete_and_conditional_writes() {
    let Some((_c, endpoint, client)) = start().await else {
        return;
    };
    create_table(&client, "kvstore", ScalarAttributeType::S, true, false).await;

    let mut up = config(&endpoint, "kvstore");
    up.write.write_mode = WriteMode::Upsert;
    up.write.key = vec!["sk".into(), "pk".into()];
    up.write.delete_marker = Some(DeleteMarker {
        field: "__op".into(),
        values: vec!["d".into()],
    });
    let sink = DynamoDbSink::new(up.clone()).await.unwrap();
    assert!(sink.dedups_by_key());
    sink.write_batch(&[
        json!({"pk": "a", "sk": 1, "v": "one", "__op": "u"}),
        json!({"pk": "a", "sk": 2, "v": "two"}),
        json!({"pk": "a", "sk": 1, "v": "uno"}),
    ])
    .await
    .unwrap();
    assert_eq!(count(&client, "kvstore").await, 2);
    let a1 = get(&client, "kvstore", item(vec![("pk", s("a")), ("sk", n(1))]))
        .await
        .unwrap();
    assert_eq!(a1["v"], s("uno"));
    assert!(!a1.contains_key("__op"));
    sink.write_batch(&[json!({"pk": "a", "sk": 2, "__op": "d"})])
        .await
        .unwrap();
    assert_eq!(count(&client, "kvstore").await, 1);

    let mut del = config(&endpoint, "kvstore");
    del.write.write_mode = WriteMode::Delete;
    del.write.key = vec!["pk".into(), "sk".into()];
    DynamoDbSink::new(del)
        .await
        .unwrap()
        .write_batch(&[json!({"pk": "a", "sk": 1, "ignored": true})])
        .await
        .unwrap();
    assert_eq!(count(&client, "kvstore").await, 0);

    let mut wrong = up.clone();
    wrong.write.key = vec!["pk".into()];
    let err = DynamoDbSink::new(wrong)
        .await
        .unwrap()
        .write_batch(&[json!({"pk": "a", "sk": 1})])
        .await
        .unwrap_err();
    assert!(err.to_string().contains("table key schema"), "{err}");

    let mut cond = config(&endpoint, "kvstore");
    cond.condition_expression = Some("attribute_not_exists(#p)".into());
    cond.expression_attribute_names
        .insert("#p".into(), "pk".into());
    let skip = DynamoDbSink::new(cond.clone()).await.unwrap();
    skip.write_batch(&[json!({"pk": "c", "sk": 1, "v": "first"})])
        .await
        .unwrap();
    skip.write_batch(&[json!({"pk": "c", "sk": 1, "v": "second"})])
        .await
        .unwrap();
    let c1 = get(&client, "kvstore", item(vec![("pk", s("c")), ("sk", n(1))]))
        .await
        .unwrap();
    assert_eq!(c1["v"], s("first"), "a failed condition skips the row");

    cond.on_condition_failure = OnConditionFailure::Fail;
    let fail = DynamoDbSink::new(cond).await.unwrap();
    let outcomes = fail
        .write_batch_partial(&[
            json!({"pk": "c", "sk": 1, "v": "third"}),
            json!({"pk": "d", "sk": 1, "v": "new"}),
            json!({"pk": "e", "sk": "not-a-number"}),
        ])
        .await
        .unwrap();
    assert!(
        outcomes[0]
            .as_ref()
            .unwrap_err()
            .to_string()
            .contains("condition")
    );
    assert!(outcomes[1].is_ok());
    assert!(
        outcomes[2]
            .as_ref()
            .unwrap_err()
            .to_string()
            .contains("rejected")
    );
    assert_eq!(count(&client, "kvstore").await, 2);

    let report = fail
        .check(&faucet_core::CheckContext::default())
        .await
        .unwrap();
    assert_eq!(report.failed_count(), 0);
    let missing = DynamoDbSink::new(config(&endpoint, "no_such_table"))
        .await
        .unwrap();
    let report = missing
        .check(&faucet_core::CheckContext::default())
        .await
        .unwrap();
    assert_eq!(report.failed_count(), 1);
}
