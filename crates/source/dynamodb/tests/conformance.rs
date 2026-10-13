//! `faucet-conformance` battery for the DynamoDB source. Check 1 and the
//! error check run offline; the rest boot DynamoDB Local (Docker) and are
//! skipped when Docker is unavailable.

mod common;

use aws_sdk_dynamodb::types::ScalarAttributeType;
use common::{config, create_table, item, n, put_items, s, start};
use faucet_conformance::{
    assert_batch_size_zero_single_page, assert_bookmark_roundtrip, assert_bounded_memory,
    assert_config_schema_valid_value, assert_connector_name_nonempty, assert_discover_roundtrips,
    assert_errors_not_panics, assert_preflight_check_wellformed, merge_config_patch,
};
use faucet_core::Source;
use faucet_source_dynamodb::{DynamoDbSource, DynamoDbSourceConfig, ReadMode};

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(DynamoDbSourceConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "faucet-source-dynamodb");
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_errors_not_panics() {
    let mut cfg = config("http://127.0.0.1:1", "missing");
    cfg.retry.max_retries = 0;
    let source = DynamoDbSource::new(cfg).await.expect("lazy");
    assert_connector_name_nonempty(&source);
    assert_errors_not_panics(&source).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_scan_battery() {
    let Some((_c, endpoint, client)) = start().await else {
        return;
    };
    create_table(&client, "conf", ScalarAttributeType::N, false, false).await;
    put_items(
        &client,
        "conf",
        (0..2_000)
            .map(|i| item(vec![("pk", n(i)), ("v", s("x"))]))
            .collect(),
    )
    .await;

    let mut cfg = config(&endpoint, "conf");
    cfg.segments = 3;
    cfg.batch_size = 250;
    let source = DynamoDbSource::new(cfg.clone()).await.unwrap();
    assert_preflight_check_wellformed(&source, &faucet_core::check::CheckContext::default()).await;
    assert_bounded_memory(&source, 250, 2_000).await;

    cfg.batch_size = 0;
    let whole = DynamoDbSource::new(cfg.clone()).await.unwrap();
    assert_batch_size_zero_single_page(&whole).await;

    let base = serde_json::to_value(&cfg).unwrap();
    assert_discover_roundtrips(&source, |patch| {
        let merged = merge_config_patch(base.clone(), &patch);
        async move {
            let cfg: DynamoDbSourceConfig = serde_json::from_value(merged).unwrap();
            Box::new(DynamoDbSource::new(cfg).await.unwrap()) as Box<dyn Source>
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_streams_bookmark_roundtrip() {
    let Some((_c, endpoint, client)) = start().await else {
        return;
    };
    create_table(&client, "cdc", ScalarAttributeType::S, false, true).await;
    for i in 0..300 {
        client
            .put_item()
            .table_name("cdc")
            .set_item(Some(item(vec![("pk", s(&format!("k{i}")))])))
            .send()
            .await
            .unwrap();
    }
    let mut cfg = config(&endpoint, "cdc");
    cfg.mode = ReadMode::Streams;
    cfg.idle_termination_secs = Some(3);
    cfg.batch_size = 100;
    let source = DynamoDbSource::new(cfg).await.unwrap();
    assert_preflight_check_wellformed(&source, &faucet_core::check::CheckContext::default()).await;
    assert_bookmark_roundtrip(&source).await;
}

/// The static descriptor answers exactly what the built source declares (RFC 0013).
#[tokio::test(flavor = "multi_thread")]
async fn conformance_descriptor_matches_built_source() {
    #[allow(unused_mut)]
    let configs = vec![
        r#"{"table_name": "t", "region": "us-east-1", "endpoint_url": "http://127.0.0.1:1", "credentials": {"type": "access_key", "config": {"access_key_id": "t", "secret_access_key": "t"}}}"#,
        r#"{"table_name": "t", "mode": "query", "key_condition_expression": "pk = :p", "expression_attribute_values": {":p": "x"}, "region": "us-east-1", "endpoint_url": "http://127.0.0.1:1", "credentials": {"type": "access_key", "config": {"access_key_id": "t", "secret_access_key": "t"}}}"#,
        r#"{"table_name": "t", "mode": "streams", "idle_termination_secs": 1, "region": "us-east-1", "endpoint_url": "http://127.0.0.1:1", "credentials": {"type": "access_key", "config": {"access_key_id": "t", "secret_access_key": "t"}}}"#,
    ];
    for raw in configs {
        let v: faucet_core::Value = faucet_core::serde_json::from_str(raw).expect("json");
        let cfg: faucet_source_dynamodb::DynamoDbSourceConfig =
            faucet_core::serde_json::from_value(v.clone()).expect("config");
        let built = faucet_source_dynamodb::DynamoDbSource::new(cfg)
            .await
            .expect("offline build");
        faucet_conformance::assert_descriptor_matches_source(
            &faucet_source_dynamodb::DESCRIPTOR,
            "dynamodb",
            &v,
            &built,
        );
    }
}

// ── Check: backend calls honour the call deadline and cancel (#880) ──────────

#[tokio::test(flavor = "multi_thread")]
async fn conformance_call_deadline() {
    let hung = faucet_conformance::hung::SilentListener::start()
        .await
        .expect("listener");
    let endpoint = hung.http_url();
    let make = || {
        let cfg = config(&endpoint, "hung");
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(DynamoDbSource::new(cfg))
        })
        .expect("source builds offline")
    };
    faucet_conformance::calls::assert_source_call_deadline(make).await;
}
