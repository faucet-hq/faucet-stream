//! `faucet-conformance` battery for the DynamoDB sink (keyed-upsert branch).
//! The live checks boot DynamoDB Local (Docker) and are skipped without it.

mod common;

use aws_sdk_dynamodb::types::ScalarAttributeType;
use common::{config, count, start};
use faucet_conformance::{
    assert_capabilities_truthful, assert_config_schema_valid_value,
    assert_connector_name_nonempty_value, assert_idempotent_replay,
    assert_sink_preflight_check_wellformed, assert_write_modes_truthful,
};
use faucet_core::{DeleteMarker, Sink, WriteMode};
use faucet_sink_dynamodb::{DynamoDbSink, DynamoDbSinkConfig};

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(DynamoDbSinkConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "faucet-sink-dynamodb");
}

async fn keyed_table(client: &aws_sdk_dynamodb::Client, name: &str) {
    use aws_sdk_dynamodb::types::{AttributeDefinition, BillingMode, KeySchemaElement, KeyType};
    client
        .create_table()
        .table_name(name)
        .billing_mode(BillingMode::PayPerRequest)
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("id")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("id")
                .attribute_type(ScalarAttributeType::N)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
}

async fn keyed_sink(endpoint: &str, table: &str) -> DynamoDbSink {
    let mut cfg = config(endpoint, table);
    cfg.write.write_mode = WriteMode::Upsert;
    cfg.write.key = vec!["id".into()];
    cfg.write.delete_marker = Some(DeleteMarker {
        field: faucet_conformance::doubles::DELETE_MARKER_FIELD.into(),
        values: vec![faucet_conformance::doubles::DELETE_MARKER_VALUE.into()],
    });
    DynamoDbSink::new(cfg).await.unwrap()
}

/// The battery keys records on a numeric `id`; each check gets its own table
/// because they count rows from an empty start.
#[tokio::test(flavor = "multi_thread")]
async fn conformance_keyed_battery() {
    let Some((_c, endpoint, client)) = start().await else {
        return;
    };
    for t in ["caps", "modes", "replay", "declared"] {
        keyed_table(&client, t).await;
    }

    let sink = keyed_sink(&endpoint, "caps").await;
    assert_connector_name_nonempty_value(sink.connector_name(), "faucet-sink-dynamodb");
    assert_sink_preflight_check_wellformed(&sink, &faucet_core::check::CheckContext::default())
        .await;
    assert_capabilities_truthful(&sink, || count(&client, "caps")).await;

    let sink = keyed_sink(&endpoint, "modes").await;
    assert_write_modes_truthful(&sink, || count(&client, "modes")).await;

    let sink = keyed_sink(&endpoint, "replay").await;
    assert_idempotent_replay(&sink, || count(&client, "replay")).await;

    // #878: per-row BatchWriteItem outcomes are truthful and a replayed keyed
    // write converges.
    let sink = keyed_sink(&endpoint, "declared").await;
    faucet_conformance::assert_partial_outcomes_truthful(&sink, || count(&client, "declared"))
        .await;
    faucet_conformance::assert_replay_safe(&sink, || count(&client, "declared")).await;
}

/// The static descriptor answers exactly what the built sink declares (RFC 0013).
#[tokio::test(flavor = "multi_thread")]
async fn conformance_descriptor_matches_built_sink() {
    #[allow(unused_mut)]
    let configs = vec![
        r#"{"table_name": "t", "region": "us-east-1", "endpoint_url": "http://127.0.0.1:1", "credentials": {"type": "access_key", "config": {"access_key_id": "t", "secret_access_key": "t"}}}"#,
        r#"{"table_name": "t", "write_mode": "upsert", "key": ["pk"], "region": "us-east-1", "endpoint_url": "http://127.0.0.1:1", "credentials": {"type": "access_key", "config": {"access_key_id": "t", "secret_access_key": "t"}}}"#,
        r#"{"table_name": "t", "write_mode": "delete", "key": ["pk"], "region": "us-east-1", "endpoint_url": "http://127.0.0.1:1", "credentials": {"type": "access_key", "config": {"access_key_id": "t", "secret_access_key": "t"}}}"#,
    ];
    for raw in configs {
        let v: faucet_core::Value = faucet_core::serde_json::from_str(raw).expect("json");
        let cfg: faucet_sink_dynamodb::DynamoDbSinkConfig =
            faucet_core::serde_json::from_value(v.clone()).expect("config");
        let built = faucet_sink_dynamodb::DynamoDbSink::new(cfg)
            .await
            .expect("offline build");
        faucet_conformance::assert_descriptor_matches_sink(
            &faucet_sink_dynamodb::DESCRIPTOR,
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
            tokio::runtime::Handle::current().block_on(DynamoDbSink::new(cfg))
        })
        .expect("sink builds offline")
    };
    faucet_conformance::calls::assert_sink_call_deadline(
        make,
        &[faucet_core::serde_json::json!({"id": 1})],
    )
    .await;
}
