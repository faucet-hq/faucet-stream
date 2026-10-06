//! `faucet-conformance` battery for the Databricks SQL source.
//!
//! Check 1 — the config JSON Schema is a valid, well-formed value.
//! Check 2 — `stream_pages` pages under a bounded batch size (peak page ≤
//! batch_size, every row streamed) against a wiremock-served result set, i.e.
//! memory stays O(batch_size) regardless of total rows.

use faucet_conformance::{
    assert_bounded_memory, assert_config_schema_valid_value, assert_connector_name_nonempty,
};
use faucet_core::AuthSpec;
use faucet_source_databricks::{
    DatabricksAuth, DatabricksReplication, DatabricksSource, DatabricksSourceConfig,
};
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(DatabricksSourceConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "faucet-source-databricks");
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_bounded_memory() {
    let total = 1_000usize;
    let batch = 100usize;

    let server = MockServer::start().await;
    let rows: Vec<Vec<String>> = (0..total).map(|i| vec![i.to_string()]).collect();
    let body = json!({
        "statement_id": "conf",
        "status": { "state": "SUCCEEDED" },
        "manifest": { "format": "JSON_ARRAY", "schema": {
            "column_count": 1,
            "columns": [{ "name": "id", "position": 0, "type_name": "LONG" }]
        }},
        "result": { "chunk_index": 0, "row_offset": 0, "row_count": total, "data_array": rows }
    });
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(&server)
        .await;

    let config = DatabricksSourceConfig {
        workspace_url: server.uri(),
        warehouse_id: "wh".into(),
        sql: "SELECT id FROM t".into(),
        auth: AuthSpec::Inline(DatabricksAuth::Pat { token: "t".into() }),
        catalog: None,
        schema: None,
        parameters: Vec::new(),
        wait_timeout_secs: 50,
        poll_interval_secs: 1,
        batch_size: batch,
        arrow_native: false,
        result_disposition: Default::default(),
        replication: DatabricksReplication::Full,
        state_key: None,
    };
    let src = DatabricksSource::new(config)
        .unwrap()
        .with_endpoint_base(server.uri());

    // Check 10: connector_name() is non-empty (reuses this instance).
    assert_connector_name_nonempty(&src);

    assert_bounded_memory(&src, batch, total).await;
}
