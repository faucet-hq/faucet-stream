//! `faucet-conformance` battery for the Databricks SQL source.
//!
//! Check 1 — the config JSON Schema is a valid, well-formed value.
//! Check 2 — `stream_pages` pages under a bounded batch size (peak page ≤
//! batch_size, every row streamed) against a wiremock-served result set, i.e.
//! memory stays O(batch_size) regardless of total rows.

use faucet_conformance::{
    assert_bounded_memory, assert_config_schema_valid_value, assert_connector_name_nonempty,
    assert_first_page_before_exhaustion,
};
use faucet_core::AuthSpec;
use faucet_source_databricks::{
    DatabricksAuth, DatabricksReplication, DatabricksSource, DatabricksSourceConfig,
};
use serde_json::json;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

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
        statement_timeout_secs: 3600,
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

/// Answers with `body` and counts the rows it has served.
struct Counted {
    body: serde_json::Value,
    rows: usize,
    served: Arc<AtomicUsize>,
}

impl Respond for Counted {
    fn respond(&self, _req: &Request) -> ResponseTemplate {
        self.served.fetch_add(self.rows, Ordering::SeqCst);
        ResponseTemplate::new(200).set_body_json(self.body.clone())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_first_page_before_exhaustion() {
    // Check 22: ten result chunks of 100 rows linked by
    // `next_chunk_internal_link`; the first page must arrive before the
    // last chunk is fetched.
    let (chunks, per_chunk, batch) = (10usize, 100usize, 100usize);
    let server = MockServer::start().await;
    let served = Arc::new(AtomicUsize::new(0));
    let data = |c: usize| -> Vec<Vec<String>> {
        (0..per_chunk)
            .map(|i| vec![(c * per_chunk + i).to_string()])
            .collect()
    };
    let link = |c: usize| format!("/api/2.0/sql/statements/conf/result/chunks/{c}");
    let mut first = json!({
        "statement_id": "conf",
        "status": { "state": "SUCCEEDED" },
        "manifest": { "format": "JSON_ARRAY", "schema": {
            "column_count": 1,
            "columns": [{ "name": "id", "position": 0, "type_name": "LONG" }]
        }},
        "result": { "chunk_index": 0, "data_array": data(0),
                    "next_chunk_index": 1, "next_chunk_internal_link": link(1) }
    });
    first["result"]["row_count"] = json!(per_chunk);
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(Counted {
            body: first,
            rows: per_chunk,
            served: Arc::clone(&served),
        })
        .mount(&server)
        .await;
    for c in 1..chunks {
        let mut body = json!({ "chunk_index": c, "data_array": data(c) });
        if c + 1 < chunks {
            body["next_chunk_index"] = json!(c + 1);
            body["next_chunk_internal_link"] = json!(link(c + 1));
        }
        Mock::given(method("GET"))
            .and(path(link(c)))
            .respond_with(Counted {
                body,
                rows: per_chunk,
                served: Arc::clone(&served),
            })
            .mount(&server)
            .await;
    }
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
        statement_timeout_secs: 3600,
        batch_size: batch,
        arrow_native: false,
        result_disposition: Default::default(),
        replication: DatabricksReplication::Full,
        state_key: None,
    };
    let src = DatabricksSource::new(config)
        .unwrap()
        .with_endpoint_base(server.uri());
    assert_first_page_before_exhaustion(&src, batch, chunks * per_chunk, || {
        served.load(Ordering::SeqCst)
    })
    .await;
}

// ── Descriptor agreement (RFC 0013) ───────────────────────────────────────────

#[test]
fn conformance_descriptor_matches_built_source() {
    let base = json!({
        "workspace_url": "https://x.cloud.databricks.com",
        "warehouse_id": "wh",
        "sql": "SELECT 1",
        "auth": {"type": "pat", "config": {"token": "t"}},
    });
    let mut configs = vec![base.clone()];
    if cfg!(feature = "arrow") {
        let mut arrow = base;
        arrow["arrow_native"] = json!(true);
        configs.push(arrow);
    }
    for v in configs {
        let cfg: DatabricksSourceConfig = serde_json::from_value(v.clone()).unwrap();
        let built = DatabricksSource::new(cfg).expect("offline source");
        faucet_conformance::assert_descriptor_matches_source(
            &faucet_source_databricks::DESCRIPTOR,
            "databricks",
            &v,
            &built,
        );
    }
}

// ── Check: backend calls honour the call deadline and cancel (#880) ──────────

#[tokio::test]
async fn conformance_call_deadline() {
    let hung = faucet_conformance::hung::SilentListener::start()
        .await
        .expect("listener");
    let uri = hung.http_url();
    let make = || {
        let config = DatabricksSourceConfig {
            workspace_url: uri.clone(),
            warehouse_id: "wh".into(),
            sql: "SELECT id FROM t".into(),
            auth: AuthSpec::Inline(DatabricksAuth::Pat { token: "t".into() }),
            catalog: None,
            schema: None,
            parameters: Vec::new(),
            wait_timeout_secs: 50,
            poll_interval_secs: 1,
            statement_timeout_secs: 3600,
            batch_size: 100,
            arrow_native: false,
            result_disposition: Default::default(),
            replication: DatabricksReplication::Full,
            state_key: None,
        };
        DatabricksSource::new(config).unwrap()
    };
    faucet_conformance::calls::assert_source_call_deadline(make).await;
}
