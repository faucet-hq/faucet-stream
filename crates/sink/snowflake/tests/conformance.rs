//! `faucet-conformance` battery — Check 1 (config schema validity) and Check 10
//! (connector_name non-empty). Passing this battery in CI is the Tier-1
//! (supported) criterion.
use faucet_conformance::{assert_config_schema_valid_value, assert_connector_name_nonempty_value};
use faucet_sink_snowflake::{Sink as _, SnowflakeAuth, SnowflakeSink, SnowflakeSinkConfig};

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(SnowflakeSinkConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "snowflake");
}

// ── Check 10: connector_name is non-empty (offline, lazy sink) ───────────────
#[test]
fn conformance_connector_name_nonempty() {
    let sink = SnowflakeSink::new(SnowflakeSinkConfig::new(
        "xy12345",
        "WH",
        "DB",
        "PUBLIC",
        "t",
        SnowflakeAuth::OAuth { token: "t".into() },
    ))
    .expect("sink builds lazily");
    faucet_conformance::assert_batch_atomicity_declared(&sink);
    assert_connector_name_nonempty_value(sink.connector_name(), sink.connector_name());
    assert_eq!(sink.connector_name(), "snowflake");
}

// ── Descriptor agreement (RFC 0013) ───────────────────────────────────────────
#[test]
fn conformance_descriptor_matches_built_sink() {
    let base = serde_json::json!({
        "account": "xy12345",
        "warehouse": "WH",
        "database": "DB",
        "schema": "PUBLIC",
        "table": "t",
        "auth": {"type": "oauth", "config": {"token": "t"}},
    });
    let mut configs = vec![base.clone()];
    if cfg!(feature = "arrow") {
        let mut bulk = base;
        bulk["bulk_load"] = serde_json::json!({"stage": "s", "url": "s3://bucket/prefix"});
        configs.push(bulk);
    }
    for v in configs {
        let cfg: SnowflakeSinkConfig = serde_json::from_value(v.clone()).unwrap();
        let built = SnowflakeSink::new(cfg).expect("sink builds lazily");
        faucet_conformance::assert_descriptor_matches_sink(
            &faucet_sink_snowflake::DESCRIPTOR,
            "snowflake",
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
        SnowflakeSink::new(SnowflakeSinkConfig::new(
            "xy12345",
            "WH",
            "DB",
            "PUBLIC",
            "t",
            SnowflakeAuth::OAuth { token: "t".into() },
        ))
        .expect("sink builds lazily")
        .with_endpoint(uri.clone())
    };
    faucet_conformance::calls::assert_sink_call_deadline(make, &[serde_json::json!({"id": 1})])
        .await;
}

// ── #878: declared capabilities against a stateful SQL-API fake ──────────────

/// A statements endpoint that keeps what the sink wrote: the rows of every
/// INSERT (inline `PARSE_JSON('…')` literals or the bound JSON payload) and the
/// commit token of every watermark `MERGE`, answering the token `SELECT` from
/// that store. A statement holding [`POISON`] fails as a whole, the way a
/// warehouse rejects a statement with a bad row — so a sink that split a page
/// declared atomic into several statements would land its other parts.
#[derive(Clone, Default)]
struct Warehouse {
    rows: std::sync::Arc<std::sync::Mutex<usize>>,
    tokens: std::sync::Arc<std::sync::Mutex<std::collections::HashMap<String, String>>>,
}

const POISON: &str = "__poison__";

fn json_array_len(text: &str) -> usize {
    serde_json::from_str::<Vec<serde_json::Value>>(text)
        .map(|v| v.len())
        .unwrap_or(0)
}

fn inline_rows(sql: &str) -> usize {
    sql.split("PARSE_JSON('")
        .skip(1)
        .map(|rest| json_array_len(&rest[..rest.find("')").unwrap_or(0)].replace("''", "'")))
        .sum()
}

/// The `'…'` literal right before `alias` in `sql`.
fn quoted_before(sql: &str, alias: &str) -> Option<String> {
    let end = sql.find(alias)?;
    let head = sql[..end].trim_end().strip_suffix('\'')?;
    let start = head.rfind('\'')?;
    Some(head[start + 1..].to_string())
}

impl wiremock::Respond for Warehouse {
    fn respond(&self, req: &wiremock::Request) -> wiremock::ResponseTemplate {
        let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
        let sql = body["statement"].as_str().unwrap_or_default();
        let ok = |data: serde_json::Value| {
            wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "code": "090001", "message": "ok", "data": data
            }))
        };
        if body.to_string().contains(POISON) {
            return wiremock::ResponseTemplate::new(422)
                .set_body_json(serde_json::json!({"code": "100038", "message": "bad row"}));
        }
        if sql.contains("information_schema.columns") {
            return ok(serde_json::json!([["id", "NUMBER"], ["v", "TEXT"]]));
        }
        if sql.starts_with("SELECT") && sql.contains("_faucet_commit_token") {
            let scope = body["bindings"]["1"]["value"].as_str().unwrap_or_default();
            let token = self.tokens.lock().unwrap().get(scope).cloned();
            return ok(serde_json::json!(
                token.map(|t| vec![vec![t]]).unwrap_or_default()
            ));
        }
        if sql.contains("INSERT INTO") {
            let n = inline_rows(sql)
                + body["bindings"]["1"]["value"]
                    .as_str()
                    .map(json_array_len)
                    .unwrap_or(0);
            *self.rows.lock().unwrap() += n;
        }
        if sql.contains("MERGE INTO") && sql.contains("_faucet_commit_token") {
            let scope = quoted_before(sql, "AS \"scope\"");
            let token = quoted_before(sql, "AS \"token\"");
            if let (Some(scope), Some(token)) = (scope, token) {
                self.tokens.lock().unwrap().insert(scope, token);
            }
        }
        ok(serde_json::json!([]))
    }
}

async fn warehouse_sink(batch_size: usize) -> (wiremock::MockServer, Warehouse, SnowflakeSink) {
    let server = wiremock::MockServer::start().await;
    let wh = Warehouse::default();
    wiremock::Mock::given(wiremock::matchers::method("POST"))
        .and(wiremock::matchers::path("/api/v2/statements"))
        .respond_with(wh.clone())
        .mount(&server)
        .await;
    let mut cfg = SnowflakeSinkConfig::new(
        "xy12345",
        "WH",
        "DB",
        "PUBLIC",
        "t",
        SnowflakeAuth::OAuth { token: "t".into() },
    );
    cfg.create_table = false;
    cfg.batch_size = batch_size;
    let sink = SnowflakeSink::new(cfg)
        .unwrap()
        .with_endpoint(format!("{}/api/v2/statements", server.uri()));
    (server, wh, sink)
}

#[tokio::test]
async fn conformance_declared_capabilities() {
    let (_server, wh, sink) = warehouse_sink(1000).await;
    let count = || {
        let wh = wh.clone();
        async move { *wh.rows.lock().unwrap() }
    };
    faucet_conformance::assert_idempotent_replay(&sink, count).await;
    faucet_conformance::assert_partial_outcomes_truthful(&sink, count).await;
    faucet_conformance::assert_cancellation_flushes(&sink, count).await;
    faucet_conformance::assert_driver_declarations_truthful(&sink).await;
    faucet_conformance::assert_driver_row_paths_agree(&sink).await;

    let (_server, wh, atomic) = warehouse_sink(0).await;
    let count = || {
        let wh = wh.clone();
        async move { *wh.rows.lock().unwrap() }
    };
    faucet_conformance::assert_batch_atomicity_truthful(
        &atomic,
        count,
        &[
            serde_json::json!({"id": 955, "v": "kept only if the batch is torn"}),
            serde_json::json!({"id": 956, "v": POISON}),
        ],
    )
    .await;
}
