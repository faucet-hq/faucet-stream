//! `faucet-conformance` battery — Check 1 (config schema validity).
//! Passing this battery in CI is the Tier-1 (supported) criterion.
use faucet_conformance::assert_config_schema_valid_value;
use faucet_sink_bigquery::{BigQuerySink, BigQuerySinkConfig};
use gcp_bigquery_client::client_builder::ClientBuilder;
use serde_json::json;

const AUTH_TOKEN_PATH: &str = "/:o/oauth2/token";
const AUTH_SCOPE_BASE: &str = "/auth/bigquery";

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(
        faucet_sink_bigquery::BigQuerySinkConfig
    ))
    .unwrap();
    assert_config_schema_valid_value(&schema, "bigquery");
}

// ── Descriptor agreement (RFC 0013) ───────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn conformance_descriptor_matches_built_sink() {
    const UNREACHABLE: &str = "http://127.0.0.1:1";
    let sa_file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(
        sa_file.path(),
        serde_json::to_string_pretty(&dummy_service_account_json(UNREACHABLE)).unwrap(),
    )
    .unwrap();
    let base = json!({
        "project_id": "p",
        "dataset_id": "d",
        "table_id": "t",
        "auth": {"type": "application_default"},
    });
    let mut configs = vec![base.clone()];
    for extra in [
        json!({"write_mode": "overwrite"}),
        json!({"write_mode": "upsert", "key": ["id"]}),
        json!({"write_mode": "delete", "key": ["id"]}),
    ] {
        let mut v = base.clone();
        for (k, val) in extra.as_object().unwrap() {
            v[k] = val.clone();
        }
        configs.push(v);
    }
    for v in configs {
        let cfg: BigQuerySinkConfig = serde_json::from_value(v.clone()).unwrap();
        let client = ClientBuilder::new()
            .with_auth_base_url(format!("{UNREACHABLE}{AUTH_SCOPE_BASE}"))
            .with_v2_base_url(UNREACHABLE.to_string())
            .build_from_service_account_key_file(sa_file.path().to_str().unwrap())
            .await
            .expect("offline client");
        let built = BigQuerySink::from_parts(cfg, client);
        faucet_conformance::assert_descriptor_matches_sink(
            &faucet_sink_bigquery::DESCRIPTOR,
            "bigquery",
            &v,
            &built,
        );
    }
}

fn dummy_service_account_json(oauth_server: &str) -> serde_json::Value {
    let token_uri = format!("{oauth_server}{AUTH_TOKEN_PATH}");
    json!({
        "type": "service_account",
        "project_id": "dummy",
        "private_key_id": "dummy",
        "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQDNk6cKkWP/4NMu\nWb3s24YHfM639IXzPtTev06PUVVQnyHmT1bZgQ/XB6BvIRaReqAqnQd61PAGtX3e\n8XocTw+u/ZfiPJOf+jrXMkRBpiBh9mbyEIqBy8BC20OmsUc+O/YYh/qRccvRfPI7\n3XMabQ8eFWhI6z/t35oRpvEVFJnSIgyV4JR/L/cjtoKnxaFwjBzEnxPiwtdy4olU\nKO/1maklXexvlO7onC7CNmPAjuEZKzdMLzFszikCDnoKJC8k6+2GZh0/JDMAcAF4\nwxlKNQ89MpHVRXZ566uKZg0MqZqkq5RXPn6u7yvNHwZ0oahHT+8ixPPrAEjuPEKM\nUPzVRz71AgMBAAECggEAfdbVWLW5Befkvam3hea2+5xdmeN3n3elrJhkiXxbAhf3\nE1kbq9bCEHmdrokNnI34vz0SWBFCwIiWfUNJ4UxQKGkZcSZto270V8hwWdNMXUsM\npz6S2nMTxJkdp0s7dhAUS93o9uE2x4x5Z0XecJ2ztFGcXY6Lupu2XvnW93V9109h\nkY3uICLdbovJq7wS/fO/AL97QStfEVRWW2agIXGvoQG5jOwfPh86GZZRYP9b8VNw\ntkAUJe4qpzNbWs9AItXOzL+50/wsFkD/iWMGWFuU8DY5ZwsL434N+uzFlaD13wtZ\n63D+tNAxCSRBfZGQbd7WxJVFfZe/2vgjykKWsdyNAQKBgQDnEBgSI836HGSRk0Ub\nDwiEtdfh2TosV+z6xtyU7j/NwjugTOJEGj1VO/TMlZCEfpkYPLZt3ek2LdNL66n8\nDyxwzTT5Q3D/D0n5yE3mmxy13Qyya6qBYvqqyeWNwyotGM7hNNOix1v9lEMtH5Rd\nUT0gkThvJhtrV663bcAWCALmtQKBgQDjw2rYlMUp2TUIa2/E7904WOnSEG85d+nc\norhzthX8EWmPgw1Bbfo6NzH4HhebTw03j3NjZdW2a8TG/uEmZFWhK4eDvkx+rxAa\n6EwamS6cmQ4+vdep2Ac4QCSaTZj02YjHb06Be3gptvpFaFrotH2jnpXxggdiv8ul\n6x+ooCffQQKBgQCR3ykzGoOI6K/c75prELyR+7MEk/0TzZaAY1cSdq61GXBHLQKT\nd/VMgAN1vN51pu7DzGBnT/dRCvEgNvEjffjSZdqRmrAVdfN/y6LSeQ5RCfJgGXSV\nJoWVmMxhCNrxiX3h01Xgp/c9SYJ3VD54AzeR/dwg32/j/oEAsDraLciXGQKBgQDF\nMNc8k/DvfmJv27R06Ma6liA6AoiJVMxgfXD8nVUDW3/tBCVh1HmkFU1p54PArvxe\nchAQqoYQ3dUMBHeh6ZRJaYp2ATfxJlfnM99P1/eHFOxEXdBt996oUMBf53bZ5cyJ\n/lAVwnQSiZy8otCyUDHGivJ+mXkTgcIq8BoEwERFAQKBgQDmImBaFqoMSVihqHIf\nDa4WZqwM7ODqOx0JnBKrKO8UOc51J5e1vpwP/qRpNhUipoILvIWJzu4efZY7GN5C\nImF9sN3PP6Sy044fkVPyw4SYEisxbvp9tfw8Xmpj/pbmugkB2ut6lz5frmEBoJSN\n3osZlZTgx+pM3sO6ITV6U4ID2Q==\n-----END PRIVATE KEY-----\n",
        "client_email": "dummy@developer.gserviceaccount.com",
        "client_id": "dummy",
        "auth_uri": format!("{oauth_server}/o/oauth2/auth"),
        "token_uri": token_uri,
        "auth_provider_x509_cert_url": format!("{oauth_server}/oauth2/v1/certs"),
        "client_x509_cert_url": format!("{oauth_server}/robot/v1/metadata/x509/dummy"),
    })
}

// ── Check: backend calls honour the call deadline and cancel (#880) ──────────

#[tokio::test(flavor = "multi_thread")]
async fn conformance_call_deadline() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(AUTH_TOKEN_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "access_token": "fake-token",
            "token_type": "bearer",
            "expires_in": 9_999_999,
        })))
        .mount(&server)
        .await;
    Mock::given(wiremock::matchers::any())
        .respond_with(ResponseTemplate::new(200).set_delay(std::time::Duration::from_secs(60)))
        .mount(&server)
        .await;
    let sa_file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(
        sa_file.path(),
        serde_json::to_string_pretty(&dummy_service_account_json(&server.uri())).unwrap(),
    )
    .unwrap();
    let make = || {
        let config = BigQuerySinkConfig::new(
            "p",
            "d",
            "t",
            faucet_sink_bigquery::BigQueryCredentials::ApplicationDefault,
        )
        .with_media_load(false);
        let client = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(
                ClientBuilder::new()
                    .with_auth_base_url(format!("{}{AUTH_SCOPE_BASE}", server.uri()))
                    .with_v2_base_url(server.uri())
                    .build_from_service_account_key_file(sa_file.path().to_str().unwrap()),
            )
        })
        .expect("client against the mock");
        BigQuerySink::from_parts(config, client)
    };
    faucet_conformance::calls::assert_sink_call_deadline(make, &[json!({"id": 1})]).await;
}

// ── #878: declared capabilities against a stateful BigQuery fake ─────────────

mod fake {
    use std::collections::{HashMap, HashSet};
    use std::io::Read as _;
    use std::sync::{Arc, Mutex};

    use serde_json::{Value, json};
    use wiremock::{Request, Respond, ResponseTemplate};

    pub const PROJECT: &str = "p";
    pub const POISON: &str = "__poison__";

    /// The parts of BigQuery the sink talks to, kept in memory: appended rows
    /// (streaming `insertAll`, the resumable NDJSON load finalized at flush, the
    /// exactly-once transaction's typed `INSERT`), keys written by a keyed
    /// `MERGE`, the watermark `MERGE` + `SELECT`, and the table schema that
    /// `ADD COLUMN` grows. A request carrying [`POISON`] fails as a whole.
    #[derive(Clone, Default)]
    pub struct BigQuery {
        appended: Arc<Mutex<usize>>,
        keyed: Arc<Mutex<HashSet<String>>>,
        tokens: Arc<Mutex<HashMap<String, String>>>,
        extra_columns: Arc<Mutex<Vec<String>>>,
        upload: Arc<Mutex<Vec<u8>>>,
        base: Arc<Mutex<String>>,
    }

    impl BigQuery {
        pub fn new(base: &str) -> Self {
            let s = Self::default();
            *s.base.lock().unwrap() = base.to_string();
            s
        }

        pub fn len(&self) -> usize {
            *self.appended.lock().unwrap() + self.keyed.lock().unwrap().len()
        }
    }

    fn param(body: &Value, name: &str) -> Option<String> {
        body["queryParameters"]
            .as_array()?
            .iter()
            .find(|p| p["name"] == name)?["parameterValue"]["value"]
            .as_str()
            .map(str::to_string)
    }

    fn payload(body: &Value) -> Vec<Value> {
        param(body, "payload")
            .and_then(|p| serde_json::from_str(&p).ok())
            .unwrap_or_default()
    }

    fn done(job: &str) -> ResponseTemplate {
        ResponseTemplate::new(200).set_body_json(json!({
            "kind": "bigquery#queryResponse",
            "jobComplete": true,
            "jobReference": {"projectId": PROJECT, "jobId": job}
        }))
    }

    impl Respond for BigQuery {
        fn respond(&self, req: &Request) -> ResponseTemplate {
            let p = req.url.path().to_string();
            let gzipped = req
                .headers
                .get("content-encoding")
                .is_some_and(|v| v.as_bytes() == b"gzip");
            let text = if gzipped {
                let mut out = String::new();
                flate2::read::GzDecoder::new(req.body.as_slice())
                    .read_to_string(&mut out)
                    .unwrap_or_default();
                out
            } else {
                String::from_utf8_lossy(&req.body).to_string()
            };
            if text.contains(POISON) {
                return ResponseTemplate::new(400)
                    .set_body_json(json!({"error": {"code": 400, "message": "bad row"}}));
            }
            if p.ends_with("/oauth2/token") {
                return ResponseTemplate::new(200).set_body_json(json!({
                    "access_token": "fake-token", "token_type": "bearer", "expires_in": 9_999_999
                }));
            }
            if req.method.as_str() == "GET" && p.contains("/jobs/") {
                return ResponseTemplate::new(200).set_body_json(json!({
                    "jobReference": {"projectId": PROJECT, "jobId": "j"},
                    "status": {"state": "DONE"}
                }));
            }
            if req.method.as_str() == "GET" && p.contains("/tables/") {
                let mut fields = vec![
                    json!({"name": "id", "type": "INTEGER", "mode": "NULLABLE"}),
                    json!({"name": "v", "type": "STRING", "mode": "NULLABLE"}),
                ];
                for c in self.extra_columns.lock().unwrap().iter() {
                    fields.push(json!({"name": c, "type": "STRING", "mode": "NULLABLE"}));
                }
                return ResponseTemplate::new(200).set_body_json(json!({
                    "tableReference": {"projectId": PROJECT, "datasetId": "d", "tableId": "t"},
                    "schema": {"fields": fields}
                }));
            }
            if p.ends_with("/insertAll") {
                let body: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
                let n = body["rows"].as_array().map_or(0, Vec::len);
                *self.appended.lock().unwrap() += n;
                return ResponseTemplate::new(200)
                    .set_body_json(json!({"kind": "bigquery#tableDataInsertAllResponse"}));
            }
            if p.contains("/upload/bigquery") {
                self.upload.lock().unwrap().clear();
                let session = format!("{}/session/conformance", self.base.lock().unwrap());
                return ResponseTemplate::new(200).insert_header("location", session.as_str());
            }
            if p.starts_with("/session/") {
                self.upload.lock().unwrap().extend_from_slice(&req.body);
                let range = req
                    .headers
                    .get("content-range")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or_default()
                    .to_string();
                if range.ends_with("/*") {
                    let len = self.upload.lock().unwrap().len();
                    return ResponseTemplate::new(308)
                        .insert_header("range", format!("bytes=0-{}", len - 1).as_str());
                }
                let gz = std::mem::take(&mut *self.upload.lock().unwrap());
                let mut ndjson = String::new();
                flate2::read::GzDecoder::new(gz.as_slice())
                    .read_to_string(&mut ndjson)
                    .unwrap_or_default();
                if ndjson.contains(POISON) {
                    return ResponseTemplate::new(400);
                }
                *self.appended.lock().unwrap() +=
                    ndjson.lines().filter(|l| !l.trim().is_empty()).count();
                return ResponseTemplate::new(200).set_body_json(json!({
                    "jobReference": {"projectId": PROJECT, "jobId": "load"},
                    "status": {"state": "DONE"}
                }));
            }
            if p.ends_with("/queries") {
                let body: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
                let q = body["query"].as_str().unwrap_or_default();
                if q.starts_with("SELECT") && q.contains("_faucet_commit_token") {
                    let scope = param(&body, "scope").unwrap_or_default();
                    let rows: Vec<Value> = self
                        .tokens
                        .lock()
                        .unwrap()
                        .get(&scope)
                        .map(|t| vec![json!({"f": [{"v": t}]})])
                        .unwrap_or_default();
                    return ResponseTemplate::new(200).set_body_json(json!({
                        "kind": "bigquery#queryResponse",
                        "jobComplete": true,
                        "jobReference": {"projectId": PROJECT, "jobId": "sel"},
                        "schema": {"fields": [{"name": "token", "type": "STRING"}]},
                        "totalRows": rows.len().to_string(),
                        "rows": rows
                    }));
                }
                if let Some(rest) = q.split("ADD COLUMN IF NOT EXISTS `").nth(1) {
                    let name = rest.split('`').next().unwrap_or_default().to_string();
                    self.extra_columns.lock().unwrap().push(name);
                }
                let rows = payload(&body);
                if q.contains("INSERT INTO `p.d.t`") {
                    *self.appended.lock().unwrap() += rows.len();
                } else if q.contains("MERGE `p.d.t`") {
                    let mut keyed = self.keyed.lock().unwrap();
                    for r in rows {
                        keyed.insert(r["id"].to_string());
                    }
                }
                if q.contains("_faucet_commit_token")
                    && let (Some(scope), Some(token)) =
                        (param(&body, "scope"), param(&body, "token"))
                {
                    self.tokens.lock().unwrap().insert(scope, token);
                }
                return done("q");
            }
            ResponseTemplate::new(404)
        }
    }
}

async fn fake_sink(
    edit: impl FnOnce(&mut BigQuerySinkConfig),
) -> (wiremock::MockServer, fake::BigQuery, BigQuerySink) {
    let server = wiremock::MockServer::start().await;
    let bq = fake::BigQuery::new(&server.uri());
    wiremock::Mock::given(wiremock::matchers::any())
        .respond_with(bq.clone())
        .mount(&server)
        .await;
    let sa = dummy_service_account_json(&server.uri());
    let mut cfg = BigQuerySinkConfig::new(
        fake::PROJECT,
        "d",
        "t",
        faucet_sink_bigquery::BigQueryCredentials::ServiceAccountKey {
            json: serde_json::to_string(&sa).unwrap(),
        },
    );
    cfg.upload_base_url = Some(server.uri());
    edit(&mut cfg);
    let sa_file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(sa_file.path(), serde_json::to_string_pretty(&sa).unwrap()).unwrap();
    let client = ClientBuilder::new()
        .with_auth_base_url(format!("{}{AUTH_SCOPE_BASE}", server.uri()))
        .with_v2_base_url(server.uri())
        .build_from_service_account_key_file(sa_file.path().to_str().unwrap())
        .await
        .expect("client against the fake");
    std::mem::forget(sa_file);
    (server, bq, BigQuerySink::from_parts(cfg, client))
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_declared_capabilities() {
    let (_s, bq, sink) = fake_sink(|_| {}).await;
    let count = || {
        let bq = bq.clone();
        async move { bq.len() }
    };
    faucet_conformance::assert_idempotent_replay(&sink, count).await;
    faucet_conformance::assert_cancellation_flushes(&sink, count).await;
    faucet_conformance::assert_schema_evolution_effective(&sink).await;

    let (_s, bq, streaming) = fake_sink(|c| c.media_load = false).await;
    let count = || {
        let bq = bq.clone();
        async move { bq.len() }
    };
    faucet_conformance::assert_partial_outcomes_truthful(&streaming, count).await;

    let (_s, bq, keyed) = fake_sink(|c| {
        c.write.write_mode = faucet_core::WriteMode::Upsert;
        c.write.key = vec!["id".into()];
    })
    .await;
    let count = || {
        let bq = bq.clone();
        async move { bq.len() }
    };
    faucet_conformance::assert_batch_atomicity_truthful(
        &keyed,
        count,
        &[
            json!({"id": 957, "v": "kept only if the batch is torn"}),
            json!({"id": 958, "v": fake::POISON}),
        ],
    )
    .await;
}
