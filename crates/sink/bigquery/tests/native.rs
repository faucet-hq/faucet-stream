//! Integration tests for the BigQuery **native byte-passthrough** load path
//! (#633): `Sink::load_native` uploads NDJSON/CSV bytes directly via a load job.
//! Driven against a wiremock BigQuery so the load-job body (explicit all-STRING
//! schema, `autodetect:false`, write disposition) can be asserted without a real
//! project — and so the row count comes back from `statistics.load.outputRows`.

use faucet_core::{NativeBatch, NativeFormat, NativeLoadContext, NativePayload, Sink, WriteMode};
use faucet_sink_bigquery::{BigQueryCredentials, BigQuerySink, BigQuerySinkConfig};
use gcp_bigquery_client::client_builder::ClientBuilder;
use serde::Serialize;
use serde_json::json;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

const PROJECT_ID: &str = "p";
const DATASET_ID: &str = "d";
const TABLE_ID: &str = "t";
const AUTH_TOKEN_PATH: &str = "/:o/oauth2/token";
const AUTH_SCOPE_BASE: &str = "/auth/bigquery";

#[derive(Serialize)]
struct FakeToken {
    access_token: &'static str,
    token_type: &'static str,
    expires_in: u32,
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

async fn mount_token_endpoint(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path(AUTH_TOKEN_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(FakeToken {
            access_token: "fake-token",
            token_type: "bearer",
            expires_in: 9_999_999,
        }))
        .mount(server)
        .await;
}

/// Mount the resumable-upload trio: initiate (POST → 200 + `Location` header
/// pointing back at the mock), the finalize PUT (200 + a DONE `Job`), and the
/// `jobs.get` poll (DONE). NDJSON native loads stream into one such session,
/// finalized in `flush`. The initiate POST body carries the load-job JSON
/// (schema, write disposition) that the tests assert on.
async fn mount_resumable(server: &MockServer, session_path: &str, job_id: &str) {
    let session_uri = format!("{}{session_path}", server.uri());
    Mock::given(method("POST"))
        .and(path(format!(
            "/upload/bigquery/v2/projects/{PROJECT_ID}/jobs"
        )))
        .and(query_param("uploadType", "resumable"))
        .respond_with(ResponseTemplate::new(200).insert_header("location", session_uri.as_str()))
        .mount(server)
        .await;
    Mock::given(method("PUT"))
        .and(path(session_path.to_string()))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobReference": {"projectId": PROJECT_ID, "jobId": job_id},
            "status": {"state": "DONE"}
        })))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/projects/{PROJECT_ID}/jobs/{job_id}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobReference": {"projectId": PROJECT_ID, "jobId": job_id},
            "status": {"state": "DONE"},
            "statistics": {"load": {"outputRows": "0"}}
        })))
        .mount(server)
        .await;
}

async fn build_sink(
    server: &MockServer,
    config: BigQuerySinkConfig,
) -> (BigQuerySink, tempfile::NamedTempFile) {
    let sa_file = tempfile::NamedTempFile::new().expect("sa tempfile");
    std::fs::write(
        sa_file.path(),
        serde_json::to_string_pretty(&dummy_service_account_json(&server.uri())).unwrap(),
    )
    .expect("write sa");
    let client = ClientBuilder::new()
        .with_auth_base_url(format!("{}{AUTH_SCOPE_BASE}", server.uri()))
        .with_v2_base_url(server.uri())
        .build_from_service_account_key_file(sa_file.path().to_str().unwrap())
        .await
        .expect("build client");
    (BigQuerySink::from_parts(config, client), sa_file)
}

fn native_config(server: &MockServer) -> BigQuerySinkConfig {
    let mut c = BigQuerySinkConfig::new(
        PROJECT_ID,
        DATASET_ID,
        TABLE_ID,
        BigQueryCredentials::ServiceAccountKey {
            json: serde_json::to_string(&dummy_service_account_json(&server.uri())).unwrap(),
        },
    );
    c.upload_base_url = Some(server.uri());
    c
}

/// The concatenated bodies of every multipart upload POST.
async fn upload_bodies(server: &MockServer) -> Vec<String> {
    server
        .received_requests()
        .await
        .unwrap()
        .into_iter()
        .filter(|r| r.url.path().contains("/upload/bigquery"))
        .map(|r| String::from_utf8_lossy(&r.body).to_string())
        .collect()
}

#[tokio::test]
async fn load_native_ndjson_streams_session_with_explicit_string_schema() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_resumable(&server, "/session/native-1", "job-native-1").await;
    let (sink, _sa) = build_sink(&server, native_config(&server)).await;

    let batch = NativeBatch::bytes(
        NativeFormat::NdJson,
        b"{\"Id\":\"1\",\"Amount\":\"9\"}\n{\"Id\":\"2\",\"Amount\":\"x\"}\n".to_vec(),
    )
    .with_records(Some(2));
    let ctx = NativeLoadContext {
        write_mode: WriteMode::Append,
        first_batch: true,
    };
    // The batch feeds the resumable session; the count is the source's row count
    // (the single load job completes at flush, like the Value media-load path).
    let n = sink
        .load_native(batch, "p::row", ctx)
        .await
        .expect("feed ok");
    assert_eq!(n, 2);
    sink.flush().await.expect("flush finalizes the load");

    // The initiate POST body is the load-job JSON (schema + disposition).
    let body = upload_bodies(&server).await.join("\n");
    assert!(body.contains("NEWLINE_DELIMITED_JSON"), "{body:.400}");
    assert!(
        body.contains("\"autodetect\":false"),
        "autodetect must be off"
    );
    assert!(body.contains("\"WRITE_APPEND\""));
    // Explicit all-STRING schema for the payload's columns — stops autodetect from
    // mis-inferring a type and failing a later row.
    assert!(
        body.contains("\"name\":\"Id\",\"type\":\"STRING\""),
        "{body:.400}"
    );
    assert!(body.contains("\"name\":\"Amount\",\"type\":\"STRING\""));
}

#[tokio::test]
async fn load_native_overwrite_first_batch_truncates_once_per_object() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_resumable(&server, "/session/native-2", "job-native-2").await;
    let mut config = native_config(&server);
    config.write.write_mode = WriteMode::Overwrite;
    let (sink, _sa) = build_sink(&server, config).await;

    // First batch opens the session WRITE_TRUNCATE; a second batch appends into the
    // SAME session — so the whole object is one atomic truncating load.
    sink.load_native(
        NativeBatch::bytes(NativeFormat::NdJson, b"{\"Id\":\"1\"}\n".to_vec())
            .with_records(Some(1)),
        "p::row",
        NativeLoadContext {
            write_mode: WriteMode::Overwrite,
            first_batch: true,
        },
    )
    .await
    .expect("feed 1");
    sink.load_native(
        NativeBatch::bytes(NativeFormat::NdJson, b"{\"Id\":\"2\"}\n".to_vec())
            .with_records(Some(1)),
        "p::row",
        NativeLoadContext {
            write_mode: WriteMode::Overwrite,
            first_batch: false,
        },
    )
    .await
    .expect("feed 2");
    sink.flush().await.expect("flush");

    let bodies = upload_bodies(&server).await;
    // Exactly one initiate (one session/load job for the object), truncating.
    assert_eq!(bodies.len(), 1, "one load job per object: {bodies:?}");
    assert!(bodies[0].contains("\"WRITE_TRUNCATE\""), "{}", bodies[0]);
}

#[tokio::test]
async fn load_native_streaming_payload_feeds_session_chunk_by_chunk() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_resumable(&server, "/session/native-3", "job-native-3").await;
    let (sink, _sa) = build_sink(&server, native_config(&server)).await;

    // A multi-chunk NDJSON stream — load_native must never buffer the whole thing.
    let chunks: Vec<Result<Vec<u8>, faucet_core::FaucetError>> = vec![
        Ok(b"{\"Id\":\"1\"}\n{\"Id\":\"2\"}\n".to_vec()),
        Ok(b"{\"Id\":\"3\"}\n".to_vec()),
    ];
    let batch = NativeBatch {
        format: NativeFormat::NdJson,
        payload: NativePayload::Stream(Box::pin(futures::stream::iter(chunks))),
        csv: faucet_core::CsvDialect::default(),
        records: None,
        bookmark: None,
    };
    let ctx = NativeLoadContext {
        write_mode: WriteMode::Append,
        first_batch: true,
    };
    // Row count is the NDJSON line count across all chunks.
    let n = sink
        .load_native(batch, "p::row", ctx)
        .await
        .expect("stream feed ok");
    assert_eq!(n, 3);
    sink.flush().await.expect("flush finalizes");

    let body = upload_bodies(&server).await.join("\n");
    // One session opened, schema derived from the first chunk's first line.
    assert!(
        body.contains("\"name\":\"Id\",\"type\":\"STRING\""),
        "{body:.400}"
    );
    assert!(body.contains("\"autodetect\":false"));
}

#[tokio::test]
async fn load_native_empty_payload_is_a_noop() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    // Nothing mounted — an empty batch must not open a session or POST anything.
    let (sink, _sa) = build_sink(&server, native_config(&server)).await;
    let ctx = NativeLoadContext {
        write_mode: WriteMode::Append,
        first_batch: true,
    };
    let n = sink
        .load_native(
            NativeBatch::bytes(NativeFormat::NdJson, Vec::new()),
            "s",
            ctx,
        )
        .await
        .expect("empty ok");
    assert_eq!(n, 0);
    assert!(upload_bodies(&server).await.is_empty());
}

// ── Capability advertisement (#633) ──────────────────────────────────────────

/// A capability set is only honest if it matches what `load_native` can
/// actually deliver: NDJSON feeds ONE resumable session finalized by the
/// terminal flush (so it satisfies the atomic-overwrite contract), while CSV
/// runs a discrete load job per batch (so a multi-batch overwrite would be
/// truncate + partial appends — hence append-only).
#[tokio::test]
async fn native_capabilities_scope_overwrite_to_ndjson_only() {
    use faucet_core::{NativeFormat, Sink as _, WriteMode};
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    let (sink, _guard) = build_sink(&server, native_config(&server)).await;

    let caps = sink.native_load_capabilities();
    assert_eq!(caps.len(), 2, "NDJSON + CSV");
    let nd = caps
        .iter()
        .find(|c| c.format == NativeFormat::NdJson)
        .expect("ndjson capability");
    assert_eq!(nd.mechanism, "bigquery-load-job");
    assert_eq!(nd.write_modes, &[WriteMode::Append, WriteMode::Overwrite]);
    let csv = caps
        .iter()
        .find(|c| c.format == NativeFormat::Csv)
        .expect("csv capability");
    assert_eq!(
        csv.write_modes,
        &[WriteMode::Append],
        "a per-batch-committing mechanism must not claim Overwrite"
    );
}

/// Upsert/delete need per-row keys, which raw bytes never carry — the sink must
/// advertise nothing rather than let the planner append duplicates.
#[tokio::test]
async fn native_capabilities_are_withdrawn_for_keyed_write_modes() {
    use faucet_core::Sink as _;
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;

    for mode in ["upsert", "delete"] {
        let mut cfg = native_config(&server);
        cfg.write = serde_json::from_value(serde_json::json!({
            "write_mode": mode,
            "key": ["id"]
        }))
        .unwrap();
        let (sink, _guard) = build_sink(&server, cfg).await;
        assert!(
            sink.native_load_capabilities().is_empty(),
            "{mode} must not advertise a byte-load path"
        );
    }
}

/// An empty payload is a successful no-op: sources emit a trailing empty batch
/// to carry the final bookmark, and an error here would break bookmark
/// persistence (the contract documented on `Sink::load_native`).
#[tokio::test]
async fn load_native_empty_payload_is_a_no_op() {
    use faucet_core::{NativeBatch, NativeFormat, NativeLoadContext, Sink as _, WriteMode};
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    let (sink, _guard) = build_sink(&server, native_config(&server)).await;
    let ctx = NativeLoadContext {
        write_mode: WriteMode::Append,
        first_batch: true,
    };
    for format in [NativeFormat::NdJson, NativeFormat::Csv] {
        let rows = sink
            .load_native(NativeBatch::bytes(format, Vec::new()), "scope", ctx)
            .await
            .expect("empty payload must not error");
        assert_eq!(rows, 0);
    }
    assert!(
        upload_bodies(&server).await.is_empty(),
        "no upload for an empty batch"
    );
}

/// An unsupported (format, payload) pair is a typed sink error rather than a
/// silent zero-row success.
#[tokio::test]
async fn load_native_rejects_an_unsupported_format() {
    use faucet_core::{NativeBatch, NativeFormat, NativeLoadContext, Sink as _, WriteMode};
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    let (sink, _guard) = build_sink(&server, native_config(&server)).await;
    let err = sink
        .load_native(
            NativeBatch::bytes(NativeFormat::Parquet, b"PAR1".to_vec()),
            "scope",
            NativeLoadContext {
                write_mode: WriteMode::Append,
                first_batch: true,
            },
        )
        .await
        .expect_err("parquet is not a load_native format here");
    assert!(err.to_string().contains("unsupported format"), "{err}");
}
