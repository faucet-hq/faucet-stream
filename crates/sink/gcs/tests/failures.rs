//! The GCS sink's failure paths (#777), against a mock JSON API endpoint:
//! each call the sink makes is refused in turn, and the error must name the
//! call and the object.

use faucet_core::Sink;
use faucet_core::check::{CheckContext, ProbeStatus};
use faucet_sink_gcs::{GcsCredentials, GcsSink, GcsSinkConfig};
use serde_json::{Value, json};
use std::time::Duration;
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

const BUCKET: &str = "b";

fn denied() -> ResponseTemplate {
    ResponseTemplate::new(403).set_body_json(json!({
        "error": {"code": 403, "message": "denied", "errors": [{"reason": "forbidden"}]}
    }))
}

fn is(method: &'static str) -> impl Fn(&Request) -> bool {
    move |r: &Request| r.method.as_str() == method
}

fn query(r: &Request, key: &str) -> Option<String> {
    r.url
        .query_pairs()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.into_owned())
}

async fn mount(
    server: &MockServer,
    m: impl Fn(&Request) -> bool + Send + Sync + 'static,
    t: ResponseTemplate,
) {
    Mock::given(m).respond_with(t).mount(server).await;
}

async fn sink(server: &MockServer, fields: Value) -> GcsSink {
    let base = GcsSinkConfig::new(BUCKET)
        .auth(GcsCredentials::Anonymous)
        .storage_host(server.uri());
    let mut cfg = serde_json::to_value(base).unwrap();
    for (k, v) in fields.as_object().unwrap() {
        cfg[k] = v.clone();
    }
    GcsSink::new(serde_json::from_value(cfg).unwrap())
        .await
        .expect("GcsSink::new")
}

fn overwrite() -> Value {
    json!({"path": "d/", "if_exists": "replace", "write_mode": "overwrite"})
}

#[tokio::test(flavor = "multi_thread")]
async fn a_refused_listing_fails_the_overwrite_abort() {
    let server = MockServer::start().await;
    mount(&server, |_: &Request| true, denied()).await;
    let s = sink(&server, overwrite()).await;
    assert!(s.is_overwrite());
    assert_eq!(
        s.supported_write_modes(),
        &[
            faucet_core::WriteMode::Append,
            faucet_core::WriteMode::Overwrite
        ]
    );
    let e = s.abort_overwrite().await.unwrap_err().to_string();
    assert!(
        e.contains("GCS get metadata error for key 'd/.faucet-overwrite-"),
        "{e}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_refused_delete_fails_the_overwrite_abort() {
    let server = MockServer::start().await;
    mount(
        &server,
        |r: &Request| r.url.path().ends_with(".faucet-commit"),
        ResponseTemplate::new(404),
    )
    .await;
    mount(
        &server,
        is("GET"),
        ResponseTemplate::new(200).set_body_json(json!({})),
    )
    .await;
    mount(&server, is("DELETE"), denied()).await;
    let s = sink(&server, overwrite()).await;
    let e = s.abort_overwrite().await.unwrap_err().to_string();
    assert!(e.contains("GCS delete error for key"), "{e}");
    assert!(e.contains(".faucet-swap"), "{e}");
}

#[tokio::test(flavor = "multi_thread")]
async fn aborting_an_overwrite_deletes_the_swap_marker() {
    let server = MockServer::start().await;
    mount(
        &server,
        |r: &Request| r.url.path().ends_with(".faucet-commit"),
        ResponseTemplate::new(404),
    )
    .await;
    mount(
        &server,
        is("GET"),
        ResponseTemplate::new(200).set_body_json(json!({})),
    )
    .await;
    Mock::given(is("DELETE"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;
    let s = sink(&server, overwrite()).await;
    s.abort_overwrite().await.unwrap();
    server.verify().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_refused_rewrite_fails_the_overwrite_commit() {
    let server = MockServer::start().await;
    mount(
        &server,
        |r: &Request| r.url.path().ends_with(".faucet-commit"),
        ResponseTemplate::new(404),
    )
    .await;
    mount(
        &server,
        |r: &Request| r.method.as_str() == "POST" && r.url.path().starts_with("/upload/"),
        ResponseTemplate::new(200).set_body_json(json!({
            "name": "d/x/.faucet-commit", "bucket": BUCKET, "generation": "1",
            "metageneration": "1", "size": "0"
        })),
    )
    .await;
    Mock::given(|r: &Request| r.method.as_str() == "GET" && r.url.path().ends_with("/o"))
        .respond_with(|r: &Request| {
            let prefix = query(r, "prefix").unwrap_or_default();
            ResponseTemplate::new(200)
                .set_body_json(json!({"items": [{"name": format!("{prefix}part-00001.jsonl")}]}))
        })
        .mount(&server)
        .await;
    mount(
        &server,
        is("GET"),
        ResponseTemplate::new(200).set_body_json(json!({"name": "d/x/.faucet-swap"})),
    )
    .await;
    mount(&server, is("POST"), denied()).await;
    let s = sink(&server, overwrite()).await;
    let e = s.commit_overwrite().await.unwrap_err().to_string();
    assert!(
        e.contains("GCS copy error for key 'd/part-00001.jsonl'"),
        "{e}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn check_times_out_on_a_slow_endpoint() {
    let server = MockServer::start().await;
    mount(
        &server,
        |_: &Request| true,
        ResponseTemplate::new(200)
            .set_body_json(json!({}))
            .set_delay(Duration::from_secs(5)),
    )
    .await;
    let s = sink(&server, json!({})).await;
    let ctx = CheckContext {
        timeout: Duration::from_millis(300),
    };
    let report = s.check(&ctx).await.unwrap();
    assert!(
        matches!(&report.probes[0].status, ProbeStatus::Fail { reason } if reason == "timed out")
    );
    assert_eq!(report.probes[0].name, "network");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_part_token_in_the_directory_is_a_config_error() {
    let server = MockServer::start().await;
    let cfg = GcsSinkConfig::new(BUCKET)
        .auth(GcsCredentials::Anonymous)
        .storage_host(server.uri());
    let mut v = serde_json::to_value(cfg).unwrap();
    v["path"] = json!("a-{part}/x.jsonl");
    let e = GcsSink::new(serde_json::from_value(v).unwrap())
        .await
        .err()
        .expect("refused")
        .to_string();
    assert!(e.contains("GCS sink: "), "{e}");
    assert!(e.contains("may appear only in the file name"), "{e}");
}

/// #783 L7: an object extended after a flush is continued from the local
/// copy of what was published, not downloaded again.
#[tokio::test(flavor = "multi_thread")]
async fn continuing_an_object_needs_no_download() {
    let server = MockServer::start().await;
    mount(
        &server,
        |r: &Request| query(r, "alt").as_deref() == Some("media"),
        denied(),
    )
    .await;
    mount(
        &server,
        |_: &Request| true,
        ResponseTemplate::new(200).set_body_json(json!({
            "name": "one.jsonl", "bucket": BUCKET, "generation": "1", "metageneration": "1",
            "size": "8"
        })),
    )
    .await;
    let s = sink(&server, json!({"path": "one.jsonl"})).await;
    s.write_batch(&[json!({"a": 1})]).await.unwrap();
    s.flush().await.expect("first object published");
    s.write_batch(&[json!({"a": 2})]).await.unwrap();
    s.flush().await.expect("extended without a download");
    let downloads = server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .filter(|r| query(r, "alt").as_deref() == Some("media"))
        .count();
    assert_eq!(downloads, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn check_passes_and_fails() {
    let ctx = CheckContext {
        timeout: Duration::from_secs(10),
    };
    let ok = MockServer::start().await;
    mount(
        &ok,
        is("GET"),
        ResponseTemplate::new(200).set_body_json(json!({})),
    )
    .await;
    let report = sink(&ok, json!({})).await.check(&ctx).await.unwrap();
    assert!(matches!(report.probes[0].status, ProbeStatus::Pass));

    let refused = MockServer::start().await;
    mount(&refused, |_: &Request| true, denied()).await;
    let report = sink(&refused, json!({})).await.check(&ctx).await.unwrap();
    assert!(matches!(report.probes[0].status, ProbeStatus::Fail { .. }));
    assert_eq!(
        report.probes[0].hint.as_deref(),
        Some("check bucket name, credentials, and network")
    );
}
