//! A task that may not use the host's identity reads a table with the
//! credentials its config names and nothing from the process environment.
//! One test, because it rewrites the process environment.
#![cfg(all(feature = "s3", feature = "azure", feature = "gcs"))]
#![allow(unsafe_code)] // the test sets process environment variables

use faucet_common_delta::DeltaConnection;
use faucet_core::resolve::with_host_credentials;
use serde_json::json;
use wiremock::{Mock, MockServer, ResponseTemplate};

fn set(vars: &[(&str, &str)]) {
    // SAFETY: this binary has one test; nothing reads the environment concurrently.
    unsafe {
        for (k, v) in vars {
            std::env::set_var(k, v);
        }
    }
}

async fn server() -> MockServer {
    let server = MockServer::start().await;
    Mock::given(wiremock::matchers::any())
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;
    server
}

fn conn(v: serde_json::Value) -> DeltaConnection {
    serde_json::from_value(v).unwrap()
}

#[tokio::test]
async fn a_caller_run_reads_only_the_credentials_its_config_names() {
    set(&[
        ("AWS_SESSION_TOKEN", "server-session-token"),
        ("AZURE_STORAGE_ACCOUNT_KEY", "c2VydmVyLWtleQ=="),
        ("GOOGLE_SERVICE_ACCOUNT", "/nonexistent/server-sa.json"),
    ]);

    let s3 = server().await;
    let c = conn(
        json!({"table_uri": "s3://bucket/t", "credentials": {"type": "aws", "config": {
        "access_key_id": "caller-id", "secret_access_key": "caller-secret",
        "region": "us-east-1", "endpoint_url": s3.uri(), "allow_http": true}}}),
    );
    let _ = with_host_credentials(false, c.open_optional()).await;
    let requests = s3.received_requests().await.unwrap();
    assert!(!requests.is_empty(), "the table was read");
    for r in &requests {
        assert!(
            r.headers.get("x-amz-security-token").is_none(),
            "{:?}",
            r.headers
        );
        let auth = r.headers.get("authorization").unwrap().to_str().unwrap();
        assert!(auth.contains("Credential=caller-id/"), "{auth}");
    }

    let az = server().await;
    let c = conn(json!({"table_uri": "abfss://c@acct.dfs.core.windows.net/t",
        "credentials": {"type": "azure", "config": {
            "account_name": "acct", "sas_token": "sv=2022-11-02&sig=caller"}},
        "storage_options": {"azure_storage_endpoint": az.uri(), "azure_allow_http": "true"}}));
    let _ = with_host_credentials(false, c.open_optional()).await;
    let requests = az.received_requests().await.unwrap();
    assert!(!requests.is_empty(), "the table was read");
    for r in &requests {
        assert!(r.headers.get("authorization").is_none(), "{:?}", r.headers);
        assert!(
            r.url.query().unwrap_or("").contains("sig=caller"),
            "{}",
            r.url
        );
    }

    let c = conn(
        json!({"table_uri": "gs://bucket/t", "credentials": {"type": "gcp", "config": {
            "service_account_key":
                "{\"private_key\": \"x\", \"private_key_id\": \"k\", \"client_email\": \"c@example.com\"}"
        }}}),
    );
    let err = with_host_credentials(false, c.open_optional())
        .await
        .expect_err("the caller's key is not a valid key")
        .to_string();
    assert!(
        !err.contains("service account path or service account key"),
        "{err}"
    );
}
