//! The Elasticsearch sink over HTTPS (#967) against an Elasticsearch 8 with
//! security on (HTTPS only, every request authenticated). The connector has
//! no CA setting: it trusts the platform store. Everywhere, a server signed by
//! an unknown CA and plaintext HTTP are refused with a typed error. On Linux,
//! where the platform verifier honours `SSL_CERT_FILE`, the fixture CA is then
//! trusted and `basic` and `api_key` auth write documents a wrong password
//! cannot.

use faucet_conformance::containers;
use faucet_conformance::tls::{TlsFixture, images};
use faucet_core::{FaucetError, Sink};
use faucet_sink_elasticsearch::{ElasticsearchAuth, ElasticsearchSink, ElasticsearchSinkConfig};
use serde_json::json;

async fn write(base: &str, auth: ElasticsearchAuth, id: u32) -> Result<usize, FaucetError> {
    let config = ElasticsearchSinkConfig::new(base, "tls-items").auth(auth);
    let sink = ElasticsearchSink::new(config)?;
    let written = sink.write_batch(&[json!({"id": id})]).await?;
    sink.flush().await?;
    Ok(written)
}

fn basic(password: &str) -> ElasticsearchAuth {
    ElasticsearchAuth::Basic {
        username: "elastic".into(),
        password: password.into(),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn writes_over_https_and_refuses_bad_certificates() {
    let tls = TlsFixture::generate();
    let es = containers::start(|| images::elasticsearch(&tls)).await;
    let port = es
        .get_host_port_ipv4(images::ELASTICSEARCH_PORT)
        .await
        .unwrap();
    let https = format!("https://localhost:{port}");
    let password = images::ELASTICSEARCH_PASSWORD;

    let untrusted = write(&https, basic(password), 0).await;
    assert!(
        untrusted.is_err(),
        "an unknown CA must be refused: {untrusted:?}"
    );
    let plaintext = write(&format!("http://localhost:{port}"), basic(password), 0).await;
    assert!(
        plaintext.is_err(),
        "plaintext must be refused: {plaintext:?}"
    );

    if !cfg!(target_os = "linux") {
        return;
    }
    // SAFETY: this binary runs one test; nothing else reads the environment concurrently.
    unsafe { std::env::set_var("SSL_CERT_FILE", tls.ca_path()) };
    assert_eq!(write(&https, basic(password), 1).await.expect("basic"), 1);
    let key = images::exec(
        &es,
        &[
            "curl",
            "-sk",
            "-u",
            &format!("elastic:{password}"),
            "-H",
            "Content-Type: application/json",
            "-X",
            "POST",
            "https://localhost:9200/_security/api_key",
            "-d",
            r#"{"name":"faucet"}"#,
        ],
    )
    .await;
    let key: serde_json::Value = serde_json::from_str(&key).expect("api key json");
    let encoded = key["encoded"].as_str().expect("encoded key").to_string();
    assert_eq!(
        write(&https, ElasticsearchAuth::ApiKey { key: encoded }, 2)
            .await
            .expect("api_key"),
        1
    );
    assert!(
        write(&https, basic("wrong"), 3).await.is_err(),
        "a wrong password must be refused"
    );

    let count = images::exec(
        &es,
        &[
            "curl",
            "-sk",
            "-u",
            &format!("elastic:{password}"),
            "https://localhost:9200/tls-items/_count?q=*",
        ],
    )
    .await;
    let count: serde_json::Value = serde_json::from_str(&count).unwrap();
    assert_eq!(count["count"], 2, "{count}");
}
