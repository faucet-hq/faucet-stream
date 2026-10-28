//! The Elasticsearch source over HTTPS (#967) against an Elasticsearch 8 with
//! security on (HTTPS only, every request authenticated). The connector has
//! no CA setting: it trusts the platform store. Everywhere, a server signed by
//! an unknown CA and plaintext HTTP are refused with a typed error. On Linux,
//! where the platform verifier honours `SSL_CERT_FILE`, the fixture CA is then
//! trusted and `basic` auth reads documents a wrong password cannot.

use faucet_conformance::containers;
use faucet_conformance::tls::{TlsFixture, images};
use faucet_core::{FaucetError, Source};
use faucet_source_elasticsearch::{
    ElasticsearchAuth, ElasticsearchSource, ElasticsearchSourceConfig,
};
use serde_json::Value;

async fn read(base: &str, password: &str) -> Result<Vec<Value>, FaucetError> {
    let config = ElasticsearchSourceConfig::new(base, "tls-items").auth(ElasticsearchAuth::Basic {
        username: "elastic".into(),
        password: password.into(),
    });
    ElasticsearchSource::new(config)?.fetch_all().await
}

#[tokio::test(flavor = "multi_thread")]
async fn reads_over_https_and_refuses_bad_certificates() {
    let tls = TlsFixture::generate();
    let es = containers::start(|| images::elasticsearch(&tls)).await;
    let port = es
        .get_host_port_ipv4(images::ELASTICSEARCH_PORT)
        .await
        .unwrap();
    let password = images::ELASTICSEARCH_PASSWORD;
    images::exec(
        &es,
        &[
            "curl",
            "-sfk",
            "-u",
            &format!("elastic:{password}"),
            "-H",
            "Content-Type: application/json",
            "-X",
            "PUT",
            "https://localhost:9200/tls-items/_doc/1?refresh=true",
            "-d",
            r#"{"id":1,"v":"a"}"#,
        ],
    )
    .await;
    let https = format!("https://localhost:{port}");

    let untrusted = read(&https, password).await;
    assert!(
        untrusted.is_err(),
        "an unknown CA must be refused: {untrusted:?}"
    );
    let plaintext = read(&format!("http://localhost:{port}"), password).await;
    assert!(
        plaintext.is_err(),
        "plaintext must be refused: {plaintext:?}"
    );

    if !cfg!(target_os = "linux") {
        return;
    }
    // SAFETY: this binary runs one test; nothing else reads the environment concurrently.
    unsafe { std::env::set_var("SSL_CERT_FILE", tls.ca_path()) };
    let docs = read(&https, password).await.expect("verified HTTPS read");
    assert_eq!(docs.len(), 1, "{docs:?}");
    assert_eq!(docs[0]["v"], "a", "{docs:?}");
    assert!(
        read(&https, "wrong").await.is_err(),
        "a wrong password must be refused"
    );
}
