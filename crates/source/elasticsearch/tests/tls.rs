//! The Elasticsearch source over HTTPS (#967, #973) against an Elasticsearch 8
//! with security on (HTTPS only, every request authenticated). Without a
//! `tls:` block a server signed by an unknown CA and plaintext HTTP are
//! refused with a typed error; with the fixture CA from config (path or
//! inline PEM, every OS) `basic` and `api_key` auth read documents a wrong
//! password cannot.

use faucet_conformance::containers;
use faucet_conformance::tls::{TlsFixture, images};
use faucet_core::{FaucetError, Source};
use faucet_source_elasticsearch::{
    ElasticsearchAuth, ElasticsearchSource, ElasticsearchSourceConfig, ElasticsearchTls,
};
use serde_json::Value;

async fn read_as(
    base: &str,
    auth: ElasticsearchAuth,
    tls: ElasticsearchTls,
) -> Result<Vec<Value>, FaucetError> {
    let config = ElasticsearchSourceConfig::new(base, "tls-items")
        .auth(auth)
        .with_tls(tls);
    ElasticsearchSource::new(config)?.fetch_all().await
}

fn basic(password: &str) -> ElasticsearchAuth {
    ElasticsearchAuth::Basic {
        username: "elastic".into(),
        password: password.into(),
    }
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
    let ca = ElasticsearchTls {
        ca_cert_path: Some(tls.ca_path()),
        ..Default::default()
    };

    let untrusted = read_as(&https, basic(password), ElasticsearchTls::default()).await;
    assert!(
        untrusted.is_err(),
        "an unknown CA must be refused: {untrusted:?}"
    );
    let plaintext = read_as(
        &format!("http://localhost:{port}"),
        basic(password),
        ca.clone(),
    )
    .await;
    assert!(
        plaintext.is_err(),
        "plaintext must be refused: {plaintext:?}"
    );

    let docs = read_as(&https, basic(password), ca.clone())
        .await
        .expect("verified HTTPS read");
    assert_eq!(docs.len(), 1, "{docs:?}");
    assert_eq!(docs[0]["v"], "a", "{docs:?}");
    assert!(
        read_as(&https, basic("wrong"), ca.clone()).await.is_err(),
        "a wrong password must be refused"
    );

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
            r#"{"name":"faucet-source"}"#,
        ],
    )
    .await;
    let key: Value = serde_json::from_str(&key).expect("api key json");
    let inline = ElasticsearchTls {
        ca_cert_pem: Some(tls.ca_pem().to_string()),
        ..Default::default()
    };
    let api_key = ElasticsearchAuth::ApiKey {
        key: key["encoded"].as_str().expect("encoded key").to_string(),
    };
    let docs = read_as(&https, api_key, inline)
        .await
        .expect("api_key read");
    assert_eq!(docs.len(), 1, "{docs:?}");
}
