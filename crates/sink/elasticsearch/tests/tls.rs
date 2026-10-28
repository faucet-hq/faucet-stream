//! The Elasticsearch sink over HTTPS (#967, #973) against an Elasticsearch 8
//! with security on (HTTPS only, every request authenticated). Without a
//! `tls:` block a server signed by an unknown CA and plaintext HTTP are
//! refused with a typed error; with the fixture CA in `tls.ca_cert_path`
//! (every OS, no `SSL_CERT_FILE`) `basic`, `api_key` and `bearer` (an OAuth2
//! access token from `_security/oauth2/token`) auth write documents a wrong
//! credential cannot, an unrelated CA is still refused, and
//! `insecure_skip_verify` connects without one.

use faucet_conformance::containers;
use faucet_conformance::tls::{TlsFixture, images};
use faucet_core::{FaucetError, Sink};
use faucet_sink_elasticsearch::{
    ElasticsearchAuth, ElasticsearchSink, ElasticsearchSinkConfig, ElasticsearchTls,
};
use serde_json::json;

async fn write_with(
    base: &str,
    auth: ElasticsearchAuth,
    id: u32,
    tls: ElasticsearchTls,
) -> Result<usize, FaucetError> {
    let config = ElasticsearchSinkConfig::new(base, "tls-items")
        .auth(auth)
        .with_tls(tls);
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
    let ca = ElasticsearchTls {
        ca_cert_path: Some(tls.ca_path()),
        ..Default::default()
    };
    let write = |base: String, auth: ElasticsearchAuth, id: u32| {
        let ca = ca.clone();
        async move { write_with(&base, auth, id, ca).await }
    };

    let untrusted = write_with(&https, basic(password), 0, ElasticsearchTls::default()).await;
    assert!(
        untrusted.is_err(),
        "an unknown CA must be refused: {untrusted:?}"
    );
    let other = ElasticsearchTls {
        ca_cert_path: Some(tls.other_ca_path()),
        ..Default::default()
    };
    let unrelated = write_with(&https, basic(password), 0, other).await;
    assert!(
        unrelated.is_err(),
        "a CA that did not sign the server must be refused: {unrelated:?}"
    );
    let plaintext = write(format!("http://localhost:{port}"), basic(password), 0).await;
    assert!(
        plaintext.is_err(),
        "plaintext must be refused: {plaintext:?}"
    );

    assert_eq!(
        write(https.clone(), basic(password), 1)
            .await
            .expect("basic"),
        1
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
            r#"{"name":"faucet"}"#,
        ],
    )
    .await;
    let key: serde_json::Value = serde_json::from_str(&key).expect("api key json");
    let encoded = key["encoded"].as_str().expect("encoded key").to_string();
    assert_eq!(
        write(https.clone(), ElasticsearchAuth::ApiKey { key: encoded }, 2)
            .await
            .expect("api_key"),
        1
    );
    let token = images::exec(
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
            "https://localhost:9200/_security/oauth2/token",
            "-d",
            r#"{"grant_type":"client_credentials"}"#,
        ],
    )
    .await;
    let reply: serde_json::Value =
        serde_json::from_str(&token).unwrap_or_else(|e| panic!("token reply {token:?}: {e}"));
    let token = reply["access_token"]
        .as_str()
        .unwrap_or_else(|| panic!("no access token in {reply}"))
        .to_string();
    assert_eq!(
        write(https.clone(), ElasticsearchAuth::Bearer { token }, 6)
            .await
            .expect("bearer"),
        1
    );
    assert!(
        write(
            https.clone(),
            ElasticsearchAuth::Bearer {
                token: "not-a-token".into()
            },
            7
        )
        .await
        .is_err(),
        "an unknown bearer token must be refused"
    );
    assert!(
        write(https.clone(), basic("wrong"), 3).await.is_err(),
        "a wrong password must be refused"
    );

    images::exec(
        &es,
        &[
            "curl",
            "-sk",
            "-u",
            &format!("elastic:{password}"),
            "-X",
            "POST",
            "https://localhost:9200/tls-items/_refresh",
        ],
    )
    .await;
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
    assert_eq!(count["count"], 3, "{count}");

    let insecure = ElasticsearchTls {
        insecure_skip_verify: true,
        ..Default::default()
    };
    assert_eq!(
        write_with(&https, basic(password), 4, insecure)
            .await
            .expect("insecure_skip_verify"),
        1
    );
}
