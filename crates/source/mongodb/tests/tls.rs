//! The MongoDB source over TLS (#967) against a `mongo:7.0` started with
//! `--tlsMode requireTLS`: verified, mutual and relaxed TLS read real
//! documents; an unknown CA, a name the certificate does not cover and
//! plaintext are refused with a typed error.

use faucet_conformance::containers;
use faucet_conformance::tls::{TlsFixture, files, images};
use faucet_core::FaucetError;
use faucet_source_mongodb::{MongoSource, MongoSourceConfig};
use serde_json::Value;

async fn read(uri: String) -> Result<Vec<Value>, FaucetError> {
    let config = MongoSourceConfig::new(uri, "shop", "items").sort(serde_json::json!({"n": 1}));
    MongoSource::new(config).await?.fetch_all().await
}

#[tokio::test(flavor = "multi_thread")]
async fn reads_over_tls_and_refuses_bad_certificates() {
    let tls = TlsFixture::for_names(&["localhost"]);
    let mongo = containers::start(|| images::mongo(&tls)).await;
    let port = mongo.get_host_port_ipv4(images::MONGO_PORT).await.unwrap();
    images::mongosh(
        &mongo,
        "db.getSiblingDB('shop').items.insertMany([{n: 1}, {n: 2}])",
    )
    .await;

    let uri = |host: &str, query: String| {
        format!(
            "mongodb://{host}:{port}/?directConnection=true&serverSelectionTimeoutMS=3000&{query}"
        )
    };
    let ca = tls.ca_path().display().to_string();
    let other = tls.other_ca_path().display().to_string();
    let client = tls.path(files::CLIENT_CERT_KEY).display().to_string();

    for (host, query) in [
        ("localhost", format!("tls=true&tlsCAFile={ca}")),
        (
            "localhost",
            format!("tls=true&tlsCAFile={ca}&tlsCertificateKeyFile={client}"),
        ),
        (
            "127.0.0.1",
            "tls=true&tlsAllowInvalidCertificates=true".to_string(),
        ),
        (
            "localhost",
            format!("tls=true&tlsCAFile={other}&tlsInsecure=true"),
        ),
    ] {
        let docs = read(uri(host, query.clone()))
            .await
            .unwrap_or_else(|e| panic!("{query}: {e}"));
        let ns: Vec<i64> = docs.iter().map(|d| d["n"].as_i64().unwrap()).collect();
        assert_eq!(ns, [1, 2], "{query}: {docs:?}");
    }

    for (host, query, why) in [
        (
            "localhost",
            format!("tls=true&tlsCAFile={other}"),
            "unknown CA",
        ),
        ("127.0.0.1", format!("tls=true&tlsCAFile={ca}"), "hostname"),
        ("localhost", "tls=false".to_string(), "plaintext"),
    ] {
        assert!(
            read(uri(host, query.clone())).await.is_err(),
            "{query} ({why}) must be refused"
        );
    }
}
