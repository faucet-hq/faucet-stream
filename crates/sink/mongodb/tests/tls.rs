//! The MongoDB sink over TLS (#967) against a `mongo:7.0` started with
//! `--tlsMode requireTLS`: verified and mutual TLS write real documents; an
//! unknown CA, a name the certificate does not cover and plaintext are
//! refused with a typed error.

use faucet_conformance::containers;
use faucet_conformance::tls::{TlsFixture, files, images};
use faucet_core::{FaucetError, Sink};
use faucet_sink_mongodb::{MongoSink, MongoSinkConfig};
use serde_json::json;

async fn write(uri: String, n: i64) -> Result<usize, FaucetError> {
    let sink = MongoSink::new(MongoSinkConfig::new(uri, "shop", "items")).await?;
    let written = sink.write_batch(&[json!({"n": n})]).await?;
    sink.flush().await?;
    Ok(written)
}

#[tokio::test(flavor = "multi_thread")]
async fn writes_over_tls_and_refuses_bad_certificates() {
    let tls = TlsFixture::for_names(&["localhost"]);
    let mongo = containers::start(|| images::mongo(&tls)).await;
    let port = mongo.get_host_port_ipv4(images::MONGO_PORT).await.unwrap();

    let uri = |host: &str, query: String| {
        format!(
            "mongodb://{host}:{port}/?directConnection=true&serverSelectionTimeoutMS=3000&{query}"
        )
    };
    let ca = tls.ca_path().display().to_string();
    let other = tls.other_ca_path().display().to_string();
    let client = tls.path(files::CLIENT_CERT_KEY).display().to_string();

    let ok = [
        ("localhost", format!("tls=true&tlsCAFile={ca}")),
        (
            "localhost",
            format!("tls=true&tlsCAFile={ca}&tlsCertificateKeyFile={client}"),
        ),
    ];
    for (n, (host, query)) in ok.iter().enumerate() {
        let written = write(uri(host, query.clone()), n as i64)
            .await
            .unwrap_or_else(|e| panic!("{query}: {e}"));
        assert_eq!(written, 1, "{query}");
    }
    let stored = images::mongosh(
        &mongo,
        "print(db.getSiblingDB('shop').items.find().sort({n: 1}).toArray().map(d => d.n).join(','))",
    )
    .await;
    assert_eq!(stored.trim(), "0,1");

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
            write(uri(host, query.clone()), 99).await.is_err(),
            "{query} ({why}) must be refused"
        );
    }
}
