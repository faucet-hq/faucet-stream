//! Every Kafka auth mode the source offers (#967) against one `apache/kafka`
//! broker with SSL, SASL_PLAINTEXT and SASL_SSL listeners: `sasl_plain`,
//! `sasl_scram` (SHA-256 and SHA-512), `sasl_ssl` (with and without a client
//! certificate), `ssl` (mutual TLS) and `tls`. A producer using the same auth
//! writes a record the source reads back; a wrong password and an unknown CA
//! are refused with a typed error.

use faucet_common_kafka::{KafkaAuth, ScramMechanism};
use faucet_conformance::containers;
use faucet_conformance::tls::{TlsFixture, images};
use faucet_core::{FaucetError, Source};
use faucet_source_kafka::{KafkaSource, KafkaSourceConfig};
use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde_json::{Value, json};
use std::time::Duration;

fn plain(password: &str) -> KafkaAuth {
    KafkaAuth::SaslPlain {
        username: images::KAFKA_USER.0.into(),
        password: password.into(),
    }
}

fn scram(mechanism: ScramMechanism, password: &str) -> KafkaAuth {
    KafkaAuth::SaslScram {
        mechanism,
        username: images::KAFKA_USER.0.into(),
        password: password.into(),
    }
}

fn source_config(brokers: &str, topic: &str, auth: &KafkaAuth, idle: u64) -> KafkaSourceConfig {
    serde_json::from_value(json!({
        "brokers": brokers,
        "topics": [topic],
        "group_id": format!("faucet-{topic}"),
        "auth": auth,
        "auto_offset_reset": "earliest",
        "max_messages": 1,
        "idle_timeout": idle,
    }))
    .expect("source config")
}

async fn produce(brokers: &str, topic: &str, auth: &KafkaAuth, body: &Value) {
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "30000");
    auth.apply(&mut cfg).expect("producer auth");
    let producer: FutureProducer = cfg.create().expect("producer");
    let payload = serde_json::to_vec(body).unwrap();
    producer
        .send(
            FutureRecord::<(), _>::to(topic).payload(&payload),
            Duration::from_secs(30),
        )
        .await
        .unwrap_or_else(|(e, _)| panic!("{topic}: produce: {e}"));
}

async fn read(
    brokers: &str,
    topic: &str,
    auth: &KafkaAuth,
    idle: u64,
) -> Result<Vec<Value>, FaucetError> {
    KafkaSource::new(source_config(brokers, topic, auth, idle))
        .await?
        .fetch_all()
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn every_auth_mode_consumes_and_bad_credentials_are_refused() {
    let tls = TlsFixture::generate();
    let broker = containers::start(|| images::kafka(&tls)).await;
    let port = |p| broker.get_host_port_ipv4(p);
    let ssl = format!("127.0.0.1:{}", port(images::KAFKA_SSL_PORT).await.unwrap());
    let sasl = format!(
        "127.0.0.1:{}",
        port(images::KAFKA_SASL_PLAINTEXT_PORT).await.unwrap()
    );
    let sasl_ssl = format!(
        "127.0.0.1:{}",
        port(images::KAFKA_SASL_SSL_PORT).await.unwrap()
    );

    let password = images::KAFKA_USER.1;
    let tls_ca = KafkaAuth::Tls {
        ca_path: Some(tls.ca_path()),
    };
    let mutual = KafkaAuth::Ssl {
        ca_path: tls.ca_path(),
        cert_path: tls.client_cert_path(),
        key_path: tls.client_key_path(),
        key_password: None,
    };
    let cases = [
        ("sasl-plain", &sasl, plain(password)),
        (
            "sasl-scram-256",
            &sasl,
            scram(ScramMechanism::Sha256, password),
        ),
        (
            "sasl-scram-512",
            &sasl,
            scram(ScramMechanism::Sha512, password),
        ),
        (
            "sasl-ssl-plain",
            &sasl_ssl,
            KafkaAuth::SaslSsl {
                sasl: Box::new(plain(password)),
                ssl: Box::new(tls_ca.clone()),
            },
        ),
        (
            "sasl-ssl-scram-mtls",
            &sasl_ssl,
            KafkaAuth::SaslSsl {
                sasl: Box::new(scram(ScramMechanism::Sha512, password)),
                ssl: Box::new(mutual.clone()),
            },
        ),
        ("ssl-mutual", &ssl, mutual.clone()),
        ("tls", &ssl, tls_ca.clone()),
    ];
    for (i, (topic, brokers, auth)) in cases.iter().enumerate() {
        let body = json!({"case": topic, "n": i});
        produce(brokers, topic, auth, &body).await;
        let records = read(brokers, topic, auth, 60)
            .await
            .unwrap_or_else(|e| panic!("{topic}: {e}"));
        let values: Vec<&Value> = records.iter().map(|r| &r["value"]).collect();
        assert_eq!(values, vec![&body], "{topic}");
        assert_eq!(records[0]["topic"], *topic);
    }

    produce(&sasl, "guarded", &plain(password), &json!({"secret": true})).await;
    let refused = [
        ("bad-plain", &sasl, plain("wrong")),
        ("bad-scram", &sasl, scram(ScramMechanism::Sha256, "wrong")),
        (
            "bad-ca",
            &ssl,
            KafkaAuth::Tls {
                ca_path: Some(tls.other_ca_path()),
            },
        ),
    ];
    for (name, brokers, auth) in refused {
        let outcome = read(brokers, "guarded", &auth, 10).await;
        assert!(
            matches!(&outcome, Err(FaucetError::Source(msg)) if msg.contains("kafka recv")),
            "{name} must be refused with a typed error: {outcome:?}"
        );
    }
}
