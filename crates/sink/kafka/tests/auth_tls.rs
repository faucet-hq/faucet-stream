//! Every Kafka auth mode the sink offers (#967) against one `apache/kafka`
//! broker with SSL, SASL_PLAINTEXT and SASL_SSL listeners: `sasl_plain`,
//! `sasl_scram` (SHA-256 and SHA-512), `sasl_ssl` (with and without a client
//! certificate), `ssl` (mutual TLS) and `tls`. Each produces a record that a
//! consumer using the same auth reads back; a wrong password and an unknown
//! CA are refused.

use faucet_common_kafka::{KafkaAuth, ScramMechanism};
use faucet_conformance::containers;
use faucet_conformance::tls::{TlsFixture, images};
use faucet_core::Sink;
use faucet_sink_kafka::{KafkaSink, KafkaSinkConfig};
use rdkafka::ClientConfig;
use rdkafka::Message;
use rdkafka::consumer::{Consumer, StreamConsumer};
use serde_json::json;
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

fn sink_config(brokers: &str, topic: &str, auth: &KafkaAuth) -> KafkaSinkConfig {
    serde_json::from_value(json!({
        "brokers": brokers,
        "topic": {"type": "fixed", "name": topic},
        "auth": auth,
        "message_timeout": 10,
    }))
    .expect("sink config")
}

async fn read_one(brokers: &str, topic: &str, auth: &KafkaAuth) -> serde_json::Value {
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", brokers)
        .set("group.id", format!("verify-{topic}"))
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false");
    auth.apply(&mut cfg).expect("consumer auth");
    let consumer: StreamConsumer = cfg.create().expect("consumer");
    consumer.subscribe(&[topic]).unwrap();
    let msg = tokio::time::timeout(Duration::from_secs(60), consumer.recv())
        .await
        .expect("a record within 60s")
        .expect("recv");
    serde_json::from_slice(msg.payload().unwrap()).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn every_auth_mode_produces_and_bad_credentials_are_refused() {
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
        let sink = KafkaSink::new(sink_config(brokers, topic, auth))
            .await
            .unwrap_or_else(|e| panic!("{topic}: {e}"));
        let written = sink
            .write_batch(&[json!({"case": topic, "n": i})])
            .await
            .unwrap_or_else(|e| panic!("{topic}: {e}"));
        assert_eq!(written, 1, "{topic}");
        sink.flush().await.unwrap();
        assert_eq!(
            read_one(brokers, topic, auth).await,
            json!({"case": topic, "n": i})
        );
    }

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
    for (topic, brokers, auth) in refused {
        let outcome = match KafkaSink::new(sink_config(brokers, topic, &auth)).await {
            Err(e) => Err(e),
            Ok(sink) => sink.write_batch(&[json!({"case": topic})]).await,
        };
        assert!(outcome.is_err(), "{topic} must be refused: {outcome:?}");
    }
}
