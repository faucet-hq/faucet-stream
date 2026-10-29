//! SASL `EXTERNAL` auth (#967): a `rabbitmq:3.13` broker with
//! `rabbitmq_auth_mechanism_ssl` and a TLS listener that requires a client
//! certificate and logs in the certificate's common name. Before that user
//! exists the connection is refused; once it does, the source reads over
//! `amqps://` with the fixture's client certificate.
#![cfg(feature = "tls")]

mod common;

use faucet_conformance::tls::{TlsFixture, images};
use faucet_core::Source;
use faucet_source_rabbitmq::{
    RabbitMqAuth, RabbitMqConnectionConfig, RabbitMqSource, RabbitMqSourceConfig,
};
use serde_json::json;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::{GenericImage, ImageExt};

const CLIENT: &str = "faucet-test-client";
const TLS_CONF: &str = "listeners.ssl.default = 5671
ssl_options.cacertfile = /certs/ca.pem
ssl_options.certfile = /certs/server.pem
ssl_options.keyfile = /certs/server.key
ssl_options.verify = verify_peer
ssl_options.fail_if_no_peer_cert = true
ssl_cert_login_from = common_name
auth_mechanisms.1 = PLAIN
auth_mechanisms.2 = AMQPLAIN
auth_mechanisms.3 = EXTERNAL
";

fn config(port: u16, tls: &TlsFixture) -> RabbitMqSourceConfig {
    let mut cfg = RabbitMqSourceConfig::new("ext");
    cfg.connection = RabbitMqConnectionConfig::from_url(format!("amqps://localhost:{port}/%2f"));
    cfg.connection.auth = RabbitMqAuth::External;
    cfg.connection.tls.enabled = true;
    cfg.connection.tls.ca_cert_path = Some(tls.ca_path());
    cfg.connection.tls.client_cert_path = Some(tls.client_cert_path());
    cfg.connection.tls.client_key_path = Some(tls.client_key_path());
    cfg.connection.connect_retries = 0;
    cfg.max_messages = Some(3);
    cfg.idle_timeout_secs = Some(5);
    cfg
}

#[tokio::test(flavor = "multi_thread")]
async fn external_auth_logs_in_the_client_certificate() {
    let tls = TlsFixture::generate();
    let container = faucet_conformance::containers::start(|| {
        tls.mount(
            GenericImage::new("rabbitmq", "3.13")
                .with_exposed_port(5672.tcp())
                .with_exposed_port(5671.tcp())
                .with_wait_for(WaitFor::message_on_stdout("Server startup complete")),
            "/certs",
        )
        .with_copy_to(
            "/etc/rabbitmq/conf.d/20-tls.conf",
            TLS_CONF.as_bytes().to_vec(),
        )
        .with_copy_to(
            "/etc/rabbitmq/enabled_plugins",
            b"[rabbitmq_auth_mechanism_ssl].".to_vec(),
        )
    })
    .await;
    let plain = container.get_host_port_ipv4(5672).await.unwrap();
    let tls_port = container.get_host_port_ipv4(5671).await.unwrap();
    let url = format!("amqp://guest:guest@localhost:{plain}/%2f");
    common::declare_queue(&url, "ext").await;
    common::publish_json(&url, "ext", 3).await;

    let refused = match RabbitMqSource::new(config(tls_port, &tls)).await {
        Err(e) => e.to_string(),
        Ok(source) => source.fetch_all().await.unwrap_err().to_string(),
    };
    assert!(
        !refused.is_empty(),
        "no user named after the certificate yet"
    );

    images::exec(
        &container,
        &["rabbitmqctl", "add_user", CLIENT, "unused-password"],
    )
    .await;
    images::exec(
        &container,
        &[
            "rabbitmqctl",
            "set_permissions",
            "-p",
            "/",
            CLIENT,
            ".*",
            ".*",
            ".*",
        ],
    )
    .await;
    let source = RabbitMqSource::new(config(tls_port, &tls)).await.unwrap();
    let mut ids: Vec<i64> = source
        .fetch_all()
        .await
        .unwrap()
        .iter()
        .map(|r| r["id"].as_i64().unwrap())
        .collect();
    ids.sort_unstable();
    assert_eq!(ids, vec![1, 2, 3], "{}", json!(ids));
}
