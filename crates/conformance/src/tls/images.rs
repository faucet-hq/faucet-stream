//! Container requests for servers that only accept TLS, built on a
//! [`TlsFixture`].
//!
//! Each builder mounts the fixture at [`CERT_DIR`] and configures the server
//! so a plaintext client is refused, which is how a test proves the connector
//! really negotiated TLS. Start one with
//! [`containers::start`](crate::containers::start):
//!
//! ```no_run
//! # async fn ex() {
//! use faucet_conformance::{containers, tls::{TlsFixture, images}};
//!
//! let tls = TlsFixture::generate();
//! let pg = containers::start(|| images::postgres(&tls)).await;
//! let port = pg.get_host_port_ipv4(images::POSTGRES_PORT).await.unwrap();
//! # drop(port);
//! # }
//! ```

use std::borrow::Cow;

use testcontainers::core::CmdWaitFor;
use testcontainers::core::wait::LogWaitStrategy;
use testcontainers::core::{
    ContainerPort, ContainerState, ExecCommand, IntoContainerPort, WaitFor,
};
use testcontainers::{
    ContainerAsync, ContainerRequest, GenericImage, Image, ImageExt, TestcontainersError,
};

use super::{TlsFixture, files};

/// Where every builder mounts the fixture inside the container.
pub const CERT_DIR: &str = "/certs";

/// PostgreSQL image (Docker Hub `postgres`).
pub const POSTGRES_IMAGE: (&str, &str) = ("postgres", "16-alpine");
/// PostgreSQL port.
pub const POSTGRES_PORT: u16 = 5432;
/// Superuser name and password of [`postgres`].
pub const POSTGRES_USER: (&str, &str) = ("postgres", "postgres");

/// MySQL image.
pub const MYSQL_IMAGE: (&str, &str) = ("mysql", "8.1");
/// MySQL port.
pub const MYSQL_PORT: u16 = 3306;
/// Root password of [`mysql`].
pub const MYSQL_ROOT_PASSWORD: &str = "root";
/// Database [`mysql`] creates.
pub const MYSQL_DATABASE: &str = "test";

/// Redis image.
pub const REDIS_IMAGE: (&str, &str) = ("redis", "7.4");
/// Redis TLS port (the plaintext port is disabled).
pub const REDIS_PORT: u16 = 6379;

/// MongoDB image.
pub const MONGO_IMAGE: (&str, &str) = ("mongo", "7.0");
/// MongoDB port.
pub const MONGO_PORT: u16 = 27017;

/// NATS image.
pub const NATS_IMAGE: (&str, &str) = ("nats", "2.10");
/// NATS client port.
pub const NATS_PORT: u16 = 4222;

/// Elasticsearch image.
pub const ELASTICSEARCH_IMAGE: (&str, &str) =
    ("docker.elastic.co/elasticsearch/elasticsearch", "8.15.3");
/// Elasticsearch HTTPS port.
pub const ELASTICSEARCH_PORT: u16 = 9200;
/// The `elastic` superuser password of [`elasticsearch`].
pub const ELASTICSEARCH_PASSWORD: &str = "faucet-test";

/// Kafka image (the JVM image: it ships the `kafka-storage.sh` tool).
pub const KAFKA_IMAGE: (&str, &str) = ("apache/kafka", "3.9.1");
/// Kafka listener speaking TLS (client certificate requested, not required).
pub const KAFKA_SSL_PORT: u16 = 9093;
/// Kafka listener speaking SASL without TLS.
pub const KAFKA_SASL_PLAINTEXT_PORT: u16 = 9094;
/// Kafka listener speaking SASL over TLS.
pub const KAFKA_SASL_SSL_PORT: u16 = 9095;
/// The SASL user every Kafka mechanism (PLAIN, SCRAM-SHA-256/512) knows.
pub const KAFKA_USER: (&str, &str) = ("alice", "alice-secret");

fn cert(name: &str) -> String {
    format!("{CERT_DIR}/{name}")
}

const PG_HBA: &str = "\
local all all trust
hostssl all all all scram-sha-256
hostssl replication all all scram-sha-256
hostnossl all all all reject
hostnossl replication all all reject
";

/// PostgreSQL with `ssl=on` whose `pg_hba.conf` rejects every non-TLS TCP
/// connection. `wal_level=logical`, so logical replication works too.
pub fn postgres(tls: &TlsFixture) -> ContainerRequest<GenericImage> {
    let (name, tag) = POSTGRES_IMAGE;
    let key = "/var/lib/postgresql/server.key";
    let script = format!(
        "cp {src} {key} && chown postgres:postgres {key} && chmod 600 {key} && \
         exec docker-entrypoint.sh postgres -c ssl=on -c ssl_cert_file={cert} \
         -c ssl_key_file={key} -c ssl_ca_file={ca} -c hba_file={hba} \
         -c wal_level=logical -c max_replication_slots=10 -c max_wal_senders=10",
        src = cert(files::SERVER_KEY),
        cert = cert(files::SERVER_CERT),
        ca = cert(files::CA),
        hba = cert("pg_hba.conf"),
    );
    let image = GenericImage::new(name, tag)
        .with_exposed_port(POSTGRES_PORT.tcp())
        .with_wait_for(WaitFor::log(
            LogWaitStrategy::stdout_or_stderr("database system is ready to accept connections")
                .with_times(2),
        ))
        .with_entrypoint("sh");
    tls.mount(image, CERT_DIR)
        .with_copy_to(cert("pg_hba.conf"), PG_HBA.as_bytes().to_vec())
        .with_env_var("POSTGRES_USER", POSTGRES_USER.0)
        .with_env_var("POSTGRES_PASSWORD", POSTGRES_USER.1)
        .with_cmd(["-c".to_string(), script])
}

/// MySQL with `require_secure_transport=ON` and a row-based binlog, so both
/// queries and binlog replication must use TLS.
pub fn mysql(tls: &TlsFixture) -> ContainerRequest<GenericImage> {
    let (name, tag) = MYSQL_IMAGE;
    let image = GenericImage::new(name, tag)
        .with_exposed_port(MYSQL_PORT.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "X Plugin ready for connections. Bind-address",
        ))
        .with_wait_for(WaitFor::message_on_stderr(
            "/usr/sbin/mysqld: ready for connections.",
        ));
    tls.mount(image, CERT_DIR)
        .with_env_var("MYSQL_ROOT_PASSWORD", MYSQL_ROOT_PASSWORD)
        .with_env_var("MYSQL_DATABASE", MYSQL_DATABASE)
        .with_cmd([
            "--require-secure-transport=ON".to_string(),
            format!("--ssl-ca={}", cert(files::CA)),
            format!("--ssl-cert={}", cert(files::SERVER_CERT)),
            format!("--ssl-key={}", cert(files::SERVER_KEY)),
            "--server-id=1".to_string(),
            "--log-bin=mysql-bin".to_string(),
            "--binlog-format=ROW".to_string(),
            "--binlog-row-image=FULL".to_string(),
            "--binlog-row-metadata=FULL".to_string(),
        ])
}

/// Redis listening only on its TLS port; client certificates are optional.
pub fn redis(tls: &TlsFixture) -> ContainerRequest<GenericImage> {
    redis_with(tls, "optional")
}

/// [`redis`] that requires a client certificate signed by the fixture CA.
pub fn redis_mutual(tls: &TlsFixture) -> ContainerRequest<GenericImage> {
    redis_with(tls, "yes")
}

fn redis_with(tls: &TlsFixture, auth_clients: &str) -> ContainerRequest<GenericImage> {
    let (name, tag) = REDIS_IMAGE;
    let image = GenericImage::new(name, tag)
        .with_exposed_port(REDIS_PORT.tcp())
        .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"));
    tls.mount(image, CERT_DIR).with_cmd([
        "redis-server".to_string(),
        "--port".to_string(),
        "0".to_string(),
        "--tls-port".to_string(),
        REDIS_PORT.to_string(),
        "--tls-cert-file".to_string(),
        cert(files::SERVER_CERT),
        "--tls-key-file".to_string(),
        cert(files::SERVER_KEY),
        "--tls-ca-cert-file".to_string(),
        cert(files::CA),
        "--tls-auth-clients".to_string(),
        auth_clients.to_string(),
    ])
}

/// MongoDB with `--tlsMode requireTLS`; client certificates are optional.
pub fn mongo(tls: &TlsFixture) -> ContainerRequest<GenericImage> {
    let (name, tag) = MONGO_IMAGE;
    let image = GenericImage::new(name, tag)
        .with_exposed_port(MONGO_PORT.tcp())
        .with_wait_for(WaitFor::message_on_stdout("Waiting for connections"));
    tls.mount(image, CERT_DIR).with_cmd([
        "--tlsMode".to_string(),
        "requireTLS".to_string(),
        "--tlsCertificateKeyFile".to_string(),
        cert(files::SERVER_CERT_KEY),
        "--tlsCAFile".to_string(),
        cert(files::CA),
        "--tlsAllowConnectionsWithoutCertificates".to_string(),
        "--bind_ip_all".to_string(),
    ])
}

/// NATS with JetStream that requires TLS on its client port.
pub fn nats(tls: &TlsFixture) -> ContainerRequest<GenericImage> {
    nats_with(tls, false)
}

/// [`nats`] that also requires a client certificate signed by the fixture CA.
pub fn nats_mutual(tls: &TlsFixture) -> ContainerRequest<GenericImage> {
    nats_with(tls, true)
}

fn nats_with(tls: &TlsFixture, verify_clients: bool) -> ContainerRequest<GenericImage> {
    let (name, tag) = NATS_IMAGE;
    let image = GenericImage::new(name, tag)
        .with_exposed_port(NATS_PORT.tcp())
        .with_wait_for(WaitFor::message_on_stderr("Server is ready"));
    let mut cmd = vec![
        "--tls".to_string(),
        "--tlscert".to_string(),
        cert(files::SERVER_CERT),
        "--tlskey".to_string(),
        cert(files::SERVER_KEY),
    ];
    if verify_clients {
        cmd.extend([
            "--tlsverify".to_string(),
            "--tlscacert".to_string(),
            cert(files::CA),
        ]);
    }
    cmd.push("-js".to_string());
    tls.mount(image, CERT_DIR).with_cmd(cmd)
}

/// A single-node Elasticsearch with security on: HTTPS only, and every
/// request needs the `elastic` user ([`ELASTICSEARCH_PASSWORD`]).
pub fn elasticsearch(tls: &TlsFixture) -> ContainerRequest<GenericImage> {
    let (name, tag) = ELASTICSEARCH_IMAGE;
    let dir = "/usr/share/elasticsearch/config/certs";
    let image = GenericImage::new(name, tag)
        .with_exposed_port(ELASTICSEARCH_PORT.tcp())
        .with_wait_for(WaitFor::message_on_stdout("\"message\":\"started"));
    tls.mount(image, dir)
        .with_env_var("discovery.type", "single-node")
        .with_env_var("xpack.security.enabled", "true")
        .with_env_var("xpack.security.http.ssl.enabled", "true")
        .with_env_var("xpack.security.http.ssl.key", "certs/server.key")
        .with_env_var("xpack.security.http.ssl.certificate", "certs/server.pem")
        .with_env_var(
            "xpack.security.http.ssl.certificate_authorities",
            "certs/ca.pem",
        )
        .with_env_var("xpack.security.transport.ssl.enabled", "false")
        .with_env_var("xpack.ml.enabled", "false")
        // The token service (bearer auth tests) needs more than the basic license.
        .with_env_var("xpack.license.self_generated.type", "trial")
        .with_env_var("ELASTIC_PASSWORD", ELASTICSEARCH_PASSWORD)
        .with_env_var("ES_JAVA_OPTS", "-Xms512m -Xmx512m")
}

/// Run `cmd` inside `container`, wait for it to exit 0 and return its stdout.
/// Panics with the command's stderr otherwise: a test fixture step failed.
pub async fn exec<I: Image>(container: &ContainerAsync<I>, cmd: &[&str]) -> String {
    let mut result = container
        .exec(
            ExecCommand::new(cmd.iter().map(|c| (*c).to_string()))
                .with_cmd_ready_condition(CmdWaitFor::exit()),
        )
        .await
        .unwrap_or_else(|e| panic!("exec {cmd:?}: {e}"));
    let stdout = result.stdout_to_vec().await.unwrap_or_default();
    let stderr = result.stderr_to_vec().await.unwrap_or_default();
    let code = result.exit_code().await.ok().flatten();
    assert_eq!(
        code,
        Some(0),
        "exec {cmd:?} failed: {}",
        String::from_utf8_lossy(&stderr)
    );
    String::from_utf8_lossy(&stdout).into_owned()
}

/// Run SQL in a [`postgres`] container over its local socket (no TLS needed).
pub async fn psql<I: Image>(container: &ContainerAsync<I>, sql: &str) -> String {
    let user = POSTGRES_USER.0;
    exec(
        container,
        &["psql", "-U", user, "-v", "ON_ERROR_STOP=1", "-Atc", sql],
    )
    .await
}

/// Run SQL in a [`mysql`] container as root over its local socket.
pub async fn mysql_exec<I: Image>(container: &ContainerAsync<I>, sql: &str) -> String {
    let password = format!("-p{MYSQL_ROOT_PASSWORD}");
    exec(
        container,
        &[
            "mysql",
            "-uroot",
            &password,
            "-N",
            "-B",
            MYSQL_DATABASE,
            "-e",
            sql,
        ],
    )
    .await
}

/// Evaluate JavaScript with `mongosh` in a [`mongo`] container, over TLS
/// with the mounted CA, and return what it prints.
pub async fn mongosh<I: Image>(container: &ContainerAsync<I>, script: &str) -> String {
    let ca = cert(files::CA);
    exec(
        container,
        &[
            "mongosh",
            "--quiet",
            "--tls",
            "--tlsCAFile",
            &ca,
            "--host",
            "localhost",
            "--eval",
            script,
        ],
    )
    .await
}

const KAFKA_START_SCRIPT: &str = "/tmp/faucet_kafka_start.sh";

/// A single KRaft Kafka node with three client listeners: [`KAFKA_SSL_PORT`]
/// (TLS), [`KAFKA_SASL_PLAINTEXT_PORT`] and [`KAFKA_SASL_SSL_PORT`] (SASL
/// `PLAIN`, `SCRAM-SHA-256` and `SCRAM-SHA-512` for [`KAFKA_USER`]). There is
/// no plaintext client listener.
///
/// The advertised listeners need the mapped host ports, so the broker is
/// configured and started after the container is up, as
/// `testcontainers-modules`' Kafka image does.
#[derive(Debug, Clone)]
pub struct TlsKafka {
    _private: (),
}

impl TlsKafka {
    /// The broker as a container request with the fixture mounted.
    pub fn request(tls: &TlsFixture) -> ContainerRequest<TlsKafka> {
        tls.mount(TlsKafka { _private: () }, CERT_DIR)
    }
}

/// Shorthand for [`TlsKafka::request`].
pub fn kafka(tls: &TlsFixture) -> ContainerRequest<TlsKafka> {
    TlsKafka::request(tls)
}

fn kafka_properties(ssl: u16, sasl_plain: u16, sasl_ssl: u16) -> String {
    let (user, password) = KAFKA_USER;
    let plain = format!(
        "org.apache.kafka.common.security.plain.PlainLoginModule required user_{user}=\"{password}\";"
    );
    let scram = "org.apache.kafka.common.security.scram.ScramLoginModule required;";
    let mut props = vec![
        "process.roles=broker,controller".to_string(),
        "node.id=1".to_string(),
        "controller.quorum.voters=1@localhost:9096".to_string(),
        "controller.listener.names=CONTROLLER".to_string(),
        "inter.broker.listener.name=BROKER".to_string(),
        "listeners=BROKER://0.0.0.0:9092,SSL://0.0.0.0:9093,SASL_PLAINTEXT://0.0.0.0:9094,\
         SASL_SSL://0.0.0.0:9095,CONTROLLER://0.0.0.0:9096"
            .to_string(),
        format!(
            "advertised.listeners=BROKER://localhost:9092,SSL://127.0.0.1:{ssl},\
             SASL_PLAINTEXT://127.0.0.1:{sasl_plain},SASL_SSL://127.0.0.1:{sasl_ssl}"
        ),
        "listener.security.protocol.map=BROKER:PLAINTEXT,CONTROLLER:PLAINTEXT,SSL:SSL,\
         SASL_PLAINTEXT:SASL_PLAINTEXT,SASL_SSL:SASL_SSL"
            .to_string(),
        "sasl.enabled.mechanisms=PLAIN,SCRAM-SHA-256,SCRAM-SHA-512".to_string(),
        "ssl.keystore.type=PEM".to_string(),
        format!("ssl.keystore.location={}", cert(files::SERVER_BUNDLE)),
        "ssl.truststore.type=PEM".to_string(),
        format!("ssl.truststore.location={}", cert(files::CA)),
        "ssl.client.auth=requested".to_string(),
        "offsets.topic.replication.factor=1".to_string(),
        "transaction.state.log.replication.factor=1".to_string(),
        "transaction.state.log.min.isr=1".to_string(),
        "group.initial.rebalance.delay.ms=0".to_string(),
        "auto.create.topics.enable=true".to_string(),
        "log.dirs=/tmp/kraft-logs".to_string(),
    ];
    for listener in ["sasl_plaintext", "sasl_ssl"] {
        props.push(format!(
            "listener.name.{listener}.plain.sasl.jaas.config={plain}"
        ));
        for mech in ["scram-sha-256", "scram-sha-512"] {
            props.push(format!(
                "listener.name.{listener}.{mech}.sasl.jaas.config={scram}"
            ));
        }
    }
    props.join("\n")
}

fn kafka_start_script(ssl: u16, sasl_plain: u16, sasl_ssl: u16) -> String {
    let (user, password) = KAFKA_USER;
    format!(
        "#!/usr/bin/env bash\nset -e\ncat > /tmp/server.properties <<'EOF'\n{props}\nEOF\n\
         /opt/kafka/bin/kafka-storage.sh format -t 5L6g3nShT-eMCtK--X86sw -c /tmp/server.properties \
         --add-scram 'SCRAM-SHA-256=[name={user},password={password}]' \
         --add-scram 'SCRAM-SHA-512=[name={user},password={password}]'\n\
         exec /opt/kafka/bin/kafka-server-start.sh /tmp/server.properties\n",
        props = kafka_properties(ssl, sasl_plain, sasl_ssl),
    )
}

impl Image for TlsKafka {
    fn name(&self) -> &str {
        KAFKA_IMAGE.0
    }

    fn tag(&self) -> &str {
        KAFKA_IMAGE.1
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        Vec::new()
    }

    fn entrypoint(&self) -> Option<&str> {
        Some("bash")
    }

    fn cmd(&self) -> impl IntoIterator<Item = impl Into<Cow<'_, str>>> {
        vec![
            "-c".to_string(),
            format!(
                "while [ ! -f {KAFKA_START_SCRIPT} ]; do sleep 0.1; done; \
                 chmod 755 {KAFKA_START_SCRIPT} && {KAFKA_START_SCRIPT}"
            ),
        ]
    }

    fn expose_ports(&self) -> &[ContainerPort] {
        const PORTS: [ContainerPort; 3] = [
            ContainerPort::Tcp(KAFKA_SSL_PORT),
            ContainerPort::Tcp(KAFKA_SASL_PLAINTEXT_PORT),
            ContainerPort::Tcp(KAFKA_SASL_SSL_PORT),
        ];
        &PORTS
    }

    fn exec_after_start(
        &self,
        cs: ContainerState,
    ) -> Result<Vec<ExecCommand>, TestcontainersError> {
        let script = kafka_start_script(
            cs.host_port_ipv4(KAFKA_SSL_PORT.tcp())?,
            cs.host_port_ipv4(KAFKA_SASL_PLAINTEXT_PORT.tcp())?,
            cs.host_port_ipv4(KAFKA_SASL_SSL_PORT.tcp())?,
        );
        let write = format!(
            "cat > {KAFKA_START_SCRIPT}.tmp <<'FAUCET_EOF'\n{script}FAUCET_EOF\n\
             mv {KAFKA_START_SCRIPT}.tmp {KAFKA_START_SCRIPT}"
        );
        Ok(vec![
            ExecCommand::new(["bash".to_string(), "-c".to_string(), write])
                .with_container_ready_conditions(vec![WaitFor::message_on_stdout(
                    "Kafka Server started",
                )]),
        ])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn copies<I: Image>(req: &ContainerRequest<I>) -> usize {
        req.copy_to_sources().count()
    }

    #[test]
    fn every_builder_mounts_the_fixture() {
        let tls = TlsFixture::generate();
        let files = tls.contents().len();
        assert_eq!(copies(&postgres(&tls)), files + 1);
        assert_eq!(copies(&mysql(&tls)), files);
        assert_eq!(copies(&redis(&tls)), files);
        assert_eq!(copies(&redis_mutual(&tls)), files);
        assert_eq!(copies(&nats_mutual(&tls)), files);
        assert_eq!(copies(&mongo(&tls)), files);
        assert_eq!(copies(&nats(&tls)), files);
        assert_eq!(copies(&elasticsearch(&tls)), files);
        assert_eq!(copies(&kafka(&tls)), files);
    }

    #[test]
    fn kafka_script_advertises_the_mapped_ports_and_seeds_scram_users() {
        let script = kafka_start_script(1, 2, 3);
        assert!(script.contains("SSL://127.0.0.1:1"));
        assert!(script.contains("SASL_PLAINTEXT://127.0.0.1:2"));
        assert!(script.contains("SASL_SSL://127.0.0.1:3"));
        assert!(script.contains("SCRAM-SHA-512=[name=alice,password=alice-secret]"));
        assert!(script.contains("listener.name.sasl_ssl.scram-sha-256.sasl.jaas.config"));
        let image = TlsKafka { _private: () };
        assert_eq!(image.name(), KAFKA_IMAGE.0);
        assert_eq!(image.tag(), KAFKA_IMAGE.1);
        assert_eq!(image.entrypoint(), Some("bash"));
        assert_eq!(image.expose_ports().len(), 3);
        assert!(image.ready_conditions().is_empty());
        let cmd: Vec<String> = image
            .cmd()
            .into_iter()
            .map(|c| c.into().into_owned())
            .collect();
        assert!(cmd[1].contains(KAFKA_START_SCRIPT));
    }
}
