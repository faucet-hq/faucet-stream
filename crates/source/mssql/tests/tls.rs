//! The SQL Server source over TLS (#967) against an `mcr.microsoft.com/mssql/
//! server` with `forceencryption` and the fixture's certificate: `require`
//! with the fixture CA and `trust_server_certificate` read over an encrypted
//! session; an unknown CA and `disable` are refused with a typed error.
//!
//! The image does not start on Apple-silicon hosts; this test runs in CI.

use faucet_common_mssql::{MssqlConnectionConfig, MssqlTls, MssqlTlsMode};
use faucet_conformance::containers;
use faucet_conformance::tls::{TlsFixture, files, images};
use faucet_core::{FaucetError, Source};
use faucet_source_mssql::{MssqlSource, MssqlSourceConfig};
use serde_json::Value;
use std::path::PathBuf;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::{GenericImage, ImageExt};

const PASSWORD: &str = "yourStrong(!)Password";
const ENCODED_PW: &str = "yourStrong%28%21%29Password";
const MSSQL_CONF: &str = "[network]\ntlscert = /certs/server.pem\ntlskey = /certs/server.key\n\
                          tlsprotocols = 1.2\nforceencryption = 1\n";
const QUERY: &str = "SELECT CAST(encrypt_option AS NVARCHAR(10)) AS encrypted \
                     FROM sys.dm_exec_connections WHERE session_id = @@SPID";

async fn read(
    port: u16,
    mode: MssqlTlsMode,
    ca: Option<PathBuf>,
) -> Result<Vec<Value>, FaucetError> {
    let mut config = MssqlSourceConfig::new(
        format!("mssql://sa:{ENCODED_PW}@localhost:{port}/master"),
        QUERY,
    );
    config.connection = MssqlConnectionConfig {
        tls: MssqlTls {
            mode,
            ca_cert_path: ca,
        },
        ..config.connection
    };
    MssqlSource::new(config).await?.fetch_all().await
}

#[tokio::test(flavor = "multi_thread")]
async fn reads_over_tls_and_refuses_bad_certificates() {
    let tls = TlsFixture::generate();
    let image = GenericImage::new("mcr.microsoft.com/mssql/server", "2022-CU14-ubuntu-22.04")
        .with_exposed_port(1433.tcp())
        .with_wait_for(WaitFor::message_on_stdout(
            "SQL Server is now ready for client connections",
        ));
    let server = containers::start(|| {
        tls.mount(image.clone(), images::CERT_DIR)
            .with_copy_to("/var/opt/mssql/mssql.conf", MSSQL_CONF.as_bytes().to_vec())
            .with_env_var("ACCEPT_EULA", "Y")
            .with_env_var("MSSQL_SA_PASSWORD", PASSWORD)
    })
    .await;
    let port = server.get_host_port_ipv4(1433).await.unwrap();

    for (name, mode, ca) in [
        ("require", MssqlTlsMode::Require, Some(tls.ca_path())),
        ("prefer", MssqlTlsMode::Prefer, Some(tls.ca_path())),
        (
            "trust_server_certificate",
            MssqlTlsMode::TrustServerCertificate,
            None,
        ),
    ] {
        let rows = read(port, mode, ca)
            .await
            .unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(
            rows,
            vec![serde_json::json!({"encrypted": "TRUE"})],
            "{name}"
        );
    }

    for (name, mode, ca) in [
        (
            "unknown_ca",
            MssqlTlsMode::Require,
            Some(tls.path(files::OTHER_CA)),
        ),
        ("disable", MssqlTlsMode::Disable, None),
    ] {
        let outcome = read(port, mode, ca).await;
        assert!(
            matches!(&outcome, Err(FaucetError::Config(_))),
            "{name} must be refused with a typed error: {outcome:?}"
        );
    }
}
