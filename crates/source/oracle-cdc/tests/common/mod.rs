//! Shared Oracle Free container harness. Tests skip (return `None`) when the
//! Oracle Instant Client library cannot be loaded or Docker is unavailable.

#![allow(dead_code)]

use std::time::Duration;

use faucet_common_oracle::OracleConnectionConfig;
use faucet_common_oracle::oracle;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

pub const USER: &str = "FAUCET";
pub const PASSWORD: &str = "faucet";

pub fn client_available() -> bool {
    match oracle::Version::client() {
        Ok(_) => true,
        Err(e) => {
            eprintln!("skipping Oracle integration test: Instant Client unavailable: {e}");
            false
        }
    }
}

pub async fn start_oracle() -> Option<(ContainerAsync<GenericImage>, OracleConnectionConfig)> {
    if !client_available() {
        return None;
    }
    let image = GenericImage::new("gvenzl/oracle-free", "23-slim")
        .with_exposed_port(1521.tcp())
        .with_wait_for(WaitFor::message_on_stdout("DATABASE IS READY TO USE!"))
        .with_env_var("ORACLE_PASSWORD", "faucetsys")
        .with_env_var("APP_USER", USER)
        .with_env_var("APP_USER_PASSWORD", PASSWORD)
        .with_startup_timeout(Duration::from_secs(900));
    let container = match image.start().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("skipping Oracle integration test: container failed to start: {e}");
            return None;
        }
    };
    let port = container.get_host_port_ipv4(1521).await.ok()?;
    Some((
        container,
        OracleConnectionConfig::new("127.0.0.1", port, "FREEPDB1", USER, PASSWORD),
    ))
}

pub fn sys_config(app: &OracleConnectionConfig) -> OracleConnectionConfig {
    OracleConnectionConfig {
        username: "SYSTEM".into(),
        password: "faucetsys".into(),
        ..app.clone()
    }
}

/// Run statements on a fresh session, committing at the end.
pub async fn exec(cfg: &OracleConnectionConfig, statements: &[&str]) {
    let cfg = cfg.clone();
    let statements: Vec<String> = statements.iter().map(|s| s.to_string()).collect();
    tokio::task::spawn_blocking(move || {
        let conn = oracle::Connection::connect(
            &cfg.username,
            &cfg.password,
            cfg.resolve_connect_string().unwrap(),
        )
        .expect("connect");
        for s in &statements {
            conn.execute(s, &[]).unwrap_or_else(|e| panic!("{s}: {e}"));
        }
        conn.commit().expect("commit");
    })
    .await
    .expect("join");
}

/// First column of every row of `sql`, as text.
pub async fn query_strings(cfg: &OracleConnectionConfig, sql: &str) -> Vec<Option<String>> {
    let cfg = cfg.clone();
    let sql = sql.to_string();
    tokio::task::spawn_blocking(move || {
        let conn = oracle::Connection::connect(
            &cfg.username,
            &cfg.password,
            cfg.resolve_connect_string().unwrap(),
        )
        .expect("connect");
        conn.query_as::<Option<String>>(&sql, &[])
            .unwrap_or_else(|e| panic!("{sql}: {e}"))
            .map(|r| r.expect("row"))
            .collect()
    })
    .await
    .expect("join")
}

/// Turn on the database-wide supplemental logging LogMiner needs (SYSDBA,
/// CDB root) and grant the capture privileges to the app user.
pub async fn enable_logminer(
    container: &ContainerAsync<GenericImage>,
    app: &OracleConnectionConfig,
) {
    use testcontainers::core::ExecCommand;
    let mut out = container
        .exec(ExecCommand::new([
            "bash",
            "-c",
            "printf 'SHUTDOWN IMMEDIATE\nSTARTUP MOUNT\nALTER DATABASE ARCHIVELOG;\n\
             ALTER DATABASE OPEN;\nALTER PLUGGABLE DATABASE ALL OPEN;\n\
             ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;\n' | sqlplus -s / as sysdba",
        ]))
        .await
        .expect("exec sqlplus");
    let stdout =
        String::from_utf8(out.stdout_to_vec().await.unwrap_or_default()).unwrap_or_default();
    assert!(
        stdout.contains("Database altered"),
        "archivelog + supplemental logging: {stdout}"
    );
    exec(
        &sys_config(app),
        &[
            "GRANT LOGMINING, SELECT ANY TRANSACTION, SELECT_CATALOG_ROLE, EXECUTE_CATALOG_ROLE TO FAUCET",
        ],
    )
    .await;
}

/// Run a SQL*Plus script as SYSDBA in the CDB root; returns its output.
pub async fn sysdba(container: &ContainerAsync<GenericImage>, script: &str) -> String {
    use testcontainers::core::ExecCommand;
    let cmd = format!(
        "printf '%s\\n' \"{}\" | sqlplus -s / as sysdba",
        script.replace('"', "\\\"")
    );
    let mut out = container
        .exec(ExecCommand::new(["bash", "-c", cmd.as_str()]))
        .await
        .expect("exec sqlplus");
    String::from_utf8(out.stdout_to_vec().await.unwrap_or_default()).unwrap_or_default()
}
