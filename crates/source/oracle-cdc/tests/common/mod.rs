//! Shared Oracle Free container harness. Tests skip (return `None`) when the
//! Oracle Instant Client library cannot be loaded or Docker is unavailable.

#![allow(dead_code)]

use std::time::Duration;

use faucet_common_oracle::OracleConnectionConfig;
use faucet_common_oracle::oracle;
use faucet_conformance::containers::{self, StartOptions};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

pub const USER: &str = "FAUCET";
pub const PASSWORD: &str = "faucet";

pub fn client_available() -> bool {
    match oracle::Version::client() {
        Ok(_) => true,
        Err(e) => {
            containers::backend_missing(&format!(
                "Oracle integration test: Instant Client unavailable: {e}"
            ));
            false
        }
    }
}

pub async fn start_oracle() -> Option<(ContainerAsync<GenericImage>, OracleConnectionConfig)> {
    if !client_available() {
        return None;
    }
    // Oracle Free starts only where it is required (the nightly job), so
    // only there is a failed start worth another attempt.
    let attempts = if containers::required(containers::REQUIRE_ORACLE) {
        containers::DEFAULT_ATTEMPTS
    } else {
        1
    };
    let opts = StartOptions::default()
        .startup_timeout(Duration::from_secs(900))
        .attempts(attempts)
        .require_env(containers::REQUIRE_ORACLE);
    let container = containers::start_or_skip(
        || {
            GenericImage::new("gvenzl/oracle-free", "23-slim")
                .with_exposed_port(1521.tcp())
                .with_wait_for(WaitFor::message_on_stdout("DATABASE IS READY TO USE!"))
                .with_env_var("ORACLE_PASSWORD", "faucetsys")
                .with_env_var("APP_USER", USER)
                .with_env_var("APP_USER_PASSWORD", PASSWORD)
        },
        &opts,
    )
    .await?;
    let port = container
        .get_host_port_ipv4(1521)
        .await
        .expect("oracle container port");
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

/// Put the database in ARCHIVELOG mode with the database-wide supplemental
/// logging LogMiner needs (SYSDBA, CDB root), then grant the capture
/// privileges to the app user.
///
/// The image's online logs were written in NOARCHIVELOG mode and never
/// archived, so LogMiner cannot mine them (ORA-01291) even though `V$LOG`
/// still lists them. Switching through every group once overwrites them, so
/// all redo still listed afterwards is archived or current — what a database
/// that has always run in ARCHIVELOG mode looks like.
pub async fn enable_logminer(
    container: &ContainerAsync<GenericImage>,
    app: &OracleConnectionConfig,
) {
    let out = sysdba(container, ENABLE_ARCHIVELOG_SCRIPT).await;
    assert_archivelog_enabled(&out);
    wait_for_pdb(app).await;
    wait_for_minable_transactions(app).await;
    exec(
        &sys_config(app),
        &[
            "GRANT LOGMINING, SELECT ANY TRANSACTION, SELECT_CATALOG_ROLE, EXECUTE_CATALOG_ROLE TO FAUCET",
        ],
    )
    .await;
}

/// SQL*Plus script that restarts into ARCHIVELOG mode, turns on minimal
/// supplemental logging, and archives every pre-existing online log group.
pub const ENABLE_ARCHIVELOG_SCRIPT: &str = "WHENEVER SQLERROR EXIT FAILURE
SHUTDOWN IMMEDIATE
STARTUP MOUNT
ALTER DATABASE ARCHIVELOG;
ALTER DATABASE OPEN;
ALTER PLUGGABLE DATABASE ALL OPEN;
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
DECLARE
  groups NUMBER;
BEGIN
  SELECT COUNT(*) INTO groups FROM V$LOG;
  FOR i IN 1 .. groups + 1 LOOP
    EXECUTE IMMEDIATE 'ALTER SYSTEM ARCHIVE LOG CURRENT';
  END LOOP;
END;
/
SELECT 'LOG_MODE=' || LOG_MODE FROM V$DATABASE;
SELECT 'UNARCHIVED=' || COUNT(*) FROM V$LOG WHERE ARCHIVED = 'NO' AND STATUS <> 'CURRENT';";

/// Panic unless the [`ENABLE_ARCHIVELOG_SCRIPT`] output shows ARCHIVELOG
/// mode with every non-current online log archived.
pub fn assert_archivelog_enabled(out: &str) {
    assert!(
        out.contains("LOG_MODE=ARCHIVELOG") && out.contains("UNARCHIVED=0"),
        "archivelog + supplemental logging: {out}"
    );
}

/// Wait until the app user can open a session in FREEPDB1 again after the
/// restart.
pub async fn wait_for_pdb(app: &OracleConnectionConfig) {
    let cfg = app.clone();
    tokio::task::spawn_blocking(move || {
        let connect = cfg.resolve_connect_string().unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(300);
        loop {
            match oracle::Connection::connect(&cfg.username, &cfg.password, &connect) {
                Ok(_) => return,
                Err(e) if std::time::Instant::now() >= deadline => {
                    panic!(
                        "FREEPDB1 did not accept {} after the restart: {e}",
                        cfg.username
                    )
                }
                Err(_) => std::thread::sleep(Duration::from_secs(2)),
            }
        }
    })
    .await
    .expect("join");
}

/// Open transactions in the PDB that began before the oldest archived log: a
/// capture anchored now would reach back to their start, into redo LogMiner
/// cannot read.
pub const UNMINABLE_TRANSACTIONS_SQL: &str = "SELECT TO_CHAR(COUNT(*)) FROM V$TRANSACTION \
    WHERE START_SCN < (SELECT MIN(FIRST_CHANGE#) FROM V$ARCHIVED_LOG WHERE STATUS = 'A' \
    AND RESETLOGS_CHANGE# = (SELECT RESETLOGS_CHANGE# FROM V$DATABASE))";

/// Wait until no transaction left over from the restart (an instance-startup
/// transaction can report a `START_SCN` of 0) is still open, so
/// `capture_resume_position` anchors inside the archived redo.
pub async fn wait_for_minable_transactions(app: &OracleConnectionConfig) {
    let sys = sys_config(app);
    let deadline = std::time::Instant::now() + Duration::from_secs(300);
    loop {
        let open = query_strings(&sys, UNMINABLE_TRANSACTIONS_SQL).await;
        if open == [Some("0".to_string())] {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "transactions from before ARCHIVELOG mode are still open: {open:?}"
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// A bookmark at the database's current SCN with nothing emitted there, so a
/// capture mines only what happens from now on.
pub async fn current_scn_anchor(cfg: &OracleConnectionConfig) -> serde_json::Value {
    let scn: u64 = query_strings(cfg, "SELECT TO_CHAR(CURRENT_SCN) FROM V$DATABASE")
        .await
        .first()
        .cloned()
        .flatten()
        .expect("current SCN")
        .parse()
        .expect("numeric SCN");
    scn_anchor(scn)
}

/// The bookmark for "everything after `scn`".
pub fn scn_anchor(scn: u64) -> serde_json::Value {
    serde_json::json!({"commit_scn": scn, "restart_scn": scn + 1, "committed_xids": []})
}

/// Run a SQL*Plus script as SYSDBA in the CDB root; returns its output.
pub async fn sysdba(container: &ContainerAsync<GenericImage>, script: &str) -> String {
    use testcontainers::core::ExecCommand;
    // A quoted heredoc passes the script through the shell untouched.
    let cmd = format!("sqlplus -s / as sysdba <<'FAUCET_SQL_EOF'\n{script}\nFAUCET_SQL_EOF\n");
    let mut out = container
        .exec(ExecCommand::new(["bash", "-c", cmd.as_str()]))
        .await
        .expect("exec sqlplus");
    String::from_utf8(out.stdout_to_vec().await.unwrap_or_default()).unwrap_or_default()
}

#[test]
fn archivelog_check_requires_every_old_log_archived() {
    assert_archivelog_enabled("LOG_MODE=ARCHIVELOG\nUNARCHIVED=0\n");
    for bad in [
        "LOG_MODE=NOARCHIVELOG\nUNARCHIVED=0",
        "LOG_MODE=ARCHIVELOG\nUNARCHIVED=1",
    ] {
        assert!(std::panic::catch_unwind(|| assert_archivelog_enabled(bad)).is_err());
    }
}

#[test]
fn scn_anchor_resumes_after_the_scn() {
    let a = scn_anchor(2_300_000);
    assert_eq!(a["commit_scn"], 2_300_000);
    assert_eq!(a["restart_scn"], 2_300_001);
}
