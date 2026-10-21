//! Binary (`VARBINARY`) key columns, against a real MySQL: every keyed
//! statement — delete, scoped cleanup, the rollback journal — must compare the
//! decoded bytes the upsert stored, never the encoded text.

#![allow(deprecated)] // the tests drive the 1.x lifecycle hooks directly (RFC 0014)
use faucet_core::rollback::{RollbackMode, RollbackOptions, RollbackWriteSpec};
use faucet_core::{SeenKeys, Sink, WriteMode, WriteSpec};
use faucet_sink_mysql::{MysqlColumnMapping, MysqlSink, MysqlSinkConfig};
use serde_json::json;
use std::collections::BTreeMap;
use testcontainers::ContainerAsync;
use testcontainers_modules::mysql::Mysql;

const A: &str = "AAECAwQFBgcICQoLDA0ODw=="; // 16 bytes 00..0f
const B: &str = "EBESExQVFhcYGRobHB0eHw=="; // 16 bytes 10..1f
const C: &str = "ICEiIyQlJicoKSorLC0uLw=="; // 16 bytes 20..2f

async fn start_mysql() -> (ContainerAsync<Mysql>, String) {
    let container: ContainerAsync<Mysql> =
        faucet_conformance::containers::start(Mysql::default).await;
    let port = container
        .get_host_port_ipv4(3306)
        .await
        .expect("mysql port");
    (container, format!("mysql://root@127.0.0.1:{port}/test"))
}

async fn exec(url: &str, sql: &str) {
    let pool = sqlx::MySqlPool::connect(url).await.expect("pool");
    sqlx::query(sql).execute(&pool).await.expect("exec");
    pool.close().await;
}

/// The hex ids in the table, sorted.
async fn ids(url: &str, table: &str) -> Vec<String> {
    let pool = sqlx::MySqlPool::connect(url).await.expect("pool");
    let out: Vec<String> = sqlx::query_scalar(&format!(
        "SELECT CAST(LOWER(HEX(id)) AS CHAR) FROM {table} ORDER BY id"
    ))
    .fetch_all(&pool)
    .await
    .expect("ids");
    pool.close().await;
    out
}

fn hex(b64: &str) -> String {
    match b64 {
        A => "000102030405060708090a0b0c0d0e0f",
        B => "101112131415161718191a1b1c1d1e1f",
        C => "202122232425262728292a2b2c2d2e2f",
        _ => unreachable!(),
    }
    .to_string()
}

fn config(
    url: &str,
    table: &str,
    mode: WriteMode,
    rollback: Option<RollbackWriteSpec>,
) -> MysqlSinkConfig {
    let mut c = MysqlSinkConfig::new(url, table).column_mapping(MysqlColumnMapping::AutoMap);
    c.write = WriteSpec {
        write_mode: mode,
        key: vec!["id".to_string()],
        delete_marker: None,
        rollback,
    };
    c
}

#[tokio::test(flavor = "multi_thread")]
async fn binary_keys_match_on_delete_cleanup_and_rollback() {
    let (_c, url) = start_mysql().await;
    exec(
        &url,
        "CREATE TABLE t (id VARBINARY(32) PRIMARY KEY, contact_id INT, v TEXT)",
    )
    .await;

    // Upsert three rows (stored as 16 decoded bytes each).
    let up = MysqlSink::new(config(&url, "t", WriteMode::Upsert, None))
        .await
        .unwrap();
    up.write_batch(&[
        json!({"id": A, "contact_id": 7, "v": "a"}),
        json!({"id": B, "contact_id": 7, "v": "b"}),
        json!({"id": C, "contact_id": 8, "v": "c"}),
    ])
    .await
    .unwrap();
    assert_eq!(ids(&url, "t").await, vec![hex(A), hex(B), hex(C)]);

    // A keyed delete matches the stored bytes.
    let del = MysqlSink::new(config(&url, "t", WriteMode::Delete, None))
        .await
        .unwrap();
    del.write_batch(&[json!({"id": C})]).await.unwrap();
    assert_eq!(ids(&url, "t").await, vec![hex(A), hex(B)], "C deleted");

    // Scoped cleanup keeps the row this run wrote and removes only the stale one.
    let mut seen = SeenKeys::new();
    seen.record_page(
        &[json!({"id": A, "contact_id": 7})],
        &["id".to_string()],
        1000,
    );
    let scope = BTreeMap::from([("contact_id".to_string(), json!(7))]);
    let deleted = up.cleanup_scope(&scope, &seen).await.unwrap();
    assert_eq!(deleted, 1, "only B is stale");
    assert_eq!(
        ids(&url, "t").await,
        vec![hex(A)],
        "the written row survives"
    );

    // The rollback journal finds the before-image of a binary key, so undoing
    // an upsert restores the row instead of deleting it.
    exec(&url, "ALTER TABLE t ADD COLUMN _faucet_run_id VARCHAR(64)").await;
    let spec = RollbackWriteSpec {
        run_id: "r1".into(),
        run_id_column: "_faucet_run_id".into(),
        journal: true,
        keep_previous: false,
    };
    let r1 = MysqlSink::new(config(&url, "t", WriteMode::Upsert, Some(spec)))
        .await
        .unwrap();
    r1.write_batch(&[json!({"id": A, "contact_id": 7, "v": "changed", "_faucet_run_id": "r1"})])
        .await
        .unwrap();
    let out = r1
        .rollback_run(
            "r1",
            &RollbackOptions {
                run_id_column: "_faucet_run_id".into(),
                mode: RollbackMode::Upsert,
                force: false,
                dry_run: false,
                later_runs: false,
            },
        )
        .await
        .unwrap();
    assert!(out.applied, "{out:?}");
    assert_eq!(
        ids(&url, "t").await,
        vec![hex(A)],
        "A restored, not deleted"
    );
    let pool = sqlx::MySqlPool::connect(&url).await.unwrap();
    let v: String = sqlx::query_scalar("SELECT v FROM t")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(v, "a", "the before-image is back");
    pool.close().await;
}
