//! #789 group C1 regressions for the MySQL sink against one MySQL container:
//! binary columns from base64 (SQL-02), upserts that keep absent columns
//! (SQL-10), JSON-mode drift baseline (SQL-20), case-variant rollback keys
//! (SQL-32) and NOT NULL relaxation that keeps the column's definition
//! (SQL-33). Requires Docker.

use faucet_core::rollback::{RollbackMode, RollbackOptions, RollbackWriteSpec};
use faucet_core::{SchemaEvolution, Sink, WriteMode, WriteSpec};
use faucet_sink_mysql::{MysqlColumnMapping, MysqlSink, MysqlSinkConfig};
use serde_json::json;
use sqlx::Row;
use testcontainers::ContainerAsync;
use testcontainers_modules::mysql::Mysql;

async fn start_mysql() -> (ContainerAsync<Mysql>, String) {
    let container = faucet_conformance::containers::start(Mysql::default).await;
    let port = container
        .get_host_port_ipv4(3306)
        .await
        .expect("mysql port");
    (container, format!("mysql://root@127.0.0.1:{port}/test"))
}

async fn exec(pool: &sqlx::MySqlPool, sql: &str) {
    sqlx::query(sql).execute(pool).await.expect(sql);
}

fn upsert(url: &str, table: &str, rollback: Option<RollbackWriteSpec>) -> MysqlSinkConfig {
    let mut c = MysqlSinkConfig::new(url, table).column_mapping(MysqlColumnMapping::AutoMap);
    c.write = WriteSpec {
        write_mode: WriteMode::Upsert,
        key: vec!["id".into()],
        delete_marker: None,
        rollback,
    };
    c
}

#[tokio::test(flavor = "multi_thread")]
async fn mysql_group_c1_regressions() {
    let (_c, url) = start_mysql().await;
    let pool = sqlx::MySqlPool::connect(&url).await.expect("pool");

    // SQL-02: base64 into BLOB / VARBINARY lands as bytes; other text as is.
    exec(
        &pool,
        "CREATE TABLE bin (id INT PRIMARY KEY, b BLOB, v VARBINARY(16), t TEXT)",
    )
    .await;
    let sink = MysqlSink::new(
        MysqlSinkConfig::new(&url, "bin").column_mapping(MysqlColumnMapping::AutoMap),
    )
    .await
    .expect("sink");
    sink.write_batch(&[
        json!({"id": 1, "b": "SGVsbG8=", "v": "AAH/", "t": "SGVsbG8="}),
        json!({"id": 2, "b": "not base64!"}),
    ])
    .await
    .expect("write bin");
    let r = sqlx::query("SELECT b, v, t FROM bin WHERE id = 1")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(r.get::<Vec<u8>, _>("b"), b"Hello".to_vec(), "SQL-02 blob");
    assert_eq!(
        r.get::<Vec<u8>, _>("v"),
        vec![0x00, 0x01, 0xff],
        "SQL-02 varbinary"
    );
    assert_eq!(r.get::<String, _>("t"), "SGVsbG8=", "SQL-02 text untouched");
    let r = sqlx::query("SELECT b FROM bin WHERE id = 2")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(r.get::<Vec<u8>, _>("b"), b"not base64!".to_vec());

    // SQL-10: an upsert row that omits a column keeps the stored value.
    exec(
        &pool,
        "CREATE TABLE kv (id INT PRIMARY KEY, name VARCHAR(20), body TEXT)",
    )
    .await;
    exec(
        &pool,
        "INSERT INTO kv VALUES (1, 'a', 'big-1'), (2, 'b', 'big-2')",
    )
    .await;
    let sink = MysqlSink::new(upsert(&url, "kv", None))
        .await
        .expect("sink");
    sink.write_batch(&[
        json!({"id": 1, "body": "big-1-changed"}),
        json!({"id": 2, "name": "b2"}),
    ])
    .await
    .expect("upsert");
    let rows: Vec<(i32, Option<String>, Option<String>)> =
        sqlx::query_as("SELECT id, name, body FROM kv ORDER BY id")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(
        rows,
        vec![
            (1, Some("a".into()), Some("big-1-changed".into())),
            (2, Some("b2".into()), Some("big-2".into())),
        ],
        "SQL-10"
    );

    // SQL-20: JSON-column mode reports no drift baseline.
    let json_sink = MysqlSink::new(MysqlSinkConfig::new(&url, "kv"))
        .await
        .expect("sink");
    assert!(
        json_sink.current_schema().await.unwrap().is_none(),
        "SQL-20"
    );

    // SQL-33: relaxing NOT NULL keeps type, charset/collation, default,
    // AUTO_INCREMENT and ON UPDATE.
    exec(
        &pool,
        "CREATE TABLE ev (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, \
         amount DECIMAL(12,2) NOT NULL DEFAULT 0.00 COMMENT 'it''s money', \
         code VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL DEFAULT 'x', \
         changed DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)",
    )
    .await;
    let sink = MysqlSink::new(
        MysqlSinkConfig::new(&url, "ev").column_mapping(MysqlColumnMapping::AutoMap),
    )
    .await
    .expect("sink");
    sink.evolve_schema(&SchemaEvolution {
        additions: vec![],
        widenings: vec![],
        relax_nullability: vec!["amount".into(), "code".into(), "changed".into()],
    })
    .await
    .expect("relax");
    let cols = sqlx::query(
        "SELECT COLUMN_NAME AS n, CAST(COLUMN_TYPE AS CHAR) AS t, IS_NULLABLE AS nl, \
         CAST(COLUMN_DEFAULT AS CHAR) AS d, CAST(EXTRA AS CHAR) AS x, \
         CAST(COLLATION_NAME AS CHAR) AS c, CAST(COLUMN_COMMENT AS CHAR) AS m \
         FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'ev' \
         ORDER BY ORDINAL_POSITION",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    let get = |i: usize, c: &str| -> Option<String> { cols[i].get(c) };
    assert_eq!(
        get(0, "x").as_deref(),
        Some("auto_increment"),
        "SQL-33 id untouched"
    );
    assert_eq!(
        get(1, "t").as_deref(),
        Some("decimal(12,2)"),
        "SQL-33 decimal"
    );
    assert_eq!(get(1, "nl").as_deref(), Some("YES"));
    assert_eq!(get(1, "d").as_deref(), Some("0.00"));
    assert_eq!(get(1, "m").as_deref(), Some("it's money"));
    assert_eq!(
        get(2, "t").as_deref(),
        Some("varchar(20)"),
        "SQL-33 varchar"
    );
    assert_eq!(get(2, "c").as_deref(), Some("utf8mb4_bin"));
    assert_eq!(get(2, "d").as_deref(), Some("x"));
    assert_eq!(get(3, "t").as_deref(), Some("datetime"), "SQL-33 datetime");
    assert_eq!(get(3, "d").as_deref(), Some("CURRENT_TIMESTAMP"));
    assert!(
        get(3, "x")
            .unwrap_or_default()
            .contains("on update CURRENT_TIMESTAMP"),
        "SQL-33 on update kept: {:?}",
        get(3, "x")
    );

    // SQL-32: a pre-existing row matched by a case-variant key under a _ci
    // collation is restored by rollback, not deleted.
    exec(
        &pool,
        "CREATE TABLE users (id VARCHAR(20) PRIMARY KEY, name TEXT, _faucet_run_id TEXT) \
         COLLATE utf8mb4_0900_ai_ci",
    )
    .await;
    exec(&pool, "INSERT INTO users VALUES ('abc', 'old', 'r0')").await;
    let spec = RollbackWriteSpec {
        run_id: "r1".into(),
        run_id_column: "_faucet_run_id".into(),
        journal: true,
        keep_previous: false,
    };
    let sink = MysqlSink::new(upsert(&url, "users", Some(spec)))
        .await
        .expect("sink");
    sink.write_batch(&[json!({"id": "ABC", "name": "new", "_faucet_run_id": "r1"})])
        .await
        .expect("upsert");
    let out = sink
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
        .expect("rollback");
    assert_eq!((out.deleted, out.restored), (0, 1), "SQL-32 {out:?}");
    let name: String = sqlx::query_scalar("SELECT name FROM users")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(name, "old");
    pool.close().await;
}
