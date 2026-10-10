//! Regressions for the #789 MySQL sink findings, against one MySQL container.
//! Requires Docker.

use faucet_core::rollback::{RollbackMode, RollbackOptions, RollbackWriteSpec};
use faucet_core::{ColumnChange, SchemaEvolution, Sink, WriteMode, WriteSpec};
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
    sqlx::raw_sql(sql).execute(pool).await.expect(sql);
}

async fn texts(pool: &sqlx::MySqlPool, sql: &str) -> Vec<String> {
    sqlx::query(sql)
        .fetch_all(pool)
        .await
        .expect(sql)
        .iter()
        .map(|r| r.get::<String, _>(0))
        .collect()
}

fn config(url: &str, table: &str, mode: WriteMode, key: &[&str]) -> MysqlSinkConfig {
    let mut c = MysqlSinkConfig::new(url, table).column_mapping(MysqlColumnMapping::AutoMap);
    c.write = WriteSpec {
        write_mode: mode,
        key: key.iter().map(|s| s.to_string()).collect(),
        delete_marker: None,
        rollback: None,
    };
    c
}

fn journaled(mut c: MysqlSinkConfig, run: &str, keep_previous: bool) -> MysqlSinkConfig {
    c.write.rollback = Some(RollbackWriteSpec {
        run_id: run.into(),
        run_id_column: "_faucet_run_id".into(),
        journal: true,
        keep_previous,
    });
    c
}

fn force(mode: RollbackMode) -> RollbackOptions {
    RollbackOptions {
        run_id_column: "_faucet_run_id".into(),
        mode,
        force: true,
        dry_run: false,
        later_runs: false,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn mysql_audit_regressions() {
    let (_c, url) = start_mysql().await;
    let pool = sqlx::MySqlPool::connect(&url).await.expect("pool");

    // SQL-74 first, while no journal table exists: a keyed page that fails
    // part-way leaves nothing behind, because the journal DDL no longer
    // commits the write transaction.
    exec(
        &pool,
        "CREATE TABLE acct (id INT PRIMARY KEY, name VARCHAR(20), note VARCHAR(20) NOT NULL DEFAULT '', \
         _faucet_run_id VARCHAR(64))",
    )
    .await;
    let sink = MysqlSink::new(journaled(
        config(&url, "acct", WriteMode::Upsert, &["id"]),
        "r1",
        false,
    ))
    .await
    .unwrap();
    let err = sink
        .write_batch(&[
            json!({"id": 1, "name": "a", "_faucet_run_id": "r1"}),
            json!({"id": 2, "note": null, "_faucet_run_id": "r1"}),
        ])
        .await
        .unwrap_err();
    assert!(err.to_string().contains("MySQL insert failed"), "{err}");
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM acct")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(n, 0, "the failed page left no rows");
    let journal: i64 = sqlx::query_scalar("SELECT count(*) FROM _faucet_run_journal")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(journal, 0, "nor journal rows");

    // SQL-75 + SQL-107: rollback restores binary, BIT and DECIMAL exactly and
    // leaves generated columns to the table.
    exec(
        &pool,
        "CREATE TABLE typed (id INT PRIMARY KEY, b VARBINARY(8), f BIT(8), \
         d DECIMAL(40,20), g INT AS (id * 2) STORED, _faucet_run_id VARCHAR(64))",
    )
    .await;
    exec(
        &pool,
        "INSERT INTO typed (id, b, f, d, _faucet_run_id) VALUES \
         (1, X'00FF10', b'10100101', 12345678901234567890.12345678901234567890, 'r0')",
    )
    .await;
    let sink = MysqlSink::new(journaled(
        config(&url, "typed", WriteMode::Upsert, &["id"]),
        "r2",
        false,
    ))
    .await
    .unwrap();
    sink.write_batch(&[json!({"id": 1, "b": "AQ==", "f": 1, "d": "1.5", "_faucet_run_id": "r2"})])
        .await
        .unwrap();
    let out = sink
        .rollback_run("r2", &force(RollbackMode::Upsert))
        .await
        .unwrap();
    assert!(out.applied);
    let row = sqlx::query(
        "SELECT HEX(b), CAST(f AS UNSIGNED), CAST(d AS CHAR), g, _faucet_run_id FROM typed",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row.get::<String, _>(0), "00FF10");
    assert_eq!(row.get::<u64, _>(1), 0b1010_0101);
    assert_eq!(
        row.get::<String, _>(2),
        "12345678901234567890.12345678901234567890"
    );
    assert_eq!(row.get::<i32, _>(3), 2);
    assert_eq!(row.get::<String, _>(4), "r0");

    // SQL-50: fields match columns ignoring case; a record matching none fails.
    exec(&pool, "CREATE TABLE ev (userId INT, kind VARCHAR(10))").await;
    let sink = MysqlSink::new(config(&url, "ev", WriteMode::Append, &[]))
        .await
        .unwrap();
    let err = sink
        .write_batch(&[json!({"user_id": 1})])
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("record 0 has no field matching"),
        "{err}"
    );
    let outcomes = sink
        .write_batch_partial(&[json!({"userid": 7, "KIND": "k"}), json!({"nope": 1})])
        .await
        .unwrap();
    assert!(outcomes[0].is_ok() && outcomes[1].is_err());
    assert_eq!(
        texts(&pool, "SELECT CONCAT(userId, kind) FROM ev").await,
        vec!["7k"]
    );

    // SQL-73: BIGINT UNSIGNED above i64::MAX binds exactly.
    exec(&pool, "CREATE TABLE big (id BIGINT UNSIGNED PRIMARY KEY)").await;
    let sink = MysqlSink::new(config(&url, "big", WriteMode::Upsert, &["id"]))
        .await
        .unwrap();
    sink.write_batch(&[
        json!({"id": 18446744073709551000u64}),
        json!({"id": 18446744073709550999u64}),
    ])
    .await
    .unwrap();
    assert_eq!(
        texts(&pool, "SELECT CAST(id AS CHAR) FROM big ORDER BY id").await,
        vec!["18446744073709550999", "18446744073709551000"]
    );

    // SQL-76: upsert refuses a table with a second unique index.
    exec(
        &pool,
        "CREATE TABLE people (id INT PRIMARY KEY, email VARCHAR(50) UNIQUE, name VARCHAR(10))",
    )
    .await;
    let err = MysqlSink::new(config(&url, "people", WriteMode::Upsert, &["id"]))
        .await
        .err()
        .expect("refused");
    assert!(err.to_string().contains("(email)"), "{err}");

    // SQL-78: widening keeps NOT NULL, default and comment, and is exact.
    exec(
        &pool,
        "CREATE TABLE w (amount BIGINT NOT NULL DEFAULT 0 COMMENT 'cents')",
    )
    .await;
    exec(&pool, "INSERT INTO w VALUES (9007199254740993)").await;
    let sink = MysqlSink::new(config(&url, "w", WriteMode::Append, &[]))
        .await
        .unwrap();
    sink.evolve_schema(&SchemaEvolution {
        widenings: vec![ColumnChange {
            name: "amount".into(),
            from: Some(json!({"type": "integer"})),
            to: json!({"type": "number"}),
        }],
        ..Default::default()
    })
    .await
    .unwrap();
    let def = sqlx::query(
        "SELECT CAST(COLUMN_TYPE AS CHAR), CAST(IS_NULLABLE AS CHAR), \
         CAST(COLUMN_DEFAULT AS CHAR), CAST(COLUMN_COMMENT AS CHAR) \
         FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'w'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(def.get::<String, _>(0), "decimal(65,30)");
    assert_eq!(def.get::<String, _>(1), "NO");
    assert!(def.get::<String, _>(2).starts_with('0'));
    assert_eq!(def.get::<String, _>(3), "cents");
    assert_eq!(
        texts(
            &pool,
            "SELECT CAST(CAST(amount AS DECIMAL(20,0)) AS CHAR) FROM w"
        )
        .await,
        vec!["9007199254740993"]
    );

    // SQL-108: an overwrite keeps the target's triggers and foreign keys.
    exec(&pool, "CREATE TABLE country (code CHAR(2) PRIMARY KEY)").await;
    exec(&pool, "INSERT INTO country VALUES ('IN'), ('US')").await;
    exec(
        &pool,
        "CREATE TABLE city (id INT PRIMARY KEY, code CHAR(2), _faucet_run_id VARCHAR(64), \
         FOREIGN KEY (code) REFERENCES country (code))",
    )
    .await;
    exec(&pool, "CREATE TABLE audit (n INT)").await;
    exec(
        &pool,
        "CREATE TRIGGER city_ins AFTER INSERT ON city FOR EACH ROW INSERT INTO audit VALUES (NEW.id)",
    )
    .await;
    exec(&pool, "INSERT INTO city VALUES (1, 'IN', 'r0')").await;
    let ovw = || journaled(config(&url, "city", WriteMode::Overwrite, &[]), "r3", true);
    let sink = MysqlSink::new(ovw()).await.unwrap();
    sink.begin_overwrite().await.unwrap();
    sink.write_batch(&[json!({"id": 2, "code": "US", "_faucet_run_id": "r3"})])
        .await
        .unwrap();
    MysqlSink::new(ovw())
        .await
        .unwrap()
        .commit_overwrite()
        .await
        .unwrap();
    let triggers: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM INFORMATION_SCHEMA.TRIGGERS WHERE EVENT_OBJECT_TABLE = 'city'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(triggers, 1, "the trigger survives the overwrite");
    let fks: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS WHERE TABLE_NAME = 'city'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(fks, 1, "the foreign key survives the overwrite");
    assert_eq!(
        texts(&pool, "SELECT CAST(id AS CHAR) FROM city").await,
        vec!["2"]
    );
    let out = MysqlSink::new(ovw())
        .await
        .unwrap()
        .rollback_run("r3", &force(RollbackMode::Overwrite))
        .await
        .unwrap();
    assert!(out.applied);
    assert_eq!(
        texts(&pool, "SELECT CAST(id AS CHAR) FROM city").await,
        vec!["1"]
    );

    // SQL-129: an overwrite of a referenced parent is refused up front.
    let err = MysqlSink::new(config(&url, "country", WriteMode::Overwrite, &[]))
        .await
        .unwrap()
        .begin_overwrite()
        .await
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("referenced by a foreign key from city"),
        "{err}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn keyed_and_json_column_paths_of_every_write_entry_point() {
    let (_c, url) = start_mysql().await;
    let pool = sqlx::MySqlPool::connect(&url).await.unwrap();
    exec(&pool, "CREATE TABLE kv (id INT PRIMARY KEY, v VARCHAR(20))").await;

    let upsert = MysqlSink::new(config(&url, "kv", WriteMode::Upsert, &["id"]))
        .await
        .unwrap();
    let err = upsert
        .write_batch(&[json!({"v": "no key"})])
        .await
        .unwrap_err();
    assert!(err.to_string().contains("mysql upsert: row 0"), "{err}");
    let err = upsert
        .write_batch_idempotent(
            &[json!({"id": 1, "v": "a"}), json!({"v": "b"})],
            "s",
            &faucet_core::format_token(1),
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("mysql upsert: row 1"), "{err}");
    upsert
        .write_batch_idempotent(
            &[json!({"id": 1, "v": "a"}), json!({"id": 2, "v": "b"})],
            "s",
            &faucet_core::format_token(1),
        )
        .await
        .unwrap();
    let delete = MysqlSink::new(config(&url, "kv", WriteMode::Delete, &["id"]))
        .await
        .unwrap();
    delete
        .write_batch_idempotent(&[json!({"id": 1})], "s", &faucet_core::format_token(2))
        .await
        .unwrap();
    assert_eq!(
        texts(&pool, "SELECT CAST(id AS CHAR) FROM kv ORDER BY id").await,
        vec!["2"]
    );

    exec(&pool, "CREATE TABLE docs (data JSON)").await;
    let json_sink = MysqlSink::new(MysqlSinkConfig::new(&url, "docs").column_mapping(
        MysqlColumnMapping::Json {
            column: "data".into(),
        },
    ))
    .await
    .unwrap();
    let out = json_sink
        .write_batch_partial(&[json!({"a": 1}), json!({"b": 2})])
        .await
        .unwrap();
    assert!(out.len() == 2 && out.iter().all(Result::is_ok));
    assert_eq!(
        texts(&pool, "SELECT CAST(COUNT(*) AS CHAR) FROM docs").await,
        vec!["2"]
    );
}
