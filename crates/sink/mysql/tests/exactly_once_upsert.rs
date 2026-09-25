//! Composition test (#190): exactly-once delivery + `write_mode: upsert` for
//! the MySQL sink, against a real MySQL instance via testcontainers.
//!
//! Verifies that `write_batch_idempotent` routes through the upsert planner so
//! the data write AND the commit-token watermark commit atomically in one
//! transaction. Re-writing the same key in a later page (with a higher token)
//! must UPDATE the row in place — not duplicate it — and advance the token.
//!
//! Requires Docker. Boots its own container so it is isolated.

use faucet_core::{Sink, WriteMode, WriteSpec, format_token};
use faucet_sink_mysql::{MysqlColumnMapping, MysqlSink, MysqlSinkConfig};
use serde_json::json;
use sqlx::Row;
use std::sync::OnceLock;
use testcontainers::{ContainerAsync, runners::AsyncRunner};
use testcontainers_modules::mysql::Mysql;
use tokio::sync::Semaphore;

/// Bounds concurrent MySQL container startups (see `upsert.rs` for rationale).
fn startup_limit() -> &'static Semaphore {
    static SEM: OnceLock<Semaphore> = OnceLock::new();
    SEM.get_or_init(|| Semaphore::new(2))
}

async fn start_mysql() -> (ContainerAsync<Mysql>, String) {
    let _permit = startup_limit()
        .acquire()
        .await
        .expect("startup semaphore closed");
    let image = Mysql::default();
    let container: ContainerAsync<Mysql> = image.start().await.expect("mysql container start");
    let port = container
        .get_host_port_ipv4(3306)
        .await
        .expect("mysql port");
    let url = format!("mysql://root@127.0.0.1:{port}/test");
    (container, url)
}

async fn create_upsert_table(url: &str) {
    let pool = sqlx::MySqlPool::connect(url).await.expect("pool connect");
    sqlx::query("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(255))")
        .execute(&pool)
        .await
        .expect("create table");
    pool.close().await;
}

async fn row_count(url: &str) -> i64 {
    let pool = sqlx::MySqlPool::connect(url).await.expect("pool connect");
    let row = sqlx::query("SELECT COUNT(*) AS n FROM t")
        .fetch_one(&pool)
        .await
        .expect("count");
    let n: i64 = row.get("n");
    pool.close().await;
    n
}

async fn name_for_id(url: &str, id: i32) -> Option<String> {
    let pool = sqlx::MySqlPool::connect(url).await.expect("pool connect");
    let row = sqlx::query("SELECT name FROM t WHERE id = ?")
        .bind(id)
        .fetch_optional(&pool)
        .await
        .expect("read back");
    pool.close().await;
    row.map(|r| r.get::<String, _>("name"))
}

fn upsert_sink_config(url: &str) -> MysqlSinkConfig {
    let mut config = MysqlSinkConfig::new(url, "t").column_mapping(MysqlColumnMapping::AutoMap);
    config.write = WriteSpec {
        write_mode: WriteMode::Upsert,
        key: vec!["id".to_string()],
        delete_marker: None,
        rollback: None,
    };
    config
}

#[tokio::test(flavor = "multi_thread")]
async fn idempotent_upsert_updates_in_place_and_advances_token() {
    let (_container, url) = start_mysql().await;
    create_upsert_table(&url).await;

    let sink = MysqlSink::new(upsert_sink_config(&url))
        .await
        .expect("sink new");

    let scope = "t::r1";
    let t1 = format_token(1);
    let t2 = format_token(2);

    // Page 1, token ...0001: upsert id=1 -> "a" in the idempotent path.
    let written = sink
        .write_batch_idempotent(&[json!({"id": 1, "name": "a"})], scope, &t1)
        .await
        .expect("idempotent upsert 1");
    assert_eq!(written, 1, "one upsert applied");
    assert_eq!(
        sink.last_committed_token(scope).await.expect("token read"),
        Some(t1.clone()),
        "token advanced to t1"
    );
    assert_eq!(row_count(&url).await, 1);
    assert_eq!(name_for_id(&url, 1).await.as_deref(), Some("a"));

    // Page 2, token ...0002: upsert id=1 -> "b".
    let written = sink
        .write_batch_idempotent(&[json!({"id": 1, "name": "b"})], scope, &t2)
        .await
        .expect("idempotent upsert 2");
    assert_eq!(written, 1, "one upsert applied (update in place)");

    // Exactly ONE row id=1, now "b" — upserted in the idempotent path, not duplicated.
    assert_eq!(
        row_count(&url).await,
        1,
        "upsert in the idempotent path must update, not duplicate"
    );
    assert_eq!(
        name_for_id(&url, 1).await.as_deref(),
        Some("b"),
        "row must reflect the latest value 'b'"
    );

    // Token advanced to t2 — committed atomically with the upsert.
    assert_eq!(
        sink.last_committed_token(scope).await.expect("token read"),
        Some(t2),
        "token advanced to t2 alongside the upsert"
    );
}
