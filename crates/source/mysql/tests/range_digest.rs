//! Server-side range digests for `faucet verify` (#701) on MySQL: the digest
//! is computed inside the server, agrees with itself regardless of row order,
//! moves when a row changes, and reports the key bounds. Requires Docker.

use faucet_core::Source;
use faucet_core::diff::KeyRange;
use faucet_source_mysql::stream::DIGEST_ALGORITHM;
use faucet_source_mysql::{MysqlSource, MysqlSourceConfig};
use std::sync::OnceLock;
use testcontainers::{ContainerAsync, runners::AsyncRunner};
use testcontainers_modules::mysql::Mysql;
use tokio::sync::Semaphore;

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

#[tokio::test(flavor = "multi_thread")]
async fn range_digest_reflects_content_and_bounds() {
    let (_c, url) = start_mysql().await;
    let pool = sqlx::MySqlPool::connect(&url).await.unwrap();
    sqlx::query("CREATE TABLE t (id BIGINT PRIMARY KEY, v VARCHAR(32), n INT)")
        .execute(&pool)
        .await
        .unwrap();
    for id in 1..=50i64 {
        sqlx::query("INSERT INTO t VALUES (?, ?, ?)")
            .bind(id)
            .bind(format!("v{id}"))
            .bind((id * 2) as i32)
            .execute(&pool)
            .await
            .unwrap();
    }
    sqlx::query("INSERT INTO t VALUES (51, NULL, NULL)")
        .execute(&pool)
        .await
        .unwrap();

    let cols = vec!["v".to_string(), "n".to_string()];
    let a = MysqlSource::new(MysqlSourceConfig::new(&url, "SELECT * FROM t"))
        .await
        .unwrap();
    let b = MysqlSource::new(MysqlSourceConfig::new(
        &url,
        "SELECT id, v, n FROM t ORDER BY id DESC",
    ))
    .await
    .unwrap();
    let da = a
        .range_digest(&KeyRange::ALL, "id", &cols)
        .await
        .unwrap()
        .unwrap();
    let db = b
        .range_digest(&KeyRange::ALL, "id", &cols)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(da.algorithm, DIGEST_ALGORITHM);
    assert!(da.same(&db), "order-independent: {da:?} vs {db:?}");
    assert_eq!(da.rows, 51);
    assert_eq!((da.key_min, da.key_max), (Some(1), Some(51)));

    let mid = a
        .range_digest(
            &KeyRange {
                lo: Some(10),
                hi: Some(20),
            },
            "id",
            &cols,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(mid.rows, 10);
    assert_eq!((mid.key_min, mid.key_max), (Some(10), Some(19)));
    let none = a
        .range_digest(
            &KeyRange {
                lo: Some(1000),
                hi: Some(2000),
            },
            "id",
            &cols,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(none.rows, 0);
    assert_eq!(none.digest, "0");

    sqlx::query("UPDATE t SET v = '' WHERE id = 51")
        .execute(&pool)
        .await
        .unwrap();
    let changed = a
        .range_digest(&KeyRange::ALL, "id", &cols)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(changed.rows, da.rows);
    assert_ne!(changed.digest, da.digest, "NULL ≠ empty string");
    pool.close().await;
}
