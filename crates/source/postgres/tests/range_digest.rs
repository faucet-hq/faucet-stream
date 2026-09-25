//! Server-side range digests for `faucet verify` (#701): the digest is
//! computed inside Postgres, agrees with itself, moves when a row changes,
//! and reports the key bounds the bisection needs. Requires Docker.

use faucet_core::Source;
use faucet_core::diff::KeyRange;
use faucet_source_postgres::stream::DIGEST_ALGORITHM;
use faucet_source_postgres::{PostgresSource, PostgresSourceConfig};
use testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};
use testcontainers_modules::postgres::Postgres;

async fn start_postgres() -> (ContainerAsync<Postgres>, String) {
    let image = Postgres::default().with_tag("16-alpine");
    let container: ContainerAsync<Postgres> =
        image.start().await.expect("postgres container start");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("postgres port");
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    (container, url)
}

#[tokio::test(flavor = "multi_thread")]
async fn range_digest_reflects_content_and_bounds() {
    let (_c, url) = start_postgres().await;
    let pool = sqlx::PgPool::connect(&url).await.unwrap();
    sqlx::query("CREATE TABLE t (id BIGINT PRIMARY KEY, v TEXT, n INT)")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO t SELECT g, 'v' || g, g * 2 FROM generate_series(1, 100) g")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO t VALUES (101, NULL, NULL)")
        .execute(&pool)
        .await
        .unwrap();

    let cols = vec!["v".to_string(), "n".to_string()];
    let a = PostgresSource::new(PostgresSourceConfig::new(&url, "SELECT * FROM t"))
        .await
        .unwrap();
    let b = PostgresSource::new(PostgresSourceConfig::new(
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
    assert_eq!(da.rows, 101);
    assert_eq!((da.key_min, da.key_max), (Some(1), Some(101)));

    // A sub-range digests only its rows, and a range with none is empty.
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
    assert_eq!(none.key_min, None);

    // Changing one value changes the digest; NULL ≠ empty string.
    sqlx::query("UPDATE t SET v = '' WHERE id = 101")
        .execute(&pool)
        .await
        .unwrap();
    let changed = a
        .range_digest(&KeyRange::ALL, "id", &cols)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(changed.rows, da.rows);
    assert_ne!(changed.digest, da.digest);
    // Digesting a subset of columns ignores the others.
    let only_n = a
        .range_digest(&KeyRange::ALL, "id", &["n".to_string()])
        .await
        .unwrap()
        .unwrap();
    let before_n = da.digest.clone();
    let _ = before_n;
    sqlx::query("UPDATE t SET v = 'zzz' WHERE id = 5")
        .execute(&pool)
        .await
        .unwrap();
    let only_n_after = a
        .range_digest(&KeyRange::ALL, "id", &["n".to_string()])
        .await
        .unwrap()
        .unwrap();
    assert!(only_n.same(&only_n_after), "v is not digested");
    pool.close().await;
}
