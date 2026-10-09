//! #789 SQL-13: a VARCHAR/TEXT column with a `_bin` collation is text, so it
//! arrives as its string — only real binary columns are base64-encoded.
//! Requires Docker.

use faucet_core::Source;
use faucet_source_mysql::{MysqlSource, MysqlSourceConfig};
use serde_json::json;
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

#[tokio::test(flavor = "multi_thread")]
async fn binary_collation_text_is_text_and_binary_is_base64() {
    let (_c, url) = start_mysql().await;
    let pool = sqlx::MySqlPool::connect(&url).await.expect("pool");
    for sql in [
        "CREATE TABLE t (id INT PRIMARY KEY, \
         slug VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin, \
         token TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin, \
         code CHAR(3) BINARY, raw VARBINARY(8), blob_col BLOB, plain VARCHAR(5))",
        "INSERT INTO t VALUES (1, 'abc', 'Tok-é', 'xyz', X'0001FF', X'48656C6C6F', 'p'), \
         (2, NULL, NULL, NULL, NULL, NULL, NULL)",
    ] {
        sqlx::query(sql).execute(&pool).await.expect(sql);
    }
    pool.close().await;

    let source = MysqlSource::new(MysqlSourceConfig::new(&url, "SELECT * FROM t ORDER BY id"))
        .await
        .expect("source");
    let rows = source.fetch_all().await.expect("fetch");
    assert_eq!(
        rows[0],
        json!({
            "id": 1, "slug": "abc", "token": "Tok-é", "code": "xyz",
            "raw": "AAH/", "blob_col": "SGVsbG8=", "plain": "p"
        })
    );
    assert_eq!(rows[1]["slug"], json!(null));

    let streamed = {
        use futures::StreamExt;
        let ctx = std::collections::HashMap::new();
        let pages: Vec<_> = source.stream_pages(&ctx, 10).collect().await;
        pages
            .into_iter()
            .flat_map(|p| p.expect("page").records)
            .collect::<Vec<_>>()
    };
    assert_eq!(streamed, rows, "the streaming path decodes the same way");

    // Duplicate names cannot be probed; the query still runs, as before.
    let dup = MysqlSource::new(MysqlSourceConfig::new(&url, "SELECT raw, raw FROM t"))
        .await
        .expect("source");
    assert_eq!(dup.fetch_all().await.expect("fetch").len(), 2);
}
