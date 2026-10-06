//! Integration tests against a real Microsoft SQL Server in Docker.
//!
//! Requires Docker (the `mcr.microsoft.com/mssql/server` image). Run with:
//! `cargo test -p faucet-source-mssql --test integration`. These are skipped by
//! a plain `cargo test --lib` (unit tests), mirroring the postgres/kafka pattern.

use faucet_common_mssql::{MssqlConnectionConfig, MssqlTls, MssqlTlsMode, build_pool};
use faucet_core::Source;
use faucet_source_mssql::{MssqlReplication, MssqlSource, MssqlSourceConfig};
use futures::StreamExt;
use serde_json::Value;
use testcontainers_modules::mssql_server::MssqlServer;
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

/// `yourStrong(!)Password` percent-encoded for a URL userinfo segment.
const ENCODED_PW: &str = "yourStrong%28%21%29Password";

// SQL Server containers need ~2 GB RAM each. `cargo test` runs a binary's tests
// in parallel; serialize them so at most one container runs at a time and the
// CI runner doesn't run out of memory.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn start_mssql() -> (ContainerAsync<MssqlServer>, u16) {
    let container = MssqlServer::default()
        .with_accept_eula()
        .start()
        .await
        .expect("start mssql container");
    let port = container
        .get_host_port_ipv4(1433)
        .await
        .expect("mssql host port");
    (container, port)
}

fn conn_cfg(port: u16) -> MssqlConnectionConfig {
    MssqlConnectionConfig {
        connection_url: Some(format!("mssql://sa:{ENCODED_PW}@127.0.0.1:{port}/master")),
        connection_string: None,
        tls: MssqlTls {
            mode: MssqlTlsMode::TrustServerCertificate,
            ca_cert_path: None,
        },
    }
}

async fn exec(pool: &faucet_common_mssql::MssqlPool, sql: &str) {
    let mut conn = pool.get().await.expect("checkout");
    conn.execute(sql, &[]).await.expect("execute setup sql");
}

#[tokio::test(flavor = "multi_thread")]
async fn decodes_column_types_and_streams_pages() {
    let _serial = SERIAL.lock().await;
    let (_c, port) = start_mssql().await;
    let cfg = conn_cfg(port);
    let pool = build_pool(&cfg, 4).await.expect("pool");

    exec(
        &pool,
        "CREATE TABLE dbo.types_test (
            id INT NOT NULL,
            name NVARCHAR(50),
            price DECIMAL(10,2),
            active BIT,
            created DATETIME2,
            uid UNIQUEIDENTIFIER,
            payload VARBINARY(8),
            big BIGINT,
            ratio FLOAT,
            maybe INT
        )",
    )
    .await;
    exec(
        &pool,
        "INSERT INTO dbo.types_test
            (id, name, price, active, created, uid, payload, big, ratio, maybe)
         VALUES
            (1, N'alice', 19.99, 1, '2024-01-02T03:04:05',
             '00000000-0000-0000-0000-000000000001', 0x010203, 9000000000, 1.5, NULL)",
    )
    .await;

    let mut scfg = MssqlSourceConfig::new(
        cfg.connection_url.clone().unwrap(),
        "SELECT id, name, price, active, created, uid, payload, big, ratio, maybe FROM dbo.types_test",
    );
    scfg.connection.tls = cfg.tls.clone();
    let source = MssqlSource::new(scfg).await.expect("source");

    let rows = source.fetch_all().await.expect("fetch");
    assert_eq!(rows.len(), 1);
    let r = &rows[0];
    assert_eq!(r["id"], Value::from(1));
    assert_eq!(r["name"], Value::from("alice"));
    assert_eq!(r["price"], Value::from("19.99")); // DECIMAL -> string
    assert_eq!(r["active"], Value::from(true)); // BIT -> bool
    assert!(
        r["created"]
            .as_str()
            .unwrap()
            .starts_with("2024-01-02T03:04:05"),
        "datetime2 -> {}",
        r["created"]
    );
    assert_eq!(
        r["uid"],
        Value::from("00000000-0000-0000-0000-000000000001")
    );
    assert_eq!(r["payload"], Value::from("AQID")); // 0x010203 -> base64
    assert_eq!(r["big"], Value::from(9_000_000_000_i64));
    assert_eq!(r["ratio"], Value::from(1.5));
    assert_eq!(r["maybe"], Value::Null);

    // Streaming: 5 rows at batch_size 2 -> pages of 2,2,1.
    exec(&pool, "CREATE TABLE dbo.nums (n INT)").await;
    for n in 1..=5 {
        exec(&pool, &format!("INSERT INTO dbo.nums (n) VALUES ({n})")).await;
    }
    let mut scfg = MssqlSourceConfig::new(
        cfg.connection_url.clone().unwrap(),
        "SELECT n FROM dbo.nums ORDER BY n",
    );
    scfg.connection.tls = cfg.tls.clone();
    scfg.batch_size = 2;
    let source = MssqlSource::new(scfg).await.expect("source");

    let ctx = std::collections::HashMap::new();
    let pages: Vec<_> = source
        .stream_pages(&ctx, 2)
        .map(|p| p.expect("page"))
        .collect()
        .await;
    let sizes: Vec<usize> = pages.iter().map(|p| p.records.len()).collect();
    assert_eq!(sizes, vec![2, 2, 1], "batch_size=2 over 5 rows");
    let total: usize = pages.iter().map(|p| p.records.len()).sum();
    assert_eq!(total, 5);
}

#[tokio::test(flavor = "multi_thread")]
async fn incremental_resumes_without_duplicates() {
    let _serial = SERIAL.lock().await;
    let (_c, port) = start_mssql().await;
    let cfg = conn_cfg(port);
    let pool = build_pool(&cfg, 4).await.expect("pool");

    exec(
        &pool,
        "CREATE TABLE dbo.events (id INT, updated_at NVARCHAR(30))",
    )
    .await;
    for (id, ts) in [(1, "2024-01-01"), (2, "2024-02-01"), (3, "2024-03-01")] {
        exec(
            &pool,
            &format!("INSERT INTO dbo.events (id, updated_at) VALUES ({id}, '{ts}')"),
        )
        .await;
    }

    let mut scfg = MssqlSourceConfig::new(
        cfg.connection_url.clone().unwrap(),
        "SELECT id, updated_at FROM dbo.events WHERE updated_at > @bookmark ORDER BY updated_at",
    );
    scfg.connection.tls = cfg.tls.clone();
    scfg.replication = MssqlReplication::Incremental {
        column: "updated_at".into(),
        initial_value: Value::from("2024-01-15"),
    };
    let source = MssqlSource::new(scfg).await.expect("source");

    // Run 1: initial_value=2024-01-15 -> rows 2024-02-01 and 2024-03-01.
    let (rows, bookmark) = source.fetch_all_incremental().await.expect("run 1");
    let ids: Vec<i64> = rows.iter().map(|r| r["id"].as_i64().unwrap()).collect();
    assert_eq!(ids, vec![2, 3], "run 1 honours initial_value");
    assert_eq!(bookmark, Some(Value::from("2024-03-01")));

    // Persist the bookmark, add a newer row, run again.
    source
        .apply_start_bookmark(bookmark.unwrap())
        .await
        .unwrap();
    exec(
        &pool,
        "INSERT INTO dbo.events (id, updated_at) VALUES (4, '2024-04-01')",
    )
    .await;

    // Run 2: only the strictly-newer row, no duplicates of 2/3.
    let (rows2, bookmark2) = source.fetch_all_incremental().await.expect("run 2");
    let ids2: Vec<i64> = rows2.iter().map(|r| r["id"].as_i64().unwrap()).collect();
    assert_eq!(ids2, vec![4], "run 2 resumes from bookmark, no duplicates");
    assert_eq!(bookmark2, Some(Value::from("2024-04-01")));
}

/// SQL-40: a legacy `DATETIME` cursor renders as milliseconds, so the bookmark
/// converts back to `DATETIME` and the second run succeeds.
#[tokio::test(flavor = "multi_thread")]
async fn a_legacy_datetime_cursor_resumes_on_the_next_run() {
    let _serial = SERIAL.lock().await;
    let (_c, port) = start_mssql().await;
    let cfg = conn_cfg(port);
    let pool = build_pool(&cfg, 4).await.expect("pool");

    exec(
        &pool,
        "CREATE TABLE dbo.legacy (id INT, updated_at DATETIME)",
    )
    .await;
    exec(
        &pool,
        "INSERT INTO dbo.legacy VALUES (1, '2024-03-01T12:30:45.003'), \
         (2, '2024-03-01T12:30:45.007')",
    )
    .await;
    let mut scfg = MssqlSourceConfig::new(
        cfg.connection_url.clone().unwrap(),
        "SELECT id, updated_at FROM dbo.legacy WHERE updated_at > @bookmark ORDER BY updated_at",
    );
    scfg.connection.tls = cfg.tls.clone();
    scfg.replication = MssqlReplication::Incremental {
        column: "updated_at".into(),
        initial_value: Value::from("2000-01-01T00:00:00.000"),
    };
    let source = MssqlSource::new(scfg).await.expect("source");

    let (rows, bookmark) = source.fetch_all_incremental().await.expect("run 1");
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0]["updated_at"],
        Value::from("2024-03-01T12:30:45.003")
    );
    assert_eq!(bookmark, Some(Value::from("2024-03-01T12:30:45.007")));

    source
        .apply_start_bookmark(bookmark.unwrap())
        .await
        .unwrap();
    exec(
        &pool,
        "INSERT INTO dbo.legacy VALUES (3, '2024-03-01T12:30:46.000')",
    )
    .await;
    let (rows2, _) = source.fetch_all_incremental().await.expect("run 2");
    let ids: Vec<i64> = rows2.iter().map(|r| r["id"].as_i64().unwrap()).collect();
    assert_eq!(ids, vec![3]);
}

// ── PK-range sharding (Mode B, #262) ────────────────────────────────────────

/// The core Mode B correctness guarantee: enumerating a source into N shards and
/// reading each shard yields every row exactly once — no duplication, no loss.
/// Includes a NULL-key row, which MIN/MAX enumeration cannot see but exactly one
/// shard must still read (F37).
#[tokio::test(flavor = "multi_thread")]
async fn shards_partition_rows_disjointly_and_completely() {
    use faucet_source_mssql::ShardConfig;

    let _serial = SERIAL.lock().await;
    let (_c, port) = start_mssql().await;
    let cfg = conn_cfg(port);
    let pool = build_pool(&cfg, 4).await.expect("pool");

    exec(
        &pool,
        "CREATE TABLE dbo.items (k BIGINT, label NVARCHAR(20))",
    )
    .await;
    // Multi-row VALUES keeps the seeding to a handful of round trips.
    for chunk in (1..=200i64).collect::<Vec<_>>().chunks(50) {
        let values: Vec<String> = chunk.iter().map(|i| format!("({i}, N'row-{i}')")).collect();
        exec(
            &pool,
            &format!(
                "INSERT INTO dbo.items (k, label) VALUES {}",
                values.join(", ")
            ),
        )
        .await;
    }
    exec(
        &pool,
        "INSERT INTO dbo.items (k, label) VALUES (NULL, N'null-row')",
    )
    .await;

    let mut scfg = MssqlSourceConfig::new(
        cfg.connection_url.clone().unwrap(),
        "SELECT k, label FROM dbo.items",
    );
    scfg.connection.tls = cfg.tls.clone();
    scfg.shard = Some(ShardConfig { key: "k".into() });

    let source = MssqlSource::new(scfg).await.expect("source");
    assert!(source.is_shardable());
    let shards = source.enumerate_shards(4).await.expect("enumerate");
    assert!(
        (2..=4).contains(&shards.len()),
        "expected 2..=4 shards, got {}",
        shards.len()
    );

    // Each shard is applied on the same source sequentially (apply_shard
    // replaces the previous narrowing), mirroring what a claiming worker does.
    let mut labels: Vec<String> = Vec::new();
    for shard in &shards {
        source.apply_shard(shard).await.expect("apply_shard");
        for rec in source.fetch_all().await.expect("fetch shard") {
            labels.push(rec["label"].as_str().unwrap().to_string());
        }
    }

    labels.sort();
    let mut expected: Vec<String> = (1..=200i64).map(|i| format!("row-{i}")).collect();
    expected.push("null-row".to_string());
    expected.sort();
    assert_eq!(
        labels, expected,
        "shards must union to all rows exactly once (no dup, no loss)"
    );

    // Error paths: a bad key errors at enumeration; a malformed descriptor is
    // rejected by apply_shard. Reuses the running container to keep CI time down.
    let mut bad_cfg = MssqlSourceConfig::new(
        cfg.connection_url.clone().unwrap(),
        "SELECT k FROM dbo.items",
    );
    bad_cfg.connection.tls = cfg.tls.clone();
    bad_cfg.shard = Some(ShardConfig {
        key: "no_such_column".into(),
    });
    let bad = MssqlSource::new(bad_cfg).await.expect("source");
    assert!(bad.enumerate_shards(4).await.is_err());
    let malformed = faucet_core::ShardSpec::new("0", serde_json::json!({ "key": "k" }));
    assert!(bad.apply_shard(&malformed).await.is_err());
}

/// `discover()` enumerates real tables with column schemas, row estimates,
/// and a ready-to-run bracket-quoted `query` config patch (#211).
#[tokio::test(flavor = "multi_thread")]
async fn discover_enumerates_tables_with_schemas() {
    let _serial = SERIAL.lock().await;
    let (_c, port) = start_mssql().await;
    let cfg = conn_cfg(port);
    let pool = build_pool(&cfg, 4).await.expect("pool");

    exec(
        &pool,
        "CREATE TABLE dbo.orders (
            id BIGINT NOT NULL PRIMARY KEY,
            note NVARCHAR(50) NULL,
            total DECIMAL(10,2) NULL
        )",
    )
    .await;
    // CREATE SCHEMA must be the only statement in its batch; tiberius's
    // execute() RPC path trips T-SQL error 156 on it — EXEC() gives it a
    // batch of its own.
    exec(&pool, "EXEC('CREATE SCHEMA sales')").await;
    exec(
        &pool,
        "CREATE TABLE sales.leads (id INT NOT NULL, active BIT NULL)",
    )
    .await;
    for chunk in (1..=50i64).collect::<Vec<_>>().chunks(25) {
        let values: Vec<String> = chunk.iter().map(|i| format!("({i})")).collect();
        exec(
            &pool,
            &format!("INSERT INTO dbo.orders (id) VALUES {}", values.join(", ")),
        )
        .await;
    }

    let mut scfg = MssqlSourceConfig::new(cfg.connection_url.clone().unwrap(), "SELECT 1");
    scfg.connection.tls = cfg.tls.clone();
    let source = MssqlSource::new(scfg).await.expect("source");

    assert!(source.supports_discover());
    let datasets = source.discover().await.expect("discover");

    let names: Vec<&str> = datasets.iter().map(|d| d.name.as_str()).collect();
    assert!(names.contains(&"dbo.orders"), "got: {names:?}");
    assert!(names.contains(&"sales.leads"), "got: {names:?}");

    let orders = datasets
        .iter()
        .find(|d| d.name == "dbo.orders")
        .expect("orders dataset");
    assert_eq!(orders.kind, "table");
    assert_eq!(orders.config_patch["query"], "SELECT * FROM [dbo].[orders]");
    assert_eq!(orders.primary_key, Some(vec!["id".to_string()]));
    let leads = datasets.iter().find(|d| d.name == "sales.leads").unwrap();
    assert_eq!(leads.primary_key, None);
    assert_eq!(
        orders.estimated_rows,
        Some(50),
        "sys.partitions heap row count"
    );
    let schema = orders.schema.as_ref().expect("schema");
    assert_eq!(schema["properties"]["id"]["type"], "integer");
    assert_eq!(
        schema["properties"]["total"]["type"],
        serde_json::json!(["number", "null"])
    );
    assert_eq!(
        schema["properties"]["note"]["type"],
        serde_json::json!(["string", "null"]),
        "nullable column"
    );

    let leads = datasets
        .iter()
        .find(|d| d.name == "sales.leads")
        .expect("leads dataset");
    assert_eq!(leads.config_patch["query"], "SELECT * FROM [sales].[leads]");
    assert_eq!(
        leads.schema.as_ref().unwrap()["properties"]["active"]["type"],
        serde_json::json!(["boolean", "null"]),
        "BIT maps to boolean"
    );
}
