//! Regressions for the #789 PostgreSQL sink findings. Requires Docker.

use faucet_core::rollback::{RollbackMode, RollbackOptions, RollbackWriteSpec};
use faucet_core::{ColumnChange, OverwriteScope, SchemaEvolution, Sink, WriteMode, WriteSpec};
use faucet_sink_postgres::{
    PostgresColumnMapping, PostgresSink, PostgresSinkConfig, PostgresWriteMethod,
};
use serde_json::{Value, json};
use testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};
use testcontainers_modules::postgres::Postgres;

async fn start_postgres() -> (ContainerAsync<Postgres>, String) {
    let container = Postgres::default()
        .with_tag("16-alpine")
        .start()
        .await
        .expect("postgres container start");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    (container, url)
}

async fn exec(url: &str, sql: &str) {
    let pool = sqlx::PgPool::connect(url).await.expect("pool");
    sqlx::raw_sql(sql).execute(&pool).await.expect("exec");
    pool.close().await;
}

async fn text_rows(url: &str, sql: &str) -> Vec<String> {
    let pool = sqlx::PgPool::connect(url).await.expect("pool");
    let out: Vec<String> = sqlx::query_scalar(sql)
        .fetch_all(&pool)
        .await
        .expect("rows");
    pool.close().await;
    out
}

fn config(url: &str, table: &str, mode: WriteMode, key: &[&str]) -> PostgresSinkConfig {
    let mut c = PostgresSinkConfig::new(url, table).column_mapping(PostgresColumnMapping::AutoMap);
    c.write = WriteSpec {
        write_mode: mode,
        key: key.iter().map(|s| s.to_string()).collect(),
        delete_marker: None,
        rollback: None,
    };
    c
}

fn with_rollback(
    mut c: PostgresSinkConfig,
    run_id: &str,
    keep_previous: bool,
) -> PostgresSinkConfig {
    c.write.rollback = Some(RollbackWriteSpec {
        run_id: run_id.into(),
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
async fn unmatched_records_fail_instead_of_vanishing() {
    let (_c, url) = start_postgres().await;
    exec(&url, "CREATE TABLE ev (user_id INT, kind TEXT)").await;
    for method in [PostgresWriteMethod::Insert, PostgresWriteMethod::Copy] {
        let sink =
            PostgresSink::new(config(&url, "ev", WriteMode::Append, &[]).with_write_method(method))
                .await
                .unwrap();
        let err = sink
            .write_batch(&[json!({"user_id": 1}), json!({"userId": 2})])
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("record 1 has no field matching"),
            "{err}"
        );
        let outcomes = sink
            .write_batch_partial(&[json!({"userId": 2}), json!({"user_id": 3, "kind": "k"})])
            .await
            .unwrap();
        assert!(outcomes[0].is_err());
        assert!(outcomes[1].is_ok());
    }
    let rows = text_rows(&url, "SELECT user_id::text FROM ev ORDER BY user_id").await;
    assert_eq!(
        rows,
        vec!["3", "3"],
        "only the matching rows of the partial writes"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn overwrite_and_rollback_handle_scope_identity_generated_and_long_names() {
    let (_c, url) = start_postgres().await;

    // Scoped overwrite refuses staged rows outside the window (SQL-51).
    exec(
        &url,
        "CREATE TABLE daily (day DATE, v INT); \
         INSERT INTO daily VALUES ('2024-01-01', 1), ('2024-01-02', 2)",
    )
    .await;
    let mut cfg = config(&url, "daily", WriteMode::Overwrite, &[]);
    cfg.scope = Some(OverwriteScope::Window {
        column: "day".into(),
        from: json!("2024-01-02"),
        to: json!("2024-01-03"),
    });
    let sink = PostgresSink::new(cfg).await.unwrap();
    sink.begin_overwrite().await.unwrap();
    sink.write_batch(&[
        json!({"day": "2024-01-02", "v": 20}),
        json!({"day": "2024-01-01", "v": 10}),
    ])
    .await
    .unwrap();
    let err = sink.commit_overwrite().await.unwrap_err();
    assert!(
        err.to_string().contains("outside the overwrite scope"),
        "{err}"
    );
    sink.abort_overwrite().await.unwrap();
    let rows = text_rows(&url, "SELECT day::text || '=' || v FROM daily ORDER BY day").await;
    assert_eq!(rows, vec!["2024-01-01=1", "2024-01-02=2"]);

    // Identity ALWAYS + generated column + an FK-referenced parent (SQL-107),
    // under a 63-byte table name (SQL-77), with the previous copy kept.
    let long = "p".repeat(63);
    exec(
        &url,
        &format!(
            "CREATE TABLE \"{long}\" (id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, \
             name TEXT, upper_name TEXT GENERATED ALWAYS AS (upper(name)) STORED, \
             _faucet_run_id TEXT); \
             INSERT INTO \"{long}\" (name, _faucet_run_id) VALUES ('a', 'r0'), ('b', 'r0'); \
             CREATE TABLE child (pid INT REFERENCES \"{long}\" (id) DEFERRABLE)"
        ),
    )
    .await;
    let cfg = || with_rollback(config(&url, &long, WriteMode::Overwrite, &[]), "r1", true);
    let sink = PostgresSink::new(cfg()).await.unwrap();
    sink.begin_overwrite().await.unwrap();
    sink.write_batch(&[
        json!({"id": 1, "name": "a2", "_faucet_run_id": "r1"}),
        json!({"name": "c", "_faucet_run_id": "r1"}),
    ])
    .await
    .unwrap();
    PostgresSink::new(cfg())
        .await
        .unwrap()
        .commit_overwrite()
        .await
        .unwrap();
    let rows = text_rows(
        &url,
        &format!("SELECT id || ':' || name || ':' || upper_name FROM \"{long}\" ORDER BY name"),
    )
    .await;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], "1:a2:A2");
    assert!(
        rows[1].ends_with(":c:C"),
        "identity filled by the target: {rows:?}"
    );

    let out = PostgresSink::new(cfg())
        .await
        .unwrap()
        .rollback_run("r1", &force(RollbackMode::Overwrite))
        .await
        .unwrap();
    assert!(out.applied);
    let rows = text_rows(
        &url,
        &format!("SELECT id || ':' || name || ':' || upper_name FROM \"{long}\" ORDER BY id"),
    )
    .await;
    assert_eq!(rows, vec!["1:a:A", "2:b:B"]);

    // A cascading reference is refused before anything is loaded.
    exec(
        &url,
        &format!(
            "CREATE TABLE cascade_child (pid INT REFERENCES \"{long}\" (id) ON DELETE CASCADE)"
        ),
    )
    .await;
    let err = PostgresSink::new(cfg())
        .await
        .unwrap()
        .begin_overwrite()
        .await
        .unwrap_err();
    assert!(err.to_string().contains("ON DELETE CASCADE"), "{err}");
}

#[tokio::test(flavor = "multi_thread")]
async fn rollback_restores_exact_numerics_for_large_pages() {
    let (_c, url) = start_postgres().await;
    exec(
        &url,
        "CREATE TABLE acct (id INT PRIMARY KEY, bal NUMERIC, doc JSONB, _faucet_run_id TEXT); \
         INSERT INTO acct SELECT g, 12345678901234567890.123456789012345678, \
         '{\"n\": 1.00000000000000000001}', 'r0' FROM generate_series(1, 17000) g",
    )
    .await;
    let sink = PostgresSink::new(with_rollback(
        config(&url, "acct", WriteMode::Upsert, &["id"]),
        "r1",
        false,
    ))
    .await
    .unwrap();
    let page: Vec<Value> = (1..=17000)
        .map(|i| json!({"id": i, "bal": 1, "_faucet_run_id": "r1"}))
        .collect();
    sink.write_batch(&page).await.unwrap();
    let out = sink
        .rollback_run("r1", &force(RollbackMode::Upsert))
        .await
        .unwrap();
    assert!(out.applied);
    assert_eq!(out.restored, 17000);
    let rows = text_rows(
        &url,
        "SELECT DISTINCT bal::text || '|' || (doc->'n')::text || '|' || _faucet_run_id FROM acct",
    )
    .await;
    assert_eq!(
        rows,
        vec!["12345678901234567890.123456789012345678|1.00000000000000000001|r0"]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn evolve_widens_integers_to_exact_numeric() {
    let (_c, url) = start_postgres().await;
    exec(
        &url,
        "CREATE TABLE w (id BIGINT); INSERT INTO w VALUES (9007199254740993)",
    )
    .await;
    let sink = PostgresSink::new(config(&url, "w", WriteMode::Append, &[]))
        .await
        .unwrap();
    sink.evolve_schema(&SchemaEvolution {
        widenings: vec![ColumnChange {
            name: "id".into(),
            from: Some(json!({"type": ["integer", "null"]})),
            to: json!({"type": ["number", "null"]}),
        }],
        ..Default::default()
    })
    .await
    .unwrap();
    sink.write_batch(&[json!({"id": 1.5})]).await.unwrap();
    let rows = text_rows(&url, "SELECT id::text FROM w ORDER BY id").await;
    assert_eq!(rows, vec!["1.5", "9007199254740993"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn keyed_and_jsonb_paths_of_every_write_entry_point() {
    let (_c, url) = start_postgres().await;
    exec(&url, "CREATE TABLE kv (id INT PRIMARY KEY, v TEXT)").await;
    let upsert = PostgresSink::new(config(&url, "kv", WriteMode::Upsert, &["id"]))
        .await
        .unwrap();
    let err = upsert
        .write_batch(&[json!({"v": "no key"})])
        .await
        .unwrap_err();
    assert!(err.to_string().contains("postgres upsert: row 0"), "{err}");
    let err = upsert
        .write_batch_idempotent(
            &[json!({"id": 1, "v": "a"}), json!({"v": "b"})],
            "s",
            &faucet_core::format_token(1),
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("postgres upsert: row 1"), "{err}");
    assert!(text_rows(&url, "SELECT id::text FROM kv").await.is_empty());

    exec(&url, "CREATE TABLE docs (data JSONB)").await;
    let json_sink = PostgresSink::new(PostgresSinkConfig::new(&url, "docs").column_mapping(
        PostgresColumnMapping::Jsonb {
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
        text_rows(&url, "SELECT count(*)::text FROM docs").await,
        vec!["2"]
    );
}
