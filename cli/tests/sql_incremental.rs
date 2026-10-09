#![cfg(all(
    feature = "source-postgres",
    feature = "source-mysql",
    feature = "sink-file"
))]
//! `replication: incremental` on the postgres and mysql query sources (#825),
//! end to end through `faucet run` with a `file` state store: a second run
//! writes only the rows that changed since the first, including a row that
//! committed late with the boundary cursor value. Requires Docker.

use serde_json::Value;
use std::path::{Path, PathBuf};
use testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};
use testcontainers_modules::{mysql::Mysql, postgres::Postgres};

fn run_args(config: PathBuf) -> faucet_cli::cli::RunArgs {
    faucet_cli::cli::RunArgs {
        config: Some(config),
        no_env_file: true,
        ..Default::default()
    }
}

fn write_config(dir: &Path, kind: &str, url: &str, initial: &str, out: &Path) -> PathBuf {
    let path = dir.join("pipeline.yaml");
    std::fs::write(
        &path,
        format!(
            r#"
version: 1
name: items_sync
pipeline:
  source:
    type: {kind}
    config:
      connection_url: "{url}"
      query: "SELECT id, updated_at, v FROM items"
      batch_size: 2
      replication:
        type: incremental
        column: updated_at
        initial_value: "{initial}"
  sink:
    type: file
    config:
      path: "{out}"
  state:
    type: file
    config:
      path: "{state}"
"#,
            out = out.display(),
            state = dir.join("state").display(),
        ),
    )
    .unwrap();
    path
}

fn written_ids(path: &Path) -> Vec<i64> {
    let mut ids: Vec<i64> = std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .map(|l| {
            serde_json::from_str::<Value>(l).unwrap()["id"]
                .as_i64()
                .unwrap()
        })
        .collect();
    ids.sort_unstable();
    ids
}

async fn two_runs(kind: &str, url: &str, initial: &str, insert_more: impl AsyncFnOnce()) {
    let dir = tempfile::tempdir().unwrap();
    let first = dir.path().join("first.jsonl");
    let cfg = write_config(dir.path(), kind, url, initial, &first);
    faucet_cli::commands::run::run(run_args(cfg)).await.unwrap();
    assert_eq!(written_ids(&first), vec![2, 3, 4]);

    insert_more().await;
    let second = dir.path().join("second.jsonl");
    let cfg = write_config(dir.path(), kind, url, initial, &second);
    faucet_cli::commands::run::run(run_args(cfg)).await.unwrap();
    assert_eq!(
        written_ids(&second),
        vec![5, 6],
        "only rows new since run one"
    );

    let third = dir.path().join("third.jsonl");
    let cfg = write_config(dir.path(), kind, url, initial, &third);
    faucet_cli::commands::run::run(run_args(cfg)).await.unwrap();
    assert!(written_ids(&third).is_empty());
}

const ROWS: &str = "INSERT INTO items VALUES \
    (1, '2023-12-31 00:00:00', 'old'), (2, '2024-01-02 00:00:00', 'a'), \
    (3, '2024-01-03 00:00:00', 'b'), (4, '2024-01-03 00:00:00', 'c')";
const MORE: &str = "INSERT INTO items VALUES \
    (5, '2024-01-03 00:00:00', 'late'), (6, '2024-01-04 00:00:00', 'new')";

#[tokio::test(flavor = "multi_thread")]
async fn postgres_second_run_reads_only_new_rows() {
    let c: ContainerAsync<Postgres> = Postgres::default()
        .with_tag("16-alpine")
        .start()
        .await
        .unwrap();
    let port = c.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    let pool = sqlx::PgPool::connect(&url).await.unwrap();
    sqlx::query(
        "CREATE TABLE items (id BIGINT PRIMARY KEY, updated_at TIMESTAMP NOT NULL, v TEXT)",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(ROWS).execute(&pool).await.unwrap();
    two_runs("postgres", &url, "2024-01-01 00:00:00", async || {
        sqlx::query(MORE).execute(&pool).await.unwrap();
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn mysql_second_run_reads_only_new_rows() {
    use sqlx::Connection;
    let c: ContainerAsync<Mysql> = Mysql::default().start().await.unwrap();
    let port = c.get_host_port_ipv4(3306).await.unwrap();
    let url = format!("mysql://root@127.0.0.1:{port}/test");
    let mut conn = sqlx::MySqlConnection::connect(&url).await.unwrap();
    sqlx::raw_sql(
        "CREATE TABLE items (id BIGINT PRIMARY KEY, updated_at DATETIME NOT NULL, v TEXT)",
    )
    .execute(&mut conn)
    .await
    .unwrap();
    sqlx::raw_sql(ROWS).execute(&mut conn).await.unwrap();
    two_runs("mysql", &url, "2024-01-01 00:00:00", async || {
        sqlx::raw_sql(MORE).execute(&mut conn).await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn validate_accepts_the_replication_block_and_refuses_a_strict_token() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("o.jsonl");
    for kind in ["postgres", "mysql"] {
        let cfg = write_config(dir.path(), kind, "x://h/db", "2024-01-01", &out);
        let text = std::fs::read_to_string(&cfg).unwrap();
        let parsed = faucet_cli::config::PipelineConfig::from_text(&text, &cfg).unwrap();
        faucet_cli::registry::validate_source_config(kind, "default", parsed_source(&parsed))
            .unwrap();
        let token = if kind == "postgres" {
            "${bookmark}"
        } else {
            "@bookmark"
        };
        let strict = text.replace(
            "FROM items\"",
            &format!("FROM items WHERE updated_at > {token}\""),
        );
        let parsed = faucet_cli::config::PipelineConfig::from_text(&strict, &cfg).unwrap();
        let err =
            faucet_cli::registry::validate_source_config(kind, "default", parsed_source(&parsed))
                .unwrap_err();
        assert!(err.to_string().contains(">="), "{err}");
    }
}

fn parsed_source(cfg: &faucet_cli::config::PipelineConfig) -> Value {
    cfg.pipeline.source.as_ref().unwrap().config.clone()
}
