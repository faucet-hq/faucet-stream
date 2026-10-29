//! #975: a fan-out child row describes its source query with the parent's
//! values bound, so a sink that creates tables from declared column types
//! (#955) creates them typed for child rows too.
#![cfg(all(feature = "source-duckdb", feature = "sink-duckdb"))]

use clap::Parser as _;
use faucet_cli::cli::Cli;
use serde_json::{Value, json};
use std::collections::HashMap;

fn on_big_stack<F>(f: impl FnOnce() -> F + Send + 'static)
where
    F: std::future::Future<Output = ()>,
{
    std::thread::Builder::new()
        .stack_size(32 * 1024 * 1024)
        .spawn(move || {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(f())
        })
        .unwrap()
        .join()
        .unwrap();
}

async fn run(args: &[&str]) {
    let mut argv = vec!["faucet"];
    argv.extend_from_slice(args);
    let cli = Cli::try_parse_from(argv).expect("argv parses");
    Box::pin(faucet_cli::run_command(cli))
        .await
        .expect("run succeeds");
}

async fn query(database: &str, sql: &str) -> Vec<Value> {
    let source = faucet_cli::registry::build_source(
        "duckdb",
        json!({ "database": database, "query": sql, "read_only": true }),
        &HashMap::new(),
        None,
    )
    .await
    .expect("read-back source");
    source.fetch_all().await.expect("read back")
}

#[test]
fn a_child_row_creates_its_table_from_the_bound_query_types() {
    on_big_stack(|| async {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("out.duckdb");
        let out = out.to_str().unwrap();
        let cfg = dir.path().join("fanout.yaml");
        std::fs::write(
            &cfg,
            format!(
                "version: 1\n\
                 name: fanout-declared\n\
                 pipeline:\n\
                 \x20 source:\n\
                 \x20   type: duckdb\n\
                 \x20   config: {{ database: \":memory:\", query: \"SELECT * FROM (VALUES (1), (2)) AS p(id)\" }}\n\
                 \x20 sink:\n\
                 \x20   type: duckdb\n\
                 \x20   config: {{ database: \"{out}\", table_name: parents, column_mapping: auto_map }}\n\
                 execution: {{ max_concurrent: 1 }}\n\
                 matrix:\n\
                 \x20 - id: bills\n\
                 \x20 - id: prices\n\
                 \x20   parent: bills\n\
                 \x20   source:\n\
                 \x20     config:\n\
                 \x20       query: \"SELECT id, amount FROM (VALUES (1, CAST(12.50 AS DECIMAL(10,2))), (2, CAST(7.25 AS DECIMAL(10,2)))) AS t(id, amount) WHERE id = ${{bills.id}}\"\n\
                 \x20   sink:\n\
                 \x20     config: {{ table_name: prices }}\n"
            ),
        )
        .unwrap();
        run(&["run", "--output", "json", cfg.to_str().unwrap()]).await;

        let types = query(
            out,
            "SELECT column_name, data_type FROM information_schema.columns \
             WHERE table_name = 'prices' ORDER BY column_name",
        )
        .await;
        let amount = types
            .iter()
            .find(|r| r["column_name"] == "amount")
            .unwrap_or_else(|| panic!("no amount column in {types:?}"));
        assert_eq!(
            amount["data_type"], "DECIMAL(10,2)",
            "the child's table must take the declared type: {types:?}"
        );
        let rows = query(out, "SELECT id, amount FROM prices ORDER BY id").await;
        assert_eq!(rows.len(), 2, "{rows:?}");
        assert_eq!(rows[0]["amount"], "12.50", "{rows:?}");
        assert_eq!(rows[1]["amount"], "7.25", "{rows:?}");
    });
}
