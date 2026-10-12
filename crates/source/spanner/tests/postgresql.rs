//! Integration tests against a **PostgreSQL-dialect** database on the Cloud
//! Spanner emulator: type decoding, `@bookmark` / `@name` parameter binding
//! (rewritten to `$N`), discovery and PK-range sharding.

mod support;

use std::collections::HashMap;

use faucet_core::Source;
use faucet_source_spanner::{ShardConfig, SpannerReplication, SpannerSource, SpannerSourceConfig};
use futures::TryStreamExt;
use serde_json::{Value, json};

fn cfg(host: &str, database: &str, query: &str) -> SpannerSourceConfig {
    let mut cfg = SpannerSourceConfig::new(support::PROJECT, support::INSTANCE, database, query);
    cfg.connection = support::pg_connection(host, database);
    cfg
}

async fn drain(source: &SpannerSource) -> (Vec<Value>, Option<Value>) {
    let ctx = HashMap::new();
    let mut stream = source.stream_pages(&ctx, 0);
    let mut records = Vec::new();
    let mut bookmark = None;
    while let Some(page) = stream.try_next().await.expect("page") {
        records.extend(page.records);
        if page.bookmark.is_some() {
            bookmark = page.bookmark;
        }
    }
    (records, bookmark)
}

#[tokio::test(flavor = "multi_thread")]
async fn pg_types_round_trip_to_json() {
    let Some(emu) = support::start_emulator().await else {
        return;
    };
    support::create_pg_database(
        &emu.host,
        "pg-types",
        &["CREATE TABLE typed (
            id bigint NOT NULL,
            f double precision,
            b boolean,
            s character varying,
            bin bytea,
            n numeric,
            ts timestamp with time zone,
            d date,
            j jsonb,
            arr bigint[],
            PRIMARY KEY (id)
        )"],
    )
    .await;
    let client = support::raw_client(&emu.host, "pg-types").await;
    support::execute_dml(
        &client,
        "INSERT INTO typed (id, f, b, s, bin, n, ts, d, j, arr) VALUES (
            9007199254740993, 2.5, TRUE, 'hello',
            '\\x0102'::bytea, 123.456789,
            '2026-01-02 03:04:05+00'::timestamptz, '2026-01-02'::date,
            '{\"a\": [1, 2]}'::jsonb, ARRAY[1, 2, 3]::bigint[]
        )",
    )
    .await;
    support::execute_dml(&client, "INSERT INTO typed (id) VALUES (2)").await;

    let source = SpannerSource::new(cfg(
        &emu.host,
        "pg-types",
        "SELECT * FROM typed ORDER BY id",
    ))
    .await
    .expect("source");
    // `ORDER BY id` puts the large id second.
    let (records, _) = drain(&source).await;
    assert_eq!(records.len(), 2);
    let r = records
        .iter()
        .find(|r| r["id"] == json!(9007199254740993i64))
        .expect("typed row");
    assert_eq!(r["f"], json!(2.5));
    assert_eq!(r["b"], json!(true));
    assert_eq!(r["s"], json!("hello"));
    assert_eq!(r["bin"], json!("AQI="));
    assert_eq!(r["n"], json!("123.456789"));
    assert!(r["ts"].as_str().unwrap().starts_with("2026-01-02T03:04:05"));
    assert_eq!(r["d"], json!("2026-01-02"));
    assert_eq!(r["j"], json!({"a": [1, 2]}));
    assert_eq!(r["arr"], json!([1, 2, 3]));
    let null_row = records.iter().find(|r| r["id"] == json!(2)).unwrap();
    for col in ["f", "b", "s", "bin", "n", "ts", "d", "j", "arr"] {
        assert_eq!(null_row[col], Value::Null, "column {col} must be null");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn pg_named_params_are_rewritten_and_bound() {
    let Some(emu) = support::start_emulator().await else {
        return;
    };
    support::create_pg_database(
        &emu.host,
        "pg-params",
        &["CREATE TABLE ev (id bigint NOT NULL, region character varying, PRIMARY KEY (id))"],
    )
    .await;
    let client = support::raw_client(&emu.host, "pg-params").await;
    support::execute_dml(
        &client,
        "INSERT INTO ev (id, region) VALUES (1, 'eu'), (2, 'us'), (3, 'eu')",
    )
    .await;

    let mut config = cfg(
        &emu.host,
        "pg-params",
        "SELECT * FROM ev WHERE region = @region ORDER BY id",
    );
    config.params.insert("region".into(), json!("eu"));
    // A param the query never mentions must be dropped, not bound.
    config.params.insert("unused".into(), json!(1));
    let source = SpannerSource::new(config).await.expect("source");
    let (rows, _) = drain(&source).await;
    let ids: Vec<i64> = rows.iter().map(|r| r["id"].as_i64().unwrap()).collect();
    assert_eq!(ids, vec![1, 3]);
}

#[tokio::test(flavor = "multi_thread")]
async fn pg_incremental_resume_reads_only_new_rows() {
    let Some(emu) = support::start_emulator().await else {
        return;
    };
    support::create_pg_database(
        &emu.host,
        "pg-incr",
        &["CREATE TABLE ev (id bigint NOT NULL, v character varying, PRIMARY KEY (id))"],
    )
    .await;
    let client = support::raw_client(&emu.host, "pg-incr").await;
    support::execute_dml(&client, "INSERT INTO ev (id, v) VALUES (1, 'a'), (2, 'b')").await;

    let mut config = cfg(
        &emu.host,
        "pg-incr",
        "SELECT * FROM ev WHERE id > @bookmark",
    );
    config.replication = SpannerReplication::Incremental {
        column: "id".into(),
        initial_value: json!(0),
    };
    let source = SpannerSource::new(config).await.expect("source");
    let (first, bookmark) = drain(&source).await;
    assert_eq!(first.len(), 2);
    let bookmark = bookmark.expect("bookmark on final page");
    assert_eq!(bookmark, json!(2));

    support::execute_dml(&client, "INSERT INTO ev (id, v) VALUES (3, 'c'), (4, 'd')").await;
    source
        .apply_start_bookmark(bookmark)
        .await
        .expect("apply bookmark");
    let (second, bookmark) = drain(&source).await;
    let ids: Vec<i64> = second.iter().map(|r| r["id"].as_i64().unwrap()).collect();
    assert_eq!(ids, vec![3, 4]);
    assert_eq!(bookmark, Some(json!(4)));
}

#[tokio::test(flavor = "multi_thread")]
async fn pg_discover_returns_public_tables_with_schema() {
    let Some(emu) = support::start_emulator().await else {
        return;
    };
    support::create_pg_database(
        &emu.host,
        "pg-disco",
        &[
            "CREATE TABLE orders (id bigint NOT NULL, note character varying(64), PRIMARY KEY (id))",
            "CREATE TABLE users (uid bigint NOT NULL, score double precision NOT NULL, \
             PRIMARY KEY (uid))",
        ],
    )
    .await;
    let source = SpannerSource::new(cfg(&emu.host, "pg-disco", "SELECT 1"))
        .await
        .expect("source");
    let mut datasets = source.discover().await.expect("discover");
    datasets.sort_by(|a, b| a.name.cmp(&b.name));
    assert_eq!(datasets.len(), 2, "system schemas are excluded");
    assert_eq!(datasets[0].name, "orders");
    assert_eq!(
        datasets[0].config_patch["query"],
        "SELECT * FROM \"orders\""
    );
    let schema = datasets[0].schema.as_ref().expect("schema");
    assert_eq!(schema["properties"]["id"]["type"], "integer");
    assert_eq!(
        schema["properties"]["note"]["type"],
        json!(["string", "null"])
    );
    assert_eq!(
        datasets[1].schema.as_ref().unwrap()["properties"]["score"]["type"],
        "number"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn pg_shards_partition_the_read_without_loss_or_duplication() {
    let Some(emu) = support::start_emulator().await else {
        return;
    };
    support::create_pg_database(
        &emu.host,
        "pg-shards",
        &["CREATE TABLE items (id bigint NOT NULL, v character varying, PRIMARY KEY (id))"],
    )
    .await;
    let client = support::raw_client(&emu.host, "pg-shards").await;
    let values: Vec<String> = (0..100).map(|i| format!("({i}, 'v{i}')")).collect();
    support::execute_dml(
        &client,
        &format!("INSERT INTO items (id, v) VALUES {}", values.join(", ")),
    )
    .await;

    let mut config = cfg(&emu.host, "pg-shards", "SELECT * FROM items");
    config.shard = Some(ShardConfig { key: "id".into() });
    let source = SpannerSource::new(config).await.expect("source");
    let shards = source.enumerate_shards(4).await.expect("enumerate");
    assert_eq!(shards.len(), 4);

    let mut seen: Vec<i64> = Vec::new();
    for shard in &shards {
        source.apply_shard(shard).await.expect("apply");
        let (records, _) = drain(&source).await;
        seen.extend(records.iter().map(|r| r["id"].as_i64().unwrap()));
    }
    seen.sort_unstable();
    assert_eq!(seen, (0..100).collect::<Vec<i64>>(), "no loss, no dupes");
}
