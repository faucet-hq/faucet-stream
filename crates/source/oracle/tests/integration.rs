//! End-to-end tests against Oracle Free 23ai (`gvenzl/oracle-free:23-slim`).
//! Skips cleanly without Docker or Oracle Instant Client.

mod common;

use std::collections::HashMap;

use faucet_core::{Source, StreamPage};
use faucet_source_oracle::{OracleReplication, OracleSource, OracleSourceConfig, ShardConfig};
use futures::StreamExt;
use serde_json::{Value, json};

async fn drain(source: &OracleSource) -> Vec<StreamPage> {
    let ctx = HashMap::new();
    let mut s = source.stream_pages(&ctx, 0);
    let mut out = Vec::new();
    while let Some(p) = s.next().await {
        out.push(p.expect("page"));
    }
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn oracle_query_source_end_to_end() {
    let Some((_container, conn)) = common::start_oracle().await else {
        return;
    };
    common::exec(
        &conn,
        &[
            "CREATE TABLE T_TYPES (ID NUMBER(10) PRIMARY KEY, BIG NUMBER(38), AMOUNT NUMBER(30,10), \
             RATIO BINARY_DOUBLE, NAME VARCHAR2(50), NOTE CLOB, BIN RAW(8), BLOBV BLOB, \
             D DATE, TS TIMESTAMP(6), TSZ TIMESTAMP(6) WITH TIME ZONE, \
             IDS INTERVAL DAY(2) TO SECOND(6), IYM INTERVAL YEAR(2) TO MONTH, DOC JSON, \
             UPDATED_AT NUMBER(10))",
            "INSERT INTO T_TYPES VALUES (1, 123456789012345678901234567890, 12345678901234567.25, \
             1.5, 'alice', 'long note', HEXTORAW('0102'), HEXTORAW('FF'), \
             DATE '2024-01-02', TIMESTAMP '2024-01-02 03:04:05.5', \
             TIMESTAMP '2024-01-02 03:04:05 +05:30', INTERVAL '1 02:03:04.5' DAY TO SECOND, \
             INTERVAL '1-2' YEAR TO MONTH, JSON('{\"a\":1}'), 10)",
            "INSERT INTO T_TYPES (ID, NAME, UPDATED_AT) VALUES (2, 'bob', 20)",
            "INSERT INTO T_TYPES (ID, NAME, UPDATED_AT) VALUES (3, NULL, 30)",
        ],
    )
    .await;

    // Full read + exact type mapping.
    let mut cfg = OracleSourceConfig::new(
        conn.clone(),
        "SELECT ID, BIG, AMOUNT, RATIO, NAME, NOTE, BIN, BLOBV, D, TS, TSZ, IDS, IYM, \
         JSON_SERIALIZE(DOC RETURNING CLOB) AS DOC FROM T_TYPES ORDER BY ID;",
    );
    cfg.json_columns = vec!["DOC".into()];
    cfg.batch_size = 2;
    let source = OracleSource::new(cfg).await.expect("source");
    let pages = drain(&source).await;
    let rows: Vec<Value> = pages.iter().flat_map(|p| p.records.clone()).collect();
    assert_eq!(rows.len(), 3);
    assert_eq!(pages[0].records.len(), 2, "batch_size bounds pages");
    let r = &rows[0];
    assert_eq!(r["ID"], json!(1));
    assert_eq!(r["BIG"], json!("123456789012345678901234567890"));
    assert_eq!(r["AMOUNT"], json!("12345678901234567.25"));
    assert_eq!(r["RATIO"], json!(1.5));
    assert_eq!(r["NAME"], json!("alice"));
    assert_eq!(r["NOTE"], json!("long note"));
    assert_eq!(r["BIN"], json!("AQI="));
    assert_eq!(r["BLOBV"], json!("/w=="));
    assert_eq!(r["D"], json!("2024-01-02T00:00:00"));
    assert_eq!(r["TS"], json!("2024-01-02T03:04:05.500"));
    assert_eq!(r["TSZ"], json!("2024-01-02T03:04:05+05:30"));
    assert_eq!(r["IDS"], json!("P1DT2H3M4.5S"));
    assert_eq!(r["IYM"], json!("P1Y2M"));
    assert_eq!(r["DOC"], json!({"a": 1}));
    assert_eq!(rows[2]["NAME"], Value::Null);
    let (all, bm) = source.fetch_all_incremental().await.expect("fetch_all");
    assert_eq!(all.len(), 3);
    assert_eq!(bm, None);

    // Incremental with server pushdown and a resumed bookmark.
    let mut inc = OracleSourceConfig::new(
        conn.clone(),
        "SELECT ID, UPDATED_AT FROM T_TYPES WHERE UPDATED_AT > :bookmark AND ID < :1",
    );
    inc.params = vec![json!(100)];
    inc.replication = OracleReplication::Incremental {
        column: "UPDATED_AT".into(),
        initial_value: json!(10),
    };
    let source = OracleSource::new(inc).await.expect("source");
    assert!(source.state_key().unwrap().starts_with("oracle:FREEPDB1:"));
    let pages = drain(&source).await;
    let rows: Vec<Value> = pages.iter().flat_map(|p| p.records.clone()).collect();
    assert_eq!(rows.len(), 2);
    assert_eq!(pages.last().unwrap().bookmark, Some(json!(30)));
    source.apply_start_bookmark(json!(20)).await.unwrap();
    let (rows, bm) = source.fetch_all_incremental().await.unwrap();
    assert_eq!(rows, vec![json!({"ID": 3, "UPDATED_AT": 30})]);
    assert_eq!(bm, Some(json!(30)));

    // PK-range sharding covers every row exactly once.
    let mut sharded = OracleSourceConfig::new(conn.clone(), "SELECT ID FROM T_TYPES");
    sharded.shard = Some(ShardConfig { key: "ID".into() });
    let source = OracleSource::new(sharded).await.expect("source");
    assert!(source.is_shardable());
    let shards = source.enumerate_shards(2).await.expect("shards");
    assert!(shards.len() >= 2, "{shards:?}");
    let mut ids = Vec::new();
    for s in &shards {
        source.apply_shard(s).await.unwrap();
        for p in drain(&source).await {
            ids.extend(p.records.iter().map(|r| r["ID"].as_i64().unwrap()));
        }
    }
    ids.sort();
    assert_eq!(ids, vec![1, 2, 3]);
    source
        .apply_shard(&faucet_core::ShardSpec::whole())
        .await
        .unwrap();
    assert_eq!(
        drain(&source)
            .await
            .iter()
            .map(|p| p.records.len())
            .sum::<usize>(),
        3
    );

    // Discovery and the preflight probe.
    let ds = source.discover().await.expect("discover");
    let t = ds
        .iter()
        .find(|d| d.name == "FAUCET.T_TYPES")
        .expect("T_TYPES discovered");
    assert_eq!(t.config_patch["json_columns"], json!(["DOC"]));
    assert_eq!(
        t.schema.as_ref().unwrap()["properties"]["ID"]["type"],
        "integer"
    );
    let report = source
        .check(&faucet_core::check::CheckContext::default())
        .await
        .unwrap();
    assert_eq!(report.failed_count(), 0, "{report:?}");

    // A bad query surfaces as a typed error, not a panic or a silent empty stream.
    let bad = OracleSource::new(OracleSourceConfig::new(conn.clone(), "SELECT * FROM NOPE"))
        .await
        .unwrap();
    let ctx = HashMap::new();
    let mut s = bad.stream_pages(&ctx, 0);
    let err = s.next().await.expect("an item").unwrap_err();
    assert!(err.to_string().contains("ORA-00942"), "{err}");
    let native = OracleSource::new(OracleSourceConfig::new(conn, "SELECT DOC FROM T_TYPES"))
        .await
        .unwrap();
    let err = native.fetch_all().await.unwrap_err();
    assert!(err.to_string().contains("JSON_SERIALIZE"), "{err}");
}
