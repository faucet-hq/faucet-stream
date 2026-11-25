//! A `>=` cursor on a BINARY_FLOAT column keeps the rows at the bookmark value
//! (#1114): `0.7` as a BINARY_FLOAT is `0.699999988…`, below its decimal.
//! Skips cleanly without Docker or Oracle Instant Client.

mod common;

use faucet_core::Source;
use faucet_source_oracle::{OracleReplication, OracleSource, OracleSourceConfig};
use serde_json::{Value, json};

async fn read(cfg: OracleSourceConfig, bookmark: Option<&Value>) -> (Vec<i64>, Value) {
    let source = OracleSource::new(cfg).await.expect("source");
    if let Some(b) = bookmark {
        source
            .apply_start_bookmark(b.clone())
            .await
            .expect("bookmark");
    }
    let (rows, last) = source.fetch_all_incremental().await.expect("read");
    let mut ids: Vec<i64> = rows.iter().map(|r| r["ID"].as_i64().unwrap()).collect();
    ids.sort_unstable();
    (ids, last.unwrap_or(Value::Null))
}

#[tokio::test(flavor = "multi_thread")]
async fn binary_float_cursor_keeps_rows_at_the_bookmark() {
    let Some((_container, conn)) = common::start_oracle().await else {
        return;
    };
    for (table, ty, query) in [
        ("T_F", "BINARY_FLOAT", "SELECT ID, C FROM T_F"),
        (
            "T_FU",
            "BINARY_FLOAT",
            "SELECT ID, C FROM T_FU WHERE C >= :bookmark",
        ),
        ("T_D", "BINARY_DOUBLE", "SELECT ID, C FROM T_D"),
    ] {
        common::exec(
            &conn,
            &[
                &format!("CREATE TABLE {table} (ID NUMBER, C {ty})"),
                &format!("INSERT INTO {table} VALUES (1, 0.5)"),
                &format!("INSERT INTO {table} VALUES (2, 0.7)"),
            ],
        )
        .await;
        let mut cfg = OracleSourceConfig::new(conn.clone(), query);
        cfg.replication = OracleReplication::Incremental {
            column: "C".into(),
            initial_value: json!(0.0),
        };
        let (first, bookmark) = read(cfg.clone(), None).await;
        assert_eq!(first, vec![1, 2], "{query}");
        assert_eq!(bookmark["value"], json!(0.7), "{query}: {bookmark}");

        common::exec(
            &conn,
            &[
                &format!("INSERT INTO {table} VALUES (3, 0.7)"),
                &format!("INSERT INTO {table} VALUES (4, 0.9)"),
            ],
        )
        .await;
        let (second, _) = read(cfg, Some(&bookmark)).await;
        assert_eq!(
            second,
            vec![3, 4],
            "{query}: rows committed at the bookmark value"
        );
    }
}
