//! LogMiner CDC end-to-end against Oracle Free 23ai. Skips cleanly without
//! Docker or Oracle Instant Client.

mod common;

use std::collections::HashMap;
use std::time::Duration;

use faucet_common_oracle::OracleConnectionConfig;
use faucet_common_oracle::oracle;
use faucet_core::Source;
use faucet_source_oracle_cdc::{OracleCdcSource, OracleCdcSourceConfig, StartPosition};
use futures::StreamExt;
use serde_json::{Value, json};

async fn drain(source: &OracleCdcSource) -> (Vec<Value>, Option<Value>, usize) {
    let ctx = HashMap::new();
    let mut s = source.stream_pages(&ctx, 0);
    let (mut records, mut bookmark, mut pages) = (Vec::new(), None, 0);
    while let Some(p) = s.next().await {
        let p = p.expect("page");
        if !p.records.is_empty() {
            pages += 1;
        }
        records.extend(p.records);
        if p.bookmark.is_some() {
            bookmark = p.bookmark;
        }
    }
    (records, bookmark, pages)
}

fn cfg(conn: &OracleConnectionConfig) -> OracleCdcSourceConfig {
    let mut c = OracleCdcSourceConfig::new(conn.clone(), vec!["FAUCET.CDC_T".into()]);
    c.idle_timeout = Duration::from_secs(4);
    c.poll_interval = Duration::from_millis(500);
    c
}

fn first_scn(records: &[Value]) -> u64 {
    records
        .iter()
        .filter_map(|r| r["scn"].as_u64())
        .min()
        .unwrap()
}

fn dml_ops(records: &[Value]) -> Vec<String> {
    records
        .iter()
        .filter(|r| r["table"] == "CDC_T")
        .map(|r| {
            let img = if r["op"] == "d" {
                &r["before"]
            } else {
                &r["after"]
            };
            format!("{}:{}", r["op"].as_str().unwrap(), img["ID"])
        })
        .collect()
}

/// Open a session that inserts without committing; commit it later.
async fn open_txn(conn: &OracleConnectionConfig, sql: &'static str) -> oracle::Connection {
    let conn = conn.clone();
    tokio::task::spawn_blocking(move || {
        let c = oracle::Connection::connect(
            &conn.username,
            &conn.password,
            conn.resolve_connect_string().unwrap(),
        )
        .unwrap();
        c.execute(sql, &[]).unwrap();
        c
    })
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn oracle_logminer_cdc_end_to_end() {
    let t0 = std::time::Instant::now();
    let Some((container, conn)) = common::start_oracle().await else {
        return;
    };
    eprintln!("container up after {:?}", t0.elapsed());
    common::enable_logminer(&container, &conn).await;
    eprintln!("archivelog on after {:?}", t0.elapsed());
    common::exec(
        &conn,
        &[
            "CREATE TABLE CDC_T (ID NUMBER(10) PRIMARY KEY, NAME VARCHAR2(50), AMT NUMBER(12,2), \
           NOTE CLOB, TS TIMESTAMP(6))",
        ],
    )
    .await;

    let err = OracleCdcSource::new(cfg(&conn))
        .await
        .err()
        .expect("keyless logging rejected");
    assert!(
        err.to_string()
            .contains("ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS"),
        "{err}"
    );
    common::exec(
        &conn,
        &["ALTER TABLE CDC_T ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS"],
    )
    .await;
    let source = OracleCdcSource::new(cfg(&conn)).await.expect("source");
    assert!(source.supports_exactly_once());
    assert_eq!(
        source.state_key().unwrap(),
        "oracle-cdc:FREEPDB1:FAUCET.CDC_T"
    );
    let report = source
        .check(&faucet_core::check::CheckContext::default())
        .await
        .unwrap();
    assert_eq!(report.failed_count(), 0, "{report:?}");

    let anchor = source
        .capture_resume_position()
        .await
        .unwrap()
        .expect("position");
    common::exec(
        &conn,
        &[
            "INSERT INTO CDC_T VALUES (1, 'it''s', 12.5, 'short note', TIMESTAMP '2024-01-02 03:04:05.5')",
            "INSERT INTO CDC_T (ID, NAME) VALUES (2, 'two')",
        ],
    )
    .await;
    common::exec(
        &conn,
        &["UPDATE CDC_T SET NAME = 'uno', AMT = NULL WHERE ID = 1"],
    )
    .await;
    common::exec(&conn, &["DELETE FROM CDC_T WHERE ID = 2"]).await;
    common::exec(
        &conn,
        &[
            "DECLARE c CLOB; BEGIN c := RPAD('a', 5000, 'a') || RPAD('b', 3000, 'b'); \
             INSERT INTO CDC_T (ID, NOTE) VALUES (3, c); END;",
        ],
    )
    .await;
    // Rolled back work and a savepoint undo must never surface.
    let rb = open_txn(&conn, "INSERT INTO CDC_T (ID) VALUES (90)").await;
    tokio::task::spawn_blocking(move || rb.rollback().unwrap())
        .await
        .unwrap();
    common::exec(
        &conn,
        &[
            "INSERT INTO CDC_T (ID) VALUES (4)",
            "SAVEPOINT sp",
            "INSERT INTO CDC_T (ID) VALUES (91)",
            "ROLLBACK TO sp",
        ],
    )
    .await;

    source.apply_start_bookmark(anchor.clone()).await.unwrap();
    let (records, bookmark, pages) = drain(&source).await;
    eprintln!("first drain done after {:?}", t0.elapsed());
    assert_eq!(
        dml_ops(&records),
        vec!["i:1", "i:2", "u:1", "d:2", "i:3", "i:4"],
        "{:?}",
        records
            .iter()
            .map(|r| format!("{} {} {}", r["op"], r["table"], r["after"]["ID"]))
            .collect::<Vec<_>>()
    );
    assert_eq!(pages, 5, "one page per committed transaction");
    let first = &records[0];
    assert_eq!(first["schema"], "FAUCET");
    assert_eq!(
        first["after"],
        json!({"ID": 1, "NAME": "it's", "AMT": 12.5, "NOTE": "short note", "TS": "2024-01-02T03:04:05.500"})
    );
    let upd = records.iter().find(|r| r["op"] == "u").unwrap();
    assert_eq!(upd["before"]["NAME"], "it's");
    assert_eq!(upd["after"]["NAME"], "uno");
    assert_eq!(upd["after"]["AMT"], Value::Null);
    let big = records.iter().find(|r| r["after"]["ID"] == 3).unwrap();
    let note = big["after"]["NOTE"].as_str().unwrap();
    assert_eq!(note.len(), 8000);
    assert!(note.starts_with("aaaa") && note.ends_with("bbbb"));
    let bookmark = bookmark.expect("bookmark");

    // Resume: nothing re-emitted, new changes captured once.
    common::exec(&conn, &["INSERT INTO CDC_T (ID) VALUES (5)"]).await;
    source.apply_start_bookmark(bookmark.clone()).await.unwrap();
    let (records, bookmark2, _) = drain(&source).await;
    assert_eq!(dml_ops(&records), vec!["i:5"], "{records:#?}");

    // A transaction open across the anchor is captured whole.
    let open = open_txn(&conn, "INSERT INTO CDC_T (ID) VALUES (6)").await;
    let anchor = source.capture_resume_position().await.unwrap().unwrap();
    tokio::task::spawn_blocking(move || {
        open.execute("INSERT INTO CDC_T (ID) VALUES (7)", &[])
            .unwrap();
        open.commit().unwrap();
    })
    .await
    .unwrap();
    source.apply_start_bookmark(anchor).await.unwrap();
    let (records, bookmark3, _) = drain(&source).await;
    assert_eq!(dml_ops(&records), vec!["i:6", "i:7"], "{records:#?}");
    let pre_ddl_scn = first_scn(&records);
    let _ = bookmark2;

    // DDL and TRUNCATE surface as envelopes cdc_unwrap drops.
    common::exec(
        &conn,
        &[
            "ALTER TABLE CDC_T ADD (EXTRA NUMBER)",
            "INSERT INTO CDC_T (ID, EXTRA) VALUES (8, 42)",
        ],
    )
    .await;
    common::exec(&conn, &["TRUNCATE TABLE CDC_T"]).await;
    source
        .apply_start_bookmark(bookmark3.unwrap())
        .await
        .unwrap();
    let (records, bookmark4, _) = drain(&source).await;
    let ops: Vec<&str> = records.iter().map(|r| r["op"].as_str().unwrap()).collect();
    assert!(ops.contains(&"ddl") && ops.contains(&"truncate"), "{ops:?}");
    let eight = records
        .iter()
        .find(|r| r["after"]["ID"] == 8)
        .expect("row 8");
    assert_eq!(eight["after"]["EXTRA"], 42, "metadata reloads after DDL");

    // Re-mining redo written before the DDL is a dictionary mismatch: fail loudly.
    assert!(bookmark4.is_some());
    let before_ddl = serde_json::json!({"commit_scn": 1, "restart_scn": pre_ddl_scn - 1});
    source.apply_start_bookmark(before_ddl).await.unwrap();
    let ctx = HashMap::new();
    let mut s = source.stream_pages(&ctx, 0);
    let mut failed = None;
    while let Some(p) = s.next().await {
        if let Err(e) = p {
            failed = Some(e);
            break;
        }
    }
    drop(s);
    assert!(
        failed
            .expect("mismatch surfaces")
            .to_string()
            .contains("dictionary mismatch")
    );

    // Aggregate mode and a fresh `earliest` start, on a table with no DDL history.
    common::exec(
        &conn,
        &[
            "CREATE TABLE CDC_E (ID NUMBER PRIMARY KEY)",
            "ALTER TABLE CDC_E ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
            "INSERT INTO CDC_E VALUES (9)",
        ],
    )
    .await;
    let mut agg = cfg(&conn);
    agg.tables = vec!["FAUCET.CDC_E".into()];
    agg.batch_size = 0;
    agg.start_position = StartPosition::Earliest;
    let agg = OracleCdcSource::new(agg).await.unwrap();
    let all = agg.fetch_all().await.unwrap();
    assert!(all.iter().any(|r| r["after"]["ID"] == 9), "{all:#?}");

    // A fresh `current` start anchors and captures nothing historical.
    let fresh = OracleCdcSource::new(cfg(&conn)).await.unwrap();
    let (records, bookmark, _) = drain(&fresh).await;
    assert!(records.is_empty(), "{records:#?}");
    assert!(bookmark.is_some());

    // Misconfiguration is reported up front.
    let missing = OracleCdcSource::new(OracleCdcSourceConfig {
        tables: vec!["FAUCET.NOPE".into()],
        ..cfg(&conn)
    })
    .await;
    assert!(missing.err().unwrap().to_string().contains("not visible"));
}
