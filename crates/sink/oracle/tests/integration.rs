//! End-to-end tests against Oracle Free 23ai (`gvenzl/oracle-free:23-slim`).
//! Skips cleanly without Docker or Oracle Instant Client.

mod common;

use faucet_common_oracle::OracleConnectionConfig;
use faucet_core::drift::{ColumnChange, SchemaEvolution};
use faucet_core::{DeleteMarker, Sink, WriteMode, WriteSpec};
use faucet_sink_oracle::{
    IdentifierCase, OnUnknownField, OracleColumnMapping, OracleSink, OracleSinkConfig,
};
use serde_json::json;

async fn count(conn: &OracleConnectionConfig, table: &str) -> usize {
    common::query_strings(conn, &format!("SELECT TO_CHAR(COUNT(*)) FROM {table}")).await[0]
        .as_deref()
        .unwrap()
        .parse()
        .unwrap()
}

fn keyed(conn: &OracleConnectionConfig, table: &str, mode: WriteMode) -> OracleSinkConfig {
    let mut cfg = OracleSinkConfig::new(conn.clone(), table);
    cfg.write = WriteSpec {
        write_mode: mode,
        key: vec!["ID".into()],
        delete_marker: Some(DeleteMarker {
            field: "__op".into(),
            values: vec!["d".into()],
        }),
        rollback: None,
    };
    cfg
}

#[tokio::test(flavor = "multi_thread")]
async fn oracle_sink_end_to_end() {
    let Some((_container, conn)) = common::start_oracle().await else {
        return;
    };

    // Append into an auto-created table, typed from the first page.
    let sink = OracleSink::new(OracleSinkConfig::new(conn.clone(), "EVENTS"))
        .await
        .expect("sink");
    let n = sink
        .write_batch(&[
            json!({"ID": 1, "NAME": "a", "AMOUNT": 1.5, "OK": true, "DOC": {"x": 1}}),
            json!({"ID": 2, "NAME": "b", "AMOUNT": 2}),
        ])
        .await
        .expect("append");
    assert_eq!(n, 2);
    assert_eq!(count(&conn, "EVENTS").await, 2);
    let docs = common::query_strings(&conn, "SELECT DOC FROM EVENTS ORDER BY ID").await;
    assert_eq!(docs, vec![Some("{\"x\":1}".into()), None]);
    let flags = common::query_strings(&conn, "SELECT TO_CHAR(OK) FROM EVENTS ORDER BY ID").await;
    assert_eq!(flags, vec![Some("1".into()), None]);
    assert_eq!(sink.write_batch(&[]).await.unwrap(), 0);
    assert_eq!(sink.connector_name(), "oracle");
    assert!(sink.dataset_uri().ends_with("?table=EVENTS"));

    // Exact types into a pre-created table: big NUMBERs, timestamps, binary, intervals.
    common::exec(
        &conn,
        &[
            "CREATE TABLE TYPED (ID NUMBER(38) PRIMARY KEY, AMT NUMBER(30,10), TS TIMESTAMP(6), \
           TSZ TIMESTAMP(6) WITH TIME ZONE, D DATE, BIN RAW(16), B BLOB, IDS INTERVAL DAY(2) TO \
           SECOND(6), IYM INTERVAL YEAR(2) TO MONTH, R BINARY_DOUBLE, NOTE NVARCHAR2(20), J JSON)",
        ],
    )
    .await;
    let typed = OracleSink::new(OracleSinkConfig::new(conn.clone(), "TYPED"))
        .await
        .unwrap();
    typed
        .write_batch(&[json!({
            "ID": "123456789012345678901234567890", "AMT": "12345678901234567.25",
            "TS": "2024-01-02T03:04:05.5", "TSZ": "2024-01-02T03:04:05+05:30",
            "D": "2024-01-02T00:00:00", "BIN": "AQI=", "B": "/w==", "IDS": "P1DT2H3M4.5S",
            "IYM": "P1Y2M", "R": 2.5, "NOTE": "héllo", "J": {"k": [1, 2]}
        })])
        .await
        .expect("typed write");
    let row = common::query_strings(
        &conn,
        "SELECT TO_CHAR(ID) || '|' || TO_CHAR(AMT) || '|' || TO_CHAR(TS, 'YYYY-MM-DD HH24:MI:SS.FF1') \
         || '|' || TO_CHAR(TSZ, 'TZH:TZM') || '|' || RAWTOHEX(BIN) || '|' || TO_CHAR(IDS) || '|' || \
         TO_CHAR(IYM) || '|' || NOTE || '|' || JSON_SERIALIZE(J) FROM TYPED",
    )
    .await;
    assert_eq!(
        row[0].as_deref().unwrap(),
        "123456789012345678901234567890|12345678901234567.25|2024-01-02 03:04:05.5|+05:30|0102|\
         +01 02:03:04.500000|+01-02|héllo|{\"k\":[1,2]}"
    );

    // Per-row DLQ outcomes: a server rejection and a client-side encoding error.
    common::exec(
        &conn,
        &["CREATE TABLE NARROW (ID NUMBER PRIMARY KEY, CODE VARCHAR2(3))"],
    )
    .await;
    let narrow = OracleSink::new(OracleSinkConfig::new(conn.clone(), "NARROW"))
        .await
        .unwrap();
    let outcomes = narrow
        .write_batch_partial(&[
            json!({"ID": 1, "CODE": "ok"}),
            json!({"ID": 2, "CODE": "far too long"}),
            json!({"ID": "not-a-number", "CODE": "x"}),
            json!({"ID": 4, "CODE": "ok"}),
        ])
        .await
        .expect("partial");
    assert!(outcomes[0].is_ok() && outcomes[3].is_ok(), "{outcomes:?}");
    assert!(
        outcomes[1]
            .as_ref()
            .unwrap_err()
            .to_string()
            .contains("ORA-12899")
    );
    assert!(
        outcomes[2]
            .as_ref()
            .unwrap_err()
            .to_string()
            .contains("not a number")
    );
    assert_eq!(count(&conn, "NARROW").await, 2);
    let err = narrow
        .write_batch(&[json!({"ID": 9, "CODE": "far too long"})])
        .await
        .unwrap_err();
    assert!(err.to_string().contains("ORA-12899"), "{err}");
    assert_eq!(
        count(&conn, "NARROW").await,
        2,
        "a failed page commits nothing"
    );

    // Upsert + delete via MERGE, on an auto-created keyed table.
    let up = OracleSink::new(keyed(&conn, "ITEMS", WriteMode::Upsert))
        .await
        .unwrap();
    assert!(up.dedups_by_key());
    up.write_batch(&[json!({"ID": 1, "V": "a"}), json!({"ID": 2, "V": "b"})])
        .await
        .unwrap();
    up.write_batch(&[json!({"ID": 1, "V": "a2"}), json!({"ID": 2, "__op": "d"})])
        .await
        .unwrap();
    let vals = common::query_strings(&conn, "SELECT TO_CHAR(ID) || V FROM ITEMS ORDER BY ID").await;
    assert_eq!(vals, vec![Some("1a2".into())]);
    let outcomes = up
        .write_batch_partial(&[json!({"ID": 3, "V": "c"}), json!({"V": "no key"})])
        .await
        .unwrap();
    assert!(outcomes[0].is_ok() && outcomes[1].is_err());
    assert!(up.write_batch(&[json!({"V": "no key"})]).await.is_err());
    let del = OracleSink::new(keyed(&conn, "ITEMS", WriteMode::Delete))
        .await
        .unwrap();
    del.write_batch(&[json!({"ID": 3})]).await.unwrap();
    assert_eq!(count(&conn, "ITEMS").await, 1);

    // Exactly-once: data + watermark commit together; replays are detectable.
    let eo = OracleSink::new(keyed(&conn, "EO_T", WriteMode::Upsert))
        .await
        .unwrap();
    assert!(eo.supports_idempotent_writes());
    assert_eq!(eo.last_committed_token("pipe::row").await.unwrap(), None);
    let token = format!("{}#{{\"offset\":7}}", "0".repeat(20));
    eo.write_batch_idempotent(&[json!({"ID": 1, "V": "x"})], "pipe::row", &token)
        .await
        .unwrap();
    assert_eq!(
        eo.last_committed_token("pipe::row").await.unwrap(),
        Some(token.clone())
    );
    eo.write_batch_idempotent(&[], "pipe::row", "00000000000000000002")
        .await
        .unwrap();
    assert_eq!(
        eo.last_committed_token("pipe::row")
            .await
            .unwrap()
            .as_deref(),
        Some("00000000000000000002")
    );
    let eo_append = OracleSink::new(OracleSinkConfig::new(conn.clone(), "EO_A"))
        .await
        .unwrap();
    eo_append
        .write_batch_idempotent(&[json!({"ID": 1})], "pipe::a", "00000000000000000001")
        .await
        .unwrap();
    assert_eq!(count(&conn, "EO_A").await, 1);

    // Overwrite: first run renames staging into place; later runs swap atomically.
    let ow_cfg = OracleSinkConfig {
        write: WriteSpec {
            write_mode: WriteMode::Overwrite,
            ..Default::default()
        },
        ..OracleSinkConfig::new(conn.clone(), "SNAP")
    };
    let ow = OracleSink::new(ow_cfg.clone()).await.unwrap();
    assert!(ow.is_overwrite());
    ow.begin_overwrite().await.unwrap();
    ow.write_batch(&[json!({"ID": 1}), json!({"ID": 2})])
        .await
        .unwrap();
    ow.commit_overwrite().await.unwrap();
    assert_eq!(count(&conn, "SNAP").await, 2);
    let ow = OracleSink::new(ow_cfg.clone()).await.unwrap();
    ow.begin_overwrite().await.unwrap();
    ow.write_batch(&[json!({"ID": 9})]).await.unwrap();
    assert_eq!(
        count(&conn, "SNAP").await,
        2,
        "target untouched until commit"
    );
    ow.commit_overwrite().await.unwrap();
    let ids = common::query_strings(&conn, "SELECT TO_CHAR(ID) FROM SNAP").await;
    assert_eq!(ids, vec![Some("9".into())]);
    let ow = OracleSink::new(ow_cfg.clone()).await.unwrap();
    ow.begin_overwrite().await.unwrap();
    ow.write_batch(&[json!({"ID": 5})]).await.unwrap();
    ow.abort_overwrite().await.unwrap();
    assert_eq!(
        count(&conn, "SNAP").await,
        1,
        "abort leaves the target intact"
    );
    let missing = OracleSink::new(OracleSinkConfig {
        create_table: false,
        ..OracleSinkConfig {
            table: "NOPE_OW".into(),
            ..ow_cfg.clone()
        }
    })
    .await
    .unwrap();
    assert!(missing.begin_overwrite().await.is_err());
    let empty_first = OracleSink::new(OracleSinkConfig {
        table: "EMPTY_OW".into(),
        ..ow_cfg
    })
    .await
    .unwrap();
    empty_first.begin_overwrite().await.unwrap();
    empty_first.commit_overwrite().await.unwrap();

    // Schema drift: live schema, add / widen / relax.
    common::exec(
        &conn,
        &["CREATE TABLE DRIFT (ID NUMBER(19) NOT NULL, QTY NUMBER(19) NOT NULL)"],
    )
    .await;
    let drift = OracleSink::new(OracleSinkConfig::new(conn.clone(), "DRIFT"))
        .await
        .unwrap();
    assert!(drift.supports_schema_evolution());
    let schema = drift.current_schema().await.unwrap().unwrap();
    assert_eq!(schema["properties"]["QTY"]["type"], "integer");
    drift
        .evolve_schema(&SchemaEvolution {
            additions: vec![ColumnChange {
                name: "NOTE".into(),
                from: None,
                to: json!({"type": "string"}),
            }],
            widenings: vec![ColumnChange {
                name: "QTY".into(),
                from: Some(json!({"type": "integer"})),
                to: json!({"type": "number"}),
            }],
            relax_nullability: vec!["QTY".into()],
        })
        .await
        .unwrap();
    drift
        .evolve_schema(&SchemaEvolution {
            additions: vec![ColumnChange {
                name: "NOTE".into(),
                from: None,
                to: json!({"type": "string"}),
            }],
            widenings: vec![],
            relax_nullability: vec!["QTY".into()],
        })
        .await
        .expect("evolution is idempotent");
    let schema = drift.current_schema().await.unwrap().unwrap();
    assert_eq!(
        schema["properties"]["QTY"]["type"],
        json!(["number", "null"])
    );
    assert_eq!(
        schema["properties"]["NOTE"]["type"],
        json!(["string", "null"])
    );
    drift
        .write_batch(&[json!({"ID": 1, "QTY": 1.25, "NOTE": "n"})])
        .await
        .unwrap();
    let absent = OracleSink::new(OracleSinkConfig::new(conn.clone(), "NOT_THERE"))
        .await
        .unwrap();
    assert_eq!(absent.current_schema().await.unwrap(), None);

    // Upper-cased identifiers + json_column mode.
    let json_cfg = OracleSinkConfig {
        identifier_case: IdentifierCase::Upper,
        column_mapping: OracleColumnMapping::JsonColumn {
            column: "payload".into(),
        },
        ..OracleSinkConfig::new(conn.clone(), "raw_events")
    };
    let js = OracleSink::new(json_cfg).await.unwrap();
    js.write_batch(&[json!({"a": 1}), json!({"b": [true]})])
        .await
        .unwrap();
    let docs = common::query_strings(&conn, "SELECT PAYLOAD FROM RAW_EVENTS ORDER BY ID").await;
    assert_eq!(
        docs,
        vec![Some("{\"a\":1}".into()), Some("{\"b\":[true]}".into())]
    );
    let upper = OracleSink::new(OracleSinkConfig {
        identifier_case: IdentifierCase::Upper,
        column_mapping: OracleColumnMapping::AutoColumns {
            on_unknown_field: OnUnknownField::Error,
        },
        ..OracleSinkConfig::new(conn.clone(), "narrow")
    })
    .await
    .unwrap();
    upper
        .write_batch(&[json!({"id": 10, "code": "u"})])
        .await
        .unwrap();
    assert!(
        upper
            .write_batch(&[json!({"id": 11, "zzz": 1})])
            .await
            .is_err()
    );

    // create_table: false fails with a typed error naming the fix.
    let strict = OracleSink::new(OracleSinkConfig {
        create_table: false,
        ..OracleSinkConfig::new(conn.clone(), "ABSENT")
    })
    .await
    .unwrap();
    let err = strict.write_batch(&[json!({"ID": 1})]).await.unwrap_err();
    assert!(err.to_string().contains("create_table"), "{err}");
    let report = strict
        .check(&faucet_core::check::CheckContext::default())
        .await
        .unwrap();
    assert_eq!(report.failed_count(), 1, "{report:?}");
    let report = sink
        .check(&faucet_core::check::CheckContext::default())
        .await
        .unwrap();
    assert_eq!(report.failed_count(), 0, "{report:?}");
}
