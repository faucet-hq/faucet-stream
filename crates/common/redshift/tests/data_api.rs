//! The Redshift Data API client against a stateful contract fake that runs
//! each statement on a real PostgreSQL container (`support::DataApiFake`):
//! the request bodies, the SUBMITTED → STARTED → FINISHED / FAILED flow,
//! result paging, round-trip metering, and a run deadline cancelling the
//! statement on the service.
#![allow(clippy::disallowed_methods)]

mod support;

use std::sync::Arc;
use std::time::Duration;

use faucet_common_redshift::data_api::{Effect, Statement, named_parameters};
use faucet_common_redshift::{DataApiClient, RedshiftConnection};
use faucet_core::FaucetError;
use faucet_core::io::CallContext;
use serde_json::{Value, json};
use support::{DataApiFake, REGION, access_key, admin_url};
use wiremock::MockServer;

fn connection(server: &MockServer, target: Value) -> RedshiftConnection {
    let mut config = target;
    config["region"] = json!(REGION);
    config["aws_credentials"] = access_key();
    config["endpoint_url"] = json!(server.uri());
    serde_json::from_value(json!({
        "database": "postgres",
        "credentials": {"type": "redshift_data_api", "config": config}
    }))
    .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn statements_run_poll_page_and_meter_their_calls() {
    let (_pg, port) = support::start_postgres().await;
    let server = MockServer::start().await;
    let fake = DataApiFake::new(admin_url(port), 2);
    fake.mount(&server).await;
    let api = DataApiClient::from_connection(
        &connection(
            &server,
            json!({"cluster_identifier": "c1", "db_user": "loader"}),
        ),
        FaucetError::Source,
    )
    .unwrap()
    .unwrap();
    let meter = Arc::new(faucet_core::usage::UsageMeter::new());
    let recorder = faucet_core::observability::RoundtripRecorder::new(
        faucet_core::observability::RoundtripSide::Source,
        "p",
        "r",
        "redshift",
    )
    .with_meter(meter.clone());
    let ctx = CallContext::new().with_recorder(Arc::new(recorder));

    let ddl = api
        .execute(
            &ctx,
            Effect::Write,
            &Statement::new("CREATE TABLE t (id INT, name TEXT)"),
        )
        .await
        .unwrap();
    assert!(!ddl.has_result_set);
    api.execute(
        &ctx,
        Effect::Write,
        &named_parameters(
            "INSERT INTO t VALUES ($1, $2), ($3, $4), ($5, $6)",
            &[
                json!(1),
                json!("a"),
                json!(2),
                json!(""),
                json!(3),
                json!(null),
            ],
        ),
    )
    .await
    .unwrap();
    let (columns, rows) = api
        .query(
            &ctx,
            &named_parameters("SELECT * FROM t WHERE id >= $1 ORDER BY id", &[json!(1)]),
        )
        .await
        .unwrap();
    assert_eq!(
        columns
            .iter()
            .map(|c| (c.name.as_str(), c.type_name.as_str()))
            .collect::<Vec<_>>(),
        [("id", "int4"), ("name", "text")]
    );
    let rows: Vec<Value> = rows.into_iter().map(Value::Object).collect();
    assert_eq!(
        rows,
        [
            json!({"id": 1, "name": "a"}),
            json!({"id": 2, "name": ""}),
            json!({"id": 3, "name": null})
        ]
    );

    let bodies: Vec<Value> = fake.submitted().into_iter().map(|s| s.body).collect();
    assert_eq!(bodies[0]["ClusterIdentifier"], "c1");
    assert_eq!(bodies[0]["DbUser"], "loader");
    assert_eq!(bodies[0]["Database"], "postgres");
    assert!(bodies[0].get("Parameters").is_none());
    assert_eq!(
        bodies[1]["Sql"],
        "INSERT INTO t VALUES (:p1, :p2), (:p3, ''), (:p5, NULL)"
    );
    assert_eq!(
        bodies[1]["Parameters"],
        json!([{"name": "p1", "value": "1"}, {"name": "p2", "value": "a"}, {"name": "p3", "value": "2"}, {"name": "p5", "value": "3"}])
    );
    let targets = fake.targets();
    let count = |t: &str| targets.iter().filter(|x| x.as_str() == t).count();
    assert_eq!(count("RedshiftData.ExecuteStatement"), 3);
    // Each statement is polled through SUBMITTED, STARTED and FINISHED.
    assert_eq!(count("RedshiftData.DescribeStatement"), 9);
    assert_eq!(
        count("RedshiftData.GetStatementResult"),
        2,
        "three rows, two per page"
    );
    let snap = meter.snapshot();
    assert_eq!(snap.source_roundtrips.get("submit"), Some(&3));
    assert_eq!(snap.source_roundtrips.get("poll"), Some(&9));
    assert_eq!(snap.source_roundtrips.get("fetch"), Some(&2));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_statement_carries_the_server_error() {
    let (_pg, port) = support::start_postgres().await;
    let server = MockServer::start().await;
    let fake = DataApiFake::new(admin_url(port), 100);
    fake.mount(&server).await;
    let api = DataApiClient::from_connection(
        &connection(
            &server,
            json!({"workgroup_name": "wg", "secret_arn": "arn:aws:secretsmanager:us-east-1:1:secret:s"}),
        ),
        FaucetError::Sink,
    )
    .unwrap()
    .unwrap();
    let err = api
        .execute(
            &CallContext::new(),
            Effect::Write,
            &Statement::new("INSERT INTO missing VALUES (1)"),
        )
        .await
        .unwrap_err();
    match err {
        FaucetError::Sink(m) => {
            assert!(m.contains("statement failed"), "{m}");
            assert!(m.contains("\"missing\" does not exist"), "{m}");
        }
        other => panic!("{other:?}"),
    }
    let body = &fake.submitted()[0].body;
    assert_eq!(body["WorkgroupName"], "wg");
    assert_eq!(
        body["SecretArn"],
        "arn:aws:secretsmanager:us-east-1:1:secret:s"
    );
    assert!(body.get("DbUser").is_none() && body.get("ClusterIdentifier").is_none());

    let aborted = api
        .execute(
            &CallContext::new(),
            Effect::Read,
            &Statement::new("SELECT 1 /* abort */"),
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(aborted.contains("statement was aborted"), "{aborted}");

    // A request the service rejects outright.
    let bad = api
        .execute(
            &CallContext::new(),
            Effect::Read,
            &Statement {
                sql: "SELECT :p1".into(),
                parameters: vec![("p1".into(), String::new())],
            },
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(bad.contains("ExecuteStatement failed"), "{bad}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_deadline_ends_the_poll_and_cancels_the_statement() {
    let (_pg, port) = support::start_postgres().await;
    let server = MockServer::start().await;
    let fake = DataApiFake::new(admin_url(port), 100).stalled();
    fake.mount(&server).await;
    let api = DataApiClient::from_connection(
        &connection(&server, json!({"workgroup_name": "wg"})),
        FaucetError::Source,
    )
    .unwrap()
    .unwrap();
    let ctx =
        CallContext::new().with_deadline(tokio::time::Instant::now() + Duration::from_millis(600));
    let err = api
        .execute(&ctx, Effect::Read, &Statement::new("SELECT 1"))
        .await
        .unwrap_err();
    assert!(matches!(err, FaucetError::Timeout { .. }), "{err:?}");
    for _ in 0..50 {
        if !fake.cancelled().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(fake.cancelled(), ["stmt-1"]);

    let cancel = faucet_core::CancellationToken::new();
    cancel.cancel();
    let err = api
        .execute(
            &CallContext::new().with_cancel(cancel),
            Effect::Write,
            &Statement::new("SELECT 1"),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, FaucetError::Cancelled { .. }), "{err:?}");
}
