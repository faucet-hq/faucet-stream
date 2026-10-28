//! `credentials: { type: iam }` end to end: temporary credentials from a fake
//! of `redshift:GetClusterCredentials` (Query protocol) or serverless
//! `GetCredentials` (JSON 1.1) log in to a real PostgreSQL container, which
//! holds the role the fake hands out. Refresh is checked by expiring the
//! first password and changing the role's password: a connection opened after
//! the refresh logs in only because it got the new one.
#![allow(clippy::disallowed_methods)] // tests pace time directly

mod support;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use faucet_common_redshift::{RedshiftConnection, connect_lazy};
use faucet_core::FaucetError;
use faucet_core::io::CallContext;
use serde_json::json;
use support::{ACCESS_KEY, REGION, access_key, admin_url, credentials_response, form};
use wiremock::{MockServer, Request, Respond, ResponseTemplate};

/// One request: its form fields and `Authorization` header.
type Seen = (std::collections::HashMap<String, String>, String);

/// Hands out `passwords` in turn, each valid for `lifetime`, and keeps every
/// request's form fields and `Authorization` header.
#[derive(Clone)]
struct ClusterCredentials {
    user: &'static str,
    passwords: Vec<&'static str>,
    lifetime: Duration,
    calls: Arc<AtomicUsize>,
    seen: Arc<Mutex<Vec<Seen>>>,
}

impl Respond for ClusterCredentials {
    fn respond(&self, req: &Request) -> ResponseTemplate {
        let n = self.calls.fetch_add(1, Ordering::SeqCst);
        let auth = req
            .headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_string();
        self.seen.lock().unwrap().push((form(req), auth));
        let pw = self.passwords[n.min(self.passwords.len() - 1)];
        credentials_response(self.user, pw, self.lifetime)
    }
}

fn iam_connection(port: u16, config: serde_json::Value) -> RedshiftConnection {
    let mut conn: RedshiftConnection = serde_json::from_value(json!({
        "host": "127.0.0.1",
        "port": port,
        "database": "postgres",
        "user": "fallback",
        "tls": false,
        "credentials": {"type": "iam", "config": config}
    }))
    .unwrap();
    conn.port = port;
    conn
}

async fn current_user(pool: &sqlx::PgPool) -> String {
    sqlx::query_scalar("SELECT current_user::text")
        .fetch_one(pool)
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn cluster_credentials_log_in_and_are_refreshed_before_they_expire() {
    let (_pg, port) = support::start_postgres().await;
    let admin = sqlx::PgPool::connect(&admin_url(port)).await.unwrap();
    sqlx::query(r#"CREATE ROLE "IAM:analyst" LOGIN PASSWORD 'pw-1'"#)
        .execute(&admin)
        .await
        .unwrap();
    let server = MockServer::start().await;
    let fake = ClusterCredentials {
        user: "IAM:analyst",
        passwords: vec!["pw-1", "pw-2"],
        lifetime: Duration::from_secs(4),
        calls: Arc::default(),
        seen: Arc::default(),
    };
    wiremock::Mock::given(wiremock::matchers::method("POST"))
        .respond_with(fake.clone())
        .mount(&server)
        .await;
    let conn = iam_connection(
        port,
        json!({
            "region": REGION,
            "cluster_identifier": "faucet-cluster",
            "db_user": "analyst",
            "auto_create": true,
            "db_groups": ["loaders", "readers"],
            "duration_secs": 900,
            "aws_credentials": access_key(),
            "endpoint_url": server.uri(),
        }),
    );
    let pool = connect_lazy(&conn, 4).unwrap();
    let ctx = CallContext::new();

    let first = pool.get(&ctx).await.unwrap();
    assert_eq!(current_user(&first).await, "IAM:analyst");
    let held = first.acquire().await.unwrap();
    pool.get(&ctx).await.unwrap();
    assert_eq!(
        fake.calls.load(Ordering::SeqCst),
        1,
        "still valid: no refresh"
    );

    let (fields, auth) = fake.seen.lock().unwrap()[0].clone();
    for (k, v) in [
        ("Action", "GetClusterCredentials"),
        ("Version", "2012-12-01"),
        ("ClusterIdentifier", "faucet-cluster"),
        ("DbUser", "analyst"),
        ("DbName", "postgres"),
        ("AutoCreate", "true"),
        ("DbGroups.DbGroup.1", "loaders"),
        ("DbGroups.DbGroup.2", "readers"),
        ("DurationSeconds", "900"),
    ] {
        assert_eq!(
            fields.get(k).map(String::as_str),
            Some(v),
            "{k}: {fields:?}"
        );
    }
    assert!(
        auth.contains(&format!("Credential={ACCESS_KEY}/"))
            && auth.contains(&format!("/{REGION}/redshift/aws4_request")),
        "{auth}"
    );

    // The first password stops working; only a refreshed pool can log in.
    sqlx::query(r#"ALTER ROLE "IAM:analyst" PASSWORD 'pw-2'"#)
        .execute(&admin)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(2500)).await;
    let refreshed = pool.get(&ctx).await.unwrap();
    assert_eq!(fake.calls.load(Ordering::SeqCst), 2);
    let second = refreshed.acquire().await.expect("a new connection logs in");
    drop(second);
    drop(held);

    let stale = sqlx::PgPool::connect(&format!(
        "postgres://IAM%3Aanalyst:pw-1@127.0.0.1:{port}/postgres"
    ))
    .await;
    assert!(stale.is_err(), "the old password is no longer accepted");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_serverless_workgroup_logs_in_with_get_credentials() {
    let (_pg, port) = support::start_postgres().await;
    let admin = sqlx::PgPool::connect(&admin_url(port)).await.unwrap();
    sqlx::query(r#"CREATE ROLE "IAMR:faucet-role" LOGIN PASSWORD 'wg-pw'"#)
        .execute(&admin)
        .await
        .unwrap();
    let server = MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::header(
        "x-amz-target",
        "RedshiftServerless.GetCredentials",
    ))
    .and(wiremock::matchers::body_json(json!({
        "workgroupName": "faucet-wg",
        "dbName": "postgres",
        "durationSeconds": 1800
    })))
    .respond_with(support::serverless_response(
        "IAMR:faucet-role",
        "wg-pw",
        Duration::from_secs(1800),
    ))
    .expect(1)
    .mount(&server)
    .await;
    let conn = iam_connection(
        port,
        json!({
            "region": REGION,
            "workgroup_name": "faucet-wg",
            "duration_secs": 1800,
            "aws_credentials": access_key(),
            "endpoint_url": server.uri(),
        }),
    );
    let pool = connect_lazy(&conn, 1).unwrap();
    let ctx = CallContext::new();
    assert_eq!(
        current_user(&pool.get(&ctx).await.unwrap()).await,
        "IAMR:faucet-role"
    );
    pool.get(&ctx).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_refused_credentials_call_is_an_auth_error() {
    let server = MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("POST"))
        .respond_with(
            ResponseTemplate::new(403)
                .insert_header("content-type", "text/xml")
                .set_body_string(
                    "<ErrorResponse><Error><Type>Sender</Type><Code>AccessDenied</Code>\
                     <Message>not authorized to perform redshift:GetClusterCredentials</Message>\
                     </Error><RequestId>r</RequestId></ErrorResponse>",
                ),
        )
        .mount(&server)
        .await;
    let conn = iam_connection(
        1,
        json!({
            "region": REGION,
            "cluster_identifier": "c",
            "aws_credentials": access_key(),
            "endpoint_url": server.uri(),
        }),
    );
    let pool = connect_lazy(&conn, 1).unwrap();
    match pool.get(&CallContext::new()).await {
        Err(FaucetError::Auth(m)) => {
            assert!(m.contains("GetClusterCredentials failed"), "{m}");
            assert!(m.contains("not authorized"), "{m}");
        }
        other => panic!("expected an auth error, got {other:?}"),
    }
}
