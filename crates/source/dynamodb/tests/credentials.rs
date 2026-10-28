//! Every DynamoDB credential source (#967) against LocalStack (DynamoDB +
//! STS): `profile` (a temporary shared config/credentials file),
//! `assume_role` and `web_identity` (STS on LocalStack via
//! `AWS_ENDPOINT_URL_STS`). One test, because each phase rewrites the process
//! environment the default credential chain reads.
//!
//! LocalStack scopes tables by the account an access key names (a 12-digit
//! key is that account; an STS session belongs to the role's account), so each
//! table lives in its own account and only the credentials the variant must
//! produce can scan it.
#![allow(clippy::disallowed_methods)] // tests drive time and raw clients directly

mod common;

use aws_sdk_dynamodb::types::ScalarAttributeType;
use common::{create_table, item, n, put_items, s};
use faucet_core::Source;
use faucet_source_dynamodb::{DynamoDbCredentials, DynamoDbSource};
use serde_json::{Value, json};
use testcontainers::ImageExt;
use testcontainers_modules::localstack::LocalStack;

const REGION: &str = "us-east-1";
const ENV_KEYS: [&str; 2] = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"];

fn set(vars: &[(&str, &str)]) {
    // SAFETY: this binary has one test; nothing reads the environment concurrently.
    unsafe {
        for (k, v) in vars {
            std::env::set_var(k, v);
        }
    }
}

fn unset(vars: &[&str]) {
    // SAFETY: as in `set`.
    unsafe {
        for k in vars {
            std::env::remove_var(k);
        }
    }
}

fn role(account: &str) -> String {
    format!("arn:aws:iam::{account}:role/faucet-reader")
}

async fn seed(endpoint: &str, account: &str) {
    let admin = faucet_source_dynamodb::build_client(
        Some(REGION),
        Some(endpoint),
        &DynamoDbCredentials::AccessKey {
            access_key_id: account.into(),
            secret_access_key: "test".into(),
            session_token: None,
        },
    )
    .await
    .unwrap();
    for _ in 0..240 {
        if admin.list_tables().send().await.is_ok() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    create_table(&admin, "people", ScalarAttributeType::S, false, false).await;
    put_items(
        &admin,
        "people",
        vec![item(vec![("pk", s(account)), ("v", n(1))])],
    )
    .await;
}

async fn scan(endpoint: &str, credentials: DynamoDbCredentials) -> Result<Vec<Value>, String> {
    let mut cfg = common::config(endpoint, "people");
    cfg.credentials = credentials;
    let source = DynamoDbSource::new(cfg).await.map_err(|e| e.to_string())?;
    source.fetch_all().await.map_err(|e| e.to_string())
}

#[tokio::test(flavor = "multi_thread")]
async fn profile_assume_role_and_web_identity_scan_their_accounts_table() {
    let container = faucet_conformance::containers::start(|| {
        LocalStack::default().with_env_var("SERVICES", "dynamodb,sts")
    })
    .await;
    let port = container.get_host_port_ipv4(4566).await.unwrap();
    let endpoint = format!("http://127.0.0.1:{port}");
    set(&[
        ("AWS_ENDPOINT_URL_STS", endpoint.as_str()),
        ("AWS_EC2_METADATA_DISABLED", "true"),
        ("AWS_REGION", REGION),
    ]);
    for account in ["111111111111", "222222222222", "333333333333"] {
        seed(&endpoint, account).await;
    }
    let only = |account: &str| vec![json!({"pk": account, "v": 1})];
    let dir = tempfile::tempdir().unwrap();

    unset(&ENV_KEYS);
    let config = dir.path().join("config");
    let credentials = dir.path().join("credentials");
    std::fs::write(
        &config,
        format!("[profile faucet-test]\nregion = {REGION}\n"),
    )
    .unwrap();
    std::fs::write(
        &credentials,
        "[faucet-test]\naws_access_key_id = 111111111111\naws_secret_access_key = s\n",
    )
    .unwrap();
    set(&[
        ("AWS_CONFIG_FILE", config.to_str().unwrap()),
        ("AWS_SHARED_CREDENTIALS_FILE", credentials.to_str().unwrap()),
    ]);
    let profile = DynamoDbCredentials::Profile {
        name: "faucet-test".into(),
    };
    assert_eq!(
        scan(&endpoint, profile).await.unwrap(),
        only("111111111111")
    );

    let none = dir.path().join("absent");
    set(&[
        ("AWS_CONFIG_FILE", none.to_str().unwrap()),
        ("AWS_SHARED_CREDENTIALS_FILE", none.to_str().unwrap()),
        ("AWS_ACCESS_KEY_ID", "222222222222"),
        ("AWS_SECRET_ACCESS_KEY", "test"),
    ]);
    // A role in an account with no `people` table: the scan must fail, so the
    // read below cannot be the base credentials answering.
    assert!(
        scan(
            &endpoint,
            DynamoDbCredentials::AssumeRole {
                role_arn: role("444444444444"),
                session_name: None,
                external_id: None,
            }
        )
        .await
        .is_err()
    );
    set(&[("AWS_ACCESS_KEY_ID", "000000000000")]);
    let assume = DynamoDbCredentials::AssumeRole {
        role_arn: role("222222222222"),
        session_name: Some("faucet-it".into()),
        external_id: Some("ext-1".into()),
    };
    assert_eq!(scan(&endpoint, assume).await.unwrap(), only("222222222222"));

    unset(&ENV_KEYS);
    let token = dir.path().join("token");
    std::fs::write(&token, "eyJhbGciOiJub25lIn0.eyJzdWIiOiJmYXVjZXQifQ.").unwrap();
    set(&[
        ("AWS_WEB_IDENTITY_TOKEN_FILE", token.to_str().unwrap()),
        ("AWS_ROLE_ARN", role("333333333333").as_str()),
        ("AWS_ROLE_SESSION_NAME", "faucet-web"),
    ]);
    assert_eq!(
        scan(&endpoint, DynamoDbCredentials::WebIdentity)
            .await
            .unwrap(),
        only("333333333333")
    );
}
