//! Every SQS credential source (#967) against LocalStack (SQS + STS):
//! `profile` (a temporary shared config/credentials file), `assume_role` and
//! `web_identity` (STS on LocalStack via `AWS_ENDPOINT_URL_STS`). One test,
//! because each phase rewrites the process environment the default credential
//! chain reads.
//!
//! LocalStack's SQS resolves a queue by its URL, not the caller's account, so
//! each phase leaves only the variant's own credential source in place (no
//! environment keys or shared files besides it) and the STS exchanges are read
//! back from LocalStack's request log.
#![allow(clippy::disallowed_methods)] // tests drive time and raw clients directly

use faucet_core::Source;
use faucet_source_sqs::{SqsCredentials, SqsSource, SqsSourceConfig};
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

fn key(account: &str) -> SqsCredentials {
    SqsCredentials::AccessKey {
        access_key_id: account.into(),
        secret_access_key: "test".into(),
        session_token: None,
    }
}

/// Create `name` in `account` with two messages; returns its URL.
async fn seed(endpoint: &str, account: &str, name: &str) -> String {
    let admin = faucet_source_sqs::build_client(Some(REGION), Some(endpoint), &key(account))
        .await
        .unwrap();
    let mut url = None;
    for _ in 0..120 {
        if let Ok(out) = admin.create_queue().queue_name(name).send().await {
            url = out.queue_url().map(str::to_string);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    let url = url.expect("queue created");
    for n in 0..2 {
        admin
            .send_message()
            .queue_url(&url)
            .message_body(json!({"account": account, "n": n}).to_string())
            .send()
            .await
            .unwrap();
    }
    url
}

async fn read(
    endpoint: &str,
    url: &str,
    credentials: SqsCredentials,
) -> Result<Vec<Value>, String> {
    let mut cfg = SqsSourceConfig::new(url);
    cfg.region = Some(REGION.into());
    cfg.endpoint_url = Some(endpoint.into());
    cfg.credentials = credentials;
    cfg.wait_time_seconds = 1;
    cfg.idle_timeout_secs = Some(3);
    let source = SqsSource::new(cfg).await.map_err(|e| e.to_string())?;
    let mut records = source.fetch_all().await.map_err(|e| e.to_string())?;
    records.sort_by_key(|r| r["n"].as_i64());
    Ok(records)
}

fn expected(account: &str) -> Vec<Value> {
    (0..2)
        .map(|n| json!({"account": account, "n": n}))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn profile_assume_role_and_web_identity_read_the_queue_of_their_account() {
    let container = faucet_conformance::containers::start(|| {
        LocalStack::default().with_env_var("SERVICES", "sqs,sts")
    })
    .await;
    let port = container.get_host_port_ipv4(4566).await.unwrap();
    let endpoint = format!("http://127.0.0.1:{port}");
    set(&[
        ("AWS_ENDPOINT_URL_STS", endpoint.as_str()),
        ("AWS_EC2_METADATA_DISABLED", "true"),
        ("AWS_REGION", REGION),
    ]);
    let profile_q = seed(&endpoint, "111111111111", "faucet-profile").await;
    let role_q = seed(&endpoint, "222222222222", "faucet-role").await;
    let web_q = seed(&endpoint, "333333333333", "faucet-web").await;
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
    let profile = SqsCredentials::Profile {
        name: "faucet-test".into(),
    };
    assert_eq!(
        read(&endpoint, &profile_q, profile).await.unwrap(),
        expected("111111111111")
    );

    let none = dir.path().join("absent");
    set(&[
        ("AWS_CONFIG_FILE", none.to_str().unwrap()),
        ("AWS_SHARED_CREDENTIALS_FILE", none.to_str().unwrap()),
        ("AWS_ACCESS_KEY_ID", "000000000000"),
        ("AWS_SECRET_ACCESS_KEY", "test"),
    ]);
    let assume = SqsCredentials::AssumeRole {
        role_arn: role("222222222222"),
        session_name: Some("faucet-it".into()),
        external_id: None,
    };
    assert_eq!(
        read(&endpoint, &role_q, assume).await.unwrap(),
        expected("222222222222")
    );

    unset(&ENV_KEYS);
    let token = dir.path().join("token");
    std::fs::write(&token, "eyJhbGciOiJub25lIn0.eyJzdWIiOiJmYXVjZXQifQ.").unwrap();
    set(&[
        ("AWS_WEB_IDENTITY_TOKEN_FILE", token.to_str().unwrap()),
        ("AWS_ROLE_ARN", role("333333333333").as_str()),
        ("AWS_ROLE_SESSION_NAME", "faucet-web"),
    ]);
    assert_eq!(
        read(&endpoint, &web_q, SqsCredentials::WebIdentity)
            .await
            .unwrap(),
        expected("333333333333")
    );

    let log = String::from_utf8_lossy(&container.stdout_to_vec().await.unwrap()).into_owned();
    assert!(
        log.contains("AWS sts.AssumeRole => 200"),
        "assume_role used STS"
    );
    assert!(
        log.contains("AWS sts.AssumeRoleWithWebIdentity => 200"),
        "web_identity used STS"
    );
}
