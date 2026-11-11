//! The `profile`, `assume_role` and `web_identity` credential sources against
//! LocalStack (S3 + STS), each reading a real object through the source. One
//! test, because each phase rewrites the process environment the default
//! credential chain reads.
#![allow(unsafe_code)] // tests set process environment variables

use std::collections::HashMap;
use std::time::Duration;

use aws_config::BehaviorVersion;
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::{Client, Config as S3Config};
use faucet_conformance::containers::{self, ReadyProbe, StartOptions};
use faucet_core::Source;
use faucet_source_s3::{S3Credentials, S3Source, S3SourceConfig};
use serde_json::{Value, json};
use testcontainers::ImageExt;
use testcontainers_modules::localstack::LocalStack;

const REGION: &str = "us-east-1";
const ENV_KEYS: [&str; 2] = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"];
const PHASE_TIMEOUT: Duration = Duration::from_secs(60);

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

async fn phase<T>(what: &str, fut: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(PHASE_TIMEOUT, fut)
        .await
        .unwrap_or_else(|_| panic!("{what} did not finish within {PHASE_TIMEOUT:?}"))
}

/// A client for `account` (LocalStack scopes resources by the 12-digit key).
async fn admin(endpoint: &str, account: &str) -> Client {
    let sdk_config = aws_config::defaults(BehaviorVersion::latest())
        .region(aws_config::Region::new(REGION))
        .endpoint_url(endpoint)
        .credentials_provider(Credentials::new(account, "test", None, None, "test"))
        .load()
        .await;
    Client::from_conf(
        S3Config::from(&sdk_config)
            .to_builder()
            .force_path_style(true)
            .build(),
    )
}

async fn seed(endpoint: &str, account: &str, bucket: &str) {
    let client = admin(endpoint, account).await;
    client
        .create_bucket()
        .bucket(bucket)
        .send()
        .await
        .unwrap_or_else(|e| panic!("create {bucket}: {e:?}"));
    client
        .put_object()
        .bucket(bucket)
        .key("data/a.jsonl")
        .body(ByteStream::from(
            format!("{{\"bucket\":\"{bucket}\"}}\n").into_bytes(),
        ))
        .send()
        .await
        .unwrap_or_else(|e| panic!("put into {bucket}: {e:?}"));
}

async fn read(endpoint: &str, bucket: &str, credentials: S3Credentials) -> Vec<Value> {
    let label = format!("{credentials:?}");
    let config = S3SourceConfig::new(bucket)
        .prefix("data/")
        .region(REGION)
        .endpoint_url(endpoint)
        .credentials(credentials);
    let source = S3Source::new(config)
        .await
        .unwrap_or_else(|e| panic!("{label}: {e}"));
    source
        .fetch_with_context(&HashMap::new())
        .await
        .unwrap_or_else(|e| panic!("{label}: {e}"))
}

#[tokio::test(flavor = "multi_thread")]
async fn profile_assume_role_and_web_identity_read_objects() {
    let opts = StartOptions::default().ready(ReadyProbe::http(4566, "/_localstack/health"));
    let container = containers::start_with(
        || LocalStack::default().with_env_var("SERVICES", "s3,sts"),
        &opts,
    )
    .await;
    let port = container
        .get_host_port_ipv4(4566)
        .await
        .expect("localstack port");
    let endpoint = format!("http://127.0.0.1:{port}");
    set(&[
        ("AWS_ENDPOINT_URL_STS", endpoint.as_str()),
        ("AWS_EC2_METADATA_DISABLED", "true"),
        ("AWS_REGION", REGION),
    ]);
    for (account, bucket) in [
        ("111111111111", "profile-data"),
        ("222222222222", "role-data"),
        ("333333333333", "web-data"),
    ] {
        phase("seeding", seed(&endpoint, account, bucket)).await;
    }
    let dir = tempfile::tempdir().unwrap();

    // profile: the named profile in the shared files, not the environment.
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
        "[faucet-test]\naws_access_key_id = 111111111111\naws_secret_access_key = profilesecret\n",
    )
    .unwrap();
    set(&[
        ("AWS_CONFIG_FILE", config.to_str().unwrap()),
        ("AWS_SHARED_CREDENTIALS_FILE", credentials.to_str().unwrap()),
    ]);
    let got = phase(
        "the profile read",
        read(
            &endpoint,
            "profile-data",
            S3Credentials::Profile {
                name: "faucet-test".into(),
            },
        ),
    )
    .await;
    assert_eq!(got, vec![json!({"bucket": "profile-data"})]);
    let none = dir.path().join("absent");
    set(&[
        ("AWS_CONFIG_FILE", none.to_str().unwrap()),
        ("AWS_SHARED_CREDENTIALS_FILE", none.to_str().unwrap()),
    ]);

    // assume_role: STS trades the base credentials for a role session.
    set(&[
        ("AWS_ACCESS_KEY_ID", "000000000000"),
        ("AWS_SECRET_ACCESS_KEY", "test"),
    ]);
    let got = phase(
        "the assume_role read",
        read(
            &endpoint,
            "role-data",
            S3Credentials::AssumeRole {
                role_arn: role("222222222222"),
                session_name: Some("faucet-it".into()),
                external_id: Some("ext-1".into()),
            },
        ),
    )
    .await;
    assert_eq!(got, vec![json!({"bucket": "role-data"})]);

    // web_identity: the default chain exchanges the token file through STS.
    unset(&ENV_KEYS);
    let token = dir.path().join("token");
    std::fs::write(&token, "eyJhbGciOiJub25lIn0.eyJzdWIiOiJmYXVjZXQifQ.").unwrap();
    set(&[
        ("AWS_WEB_IDENTITY_TOKEN_FILE", token.to_str().unwrap()),
        ("AWS_ROLE_ARN", role("333333333333").as_str()),
        ("AWS_ROLE_SESSION_NAME", "faucet-web"),
    ]);
    let got = phase(
        "the web_identity read",
        read(&endpoint, "web-data", S3Credentials::WebIdentity),
    )
    .await;
    assert_eq!(got, vec![json!({"bucket": "web-data"})]);
}
