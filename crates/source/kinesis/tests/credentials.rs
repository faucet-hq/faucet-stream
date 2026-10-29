//! Every Kinesis credential source and value format (#967) against LocalStack
//! (Kinesis + STS): `profile` (a temporary shared config/credentials file),
//! `assume_role` and `web_identity` (STS on LocalStack via
//! `AWS_ENDPOINT_URL_STS`), each reading a real stream, with `string` and
//! `bytes` payload decoding. One test, because each phase rewrites the
//! process environment the default credential chain reads.
//!
//! LocalStack scopes resources by the account an access key names (a
//! 12-digit key is that account; STS sessions belong to the role's account),
//! so each stream is created in its own account and only the credentials the
//! variant must produce can read it.
#![allow(clippy::disallowed_methods)] // tests drive time and raw clients directly

use aws_sdk_kinesis::primitives::Blob;
use base64::Engine as _;
use faucet_core::Source;
use faucet_source_kinesis::{KinesisCredentials, KinesisSource, KinesisSourceConfig, ValueFormat};
use serde_json::Value;
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

/// Bounds one phase so a stalled emulator fails the test with its name instead of hanging CI.
async fn phase<T>(name: &str, fut: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(std::time::Duration::from_secs(180), fut)
        .await
        .unwrap_or_else(|_| panic!("{name} did not finish within 180s"))
}

fn role(account: &str) -> String {
    format!("arn:aws:iam::{account}:role/faucet-reader")
}

async fn seed(endpoint: &str, account: &str, stream: &str, payloads: &[&[u8]]) {
    let admin = faucet_source_kinesis::build_client(
        Some(REGION),
        Some(endpoint),
        &KinesisCredentials::AccessKey {
            access_key_id: account.into(),
            secret_access_key: "test".into(),
            session_token: None,
        },
    )
    .await
    .unwrap();
    for _ in 0..120 {
        if admin.list_streams().send().await.is_ok() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    admin
        .create_stream()
        .stream_name(stream)
        .shard_count(1)
        .send()
        .await
        .unwrap();
    for _ in 0..60 {
        let d = admin
            .describe_stream_summary()
            .stream_name(stream)
            .send()
            .await
            .unwrap();
        if d.stream_description_summary().map(|s| s.stream_status())
            == Some(&aws_sdk_kinesis::types::StreamStatus::Active)
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    }
    for p in payloads {
        admin
            .put_record()
            .stream_name(stream)
            .partition_key("k")
            .data(Blob::new(p.to_vec()))
            .send()
            .await
            .unwrap();
    }
}

async fn read(
    endpoint: &str,
    stream: &str,
    credentials: KinesisCredentials,
    format: ValueFormat,
) -> Vec<Value> {
    let label = format!("{credentials:?}");
    let mut cfg = KinesisSourceConfig::new(stream);
    cfg.region = Some(REGION.into());
    cfg.endpoint_url = Some(endpoint.into());
    cfg.credentials = credentials;
    cfg.value_format = format;
    cfg.poll_interval_ms = 200;
    cfg.idle_termination_secs = Some(2);
    let source = KinesisSource::new(cfg).await.unwrap();
    let records = source
        .fetch_all()
        .await
        .unwrap_or_else(|e| panic!("{label}: {e}"));
    records.into_iter().map(|r| r["data"].clone()).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn profile_assume_role_and_web_identity_read_string_and_bytes_payloads() {
    // Printed only when the test fails: where a stalled credentials or
    // Kinesis call was waiting.
    let _ = tracing_subscriber::fmt()
        .with_env_filter("aws_config=debug,aws_smithy_runtime=debug,aws_sdk_sts=debug,faucet_source_kinesis=debug")
        .with_test_writer()
        .try_init();
    let container = phase(
        "starting LocalStack",
        faucet_conformance::containers::start(|| {
            LocalStack::default().with_env_var("SERVICES", "kinesis,sts")
        }),
    )
    .await;
    let port = container.get_host_port_ipv4(4566).await.unwrap();
    let endpoint = format!("http://127.0.0.1:{port}");
    set(&[
        ("AWS_ENDPOINT_URL_STS", endpoint.as_str()),
        ("AWS_EC2_METADATA_DISABLED", "true"),
        ("AWS_REGION", REGION),
    ]);
    let text: [&[u8]; 2] = [b"plain text", "caf\u{e9}".as_bytes()];
    phase(
        "seeding account 111111111111",
        seed(&endpoint, "111111111111", "text", &text),
    )
    .await;
    phase(
        "seeding account 222222222222",
        seed(&endpoint, "222222222222", "binary", &[&[0xff, 0x00, 0x7f]]),
    )
    .await;
    phase(
        "seeding account 333333333333",
        seed(&endpoint, "333333333333", "text", &text),
    )
    .await;
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
            "text",
            KinesisCredentials::Profile {
                name: "faucet-test".into(),
            },
            ValueFormat::String,
        ),
    )
    .await;
    assert_eq!(
        got,
        vec![Value::from("plain text"), Value::from("caf\u{e9}")]
    );
    // No shared files from here on: the host's own ~/.aws must not answer.
    let none = dir.path().join("absent");
    set(&[
        ("AWS_CONFIG_FILE", none.to_str().unwrap()),
        ("AWS_SHARED_CREDENTIALS_FILE", none.to_str().unwrap()),
    ]);

    // assume_role: STS trades the base credentials for a role session. The
    // base key is a 12-digit account id, as LocalStack scopes by account.
    set(&[
        ("AWS_ACCESS_KEY_ID", "000000000000"),
        ("AWS_SECRET_ACCESS_KEY", "test"),
    ]);
    let got = phase(
        "the assume_role read",
        read(
            &endpoint,
            "binary",
            KinesisCredentials::AssumeRole {
                role_arn: role("222222222222"),
                session_name: Some("faucet-it".into()),
                external_id: Some("ext-1".into()),
            },
            ValueFormat::Bytes,
        ),
    )
    .await;
    let b64 = base64::engine::general_purpose::STANDARD;
    assert_eq!(got, vec![Value::from(b64.encode([0xff, 0x00, 0x7f]))]);

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
        read(
            &endpoint,
            "text",
            KinesisCredentials::WebIdentity,
            ValueFormat::String,
        ),
    )
    .await;
    assert_eq!(
        got,
        vec![Value::from("plain text"), Value::from("caf\u{e9}")]
    );
}
