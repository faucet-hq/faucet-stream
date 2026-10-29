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

mod support;
use support::phase;

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

/// A bounded client for `account` (LocalStack scopes resources by the
/// 12-digit access key).
async fn admin(endpoint: &str, account: &str) -> aws_sdk_kinesis::Client {
    support::bounded(
        faucet_source_kinesis::build_client(
            Some(REGION),
            Some(endpoint),
            &KinesisCredentials::AccessKey {
                access_key_id: account.into(),
                secret_access_key: "test".into(),
                session_token: None,
            },
        )
        .await
        .unwrap(),
    )
}

async fn seed(endpoint: &str, account: &str, stream: &str, payloads: &[&[u8]]) {
    let admin = admin(endpoint, account).await;
    support::create_stream(&admin, stream, 1).await;
    for p in payloads {
        admin
            .put_record()
            .stream_name(stream)
            .partition_key("k")
            .data(Blob::new(p.to_vec()))
            .send()
            .await
            .unwrap_or_else(|e| panic!("PutRecord on {stream}: {e:?}"));
    }
}

/// Read `stream` (owned by `account`) through the connector with `credentials`.
///
/// The connector retries a failed `GetRecords` forever, so a bounded
/// `GetRecords` with the owner's key runs first: a stalled emulator then fails
/// here with the SDK error rather than at the phase timeout.
async fn read(
    endpoint: &str,
    account: &str,
    stream: &str,
    credentials: KinesisCredentials,
    format: ValueFormat,
) -> Vec<Value> {
    support::probe_reads(&admin(endpoint, account).await, stream).await;
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
    // Prints the emulator's log if anything below panics.
    let emulator = support::start("kinesis,sts").await;
    let endpoint = emulator.endpoint.clone();
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
            "111111111111",
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
            "222222222222",
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
            "333333333333",
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
