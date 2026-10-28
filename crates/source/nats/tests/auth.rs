//! Every NATS auth method (#967) against a `nats:2.10` that requires it:
//! `token`, `user_password`, `nkey`, and `creds_file` (an operator-mode server
//! with an in-memory account resolver; the operator, account and user JWTs are
//! signed here). Each source reads a real message published with the same
//! credentials, and anonymous or wrong credentials are refused. The `string`
//! value format is read on the token server.
#![allow(clippy::disallowed_methods)] // the publisher waits for the subscription

use base64::Engine as _;
use faucet_common_nats::NatsAuth;
use faucet_conformance::containers::{ReadyProbe, StartOptions};
use faucet_core::Source;
use faucet_source_nats::{NatsSource, NatsSourceConfig, NatsValueFormat};
use futures::StreamExt;
use serde_json::{Value, json};
use std::collections::HashMap;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::io::AsyncReadExt;

const PORT: u16 = 4222;

async fn nats(config: &str) -> (ContainerAsync<GenericImage>, String) {
    // The server greets every client with INFO; a forwarded port that accepts
    // before the server is reachable does not.
    let ready = ReadyProbe::custom(PORT, |ep| async move {
        let mut conn = tokio::net::TcpStream::connect(ep.addr())
            .await
            .map_err(|e| e.to_string())?;
        let mut buf = [0u8; 4];
        conn.read_exact(&mut buf).await.map_err(|e| e.to_string())?;
        (&buf == b"INFO")
            .then_some(())
            .ok_or_else(|| "no INFO".to_string())
    });
    let container = faucet_conformance::containers::start_with(
        || {
            GenericImage::new("nats", "2.10")
                .with_exposed_port(PORT.tcp())
                .with_wait_for(WaitFor::message_on_stderr("Server is ready"))
                .with_copy_to("/etc/faucet.conf", config.as_bytes().to_vec())
                .with_cmd(["-c", "/etc/faucet.conf"])
        },
        &StartOptions::default().ready(ready),
    )
    .await;
    let port = container.get_host_port_ipv4(PORT).await.unwrap();
    (container, format!("nats://127.0.0.1:{port}"))
}

fn config(server: &str, subject: &str, auth: NatsAuth) -> NatsSourceConfig {
    let mut cfg = NatsSourceConfig::new(subject);
    cfg.connection.servers = vec![server.to_string()];
    cfg.connection.auth = auth;
    cfg.max_messages = Some(1);
    cfg.idle_timeout_secs = Some(15);
    cfg
}

/// Read one message the `publisher` options send to `subject`.
async fn read_one(
    cfg: NatsSourceConfig,
    publisher: async_nats::ConnectOptions,
    payload: &'static [u8],
) -> Result<Vec<Value>, String> {
    let server = cfg.connection.servers[0].clone();
    let subject = cfg.subject.clone();
    let source = NatsSource::new(cfg).await.map_err(|e| e.to_string())?;
    let ctx = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 10);
    let send = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let client = publisher.connect(server).await.expect("publisher connect");
        client.publish(subject, payload.into()).await.unwrap();
        client.flush().await.unwrap();
    });
    let mut records = Vec::new();
    while let Some(page) = pages.next().await {
        records.extend(page.map_err(|e| e.to_string())?.records);
    }
    send.await.unwrap();
    Ok(records)
}

async fn refused(cfg: NatsSourceConfig) -> String {
    match NatsSource::new(cfg).await {
        Err(e) => e.to_string(),
        Ok(source) => {
            let ctx = HashMap::new();
            match source.stream_pages(&ctx, 10).next().await {
                Some(Err(e)) => e.to_string(),
                other => panic!("must be refused, got {other:?}"),
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn token_auth_reads_and_the_string_format_keeps_the_text() {
    let (_c, server) = nats("authorization { token: \"s3cret-token\" }").await;
    let mut cfg = config(
        &server,
        "auth.token",
        NatsAuth::Token {
            token: "s3cret-token".into(),
        },
    );
    cfg.value_format = NatsValueFormat::String;
    let got = read_one(
        cfg,
        async_nats::ConnectOptions::with_token("s3cret-token".into()),
        br#"{"id":1}"#,
    )
    .await
    .unwrap();
    assert_eq!(got, vec![json!("{\"id\":1}")], "string keeps JSON as text");
    let err = refused(config(
        &server,
        "auth.token",
        NatsAuth::Token {
            token: "wrong".into(),
        },
    ))
    .await;
    assert!(err.to_lowercase().contains("authorization"), "{err}");
    refused(config(&server, "auth.token", NatsAuth::None)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn user_password_auth_reads() {
    let (_c, server) =
        nats("authorization { users: [ { user: \"reader\", password: \"pa$$word\" } ] }").await;
    let auth = NatsAuth::UserPassword {
        username: "reader".into(),
        password: "pa$$word".into(),
    };
    let got = read_one(
        config(&server, "auth.user", auth),
        async_nats::ConnectOptions::with_user_and_password("reader".into(), "pa$$word".into()),
        br#"{"id":2}"#,
    )
    .await
    .unwrap();
    assert_eq!(got, vec![json!({"id": 2})]);
    refused(config(
        &server,
        "auth.user",
        NatsAuth::UserPassword {
            username: "reader".into(),
            password: "nope".into(),
        },
    ))
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn nkey_auth_reads() {
    let user = nkeys::KeyPair::new_user();
    let (_c, server) = nats(&format!(
        "authorization {{ users: [ {{ nkey: \"{}\" }} ] }}",
        user.public_key()
    ))
    .await;
    let seed = user.seed().unwrap();
    let got = read_one(
        config(&server, "auth.nkey", NatsAuth::NKey { nkey: seed.clone() }),
        async_nats::ConnectOptions::with_nkey(seed),
        br#"{"id":3}"#,
    )
    .await
    .unwrap();
    assert_eq!(got, vec![json!({"id": 3})]);
    let stranger = nkeys::KeyPair::new_user().seed().unwrap();
    refused(config(
        &server,
        "auth.nkey",
        NatsAuth::NKey { nkey: stranger },
    ))
    .await;
}

/// A NATS JWT (`ed25519-nkey`) for `claims`, signed by `issuer`.
fn jwt(issuer: &nkeys::KeyPair, subject: &str, name: &str, nats: Value) -> String {
    let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    let header = b64.encode(br#"{"typ":"JWT","alg":"ed25519-nkey"}"#);
    let claims = json!({
        "jti": format!("{name}-{}", std::process::id()),
        "iat": 1_700_000_000u64,
        "iss": issuer.public_key(),
        "name": name,
        "sub": subject,
        "nats": nats,
    });
    let body = b64.encode(claims.to_string());
    let signing_input = format!("{header}.{body}");
    let sig = issuer.sign(signing_input.as_bytes()).unwrap();
    format!("{signing_input}.{}", b64.encode(sig))
}

#[tokio::test(flavor = "multi_thread")]
async fn creds_file_auth_reads_on_an_operator_mode_server() {
    let operator = nkeys::KeyPair::new_operator();
    let account = nkeys::KeyPair::new_account();
    let user = nkeys::KeyPair::new_user();
    let operator_jwt = jwt(
        &operator,
        &operator.public_key(),
        "faucet-op",
        json!({"type": "operator", "version": 2}),
    );
    let unlimited = json!({"subs": -1, "data": -1, "payload": -1, "imports": -1,
        "exports": -1, "wildcards": true, "conn": -1, "leaf": -1});
    let account_jwt = jwt(
        &operator,
        &account.public_key(),
        "faucet-acct",
        json!({"type": "account", "version": 2, "limits": unlimited}),
    );
    let user_jwt = jwt(
        &account,
        &user.public_key(),
        "faucet-user",
        json!({"type": "user", "version": 2, "pub": {}, "sub": {},
            "subs": -1, "data": -1, "payload": -1}),
    );
    let (_c, server) = nats(&format!(
        "operator: {operator_jwt}\nresolver: MEMORY\nresolver_preload: {{ {}: {account_jwt} }}\n",
        account.public_key()
    ))
    .await;
    let creds = format!(
        "-----BEGIN NATS USER JWT-----\n{user_jwt}\n------END NATS USER JWT------\n\n\
         -----BEGIN USER NKEY SEED-----\n{}\n------END USER NKEY SEED------\n",
        user.seed().unwrap()
    );
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("user.creds");
    std::fs::write(&path, creds).unwrap();

    let publisher = async_nats::ConnectOptions::with_credentials_file(&path)
        .await
        .unwrap();
    let got = read_one(
        config(
            &server,
            "auth.creds",
            NatsAuth::CredsFile { path: path.clone() },
        ),
        publisher,
        br#"{"id":4}"#,
    )
    .await
    .unwrap();
    assert_eq!(got, vec![json!({"id": 4})]);
    refused(config(&server, "auth.creds", NatsAuth::None)).await;
    let err = refused(config(
        &server,
        "auth.creds",
        NatsAuth::CredsFile {
            path: dir.path().join("missing.creds"),
        },
    ))
    .await;
    assert!(err.contains("failed to read credentials file"), "{err}");
}
