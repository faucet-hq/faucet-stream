//! The control-plane connections connect with the credentials
//! `connection_url` names and nothing from the process environment. A
//! one-shot server stands in for PostgreSQL: it asks for a cleartext password
//! and records what it is sent. Its own test binary: it sets a process
//! environment variable.
#![allow(unsafe_code)] // tests set process environment variables

use faucet_core::check::CheckContext;
use faucet_source_postgres_cdc::{PostgresCdcSource, PostgresCdcSourceConfig, Source};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

async fn read_message(sock: &mut tokio::net::TcpStream, typed: bool) -> Option<(u8, Vec<u8>)> {
    let tag = if typed { sock.read_u8().await.ok()? } else { 0 };
    let len = sock.read_u32().await.ok()? as usize;
    let mut body = vec![0; len.checked_sub(4)?];
    sock.read_exact(&mut body).await.ok()?;
    Some((tag, body))
}

/// Accept one connection; return the password the client sent, if any.
async fn capture_password(listener: TcpListener) -> Option<String> {
    let (mut sock, _) = listener.accept().await.ok()?;
    let (_, mut body) = read_message(&mut sock, false).await?;
    if body.starts_with(&80_877_103u32.to_be_bytes()) {
        sock.write_all(b"N").await.ok()?;
        body = read_message(&mut sock, false).await?.1;
    }
    let _ = body;
    sock.write_all(&[b'R', 0, 0, 0, 8, 0, 0, 0, 3]).await.ok()?;
    let (tag, body) = read_message(&mut sock, true).await?;
    (tag == b'p').then(|| String::from_utf8_lossy(body.strip_suffix(&[0]).unwrap_or(&body)).into())
}

#[tokio::test]
async fn control_connections_use_config_credentials_only() {
    // SAFETY: this test binary runs this one test; nothing else reads the environment concurrently.
    unsafe { std::env::set_var("PGPASSWORD", "dummy-secret") };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(capture_password(listener));
    let config: PostgresCdcSourceConfig = serde_json::from_value(serde_json::json!({
        "connection_url": format!("postgres://reader@127.0.0.1:{port}/db"),
        "slot_name": "faucet_slot",
        "publication_name": "faucet_pub",
    }))
    .unwrap();
    let source = PostgresCdcSource::new(config).await.unwrap();
    let _ = source.check(&CheckContext::default()).await;
    let sent = server.await.unwrap();
    assert_eq!(
        sent.as_deref(),
        Some(""),
        "no password but the URL's is sent"
    );
}
