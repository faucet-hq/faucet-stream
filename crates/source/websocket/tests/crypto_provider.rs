//! A `wss://` connect in a build that links two rustls providers (#971).
#![allow(clippy::disallowed_methods)] // tests drive time and raw clients directly

use faucet_core::{AuthSpec, Source};
use faucet_source_websocket::{
    OnParseError, WebsocketAuth, WebsocketSource, WebsocketSourceConfig, WsMessageFormat,
};
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;

#[tokio::test]
async fn wss_connect_with_two_rustls_providers_fails_instead_of_panicking() {
    assert!(rustls::crypto::CryptoProvider::get_default().is_none());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        if let Ok((mut stream, _)) = listener.accept().await {
            let mut buf = [0u8; 1024];
            let _ = stream.read(&mut buf).await;
        }
    });
    let config = WebsocketSourceConfig {
        url: format!("wss://{addr}"),
        auth: AuthSpec::Inline(WebsocketAuth::None),
        subscribe_messages: vec![],
        message_format: WsMessageFormat::Json,
        on_parse_error: OnParseError::Fail,
        envelope: false,
        ping_interval: None,
        max_messages: Some(1),
        idle_timeout: None,
        reconnect: false,
        reconnect_backoff: Duration::from_millis(50),
        max_reconnect_attempts: None,
        max_message_bytes: None,
        batch_size: 1000,
        connect_timeout: Duration::from_secs(2),
    };
    let src = WebsocketSource::new(config).unwrap();
    assert!(src.fetch_all().await.is_err());
    assert!(rustls::crypto::CryptoProvider::get_default().is_some());
}
