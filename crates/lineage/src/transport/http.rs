//! HTTP transport — POST each event to an OpenLineage endpoint.

use super::Transport;
use crate::config::HttpAuth;
use async_trait::async_trait;
use faucet_core::FaucetError;
use std::time::Duration;

pub struct HttpTransport {
    client: reqwest::Client,
    url: String,
    auth: Option<HttpAuth>,
}

impl HttpTransport {
    pub fn new(
        url: String,
        timeout: Duration,
        auth: Option<HttpAuth>,
    ) -> Result<Self, FaucetError> {
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|e| FaucetError::Custom(Box::new(e)))?;
        Ok(Self { client, url, auth })
    }
}

#[async_trait]
impl Transport for HttpTransport {
    async fn send(&self, event_json: Vec<u8>) -> Result<(), FaucetError> {
        let mut req = self
            .client
            .post(&self.url)
            .header("content-type", "application/json")
            .body(event_json);
        if let Some(HttpAuth::Bearer { token }) = &self.auth {
            req = req.bearer_auth(token);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| FaucetError::Custom(Box::new(e.without_url())))?;
        let status = resp.status();
        if !status.is_success() {
            return Err(FaucetError::HttpStatus {
                status: status.as_u16(),
                url: faucet_core::util::redact_uri_credentials(&self.url),
                body: read_capped(resp, MAX_ERROR_BODY_BYTES).await,
            });
        }
        Ok(())
    }
}

/// Longest error-response body kept for the error message.
pub const MAX_ERROR_BODY_BYTES: usize = 8 * 1024;

async fn read_capped(mut resp: reqwest::Response, max: usize) -> String {
    let mut buf: Vec<u8> = Vec::new();
    let mut truncated = false;
    while let Ok(Some(chunk)) = resp.chunk().await {
        let room = max - buf.len();
        if chunk.len() > room {
            buf.extend_from_slice(&chunk[..room]);
            truncated = true;
            break;
        }
        buf.extend_from_slice(&chunk);
    }
    let mut text = String::from_utf8_lossy(&buf).into_owned();
    if truncated {
        text.push_str("…[truncated]");
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::HttpAuth;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn a_huge_error_body_is_truncated() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(500).set_body_string("x".repeat(1024 * 1024)))
            .mount(&server)
            .await;
        let t = HttpTransport::new(server.uri(), std::time::Duration::from_secs(5), None).unwrap();
        let err = t.send(b"{}".to_vec()).await.unwrap_err();
        let FaucetError::HttpStatus { status, body, .. } = err else {
            panic!("unexpected error {err:?}");
        };
        assert_eq!(status, 500);
        assert!(body.len() < MAX_ERROR_BODY_BYTES + 32, "{}", body.len());
        assert!(body.ends_with("[truncated]"));
    }

    #[tokio::test]
    async fn posts_event_with_bearer_auth() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v1/lineage"))
            .and(header("authorization", "Bearer tok"))
            .respond_with(ResponseTemplate::new(201))
            .expect(1)
            .mount(&server)
            .await;
        let t = HttpTransport::new(
            format!("{}/api/v1/lineage", server.uri()),
            std::time::Duration::from_secs(5),
            Some(HttpAuth::Bearer {
                token: "tok".into(),
            }),
        )
        .unwrap();
        t.send(b"{\"eventType\":\"START\"}".to_vec()).await.unwrap();
    }

    #[tokio::test]
    async fn non_2xx_is_an_error() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;
        let t = HttpTransport::new(server.uri(), std::time::Duration::from_secs(5), None).unwrap();
        assert!(t.send(b"{}".to_vec()).await.is_err());
    }

    #[tokio::test]
    async fn errors_never_carry_url_credentials() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;
        let url = server.uri().replace("http://", "http://user:hunter2@");
        let t = HttpTransport::new(url, std::time::Duration::from_secs(5), None).unwrap();
        let err = t.send(b"{}".to_vec()).await.unwrap_err().to_string();
        assert!(err.contains("503"), "{err}");
        assert!(!err.contains("hunter2"), "{err}");

        let unreachable = HttpTransport::new(
            "http://user:hunter2@127.0.0.1:1/x".into(),
            std::time::Duration::from_secs(5),
            None,
        )
        .unwrap();
        let err = unreachable.send(b"{}".to_vec()).await.unwrap_err();
        assert!(!format!("{err} {err:?}").contains("hunter2"), "{err:?}");
    }
}
