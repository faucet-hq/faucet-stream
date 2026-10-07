//! Bounded retry for token-endpoint requests (#789 API-38): a `429`, a `5xx`,
//! a `400` whose typed OAuth `error` code is transient, or a connect/timeout
//! failure is retried with jittered backoff (honouring `Retry-After`), so a
//! routine IdP blip does not fail every connector sharing the provider.

use faucet_core::FaucetError;
use reqwest::RequestBuilder;
use std::time::Duration;

/// Attempts per token request, the first included.
const MAX_ATTEMPTS: u32 = 4;
const RETRY_BASE: Duration = Duration::from_millis(500);
/// Longest `Retry-After` honoured; a longer one is capped rather than obeyed.
const MAX_RETRY_AFTER: Duration = Duration::from_secs(60);
/// RFC 6749 §5.2 `error` codes that mean "try again" (plus a common extension).
const TRANSIENT_OAUTH_ERROR_CODES: &[&str] =
    &["server_error", "temporarily_unavailable", "unknown_error"];

/// A token endpoint's final reply: its status and full body.
pub(crate) struct TokenReply {
    pub status: u16,
    pub body: Vec<u8>,
}

impl TokenReply {
    pub fn is_success(&self) -> bool {
        (200..300).contains(&self.status)
    }

    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }
}

/// Whether a non-success token response is worth retrying.
pub(crate) fn is_transient(status: u16, body: &[u8]) -> bool {
    if status == 429 || (500..600).contains(&status) {
        return true;
    }
    status == 400
        && serde_json::from_slice::<serde_json::Value>(body)
            .ok()
            .and_then(|v| v.get("error").and_then(|e| e.as_str()).map(str::to_owned))
            .is_some_and(|e| TRANSIENT_OAUTH_ERROR_CODES.contains(&e.as_str()))
}

/// A `Retry-After: <seconds>` delay, capped at [`MAX_RETRY_AFTER`].
pub(crate) fn retry_after(headers: &reqwest::header::HeaderMap) -> Option<Duration> {
    let secs: u64 = headers
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim()
        .parse()
        .ok()?;
    Some(Duration::from_secs(secs).min(MAX_RETRY_AFTER))
}

/// Send the request `make` builds, retrying transient failures, and return
/// the final reply (success, a permanent failure, or the last transient one).
pub(crate) async fn send_token_request(
    make: impl Fn() -> Result<RequestBuilder, FaucetError>,
) -> Result<TokenReply, FaucetError> {
    let mut attempt = 0;
    loop {
        attempt += 1;
        let last = attempt >= MAX_ATTEMPTS;
        let resp = match make()?.send().await {
            Ok(r) => r,
            Err(e) if !last && (e.is_timeout() || e.is_connect()) => {
                tokio::time::sleep(faucet_core::retry::backoff_with_jitter(RETRY_BASE, attempt))
                    .await;
                continue;
            }
            Err(e) => return Err(e.into()),
        };
        let status = resp.status().as_u16();
        let wait = retry_after(resp.headers());
        let body = resp.bytes().await?.to_vec();
        if !(200..300).contains(&status) && !last && is_transient(status, &body) {
            tracing::warn!(
                status,
                attempt,
                "token endpoint transient failure; retrying after backoff"
            );
            let delay =
                wait.unwrap_or_else(|| faucet_core::retry::backoff_with_jitter(RETRY_BASE, attempt));
            tokio::time::sleep(delay).await;
            continue;
        }
        return Ok(TokenReply { status, body });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use wiremock::{Mock, MockServer, Respond, ResponseTemplate};

    #[test]
    fn classification() {
        assert!(is_transient(429, b""));
        assert!(is_transient(503, b"down"));
        assert!(is_transient(400, br#"{"error":"temporarily_unavailable"}"#));
        assert!(!is_transient(400, br#"{"error":"invalid_grant"}"#));
        assert!(!is_transient(400, b"server_error"));
        assert!(!is_transient(401, b""));
    }

    #[test]
    fn retry_after_is_parsed_and_capped() {
        let mut h = reqwest::header::HeaderMap::new();
        assert_eq!(retry_after(&h), None);
        h.insert(reqwest::header::RETRY_AFTER, "2".parse().unwrap());
        assert_eq!(retry_after(&h), Some(Duration::from_secs(2)));
        h.insert(reqwest::header::RETRY_AFTER, "86400".parse().unwrap());
        assert_eq!(retry_after(&h), Some(MAX_RETRY_AFTER));
        h.insert(reqwest::header::RETRY_AFTER, "soon".parse().unwrap());
        assert_eq!(retry_after(&h), None);
    }

    struct FailThenOk(Arc<AtomicUsize>, u16);
    impl Respond for FailThenOk {
        fn respond(&self, _: &wiremock::Request) -> ResponseTemplate {
            if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
                ResponseTemplate::new(self.1).insert_header("retry-after", "0")
            } else {
                ResponseTemplate::new(200).set_body_string("ok")
            }
        }
    }

    #[tokio::test]
    async fn a_transient_status_is_retried_and_a_permanent_one_is_not() {
        let server = MockServer::start().await;
        let hits = Arc::new(AtomicUsize::new(0));
        Mock::given(wiremock::matchers::path("/t"))
            .respond_with(FailThenOk(hits.clone(), 503))
            .mount(&server)
            .await;
        Mock::given(wiremock::matchers::path("/p"))
            .respond_with(ResponseTemplate::new(401).set_body_string("nope"))
            .mount(&server)
            .await;
        let client = reqwest::Client::new();
        let url = format!("{}/t", server.uri());
        let reply = send_token_request(|| Ok(client.post(&url))).await.unwrap();
        assert!(reply.is_success());
        assert_eq!(reply.text(), "ok");
        assert_eq!(hits.load(Ordering::SeqCst), 2);

        let url = format!("{}/p", server.uri());
        let reply = send_token_request(|| Ok(client.post(&url))).await.unwrap();
        assert_eq!(reply.status, 401);
        assert_eq!(server.received_requests().await.unwrap().len(), 3);
    }

    #[tokio::test]
    async fn attempts_are_bounded() {
        let server = MockServer::start().await;
        Mock::given(wiremock::matchers::path("/t"))
            .respond_with(ResponseTemplate::new(429).insert_header("retry-after", "0"))
            .mount(&server)
            .await;
        let client = reqwest::Client::new();
        let url = format!("{}/t", server.uri());
        let reply = send_token_request(|| Ok(client.post(&url))).await.unwrap();
        assert_eq!(reply.status, 429);
        assert_eq!(
            server.received_requests().await.unwrap().len(),
            MAX_ATTEMPTS as usize
        );
        let err = send_token_request(|| Err(FaucetError::Config("bad body".into())))
            .await
            .err()
            .unwrap();
        assert!(err.to_string().contains("bad body"));
    }

    #[tokio::test]
    async fn a_connect_failure_is_retried_then_surfaced() {
        let client = reqwest::Client::new();
        let err = send_token_request(|| Ok(client.post("http://127.0.0.1:1/t")))
            .await
            .err()
            .unwrap();
        assert!(matches!(err, FaucetError::Http(_)), "{err:?}");
    }
}
