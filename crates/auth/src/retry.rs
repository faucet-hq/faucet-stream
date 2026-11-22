//! Bounded retry for token-endpoint requests (#789 API-38): a `429`, a `5xx`,
//! a `400` whose typed OAuth `error` code is transient, or a connect/timeout
//! failure is retried with jittered backoff (honouring `Retry-After`), so a
//! routine IdP blip does not fail every connector sharing the provider.
//!
//! A grant that rotates its credential (the refresh-token grant) is not
//! idempotent: an IdP that already acted on a request issues a new refresh
//! token and revokes the old one, so resending it after a lost reply would
//! fail (or, with reuse detection, revoke the whole grant). Such a grant is
//! retried only when the IdP provably did not act: a failed connect, a `429`,
//! a `503` with `Retry-After`, or `temporarily_unavailable`.

use faucet_core::FaucetError;
use faucet_core::http::Client;
use faucet_core::io::CallContext;
use reqwest::RequestBuilder;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

/// Attempts per token request, the first included.
const MAX_ATTEMPTS: u32 = 4;
const RETRY_BASE: Duration = Duration::from_millis(500);
/// Longest `Retry-After` honoured; a longer one is capped rather than obeyed.
const MAX_RETRY_AFTER: Duration = Duration::from_secs(60);
/// RFC 6749 §5.2 `error` codes that mean "try again" (plus a common extension).
const TRANSIENT_OAUTH_ERROR_CODES: &[&str] =
    &["server_error", "temporarily_unavailable", "unknown_error"];

/// Whether resending a token request is safe once the endpoint may have
/// acted on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Grant {
    /// The grant issues a token without consuming anything (client
    /// credentials, a token endpoint, a signed assertion).
    Idempotent,
    /// The grant spends a credential the endpoint rotates (refresh token).
    Rotating,
}

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

/// The typed OAuth `error` code of a token response body.
fn oauth_error(body: &[u8]) -> Option<String> {
    serde_json::from_slice::<serde_json::Value>(body)
        .ok()?
        .get("error")?
        .as_str()
        .map(str::to_owned)
}

/// Whether the endpoint explicitly declined a request without acting on it:
/// the only replies a rotating grant may be resent after.
pub(crate) fn is_decline(status: u16, body: &[u8], retry_after: Option<Duration>) -> bool {
    status == 429
        || (status == 503 && retry_after.is_some())
        || ((status == 400 || status == 503)
            && oauth_error(body).as_deref() == Some("temporarily_unavailable"))
}

/// Whether a failed rotating-grant request may have reached the endpoint (an
/// attempt timeout, a connection lost after sending): its reply is unknown.
pub(crate) fn may_have_reached(e: &FaucetError) -> bool {
    match e {
        FaucetError::Timeout { .. } => true,
        FaucetError::Http(e) => !e.is_connect() && !e.is_builder(),
        _ => false,
    }
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
///
/// Runs through [`CallContext::call`](faucet_core::io::CallContext::call): a
/// transient reply becomes a [`FaucetError::RateLimited`] carrying its wait
/// (the capped `Retry-After`, else jittered backoff), and a connect / timeout
/// failure is retried by class.
pub(crate) async fn send_token_request(
    http: &Client,
    grant: Grant,
    make: impl Fn() -> Result<RequestBuilder, FaucetError>,
) -> Result<TokenReply, FaucetError> {
    let ctx = CallContext::new().with_retry(faucet_core::RetryPolicy {
        max_attempts: MAX_ATTEMPTS,
        backoff: faucet_core::BackoffKind::Exponential,
        base: RETRY_BASE,
        max: Duration::from_secs(60),
        jitter: true,
        retry_on: faucet_core::RetryClassSet::default(),
    });
    let ctx = match grant {
        Grant::Idempotent => ctx,
        Grant::Rotating => ctx.non_idempotent(),
    };
    let last_transient: Mutex<Option<TokenReply>> = Mutex::new(None);
    let attempt = AtomicU32::new(0);
    let outcome = ctx
        .call("token", || async {
            let n = attempt.fetch_add(1, Ordering::Relaxed) + 1;
            let resp = http.send(make()?).await?;
            let status = resp.status().as_u16();
            let wait = retry_after(resp.headers());
            let body = http.bytes(resp).await?;
            let retry = match grant {
                Grant::Idempotent => is_transient(status, &body),
                Grant::Rotating => is_decline(status, &body, wait),
            };
            if (200..300).contains(&status) || !retry {
                return Ok(TokenReply { status, body });
            }
            tracing::warn!(
                status,
                attempt = n,
                "token endpoint transient failure; retrying after backoff"
            );
            let delay =
                wait.unwrap_or_else(|| faucet_core::retry::backoff_with_jitter(RETRY_BASE, n));
            *last_transient.lock().unwrap_or_else(|e| e.into_inner()) =
                Some(TokenReply { status, body });
            Err(FaucetError::RateLimited(delay))
        })
        .await;
    match outcome {
        Err(FaucetError::RateLimited(_)) => last_transient
            .into_inner()
            .unwrap_or_else(|e| e.into_inner())
            .ok_or_else(|| FaucetError::Auth("token endpoint retries exhausted".into())),
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
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
        let client = Client::new().unwrap();
        let url = format!("{}/t", server.uri());
        let reply = send_token_request(&client, Grant::Idempotent, || Ok(client.post(&url)))
            .await
            .unwrap();
        assert!(reply.is_success());
        assert_eq!(reply.text(), "ok");
        assert_eq!(hits.load(Ordering::SeqCst), 2);

        let url = format!("{}/p", server.uri());
        let reply = send_token_request(&client, Grant::Idempotent, || Ok(client.post(&url)))
            .await
            .unwrap();
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
        let client = Client::new().unwrap();
        let url = format!("{}/t", server.uri());
        let reply = send_token_request(&client, Grant::Idempotent, || Ok(client.post(&url)))
            .await
            .unwrap();
        assert_eq!(reply.status, 429);
        assert_eq!(
            server.received_requests().await.unwrap().len(),
            MAX_ATTEMPTS as usize
        );
        let err = send_token_request(&client, Grant::Idempotent, || {
            Err(FaucetError::Config("bad body".into()))
        })
        .await
        .err()
        .unwrap();
        assert!(err.to_string().contains("bad body"));
    }

    #[test]
    fn a_rotating_grant_counts_only_explicit_declines() {
        let after = Some(Duration::from_secs(1));
        assert!(is_decline(429, b"", None));
        assert!(is_decline(503, b"", after));
        assert!(!is_decline(503, b"down", None));
        assert!(is_decline(
            400,
            br#"{"error":"temporarily_unavailable"}"#,
            None
        ));
        assert!(!is_decline(400, br#"{"error":"server_error"}"#, None));
        assert!(!is_decline(500, br#"{"error":"unknown_error"}"#, None));
        assert!(!is_decline(502, b"", None));
        assert!(may_have_reached(&FaucetError::Timeout {
            operation: "token call".into(),
            after: Duration::from_secs(1),
        }));
        assert!(!may_have_reached(&FaucetError::Auth("x".into())));
    }

    #[tokio::test]
    async fn a_rotating_grant_is_not_resent_after_an_ambiguous_failure() {
        let server = MockServer::start().await;
        let hits = Arc::new(AtomicUsize::new(0));
        Mock::given(wiremock::matchers::path("/t"))
            .respond_with(FailThenOk(hits.clone(), 502))
            .mount(&server)
            .await;
        let client = Client::new().unwrap();
        let url = format!("{}/t", server.uri());
        let reply = send_token_request(&client, Grant::Rotating, || Ok(client.post(&url)))
            .await
            .unwrap();
        assert_eq!(reply.status, 502, "a bare 502 is the final reply");
        assert_eq!(hits.load(Ordering::SeqCst), 1);

        let slow = MockServer::start().await;
        let slow_hits = Arc::new(AtomicUsize::new(0));
        let h = slow_hits.clone();
        Mock::given(wiremock::matchers::path("/t"))
            .respond_with(move |_: &wiremock::Request| {
                h.fetch_add(1, Ordering::SeqCst);
                ResponseTemplate::new(200)
                    .set_body_string("ok")
                    .set_delay(Duration::from_millis(400))
            })
            .mount(&slow)
            .await;
        let short = Client::builder()
            .timeout(Duration::from_millis(100))
            .build()
            .unwrap();
        let url = format!("{}/t", slow.uri());
        let err = send_token_request(&short, Grant::Rotating, || Ok(short.post(&url)))
            .await
            .err()
            .unwrap();
        assert!(may_have_reached(&err), "{err:?}");
        assert_eq!(slow_hits.load(Ordering::SeqCst), 1, "sent once");
        let _ = send_token_request(&short, Grant::Idempotent, || Ok(short.post(&url))).await;
        assert!(
            slow_hits.load(Ordering::SeqCst) > 2,
            "an idempotent grant retries"
        );
    }

    #[tokio::test]
    async fn a_rotating_grant_is_resent_after_a_decline() {
        let server = MockServer::start().await;
        let hits = Arc::new(AtomicUsize::new(0));
        Mock::given(wiremock::matchers::path("/t"))
            .respond_with(FailThenOk(hits.clone(), 503))
            .mount(&server)
            .await;
        let client = Client::new().unwrap();
        let url = format!("{}/t", server.uri());
        let reply = send_token_request(&client, Grant::Rotating, || Ok(client.post(&url)))
            .await
            .unwrap();
        assert!(reply.is_success());
        assert_eq!(hits.load(Ordering::SeqCst), 2);
        let err = send_token_request(&client, Grant::Rotating, || {
            Ok(client.post("http://127.0.0.1:1/t"))
        })
        .await
        .err()
        .unwrap();
        assert!(
            matches!(&err, FaucetError::Http(e) if e.is_connect()),
            "{err:?}"
        );
        assert!(!may_have_reached(&err));
    }

    #[tokio::test]
    async fn a_connect_failure_is_retried_then_surfaced() {
        let client = Client::new().unwrap();
        let err = send_token_request(&client, Grant::Idempotent, || {
            Ok(client.post("http://127.0.0.1:1/t"))
        })
        .await
        .err()
        .unwrap();
        assert!(matches!(err, FaucetError::Http(_)), "{err:?}");
    }
}
