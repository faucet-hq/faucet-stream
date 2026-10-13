//! `http(s)://` paths: one remote file, fetched with bounded retries.
//!
//! Every request is counted on the pipeline's round-trip recorder (ops `get`
//! and `head`); a `429` is counted as a throttle, each retry by class, and a
//! rate-limit sleep is timed, so the run's throttling metrics cover this
//! source like the API sources.

use faucet_core::FaucetError;
use faucet_core::io::CallContext;
use faucet_core::observability::RecorderSlot;
use faucet_core::resilience::RetryClass;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use std::collections::BTreeMap;
use std::time::Duration;

const BASE_BACKOFF: Duration = Duration::from_millis(100);
const MAX_BACKOFF: Duration = Duration::from_secs(10);

/// What a `HEAD` reports about the remote file.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HttpMeta {
    /// `Last-Modified`, in nanoseconds since the Unix epoch.
    pub last_modified_ns: Option<i64>,
}

/// A client for one remote file.
#[derive(Debug, Clone)]
pub struct HttpFetcher {
    client: faucet_core::http::Client,
    headers: HeaderMap,
    retries: u32,
}

impl HttpFetcher {
    /// Build the client once with the default timeouts (30 s to connect, 300 s
    /// without a byte); `headers` were validated with the config.
    pub fn new(headers: &BTreeMap<String, String>, retries: u32) -> Result<Self, FaucetError> {
        Self::with_timeouts(
            headers,
            retries,
            Duration::from_secs(30),
            Duration::from_secs(300),
        )
    }

    /// Build the client with explicit connect and read (idle) timeouts.
    pub fn with_timeouts(
        headers: &BTreeMap<String, String>,
        retries: u32,
        connect_timeout: Duration,
        read_timeout: Duration,
    ) -> Result<Self, FaucetError> {
        let mut map = HeaderMap::new();
        for (k, v) in headers {
            let name = HeaderName::from_bytes(k.as_bytes())
                .map_err(|e| FaucetError::Config(format!("file source: header {k:?}: {e}")))?;
            let value = HeaderValue::from_str(v)
                .map_err(|e| FaucetError::Config(format!("file source: header {k:?}: {e}")))?;
            map.insert(name, value);
        }
        let mut builder = faucet_core::http::Client::builder()
            .connect_timeout(connect_timeout)
            .read_timeout(read_timeout)
            .max_response_bytes(None);
        for name in headers.keys() {
            builder = builder.sensitive_header(name);
        }
        let client = builder
            .build()
            .map_err(|e| FaucetError::Config(format!("file source: http client: {e}")))?;
        Ok(Self {
            client,
            headers: map,
            retries,
        })
    }

    /// `GET` the file; the response body is left for the caller to stream.
    pub async fn get(
        &self,
        url: &str,
        slot: &RecorderSlot,
    ) -> Result<reqwest::Response, FaucetError> {
        self.get_in(url, slot, &unbounded()).await
    }

    /// [`get`](Self::get) with every attempt and backoff made through `calls`
    /// (#880): the context's cancel token and deadline end it.
    pub async fn get_in(
        &self,
        url: &str,
        slot: &RecorderSlot,
        calls: &CallContext,
    ) -> Result<reqwest::Response, FaucetError> {
        self.send(reqwest::Method::GET, url, "get", slot, calls)
            .await
    }

    /// `HEAD` the file for its modification time.
    pub async fn head(&self, url: &str, slot: &RecorderSlot) -> Result<HttpMeta, FaucetError> {
        self.head_in(url, slot, &unbounded()).await
    }

    /// [`head`](Self::head) through `calls` (see [`get_in`](Self::get_in)).
    pub async fn head_in(
        &self,
        url: &str,
        slot: &RecorderSlot,
        calls: &CallContext,
    ) -> Result<HttpMeta, FaucetError> {
        let resp = self
            .send(reqwest::Method::HEAD, url, "head", slot, calls)
            .await?;
        Ok(HttpMeta {
            last_modified_ns: resp
                .headers()
                .get(reqwest::header::LAST_MODIFIED)
                .and_then(|v| v.to_str().ok())
                .and_then(parse_http_date),
        })
    }

    async fn send(
        &self,
        method: reqwest::Method,
        url: &str,
        op: &'static str,
        slot: &RecorderSlot,
        calls: &CallContext,
    ) -> Result<reqwest::Response, FaucetError> {
        // `slot` counts the round trips; the context bounds them.
        let calls = calls.clone().with_roundtrip_metering(false);
        let mut attempt = 0u32;
        loop {
            slot.record(op);
            let result = match calls
                .call_once(
                    op,
                    self.client.send(
                        self.client
                            .request(method.clone(), url)
                            .headers(self.headers.clone()),
                    ),
                )
                .await
            {
                Err(spent @ (FaucetError::Cancelled { .. } | FaucetError::Timeout { .. })) => {
                    return Err(spent);
                }
                other => other,
            };
            let (class, wait, detail) = match result {
                Ok(resp) if resp.status().is_success() => return Ok(resp),
                Ok(resp) if resp.status() == reqwest::StatusCode::TOO_MANY_REQUESTS => {
                    slot.throttled();
                    let wait = retry_after(resp.headers()).unwrap_or_else(|| backoff(attempt));
                    (
                        RetryClass::RateLimited,
                        wait,
                        format!("HTTP {}", resp.status()),
                    )
                }
                Ok(resp) if resp.status().is_server_error() => (
                    RetryClass::Http5xx,
                    backoff(attempt),
                    format!("HTTP {}", resp.status()),
                ),
                Ok(resp) => {
                    return Err(FaucetError::HttpStatus {
                        status: resp.status().as_u16(),
                        url: url.to_string(),
                        body: resp.text().await.unwrap_or_default(),
                    });
                }
                Err(e) => {
                    let class = match faucet_core::resilience::classify(&e) {
                        Some(RetryClass::Timeout) => RetryClass::Timeout,
                        _ => RetryClass::Connection,
                    };
                    let detail = match &e {
                        FaucetError::Http(inner) => inner.to_string(),
                        other => other.to_string(),
                    };
                    (class, backoff(attempt), detail)
                }
            };
            if attempt >= self.retries {
                return Err(FaucetError::Source(format!(
                    "file source: {method} {url} failed after {} attempt(s): {detail}",
                    attempt + 1
                )));
            }
            slot.retry(class);
            if class == RetryClass::RateLimited {
                let _timer = slot.throttle_wait_timer();
                calls.wait(wait).await?;
            } else {
                calls.wait(wait).await?;
            }
            attempt += 1;
        }
    }
}

/// The context a fetcher call runs in when the caller gives none: no attempt
/// bound beyond the client's own timeouts.
fn unbounded() -> CallContext {
    CallContext::new().without_timeout()
}

fn backoff(attempt: u32) -> Duration {
    BASE_BACKOFF
        .saturating_mul(1u32 << attempt.min(16))
        .min(MAX_BACKOFF)
}

/// `Retry-After` as delta-seconds or an HTTP date, capped at [`MAX_BACKOFF`].
fn retry_after(headers: &HeaderMap) -> Option<Duration> {
    let v = headers
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim();
    let wait = match v.parse::<u64>() {
        Ok(secs) => Duration::from_secs(secs),
        Err(_) => {
            let at = parse_http_date(v)?;
            let now = chrono::Utc::now().timestamp_nanos_opt()?;
            Duration::from_nanos(u64::try_from(at.saturating_sub(now)).unwrap_or(0))
        }
    };
    Some(wait.min(MAX_BACKOFF))
}

/// An HTTP date (`Sun, 06 Nov 1994 08:49:37 GMT`) in nanoseconds since the
/// Unix epoch.
pub fn parse_http_date(v: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc2822(v.trim())
        .ok()?
        .timestamp_nanos_opt()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_and_caps() {
        assert_eq!(backoff(0), BASE_BACKOFF);
        assert_eq!(backoff(1), BASE_BACKOFF * 2);
        assert_eq!(backoff(40), MAX_BACKOFF);
    }

    #[test]
    fn retry_after_reads_seconds_and_dates() {
        let mut h = HeaderMap::new();
        assert_eq!(retry_after(&h), None);
        h.insert(reqwest::header::RETRY_AFTER, HeaderValue::from_static("2"));
        assert_eq!(retry_after(&h), Some(Duration::from_secs(2)));
        h.insert(
            reqwest::header::RETRY_AFTER,
            HeaderValue::from_static("9999"),
        );
        assert_eq!(retry_after(&h), Some(MAX_BACKOFF));
        h.insert(
            reqwest::header::RETRY_AFTER,
            HeaderValue::from_static("Sun, 06 Nov 1994 08:49:37 GMT"),
        );
        assert_eq!(
            retry_after(&h),
            Some(Duration::ZERO),
            "a past date waits nothing"
        );
        h.insert(
            reqwest::header::RETRY_AFTER,
            HeaderValue::from_static("soon"),
        );
        assert_eq!(retry_after(&h), None);
    }

    #[test]
    fn dates_and_url_names() {
        assert_eq!(
            parse_http_date("Thu, 01 Jan 1970 00:00:01 GMT"),
            Some(1_000_000_000)
        );
        assert_eq!(parse_http_date("nope"), None);
    }

    #[test]
    fn invalid_headers_are_config_errors() {
        let mut h = BTreeMap::new();
        h.insert("bad name".to_string(), "v".to_string());
        assert!(matches!(
            HttpFetcher::new(&h, 0),
            Err(FaucetError::Config(_))
        ));
        let mut h = BTreeMap::new();
        h.insert("x-ok".to_string(), "a\nb".to_string());
        assert!(matches!(
            HttpFetcher::new(&h, 0),
            Err(FaucetError::Config(_))
        ));
    }
}
