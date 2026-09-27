//! `http(s)://` paths: one remote file, fetched with bounded retries.
//!
//! Every request is counted on the pipeline's round-trip recorder (ops `get`
//! and `head`); a `429` is counted as a throttle, each retry by class, and a
//! rate-limit sleep is timed, so the run's throttling metrics cover this
//! source like the API sources.

use faucet_core::FaucetError;
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
    client: reqwest::Client,
    headers: HeaderMap,
    retries: u32,
}

impl HttpFetcher {
    /// Build the client once; `headers` were validated with the config.
    pub fn new(headers: &BTreeMap<String, String>, retries: u32) -> Result<Self, FaucetError> {
        let mut map = HeaderMap::new();
        for (k, v) in headers {
            let name = HeaderName::from_bytes(k.as_bytes())
                .map_err(|e| FaucetError::Config(format!("file source: header {k:?}: {e}")))?;
            let value = HeaderValue::from_str(v)
                .map_err(|e| FaucetError::Config(format!("file source: header {k:?}: {e}")))?;
            map.insert(name, value);
        }
        let client = reqwest::Client::builder()
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
        self.send(reqwest::Method::GET, url, "get", slot).await
    }

    /// `HEAD` the file for its modification time.
    pub async fn head(&self, url: &str, slot: &RecorderSlot) -> Result<HttpMeta, FaucetError> {
        let resp = self.send(reqwest::Method::HEAD, url, "head", slot).await?;
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
    ) -> Result<reqwest::Response, FaucetError> {
        let mut attempt = 0u32;
        loop {
            slot.record(op);
            let result = self
                .client
                .request(method.clone(), url)
                .headers(self.headers.clone())
                .send()
                .await;
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
                    let class = if e.is_timeout() {
                        RetryClass::Timeout
                    } else {
                        RetryClass::Connection
                    };
                    (class, backoff(attempt), e.to_string())
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
                tokio::time::sleep(wait).await;
            } else {
                tokio::time::sleep(wait).await;
            }
            attempt += 1;
        }
    }
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
