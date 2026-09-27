//! Throttle-aware retry shared by the DynamoDB source and sink.
//!
//! DynamoDB signals capacity pressure with typed error codes
//! (`ProvisionedThroughputExceededException`, `ThrottlingException`,
//! `RequestLimitExceeded`, Streams' `LimitExceededException`). Those, plus
//! transient server/transport failures, are retried with jittered exponential
//! backoff; anything else (validation, missing table, access denied) fails
//! immediately.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::time::Duration;

/// How a failed request should be treated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorClass {
    /// Capacity throttling — back off and retry.
    Throttle,
    /// Transient server or transport failure — retry.
    Transient,
    /// Anything else — fail now.
    Fatal,
}

/// Classify an AWS error code. `None` (no service error — a dispatch,
/// timeout or transport failure) is transient.
pub fn classify_error(code: Option<&str>) -> ErrorClass {
    match code {
        None => ErrorClass::Transient,
        Some(
            "ProvisionedThroughputExceededException"
            | "ThrottlingException"
            | "Throttling"
            | "RequestLimitExceeded"
            | "LimitExceededException",
        ) => ErrorClass::Throttle,
        Some(
            "InternalServerError"
            | "InternalFailure"
            | "ServiceUnavailable"
            | "ServiceUnavailableException"
            | "TransactionInProgressException",
        ) => ErrorClass::Transient,
        Some(_) => ErrorClass::Fatal,
    }
}

/// The code + human message of an SDK error.
pub fn sdk_error_parts<E, R>(
    err: &aws_sdk_dynamodb::error::SdkError<E, R>,
) -> (Option<String>, String)
where
    E: aws_sdk_dynamodb::error::ProvideErrorMetadata + std::error::Error + 'static,
    R: std::fmt::Debug,
{
    use aws_sdk_dynamodb::error::ProvideErrorMetadata as _;
    let code = err.code().map(str::to_string);
    let message = aws_sdk_dynamodb::error::DisplayErrorContext(err).to_string();
    (code, message)
}

/// Retry/backoff settings exposed on both connector configs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RetryPolicy {
    /// Retries after the first attempt for throttled or transient failures.
    /// Default 8.
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    /// Initial backoff in milliseconds (doubled per attempt, jittered).
    /// Default 100.
    #[serde(default = "default_base_ms")]
    pub initial_backoff_ms: u64,
    /// Backoff ceiling in milliseconds. Default 10000.
    #[serde(default = "default_max_ms")]
    pub max_backoff_ms: u64,
}

fn default_max_retries() -> u32 {
    8
}
fn default_base_ms() -> u64 {
    100
}
fn default_max_ms() -> u64 {
    10_000
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: default_max_retries(),
            initial_backoff_ms: default_base_ms(),
            max_backoff_ms: default_max_ms(),
        }
    }
}

impl RetryPolicy {
    /// Jittered delay before retry number `attempt` (0-based).
    pub fn delay(&self, attempt: u32) -> Duration {
        let exp = self
            .initial_backoff_ms
            .saturating_mul(1u64 << attempt.min(20))
            .min(self.max_backoff_ms.max(1));
        faucet_core::retry::apply_jitter(Duration::from_millis(exp.max(1)))
    }

    /// Run `op` until it succeeds, a fatal error occurs, or the retry budget is
    /// spent. `op` returns the error's `(code, message)`; `what` names the call
    /// in the final error message (the caller picks the `FaucetError` variant).
    pub async fn run<T, F, Fut>(&self, what: &str, mut op: F) -> Result<T, String>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, (Option<String>, String)>>,
    {
        let mut attempt = 0u32;
        loop {
            match op().await {
                Ok(v) => return Ok(v),
                Err((code, message)) => {
                    let class = classify_error(code.as_deref());
                    if class == ErrorClass::Fatal || attempt >= self.max_retries {
                        return Err(format!(
                            "dynamodb: {what} failed after {} attempt(s): {message}",
                            attempt + 1
                        ));
                    }
                    let delay = self.delay(attempt);
                    tracing::debug!(what, ?class, attempt, delay_ms = delay.as_millis() as u64,
                        error = %message, "dynamodb: retrying");
                    tokio::time::sleep(delay).await;
                    attempt += 1;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[test]
    fn classification() {
        assert_eq!(classify_error(None), ErrorClass::Transient);
        for c in [
            "ProvisionedThroughputExceededException",
            "ThrottlingException",
            "Throttling",
            "RequestLimitExceeded",
            "LimitExceededException",
        ] {
            assert_eq!(classify_error(Some(c)), ErrorClass::Throttle, "{c}");
        }
        for c in [
            "InternalServerError",
            "InternalFailure",
            "ServiceUnavailable",
            "ServiceUnavailableException",
            "TransactionInProgressException",
        ] {
            assert_eq!(classify_error(Some(c)), ErrorClass::Transient, "{c}");
        }
        assert_eq!(
            classify_error(Some("ValidationException")),
            ErrorClass::Fatal
        );
    }

    #[test]
    fn delay_doubles_and_caps() {
        let p = RetryPolicy {
            max_retries: 3,
            initial_backoff_ms: 100,
            max_backoff_ms: 400,
        };
        let d0 = p.delay(0);
        assert!(d0 >= Duration::from_millis(50) && d0 < Duration::from_millis(150));
        let d9 = p.delay(9);
        assert!(d9 >= Duration::from_millis(200) && d9 < Duration::from_millis(600));
        let zero = RetryPolicy {
            max_retries: 0,
            initial_backoff_ms: 0,
            max_backoff_ms: 0,
        };
        assert!(!zero.delay(u32::MAX).is_zero());
        assert_eq!(RetryPolicy::default().max_retries, 8);
    }

    fn fast(max_retries: u32) -> RetryPolicy {
        RetryPolicy {
            max_retries,
            initial_backoff_ms: 1,
            max_backoff_ms: 2,
        }
    }

    #[tokio::test]
    async fn run_retries_throttles_then_succeeds() {
        let calls = AtomicU32::new(0);
        let out = fast(3)
            .run("scan", || async {
                if calls.fetch_add(1, Ordering::SeqCst) < 2 {
                    Err((Some("ThrottlingException".to_string()), "slow".to_string()))
                } else {
                    Ok(7)
                }
            })
            .await
            .unwrap();
        assert_eq!(out, 7);
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn run_stops_on_fatal_and_on_budget() {
        let calls = AtomicU32::new(0);
        let err = fast(5)
            .run::<(), _, _>("scan", || async {
                calls.fetch_add(1, Ordering::SeqCst);
                Err((Some("ValidationException".to_string()), "bad".to_string()))
            })
            .await
            .unwrap_err();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(
            err.to_string().contains("scan failed after 1 attempt"),
            "{err}"
        );

        let calls = AtomicU32::new(0);
        let err = fast(2)
            .run::<(), _, _>("query", || async {
                calls.fetch_add(1, Ordering::SeqCst);
                Err((None, "conn refused".to_string()))
            })
            .await
            .unwrap_err();
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        assert!(err.to_string().contains("conn refused"), "{err}");
    }

    #[test]
    fn policy_parses_with_defaults() {
        let p: RetryPolicy = serde_json::from_value(serde_json::json!({"max_retries": 2})).unwrap();
        assert_eq!(p.max_retries, 2);
        assert_eq!(p.initial_backoff_ms, 100);
        assert_eq!(p.max_backoff_ms, 10_000);
    }
}
