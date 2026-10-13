//! Exponential backoff retry executor with jitter, over
//! [`CallContext`] (#880).

use faucet_core::FaucetError;
use faucet_core::io::CallContext;
use faucet_core::observability::RoundtripRecorder;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

/// Hard cap on *consecutive* `RateLimited` (HTTP 429) responses before the
/// executor gives up. A single transient 429 with a `Retry-After` is honoured
/// and retried (not counting against `max_retries`), but a perpetually
/// rate-limited endpoint must not loop forever (#78/#14). The counter resets
/// whenever any non-429 outcome occurs.
pub(crate) const MAX_CONSECUTIVE_RATE_LIMITS: u32 = 10;

/// Execute an async operation with exponential backoff retries and jitter.
///
/// - **`RateLimited`** errors sleep for the server-specified `Retry-After`
///   duration and do **not** consume a `max_retries` slot, but are bounded by
///   `MAX_CONSECUTIVE_RATE_LIMITS` consecutive occurrences so a permanently
///   throttled endpoint surfaces the `RateLimited` error instead of hanging.
/// - **Retriable** errors (5xx, connection/timeout) use exponential backoff
///   with random jitter and count toward `max_retries`.
/// - **Non-retriable** errors (4xx except 429, parse errors, etc.) fail
///   immediately without retrying.
pub async fn execute_with_retry<F, Fut, T>(
    max_retries: u32,
    base_backoff: Duration,
    operation: F,
) -> Result<T, FaucetError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, FaucetError>>,
{
    execute_with_retry_recorded(max_retries, base_backoff, None, operation).await
}

/// [`execute_with_retry`] that also reports throttling to the connector's
/// round-trip recorder (#734): every rate-limited response as
/// `faucet_source_throttled_total`, every retry by class as
/// `faucet_source_retries_total`, and the time actually slept on a rate limit
/// as `faucet_source_throttle_wait_seconds` (measured, not the `Retry-After`
/// value; a sleep interrupted by the run being dropped records what elapsed).
/// Round trips themselves are not counted here — the operation counts its own.
pub async fn execute_with_retry_recorded<F, Fut, T>(
    max_retries: u32,
    base_backoff: Duration,
    recorder: Option<Arc<RoundtripRecorder>>,
    operation: F,
) -> Result<T, FaucetError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, FaucetError>>,
{
    let mut ctx = CallContext::new()
        .without_timeout()
        .with_retry(faucet_core::RetryPolicy {
            max_attempts: max_retries.saturating_add(1),
            backoff: faucet_core::BackoffKind::Exponential,
            base: base_backoff,
            max: Duration::from_secs(60),
            jitter: true,
            retry_on: faucet_core::RetryClassSet::default(),
        })
        .with_rate_limit_retries(MAX_CONSECUTIVE_RATE_LIMITS)
        .with_roundtrip_metering(false);
    if let Some(r) = recorder {
        ctx = ctx.with_recorder(r);
    }
    ctx.call("request", operation).await
}

// Backoff + jitter now lives in `faucet_core::retry::backoff_with_jitter` (capped
// at 60s, range-correct [0.5,1.5), decorrelated across concurrent retries). This
// module keeps only the 429/`Retry-After`-aware loop and delegates the sleep
// computation, so the jitter bug fixed in core can never diverge in a copy here.

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::FaucetError;

    // Backoff/jitter behaviour (range, cap, decorrelation) is tested in
    // `faucet_core::retry`; this module now delegates to it. The tests below
    // exercise the 429/`Retry-After`-aware retry loop that is unique to REST.

    #[tokio::test]
    async fn execute_with_retry_success_on_first_try() {
        let result = execute_with_retry(3, Duration::from_millis(1), || async {
            Ok::<_, FaucetError>(42)
        })
        .await;
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn execute_with_retry_non_retriable_fails_immediately() {
        let call_count = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let cc = call_count.clone();

        let result = execute_with_retry(3, Duration::from_millis(1), move || {
            cc.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            async { Err::<i32, _>(FaucetError::Auth("bad credentials".into())) }
        })
        .await;

        assert!(result.is_err());
        // Non-retriable error: should be called exactly once.
        assert_eq!(call_count.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn execute_with_retry_retriable_exhausts_retries() {
        let call_count = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let cc = call_count.clone();

        let result = execute_with_retry(2, Duration::from_millis(1), move || {
            cc.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            async {
                Err::<i32, _>(FaucetError::HttpStatus {
                    status: 500,
                    url: "http://test".into(),
                    body: "error".into(),
                })
            }
        })
        .await;

        assert!(result.is_err());
        // Initial attempt + 2 retries = 3 total calls.
        assert_eq!(call_count.load(std::sync::atomic::Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn execute_with_retry_succeeds_after_transient_failure() {
        let call_count = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let cc = call_count.clone();

        let result = execute_with_retry(3, Duration::from_millis(1), move || {
            let count = cc.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            async move {
                if count < 2 {
                    Err::<i32, _>(FaucetError::HttpStatus {
                        status: 502,
                        url: "http://test".into(),
                        body: "bad gateway".into(),
                    })
                } else {
                    Ok(99)
                }
            }
        })
        .await;

        assert_eq!(result.unwrap(), 99);
        assert_eq!(call_count.load(std::sync::atomic::Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn execute_with_retry_rate_limited_does_not_count_as_attempt() {
        let call_count = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let cc = call_count.clone();

        let result = execute_with_retry(
            0, // zero retries allowed
            Duration::from_millis(1),
            move || {
                let count = cc.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                async move {
                    if count == 0 {
                        Err::<i32, _>(FaucetError::RateLimited(Duration::from_millis(1)))
                    } else {
                        Ok(42)
                    }
                }
            },
        )
        .await;

        // Even with 0 retries, RateLimited should retry (doesn't consume a slot).
        assert_eq!(result.unwrap(), 42);
        assert_eq!(call_count.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn execute_with_retry_perpetual_rate_limit_is_bounded() {
        // Regression for #78/#14: a permanently rate-limited endpoint must not
        // loop forever. After MAX_CONSECUTIVE_RATE_LIMITS retries the executor
        // surfaces the RateLimited error.
        let call_count = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let cc = call_count.clone();

        let result = execute_with_retry(3, Duration::from_millis(1), move || {
            cc.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            async { Err::<i32, _>(FaucetError::RateLimited(Duration::from_millis(1))) }
        })
        .await;

        assert!(matches!(result, Err(FaucetError::RateLimited(_))));
        // 1 initial call + MAX_CONSECUTIVE_RATE_LIMITS retries, then it gives up.
        assert_eq!(
            call_count.load(std::sync::atomic::Ordering::SeqCst),
            MAX_CONSECUTIVE_RATE_LIMITS + 1
        );
    }

    #[tokio::test]
    async fn recorded_executor_meters_every_rate_limit_and_retry() {
        use faucet_core::observability::RoundtripSide;
        use faucet_core::usage::UsageMeter;
        use std::sync::atomic::{AtomicU32, Ordering};

        let meter = Arc::new(UsageMeter::new());
        let rec = Arc::new(
            RoundtripRecorder::new(RoundtripSide::Source, "p", "r", "rest")
                .with_meter(meter.clone()),
        );
        let calls = Arc::new(AtomicU32::new(0));
        let c = calls.clone();
        let result = execute_with_retry_recorded(
            3,
            Duration::from_millis(10),
            Some(rec.clone()),
            move || {
                let n = c.fetch_add(1, Ordering::SeqCst);
                async move {
                    match n {
                        0 => Err::<i32, _>(FaucetError::RateLimited(Duration::from_millis(30))),
                        1 => Err(FaucetError::HttpStatus {
                            status: 429,
                            url: "u".into(),
                            body: String::new(),
                        }),
                        2 => Err(FaucetError::HttpStatus {
                            status: 502,
                            url: "u".into(),
                            body: String::new(),
                        }),
                        _ => Ok(1),
                    }
                }
            },
        )
        .await;
        assert_eq!(result.unwrap(), 1);
        let tally = rec.throttle_tally();
        assert_eq!(
            tally.throttled(),
            2,
            "a RateLimited error and a bare 429 both count"
        );
        assert!(
            tally.wait() >= Duration::from_millis(30),
            "{:?}",
            tally.wait()
        );
        let usage = meter.snapshot();
        assert_eq!(usage.throttled, 2);
        assert_eq!(usage.source_retries["rate_limited"], 2);
        assert_eq!(usage.source_retries["http_5xx"], 1);

        let gave_up =
            execute_with_retry_recorded(0, Duration::from_millis(1), Some(rec.clone()), || async {
                Err::<i32, _>(FaucetError::HttpStatus {
                    status: 429,
                    url: "u".into(),
                    body: String::new(),
                })
            })
            .await;
        assert!(gave_up.is_err());
        assert_eq!(tally.throttled(), 3, "a final, unretried 429 still counts");
    }
}
