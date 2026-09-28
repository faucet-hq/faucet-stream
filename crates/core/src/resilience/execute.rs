//! The retry runner: executes a fallible async op under a [`RetryPolicy`],
//! honoring backoff, jitter, retry classification, and a cancellation token.

use crate::error::FaucetError;
use crate::observability::{RoundtripRecorder, ThrottleWait};
use crate::resilience::classify::{RetryClass, classify};
use crate::resilience::policy::RetryPolicy;
use std::future::Future;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// Per-op labels for the metered runner. Identifies which pipeline / matrix row
/// / I/O operation a retry belongs to so the resilience metrics
/// (`faucet_resilience_retries_total` / `_retry_sleep_seconds` / `_giveup_total`)
/// carry the spec's `{pipeline, row, op}` label set. `op` is a closed set
/// (`sink_write` / `flush` / `state_put` / `source`).
#[derive(Debug, Clone)]
pub struct RetryMetrics {
    /// Pipeline name label.
    pub pipeline: String,
    /// Matrix row id label (`""` for non-matrix runs).
    pub row: String,
    /// Operation label — a `&'static str` from the closed op set.
    pub op: &'static str,
}

/// Execute `op` under `policy`. Retries retriable errors (per
/// [`RetryPolicy::is_retriable`]) up to `max_attempts` total attempts, sleeping
/// the backoff delay between attempts. If `cancel` is provided, a cancellation
/// during a backoff sleep stops waiting and returns the last error immediately
/// (the caller observes the token and flushes). `Ok` returns at once.
///
/// This is the connector-facing, label-free runner used by source connectors;
/// the pipeline loop uses [`execute_with_policy_metered`] so the resilience
/// metrics get their `{pipeline, row, op}` labels.
pub async fn execute_with_policy<F, Fut, T>(
    policy: &RetryPolicy,
    cancel: Option<&CancellationToken>,
    op: F,
) -> Result<T, FaucetError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, FaucetError>>,
{
    run_with_policy(policy, cancel, None, None, op).await
}

/// Like [`execute_with_policy`], but reports source-side throttling to a
/// connector's [`RoundtripRecorder`] (#734): every rate-limited error as
/// `faucet_source_throttled_total`, each retry as
/// `faucet_source_retries_total{class}`, and the time actually slept after a
/// rate-limited attempt as `faucet_source_throttle_wait_seconds` (a sleep cut
/// short by `cancel` records the partial wait). `None` behaves exactly like
/// [`execute_with_policy`].
///
/// [`RoundtripRecorder`]: crate::observability::RoundtripRecorder
pub async fn execute_with_policy_recorded<F, Fut, T>(
    policy: &RetryPolicy,
    cancel: Option<&CancellationToken>,
    recorder: Option<&Arc<RoundtripRecorder>>,
    op: F,
) -> Result<T, FaucetError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, FaucetError>>,
{
    run_with_policy(policy, cancel, None, recorder, op).await
}

/// Like [`execute_with_policy`], but emits the resilience metrics
/// (`faucet_resilience_retries_total{op,class}`,
/// `faucet_resilience_retry_sleep_seconds{op}`, and
/// `faucet_resilience_giveup_total{op}` when retries are exhausted) using the
/// labels in `metrics`. Behavior (retry/give-up/cancel outcomes) is identical to
/// `execute_with_policy` — only the metric emission differs.
pub async fn execute_with_policy_metered<F, Fut, T>(
    policy: &RetryPolicy,
    cancel: Option<&CancellationToken>,
    metrics: &RetryMetrics,
    op: F,
) -> Result<T, FaucetError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, FaucetError>>,
{
    run_with_policy(policy, cancel, Some(metrics), None, op).await
}

/// Shared retry loop backing both the bare and metered public runners. When
/// `metrics` is `Some`, emits per-attempt retry / sleep / give-up metrics; when
/// `None`, the metric calls are skipped entirely (zero overhead on the
/// connector-facing path).
async fn run_with_policy<F, Fut, T>(
    policy: &RetryPolicy,
    cancel: Option<&CancellationToken>,
    metrics: Option<&RetryMetrics>,
    recorder: Option<&Arc<RoundtripRecorder>>,
    mut op: F,
) -> Result<T, FaucetError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, FaucetError>>,
{
    // `max_attempts` is documented as "total attempts including the first
    // (1 = no retry)". A caller-supplied `0` is meaningless — the loop always
    // runs `op()` at least once before consulting the guard — so clamp it to 1
    // rather than letting `attempt + 1 < 0` (which underflows to a large value
    // and would retry) or `< 0` decide. Keeps the public `RetryPolicy` contract
    // honest for library callers (F49).
    let max_attempts = policy.max_attempts.max(1);
    let mut attempt = 0u32;
    loop {
        let outcome = op().await;
        let class = outcome.as_ref().err().and_then(classify);
        if class == Some(RetryClass::RateLimited)
            && let Some(r) = recorder
        {
            r.throttled();
        }
        match outcome {
            Ok(val) => return Ok(val),
            Err(e) if policy.is_retriable(&e) && attempt + 1 < max_attempts => {
                let wait = match &e {
                    FaucetError::RateLimited(stated) => *stated,
                    _ => {
                        let base = policy.backoff.delay(policy.base, policy.max, attempt);
                        if policy.jitter {
                            crate::retry::apply_jitter(base)
                        } else {
                            base
                        }
                    }
                };
                tracing::warn!(
                    "operation failed (attempt {}/{}), retrying in {wait:?}: {e}",
                    attempt + 1,
                    max_attempts
                );
                if let Some(m) = metrics {
                    // `is_retriable` above guarantees a class; fall back defensively.
                    let class = class.map(|c| c.as_str()).unwrap_or("unknown");
                    crate::observability::resilience::retry(&m.pipeline, &m.row, m.op, class);
                    crate::observability::resilience::retry_sleep(
                        &m.pipeline,
                        &m.row,
                        m.op,
                        wait.as_secs_f64(),
                    );
                }
                if let (Some(r), Some(c)) = (recorder, class) {
                    r.retry(c);
                }
                let _throttle_timer = (class == Some(RetryClass::RateLimited))
                    .then(|| ThrottleWait::start(recorder.cloned()));
                if !wait.is_zero() {
                    match cancel {
                        Some(token) => {
                            tokio::select! {
                                biased;
                                _ = token.cancelled() => {
                                    // Cancelled mid-backoff: the op was retriable but
                                    // we are abandoning it — count as a give-up.
                                    if let Some(m) = metrics {
                                        crate::observability::resilience::giveup(
                                            &m.pipeline, &m.row, m.op,
                                        );
                                    }
                                    return Err(e);
                                }
                                _ = tokio::time::sleep(wait) => {}
                            }
                        }
                        None => tokio::time::sleep(wait).await,
                    }
                }
                attempt += 1;
            }
            Err(e) => {
                // Retries exhausted (or a non-retriable error). Only count a
                // give-up when at least one retry was attempted — a first-attempt
                // non-retriable failure is not an exhausted budget.
                if attempt > 0
                    && let Some(m) = metrics
                {
                    crate::observability::resilience::giveup(&m.pipeline, &m.row, m.op);
                }
                return Err(e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resilience::policy::BackoffKind;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;

    fn fast_policy(max_attempts: u32) -> RetryPolicy {
        RetryPolicy {
            max_attempts,
            backoff: BackoffKind::None,
            base: Duration::from_millis(0),
            max: Duration::from_millis(0),
            jitter: false,
            ..RetryPolicy::default()
        }
    }

    #[tokio::test]
    async fn retries_then_succeeds() {
        let calls = Arc::new(AtomicU32::new(0));
        let c = calls.clone();
        let r = execute_with_policy(&fast_policy(5), None, move || {
            let n = c.fetch_add(1, Ordering::SeqCst);
            async move {
                if n < 2 {
                    Err::<i32, _>(FaucetError::HttpStatus {
                        status: 503,
                        url: "u".into(),
                        body: "".into(),
                    })
                } else {
                    Ok(42)
                }
            }
        })
        .await;
        assert_eq!(r.unwrap(), 42);
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn gives_up_after_max_attempts() {
        let calls = Arc::new(AtomicU32::new(0));
        let c = calls.clone();
        let r = execute_with_policy(&fast_policy(3), None, move || {
            c.fetch_add(1, Ordering::SeqCst);
            async {
                Err::<i32, _>(FaucetError::HttpStatus {
                    status: 503,
                    url: "u".into(),
                    body: "".into(),
                })
            }
        })
        .await;
        assert!(r.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 3, "1 initial + 2 retries");
    }

    #[tokio::test]
    async fn max_attempts_zero_runs_op_exactly_once() {
        // The public `RetryPolicy` contract documents `max_attempts` as "total
        // attempts including the first (1 = no retry)". A misconfigured `0` is
        // clamped to 1: the op runs exactly once and a retriable error is NOT
        // retried (F49).
        let calls = Arc::new(AtomicU32::new(0));
        let c = calls.clone();
        let r = execute_with_policy(&fast_policy(0), None, move || {
            c.fetch_add(1, Ordering::SeqCst);
            async {
                Err::<i32, _>(FaucetError::HttpStatus {
                    status: 503,
                    url: "u".into(),
                    body: "".into(),
                })
            }
        })
        .await;
        assert!(r.is_err());
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "max_attempts=0 clamps to 1: one execution, no retry"
        );
    }

    #[tokio::test]
    async fn non_retriable_fails_immediately() {
        let calls = Arc::new(AtomicU32::new(0));
        let c = calls.clone();
        let r = execute_with_policy(&fast_policy(5), None, move || {
            c.fetch_add(1, Ordering::SeqCst);
            async { Err::<i32, _>(FaucetError::Auth("nope".into())) }
        })
        .await;
        assert!(r.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn jittered_nonzero_backoff_with_no_cancel_retries() {
        // Exercises the `jitter: true` + non-zero `Fixed` backoff + `None` cancel
        // arms directly (the zero-backoff `fast_policy` skips both). Backoff is a
        // few ms so the test stays fast while still sleeping a real interval.
        let policy = RetryPolicy {
            max_attempts: 3,
            backoff: BackoffKind::Fixed,
            base: Duration::from_millis(2),
            max: Duration::from_millis(2),
            jitter: true,
            ..RetryPolicy::default()
        };
        let calls = Arc::new(AtomicU32::new(0));
        let c = calls.clone();
        let r = execute_with_policy(&policy, None, move || {
            let n = c.fetch_add(1, Ordering::SeqCst);
            async move {
                if n < 1 {
                    Err::<i32, _>(FaucetError::HttpStatus {
                        status: 503,
                        url: "u".into(),
                        body: "".into(),
                    })
                } else {
                    Ok(9)
                }
            }
        })
        .await;
        assert_eq!(r.unwrap(), 9);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn cancel_during_backoff_returns_last_error_promptly() {
        let policy = RetryPolicy {
            max_attempts: 10,
            backoff: BackoffKind::Fixed,
            base: Duration::from_secs(30),
            max: Duration::from_secs(30),
            jitter: false,
            ..RetryPolicy::default()
        };
        let token = CancellationToken::new();
        token.cancel(); // already cancelled → backoff sleep returns at once
        let r = execute_with_policy(&policy, Some(&token), || async {
            Err::<i32, _>(FaucetError::HttpStatus {
                status: 503,
                url: "u".into(),
                body: "".into(),
            })
        })
        .await;
        assert!(r.is_err());
    }

    fn metrics() -> RetryMetrics {
        RetryMetrics {
            pipeline: "p".into(),
            row: "r".into(),
            op: "sink_write",
        }
    }

    // The metered runner must produce byte-for-byte identical retry / give-up /
    // cancel outcomes to the bare runner — only the metric emission differs (the
    // emits are no-ops without a recorder, so these assert behavior).

    #[tokio::test]
    async fn metered_retries_then_succeeds_same_as_bare() {
        let calls = Arc::new(AtomicU32::new(0));
        let c = calls.clone();
        let m = metrics();
        let r = execute_with_policy_metered(&fast_policy(5), None, &m, move || {
            let n = c.fetch_add(1, Ordering::SeqCst);
            async move {
                if n < 2 {
                    Err::<i32, _>(FaucetError::HttpStatus {
                        status: 503,
                        url: "u".into(),
                        body: "".into(),
                    })
                } else {
                    Ok(42)
                }
            }
        })
        .await;
        assert_eq!(r.unwrap(), 42);
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn metered_gives_up_after_max_attempts_same_as_bare() {
        let calls = Arc::new(AtomicU32::new(0));
        let c = calls.clone();
        let m = metrics();
        let r = execute_with_policy_metered(&fast_policy(3), None, &m, move || {
            c.fetch_add(1, Ordering::SeqCst);
            async {
                Err::<i32, _>(FaucetError::HttpStatus {
                    status: 503,
                    url: "u".into(),
                    body: "".into(),
                })
            }
        })
        .await;
        assert!(r.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 3, "1 initial + 2 retries");
    }

    #[tokio::test]
    async fn metered_non_retriable_fails_immediately_same_as_bare() {
        let calls = Arc::new(AtomicU32::new(0));
        let c = calls.clone();
        let m = metrics();
        let r = execute_with_policy_metered(&fast_policy(5), None, &m, move || {
            c.fetch_add(1, Ordering::SeqCst);
            async { Err::<i32, _>(FaucetError::Auth("nope".into())) }
        })
        .await;
        assert!(r.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn metered_cancel_during_backoff_returns_promptly() {
        let policy = RetryPolicy {
            max_attempts: 10,
            backoff: BackoffKind::Fixed,
            base: Duration::from_secs(30),
            max: Duration::from_secs(30),
            jitter: false,
            ..RetryPolicy::default()
        };
        let token = CancellationToken::new();
        token.cancel();
        let m = metrics();
        let r = execute_with_policy_metered(&policy, Some(&token), &m, || async {
            Err::<i32, _>(FaucetError::HttpStatus {
                status: 503,
                url: "u".into(),
                body: "".into(),
            })
        })
        .await;
        assert!(r.is_err());
    }

    fn rate_limited() -> FaucetError {
        FaucetError::HttpStatus {
            status: 429,
            url: "u".into(),
            body: "".into(),
        }
    }

    fn recorder() -> Arc<RoundtripRecorder> {
        Arc::new(RoundtripRecorder::new(
            crate::observability::RoundtripSide::Source,
            "p",
            "r",
            "graphql",
        ))
    }

    #[tokio::test]
    async fn recorded_runner_meters_rate_limits_retries_and_the_wait() {
        let policy = RetryPolicy {
            max_attempts: 5,
            backoff: BackoffKind::Fixed,
            base: Duration::from_millis(20),
            max: Duration::from_millis(20),
            jitter: false,
            ..RetryPolicy::default()
        };
        let meter = Arc::new(crate::usage::UsageMeter::new());
        let rec = Arc::new(
            RoundtripRecorder::new(crate::observability::RoundtripSide::Source, "p", "r", "xml")
                .with_meter(meter.clone()),
        );
        let calls = Arc::new(AtomicU32::new(0));
        let c = calls.clone();
        let r = execute_with_policy_recorded(&policy, None, Some(&rec), move || {
            let n = c.fetch_add(1, Ordering::SeqCst);
            async move {
                match n {
                    0 | 1 => Err::<i32, _>(rate_limited()),
                    2 => Err(FaucetError::HttpStatus {
                        status: 503,
                        url: "u".into(),
                        body: "".into(),
                    }),
                    _ => Ok(7),
                }
            }
        })
        .await;
        assert_eq!(r.unwrap(), 7);
        let tally = rec.throttle_tally();
        assert_eq!(tally.throttled(), 2);
        assert!(
            tally.wait() >= Duration::from_millis(40),
            "only the two rate-limit sleeps count: {:?}",
            tally.wait()
        );
        assert!(tally.wait() < Duration::from_millis(60) + Duration::from_secs(1));
        let usage = meter.snapshot();
        assert_eq!(usage.source_retries["rate_limited"], 2);
        assert_eq!(usage.source_retries["http_5xx"], 1);
    }

    #[tokio::test]
    async fn a_stated_rate_limit_wait_replaces_the_policy_backoff() {
        let policy = RetryPolicy {
            max_attempts: 3,
            backoff: BackoffKind::Fixed,
            base: Duration::from_millis(1),
            max: Duration::from_millis(1),
            jitter: false,
            ..RetryPolicy::default()
        };
        let calls = Arc::new(AtomicU32::new(0));
        let c = calls.clone();
        let started = std::time::Instant::now();
        let r = execute_with_policy(&policy, None, move || {
            let n = c.fetch_add(1, Ordering::SeqCst);
            async move {
                if n == 0 {
                    Err::<i32, _>(FaucetError::RateLimited(Duration::from_millis(80)))
                } else {
                    Ok(1)
                }
            }
        })
        .await;
        assert_eq!(r.unwrap(), 1);
        assert!(started.elapsed() >= Duration::from_millis(80));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn recorded_runner_counts_a_final_rate_limit_it_does_not_retry() {
        let rec = recorder();
        let r = execute_with_policy_recorded(&fast_policy(1), None, Some(&rec), || async {
            Err::<i32, _>(rate_limited())
        })
        .await;
        assert!(r.is_err());
        assert_eq!(rec.throttle_tally().throttled(), 1);
        assert_eq!(rec.throttle_tally().wait(), Duration::ZERO);
        let r = execute_with_policy_recorded(&fast_policy(3), None, None, || async {
            Err::<i32, _>(rate_limited())
        })
        .await;
        assert!(r.is_err());
    }

    #[tokio::test]
    async fn recorded_runner_records_a_partial_wait_on_cancellation() {
        let policy = RetryPolicy {
            max_attempts: 10,
            backoff: BackoffKind::Fixed,
            base: Duration::from_secs(30),
            max: Duration::from_secs(30),
            jitter: false,
            ..RetryPolicy::default()
        };
        let rec = recorder();
        let token = CancellationToken::new();
        let t = token.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            t.cancel();
        });
        let r = execute_with_policy_recorded(&policy, Some(&token), Some(&rec), || async {
            Err::<i32, _>(rate_limited())
        })
        .await;
        assert!(r.is_err());
        let wait = rec.throttle_tally().wait();
        assert!(
            wait >= Duration::from_millis(25) && wait < Duration::from_secs(5),
            "{wait:?}"
        );
    }
}
