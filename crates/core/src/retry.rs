//! Shared exponential-backoff retry executor for HTTP-style sources.
//!
//! Connector authors can wrap any fallible async operation so transient
//! failures (5xx, connection resets, timeouts — see
//! [`FaucetError::is_retriable`]) are retried with jittered backoff, while
//! non-retriable errors fail fast. Used by the XML, GraphQL, and other
//! sources that talk to flaky HTTP endpoints.

use crate::error::FaucetError;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Hard cap on a single backoff sleep (before jitter). Bounds the exponential so
/// a large `max_retries`/`attempt` can't produce multi-hour — or, once
/// `2^attempt` saturates, effectively unbounded — sleeps. The jitter factor
/// (`< 1.5`) keeps the realised sleep under ~1.5× this.
const MAX_BACKOFF: Duration = Duration::from_secs(60);

/// Execute `operation` with up to `max_retries` retries on
/// [retriable](FaucetError::is_retriable) errors, using exponential backoff
/// (`base_backoff * 2^attempt`) with random jitter. Non-retriable errors
/// return immediately; `Ok` returns immediately.
pub async fn execute_with_retry<F, Fut, T>(
    max_retries: u32,
    base_backoff: Duration,
    operation: F,
) -> Result<T, FaucetError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, FaucetError>>,
{
    let policy = crate::resilience::RetryPolicy {
        // max_attempts is total tries; legacy `max_retries` is retries-after-first.
        max_attempts: max_retries.saturating_add(1),
        backoff: crate::resilience::BackoffKind::Exponential,
        base: base_backoff,
        max: MAX_BACKOFF,
        jitter: true,
        retry_on: crate::resilience::RetryClassSet::default(),
    };
    crate::resilience::execute_with_policy(&policy, None, operation).await
}

/// `base * 2^attempt`, capped at `MAX_BACKOFF` (60s), scaled by a random factor
/// in `[0.5, 1.5)` to avoid a thundering herd.
///
/// Public so a connector with a bespoke retry loop (e.g. one that also honours
/// `Retry-After`, like the REST source) reuses this one tested, capped,
/// decorrelated backoff instead of re-implementing jitter — which is exactly
/// how the range-bias / no-cap bugs crept into a copy.
pub fn backoff_with_jitter(base: Duration, attempt: u32) -> Duration {
    let exp = base
        .saturating_mul(2u32.saturating_pow(attempt))
        .min(MAX_BACKOFF);
    let nanos = exp.as_nanos() as u64;
    Duration::from_nanos((nanos as f64 * pseudo_random_factor()) as u64)
}

/// Cheap, non-cryptographic random factor in `[0.5, 1.5)`.
fn pseudo_random_factor() -> f64 {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
    jitter_factor(decorrelate(nanos, counter))
}

/// Mix the clock's sub-second component with a monotonic per-call counter so two
/// retries firing in the *same* nanosecond (across tasks/connectors sharing the
/// process) still draw different jitter — otherwise concurrent retries align and
/// re-create the very thundering herd the jitter exists to break. Returns a
/// value in `[0, 1_000_000_000)` for [`jitter_factor`]. splitmix64 finaliser:
/// fast, non-cryptographic, well-distributed.
fn decorrelate(nanos: u32, counter: u64) -> u32 {
    let mut x = (nanos as u64) ^ counter.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    x ^= x >> 30;
    x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^= x >> 31;
    (x % 1_000_000_000) as u32
}

/// Map a sub-second nanosecond count (`[0, 1_000_000_000)`) to a jitter factor
/// in `[0.5, 1.5)`. `subsec_nanos()` is bounded by 1e9, so the divisor must be
/// 1e9 — not `u32::MAX` (~4.29e9), which would cap the factor at ~0.733 and
/// make every backoff shorter than documented.
fn jitter_factor(nanos: u32) -> f64 {
    0.5 + (nanos as f64 / 1_000_000_000.0)
}

/// Apply the same `[0.5, 1.5)` decorrelated jitter used by [`backoff_with_jitter`]
/// to an already-computed delay. Used by the resilience runner, which computes
/// the base delay from its own [`BackoffKind`](crate::resilience::BackoffKind).
///
/// **Public so connectors never have to re-derive it.** A connector that must
/// compute its own base delay (a protocol-specific cadence, a server-supplied
/// hint) should still jitter through this function rather than hand-rolling
/// it: the copies that existed before it was exported all missed the
/// range-bias and same-nanosecond-alignment fixes made here, so concurrent
/// workers retried in lockstep.
pub fn apply_jitter(delay: std::time::Duration) -> std::time::Duration {
    let nanos = delay.as_nanos() as u64;
    std::time::Duration::from_nanos((nanos as f64 * pseudo_random_factor()) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[tokio::test]
    async fn returns_immediately_on_success() {
        let calls = Arc::new(AtomicU32::new(0));
        let c = calls.clone();
        let r = execute_with_retry(3, Duration::from_millis(1), move || {
            c.fetch_add(1, Ordering::SeqCst);
            async { Ok::<_, FaucetError>(7) }
        })
        .await;
        assert_eq!(r.unwrap(), 7);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn retries_then_succeeds_on_transient_5xx() {
        let calls = Arc::new(AtomicU32::new(0));
        let c = calls.clone();
        let r = execute_with_retry(3, Duration::from_millis(1), move || {
            let n = c.fetch_add(1, Ordering::SeqCst);
            async move {
                if n < 2 {
                    Err::<i32, _>(FaucetError::HttpStatus {
                        status: 503,
                        url: "http://t".into(),
                        body: "x".into(),
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

    #[test]
    fn jitter_factor_spans_documented_half_to_one_and_a_half_range() {
        // The factor must span [0.5, 1.5): 0 nanos → 0.5, the midpoint → 1.0,
        // and the maximum sub-second value → just under 1.5. A `u32::MAX`
        // divisor caps the top at ~0.733, so backoff is always too short.
        assert_eq!(jitter_factor(0), 0.5);
        let mid = jitter_factor(500_000_000);
        assert!((mid - 1.0).abs() < 1e-6, "midpoint factor was {mid}");
        let hi = jitter_factor(999_999_999);
        assert!(
            (1.4..1.5).contains(&hi),
            "factor at max sub-second nanos was {hi}, expected ~1.5"
        );
    }

    #[test]
    fn backoff_is_capped_for_large_attempt() {
        // Without a cap, `base * 2^attempt` saturates and the sleep becomes
        // effectively unbounded (multi-century). It must stay under
        // MAX_BACKOFF * max-jitter (<1.5) → < 90s for a 60s cap.
        let d = backoff_with_jitter(Duration::from_secs(1), 60);
        assert!(d < Duration::from_secs(90), "backoff not capped: {d:?}");
        // …and never collapses to zero for a non-zero base.
        assert!(
            d >= Duration::from_secs(30),
            "backoff unexpectedly tiny: {d:?}"
        );
    }

    #[test]
    fn decorrelate_diverges_for_same_nanos_concurrent_calls() {
        // Two retries observing the *same* clock nanosecond but different
        // per-call counters must draw different jitter, or concurrent retries
        // re-align into the thundering herd the jitter exists to break.
        let a = decorrelate(123_456_789, 0);
        let b = decorrelate(123_456_789, 1);
        let c = decorrelate(123_456_789, 2);
        assert_ne!(a, b);
        assert_ne!(b, c);
        assert_ne!(a, c);
        for v in [a, b, c] {
            assert!(
                v < 1_000_000_000,
                "decorrelate out of jitter_factor range: {v}"
            );
        }
    }

    #[tokio::test]
    async fn non_retriable_fails_immediately() {
        let calls = Arc::new(AtomicU32::new(0));
        let c = calls.clone();
        let r = execute_with_retry(3, Duration::from_millis(1), move || {
            c.fetch_add(1, Ordering::SeqCst);
            async { Err::<i32, _>(FaucetError::Auth("nope".into())) }
        })
        .await;
        assert!(r.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
