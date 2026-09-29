//! Configurable retriable-response matchers (#756), shared with the graphql
//! source through `faucet_core::resilience::response_matcher`.

pub use faucet_core::resilience::response_matcher::{
    BackoffFrom, DEFAULT_MAX_WAIT_SECS, RetryMatcher, WaitUnit, find_match,
};
use std::time::Duration;

/// The wait before retrying a matched response when there is no response to
/// read `backoff_from` from: `backoff_secs`, else `Retry-After`, else
/// exponential backoff. Use [`RetryMatcher::wait`] to honour `backoff_from`.
pub fn matched_wait(
    matcher: &RetryMatcher,
    retry_after: Option<Duration>,
    base: Duration,
    attempt: u32,
) -> Duration {
    matcher
        .backoff_secs
        .map(Duration::from_secs)
        .or(retry_after)
        .unwrap_or_else(|| faucet_core::retry::backoff_with_jitter(base, attempt))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn wait_precedence() {
        let mut m: RetryMatcher =
            serde_json::from_value(json!({"status": [400], "body_path": "$.a", "values": [1]}))
                .unwrap();
        let base = Duration::from_millis(100);
        assert_eq!(
            matched_wait(&m, Some(Duration::from_secs(3)), base, 0),
            Duration::from_secs(3)
        );
        let exp = matched_wait(&m, None, base, 0);
        assert!(exp >= Duration::from_millis(50) && exp <= Duration::from_millis(150));
        m.backoff_secs = Some(60);
        assert_eq!(
            matched_wait(&m, Some(Duration::from_secs(3)), base, 0),
            Duration::from_secs(60)
        );
    }
}
