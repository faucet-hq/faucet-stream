//! Classify a [`FaucetError`] into a retry class, and a small set of classes
//! the user opts into via `retry_on: [...]`.

use crate::error::FaucetError;

/// The transient-failure class an error belongs to. Closed set so it can be a
/// bounded metric label and a `retry_on` config value.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum RetryClass {
    /// HTTP 5xx server error.
    #[serde(rename = "http_5xx")]
    Http5xx,
    /// HTTP 429 / rate-limit signal.
    RateLimited,
    /// Connection-level failure (DNS, refused, reset) with no HTTP status.
    Connection,
    /// Request timeout.
    Timeout,
}

impl RetryClass {
    /// Stable Prometheus label value.
    pub fn as_str(self) -> &'static str {
        match self {
            RetryClass::Http5xx => "http_5xx",
            RetryClass::RateLimited => "rate_limited",
            RetryClass::Connection => "connection",
            RetryClass::Timeout => "timeout",
        }
    }
}

/// The set of classes the user has opted into retrying. Backed by a fixed-size
/// bool array (only four variants), so it is `Copy` and allocation-free.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryClassSet([bool; 4]);

impl RetryClassSet {
    fn idx(c: RetryClass) -> usize {
        match c {
            RetryClass::Http5xx => 0,
            RetryClass::RateLimited => 1,
            RetryClass::Connection => 2,
            RetryClass::Timeout => 3,
        }
    }

    /// True when `c` is in the set.
    pub fn contains(&self, c: RetryClass) -> bool {
        self.0[Self::idx(c)]
    }
}

impl Default for RetryClassSet {
    /// All four classes — reproduces today's `FaucetError::is_retriable`.
    fn default() -> Self {
        RetryClassSet([true; 4])
    }
}

impl FromIterator<RetryClass> for RetryClassSet {
    fn from_iter<I: IntoIterator<Item = RetryClass>>(iter: I) -> Self {
        let mut set = RetryClassSet([false; 4]);
        for c in iter {
            set.0[Self::idx(c)] = true;
        }
        set
    }
}

/// Map a [`FaucetError`] to its [`RetryClass`], or `None` if it is not a
/// transient failure. The single source of truth for retriability — both the
/// policy runner and `FaucetError::is_retriable` defer to this.
pub fn classify(err: &FaucetError) -> Option<RetryClass> {
    match err {
        FaucetError::HttpStatus { status, .. } if *status == 429 => Some(RetryClass::RateLimited),
        FaucetError::HttpStatus { status, .. } if *status >= 500 => Some(RetryClass::Http5xx),
        FaucetError::RateLimited(_) => Some(RetryClass::RateLimited),
        FaucetError::Timeout { .. } => Some(RetryClass::Timeout),
        FaucetError::Http(e) => classify_transport(
            e.is_timeout(),
            e.status().map(|s| s.as_u16()),
            e.is_connect(),
        ),
        _ => None,
    }
}

/// Pure classification of a reqwest transport error from its discriminating
/// predicates. Factored out of the [`FaucetError::Http`] arm so it is unit-testable
/// without constructing a `reqwest::Error` (which has no public constructor).
///
/// A timeout wins regardless of any attached status. A 5xx/429 status maps to the
/// matching class; any other status is non-transient (`None`). With no status, a
/// connect failure and every other transport error (body/decode mid-stream) are
/// treated as connection-class transient — so `is_connect` does not change the
/// outcome and is accepted only to mirror the reqwest predicate at the call site.
/// This is a strict superset of the legacy `FaucetError::is_retriable`, so behavior
/// is preserved.
fn classify_transport(
    is_timeout: bool,
    status: Option<u16>,
    _is_connect: bool,
) -> Option<RetryClass> {
    if is_timeout {
        Some(RetryClass::Timeout)
    } else if let Some(status) = status {
        if status >= 500 {
            Some(RetryClass::Http5xx)
        } else if status == 429 {
            Some(RetryClass::RateLimited)
        } else {
            None
        }
    } else {
        // Statusless, non-timeout transport error: a connect failure and every
        // other transport error (body/decode mid-stream) are connection-class.
        Some(RetryClass::Connection)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn classifies_5xx_429_and_rate_limited() {
        assert_eq!(
            classify(&FaucetError::HttpStatus {
                status: 503,
                url: "u".into(),
                body: "".into()
            }),
            Some(RetryClass::Http5xx)
        );
        assert_eq!(
            classify(&FaucetError::HttpStatus {
                status: 429,
                url: "u".into(),
                body: "".into()
            }),
            Some(RetryClass::RateLimited)
        );
        assert_eq!(
            classify(&FaucetError::RateLimited(Duration::from_secs(1))),
            Some(RetryClass::RateLimited)
        );
    }

    #[test]
    fn a_typed_timeout_is_the_timeout_class() {
        assert_eq!(
            classify(&FaucetError::Timeout {
                operation: "rabbitmq publish".into(),
                after: Duration::from_secs(60),
            }),
            Some(RetryClass::Timeout)
        );
    }

    #[test]
    fn transport_classification_covers_every_arm() {
        // Timeout wins regardless of status.
        assert_eq!(
            classify_transport(true, None, false),
            Some(RetryClass::Timeout)
        );
        assert_eq!(
            classify_transport(true, Some(500), true),
            Some(RetryClass::Timeout)
        );
        // Status-bearing transport errors.
        assert_eq!(
            classify_transport(false, Some(503), false),
            Some(RetryClass::Http5xx)
        );
        assert_eq!(
            classify_transport(false, Some(429), false),
            Some(RetryClass::RateLimited)
        );
        // A non-5xx/429 status is not transient.
        assert_eq!(classify_transport(false, Some(404), false), None);
        // Statusless: connect and other transport errors are connection-class.
        assert_eq!(
            classify_transport(false, None, true),
            Some(RetryClass::Connection)
        );
        assert_eq!(
            classify_transport(false, None, false),
            Some(RetryClass::Connection)
        );
    }

    #[test]
    fn non_retriable_errors_classify_none() {
        assert_eq!(classify(&FaucetError::Auth("x".into())), None);
        assert_eq!(classify(&FaucetError::Config("x".into())), None);
        assert_eq!(classify(&FaucetError::Sink("x".into())), None);
    }

    #[test]
    fn retry_class_serde_round_trips() {
        use serde_json::json;
        // Http5xx serializes/deserializes as `http_5xx`, matching the metric
        // label and the docs/example config value.
        assert_eq!(
            serde_json::from_value::<RetryClass>(json!("http_5xx")).unwrap(),
            RetryClass::Http5xx
        );
        assert_eq!(
            serde_json::to_value(RetryClass::Http5xx).unwrap(),
            json!("http_5xx")
        );
        // The other three round-trip via their snake_case names.
        for (s, c) in [
            ("rate_limited", RetryClass::RateLimited),
            ("connection", RetryClass::Connection),
            ("timeout", RetryClass::Timeout),
        ] {
            assert_eq!(serde_json::from_value::<RetryClass>(json!(s)).unwrap(), c);
            assert_eq!(serde_json::to_value(c).unwrap(), json!(s));
        }
    }

    #[test]
    fn set_contains_and_default_covers_all_four() {
        let set = RetryClassSet::default();
        for c in [
            RetryClass::Http5xx,
            RetryClass::RateLimited,
            RetryClass::Connection,
            RetryClass::Timeout,
        ] {
            assert!(set.contains(c), "default set should contain {c:?}");
        }
        let only5xx = RetryClassSet::from_iter([RetryClass::Http5xx]);
        assert!(only5xx.contains(RetryClass::Http5xx));
        assert!(!only5xx.contains(RetryClass::RateLimited));
    }
}
