//! Configurable retriable-response matchers (#756).

use faucet_core::FaucetError;
use jsonpath_rust::JsonPath;
use reqwest::header::HeaderMap;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::Duration;

/// One `retry_on_response` rule: a non-2xx response that matches every set
/// condition is treated as throttling.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RetryMatcher {
    /// HTTP statuses this rule applies to (empty = any non-2xx).
    #[serde(default)]
    pub status: Vec<u16>,
    /// JSONPath into the (JSON) error body; its first match is compared with
    /// `values`. A non-JSON body never matches.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body_path: Option<String>,
    /// Values that match (numbers and strings compare by their text, so `17`
    /// matches `"17"`). Required with `body_path`; with only `header`, they
    /// are compared against the header value.
    #[serde(default)]
    pub values: Vec<Value>,
    /// A response header that must be present (and, without `body_path`,
    /// equal one of `values` when any are listed).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub header: Option<String>,
    /// Fixed wait before retrying. Without it the server's `Retry-After` is
    /// honoured, else the exponential `retry_backoff` policy applies.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backoff_secs: Option<u64>,
}

impl RetryMatcher {
    /// Load-time validation.
    pub fn validate(&self, idx: usize) -> Result<(), FaucetError> {
        let err = |m: &str| FaucetError::Config(format!("rest: `retry_on_response[{idx}]`: {m}"));
        if self.status.is_empty() && self.body_path.is_none() && self.header.is_none() {
            return Err(err("set at least one of `status`, `body_path`, `header`"));
        }
        if let Some(s) = self
            .status
            .iter()
            .find(|s| (200..300).contains(*s) || **s < 100 || **s > 599)
        {
            return Err(err(&format!("{s} is not an error status")));
        }
        if let Some(path) = &self.body_path {
            if self.values.is_empty() {
                return Err(err("`values` must not be empty when `body_path` is set"));
            }
            Value::Null
                .query(path)
                .map_err(|e| err(&format!("invalid `body_path` '{path}': {e}")))?;
        } else if !self.values.is_empty() && self.header.is_none() {
            return Err(err(
                "`values` needs `body_path` or `header` to compare against",
            ));
        }
        if let Some(h) = &self.header
            && reqwest::header::HeaderName::from_bytes(h.as_bytes()).is_err()
        {
            return Err(err(&format!("invalid header name '{h}'")));
        }
        if self.backoff_secs == Some(0) {
            return Err(err("`backoff_secs` must be greater than 0"));
        }
        Ok(())
    }

    fn matches(&self, status: u16, headers: &HeaderMap, body: &str) -> bool {
        if !self.status.is_empty() && !self.status.contains(&status) {
            return false;
        }
        if let Some(h) = &self.header {
            let Some(v) = headers.get(h.as_str()).and_then(|v| v.to_str().ok()) else {
                return false;
            };
            if self.body_path.is_none()
                && !self.values.is_empty()
                && !self
                    .values
                    .iter()
                    .any(|c| scalar_text(c).as_deref() == Some(v.trim()))
            {
                return false;
            }
        }
        if let Some(path) = &self.body_path {
            let Ok(doc) = serde_json::from_str::<Value>(body) else {
                return false;
            };
            let Some(found) = doc
                .query(path)
                .ok()
                .and_then(|r| r.first().cloned().cloned())
            else {
                return false;
            };
            let found = scalar_text(&found);
            if found.is_none() || !self.values.iter().any(|c| scalar_text(c) == found) {
                return false;
            }
        }
        true
    }
}

fn scalar_text(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

/// The first matcher a response satisfies, if any.
pub fn find_match<'a>(
    matchers: &'a [RetryMatcher],
    status: u16,
    headers: &HeaderMap,
    body: &str,
) -> Option<&'a RetryMatcher> {
    if (200..300).contains(&status) {
        return None;
    }
    matchers.iter().find(|m| m.matches(status, headers, body))
}

/// The wait before retrying a matched response: the matcher's fixed
/// `backoff_secs`, else the server's `Retry-After`, else exponential backoff
/// from `base` for the `attempt`-th consecutive match.
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

    fn meta() -> RetryMatcher {
        serde_json::from_value(json!({
            "status": [400, 403],
            "body_path": "$.error.code",
            "values": [17, "80004"]
        }))
        .unwrap()
    }

    #[test]
    fn body_code_matching_compares_text() {
        let m = [meta()];
        let h = HeaderMap::new();
        assert!(find_match(&m, 400, &h, r#"{"error":{"code":17}}"#).is_some());
        assert!(find_match(&m, 403, &h, r#"{"error":{"code":"17"}}"#).is_some());
        assert!(find_match(&m, 400, &h, r#"{"error":{"code":80004}}"#).is_some());
        assert!(find_match(&m, 400, &h, r#"{"error":{"code":100}}"#).is_none());
        assert!(find_match(&m, 500, &h, r#"{"error":{"code":17}}"#).is_none());
        assert!(find_match(&m, 400, &h, "not json").is_none());
        assert!(find_match(&m, 400, &h, r#"{"error":{}}"#).is_none());
        assert!(find_match(&m, 400, &h, r#"{"error":{"code":{"x":1}}}"#).is_none());
        assert!(find_match(&m, 200, &h, r#"{"error":{"code":17}}"#).is_none());
    }

    #[test]
    fn header_matching() {
        let m: RetryMatcher =
            serde_json::from_value(json!({"header": "x-throttled", "values": ["true"]})).unwrap();
        m.validate(0).unwrap();
        let mut h = HeaderMap::new();
        assert!(!m.matches(403, &h, ""));
        h.insert("x-throttled", "false".parse().unwrap());
        assert!(!m.matches(403, &h, ""));
        h.insert("x-throttled", "true".parse().unwrap());
        assert!(m.matches(403, &h, ""));
        let present: RetryMatcher =
            serde_json::from_value(json!({"status": [403], "header": "x-throttled"})).unwrap();
        assert!(present.matches(403, &h, ""));
        assert!(!present.matches(401, &h, ""));
        let status_only: RetryMatcher = serde_json::from_value(json!({"status": [503]})).unwrap();
        assert!(status_only.matches(503, &HeaderMap::new(), ""));
    }

    #[test]
    fn validation() {
        let v = |j: Value| {
            serde_json::from_value::<RetryMatcher>(j)
                .unwrap()
                .validate(0)
        };
        assert!(v(json!({})).is_err());
        assert!(v(json!({"status": [200]})).is_err());
        assert!(v(json!({"status": [99]})).is_err());
        assert!(v(json!({"body_path": "$.a"})).is_err());
        assert!(v(json!({"body_path": "$[", "values": [1]})).is_err());
        assert!(v(json!({"status": [400], "values": [1]})).is_err());
        assert!(v(json!({"header": "bad header"})).is_err());
        assert!(v(json!({"status": [400], "backoff_secs": 0})).is_err());
        assert!(meta().validate(0).is_ok());
    }

    #[test]
    fn wait_precedence() {
        let mut m = meta();
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
