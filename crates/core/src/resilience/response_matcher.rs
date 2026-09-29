//! Retriable-response matchers (#756, #767, #771): a response that matches a
//! rule is treated as throttling, and the rule says how long to wait.

use crate::FaucetError;
use chrono::{DateTime, Utc};
use jsonpath_rust::JsonPath;
use reqwest::header::HeaderMap;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::Duration;

/// Default ceiling on a wait computed from `backoff_from`.
pub const DEFAULT_MAX_WAIT_SECS: u64 = 3600;

/// One `retry_on_response` rule: a response that matches every set condition
/// is treated as throttling.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RetryMatcher {
    /// HTTP statuses this rule applies to (empty = any non-2xx, or any status
    /// with `match_success`).
    #[serde(default)]
    pub status: Vec<u16>,
    /// Also match 2xx responses, for APIs that report throttling inside a
    /// successful response (GraphQL `THROTTLED` errors, `x-ratelimit-remaining:
    /// 0`). Requires `body_path` or `header`.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub match_success: bool,
    /// JSONPath into the (JSON) response body; any match equal to one of
    /// `values` satisfies the rule. A non-JSON body never matches.
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
    /// Where the response says how long to wait. Takes precedence over
    /// `backoff_secs`; when the value is missing or unreadable the rule falls
    /// back to `backoff_secs`, then `Retry-After`, then exponential backoff.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backoff_from: Option<BackoffFrom>,
    /// Fixed wait before retrying. Without it the server's `Retry-After` is
    /// honoured, else exponential backoff applies.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backoff_secs: Option<u64>,
    /// Longest wait `backoff_from` may ask for (default 3600). A longer one
    /// fails the run with an error naming the reset instead of parking it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_wait_secs: Option<u64>,
}

/// Where a matched response states its wait.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", content = "config", rename_all = "snake_case")]
pub enum BackoffFrom {
    /// A response header (`x-ratelimit-reset`).
    Header {
        /// Header name.
        name: String,
        /// How to read the value.
        unit: WaitUnit,
    },
    /// A JSON-valued response header, read at a JSONPath
    /// (`x-business-use-case-usage`).
    HeaderJson {
        /// Header name.
        name: String,
        /// JSONPath into the parsed header value; the first number or string
        /// found is used.
        path: String,
        /// How to read the value.
        unit: WaitUnit,
    },
    /// A value in the JSON response body.
    Body {
        /// JSONPath into the body; the first number or string found is used.
        path: String,
        /// How to read the value.
        unit: WaitUnit,
    },
    /// A leaky-bucket cost report (Shopify GraphQL): wait
    /// `ceil((requested - available) / restore_rate)` seconds, at least 1.
    CostBucket {
        /// JSONPath to the cost the request asked for.
        requested: String,
        /// JSONPath to the points currently available.
        available: String,
        /// JSONPath to the points restored per second.
        restore_rate: String,
    },
}

/// How a `backoff_from` value is read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum WaitUnit {
    /// A wait in seconds.
    Seconds,
    /// A wait in milliseconds.
    Ms,
    /// A wait in minutes.
    Minutes,
    /// An absolute Unix time in seconds.
    EpochS,
    /// An absolute Unix time in milliseconds.
    EpochMs,
    /// An absolute RFC 3339 instant.
    Rfc3339,
}

impl RetryMatcher {
    /// Load-time validation; messages name `retry_on_response[idx]`.
    pub fn validate(&self, idx: usize) -> Result<(), FaucetError> {
        let err = |m: &str| FaucetError::Config(format!("`retry_on_response[{idx}]`: {m}"));
        if self.status.is_empty() && self.body_path.is_none() && self.header.is_none() {
            return Err(err("set at least one of `status`, `body_path`, `header`"));
        }
        if let Some(s) = self.status.iter().find(|s| **s < 100 || **s > 599) {
            return Err(err(&format!("{s} is not an HTTP status")));
        }
        if !self.match_success
            && let Some(s) = self.status.iter().find(|s| (200..300).contains(*s))
        {
            return Err(err(&format!(
                "{s} is not an error status (set `match_success: true` to match 2xx responses)"
            )));
        }
        if self.match_success && self.body_path.is_none() && self.header.is_none() {
            return Err(err(
                "`match_success` needs `body_path` or `header`; retrying every success would loop",
            ));
        }
        if let Some(path) = &self.body_path {
            if self.values.is_empty() {
                return Err(err("`values` must not be empty when `body_path` is set"));
            }
            check_path(path).map_err(|e| err(&format!("invalid `body_path` '{path}': {e}")))?;
        } else if !self.values.is_empty() && self.header.is_none() {
            return Err(err(
                "`values` needs `body_path` or `header` to compare against",
            ));
        }
        if let Some(h) = &self.header {
            check_header(h).map_err(|e| err(&e))?;
        }
        if self.backoff_secs == Some(0) {
            return Err(err("`backoff_secs` must be greater than 0"));
        }
        if self.max_wait_secs == Some(0) {
            return Err(err("`max_wait_secs` must be greater than 0"));
        }
        if let Some(from) = &self.backoff_from {
            from.validate()
                .map_err(|e| err(&format!("`backoff_from`: {e}")))?;
        }
        Ok(())
    }

    /// Whether a response satisfies every condition of this rule.
    pub fn matches(&self, status: u16, headers: &HeaderMap, body: &str) -> bool {
        let success = (200..300).contains(&status);
        if success && !self.match_success {
            return false;
        }
        if self.status.is_empty() {
            if !success && self.match_success && self.body_path.is_none() && self.header.is_none() {
                return false;
            }
        } else if !self.status.contains(&status) {
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
            let Ok(found) = doc.query(path) else {
                return false;
            };
            let hit = found.iter().filter_map(|f| scalar_text(f)).any(|f| {
                self.values
                    .iter()
                    .any(|c| scalar_text(c).as_deref() == Some(&f))
            });
            if !hit {
                return false;
            }
        }
        true
    }

    /// The wait before retrying a matched response: `backoff_from`, else
    /// `backoff_secs`, else the server's `Retry-After`, else exponential
    /// backoff from `base` for the `attempt`-th consecutive match. Errors only
    /// when `backoff_from` asks for more than `max_wait_secs`.
    pub fn wait(
        &self,
        headers: &HeaderMap,
        body: &str,
        retry_after: Option<Duration>,
        base: Duration,
        attempt: u32,
    ) -> Result<Duration, FaucetError> {
        self.wait_at(headers, body, retry_after, base, attempt, Utc::now())
    }

    fn wait_at(
        &self,
        headers: &HeaderMap,
        body: &str,
        retry_after: Option<Duration>,
        base: Duration,
        attempt: u32,
        now: DateTime<Utc>,
    ) -> Result<Duration, FaucetError> {
        if let Some(from) = &self.backoff_from {
            let cap = Duration::from_secs(self.max_wait_secs.unwrap_or(DEFAULT_MAX_WAIT_SECS));
            match from.resolve(headers, body, now) {
                Some(w) if w > cap => {
                    return Err(FaucetError::Source(format!(
                        "rate limited: the response asks to wait {}s ({}), more than \
                         `max_wait_secs` ({}s)",
                        w.as_secs(),
                        from.describe(),
                        cap.as_secs()
                    )));
                }
                Some(w) => return Ok(w),
                None => tracing::warn!(
                    "retry_on_response: could not read a wait from {}; falling back",
                    from.describe()
                ),
            }
        }
        Ok(self
            .backoff_secs
            .map(Duration::from_secs)
            .or(retry_after)
            .unwrap_or_else(|| crate::retry::backoff_with_jitter(base, attempt)))
    }
}

impl BackoffFrom {
    fn validate(&self) -> Result<(), String> {
        match self {
            Self::Header { name, .. } => check_header(name),
            Self::HeaderJson { name, path, .. } => {
                check_header(name)?;
                check_path(path).map_err(|e| format!("invalid `path` '{path}': {e}"))
            }
            Self::Body { path, .. } => {
                check_path(path).map_err(|e| format!("invalid `path` '{path}': {e}"))
            }
            Self::CostBucket {
                requested,
                available,
                restore_rate,
            } => [requested, available, restore_rate]
                .iter()
                .try_for_each(|p| check_path(p).map_err(|e| format!("invalid path '{p}': {e}"))),
        }
    }

    fn describe(&self) -> String {
        match self {
            Self::Header { name, .. } => format!("header `{name}`"),
            Self::HeaderJson { name, path, .. } => format!("`{path}` in header `{name}`"),
            Self::Body { path, .. } => format!("`{path}` in the body"),
            Self::CostBucket { .. } => "the body's cost report".to_owned(),
        }
    }

    /// The wait this source states, or `None` when it is missing/unreadable.
    fn resolve(&self, headers: &HeaderMap, body: &str, now: DateTime<Utc>) -> Option<Duration> {
        let reference = headers
            .get(reqwest::header::DATE)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| DateTime::parse_from_rfc2822(v).ok())
            .map(|d| d.with_timezone(&Utc))
            .unwrap_or(now);
        match self {
            Self::Header { name, unit } => {
                let raw = headers.get(name.as_str())?.to_str().ok()?.trim().to_owned();
                unit.wait(&Value::String(raw), reference)
            }
            Self::HeaderJson { name, path, unit } => {
                let raw = headers.get(name.as_str())?.to_str().ok()?;
                let doc: Value = serde_json::from_str(raw).ok()?;
                unit.wait(&first_scalar(&doc, path)?, reference)
            }
            Self::Body { path, unit } => {
                let doc: Value = serde_json::from_str(body).ok()?;
                unit.wait(&first_scalar(&doc, path)?, reference)
            }
            Self::CostBucket {
                requested,
                available,
                restore_rate,
            } => {
                let doc: Value = serde_json::from_str(body).ok()?;
                let num = |p: &str| first_scalar(&doc, p).and_then(|v| as_f64(&v));
                let rate = num(restore_rate)?;
                if !rate.is_finite() || rate <= 0.0 {
                    return None;
                }
                let deficit = (num(requested)? - num(available)?).max(0.0);
                let secs = (deficit / rate).ceil().max(1.0);
                secs.is_finite().then(|| Duration::from_secs(secs as u64))
            }
        }
    }
}

impl WaitUnit {
    /// The wait a value in this unit states; an instant in the past is 0.
    fn wait(self, value: &Value, reference: DateTime<Utc>) -> Option<Duration> {
        let until = |at: DateTime<Utc>| Some((at - reference).to_std().unwrap_or(Duration::ZERO));
        match self {
            Self::Seconds => relative(as_f64(value)?, 1.0),
            Self::Ms => relative(as_f64(value)?, 0.001),
            Self::Minutes => relative(as_f64(value)?, 60.0),
            Self::EpochS => until(DateTime::from_timestamp(as_f64(value)? as i64, 0)?),
            Self::EpochMs => until(DateTime::from_timestamp_millis(as_f64(value)? as i64)?),
            Self::Rfc3339 => until(
                DateTime::parse_from_rfc3339(value.as_str()?.trim())
                    .ok()?
                    .with_timezone(&Utc),
            ),
        }
    }
}

fn relative(n: f64, scale: f64) -> Option<Duration> {
    let secs = n * scale;
    (secs.is_finite() && secs >= 0.0).then(|| Duration::from_secs_f64(secs))
}

fn as_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.trim().parse().ok(),
        _ => None,
    }
}

fn first_scalar(doc: &Value, path: &str) -> Option<Value> {
    doc.query(path)
        .ok()?
        .into_iter()
        .find(|v| v.is_number() || v.is_string())
        .cloned()
}

fn check_path(path: &str) -> Result<(), String> {
    Value::Null
        .query(path)
        .map(|_| ())
        .map_err(|e| e.to_string())
}

fn check_header(name: &str) -> Result<(), String> {
    reqwest::header::HeaderName::from_bytes(name.as_bytes())
        .map(|_| ())
        .map_err(|_| format!("invalid header name '{name}'"))
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
    matchers.iter().find(|m| m.matches(status, headers, body))
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

    fn m(j: Value) -> RetryMatcher {
        serde_json::from_value(j).unwrap()
    }

    fn at(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    fn headers(pairs: &[(&'static str, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            h.insert(*k, v.parse().unwrap());
        }
        h
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
        let hv = m(json!({"header": "x-throttled", "values": ["true"]}));
        hv.validate(0).unwrap();
        let mut h = HeaderMap::new();
        assert!(!hv.matches(403, &h, ""));
        h.insert("x-throttled", "false".parse().unwrap());
        assert!(!hv.matches(403, &h, ""));
        h.insert("x-throttled", "true".parse().unwrap());
        assert!(hv.matches(403, &h, ""));
        let present = m(json!({"status": [403], "header": "x-throttled"}));
        assert!(present.matches(403, &h, ""));
        assert!(!present.matches(401, &h, ""));
        let status_only = m(json!({"status": [503]}));
        assert!(status_only.matches(503, &HeaderMap::new(), ""));
    }

    #[test]
    fn success_matching_is_opt_in() {
        let graphql = m(json!({
            "match_success": true,
            "body_path": "$.errors[*].extensions.code",
            "values": ["THROTTLED"]
        }));
        graphql.validate(0).unwrap();
        let h = HeaderMap::new();
        let throttled =
            r#"{"data":null,"errors":[{"message":"x"},{"extensions":{"code":"THROTTLED"}}]}"#;
        assert!(graphql.matches(200, &h, throttled));
        assert!(graphql.matches(429, &h, throttled));
        assert!(!graphql.matches(200, &h, r#"{"errors":[{"extensions":{"code":"BAD"}}]}"#));
        assert!(!graphql.matches(200, &h, r#"{"data":{}}"#));
        let listed = m(
            json!({"status": [200], "match_success": true, "header": "x-ratelimit-remaining", "values": ["0"]}),
        );
        listed.validate(0).unwrap();
        assert!(listed.matches(200, &headers(&[("x-ratelimit-remaining", "0")]), ""));
        assert!(!listed.matches(201, &headers(&[("x-ratelimit-remaining", "0")]), ""));
        let plain = m(json!({"header": "x-ratelimit-remaining", "values": ["0"]}));
        assert!(!plain.matches(200, &headers(&[("x-ratelimit-remaining", "0")]), ""));
    }

    #[test]
    fn validation() {
        let v = |j: Value| m(j).validate(0);
        assert!(v(json!({})).is_err());
        assert!(v(json!({"status": [200]})).is_err());
        assert!(v(json!({"status": [99]})).is_err());
        assert!(v(json!({"body_path": "$.a"})).is_err());
        assert!(v(json!({"body_path": "$[", "values": [1]})).is_err());
        assert!(v(json!({"status": [400], "values": [1]})).is_err());
        assert!(v(json!({"header": "bad header"})).is_err());
        assert!(v(json!({"status": [400], "backoff_secs": 0})).is_err());
        assert!(v(json!({"status": [400], "max_wait_secs": 0})).is_err());
        let loops = v(json!({"status": [200], "match_success": true})).unwrap_err();
        assert!(loops.to_string().contains("would loop"), "{loops}");
        assert!(v(json!({"status": [429], "backoff_from": {"type": "header", "config": {"name": "bad name", "unit": "seconds"}}})).is_err());
        assert!(v(json!({"status": [429], "backoff_from": {"type": "header_json", "config": {"name": "x", "path": "$[", "unit": "minutes"}}})).is_err());
        assert!(v(json!({"status": [429], "backoff_from": {"type": "header_json", "config": {"name": "bad name", "path": "$.a", "unit": "minutes"}}})).is_err());
        assert!(v(json!({"status": [429], "backoff_from": {"type": "body", "config": {"path": "$[", "unit": "ms"}}})).is_err());
        assert!(v(json!({"status": [429], "backoff_from": {"type": "cost_bucket", "config": {"requested": "$.a", "available": "$[", "restore_rate": "$.c"}}})).is_err());
        let msg = v(json!({"status": [429], "backoff_from": {"type": "body", "config": {"path": "$[", "unit": "ms"}}})).unwrap_err();
        assert!(msg.to_string().contains("retry_on_response[0]"), "{msg}");
        assert!(meta().validate(0).is_ok());
    }

    #[test]
    fn wait_precedence() {
        let mut rule = meta();
        let base = Duration::from_millis(100);
        let h = HeaderMap::new();
        let w = |r: &RetryMatcher, ra| r.wait(&h, "", ra, base, 0).unwrap();
        assert_eq!(
            w(&rule, Some(Duration::from_secs(3))),
            Duration::from_secs(3)
        );
        let exp = w(&rule, None);
        assert!(exp >= Duration::from_millis(50) && exp <= Duration::from_millis(150));
        rule.backoff_secs = Some(60);
        assert_eq!(
            w(&rule, Some(Duration::from_secs(3))),
            Duration::from_secs(60)
        );
        rule.backoff_from = Some(BackoffFrom::Header {
            name: "x-wait".into(),
            unit: WaitUnit::Seconds,
        });
        assert_eq!(
            w(&rule, None),
            Duration::from_secs(60),
            "missing header falls back"
        );
        let h = headers(&[("x-wait", "7")]);
        assert_eq!(
            rule.wait(&h, "", None, base, 0).unwrap(),
            Duration::from_secs(7)
        );
    }

    #[test]
    fn every_unit_and_a_past_reset() {
        let now = at("2026-09-01T00:00:00Z");
        let s = |v: Value, u: WaitUnit| u.wait(&v, now);
        assert_eq!(s(json!(5), WaitUnit::Seconds), Some(Duration::from_secs(5)));
        assert_eq!(
            s(json!("1500"), WaitUnit::Ms),
            Some(Duration::from_millis(1500))
        );
        assert_eq!(
            s(json!(2), WaitUnit::Minutes),
            Some(Duration::from_secs(120))
        );
        let epoch = now.timestamp();
        assert_eq!(
            s(json!(epoch + 30), WaitUnit::EpochS),
            Some(Duration::from_secs(30))
        );
        assert_eq!(
            s(json!((epoch + 2) * 1000), WaitUnit::EpochMs),
            Some(Duration::from_secs(2))
        );
        assert_eq!(
            s(json!("2026-09-01T00:01:00Z"), WaitUnit::Rfc3339),
            Some(Duration::from_secs(60))
        );
        assert_eq!(
            s(json!(epoch - 100), WaitUnit::EpochS),
            Some(Duration::ZERO),
            "past reset waits 0"
        );
        assert_eq!(s(json!(-3), WaitUnit::Seconds), None);
        assert_eq!(s(json!("soon"), WaitUnit::Seconds), None);
        assert_eq!(s(json!(true), WaitUnit::Seconds), None);
        assert_eq!(s(json!(5), WaitUnit::Rfc3339), None);
        assert_eq!(s(json!("not a date"), WaitUnit::Rfc3339), None);
    }

    #[test]
    fn github_reset_header_uses_the_date_header() {
        let rule = m(json!({
            "status": [403, 429],
            "header": "x-ratelimit-remaining",
            "values": ["0"],
            "backoff_from": {"type": "header", "config": {"name": "x-ratelimit-reset", "unit": "epoch_s"}}
        }));
        rule.validate(0).unwrap();
        let server = at("2026-09-01T12:00:00Z");
        let h = headers(&[
            ("x-ratelimit-remaining", "0"),
            ("x-ratelimit-reset", &(server.timestamp() + 90).to_string()),
            ("date", "Tue, 01 Sep 2026 12:00:00 GMT"),
        ]);
        assert!(rule.matches(403, &h, ""));
        let local = at("2026-09-01T12:00:45Z");
        let w = rule
            .wait_at(&h, "", None, Duration::ZERO, 0, local)
            .unwrap();
        assert_eq!(
            w,
            Duration::from_secs(90),
            "measured from the server's clock"
        );
        let mut no_date = h.clone();
        no_date.remove("date");
        let w = rule
            .wait_at(&no_date, "", None, Duration::ZERO, 0, local)
            .unwrap();
        assert_eq!(w, Duration::from_secs(45), "local clock without Date");
    }

    #[test]
    fn meta_usage_header_in_minutes() {
        let rule = m(json!({
            "status": [400, 403],
            "body_path": "$.error.code",
            "values": [80004, 17],
            "backoff_from": {"type": "header_json", "config": {
                "name": "x-business-use-case-usage",
                "path": "$.*[0].estimated_time_to_regain_access",
                "unit": "minutes"
            }}
        }));
        rule.validate(0).unwrap();
        let h = headers(&[(
            "x-business-use-case-usage",
            r#"{"123":[{"type":"ads_insights","estimated_time_to_regain_access":3}]}"#,
        )]);
        assert_eq!(
            rule.wait(&h, "", None, Duration::ZERO, 0).unwrap(),
            Duration::from_secs(180)
        );
        let bad = headers(&[("x-business-use-case-usage", "not json")]);
        let w = rule
            .wait(&bad, "", Some(Duration::from_secs(4)), Duration::ZERO, 0)
            .unwrap();
        assert_eq!(w, Duration::from_secs(4));
    }

    #[test]
    fn body_wait_and_the_cap() {
        let mut rule = m(json!({
            "status": [429],
            "backoff_from": {"type": "body", "config": {"path": "$.retry_in", "unit": "seconds"}},
            "max_wait_secs": 10
        }));
        let h = HeaderMap::new();
        assert_eq!(
            rule.wait(&h, r#"{"retry_in": 10}"#, None, Duration::ZERO, 0)
                .unwrap(),
            Duration::from_secs(10)
        );
        let e = rule
            .wait(&h, r#"{"retry_in": 11}"#, None, Duration::ZERO, 0)
            .unwrap_err();
        assert!(
            e.to_string().contains("max_wait_secs") && e.to_string().contains("$.retry_in"),
            "{e}"
        );
        assert!(!e.is_retriable());
        rule.max_wait_secs = None;
        let e = rule
            .wait(&h, r#"{"retry_in": 3601}"#, None, Duration::ZERO, 0)
            .unwrap_err();
        assert!(e.to_string().contains("3600"), "{e}");
        assert_eq!(
            rule.wait(
                &h,
                "not json",
                Some(Duration::from_secs(2)),
                Duration::ZERO,
                0
            )
            .unwrap(),
            Duration::from_secs(2)
        );
    }

    #[test]
    fn cost_bucket_maths() {
        let rule = m(json!({
            "match_success": true,
            "body_path": "$.errors[*].extensions.code",
            "values": ["THROTTLED"],
            "backoff_from": {"type": "cost_bucket", "config": {
                "requested": "$.extensions.cost.requestedQueryCost",
                "available": "$.extensions.cost.throttleStatus.currentlyAvailable",
                "restore_rate": "$.extensions.cost.throttleStatus.restoreRate"
            }},
            "backoff_secs": 9
        }));
        rule.validate(0).unwrap();
        let h = HeaderMap::new();
        let body = |req: Value, avail: Value, rate: Value| {
            json!({"extensions": {"cost": {"requestedQueryCost": req, "throttleStatus": {"currentlyAvailable": avail, "restoreRate": rate}}}}).to_string()
        };
        let w = |b: String| rule.wait(&h, &b, None, Duration::ZERO, 0).unwrap();
        assert_eq!(
            w(body(json!(502), json!(2), json!(50.0))),
            Duration::from_secs(10)
        );
        assert_eq!(
            w(body(json!(101), json!(0), json!(50))),
            Duration::from_secs(3)
        );
        assert_eq!(
            w(body(json!(10), json!(900), json!(50))),
            Duration::from_secs(1),
            "at least 1s"
        );
        assert_eq!(
            w(body(json!(10), json!(0), json!(0))),
            Duration::from_secs(9),
            "zero rate falls back"
        );
        assert_eq!(
            w(body(json!(10), json!(0), json!(-1))),
            Duration::from_secs(9)
        );
        assert_eq!(
            w(json!({"extensions": {}}).to_string()),
            Duration::from_secs(9),
            "missing fields fall back"
        );
        assert_eq!(w("not json".into()), Duration::from_secs(9));
        let big = rule.wait(
            &h,
            &body(json!(1e9), json!(0), json!(1)),
            None,
            Duration::ZERO,
            0,
        );
        assert!(big.is_err());
    }
}
