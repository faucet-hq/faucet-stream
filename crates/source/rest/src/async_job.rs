//! Async-job source pattern (#514): submit → poll → fetch result.
//!
//! Covers the "big-data export / bulk / report-run" class of APIs that a
//! paginated GET can't express (Salesforce Bulk, Stripe Reporting, warehouse
//! UNLOAD, …). Configured as an `async_job:` block on the REST source; the
//! fetched result is handed to the `decode:` pipeline (#515) or the normal
//! body parsing.
//!
//! ```yaml
//! async_job:
//!   submit: { method: POST, url: "/jobs", json: { query: "SELECT ..." } }
//!   job_id: "$.id"
//!   poll:   { url: "/jobs/${job_id}", interval_secs: 5, timeout_secs: 1800 }
//!   status: { path: "$.state", success: [JobComplete], failure: [Failed, Aborted] }
//!   fetch:  { url: "/jobs/${job_id}/result" }
//! decode:
//!   - parse: { format: csv }
//! ```

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

fn default_get() -> String {
    "GET".to_owned()
}
fn default_interval() -> u64 {
    5
}
fn default_timeout() -> u64 {
    1800
}

/// One HTTP request in a job lifecycle (submit / fetch).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct JobRequest {
    /// HTTP method (default `GET`; set `POST` for `submit`).
    #[serde(default = "default_get")]
    pub method: String,
    /// URL — absolute, or a `base_url`-relative path. `${job_id}` is substituted.
    ///
    /// Required for `submit`. For `fetch`, set **exactly one** of `url` (a fixed
    /// template) or [`url_from`](Self::url_from) (a JSONPath into the poll body).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// `fetch` only (#543): resolve the download URL from the **last poll
    /// response body** via JSONPath, instead of rendering [`url`](Self::url).
    /// For APIs that return a one-time signed download link in the poll body
    /// (e.g. a Stripe report run's `result.url`) rather than at a deterministic
    /// `/{job_id}` path. The matched value must be a string; an absolute URL is
    /// used verbatim, a relative one is resolved against `base_url`. Mutually
    /// exclusive with [`url`](Self::url).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url_from: Option<String>,
    /// Extra headers.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub headers: HashMap<String, String>,
    /// Extra query params.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub query: HashMap<String, String>,
    /// JSON request body.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub json: Option<Value>,
    /// `fetch` only (#557): result-set continuation. Response header carrying a
    /// pagination locator (e.g. Salesforce Bulk `Sforce-Locator`). While present
    /// (and not empty / `"null"`), the fetch is repeated with the locator sent as
    /// [`locator_param`](Self::locator_param), appending records across pages.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub locator_header: Option<String>,
    /// `fetch` only (#557): JSONPath into the fetch response **body** for the
    /// continuation locator, when it rides the body rather than a header.
    /// Alternative to [`locator_header`](Self::locator_header).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub locator_body: Option<String>,
    /// `fetch` only (#557): query-param name the locator is sent as on each
    /// continuation request. Required when a locator source is configured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub locator_param: Option<String>,
    /// `fetch` only (#557): JSONPath for extracting records from each fetch page,
    /// overriding the source-level `records_path`. Applies to a JSON result body.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub records_path: Option<String>,
    /// `fetch` only (#654 M24): locator values that mean **no more pages**,
    /// compared case-insensitively after trimming. Defaults to `["null"]` — one
    /// vendor's sentinel, which used to be hardcoded in the pagination loop, so
    /// an API signalling completion with `none` / `-1` / `EOF` needed a code
    /// change. An empty/whitespace locator is always terminal regardless of
    /// this list; set it to `[]` to make *only* emptiness terminal.
    #[serde(default = "default_locator_terminal_values")]
    pub locator_terminal_values: Vec<String>,
}

fn default_locator_terminal_values() -> Vec<String> {
    vec!["null".to_string()]
}

/// The poll request + cadence.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PollSpec {
    /// HTTP method (default `GET`).
    #[serde(default = "default_get")]
    pub method: String,
    /// Status URL — absolute or `base_url`-relative; `${job_id}` substituted.
    pub url: String,
    /// Extra headers.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub headers: HashMap<String, String>,
    /// Extra query params.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub query: HashMap<String, String>,
    /// Ceiling on the poll cadence in seconds (default `5`). Polling starts at
    /// 1s and doubles up to this cap, so a job that finishes seconds after
    /// submit is noticed quickly while a long-running one isn't hammered. Set
    /// it to the slowest acceptable poll rate — it is the maximum gap between
    /// polls, not a fixed wait.
    #[serde(default = "default_interval")]
    pub interval_secs: u64,
    /// Give up after this many seconds (default `1800`).
    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,
}

/// How to read the job's terminal state from a poll response.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct JobStatus {
    /// JSONPath to the status value in the poll response.
    pub path: String,
    /// Status values meaning "done — go fetch".
    pub success: Vec<String>,
    /// Status values meaning "failed — abort".
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub failure: Vec<String>,
}

/// Terminal classification of a poll status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobOutcome {
    /// Ready to fetch.
    Success,
    /// Failed / aborted.
    Failure,
    /// Not terminal yet — keep polling.
    Pending,
}

impl JobStatus {
    /// Classify a poll's status value.
    pub fn classify(&self, status: &str) -> JobOutcome {
        if self.success.iter().any(|s| s == status) {
            JobOutcome::Success
        } else if self.failure.iter().any(|s| s == status) {
            JobOutcome::Failure
        } else {
            JobOutcome::Pending
        }
    }
}

/// The `async_job:` config block.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AsyncJobConfig {
    /// Job-creation request.
    pub submit: JobRequest,
    /// JSONPath to the job id in the submit response.
    pub job_id: String,
    /// Status polling.
    pub poll: PollSpec,
    /// Terminal-state classification.
    pub status: JobStatus,
    /// Result-download request.
    pub fetch: JobRequest,
    /// Incremental replication only: re-read margin subtracted from the
    /// persisted bookmark, same grammar as `window.lookback` (`45s` / `30m` /
    /// `6h` / `30d`; default `5m`). The bookmark is the client's run-start
    /// clock, so without a margin a client clock running *ahead* of the server
    /// would permanently skip records stamped in the gap; the margin turns
    /// that loss into a bounded re-read (deduped by an upsert sink).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lookback: Option<String>,
    /// Where the bulk **statement** sits inside the [`submit`](Self::submit)
    /// body, as an RFC 6901 JSON Pointer (#654 M23). Defaults to `/query` — the
    /// top-level `query` key that used to be hardcoded, so an API putting its
    /// statement at `sql`, `statement`, or a nested path silently lost
    /// incremental push-down and collapsed every object onto one dataset
    /// identity. Three behaviours read it: the incremental predicate
    /// injection, `validate()`'s incremental gate, and the catalog/lineage
    /// dataset name.
    #[serde(default = "default_query_path")]
    pub query_path: String,
}

fn default_query_path() -> String {
    "/query".to_string()
}

/// Default bookmark re-read margin (seconds) when `lookback` is unset: wide
/// enough to absorb realistic client-clock skew ahead of the server (NTP drift
/// plus modest misconfiguration), small enough that the per-run re-read stays
/// cheap.
const DEFAULT_BOOKMARK_LOOKBACK_SECS: i64 = 300;

impl AsyncJobConfig {
    /// Whether the submit body carries a top-level string `query` the
    /// incremental predicate can be injected into (#630). Without one every
    /// run is a full export, so the source config's `validate()` rejects
    /// `replication_method: incremental` in that shape — otherwise a bookmark
    /// would advance while the export silently stays full-table.
    pub fn supports_incremental_query(&self) -> bool {
        self.submit_query().is_some()
    }

    /// The bulk statement in the submit body, resolved through
    /// [`query_path`](Self::query_path). `None` when there is no submit body,
    /// the pointer matches nothing, or the match is not a string.
    pub fn submit_query(&self) -> Option<&str> {
        self.submit
            .json
            .as_ref()?
            .pointer(&self.query_path)?
            .as_str()
    }

    /// The parsed `lookback` margin (default 5 minutes — see the field docs).
    /// A malformed value is rejected by `validate()`; this falls back to the
    /// default rather than panicking for callers that skipped validation.
    pub fn lookback_duration(&self) -> chrono::Duration {
        self.lookback
            .as_deref()
            .and_then(|s| faucet_core::parse_step(s).ok())
            .unwrap_or_else(|| chrono::Duration::seconds(DEFAULT_BOOKMARK_LOOKBACK_SECS))
    }

    /// Validate the block at config-load time.
    pub fn validate(&self) -> Result<(), faucet_core::FaucetError> {
        // `submit` needs a fixed `url`; `url_from` is meaningless there (no poll
        // body exists yet).
        if self.submit.url_from.is_some() {
            return Err(faucet_core::FaucetError::Config(
                "async_job: `submit.url_from` is not supported — `submit` needs a fixed `url`"
                    .into(),
            ));
        }
        if self.submit.url.as_deref().unwrap_or("").trim().is_empty() {
            return Err(faucet_core::FaucetError::Config(
                "async_job: `submit.url` must not be empty".into(),
            ));
        }
        // `fetch` needs exactly one of `url` (templated) or `url_from` (JSONPath
        // into the poll body, #543).
        let fetch_url = self
            .fetch
            .url
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty());
        let fetch_url_from = self
            .fetch
            .url_from
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty());
        match (fetch_url, fetch_url_from) {
            (Some(_), Some(_)) => {
                return Err(faucet_core::FaucetError::Config(
                    "async_job: set exactly one of `fetch.url` or `fetch.url_from`, not both"
                        .into(),
                ));
            }
            (None, None) => {
                return Err(faucet_core::FaucetError::Config(
                    "async_job: `fetch` requires exactly one of `url` (templated) or `url_from` \
                     (a JSONPath into the poll response body)"
                        .into(),
                ));
            }
            _ => {}
        }
        if self.job_id.trim().is_empty() {
            return Err(faucet_core::FaucetError::Config(
                "async_job: `job_id` (JSONPath) must not be empty".into(),
            ));
        }
        if self.status.success.is_empty() {
            return Err(faucet_core::FaucetError::Config(
                "async_job: `status.success` must list at least one terminal value".into(),
            ));
        }
        if self.poll.interval_secs == 0 && self.poll.timeout_secs == 0 {
            return Err(faucet_core::FaucetError::Config(
                "async_job: `poll.timeout_secs` must be > 0".into(),
            ));
        }
        if let Some(lb) = &self.lookback {
            faucet_core::parse_step(lb).map_err(|_| {
                faucet_core::FaucetError::Config(format!(
                    "async_job: `lookback` '{lb}' is not a valid duration — use e.g. 45s, 30m, 6h"
                ))
            })?;
        }
        // #557: result-set continuation (locator paging) is a `fetch`-only
        // feature and needs a `locator_param` to request the next page.
        if self.submit.locator_header.is_some()
            || self.submit.locator_body.is_some()
            || self.submit.locator_param.is_some()
            || self.submit.records_path.is_some()
        {
            return Err(faucet_core::FaucetError::Config(
                "async_job: locator/`records_path` fields are `fetch`-only, not valid on `submit`"
                    .into(),
            ));
        }
        let has_locator_source =
            self.fetch.locator_header.is_some() || self.fetch.locator_body.is_some();
        if has_locator_source
            && self
                .fetch
                .locator_param
                .as_deref()
                .unwrap_or("")
                .trim()
                .is_empty()
        {
            return Err(faucet_core::FaucetError::Config(
                "async_job: `fetch.locator_param` is required when a `locator_header` or \
                 `locator_body` is configured (it names the query param the locator is sent as)"
                    .into(),
            ));
        }
        if self.fetch.locator_param.is_some() && !has_locator_source {
            return Err(faucet_core::FaucetError::Config(
                "async_job: `fetch.locator_param` needs a `locator_header` or `locator_body` to \
                 read the locator from"
                    .into(),
            ));
        }
        Ok(())
    }
}

/// Substitute `${job_id}` in a URL/template.
pub fn substitute_job_id(template: &str, job_id: &str) -> String {
    template.replace("${job_id}", job_id)
}

/// Resolve a possibly-relative URL against `base_url`.
pub fn resolve_url(base_url: &str, url: &str) -> String {
    if url.starts_with("http://") || url.starts_with("https://") {
        url.to_string()
    } else {
        format!(
            "{}/{}",
            base_url.trim_end_matches('/'),
            url.trim_start_matches('/')
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn classify_maps_status_to_outcome() {
        let s = JobStatus {
            path: "$.state".into(),
            success: vec!["JobComplete".into()],
            failure: vec!["Failed".into(), "Aborted".into()],
        };
        assert_eq!(s.classify("JobComplete"), JobOutcome::Success);
        assert_eq!(s.classify("Failed"), JobOutcome::Failure);
        assert_eq!(s.classify("Aborted"), JobOutcome::Failure);
        assert_eq!(s.classify("InProgress"), JobOutcome::Pending);
    }

    #[test]
    fn substitute_and_resolve_urls() {
        assert_eq!(substitute_job_id("/jobs/${job_id}/x", "42"), "/jobs/42/x");
        assert_eq!(resolve_url("https://h", "/jobs"), "https://h/jobs");
        assert_eq!(resolve_url("https://h/", "jobs"), "https://h/jobs");
        assert_eq!(
            resolve_url("https://h", "https://other/x"),
            "https://other/x"
        );
    }

    #[test]
    fn validate_rejects_empty_and_no_success() {
        let base: AsyncJobConfig = serde_json::from_value(json!({
            "submit": { "url": "/jobs" },
            "job_id": "$.id",
            "poll": { "url": "/jobs/${job_id}" },
            "status": { "path": "$.state", "success": ["Done"] },
            "fetch": { "url": "/jobs/${job_id}/result", "method": "GET" }
        }))
        .unwrap();
        assert!(base.validate().is_ok());

        let mut no_success = base.clone();
        no_success.status.success.clear();
        assert!(no_success.validate().is_err());

        let mut empty_id = base.clone();
        empty_id.job_id = " ".into();
        assert!(empty_id.validate().is_err());
    }

    #[test]
    fn validate_fetch_url_xor_url_from() {
        let make = |fetch: Value| -> AsyncJobConfig {
            serde_json::from_value(json!({
                "submit": { "method": "POST", "url": "/jobs" },
                "job_id": "$.id",
                "poll": { "url": "/jobs/${job_id}" },
                "status": { "path": "$.state", "success": ["Done"] },
                "fetch": fetch
            }))
            .unwrap()
        };

        // Exactly one → ok.
        assert!(
            make(json!({ "url": "/jobs/${job_id}/result" }))
                .validate()
                .is_ok()
        );
        assert!(
            make(json!({ "url_from": "$.result.url" }))
                .validate()
                .is_ok()
        );

        // Both → error.
        let both = make(json!({ "url": "/r", "url_from": "$.result.url" }));
        let err = both.validate().unwrap_err();
        assert!(
            matches!(err, faucet_core::FaucetError::Config(_)),
            "{err:?}"
        );
        assert!(err.to_string().contains("exactly one"), "{err}");

        // Neither → error.
        let neither = make(json!({}));
        assert!(neither.validate().is_err());

        // Empty strings count as unset → neither → error.
        let empty = make(json!({ "url": "  " }));
        assert!(empty.validate().is_err());
    }

    #[test]
    fn validate_rejects_url_from_on_submit() {
        let cfg: AsyncJobConfig = serde_json::from_value(json!({
            "submit": { "method": "POST", "url": "/jobs", "url_from": "$.x" },
            "job_id": "$.id",
            "poll": { "url": "/jobs/${job_id}" },
            "status": { "path": "$.state", "success": ["Done"] },
            "fetch": { "url_from": "$.result.url" }
        }))
        .unwrap();
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("submit.url_from"), "{err}");
    }

    #[test]
    fn validate_locator_continuation_fields() {
        let make = |fetch: Value| -> AsyncJobConfig {
            serde_json::from_value(json!({
                "submit": { "method": "POST", "url": "/jobs" },
                "job_id": "$.id",
                "poll": { "url": "/jobs/${job_id}" },
                "status": { "path": "$.state", "success": ["Done"] },
                "fetch": fetch
            }))
            .unwrap()
        };

        // Header locator + param → ok.
        assert!(
            make(json!({
                "url": "/jobs/${job_id}/results",
                "locator_header": "Sforce-Locator",
                "locator_param": "locator",
                "records_path": "$.records[*]"
            }))
            .validate()
            .is_ok()
        );
        // Body locator + param → ok.
        assert!(
            make(json!({
                "url_from": "$.result.url",
                "locator_body": "$.next_locator",
                "locator_param": "locator"
            }))
            .validate()
            .is_ok()
        );
        // Locator source without param → error.
        let err = make(json!({
            "url": "/r",
            "locator_header": "Sforce-Locator"
        }))
        .validate()
        .unwrap_err();
        assert!(err.to_string().contains("locator_param"), "{err}");
        // Param without a source → error.
        assert!(
            make(json!({ "url": "/r", "locator_param": "locator" }))
                .validate()
                .is_err()
        );
    }

    #[test]
    fn validate_rejects_locator_on_submit() {
        let cfg: AsyncJobConfig = serde_json::from_value(json!({
            "submit": { "method": "POST", "url": "/jobs", "locator_header": "X" },
            "job_id": "$.id",
            "poll": { "url": "/jobs/${job_id}" },
            "status": { "path": "$.state", "success": ["Done"] },
            "fetch": { "url": "/r" }
        }))
        .unwrap();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn poll_defaults_apply() {
        let cfg: AsyncJobConfig = serde_json::from_value(json!({
            "submit": { "url": "/jobs" },
            "job_id": "$.id",
            "poll": { "url": "/jobs/${job_id}" },
            "status": { "path": "$.state", "success": ["Done"] },
            "fetch": { "url": "/r" }
        }))
        .unwrap();
        assert_eq!(cfg.poll.interval_secs, 5);
        assert_eq!(cfg.poll.timeout_secs, 1800);
        assert_eq!(cfg.poll.method, "GET");
        assert_eq!(cfg.submit.method, "GET"); // default; examples set POST explicitly
        assert_eq!(cfg.fetch.method, "GET");
    }

    #[test]
    fn lookback_is_validated_and_defaults_to_five_minutes() {
        let mut cfg: AsyncJobConfig = serde_json::from_value(serde_json::json!({
            "submit": { "url": "/jobs", "json": { "query": "SELECT Id FROM A" } },
            "job_id": "$.id",
            "poll": { "url": "/jobs/${job_id}" },
            "status": { "path": "$.state", "success": ["done"] },
            "fetch": { "url": "/jobs/${job_id}/result" }
        }))
        .unwrap();
        // Default margin: 5 minutes.
        assert_eq!(cfg.lookback_duration(), chrono::Duration::seconds(300));
        assert!(cfg.supports_incremental_query());

        // A valid duration parses and is honored.
        cfg.lookback = Some("90s".into());
        assert!(cfg.validate().is_ok());
        assert_eq!(cfg.lookback_duration(), chrono::Duration::seconds(90));

        // A malformed one is rejected at load time, naming the field.
        cfg.lookback = Some("soon".into());
        let err = cfg.validate().unwrap_err().to_string();
        assert!(
            err.contains("`lookback` 'soon' is not a valid duration"),
            "{err}"
        );
        // …and the accessor falls back to the default rather than panicking.
        assert_eq!(cfg.lookback_duration(), chrono::Duration::seconds(300));
    }

    #[test]
    fn supports_incremental_query_requires_a_top_level_string_query() {
        let mk = |submit: serde_json::Value| -> AsyncJobConfig {
            serde_json::from_value(serde_json::json!({
                "submit": submit,
                "job_id": "$.id",
                "poll": { "url": "/j/${job_id}" },
                "status": { "path": "$.s", "success": ["ok"] },
                "fetch": { "url": "/j/${job_id}/r" }
            }))
            .unwrap()
        };
        assert!(!mk(serde_json::json!({ "url": "/jobs" })).supports_incremental_query());
        assert!(
            !mk(serde_json::json!({ "url": "/jobs", "json": { "report": "x" } }))
                .supports_incremental_query()
        );
        // A non-string `query` is not amendable either.
        assert!(
            !mk(serde_json::json!({ "url": "/jobs", "json": { "query": 7 } }))
                .supports_incremental_query()
        );
    }
}
