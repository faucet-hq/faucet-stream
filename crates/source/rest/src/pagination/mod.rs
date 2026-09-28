//! Pagination strategies for REST APIs.

pub mod cursor;
pub mod link_header;
pub mod next_link_body;
pub mod offset;
pub mod page;

use faucet_core::FaucetError;
use reqwest::header::HeaderMap;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

fn default_true() -> bool {
    true
}

/// Resolve a next-page link against the URL of the request that returned it
/// (#750). Without a recorded request URL the link is used as given.
fn resolve_against_request(headers: &HeaderMap, link: &str) -> Result<String, FaucetError> {
    match headers
        .get(crate::url_util::REQUEST_URL_HEADER)
        .and_then(|v| v.to_str().ok())
    {
        Some(base) => crate::url_util::resolve_link_str(base, link),
        None => Ok(link.to_owned()),
    }
}

/// Where a [`PaginationStyle::RecordFieldCursor`] keyset value is injected on the
/// next request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "snake_case")]
pub enum RecordCursorTarget {
    /// Inject the cursor as a query parameter (default).
    #[default]
    Query,
    /// Inject the cursor into the JSON request body.
    Body,
}

/// How a [`PaginationStyle::RecordFieldCursor`] aggregates the cursor field over
/// a page.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "snake_case")]
pub enum RecordCursorAgg {
    /// The maximum value seen so far (ascending keyset, the default).
    #[default]
    Max,
    /// The minimum value seen so far (descending keyset).
    Min,
}

/// Supported pagination strategies.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type")]
pub enum PaginationStyle {
    None,
    Cursor {
        next_token_path: String,
        param_name: String,
    },
    /// POST-search pagination: the next-page cursor is read from the response
    /// body via `next_token_path` and written **into the request JSON body** at
    /// `body_cursor_field` for the next request (rather than a query param).
    ///
    /// The first request uses `config.body` unchanged; each subsequent request
    /// sets `body[body_cursor_field] = <extracted cursor>`. Pagination stops when
    /// `next_token_path` is null/absent, and a repeated cursor trips the same
    /// loop guard as [`PaginationStyle::Cursor`]. Used by e.g. HubSpot CRM
    /// `POST /crm/v3/objects/{obj}/search` (`$.paging.next.after` → `after`).
    ///
    /// `body_cursor_field` may also be an RFC 6901 JSON Pointer
    /// (`/variables/after`) for a nested location — e.g. a GraphQL request
    /// sent through the REST source. The pointer must resolve to an existing
    /// value, or to a new key of an existing object (#751).
    CursorInBody {
        next_token_path: String,
        body_cursor_field: String,
    },
    LinkHeader,
    /// The full URL of the next page is embedded in the response body.
    /// `next_link_path` is a JSONPath expression pointing to that URL field
    /// (e.g. `"$.next_link"`).  Pagination stops when the field is absent,
    /// null, or an empty string.
    NextLinkInBody {
        next_link_path: String,
    },
    PageNumber {
        param_name: String,
        start_page: usize,
        page_size: Option<usize>,
        page_size_param: Option<String>,
    },
    Offset {
        offset_param: String,
        limit_param: String,
        limit: usize,
        total_path: Option<String>,
    },
    /// Offset/limit pagination that writes the offset and limit into the JSON
    /// **request body** (POST-query APIs), rather than the query string
    /// ([`Offset`](Self::Offset)) or a cursor token
    /// ([`CursorInBody`](Self::CursorInBody)). Each request sends
    /// `body[offset_field] = <offset>` and `body[limit_field] = <limit>`; the
    /// offset advances by the page's record count. With `stop_when_short`
    /// (default `true`) a page shorter than `limit` ends pagination; a zero-record
    /// page always ends it, and a repeated identical page trips a loop guard.
    /// Either field may be a JSON Pointer (`/page/offset`) for a nested
    /// location, with the same rules as `CursorInBody.body_cursor_field`.
    OffsetInBody {
        offset_field: String,
        limit_field: String,
        limit: usize,
        #[serde(default = "default_true")]
        stop_when_short: bool,
        /// JSONPath whose matches are counted to advance the offset and detect a
        /// short page, instead of the extracted record count — for report APIs
        /// whose whole response is one record (`$.rows`). An array counts its
        /// elements, an object 1, null or no match 0 (which ends pagination).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        rows_path: Option<String>,
        /// JSONPath to the total row count; pagination stops once the offset
        /// reaches it. A missing or non-numeric total falls back to the
        /// short-page rule.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        total_path: Option<String>,
    },
    /// Keyset pagination by the running max/min of a record field (#554). After
    /// each page the aggregate of `field` over its records is injected into the
    /// next request (as a query param or body field per `into`) at `param`. With
    /// `stop_when_short` (default `true`) a page shorter than `page_size` ends
    /// pagination; a zero-record page always ends it, and a non-advancing cursor
    /// trips a loop guard.
    RecordFieldCursor {
        /// Record field whose value drives the cursor.
        field: String,
        /// Where the cursor is injected on the next request (default `query`).
        #[serde(default)]
        into: RecordCursorTarget,
        /// Request parameter/body-field name the cursor is written to.
        param: String,
        /// Aggregation over the page (default `max`).
        #[serde(default)]
        agg: RecordCursorAgg,
        /// Stop when a page returns fewer than `page_size` records (default `true`).
        #[serde(default = "default_true")]
        stop_when_short: bool,
        /// Expected page size, used for the short-page stop check.
        page_size: usize,
    },
}

/// Internal state tracked across pages.
#[derive(Debug, Default)]
pub struct PaginationState {
    pub page: usize,
    pub next_token: Option<String>,
    pub offset: usize,
    pub next_link: Option<String>,
    /// The previous page's token/link, used for loop detection.
    /// If `advance()` produces the same value twice in a row, pagination
    /// is stuck and we stop rather than looping forever.
    #[doc(hidden)]
    pub previous_token: Option<String>,
    /// Fingerprint of the previous page's body, used by `PageNumber` loop
    /// detection: APIs that clamp an out-of-range page to the last page and
    /// re-return it (non-empty) would otherwise loop until `max_pages`.
    #[doc(hidden)]
    pub previous_page_fingerprint: Option<u64>,
    /// Set by [`PaginationStyle::advance`] when the body-fingerprint stagnation
    /// guard fires: the page just handed to `advance` is a duplicate of the
    /// previous one and the caller must **drop** it rather than emit it a second
    /// time (audit #321 L1). Only the content-stagnation guards set it; a normal
    /// last-page stop leaves it `false` so the final page is still emitted.
    #[doc(hidden)]
    pub current_page_is_duplicate: bool,
    /// Running keyset cursor for [`PaginationStyle::RecordFieldCursor`] (#554):
    /// the aggregate (max/min) of the cursor field seen across pages so far.
    /// Injected into the next request; `None` until the first page is processed.
    #[doc(hidden)]
    pub record_field_cursor: Option<Value>,
}

/// Cheap, stable fingerprint of a response body for content-stagnation
/// loop detection.
fn body_fingerprint(body: &Value) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    body.to_string().hash(&mut h);
    h.finish()
}

/// Render a scalar cursor value for a query parameter: a string verbatim,
/// anything else via its JSON text form (numbers as `123`, bools as `true`).
pub(crate) fn value_to_param_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// Keep the max (or min) of two cursor values: numbers compare numerically,
/// strings lexicographically; a heterogeneous pair keeps the candidate.
fn pick_cursor(agg: RecordCursorAgg, current: Value, candidate: Value) -> Value {
    let candidate_wins = match (&current, &candidate) {
        (Value::Number(a), Value::Number(b)) => {
            let (a, b) = (
                a.as_f64().unwrap_or(f64::NAN),
                b.as_f64().unwrap_or(f64::NAN),
            );
            match agg {
                RecordCursorAgg::Max => b > a,
                RecordCursorAgg::Min => b < a,
            }
        }
        (Value::String(a), Value::String(b)) => match agg {
            RecordCursorAgg::Max => b > a,
            RecordCursorAgg::Min => b < a,
        },
        _ => true,
    };
    if candidate_wins { candidate } else { current }
}

impl PaginationStyle {
    pub fn apply_params(&self, params: &mut HashMap<String, String>, state: &PaginationState) {
        match self {
            PaginationStyle::None => {}
            PaginationStyle::Cursor { param_name, .. } => {
                cursor::apply_params(params, param_name, &state.next_token);
            }
            // The cursor is injected into the request body, not the query string.
            PaginationStyle::CursorInBody { .. } => {}
            PaginationStyle::LinkHeader => {}
            PaginationStyle::NextLinkInBody { .. } => {}
            PaginationStyle::PageNumber {
                param_name,
                start_page,
                page_size,
                page_size_param,
            } => {
                page::apply_params(
                    params,
                    param_name,
                    *start_page,
                    state.page,
                    *page_size,
                    page_size_param.as_deref(),
                );
            }
            PaginationStyle::Offset {
                offset_param,
                limit_param,
                limit,
                ..
            } => {
                offset::apply_params(params, offset_param, limit_param, state.offset, *limit);
            }
            // Offset/limit live in the request body, not the query string.
            PaginationStyle::OffsetInBody { .. } => {}
            // The keyset cursor is a query param only when `into: query`.
            PaginationStyle::RecordFieldCursor {
                into: RecordCursorTarget::Query,
                param,
                ..
            } => {
                if let Some(cursor) = &state.record_field_cursor {
                    params.insert(param.clone(), value_to_param_string(cursor));
                }
            }
            PaginationStyle::RecordFieldCursor { .. } => {}
        }
    }

    /// Advance pagination state based on the response body and headers.
    /// Returns `true` if there is a next page to fetch.
    ///
    /// Includes **loop detection**: if a cursor or next-link value is identical
    /// to the previous page's value, pagination stops with a warning instead of
    /// looping forever.
    pub fn advance(
        &self,
        body: &Value,
        headers: &HeaderMap,
        state: &mut PaginationState,
        record_count: usize,
    ) -> Result<bool, FaucetError> {
        match self {
            PaginationStyle::None => Ok(false),
            PaginationStyle::Cursor {
                next_token_path, ..
            } => {
                let has_next = cursor::advance(body, next_token_path, &mut state.next_token)?;
                if has_next {
                    if state.next_token == state.previous_token {
                        tracing::warn!(
                            "pagination loop detected: cursor {:?} repeated — stopping",
                            state.next_token
                        );
                        return Ok(false);
                    }
                    state.previous_token = state.next_token.clone();
                }
                Ok(has_next)
            }
            PaginationStyle::CursorInBody {
                next_token_path, ..
            } => {
                // Reuse the cursor extraction + loop guard; the only difference
                // from `Cursor` is where the cursor is applied (body, not param).
                let has_next = cursor::advance(body, next_token_path, &mut state.next_token)?;
                if has_next {
                    if state.next_token == state.previous_token {
                        tracing::warn!(
                            "pagination loop detected: body cursor {:?} repeated — stopping",
                            state.next_token
                        );
                        return Ok(false);
                    }
                    state.previous_token = state.next_token.clone();
                }
                Ok(has_next)
            }
            PaginationStyle::LinkHeader => match link_header::extract_next_link(headers) {
                Some(link) => {
                    let link = resolve_against_request(headers, &link)?;
                    if Some(&link) == state.previous_token.as_ref() {
                        tracing::warn!(
                            "pagination loop detected: link {link:?} repeated — stopping"
                        );
                        state.next_link = None;
                        return Ok(false);
                    }
                    state.previous_token = Some(link.clone());
                    state.next_link = Some(link);
                    Ok(true)
                }
                None => {
                    state.next_link = None;
                    Ok(false)
                }
            },
            PaginationStyle::NextLinkInBody { next_link_path } => {
                let has_next = next_link_body::advance(body, next_link_path, &mut state.next_link)?;
                if has_next {
                    if let Some(raw) = state.next_link.take() {
                        state.next_link =
                            Some(resolve_against_request(headers, &raw).map_err(|e| {
                                FaucetError::Source(format!(
                                    "next-link path '{next_link_path}' returned '{raw}': {e}"
                                ))
                            })?);
                    }
                    if state.next_link == state.previous_token {
                        tracing::warn!(
                            "pagination loop detected: next_link {:?} repeated — stopping",
                            state.next_link
                        );
                        return Ok(false);
                    }
                    state.previous_token = state.next_link.clone();
                }
                Ok(has_next)
            }
            PaginationStyle::PageNumber { .. } => {
                state.page += 1;
                if record_count == 0 {
                    return Ok(false);
                }
                // Content-stagnation guard: some APIs clamp an out-of-range
                // page to the last page and return it again (non-empty), which
                // would loop until `max_pages` and duplicate records. Stop if
                // this page's body is identical to the previous one (#78/#15).
                let fp = body_fingerprint(body);
                if state.previous_page_fingerprint == Some(fp) {
                    tracing::warn!(
                        "pagination loop detected: PageNumber returned an identical page — stopping"
                    );
                    // The current page IS the duplicate — signal the caller to
                    // drop it rather than emit it a second time (#321 L1).
                    state.current_page_is_duplicate = true;
                    return Ok(false);
                }
                state.previous_page_fingerprint = Some(fp);
                Ok(true)
            }
            PaginationStyle::Offset {
                limit, total_path, ..
            } => {
                let has_next = offset::advance(
                    body,
                    &mut state.offset,
                    record_count,
                    *limit,
                    total_path.as_deref(),
                )?;
                // Content-stagnation guard (#264 F18): a server that ignores
                // the `offset` parameter re-returns the identical first page
                // forever. With `total_path` absent (commonly omitted) the
                // record-count heuristic keeps `has_next` true on every full
                // page, so the run would loop until `max_pages`, duplicating
                // records to the sink. Mirror the PageNumber guard: stop if
                // this page's body is identical to the previous one. A
                // zero-record / short page has already returned `false` above,
                // so this only fires on a genuinely repeated full page.
                //
                // Scoped to `total_path.is_none()`: when `total_path` is set,
                // `offset::advance` has an authoritative stop condition (offset
                // reaches total), and a paging-metadata body that legitimately
                // repeats (e.g. `{"total": N}` echoed on every page) must not
                // be mistaken for stagnation.
                if has_next && total_path.is_none() {
                    let fp = body_fingerprint(body);
                    if state.previous_page_fingerprint == Some(fp) {
                        tracing::warn!(
                            "pagination loop detected: Offset returned an identical page \
                             (server likely ignoring the offset parameter) — stopping"
                        );
                        // Drop this duplicate page rather than emit it (#321 L1).
                        state.current_page_is_duplicate = true;
                        return Ok(false);
                    }
                    state.previous_page_fingerprint = Some(fp);
                }
                Ok(has_next)
            }
            PaginationStyle::OffsetInBody {
                limit,
                stop_when_short,
                rows_path,
                total_path,
                ..
            } => {
                // Mirror `Offset` (record-count driven), but the offset lands in
                // the body via `body_params`. A zero-row page always stops.
                let count = match rows_path {
                    Some(rp) => count_rows(body, rp)?,
                    None => record_count,
                };
                if count == 0 {
                    return Ok(false);
                }
                state.offset += count;
                if let Some(total) = total_path
                    .as_deref()
                    .map(|tp| read_total(body, tp))
                    .transpose()?
                    .flatten()
                {
                    if state.offset >= total {
                        return Ok(false);
                    }
                } else if *stop_when_short && count < *limit {
                    return Ok(false);
                }
                // Content-stagnation guard: a server ignoring the body offset
                // would re-return the identical page forever.
                let fp = body_fingerprint(body);
                if state.previous_page_fingerprint == Some(fp) {
                    tracing::warn!(
                        "pagination loop detected: OffsetInBody returned an identical page \
                         (server likely ignoring the body offset) — stopping"
                    );
                    state.current_page_is_duplicate = true;
                    return Ok(false);
                }
                state.previous_page_fingerprint = Some(fp);
                Ok(true)
            }
            PaginationStyle::RecordFieldCursor {
                page_size,
                stop_when_short,
                ..
            } => {
                // The keyset cursor itself was computed by `update_record_cursor`
                // (called with this page's records before `advance`). Here we only
                // decide whether to continue.
                if record_count == 0 {
                    return Ok(false);
                }
                if *stop_when_short && record_count < *page_size {
                    return Ok(false);
                }
                // Loop guard: if the cursor didn't advance this page, stop rather
                // than re-issue the identical request forever.
                let cursor = state
                    .record_field_cursor
                    .as_ref()
                    .map(value_to_param_string);
                if cursor.is_some() && cursor == state.previous_token {
                    tracing::warn!(
                        "pagination loop detected: RecordFieldCursor did not advance \
                         (cursor {cursor:?} repeated) — stopping"
                    );
                    return Ok(false);
                }
                state.previous_token = cursor;
                Ok(true)
            }
        }
    }

    /// Compute this page's keyset cursor for [`PaginationStyle::RecordFieldCursor`]
    /// (#554), merging the page aggregate of `field` into `state.record_field_cursor`.
    /// A no-op for every other style. Call this with the page's records *before*
    /// [`advance`](Self::advance).
    pub fn update_record_cursor(&self, records: &[Value], state: &mut PaginationState) {
        if let PaginationStyle::RecordFieldCursor { field, agg, .. } = self {
            let page_agg = records
                .iter()
                .filter_map(|r| r.get(field).cloned())
                .reduce(|a, b| pick_cursor(*agg, a, b));
            if let Some(page_agg) = page_agg {
                state.record_field_cursor = Some(match state.record_field_cursor.take() {
                    Some(prev) => pick_cursor(*agg, prev, page_agg),
                    None => page_agg,
                });
            }
        }
    }

    /// The JSONPath a cursor style reads its next-page token from
    /// ([`Cursor`](Self::Cursor) / [`CursorInBody`](Self::CursorInBody)); used by
    /// the resumable-cursor bookmark (#547). `None` for every other style.
    pub fn cursor_path(&self) -> Option<&str> {
        match self {
            PaginationStyle::Cursor {
                next_token_path, ..
            }
            | PaginationStyle::CursorInBody {
                next_token_path, ..
            } => Some(next_token_path),
            _ => None,
        }
    }

    /// Request-body fields to inject for body-carrying pagination styles
    /// (`CursorInBody`, `OffsetInBody`, and `RecordFieldCursor` with `into: body`).
    /// Empty for every other style, and for the first page of a cursor style
    /// (nothing extracted yet). Supersedes [`body_cursor`](Self::body_cursor),
    /// which is retained for API compatibility.
    pub fn body_params(&self, state: &PaginationState) -> Vec<(String, Value)> {
        match self {
            PaginationStyle::CursorInBody {
                body_cursor_field, ..
            } => state
                .next_token
                .as_deref()
                .map(|tok| vec![(body_cursor_field.clone(), Value::String(tok.to_owned()))])
                .unwrap_or_default(),
            PaginationStyle::OffsetInBody {
                offset_field,
                limit_field,
                limit,
                ..
            } => vec![
                (offset_field.clone(), Value::from(state.offset as u64)),
                (limit_field.clone(), Value::from(*limit as u64)),
            ],
            PaginationStyle::RecordFieldCursor {
                into: RecordCursorTarget::Body,
                param,
                ..
            } => state
                .record_field_cursor
                .clone()
                .map(|c| vec![(param.clone(), c)])
                .unwrap_or_default(),
            _ => Vec::new(),
        }
    }

    /// For [`PaginationStyle::CursorInBody`], the `(body_cursor_field, cursor)`
    /// to inject into the next request's JSON body — `Some` only once a cursor
    /// has been extracted (i.e. from the second page on). Every other style, and
    /// the first page of `CursorInBody`, returns `None` (the request body is
    /// used unchanged).
    pub fn body_cursor<'a>(&'a self, state: &'a PaginationState) -> Option<(&'a str, &'a str)> {
        match self {
            PaginationStyle::CursorInBody {
                body_cursor_field, ..
            } => state
                .next_token
                .as_deref()
                .map(|tok| (body_cursor_field.as_str(), tok)),
            _ => None,
        }
    }
}

/// Rows matched by `path` in `body`: an array's length, 1 for any other value,
/// 0 for null or no match.
fn count_rows(body: &Value, path: &str) -> Result<usize, FaucetError> {
    use jsonpath_rust::JsonPath;
    let hits = body
        .query(path)
        .map_err(|e| FaucetError::JsonPath(format!("rows_path '{path}': {e}")))?;
    Ok(match hits.as_slice() {
        [] => 0,
        [Value::Array(a)] => a.len(),
        [Value::Null] => 0,
        many => many.iter().filter(|v| !v.is_null()).count(),
    })
}

/// The numeric total at `path`, or `None` (with a warning when non-numeric).
fn read_total(body: &Value, path: &str) -> Result<Option<usize>, FaucetError> {
    use jsonpath_rust::JsonPath;
    let hits = body
        .query(path)
        .map_err(|e| FaucetError::JsonPath(format!("total_path '{path}': {e}")))?;
    let Some(v) = hits.first() else {
        return Ok(None);
    };
    let n = v
        .as_u64()
        .or_else(|| v.as_str().and_then(|s| s.trim().parse().ok()));
    if n.is_none() {
        tracing::warn!(
            "total_path '{path}' resolved to non-numeric {v}; using the short-page rule"
        );
    }
    Ok(n.map(|n| n as usize))
}

#[cfg(test)]
mod new_style_tests {
    use super::*;
    use reqwest::header::HeaderMap;
    use serde_json::json;

    fn offset_in_body() -> PaginationStyle {
        PaginationStyle::OffsetInBody {
            offset_field: "offset".into(),
            limit_field: "limit".into(),
            limit: 2,
            stop_when_short: true,
            rows_path: None,
            total_path: None,
        }
    }

    #[test]
    fn offset_in_body_writes_offset_and_limit_and_advances() {
        let style = offset_in_body();
        let mut state = PaginationState::default();

        // Page 1: offset 0, limit 2.
        let bp = style.body_params(&state);
        assert_eq!(
            bp,
            vec![("offset".into(), json!(0)), ("limit".into(), json!(2))]
        );
        // apply_params must NOT touch the query string.
        let mut params = HashMap::new();
        style.apply_params(&mut params, &state);
        assert!(params.is_empty());

        // A full page → advance, offset += 2.
        let body = json!([{"id": 1}, {"id": 2}]);
        assert!(
            style
                .advance(&body, &HeaderMap::new(), &mut state, 2)
                .unwrap()
        );
        assert_eq!(state.offset, 2);
        let bp = style.body_params(&state);
        assert_eq!(bp[0], ("offset".into(), json!(2)));

        // A short page ends pagination.
        let body2 = json!([{"id": 3}]);
        assert!(
            !style
                .advance(&body2, &HeaderMap::new(), &mut state, 1)
                .unwrap()
        );
        assert_eq!(state.offset, 3);
    }

    #[test]
    fn offset_in_body_zero_records_stops() {
        let style = offset_in_body();
        let mut state = PaginationState::default();
        assert!(
            !style
                .advance(&json!([]), &HeaderMap::new(), &mut state, 0)
                .unwrap()
        );
    }

    #[test]
    fn offset_in_body_stagnation_guard_stops_when_short_disabled() {
        let style = PaginationStyle::OffsetInBody {
            offset_field: "o".into(),
            limit_field: "l".into(),
            limit: 2,
            stop_when_short: false,
            rows_path: None,
            total_path: None,
        };
        let mut state = PaginationState::default();
        let body = json!([{"id": 1}, {"id": 2}]);
        // First full page → continue.
        assert!(
            style
                .advance(&body, &HeaderMap::new(), &mut state, 2)
                .unwrap()
        );
        // Identical page again (server ignored offset) → stop + mark duplicate.
        assert!(
            !style
                .advance(&body, &HeaderMap::new(), &mut state, 2)
                .unwrap()
        );
        assert!(state.current_page_is_duplicate);
    }

    #[test]
    fn rows_path_counts_and_total_path_stops() {
        let style = PaginationStyle::OffsetInBody {
            offset_field: "o".into(),
            limit_field: "l".into(),
            limit: 2,
            stop_when_short: true,
            rows_path: Some("$.rows".into()),
            total_path: Some("$.total".into()),
        };
        let h = HeaderMap::new();
        let mut state = PaginationState::default();
        let body = json!({"rows": [1, 2], "total": "5"});
        assert!(style.advance(&body, &h, &mut state, 1).unwrap());
        assert_eq!(state.offset, 2);
        // A short page below the total continues: the total is authoritative.
        let body = json!({"rows": [3], "total": 5});
        assert!(style.advance(&body, &h, &mut state, 1).unwrap());
        let body = json!({"rows": [4, 5], "total": 5});
        assert!(!style.advance(&body, &h, &mut state, 1).unwrap());
        assert_eq!(state.offset, 5);
        // A non-numeric total falls back to the short-page rule.
        let mut state = PaginationState::default();
        let body = json!({"rows": [1], "total": "many"});
        assert!(!style.advance(&body, &h, &mut state, 1).unwrap());
        // Missing rows end pagination.
        let mut state = PaginationState::default();
        assert!(!style.advance(&json!({}), &h, &mut state, 1).unwrap());
    }

    #[test]
    fn count_rows_shapes() {
        assert_eq!(count_rows(&json!({"r": [1, 2, 3]}), "$.r").unwrap(), 3);
        assert_eq!(count_rows(&json!({"r": {"a": 1}}), "$.r").unwrap(), 1);
        assert_eq!(count_rows(&json!({"r": null}), "$.r").unwrap(), 0);
        assert_eq!(count_rows(&json!({}), "$.r").unwrap(), 0);
        assert_eq!(
            count_rows(&json!({"r": [1, null, 2]}), "$.r[*]").unwrap(),
            2
        );
        assert!(count_rows(&json!({}), "$[").is_err());
        assert_eq!(read_total(&json!({}), "$.t").unwrap(), None);
        assert!(read_total(&json!({}), "$[").is_err());
    }

    fn keyset(into: RecordCursorTarget) -> PaginationStyle {
        PaginationStyle::RecordFieldCursor {
            field: "JournalNumber".into(),
            into,
            param: "offset".into(),
            agg: RecordCursorAgg::Max,
            stop_when_short: true,
            page_size: 2,
        }
    }

    #[test]
    fn record_field_cursor_computes_max_and_injects_query() {
        let style = keyset(RecordCursorTarget::Query);
        let mut state = PaginationState::default();

        // Page 1: no cursor yet → no query param.
        let mut params = HashMap::new();
        style.apply_params(&mut params, &state);
        assert!(!params.contains_key("offset"));

        let page = vec![json!({"JournalNumber": 10}), json!({"JournalNumber": 25})];
        style.update_record_cursor(&page, &mut state);
        assert_eq!(state.record_field_cursor, Some(json!(25)));

        // Full page → continue.
        assert!(
            style
                .advance(&json!({}), &HeaderMap::new(), &mut state, 2)
                .unwrap()
        );
        // Next request carries the max as the offset param.
        let mut params = HashMap::new();
        style.apply_params(&mut params, &state);
        assert_eq!(params.get("offset").unwrap(), "25");

        // A later lower page does not move a `max` cursor backwards.
        let page2 = vec![json!({"JournalNumber": 5})];
        style.update_record_cursor(&page2, &mut state);
        assert_eq!(state.record_field_cursor, Some(json!(25)));
    }

    #[test]
    fn record_field_cursor_into_body() {
        let style = keyset(RecordCursorTarget::Body);
        let mut state = PaginationState::default();
        assert!(style.body_params(&state).is_empty());
        style.update_record_cursor(&[json!({"JournalNumber": 7})], &mut state);
        assert_eq!(style.body_params(&state), vec![("offset".into(), json!(7))]);
        // apply_params does not inject into the query for `into: body`.
        let mut params = HashMap::new();
        style.apply_params(&mut params, &state);
        assert!(params.is_empty());
    }

    #[test]
    fn record_field_cursor_stops_on_short_page_and_non_advance() {
        // Short page ends pagination.
        let style = keyset(RecordCursorTarget::Query);
        let mut state = PaginationState::default();
        style.update_record_cursor(&[json!({"JournalNumber": 3})], &mut state);
        assert!(
            !style
                .advance(&json!({}), &HeaderMap::new(), &mut state, 1)
                .unwrap()
        );

        // Non-advancing cursor trips the loop guard.
        let mut state = PaginationState::default();
        let page = vec![json!({"JournalNumber": 9}), json!({"JournalNumber": 9})];
        style.update_record_cursor(&page, &mut state);
        assert!(
            style
                .advance(&json!({}), &HeaderMap::new(), &mut state, 2)
                .unwrap()
        );
        // Same max again → stop.
        style.update_record_cursor(&page, &mut state);
        assert!(
            !style
                .advance(&json!({}), &HeaderMap::new(), &mut state, 2)
                .unwrap()
        );
    }

    #[test]
    fn record_field_cursor_min_agg() {
        let style = PaginationStyle::RecordFieldCursor {
            field: "seq".into(),
            into: RecordCursorTarget::Query,
            param: "before".into(),
            agg: RecordCursorAgg::Min,
            stop_when_short: true,
            page_size: 2,
        };
        let mut state = PaginationState::default();
        style.update_record_cursor(&[json!({"seq": 10}), json!({"seq": 4})], &mut state);
        assert_eq!(state.record_field_cursor, Some(json!(4)));
        style.update_record_cursor(&[json!({"seq": 2})], &mut state);
        assert_eq!(state.record_field_cursor, Some(json!(2)));
    }

    #[test]
    fn cursor_path_only_for_cursor_styles() {
        assert_eq!(
            PaginationStyle::Cursor {
                next_token_path: "$.n".into(),
                param_name: "c".into(),
            }
            .cursor_path(),
            Some("$.n")
        );
        assert_eq!(
            PaginationStyle::CursorInBody {
                next_token_path: "$.p.next".into(),
                body_cursor_field: "after".into(),
            }
            .cursor_path(),
            Some("$.p.next")
        );
        assert_eq!(offset_in_body().cursor_path(), None);
    }
}
