//! Lossless cursor bookmarks for SQL query sources that read in cursor order
//! (#825).
//!
//! A source orders its query by the cursor column and filters it with
//! `cursor >= bookmark`. The bookmark records the last cursor value emitted
//! **and** a fingerprint of every row already emitted at that value, so a
//! resumed read re-reads the boundary value and drops only the rows it has
//! already written:
//!
//! - rows that share the boundary value but committed after the last read are
//!   not skipped (a strict `>` would skip them);
//! - a boundary row updated in place without its cursor changing has a new
//!   fingerprint and is emitted again;
//! - because the query is ordered, every page can carry a bookmark, so a crash
//!   replays at most the page that was being written.
//!
//! A fingerprint covers the whole row (sorted field names, 128-bit FNV-1a), so
//! no key column is needed. When more than [`MAX_BOUNDARY_ROWS`] rows share the
//! boundary value the set is dropped and the next read re-emits every row at
//! that value: duplicates, never loss.

use super::{Decimal, Instant, numeric_text};
use crate::diff::{FNV_BASIS_A, FNV_BASIS_B, fnv1a_64};
use crate::error::FaucetError;
use serde_json::{Map, Value};
use std::collections::HashSet;

/// The most row fingerprints a bookmark keeps for its boundary value.
pub const MAX_BOUNDARY_ROWS: usize = 10_000;

/// A cursor position: the last cursor value emitted and the rows already
/// emitted at it.
///
/// Stored as `{"value": <cursor>, "boundary": ["<fingerprint>", …]}`;
/// `"boundary": null` means the set overflowed and every row at `value` is
/// read again. A bare value (an older or hand-set bookmark) reads as that
/// value with an empty boundary.
#[derive(Debug, Clone, PartialEq)]
pub struct CursorBookmark {
    /// The last cursor value emitted (or the configured initial value).
    pub value: Value,
    /// Fingerprints of the rows emitted at `value`; `None` after an overflow.
    pub boundary: Option<Vec<String>>,
}

impl CursorBookmark {
    /// A starting position with nothing emitted yet.
    pub fn initial(value: Value) -> Self {
        Self {
            value,
            boundary: Some(Vec::new()),
        }
    }

    /// Read a stored bookmark: the structured form, or a bare value.
    pub fn from_state(state: Value) -> Result<Self, FaucetError> {
        let mut o = match state {
            Value::Object(o)
                if o.contains_key("value") && o.keys().all(|k| k == "value" || k == "boundary") =>
            {
                o
            }
            other => return Ok(Self::initial(other)),
        };
        let value = o.remove("value").unwrap_or(Value::Null);
        let boundary = match o.remove("boundary") {
            None => Some(Vec::new()),
            Some(Value::Null) => None,
            Some(Value::Array(items)) => Some(
                items
                    .into_iter()
                    .map(|v| match v {
                        Value::String(s) => Ok(s),
                        other => Err(FaucetError::State(format!(
                            "cursor bookmark: boundary entries must be strings, found {other}"
                        ))),
                    })
                    .collect::<Result<_, _>>()?,
            ),
            Some(other) => {
                return Err(FaucetError::State(format!(
                    "cursor bookmark: `boundary` must be an array or null, found {other}"
                )));
            }
        };
        if value.is_null() {
            return Err(FaucetError::State(
                "cursor bookmark: `value` must not be null".into(),
            ));
        }
        Ok(Self { value, boundary })
    }

    /// The stored form.
    pub fn to_value(&self) -> Value {
        let mut o = Map::new();
        o.insert("value".into(), self.value.clone());
        o.insert(
            "boundary".into(),
            match &self.boundary {
                Some(b) => Value::Array(b.iter().cloned().map(Value::String).collect()),
                None => Value::Null,
            },
        );
        Value::Object(o)
    }
}

/// Filters rows read in cursor order against a starting [`CursorBookmark`]
/// and tracks the position after the last row admitted.
#[derive(Debug)]
pub struct CursorTracker {
    column: String,
    start: Value,
    start_boundary: Option<HashSet<String>>,
    value: Value,
    boundary: Option<HashSet<String>>,
    missing: usize,
    overflow_warned: bool,
}

impl CursorTracker {
    /// Track `column` from `start`.
    pub fn new(column: impl Into<String>, start: CursorBookmark) -> Self {
        let start_boundary: Option<HashSet<String>> =
            start.boundary.map(|b| b.into_iter().collect());
        Self {
            column: column.into(),
            value: start.value.clone(),
            boundary: start_boundary.clone(),
            start: start.value,
            start_boundary,
            missing: 0,
            overflow_warned: false,
        }
    }

    /// Whether `row` is new. Rows already emitted at the starting value and
    /// rows provably before it are refused; a row without a cursor value is
    /// kept (and counted) without moving the position.
    pub fn admit(&mut self, row: &Value) -> bool {
        let Some(v) = row.get(&self.column).filter(|v| !v.is_null()) else {
            self.missing += 1;
            return true;
        };
        let fingerprint = row_fingerprint(row);
        if *v == self.start {
            if self
                .start_boundary
                .as_ref()
                .is_some_and(|b| b.contains(&fingerprint))
            {
                return false;
            }
        } else if before(v, &self.start) {
            return false;
        }
        if *v == self.value {
            if let Some(b) = self.boundary.as_mut() {
                b.insert(fingerprint);
                if b.len() > MAX_BOUNDARY_ROWS {
                    self.boundary = None;
                    if !self.overflow_warned {
                        self.overflow_warned = true;
                        tracing::warn!(
                            column = %self.column,
                            limit = MAX_BOUNDARY_ROWS,
                            "incremental replication: more rows share one cursor value than \
                             the bookmark tracks; a resumed read re-emits the rows at that value"
                        );
                    }
                }
            }
        } else {
            self.value = v.clone();
            self.boundary = Some(HashSet::from([fingerprint]));
        }
        true
    }

    /// The position after every row admitted so far.
    pub fn bookmark(&self) -> CursorBookmark {
        CursorBookmark {
            value: self.value.clone(),
            boundary: self.boundary.as_ref().map(|b| {
                let mut v: Vec<String> = b.iter().cloned().collect();
                v.sort_unstable();
                v
            }),
        }
    }

    /// Rows admitted without a cursor value.
    pub fn missing(&self) -> usize {
        self.missing
    }
}

/// Whether `v` orders strictly before `start` by what both hold (numbers or
/// decimal strings, instants of the same kind, ISO dates). Other values are
/// left to the server's ordering, whose collation the client cannot see.
fn before(v: &Value, start: &Value) -> bool {
    if let (Some(a), Some(b)) = (numeric_text(v), numeric_text(start))
        && let (Some(a), Some(b)) = (Decimal::parse(&a), Decimal::parse(&b))
    {
        return a < b;
    }
    let (Value::String(a), Value::String(b)) = (v, start) else {
        return false;
    };
    match (Instant::parse(a), Instant::parse(b)) {
        (Some(x @ Instant::Zoned(_)), Some(y @ Instant::Zoned(_)))
        | (Some(x @ Instant::Naive(_)), Some(y @ Instant::Naive(_))) => return x < y,
        _ => {}
    }
    let date = |s: &str| chrono::NaiveDate::parse_from_str(s.trim(), "%Y-%m-%d").ok();
    matches!((date(a), date(b)), (Some(x), Some(y)) if x < y)
}

/// A 128-bit fingerprint of a whole row, independent of field order.
pub fn row_fingerprint(row: &Value) -> String {
    let mut text = String::new();
    canonical(row, &mut text);
    let a = fnv1a_64(text.as_bytes(), FNV_BASIS_A);
    let b = fnv1a_64(text.as_bytes(), FNV_BASIS_B);
    format!("{a:016x}{b:016x}")
}

fn canonical(v: &Value, out: &mut String) {
    match v {
        Value::Object(o) => {
            let mut keys: Vec<&String> = o.keys().collect();
            keys.sort_unstable();
            out.push('{');
            for (i, k) in keys.into_iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&Value::String(k.clone()).to_string());
                out.push(':');
                canonical(&o[k], out);
            }
            out.push('}');
        }
        Value::Array(a) => {
            out.push('[');
            for (i, x) in a.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                canonical(x, out);
            }
            out.push(']');
        }
        scalar => out.push_str(&scalar.to_string()),
    }
}

/// Whether `token` is compared with a strict `>` (or `<` on its left) anywhere
/// in `query`. A cursor read must use `>=`: the boundary value is re-read and
/// deduplicated, and a strict comparison skips rows that share it.
pub fn strict_comparison(query: &str, token: &str) -> bool {
    query.match_indices(token).any(|(i, _)| {
        let before = query[..i].trim_end();
        let after = query[i + token.len()..].trim_start();
        (before.ends_with('>') && !before.ends_with("<>"))
            || (after.starts_with('<') && !after.starts_with("<=") && !after.starts_with("<>"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn tracker(start: Value) -> CursorTracker {
        CursorTracker::new("ts", CursorBookmark::initial(start))
    }

    #[test]
    fn bookmark_round_trips_and_reads_bare_values() {
        let b = CursorBookmark {
            value: json!("2024-01-01T00:00:00+00:00"),
            boundary: Some(vec!["a".into()]),
        };
        assert_eq!(CursorBookmark::from_state(b.to_value()).unwrap(), b);
        let overflowed = CursorBookmark {
            value: json!(5),
            boundary: None,
        };
        assert_eq!(
            CursorBookmark::from_state(overflowed.to_value()).unwrap(),
            overflowed
        );
        assert_eq!(
            CursorBookmark::from_state(json!(7)).unwrap(),
            CursorBookmark::initial(json!(7))
        );
        assert_eq!(
            CursorBookmark::from_state(json!({"value": 3})).unwrap(),
            CursorBookmark::initial(json!(3))
        );
        assert_eq!(
            CursorBookmark::from_state(json!({"value": 3, "other": 1})).unwrap(),
            CursorBookmark::initial(json!({"value": 3, "other": 1}))
        );
    }

    #[test]
    fn malformed_bookmarks_are_state_errors() {
        for bad in [
            json!({"value": 1, "boundary": [1]}),
            json!({"value": 1, "boundary": "x"}),
            json!({"value": null, "boundary": []}),
        ] {
            let err = CursorBookmark::from_state(bad).unwrap_err();
            assert!(matches!(err, FaucetError::State(_)), "{err}");
        }
    }

    #[test]
    fn rows_sharing_the_boundary_value_are_never_skipped() {
        let a = json!({"id": 1, "ts": 10});
        let b = json!({"id": 2, "ts": 10});
        let mut t = tracker(json!(0));
        assert!(t.admit(&a));
        let saved = t.bookmark();
        assert_eq!(saved.value, json!(10));

        // A later read sees the same row again plus one that committed late
        // at the same value.
        let mut t = CursorTracker::new("ts", saved);
        assert!(!t.admit(&a), "already emitted");
        assert!(t.admit(&b), "late row at the boundary value");
        let next = t.bookmark();
        assert_eq!(next.boundary.as_ref().unwrap().len(), 2);

        let mut t = CursorTracker::new("ts", next);
        assert!(!t.admit(&a));
        assert!(!t.admit(&b));
        assert!(t.admit(&json!({"id": 3, "ts": 11})));
        assert_eq!(t.bookmark().value, json!(11));
        assert_eq!(t.bookmark().boundary.unwrap().len(), 1);
    }

    #[test]
    fn a_boundary_row_updated_in_place_is_emitted_again() {
        let mut t = tracker(json!(0));
        assert!(t.admit(&json!({"id": 1, "ts": 10, "v": "a"})));
        let mut t = CursorTracker::new("ts", t.bookmark());
        assert!(t.admit(&json!({"id": 1, "ts": 10, "v": "b"})));
    }

    #[test]
    fn rows_before_the_start_are_refused_only_when_comparable() {
        let mut t = tracker(json!("2024-01-02T00:00:00Z"));
        assert!(!t.admit(&json!({"ts": "2024-01-01T23:00:00+00:00"})));
        assert!(
            t.admit(&json!({"ts": "2024-01-02T01:00:00+01:00"})),
            "same instant"
        );
        let mut t = tracker(json!("12.5"));
        assert!(!t.admit(&json!({"ts": 12})));
        assert!(t.admit(&json!({"ts": "12.50"})));
        let mut t = tracker(json!("2024-01-02 00:00:00"));
        assert!(!t.admit(&json!({"ts": "2024-01-01 10:00:00"})));
        let mut t = tracker(json!("2024-01-02"));
        assert!(!t.admit(&json!({"ts": "2024-01-01"})));
        let mut t = tracker(json!("b"));
        assert!(
            t.admit(&json!({"ts": "A"})),
            "collation is the server's call"
        );
        let mut t = tracker(json!("2024-01-02T00:00:00Z"));
        assert!(
            t.admit(&json!({"ts": "2024-01-01 00:00:00"})),
            "naive vs zoned"
        );
    }

    #[test]
    fn rows_without_a_cursor_are_kept_and_counted() {
        let mut t = tracker(json!(5));
        assert!(t.admit(&json!({"id": 1})));
        assert!(t.admit(&json!({"id": 2, "ts": null})));
        assert_eq!(t.missing(), 2);
        assert_eq!(t.bookmark(), CursorBookmark::initial(json!(5)));
    }

    #[test]
    fn an_overflowing_boundary_falls_back_to_rereading() {
        let mut t = tracker(json!(0));
        for i in 0..=MAX_BOUNDARY_ROWS {
            assert!(t.admit(&json!({"id": i, "ts": 1})));
        }
        let b = t.bookmark();
        assert_eq!(b.boundary, None);
        assert!(t.admit(&json!({"id": -1, "ts": 1})));
        let mut t = CursorTracker::new("ts", b);
        assert!(t.admit(&json!({"id": 0, "ts": 1})), "re-read, not lost");
        assert!(t.admit(&json!({"id": 0, "ts": 2})));
        assert_eq!(t.bookmark().boundary.unwrap().len(), 1);
    }

    #[test]
    fn fingerprints_ignore_field_order_but_not_content() {
        let a = json!({"a": 1, "b": {"y": [1, "x"], "x": null}});
        let b = json!({"b": {"x": null, "y": [1, "x"]}, "a": 1});
        assert_eq!(row_fingerprint(&a), row_fingerprint(&b));
        assert_ne!(row_fingerprint(&a), row_fingerprint(&json!({"a": 2})));
        assert_eq!(row_fingerprint(&a).len(), 32);
    }

    #[test]
    fn strict_comparisons_with_the_token_are_found() {
        let t = "${bookmark}";
        assert!(strict_comparison("WHERE ts > ${bookmark}", t));
        assert!(strict_comparison("WHERE ${bookmark} < ts", t));
        assert!(!strict_comparison("WHERE ts >= ${bookmark}", t));
        assert!(!strict_comparison("WHERE ${bookmark} <= ts", t));
        assert!(!strict_comparison("WHERE ts <> ${bookmark}", t));
        assert!(!strict_comparison("WHERE ${bookmark} <> ts", t));
        assert!(!strict_comparison("SELECT 1", t));
        assert!(strict_comparison(
            "a >= @bookmark AND b > @bookmark",
            "@bookmark"
        ));
    }
}
