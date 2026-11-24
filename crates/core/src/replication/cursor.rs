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
//! no key column is needed. Identical rows at the boundary are counted (the
//! fingerprint repeats in the bookmark), so a late copy of an emitted row is
//! still delivered. Every row the server returns past the boundary is kept:
//! the server filtered and ordered it by the column's own type and collation.
//! When more than [`MAX_BOUNDARY_ROWS`] rows share the boundary value the set
//! is dropped and the next read re-emits every row at that value: duplicates,
//! never loss.

use crate::diff::{FNV_BASIS_A, FNV_BASIS_B, fnv1a_64};
use crate::error::FaucetError;
use serde_json::{Map, Value};
use std::collections::HashMap;

/// The most row fingerprints a bookmark keeps for its boundary value.
pub const MAX_BOUNDARY_ROWS: usize = 10_000;

/// A cursor position: the last cursor value emitted and the rows already
/// emitted at it.
///
/// Stored as `{"value": <cursor>, "boundary": ["<fingerprint>", …]}`, a
/// fingerprint repeated once per identical row; `"boundary": null` means the set overflowed and every row at `value` is
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

/// The rows emitted at one cursor value: a count per fingerprint, plus any
/// stored entries that are not fingerprints this code writes (kept as found).
#[derive(Debug, Clone, Default)]
struct Boundary {
    rows: HashMap<u128, usize>,
    opaque: Vec<String>,
}

impl Boundary {
    fn from_stored(items: Vec<String>) -> Self {
        let mut boundary = Self::default();
        for item in items {
            match parse_fingerprint(&item) {
                Some(fp) => *boundary.rows.entry(fp).or_insert(0) += 1,
                None => boundary.opaque.push(item),
            }
        }
        boundary
    }

    fn len(&self) -> usize {
        self.rows.values().sum::<usize>() + self.opaque.len()
    }

    fn stored(&self) -> Vec<String> {
        let mut v: Vec<String> = self
            .rows
            .iter()
            .flat_map(|(fp, n)| std::iter::repeat_n(fingerprint_text(*fp), *n))
            .chain(self.opaque.iter().cloned())
            .collect();
        v.sort_unstable();
        v
    }
}

fn fingerprint_text(fp: u128) -> String {
    format!("{fp:032x}")
}

/// The fingerprint a stored entry spells, when it is spelled as
/// [`row_fingerprint`] writes it.
fn parse_fingerprint(text: &str) -> Option<u128> {
    if text.len() != 32 || !text.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
        return None;
    }
    u128::from_str_radix(text, 16).ok()
}

/// Filters rows read in cursor order against a starting [`CursorBookmark`]
/// and tracks the position after the last row admitted.
#[derive(Debug)]
pub struct CursorTracker {
    column: String,
    start: Value,
    /// Copies of each row still to refuse at the starting value.
    start_boundary: Option<HashMap<u128, usize>>,
    value: Value,
    boundary: Option<Boundary>,
    boundary_rows: usize,
    missing: usize,
    overflow_warned: bool,
}

impl CursorTracker {
    /// Track `column` from `start`.
    pub fn new(column: impl Into<String>, start: CursorBookmark) -> Self {
        let boundary = start.boundary.map(Boundary::from_stored);
        let boundary_rows = boundary.as_ref().map_or(0, Boundary::len);
        Self {
            boundary_rows,
            column: column.into(),
            value: start.value.clone(),
            start_boundary: boundary.as_ref().map(|b| b.rows.clone()),
            boundary,
            start: start.value,
            missing: 0,
            overflow_warned: false,
        }
    }

    /// Whether `row` is new. Copies of rows already emitted at the starting
    /// value are refused, up to the number emitted; a row without a cursor
    /// value is kept (and counted) without moving the position.
    pub fn admit(&mut self, row: &Value) -> bool {
        let Some(v) = row.get(&self.column).filter(|v| !v.is_null()) else {
            self.missing += 1;
            return true;
        };
        let fingerprint = fingerprint(row);
        if *v == self.start
            && let Some(left) = self
                .start_boundary
                .as_mut()
                .and_then(|b| b.get_mut(&fingerprint))
                .filter(|n| **n > 0)
        {
            *left -= 1;
            return false;
        }
        if *v == self.value {
            if let Some(b) = self.boundary.as_mut() {
                *b.rows.entry(fingerprint).or_insert(0) += 1;
                self.boundary_rows += 1;
                if self.boundary_rows > MAX_BOUNDARY_ROWS {
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
            self.boundary = Some(Boundary {
                rows: HashMap::from([(fingerprint, 1)]),
                opaque: Vec::new(),
            });
            self.boundary_rows = 1;
        }
        true
    }

    /// The position after every row admitted so far.
    pub fn bookmark(&self) -> CursorBookmark {
        CursorBookmark {
            value: self.value.clone(),
            boundary: self.boundary.as_ref().map(Boundary::stored),
        }
    }

    /// Rows admitted without a cursor value.
    pub fn missing(&self) -> usize {
        self.missing
    }
}

/// A 128-bit fingerprint of a whole row, independent of field order.
pub fn row_fingerprint(row: &Value) -> String {
    fingerprint_text(fingerprint(row))
}

/// [`row_fingerprint`] as a number: two FNV-1a hashes of the row's canonical
/// text, fed as it is produced.
fn fingerprint(row: &Value) -> u128 {
    let mut hash = Hash2 {
        a: FNV_BASIS_A,
        b: FNV_BASIS_B,
    };
    canonical(row, &mut hash);
    (u128::from(hash.a) << 64) | u128::from(hash.b)
}

struct Hash2 {
    a: u64,
    b: u64,
}

impl Hash2 {
    fn feed(&mut self, bytes: &[u8]) {
        self.a = fnv1a_64(bytes, self.a);
        self.b = fnv1a_64(bytes, self.b);
    }

    /// Feed `value`'s compact JSON text.
    fn json<T: serde::Serialize + ?Sized>(&mut self, value: &T) {
        // The writer never fails and these values always serialize.
        let _ = serde_json::to_writer(self, value);
    }
}

impl std::io::Write for Hash2 {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.feed(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Objects up to this many fields are ordered without a heap allocation.
const INLINE_FIELDS: usize = 32;
static NULL: Value = Value::Null;

fn canonical(v: &Value, out: &mut Hash2) {
    match v {
        Value::Object(o) => {
            let mut inline = [("", &NULL); INLINE_FIELDS];
            let mut heap;
            let fields: &mut [(&str, &Value)] = if o.len() <= INLINE_FIELDS {
                for (slot, (k, v)) in inline.iter_mut().zip(o) {
                    *slot = (k.as_str(), v);
                }
                &mut inline[..o.len()]
            } else {
                heap = o.iter().map(|(k, v)| (k.as_str(), v)).collect::<Vec<_>>();
                &mut heap
            };
            if !fields.is_sorted_by(|a, b| a.0 <= b.0) {
                fields.sort_unstable_by(|a, b| a.0.cmp(b.0));
            }
            out.feed(b"{");
            for (i, (k, v)) in fields.iter().enumerate() {
                if i > 0 {
                    out.feed(b",");
                }
                out.json(*k);
                out.feed(b":");
                canonical(v, out);
            }
            out.feed(b"}");
        }
        Value::Array(a) => {
            out.feed(b"[");
            for (i, x) in a.iter().enumerate() {
                if i > 0 {
                    out.feed(b",");
                }
                canonical(x, out);
            }
            out.feed(b"]");
        }
        scalar => out.json(scalar),
    }
}

/// Whether `token` is compared with a strict `>` (or `<` on its left) anywhere
/// in `query`. A cursor read must use `>=`: the boundary value is re-read and
/// deduplicated, and a strict comparison skips rows that share it.
///
/// The comparison is the nearest operator on either side of the token within
/// its predicate, looking through wrappers such as `CAST(… AS t)`, `(…)`,
/// `TIMESTAMP …`, `to_timestamp(…)` and `…::type`; a match must end on an
/// identifier boundary (`@bookmark` does not match `@bookmark_old`).
pub fn strict_comparison(query: &str, token: &str) -> bool {
    if token.is_empty() {
        return false;
    }
    query.match_indices(token).any(|(i, _)| {
        let end = i + token.len();
        let ident_tail = token.ends_with(is_ident_char);
        if ident_tail && query[end..].starts_with(is_ident_char) {
            return false;
        }
        nearest_operator_left(&query[..i]).as_deref() == Some(">")
            || nearest_operator_right(&query[end..]).as_deref() == Some("<")
    })
}

fn is_ident_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

fn is_operator_char(c: char) -> bool {
    matches!(c, '<' | '>' | '=' | '!' | '-')
}

/// Words that end the predicate the token sits in.
fn is_boundary_word(w: &str) -> bool {
    const WORDS: &[&str] = &[
        "and", "or", "not", "where", "on", "when", "then", "else", "having", "select", "from",
        "between", "like", "in", "is", "case", "join", "by", "set", "values",
    ];
    WORDS.iter().any(|k| w.eq_ignore_ascii_case(k))
}

/// An operator run that is not a comparison (`->`, `->>`, `=>`, `--`).
fn non_comparison(op: &str) -> bool {
    op.contains('-') || op.ends_with("=>")
}

/// The comparison operator closest before the token, or `None` when the
/// predicate starts first.
fn nearest_operator_left(text: &str) -> Option<String> {
    let chars: Vec<char> = text.chars().collect();
    let mut j = chars.len();
    while j > 0 {
        let c = chars[j - 1];
        if c == ',' || c == ';' {
            return None;
        }
        if is_operator_char(c) {
            let mut k = j;
            while k > 0 && is_operator_char(chars[k - 1]) {
                k -= 1;
            }
            let op: String = chars[k..j].iter().collect();
            if non_comparison(&op) {
                j = k;
                continue;
            }
            return Some(op);
        }
        if is_ident_char(c) {
            let mut k = j;
            while k > 0 && is_ident_char(chars[k - 1]) {
                k -= 1;
            }
            let word: String = chars[k..j].iter().collect();
            if is_boundary_word(&word) {
                return None;
            }
            j = k;
            continue;
        }
        j -= 1;
    }
    None
}

/// The comparison operator closest after the token, or `None` when the
/// predicate ends first.
fn nearest_operator_right(text: &str) -> Option<String> {
    let chars: Vec<char> = text.chars().collect();
    let mut j = 0;
    while j < chars.len() {
        let c = chars[j];
        if c == ',' || c == ';' {
            return None;
        }
        if is_operator_char(c) {
            let mut k = j;
            while k < chars.len() && is_operator_char(chars[k]) {
                k += 1;
            }
            let op: String = chars[j..k].iter().collect();
            if non_comparison(&op) {
                j = k;
                continue;
            }
            return Some(op);
        }
        if is_ident_char(c) {
            let mut k = j;
            while k < chars.len() && is_ident_char(chars[k]) {
                k += 1;
            }
            let word: String = chars[j..k].iter().collect();
            if is_boundary_word(&word) {
                return None;
            }
            j = k;
            continue;
        }
        j += 1;
    }
    None
}

#[cfg(test)]
mod tests {
    #[test]
    fn strict_comparison_handles_an_empty_token_a_leading_token_and_non_comparisons() {
        let t = "${bookmark}";
        assert!(!strict_comparison("ts > 1", ""));
        // Nothing precedes the token: the left side has no operator.
        assert!(strict_comparison("${bookmark} < ts", t));
        assert!(!strict_comparison("(${bookmark}) <= ts", t));
        // `->>` and `--` after the token are not comparisons.
        assert!(strict_comparison("WHERE ${bookmark}->>'a' < 5", t));
        assert!(!strict_comparison("WHERE ts >= ${bookmark} -- note", t));
    }
    use super::*;

    /// The fingerprint as it was computed before the canonical text was fed
    /// to the hashes directly; stored bookmarks hold these values.
    fn reference_fingerprint(row: &Value) -> String {
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
        let mut text = String::new();
        canonical(row, &mut text);
        let a = fnv1a_64(text.as_bytes(), FNV_BASIS_A);
        let b = fnv1a_64(text.as_bytes(), FNV_BASIS_B);
        format!("{a:016x}{b:016x}")
    }

    fn varied_rows() -> Vec<Value> {
        let wide: serde_json::Map<String, Value> = (0..40)
            .rev()
            .map(|i| (format!("field_{i:02}"), json!(i)))
            .collect();
        vec![
            json!({}),
            json!({"id": 1, "name": "a"}),
            json!({"name": "a", "id": 1}),
            json!({"z": null, "a": true, "m": false}),
            json!({"f": 0.1, "g": -0.0, "h": 1e300, "i": 1.5e-7, "j": 2.0}),
            json!({"big": 18446744073709551615u64, "neg": i64::MIN, "zero": 0}),
            json!({"text": "unicode ✓ \"quoted\" \\ back\n\ttab \u{1} \u{7f} é 日本"}),
            json!({"ключ": "значение", "": "empty key", "a\"b": 1}),
            json!({"nested": {"b": [1, {"y": null, "x": [[], {}]}], "a": "x"}, "list": []}),
            json!({"ts": "2026-03-04T05:06:07.123456Z", "amount": "12.500", "n": null}),
            json!([{"b": 1, "a": 2}, "s", 3]),
            json!("scalar"),
            json!(null),
            Value::Object(wide),
        ]
    }

    #[test]
    fn fingerprints_match_the_frozen_values() {
        let expected = FROZEN_FINGERPRINTS;
        let rows = varied_rows();
        assert_eq!(rows.len(), expected.len());
        for (row, want) in rows.iter().zip(expected) {
            assert_eq!(row_fingerprint(row), *want, "{row}");
        }
    }

    #[test]
    fn streamed_fingerprints_equal_the_text_built_ones() {
        let mut rows = varied_rows();
        let mut seed = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        for _ in 0..500 {
            let mut o = serde_json::Map::new();
            for _ in 0..(next() % 50) {
                let key = format!(
                    "k{}{}",
                    next() % 97,
                    char::from_u32(0x20 + (next() % 0x2000) as u32).unwrap_or('k')
                );
                let value = match next() % 7 {
                    0 => Value::Null,
                    1 => json!(next() as i64),
                    2 => json!(next()),
                    3 => json!(f64::from_bits(next())),
                    4 => json!(format!(
                        "s{}\"\\\n{}",
                        next(),
                        char::from_u32((next() % 0xd000) as u32).unwrap_or('x')
                    )),
                    5 => json!([next() % 3, {"b": next() % 5, "a": null}]),
                    _ => json!({"z": next() % 2 == 0, "y": {"q": next() as u32}}),
                };
                o.insert(key, value);
            }
            rows.push(Value::Object(o));
        }
        for row in &rows {
            assert_eq!(row_fingerprint(row), reference_fingerprint(row), "{row}");
        }
    }

    #[test]
    fn stored_entries_that_are_not_fingerprints_are_kept_until_the_value_moves() {
        let row = json!({"id": 1, "ts": 5});
        let fp = row_fingerprint(&row);
        let start = CursorBookmark {
            value: json!(5),
            boundary: Some(vec![
                "hand-edited".into(),
                fp.to_uppercase(),
                fp.clone(),
                fp.clone(),
            ]),
        };
        let mut t = CursorTracker::new("ts", start);
        let mut kept = vec![
            fp.clone(),
            fp.clone(),
            fp.to_uppercase(),
            "hand-edited".into(),
        ];
        kept.sort_unstable();
        assert_eq!(t.bookmark().boundary, Some(kept.clone()));
        assert!(!t.admit(&row));
        assert!(!t.admit(&row));
        assert!(t.admit(&row), "a third copy is new");
        kept.push(fp.clone());
        kept.sort_unstable();
        assert_eq!(t.bookmark().boundary, Some(kept));
        assert!(t.admit(&json!({"id": 2, "ts": 6})));
        assert_eq!(
            t.bookmark().boundary,
            Some(vec![row_fingerprint(&json!({"id": 2, "ts": 6}))])
        );
        assert_eq!(parse_fingerprint(&fp), Some(fingerprint(&row)));
        assert_eq!(parse_fingerprint("abc"), None);
        assert_eq!(parse_fingerprint(&"+".repeat(32)), None);
    }

    /// Computed with the text-building implementation; stored bookmarks hold
    /// values like these, so they never change.
    const FROZEN_FINGERPRINTS: &[&str] = &[
        "08f44b07b5901a25353239cb03fa4df0",
        "d4549314424cc2f522de57722bd2650e",
        "d4549314424cc2f522de57722bd2650e",
        "28bc017b9bfd421bc73634c6ca0e2274",
        "d36feb4ef0b2f0ca29b02e2850cb0613",
        "5b6e48fcca49f86e54d59fd48e8da1eb",
        "6bb56eeeefcbb7031a3a9b5537f54874",
        "8dace894988deebcf5cedc6a68d787fb",
        "1dee9420075aea120d6ccc35a02bd255",
        "ac4a76d93332e9bfa4a896b019c97c24",
        "3467135a6f16904ffc3e18a983aa3e18",
        "e105a86512a71a8daaeb049db53ff050",
        "5b9bc4ba528108e4f475ab4d403a5051",
        "ba564ed0a0ded1896b68aa2e4e32e962",
    ];
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
    fn rows_the_server_returns_past_the_start_are_kept_whatever_they_look_like() {
        let mut t = tracker(json!("10"));
        assert!(
            t.admit(&json!({"ts": "9"})),
            "text order puts '9' after '10'"
        );
        let mut t = tracker(json!("2024-01-01T20:00:00Z"));
        assert!(t.admit(&json!({"ts": "2024-01-02T00:00:00+05:00"})));
        let mut t = tracker(json!("b"));
        assert!(t.admit(&json!({"ts": "A"})));
    }

    #[test]
    fn identical_rows_at_the_boundary_are_counted_not_collapsed() {
        let click = json!({"ts": 10, "kind": "click"});
        let mut t = tracker(json!(0));
        assert!(t.admit(&click));
        let saved = t.bookmark();
        assert_eq!(saved.boundary.as_ref().unwrap().len(), 1);

        let mut t = CursorTracker::new("ts", saved);
        assert!(!t.admit(&click), "the copy already emitted");
        assert!(t.admit(&click), "a second copy committed late");
        let next = t.bookmark();
        assert_eq!(next.boundary.as_ref().unwrap().len(), 2);

        let mut t = CursorTracker::new("ts", next.clone());
        assert!(!t.admit(&click));
        assert!(!t.admit(&click));
        assert!(t.admit(&click), "a third copy");
        assert_eq!(
            CursorBookmark::from_state(next.to_value()).unwrap(),
            next,
            "repeated fingerprints round-trip"
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

    #[test]
    fn a_strict_comparison_is_found_through_wrappers() {
        let t = "${bookmark}";
        for q in [
            "WHERE ts > CAST(${bookmark} AS timestamptz)",
            "WHERE ts > (${bookmark})",
            "WHERE ts > TIMESTAMP ${bookmark}",
            "WHERE ts > to_timestamp(${bookmark})",
            "WHERE ${bookmark}::text < ts",
            "WHERE CAST(${bookmark} AS timestamp) < ts AND id > 0",
        ] {
            assert!(strict_comparison(q, t), "{q}");
        }
        for q in [
            "WHERE ts >= CAST(${bookmark} AS timestamptz)",
            "WHERE ts >= (${bookmark}) AND id > 0",
            "WHERE ${bookmark}::text <= ts",
            "WHERE id > 0 AND ts >= ${bookmark}",
            "WHERE ts BETWEEN ${bookmark} AND now()",
            "WHERE greatest(a > 1, ${bookmark}) >= ts",
            "WHERE data->>'ts' >= ${bookmark}",
            "WHERE ts >= f(x => ${bookmark})",
        ] {
            assert!(!strict_comparison(q, t), "{q}");
        }
        // The token matches only on an identifier boundary.
        assert!(!strict_comparison("WHERE ts > @bookmark_old", "@bookmark"));
        assert!(strict_comparison("WHERE ts > :bookmark", ":bookmark"));
    }
}
