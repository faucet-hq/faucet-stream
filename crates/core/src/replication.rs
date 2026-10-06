//! Incremental replication support.

use crate::error::FaucetError;
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::cmp::Ordering;

/// Determines how records are replicated from the source.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type")]
pub enum ReplicationMethod {
    /// All records are fetched on every run (default).
    #[default]
    FullTable,
    /// Only records where the `replication_key` field is strictly greater than
    /// the stored bookmark (`start_replication_value`) are kept.
    Incremental,
}

/// What to do with a record whose replication key is missing or `null` (#747).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OnMissingKey {
    /// Keep the record, count it, and warn (default): dropping it would be
    /// silent data loss.
    #[default]
    Keep,
    /// Drop the record (counted and warned, never silent).
    Drop,
    /// Fail the run.
    Fail,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum KeyForm {
    TopLevel,
    DotPath(Vec<String>),
    Pointer,
}

/// A compiled replication key: a top-level field name, a dot path
/// (`fields.updated`, numeric segments index arrays), or an RFC 6901 JSON
/// Pointer (`/fields/updated`, for field names that contain a dot) (#747).
///
/// A dot path first tries the whole string as a literal top-level field, so a
/// flat column literally named `Account.LastModifiedDate` (a CSV header) still
/// resolves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationKey {
    raw: String,
    form: KeyForm,
}

impl ReplicationKey {
    /// Parse a user-facing key (see the type docs for the accepted forms).
    pub fn parse(raw: &str) -> Result<Self, FaucetError> {
        if raw.trim().is_empty() {
            return Err(FaucetError::Config(
                "replication_key must not be empty".to_owned(),
            ));
        }
        if raw.starts_with('/') {
            return Ok(Self {
                raw: raw.to_owned(),
                form: KeyForm::Pointer,
            });
        }
        if !raw.contains('.') {
            return Ok(Self::top_level(raw));
        }
        let segments: Vec<String> = raw.split('.').map(str::to_owned).collect();
        if segments.iter().any(String::is_empty) {
            return Err(FaucetError::Config(format!(
                "replication_key '{raw}': empty path segment (use the JSON Pointer form \
                 `/a/b` for field names that contain dots)"
            )));
        }
        Ok(Self {
            raw: raw.to_owned(),
            form: KeyForm::DotPath(segments),
        })
    }

    /// A literal top-level field name, never interpreted as a path.
    pub fn top_level(name: &str) -> Self {
        Self {
            raw: name.to_owned(),
            form: KeyForm::TopLevel,
        }
    }

    /// The key as configured.
    pub fn as_str(&self) -> &str {
        &self.raw
    }

    /// Whether the key is a JSON Pointer (`/a/b`).
    pub fn is_pointer(&self) -> bool {
        self.form == KeyForm::Pointer
    }

    /// Whether the key addresses a nested value (dot path or pointer).
    pub fn is_nested(&self) -> bool {
        self.form != KeyForm::TopLevel
    }

    /// Resolve the key against one record.
    pub fn resolve<'a>(&self, record: &'a Value) -> Option<&'a Value> {
        match &self.form {
            KeyForm::TopLevel => record.get(&self.raw),
            KeyForm::Pointer => record.pointer(&self.raw),
            KeyForm::DotPath(segments) => {
                if let Some(v) = record.get(&self.raw) {
                    return Some(v);
                }
                segments.iter().try_fold(record, |cur, seg| match cur {
                    Value::Object(m) => m.get(seg),
                    Value::Array(a) => seg.parse::<usize>().ok().and_then(|i| a.get(i)),
                    _ => None,
                })
            }
        }
    }

    fn resolve_present<'a>(&self, record: &'a Value) -> Option<&'a Value> {
        self.resolve(record).filter(|v| !v.is_null())
    }
}

/// The result of [`filter_incremental_path`].
#[derive(Debug, Clone, PartialEq, Default)]
pub struct IncrementalFilter {
    /// Records to write.
    pub records: Vec<Value>,
    /// Records whose key was missing or `null` (kept or dropped per policy).
    pub missing: usize,
}

/// Filter `records` to those where `key > start`, using a compiled key.
///
/// A record whose key is missing or `null` is handled per `on_missing`
/// (counted in [`IncrementalFilter::missing`] either way); a record whose key
/// has a different JSON type than `start` is kept with a warning. Returns
/// `Err` only for [`OnMissingKey::Fail`].
pub fn filter_incremental_path(
    records: Vec<Value>,
    key: &ReplicationKey,
    start: &Value,
    on_missing: OnMissingKey,
) -> Result<IncrementalFilter, FaucetError> {
    let mut missing = 0usize;
    let mut kept = Vec::with_capacity(records.len());
    for r in records {
        let keep = match key.resolve_present(&r) {
            None => {
                missing += 1;
                match on_missing {
                    OnMissingKey::Keep => true,
                    OnMissingKey::Drop => false,
                    OnMissingKey::Fail => {
                        return Err(FaucetError::Source(format!(
                            "incremental replication: a record lacks replication_key '{}' \
                             (on_missing_key: fail)",
                            key.as_str()
                        )));
                    }
                }
            }
            Some(v) if !comparable(v, start) => {
                tracing::warn!(
                    key = key.as_str(),
                    "incremental replication: record key type does not match the bookmark \
                     type; keeping the record to avoid silently dropping data"
                );
                true
            }
            Some(v) => json_gt(v, start),
        };
        if keep {
            kept.push(r);
        }
    }
    Ok(IncrementalFilter {
        records: kept,
        missing,
    })
}

/// Filter `records` to only those where `record[key] > start`.
///
/// `key` is a literal top-level field. Values compare by what they hold (see
/// [`json_gt`]): decimal strings numerically, timestamp strings as instants,
/// integers exactly (no `f64` precision loss), floats as `f64`, other strings
/// lexicographically.
///
/// Records missing the key (or holding `null`) are **kept** and a warning is
/// logged (#747): dropping them silently is data loss. Likewise a record whose
/// key value is a *different JSON type* than `start` is kept (#78/#27).
pub fn filter_incremental(records: Vec<Value>, key: &str, start: &Value) -> Vec<Value> {
    let out = filter_incremental_path(
        records,
        &ReplicationKey::top_level(key),
        start,
        OnMissingKey::Keep,
    )
    .unwrap_or_default();
    if out.missing > 0 {
        tracing::warn!(
            key,
            missing = out.missing,
            "incremental replication: {} record(s) lacked replication_key '{key}'; kept to \
             avoid silent data loss",
            out.missing
        );
    }
    out.records
}

/// Return the maximum non-null value of `key` across all records, if any.
pub fn max_replication_value_path<'a>(
    records: &'a [Value],
    key: &ReplicationKey,
) -> Option<&'a Value> {
    records
        .iter()
        .filter_map(|r| key.resolve_present(r))
        .max_by(|a, b| json_compare(a, b))
}

/// Return the maximum value of `record[key]` across all records, if any.
pub fn max_replication_value<'a>(records: &'a [Value], key: &str) -> Option<&'a Value> {
    records
        .iter()
        .filter_map(|r| r.get(key))
        .max_by(|a, b| json_compare(a, b))
}

/// Return the larger of two replication values using the same ordering as
/// [`max_replication_value`] (see [`json_gt`]; falling back to `a` on a tie).
pub fn max_value(a: Value, b: Value) -> Value {
    match json_compare(&a, &b) {
        Ordering::Less => b,
        _ => a,
    }
}

/// Type-rank for a total ordering across JSON value kinds, so comparisons of
/// differing types are deterministic instead of collapsing to `Equal`.
fn type_rank(v: &Value) -> u8 {
    match v {
        Value::Null => 0,
        Value::Bool(_) => 1,
        Value::Number(_) => 2,
        Value::String(_) => 3,
        Value::Array(_) => 4,
        Value::Object(_) => 5,
    }
}

/// Exact integer view of a JSON number (`i64` or `u64`), widened to `i128` so
/// both halves of the range compare without `f64` precision loss. `None` for
/// non-integral (floating) numbers.
fn number_as_i128(n: &serde_json::Number) -> Option<i128> {
    n.as_i64()
        .map(i128::from)
        .or_else(|| n.as_u64().map(i128::from))
}

/// Whether two replication values can be ordered against each other: the same
/// JSON type, or a number and a decimal string (sources such as Oracle emit a
/// `NUMBER` as a number when it fits `f64` exactly and as a string otherwise).
fn comparable(a: &Value, b: &Value) -> bool {
    type_rank(a) == type_rank(b) || numeric_text(a).is_some() && numeric_text(b).is_some()
}

/// The decimal text of a number or a decimal string.
fn numeric_text(v: &Value) -> Option<std::borrow::Cow<'_, str>> {
    match v {
        Value::Number(n) => Some(std::borrow::Cow::Owned(n.to_string())),
        Value::String(s) if Decimal::parse(s).is_some() => Some(std::borrow::Cow::Borrowed(s)),
        _ => None,
    }
}

/// An exact decimal: `0.d₁d₂… × 10^exp` with no leading or trailing zero
/// digits (`digits` empty means zero). Arbitrary length, so a DECIMAL(38)
/// cursor orders exactly.
#[derive(Debug, PartialEq, Eq)]
struct Decimal {
    negative: bool,
    digits: Vec<u8>,
    exp: i64,
}

impl Decimal {
    fn parse(text: &str) -> Option<Self> {
        let s = text.trim();
        let (negative, s) = match s.as_bytes().first()? {
            b'-' => (true, &s[1..]),
            b'+' => (false, &s[1..]),
            _ => (false, s),
        };
        let (mantissa, exponent) = match s.find(['e', 'E']) {
            Some(i) => (&s[..i], s[i + 1..].parse::<i64>().ok()?),
            None => (s, 0),
        };
        let (int, frac) = match mantissa.split_once('.') {
            Some((i, f)) => (i, f),
            None => (mantissa, ""),
        };
        if int.is_empty() && frac.is_empty()
            || !int.bytes().chain(frac.bytes()).all(|b| b.is_ascii_digit())
        {
            return None;
        }
        let mut digits: Vec<u8> = int.bytes().chain(frac.bytes()).map(|b| b - b'0').collect();
        let mut point = i64::try_from(int.len()).ok()?;
        let leading = digits.iter().take_while(|d| **d == 0).count();
        digits.drain(..leading);
        point -= i64::try_from(leading).ok()?;
        while digits.last() == Some(&0) {
            digits.pop();
        }
        let exp = if digits.is_empty() {
            0
        } else {
            point.checked_add(exponent)?
        };
        Some(Self {
            negative: negative && !digits.is_empty(),
            digits,
            exp,
        })
    }

    fn signum(&self) -> i8 {
        match (self.digits.is_empty(), self.negative) {
            (true, _) => 0,
            (false, true) => -1,
            (false, false) => 1,
        }
    }

    fn cmp_magnitude(&self, other: &Self) -> Ordering {
        self.exp
            .cmp(&other.exp)
            .then_with(|| self.digits.cmp(&other.digits))
    }
}

impl Ord for Decimal {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.signum().cmp(&other.signum()) {
            Ordering::Equal => match self.signum() {
                1 => self.cmp_magnitude(other),
                -1 => other.cmp_magnitude(self),
                _ => Ordering::Equal,
            },
            unequal => unequal,
        }
    }
}

impl PartialOrd for Decimal {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// A timestamp string as an instant: RFC 3339 with an offset (a space may
/// replace the `T`), compared in UTC; or the same without an offset, compared
/// as a local date-time against another offset-less value.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Instant {
    Zoned(DateTime<Utc>),
    Naive(chrono::NaiveDateTime),
}

impl Instant {
    fn parse(text: &str) -> Option<Self> {
        let s = text.trim();
        if s.len() < 19 || !s.as_bytes()[..4].iter().all(u8::is_ascii_digit) {
            return None;
        }
        let normalized = if s.as_bytes()[10] == b' ' {
            let mut t = s.to_string();
            t.replace_range(10..11, "T");
            std::borrow::Cow::Owned(t)
        } else {
            std::borrow::Cow::Borrowed(s)
        };
        if let Ok(dt) = DateTime::parse_from_rfc3339(&normalized) {
            return Some(Self::Zoned(dt.with_timezone(&Utc)));
        }
        chrono::NaiveDateTime::parse_from_str(&normalized, "%Y-%m-%dT%H:%M:%S%.f")
            .ok()
            .map(Self::Naive)
    }
}

/// Order two strings by what they hold: both decimals → numerically, both
/// instants of the same kind → chronologically, otherwise lexicographically.
fn compare_strings(x: &str, y: &str) -> Ordering {
    if let (Some(a), Some(b)) = (Decimal::parse(x), Decimal::parse(y)) {
        return a.cmp(&b);
    }
    match (Instant::parse(x), Instant::parse(y)) {
        (Some(a @ Instant::Zoned(_)), Some(b @ Instant::Zoned(_)))
        | (Some(a @ Instant::Naive(_)), Some(b @ Instant::Naive(_))) => a.cmp(&b),
        _ => x.cmp(y),
    }
}

/// Total ordering over JSON values used for replication bookmarks.
///
/// - Numbers: compared exactly as `i128` when both are integral (so cursors
///   above 2^53 don't lose precision); otherwise as `f64`, with NaN ordered
///   last.
/// - Strings: decimal strings numerically and exactly (`"99" < "100"`),
///   timestamps chronologically after normalizing offsets and fraction widths,
///   anything else lexicographically.
/// - A number against a decimal string: numerically.
/// - Other same-type values: natural ordering (bools `false < true`, arrays
///   element-wise, objects by serialized form).
/// - Different types: ordered by [`type_rank`] so the result is always total.
pub(crate) fn json_compare(a: &Value, b: &Value) -> Ordering {
    match (a, b) {
        (Value::Number(_), Value::String(_)) | (Value::String(_), Value::Number(_)) => {
            match (numeric_text(a), numeric_text(b)) {
                (Some(x), Some(y)) => match (Decimal::parse(&x), Decimal::parse(&y)) {
                    (Some(x), Some(y)) => x.cmp(&y),
                    _ => type_rank(a).cmp(&type_rank(b)),
                },
                _ => type_rank(a).cmp(&type_rank(b)),
            }
        }
        (Value::Number(an), Value::Number(bn)) => {
            match (number_as_i128(an), number_as_i128(bn)) {
                (Some(ai), Some(bi)) => ai.cmp(&bi),
                _ => {
                    let af = an.as_f64().unwrap_or(f64::NAN);
                    let bf = bn.as_f64().unwrap_or(f64::NAN);
                    af.partial_cmp(&bf).unwrap_or_else(|| {
                        // At least one NaN — order NaN last, deterministically.
                        match (af.is_nan(), bf.is_nan()) {
                            (false, true) => Ordering::Less,
                            (true, false) => Ordering::Greater,
                            _ => Ordering::Equal,
                        }
                    })
                }
            }
        }
        (Value::String(x), Value::String(y)) => compare_strings(x, y),
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Array(x), Value::Array(y)) => {
            for (xi, yi) in x.iter().zip(y.iter()) {
                let c = json_compare(xi, yi);
                if c != Ordering::Equal {
                    return c;
                }
            }
            x.len().cmp(&y.len())
        }
        // Objects have no natural order; use the serialized form for a stable
        // total order (objects as replication keys are pathological).
        (Value::Object(_), Value::Object(_)) => a.to_string().cmp(&b.to_string()),
        // Different JSON types — order by type rank so comparison is total.
        _ => type_rank(a).cmp(&type_rank(b)),
    }
}

/// Total-order "greater than" over JSON values, using the same comparison
/// [`filter_incremental`] applies to replication keys (numbers and decimal
/// strings numerically, timestamp strings chronologically, other strings
/// lexicographically). Public
/// so callers bounding a replay window (e.g. `faucet backfill --to-bookmark`)
/// compare exactly like the incremental filter does.
pub fn json_gt(a: &Value, b: &Value) -> bool {
    json_compare(a, b) == Ordering::Greater
}

// ── Server-side incremental push-down (#513) ─────────────────────────────────

/// The placeholder replaced by the formatted bookmark inside a
/// [`ReplicationBind::template`].
pub const BIND_PLACEHOLDER: &str = "${bookmark}";

fn default_bind_template() -> String {
    BIND_PLACEHOLDER.to_owned()
}

/// Where a rendered bookmark is injected into the outgoing request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum BindTarget {
    /// A query-string parameter (default) — e.g. `?updated_after=…`.
    #[default]
    Query,
    /// A request header — e.g. `If-Modified-Since: …`.
    Header,
    /// A field of the JSON request body (POST-search APIs): a top-level field
    /// named by `name`, or any existing location addressed by a JSON Pointer
    /// `path` (#748).
    Body,
    /// A `{name}` placeholder in the request path.
    Path,
}

/// How the bookmark value is formatted before it is substituted into the
/// [`ReplicationBind::template`].
///
/// For every non-[`Raw`](BindFormat::Raw) format the bookmark is first parsed
/// into an instant: a string is read as RFC 3339, a bare `YYYY-MM-DD` date
/// (midnight UTC), or a naive `YYYY-MM-DDTHH:MM:SS` (assumed UTC); a JSON
/// number is read as **epoch seconds**. It is then re-emitted in the target
/// representation, so `epoch_ms` ← ISO string and `iso8601` ← epoch number both
/// work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum BindFormat {
    /// Emit the scalar verbatim (string as-is, number as its decimal form).
    /// The default; no timestamp parsing.
    #[default]
    Raw,
    /// RFC 3339 / ISO-8601 UTC timestamp, e.g. `2024-06-01T00:00:00Z`.
    Iso8601,
    /// Unix epoch **seconds** (integer).
    EpochS,
    /// Unix epoch **milliseconds** (integer).
    EpochMs,
    /// Calendar date `YYYY-MM-DD` (UTC).
    Date,
}

/// The JSON type a body-target bind writes (#748).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum BindValueType {
    /// A JSON string (default).
    #[default]
    String,
    /// A JSON number (e.g. an `epoch_ms` bookmark an API requires unquoted).
    Number,
}

impl BindValueType {
    /// Convert a rendered bind value into the JSON value to write.
    pub fn to_value(self, rendered: &str) -> Result<Value, FaucetError> {
        match self {
            Self::String => Ok(Value::String(rendered.to_owned())),
            Self::Number => {
                let n: Option<serde_json::Number> = rendered
                    .parse::<i64>()
                    .map(serde_json::Number::from)
                    .ok()
                    .or_else(|| rendered.parse::<u64>().ok().map(serde_json::Number::from))
                    .or_else(|| {
                        rendered
                            .parse::<f64>()
                            .ok()
                            .and_then(serde_json::Number::from_f64)
                    });
                n.map(Value::Number).ok_or_else(|| {
                    FaucetError::Source(format!(
                        "bind: rendered value '{rendered}' is not a number (value_type: number)"
                    ))
                })
            }
        }
    }
}

fn unescape_pointer_token(token: &str) -> String {
    token.replace("~1", "/").replace("~0", "~")
}

/// Write `value` into `body` at the RFC 6901 JSON Pointer `pointer` (#748).
///
/// The pointer must address an existing scalar (or `null`) — or a missing
/// final key whose parent is an existing object. Intermediate objects and
/// array elements are never created, and an object/array target is refused
/// (a bind replaces a value; it does not merge).
pub fn set_body_pointer(body: &mut Value, pointer: &str, value: Value) -> Result<(), FaucetError> {
    if !pointer.starts_with('/') {
        return Err(FaucetError::Config(format!(
            "JSON Pointer '{pointer}' must start with '/'"
        )));
    }
    if let Some(slot) = body.pointer_mut(pointer) {
        if slot.is_object() || slot.is_array() {
            return Err(FaucetError::Source(format!(
                "request body location '{pointer}' holds an object or array; a bind replaces \
                 a scalar value"
            )));
        }
        *slot = value;
        return Ok(());
    }
    let cut = pointer.rfind('/').unwrap_or(0);
    let (parent, leaf) = (&pointer[..cut], &pointer[cut + 1..]);
    let parent_value = if parent.is_empty() {
        Some(body)
    } else {
        body.pointer_mut(parent)
    };
    match parent_value {
        Some(Value::Object(map)) => {
            map.insert(unescape_pointer_token(leaf), value);
            Ok(())
        }
        _ => Err(FaucetError::Source(format!(
            "request body has no location '{pointer}' (the pointer must resolve to an \
             existing value, or to a new key of an existing object)"
        ))),
    }
}

/// Load-time check of a bind's placement: a body bind needs exactly one of
/// `name` / `path`; every other target needs `name` and refuses `path`.
pub(crate) fn validate_bind_placement(
    what: &str,
    into: BindTarget,
    name: &str,
    path: Option<&str>,
) -> Result<(), FaucetError> {
    let has_name = !name.trim().is_empty();
    match (into, path) {
        (BindTarget::Body, Some(p)) => {
            if has_name {
                return Err(FaucetError::Config(format!(
                    "{what}: set either `name` (top-level body field) or `path` (JSON Pointer), \
                     not both"
                )));
            }
            if !p.starts_with('/') || p.len() < 2 {
                return Err(FaucetError::Config(format!(
                    "{what}: `path` must be a JSON Pointer such as `/filters/0/value`, got '{p}'"
                )));
            }
            Ok(())
        }
        (_, Some(_)) => Err(FaucetError::Config(format!(
            "{what}: `path` applies only to `into: body`"
        ))),
        (BindTarget::Body, None) if !has_name => Err(FaucetError::Config(format!(
            "{what}: `into: body` needs `name` (top-level field) or `path` (JSON Pointer)"
        ))),
        (_, None) if !has_name => Err(FaucetError::Config(format!(
            "{what}: `name` must not be empty"
        ))),
        _ => Ok(()),
    }
}

/// Declarative binding of the stored bookmark into the **outgoing request** —
/// "server-side incremental push-down" (#513).
///
/// Today faucet tracks bookmarks and filters incrementally *client-side* (after
/// download). A bind lets a source instead push the bookmark into the request
/// (query param / header / body field / path) so the server returns only the
/// new rows. The existing client-side [`filter_incremental`] stays active as a
/// safety net for servers that don't honour the filter exactly.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ReplicationBind {
    /// Where to place the rendered value.
    #[serde(default)]
    pub into: BindTarget,
    /// The parameter / header / body-field / path-placeholder name. Optional
    /// only for `into: body` with a `path`.
    #[serde(default)]
    pub name: String,
    /// `into: body` only: an RFC 6901 JSON Pointer into the configured `body`
    /// (`/filterGroups/0/filters/0/value`) instead of a top-level `name` (#748).
    /// It must resolve to an existing scalar or to a new key of an existing
    /// object; array elements are never created.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// JSON type written by a body bind: `string` (default) or `number`.
    #[serde(default)]
    pub value_type: BindValueType,
    /// Template rendered with [`BIND_PLACEHOLDER`] (`${bookmark}`) replaced by
    /// the formatted bookmark. Defaults to the bare `${bookmark}`; set e.g.
    /// `"gte|${bookmark}"` (an operator-prefixed filter) or `"[${bookmark} TO *]"` (Lucene).
    #[serde(default = "default_bind_template")]
    pub template: String,
    /// How to format the bookmark before substitution.
    #[serde(default)]
    pub format: BindFormat,
    /// Optional JSONPath into the response body to advance the bookmark from,
    /// instead of `max(record[replication_key])`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub advance_from: Option<String>,
}

impl ReplicationBind {
    /// Validate the binding at config-load time.
    pub fn validate(&self) -> Result<(), FaucetError> {
        validate_bind_placement(
            "replication bind",
            self.into,
            &self.name,
            self.path.as_deref(),
        )?;
        if !self.template.contains(BIND_PLACEHOLDER) {
            return Err(FaucetError::Config(format!(
                "replication bind: `template` must contain the `{BIND_PLACEHOLDER}` placeholder"
            )));
        }
        Ok(())
    }

    /// Render the binding for a concrete bookmark: format the value, then
    /// substitute it into the template.
    pub fn render(&self, bookmark: &Value) -> Result<String, FaucetError> {
        let formatted = format_bookmark(bookmark, self.format)?;
        Ok(self.template.replace(BIND_PLACEHOLDER, &formatted))
    }
}

/// Parse a bookmark value into a UTC instant (see [`BindFormat`] for the rules).
fn bookmark_instant(value: &Value) -> Result<DateTime<Utc>, FaucetError> {
    match value {
        Value::String(s) => {
            let s = s.trim();
            if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
                return Ok(dt.with_timezone(&Utc));
            }
            if let Ok(d) = NaiveDate::parse_from_str(s, "%Y-%m-%d")
                && let Some(ndt) = d.and_hms_opt(0, 0, 0)
            {
                return Ok(DateTime::<Utc>::from_naive_utc_and_offset(ndt, Utc));
            }
            if let Ok(ndt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
                return Ok(DateTime::<Utc>::from_naive_utc_and_offset(ndt, Utc));
            }
            Err(FaucetError::Config(format!(
                "replication bind: cannot parse bookmark '{s}' as a timestamp \
                 (expected RFC 3339, YYYY-MM-DD, or YYYY-MM-DDTHH:MM:SS)"
            )))
        }
        Value::Number(n) => {
            let secs = n.as_i64().or_else(|| n.as_f64().map(|f| f as i64));
            secs.and_then(|s| DateTime::<Utc>::from_timestamp(s, 0))
                .ok_or_else(|| {
                    FaucetError::Config(format!(
                        "replication bind: numeric bookmark {n} is out of range for epoch seconds"
                    ))
                })
        }
        other => Err(FaucetError::Config(format!(
            "replication bind: bookmark must be a string or number, got {other}"
        ))),
    }
}

/// Parse a bookmark value into a UTC instant (public wrapper over the internal
/// parser; see [`BindFormat`] for the accepted forms). Used by datetime window
/// slicing (#527) to resolve the sweep's start bound from the stored bookmark.
pub fn parse_instant(value: &Value) -> Result<DateTime<Utc>, FaucetError> {
    bookmark_instant(value)
}

/// Format an already-resolved UTC instant per [`BindFormat`]. Unlike
/// [`format_bookmark`] (which takes an arbitrary scalar and, for [`BindFormat::Raw`],
/// echoes it verbatim), this always has a real instant, so `Raw` and `Iso8601`
/// both emit an RFC 3339 UTC timestamp. Used to render window boundaries (#527).
pub fn format_instant(dt: DateTime<Utc>, format: BindFormat) -> String {
    match format {
        BindFormat::Raw | BindFormat::Iso8601 => {
            dt.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
        }
        BindFormat::Date => dt.format("%Y-%m-%d").to_string(),
        BindFormat::EpochS => dt.timestamp().to_string(),
        BindFormat::EpochMs => dt.timestamp_millis().to_string(),
    }
}

/// Format a bookmark value per [`BindFormat`].
pub fn format_bookmark(value: &Value, format: BindFormat) -> Result<String, FaucetError> {
    match format {
        BindFormat::Raw => match value {
            Value::String(s) => Ok(s.clone()),
            Value::Number(n) => Ok(n.to_string()),
            Value::Bool(b) => Ok(b.to_string()),
            other => Err(FaucetError::Config(format!(
                "replication bind: cannot render {other} as a raw scalar"
            ))),
        },
        BindFormat::Iso8601 => {
            Ok(bookmark_instant(value)?.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
        }
        BindFormat::Date => Ok(bookmark_instant(value)?.format("%Y-%m-%d").to_string()),
        BindFormat::EpochS => Ok(bookmark_instant(value)?.timestamp().to_string()),
        BindFormat::EpochMs => Ok(bookmark_instant(value)?.timestamp_millis().to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_filter_incremental_strings() {
        let records = vec![
            json!({"id": 1, "updated_at": "2024-01-01"}),
            json!({"id": 2, "updated_at": "2024-06-01"}),
            json!({"id": 3, "updated_at": "2024-12-01"}),
        ];
        let start = json!("2024-06-01");
        let filtered = filter_incremental(records, "updated_at", &start);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0]["id"], 3);
    }

    #[test]
    fn test_filter_incremental_numbers() {
        let records = vec![
            json!({"id": 1, "seq": 100}),
            json!({"id": 2, "seq": 200}),
            json!({"id": 3, "seq": 300}),
        ];
        let start = json!(150);
        let filtered = filter_incremental(records, "seq", &start);
        assert_eq!(filtered.len(), 2);
        assert_eq!(filtered[0]["id"], 2);
        assert_eq!(filtered[1]["id"], 3);
    }

    #[test]
    fn test_filter_incremental_missing_key_kept() {
        // #747: a record without the key used to be dropped silently.
        let records = vec![
            json!({"id": 1}),
            json!({"id": 2, "updated_at": "2024-12-01"}),
            json!({"id": 3, "updated_at": null}),
        ];
        let start = json!("2024-01-01");
        let filtered = filter_incremental(records, "updated_at", &start);
        assert_eq!(filtered.len(), 3);
    }

    #[test]
    fn replication_key_parse_forms() {
        assert!(!ReplicationKey::parse("updated").unwrap().is_nested());
        let dot = ReplicationKey::parse("fields.updated").unwrap();
        assert!(dot.is_nested() && !dot.is_pointer());
        assert_eq!(dot.as_str(), "fields.updated");
        let ptr = ReplicationKey::parse("/a.b/c").unwrap();
        assert!(ptr.is_pointer() && ptr.is_nested());
        assert!(ReplicationKey::parse(" ").is_err());
        assert!(ReplicationKey::parse("a..b").is_err());
        assert!(ReplicationKey::parse(".a").is_err());
    }

    #[test]
    fn replication_key_resolves_nested_array_and_pointer() {
        let r = json!({
            "fields": {"updated": "2024-06-01"},
            "items": [{"date": 1}, {"date": 2}],
            "a.b": {"c": 7},
            "x": 5
        });
        let k = |s: &str| ReplicationKey::parse(s).unwrap();
        assert_eq!(k("fields.updated").resolve(&r), Some(&json!("2024-06-01")));
        assert_eq!(k("items.1.date").resolve(&r), Some(&json!(2)));
        assert_eq!(k("items.x.date").resolve(&r), None);
        assert_eq!(k("items.9.date").resolve(&r), None);
        assert_eq!(k("x.y").resolve(&r), None);
        assert_eq!(k("/a.b/c").resolve(&r), Some(&json!(7)));
        assert_eq!(k("x").resolve(&r), Some(&json!(5)));
        let flat = json!({"Account.LastModifiedDate": "2024"});
        assert_eq!(
            k("Account.LastModifiedDate").resolve(&flat),
            Some(&json!("2024"))
        );
        assert_eq!(
            ReplicationKey::top_level("a.b").resolve(&r),
            Some(&json!({"c": 7}))
        );
    }

    #[test]
    fn filter_incremental_path_nested_and_policies() {
        let records = || {
            vec![
                json!({"id": 1, "fields": {"updated": "2024-01-01"}}),
                json!({"id": 2, "fields": {"updated": "2024-12-01"}}),
                json!({"id": 3, "fields": {}}),
                json!({"id": 4, "fields": {"updated": 5}}),
            ]
        };
        let key = ReplicationKey::parse("fields.updated").unwrap();
        let start = json!("2024-06-01");
        let keep = filter_incremental_path(records(), &key, &start, OnMissingKey::Keep).unwrap();
        let ids: Vec<i64> = keep
            .records
            .iter()
            .map(|r| r["id"].as_i64().unwrap())
            .collect();
        assert_eq!(ids, vec![2, 3, 4]);
        assert_eq!(keep.missing, 1);
        let drop = filter_incremental_path(records(), &key, &start, OnMissingKey::Drop).unwrap();
        assert_eq!(drop.records.len(), 2);
        assert_eq!(drop.missing, 1);
        let err = filter_incremental_path(records(), &key, &start, OnMissingKey::Fail).unwrap_err();
        assert!(err.to_string().contains("fields.updated"), "{err}");
    }

    #[test]
    fn max_replication_value_path_skips_missing_and_null() {
        let key = ReplicationKey::parse("fields.updated").unwrap();
        let records = vec![
            json!({"fields": {"updated": "2024-01-01"}}),
            json!({"fields": {"updated": null}}),
            json!({"fields": {"updated": "2024-12-01"}}),
            json!({}),
        ];
        assert_eq!(
            max_replication_value_path(&records, &key),
            Some(&json!("2024-12-01"))
        );
        assert!(max_replication_value_path(&records[1..2], &key).is_none());
    }

    #[test]
    fn on_missing_key_serde() {
        assert_eq!(OnMissingKey::default(), OnMissingKey::Keep);
        let v: OnMissingKey = serde_json::from_value(json!("fail")).unwrap();
        assert_eq!(v, OnMissingKey::Fail);
    }

    #[test]
    fn test_filter_incremental_equal_excluded() {
        let records = vec![
            json!({"id": 1, "updated_at": "2024-06-01"}),
            json!({"id": 2, "updated_at": "2024-06-02"}),
        ];
        let start = json!("2024-06-01");
        let filtered = filter_incremental(records, "updated_at", &start);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0]["id"], 2);
    }

    #[test]
    fn test_max_replication_value_strings() {
        let records = vec![
            json!({"updated_at": "2024-01-01"}),
            json!({"updated_at": "2024-12-01"}),
            json!({"updated_at": "2024-06-01"}),
        ];
        let max = max_replication_value(&records, "updated_at").unwrap();
        assert_eq!(max, &json!("2024-12-01"));
    }

    #[test]
    fn test_max_replication_value_numbers() {
        let records = vec![json!({"seq": 5}), json!({"seq": 10}), json!({"seq": 3})];
        let max = max_replication_value(&records, "seq").unwrap();
        assert_eq!(max, &json!(10));
    }

    #[test]
    fn test_max_replication_value_empty() {
        let records: Vec<Value> = vec![];
        assert!(max_replication_value(&records, "updated_at").is_none());
    }

    #[test]
    fn test_max_value_picks_larger_string() {
        assert_eq!(
            max_value(json!("2024-01-01"), json!("2024-06-01")),
            json!("2024-06-01")
        );
    }

    #[test]
    fn test_max_value_picks_larger_number() {
        assert_eq!(max_value(json!(5), json!(10)), json!(10));
    }

    #[test]
    fn test_max_value_returns_a_on_type_mismatch() {
        // String outranks Number in the total type-rank ordering, so the
        // larger (a) is returned.
        assert_eq!(max_value(json!("string"), json!(5)), json!("string"));
    }

    #[test]
    fn filter_incremental_keeps_large_integer_beyond_f64_precision() {
        // Regression for #78/#27: integer cursors above 2^53 lose precision
        // when compared as f64, so a genuinely-greater value compared Equal
        // and was silently dropped.
        let two_pow_53 = 9_007_199_254_740_992_i64; // 2^53
        let records = vec![
            json!({"id": 1, "seq": two_pow_53 + 1}),
            json!({"id": 2, "seq": two_pow_53 + 2}),
        ];
        let start = json!(two_pow_53);
        let filtered = filter_incremental(records, "seq", &start);
        assert_eq!(
            filtered.len(),
            2,
            "both values are strictly greater than 2^53"
        );
    }

    #[test]
    fn json_compare_distinguishes_large_integers() {
        let a = json!(9_007_199_254_740_993_i64); // 2^53 + 1
        let b = json!(9_007_199_254_740_992_i64); // 2^53
        assert_eq!(json_compare(&a, &b), Ordering::Greater);
    }

    #[test]
    fn filter_incremental_keeps_records_on_type_mismatch() {
        // Regression for #78/#27: a bookmark/key type mismatch must not be
        // silently treated as "not greater" and the record dropped — that is
        // data loss. Keep the record instead.
        let records = vec![json!({"id": 1, "seq": 20_240_701})];
        let start = json!("2024-06-01"); // string bookmark vs numeric key
        let filtered = filter_incremental(records, "seq", &start);
        assert_eq!(filtered.len(), 1, "type mismatch must not silently drop");
    }

    fn cmp(a: Value, b: Value) -> Ordering {
        json_compare(&a, &b)
    }

    #[test]
    fn decimal_strings_order_numerically_and_exactly() {
        use Ordering::*;
        let cases = [
            ("99", "100", Less),
            ("100", "99", Greater),
            ("989", "99", Greater),
            ("007", "7", Equal),
            ("1.50", "1.5", Equal),
            ("0.000", "-0", Equal),
            ("-1", "0", Less),
            ("-10", "-9", Less),
            ("-0.5", "-0.25", Less),
            ("1e3", "999", Greater),
            ("1E-2", "0.01", Equal),
            ("+5", "5", Equal),
            (".5", "0.4", Greater),
            (
                "12345678901234567890123456789012345678",
                "12345678901234567890123456789012345677",
                Greater,
            ),
            (
                "0.000000000000000000000000000000000002",
                "0.000000000000000000000000000000000001",
                Greater,
            ),
        ];
        for (a, b, want) in cases {
            assert_eq!(cmp(json!(a), json!(b)), want, "{a} vs {b}");
        }
    }

    #[test]
    fn decimal_parse_rejects_what_is_not_a_number() {
        for text in [
            "",
            "-",
            ".",
            "1.2.3",
            "1e",
            "12a",
            "2024-06-01",
            "e5",
            "1e9999999999999999999",
        ] {
            assert!(Decimal::parse(text).is_none(), "{text:?}");
        }
    }

    #[test]
    fn a_decimal_string_bookmark_keeps_larger_rows() {
        let records: Vec<Value> = ["100", "989", "50", "99"]
            .iter()
            .map(|v| json!({ "k": v }))
            .collect();
        let kept = filter_incremental(records, "k", &json!("99"));
        let ks: Vec<&str> = kept.iter().map(|r| r["k"].as_str().unwrap()).collect();
        assert_eq!(ks, ["100", "989"]);
    }

    #[test]
    fn numbers_and_decimal_strings_compare_numerically() {
        assert_eq!(cmp(json!(100), json!("99")), Ordering::Greater);
        assert_eq!(
            cmp(json!("123456789012345678901234567890"), json!(5)),
            Ordering::Greater
        );
        assert_eq!(cmp(json!(2.5), json!("2.50")), Ordering::Equal);
        let records = vec![
            json!({"k": 7}),
            json!({"k": 123456789012345678901234567890_f64}),
        ];
        let kept = filter_incremental(records, "k", &json!("6.5"));
        assert_eq!(kept.len(), 2);
        assert_eq!(
            max_value(json!("12345678901234567890"), json!(12)),
            json!("12345678901234567890")
        );
        assert_eq!(max_value(json!("12"), json!(13)), json!(13));
    }

    #[test]
    fn timestamps_order_as_instants_across_offsets_and_fraction_widths() {
        use Ordering::*;
        let cases = [
            ("2024-01-01T10:00:00+05:00", "2024-01-01T06:00:00Z", Less),
            ("2024-01-01T10:00:00.5Z", "2024-01-01T10:00:00.45Z", Greater),
            ("2024-01-01T10:00:00Z", "2024-01-01T10:00:00.000Z", Equal),
            ("2024-01-01 10:00:00+00:00", "2024-01-01T09:59:59Z", Greater),
            ("2024-01-01T10:00:00.5", "2024-01-01T10:00:00.45", Greater),
            ("2024-01-01 10:00:00", "2024-01-01T09:00:00", Greater),
        ];
        for (a, b, want) in cases {
            assert_eq!(cmp(json!(a), json!(b)), want, "{a} vs {b}");
        }
        let records = vec![
            json!({"t": "2024-01-01T08:00:00+01:00"}),
            json!({"t": "2024-01-01T08:00:00-01:00"}),
        ];
        let kept = filter_incremental(records, "t", &json!("2024-01-01T08:00:00Z"));
        assert_eq!(kept, vec![json!({"t": "2024-01-01T08:00:00-01:00"})]);
    }

    #[test]
    fn mixed_or_unparseable_strings_stay_lexicographic() {
        assert_eq!(
            cmp(json!("2024-01-01T10:00:00Z"), json!("2024-01-01T10:00:00")),
            "2024-01-01T10:00:00Z".cmp("2024-01-01T10:00:00")
        );
        assert_eq!(cmp(json!("b"), json!("a")), Ordering::Greater);
        assert_eq!(
            cmp(json!("2024-13-99T99:99:99"), json!("2024")),
            Ordering::Greater
        );
        assert_eq!(cmp(json!("abc"), json!(5)), Ordering::Greater);
        assert_eq!(cmp(json!(true), json!("1")), Ordering::Less);
    }

    // ── ReplicationBind (#513) ──────────────────────────────────────────────

    fn bind(into: BindTarget, template: &str, format: BindFormat) -> ReplicationBind {
        ReplicationBind {
            into,
            name: "updated_after".to_owned(),
            template: template.to_owned(),
            format,
            advance_from: None,
            path: None,
            value_type: BindValueType::String,
        }
    }

    #[test]
    fn bind_defaults_template_to_bare_placeholder() {
        let b: ReplicationBind =
            serde_json::from_value(json!({ "name": "since" })).expect("deserializes");
        assert_eq!(b.into, BindTarget::Query);
        assert_eq!(b.template, "${bookmark}");
        assert_eq!(b.format, BindFormat::Raw);
        assert!(b.advance_from.is_none());
    }

    #[test]
    fn bind_render_raw_string_and_number() {
        let b = bind(BindTarget::Query, "${bookmark}", BindFormat::Raw);
        assert_eq!(b.render(&json!("2024-06-01")).unwrap(), "2024-06-01");
        assert_eq!(b.render(&json!(150)).unwrap(), "150");
    }

    #[test]
    fn bind_render_applies_operator_template() {
        let b = bind(BindTarget::Query, "gte|${bookmark}", BindFormat::Raw);
        assert_eq!(
            b.render(&json!("2024-06-01T00:00:00Z")).unwrap(),
            "gte|2024-06-01T00:00:00Z"
        );
        // Lucene range form.
        let l = bind(BindTarget::Query, "[${bookmark} TO *]", BindFormat::Raw);
        assert_eq!(l.render(&json!("20240601")).unwrap(), "[20240601 TO *]");
    }

    #[test]
    fn bind_format_iso8601_from_date_and_epoch() {
        let b = bind(BindTarget::Header, "${bookmark}", BindFormat::Iso8601);
        assert_eq!(
            b.render(&json!("2024-06-01")).unwrap(),
            "2024-06-01T00:00:00Z"
        );
        // Epoch seconds → ISO.
        assert_eq!(
            b.render(&json!(1_717_200_000)).unwrap(),
            "2024-06-01T00:00:00Z"
        );
    }

    #[test]
    fn bind_format_epoch_s_and_ms_from_iso() {
        let s = bind(BindTarget::Query, "${bookmark}", BindFormat::EpochS);
        assert_eq!(
            s.render(&json!("2024-06-01T00:00:00Z")).unwrap(),
            "1717200000"
        );
        let ms = bind(BindTarget::Query, "${bookmark}", BindFormat::EpochMs);
        assert_eq!(
            ms.render(&json!("2024-06-01T00:00:00Z")).unwrap(),
            "1717200000000"
        );
    }

    #[test]
    fn bind_format_date_truncates_datetime() {
        let b = bind(BindTarget::Query, "${bookmark}", BindFormat::Date);
        assert_eq!(
            b.render(&json!("2024-06-01T12:34:56Z")).unwrap(),
            "2024-06-01"
        );
    }

    #[test]
    fn bind_format_naive_datetime_assumed_utc() {
        let b = bind(BindTarget::Query, "${bookmark}", BindFormat::Iso8601);
        assert_eq!(
            b.render(&json!("2024-06-01T08:00:00")).unwrap(),
            "2024-06-01T08:00:00Z"
        );
    }

    #[test]
    fn bind_format_unparseable_string_errors() {
        let b = bind(BindTarget::Query, "${bookmark}", BindFormat::Iso8601);
        assert!(b.render(&json!("not-a-date")).is_err());
    }

    #[test]
    fn bind_format_raw_rejects_composite() {
        let b = bind(BindTarget::Query, "${bookmark}", BindFormat::Raw);
        assert!(b.render(&json!({"a": 1})).is_err());
        assert!(b.render(&json!(null)).is_err());
    }

    #[test]
    fn bind_validate_rejects_empty_name_and_missing_placeholder() {
        let mut b = bind(BindTarget::Query, "${bookmark}", BindFormat::Raw);
        b.name = "  ".to_owned();
        assert!(b.validate().is_err());

        let mut b2 = bind(BindTarget::Query, "no placeholder here", BindFormat::Raw);
        b2.name = "since".to_owned();
        assert!(b2.validate().is_err());

        let ok = bind(BindTarget::Query, "gte|${bookmark}", BindFormat::Raw);
        assert!(ok.validate().is_ok());
    }

    #[test]
    fn bind_placement_validation() {
        let mut b = bind(BindTarget::Body, "${bookmark}", BindFormat::Raw);
        assert!(b.validate().is_ok());
        b.path = Some("/a/0/b".into());
        assert!(b.validate().unwrap_err().to_string().contains("not both"));
        b.name.clear();
        assert!(b.validate().is_ok());
        b.path = Some("a".into());
        assert!(b.validate().is_err());
        b.path = Some("/".into());
        assert!(b.validate().is_err());
        b.path = None;
        assert!(
            b.validate()
                .unwrap_err()
                .to_string()
                .contains("needs `name`")
        );
        let mut q = bind(BindTarget::Query, "${bookmark}", BindFormat::Raw);
        q.path = Some("/a".into());
        assert!(
            q.validate()
                .unwrap_err()
                .to_string()
                .contains("only to `into: body`")
        );
        let parsed: ReplicationBind = serde_json::from_value(json!({
            "into": "body", "path": "/f/0/v", "value_type": "number"
        }))
        .unwrap();
        assert!(parsed.validate().is_ok());
        assert_eq!(parsed.value_type, BindValueType::Number);
    }

    #[test]
    fn value_type_conversion() {
        assert_eq!(BindValueType::String.to_value("5").unwrap(), json!("5"));
        assert_eq!(BindValueType::Number.to_value("5").unwrap(), json!(5));
        assert_eq!(
            BindValueType::Number
                .to_value("18446744073709551615")
                .unwrap(),
            json!(18_446_744_073_709_551_615_u64)
        );
        assert_eq!(BindValueType::Number.to_value("1.5").unwrap(), json!(1.5));
        assert!(BindValueType::Number.to_value("x").is_err());
    }

    #[test]
    fn set_body_pointer_rules() {
        let mut body = json!({"filterGroups": [{"filters": [{"value": null}]}], "v": {}, "a/b": 1});
        set_body_pointer(&mut body, "/filterGroups/0/filters/0/value", json!("x")).unwrap();
        assert_eq!(body["filterGroups"][0]["filters"][0]["value"], json!("x"));
        set_body_pointer(&mut body, "/v/after", json!("c")).unwrap();
        assert_eq!(body["v"]["after"], json!("c"));
        set_body_pointer(&mut body, "/top", json!(1)).unwrap();
        assert_eq!(body["top"], json!(1));
        set_body_pointer(&mut body, "/a~1b", json!(2)).unwrap();
        assert_eq!(body["a/b"], json!(2));
        set_body_pointer(&mut body, "/v/x~1y~0z", json!(3)).unwrap();
        assert_eq!(body["v"]["x/y~z"], json!(3));
        assert!(set_body_pointer(&mut body, "/filterGroups/1/filters", json!(1)).is_err());
        assert!(set_body_pointer(&mut body, "/missing/leaf", json!(1)).is_err());
        assert!(set_body_pointer(&mut body, "/v", json!(1)).is_err());
        assert!(set_body_pointer(&mut body, "/filterGroups/0/filters/5", json!(1)).is_err());
        assert!(set_body_pointer(&mut body, "nope", json!(1)).is_err());
    }

    #[test]
    fn bind_format_bookmark_bool_raw() {
        assert_eq!(
            format_bookmark(&json!(true), BindFormat::Raw).unwrap(),
            "true"
        );
    }

    #[test]
    fn bind_format_non_scalar_bookmark_errors() {
        // A composite / null bookmark cannot be parsed into an instant.
        assert!(format_bookmark(&json!({"a": 1}), BindFormat::Iso8601).is_err());
        assert!(format_bookmark(&json!(null), BindFormat::EpochS).is_err());
    }

    #[test]
    fn bind_format_out_of_range_epoch_errors() {
        // i64::MAX seconds is far outside chrono's representable range.
        assert!(format_bookmark(&json!(i64::MAX), BindFormat::Iso8601).is_err());
    }
}
