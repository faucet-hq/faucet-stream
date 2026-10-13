//! One value-fidelity model (RFC 0013, #885).
//!
//! A record is a `serde_json::Value`, and a `Value` holds every number it does
//! not keep as an `i64` / `u64` as an `f64`. That is the root of the most common
//! silent corruption in a data pipeline: a `DECIMAL(38,9)` rounded through a
//! float, a timestamp that lost its zone or its microseconds, a byte column
//! re-encoded differently by each connector. None of these fail a run.
//!
//! This module is the single definition of how a typed value from a backend
//! becomes JSON. Every connector renders through it, so a value means the same
//! thing whichever source produced it.
//!
//! # Canonical rendering rules
//!
//! | Backend value | JSON rendering | Converter |
//! |---|---|---|
//! | exact decimal (`DECIMAL`, `NUMERIC`, `MONEY`, `NUMBER(p,s)`, Arrow `Decimal*`, BSON `Decimal128`) | a **string** holding the exact digits at the column's own scale (`"12.50"`); never an `f64` | [`decimal_text`], [`decimal_from_i128`] |
//! | integer within `i64` / `u64` | a JSON number (a `Value` holds it exactly) | [`integer_i128`], [`integer_u128`], [`integer_text`] |
//! | integer beyond `i64` / `u64` (128-bit, `NUMERIC(38,0)`, `UInt128`) | a **string** of its digits | the same |
//! | number with no declared scale (a JSON document, a `NUMBER` with no scale, a document-store number) | a JSON number when the `f64` it parses to renders back to the same value, otherwise a string of the exact digits | [`number_text`], [`parse_json`] |
//! | `FLOAT` / `DOUBLE` | a JSON number; an `f32` keeps its shortest decimal form (`0.1`, not `0.10000000149011612`); `-0.0` keeps its sign | [`float64`], [`float32`] |
//! | `NaN`, `±Infinity` (float or decimal) | **`null`**, counted: [`record_non_finite`] increments the run's `non_finite` row account and `faucet_fidelity_non_finite_total{pipeline,row}`, and the first one per process logs a warning | [`float64`], [`float32`], [`decimal_text`] |
//! | instant (`TIMESTAMPTZ`, a timestamp with a zone, epoch + zone) | RFC 3339 with an explicit offset (`Z` for UTC) and the type's **native precision** — a microsecond column always renders six fractional digits | [`timestamp`], [`timestamp_from_epoch`] |
//! | local date-time (a timestamp with no zone) | ISO 8601 date-time **without** an offset at native precision — an offset the backend never stored is not invented | [`timestamp_naive`] |
//! | date / time of day | `YYYY-MM-DD` / `HH:MM:SS[.fff…]` at native precision | [`date`], [`time`] |
//! | binary (`BYTEA`, `BLOB`, `VARBINARY`, Arrow `Binary`) | standard **base64** with padding | [`binary`] |
//!
//! Sinks read the same renderings back: a string in a decimal column is parsed
//! as an exact decimal, never through `f64`.
//!
//! # The fidelity corpus
//!
//! `faucet_conformance::fidelity` holds the values that have broken a pipeline
//! somewhere. Every connector runs it — a source/sink pair round trip, a
//! one-sided read or write against the backend, or the connector's converter —
//! or names the reason it cannot in `crates/conformance/fidelity-exempt.txt`.
//! `scripts/fidelity-coverage.py` enforces that in CI, and
//! `scripts/decimal-f64.py` refuses a decimal converted through a float.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use base64::Engine as _;
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, SecondsFormat, TimeZone};
use serde_json::{Number, Value};

/// The largest integer an IEEE-754 double holds exactly (`2^53 - 1`). A
/// consumer that reads JSON numbers as doubles corrupts integers past it; a
/// `Value` itself holds every `i64` / `u64` exactly.
pub const MAX_SAFE_INTEGER: u64 = (1 << 53) - 1;

/// The precision a temporal type carries natively.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum TimePrecision {
    /// Whole seconds.
    Seconds,
    /// Milliseconds: three fractional digits.
    Millis,
    /// Microseconds: six fractional digits.
    Micros,
    /// Nanoseconds: nine fractional digits.
    Nanos,
}

impl TimePrecision {
    /// The precision of a type with `digits` fractional-second digits
    /// (`TIMESTAMP(3)`, `DATETIME2(7)`), rounded up to the next of 0/3/6/9.
    pub fn from_digits(digits: u32) -> Self {
        match digits {
            0 => Self::Seconds,
            1..=3 => Self::Millis,
            4..=6 => Self::Micros,
            _ => Self::Nanos,
        }
    }

    fn seconds_format(self) -> SecondsFormat {
        match self {
            Self::Seconds => SecondsFormat::Secs,
            Self::Millis => SecondsFormat::Millis,
            Self::Micros => SecondsFormat::Micros,
            Self::Nanos => SecondsFormat::Nanos,
        }
    }

    fn fraction(self, nanos: u32) -> String {
        match self {
            Self::Seconds => String::new(),
            Self::Millis => format!(".{:03}", nanos / 1_000_000),
            Self::Micros => format!(".{:06}", nanos / 1_000),
            Self::Nanos => format!(".{nanos:09}"),
        }
    }

    fn per_second(self) -> i64 {
        match self {
            Self::Seconds => 1,
            Self::Millis => 1_000,
            Self::Micros => 1_000_000,
            Self::Nanos => 1_000_000_000,
        }
    }
}

/// A value a converter could not read.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum FidelityError {
    /// The text is not a decimal number.
    #[error("{0:?} is not a decimal number")]
    NotANumber(String),
    /// The text is not an integer.
    #[error("{0:?} is not an integer")]
    NotAnInteger(String),
    /// An epoch offset outside the range a date-time can represent.
    #[error("epoch value {0} is outside the representable date-time range")]
    OutOfRange(i64),
}

/// `(negative, digits without leading zeros, scale)` of a decimal literal:
/// `value = digits × 10^-scale`. `None` when `text` is not one.
fn parse_decimal(text: &str) -> Option<(bool, String, i64)> {
    let t = text.trim();
    let (neg, rest) = match t.as_bytes().first()? {
        b'-' => (true, &t[1..]),
        b'+' => (false, &t[1..]),
        _ => (false, t),
    };
    let (mantissa, exp) = match rest.find(['e', 'E']) {
        Some(i) => (&rest[..i], rest[i + 1..].parse::<i64>().ok()?),
        None => (rest, 0),
    };
    let (int, frac) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if (int.is_empty() && frac.is_empty())
        || !int.bytes().chain(frac.bytes()).all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let digits: String = format!("{int}{frac}");
    let digits = digits.trim_start_matches('0').to_string();
    let scale = (frac.len() as i64).checked_sub(exp)?;
    Some((neg, digits, scale))
}

/// Positional text of `digits × 10^-scale`, keeping the scale's trailing zeros.
fn render_decimal(negative: bool, digits: &str, scale: i64) -> String {
    let mut out = String::new();
    if scale <= 0 {
        if digits.is_empty() {
            return "0".to_string();
        }
        if negative {
            out.push('-');
        }
        out.push_str(digits);
        out.extend(std::iter::repeat_n('0', (-scale) as usize));
        return out;
    }
    let scale = scale as usize;
    let padded = if digits.len() <= scale {
        format!("{}{digits}", "0".repeat(scale + 1 - digits.len()))
    } else {
        digits.to_string()
    };
    let (int, frac) = padded.split_at(padded.len() - scale);
    if negative && !digits.is_empty() {
        out.push('-');
    }
    out.push_str(int);
    out.push('.');
    out.push_str(frac);
    out
}

fn is_non_finite_word(text: &str) -> bool {
    let t = text.trim().trim_start_matches(['+', '-']);
    t.eq_ignore_ascii_case("nan")
        || t.eq_ignore_ascii_case("inf")
        || t.eq_ignore_ascii_case("infinity")
}

/// An exact decimal from its text, as a JSON string at its own scale
/// (`"1.50"` stays `"1.50"`, `"1.2E+3"` becomes `"1200"`). `NaN` / `Infinity`
/// (a `NUMERIC` can hold them) become a counted `null`.
pub fn decimal_text(text: &str) -> Result<Value, FidelityError> {
    if is_non_finite_word(text) {
        record_non_finite();
        return Ok(Value::Null);
    }
    let (neg, digits, scale) =
        parse_decimal(text).ok_or_else(|| FidelityError::NotANumber(text.to_string()))?;
    Ok(Value::String(render_decimal(neg, &digits, scale)))
}

/// An exact decimal `unscaled × 10^-scale` (the Arrow / driver representation)
/// as a JSON string.
pub fn decimal_from_i128(unscaled: i128, scale: i32) -> Value {
    let digits = unscaled.unsigned_abs().to_string();
    let digits = digits.trim_start_matches('0');
    Value::String(render_decimal(unscaled < 0, digits, i64::from(scale)))
}

/// An integer as a JSON number when a `Value` holds it exactly (`i64` / `u64`
/// range), otherwise as a string of its digits.
pub fn integer_i128(v: i128) -> Value {
    if let Ok(i) = i64::try_from(v) {
        Value::Number(i.into())
    } else if let Ok(u) = u64::try_from(v) {
        Value::Number(u.into())
    } else {
        Value::String(v.to_string())
    }
}

/// An unsigned integer: a JSON number up to `u64::MAX`, a string beyond.
pub fn integer_u128(v: u128) -> Value {
    match u64::try_from(v) {
        Ok(u) => Value::Number(u.into()),
        Err(_) => Value::String(v.to_string()),
    }
}

/// An integer from its text, of any size, per [`integer_i128`].
pub fn integer_text(text: &str) -> Result<Value, FidelityError> {
    let t = text.trim();
    let body = t.strip_prefix(['-', '+']).unwrap_or(t);
    if body.is_empty() || !body.bytes().all(|b| b.is_ascii_digit()) {
        return Err(FidelityError::NotAnInteger(text.to_string()));
    }
    if let Ok(i) = t.parse::<i64>() {
        return Ok(Value::Number(i.into()));
    }
    if let Ok(u) = t.trim_start_matches('+').parse::<u64>() {
        return Ok(Value::Number(u.into()));
    }
    let digits = body.trim_start_matches('0');
    let neg = t.starts_with('-') && !digits.is_empty();
    Ok(Value::String(format!(
        "{}{}",
        if neg { "-" } else { "" },
        if digits.is_empty() { "0" } else { digits }
    )))
}

/// A number with no declared scale (a JSON document, an unscaled `NUMBER`, a
/// document-store number): a JSON number when that is exact, the exact digits
/// as a string otherwise.
pub fn number_text(text: &str) -> Result<Value, FidelityError> {
    let t = text.trim();
    if is_non_finite_word(t) {
        record_non_finite();
        return Ok(Value::Null);
    }
    if !t.contains(['.', 'e', 'E'])
        && let Ok(v) = integer_text(t)
    {
        return Ok(v);
    }
    let (neg, digits, scale) =
        parse_decimal(t).ok_or_else(|| FidelityError::NotANumber(text.to_string()))?;
    if digits.is_empty() {
        return Ok(Value::Number(0.into()));
    }
    let positional = render_decimal(neg, &digits, scale);
    for candidate in [t.trim_start_matches('+'), positional.as_str()] {
        if crate::json_numbers::is_exact(candidate)
            && let Ok(Value::Number(n)) = serde_json::from_str::<Value>(candidate)
        {
            return Ok(Value::Number(n));
        }
    }
    if scale <= 0 && digits.len() as i64 - scale <= 39 {
        return integer_text(&positional);
    }
    Ok(Value::String(positional))
}

/// A double as a JSON number; `NaN` / `±Infinity` become a counted `null`.
pub fn float64(v: f64) -> Value {
    match Number::from_f64(v) {
        Some(n) => Value::Number(n),
        None => {
            record_non_finite();
            Value::Null
        }
    }
}

/// A single-precision float through its shortest decimal form, so `0.1f32`
/// renders `0.1`; `NaN` / `±Infinity` become a counted `null`.
pub fn float32(v: f32) -> Value {
    if !v.is_finite() {
        record_non_finite();
        return Value::Null;
    }
    v.to_string()
        .parse::<f64>()
        .map_or_else(|_| float64(f64::from(v)), float64)
}

/// The text a sink binds for a JSON number that is not an integer: its
/// shortest round-trip decimal form, so the server parses the value the record
/// holds. An `f64` bound into a `DECIMAL` column is converted from binary and
/// lands as `0.1000000000000000055…`. `None` for an integer (bind it natively).
pub fn non_integer_text(n: &Number) -> Option<String> {
    n.is_f64().then(|| n.to_string())
}

/// A PostgreSQL-wire binary `NUMERIC` (`ndigits`, `weight`, `sign`,
/// `dscale`, then base-10000 digits) as an exact decimal string at its
/// display scale — the column's declared scale. `NaN` / `±Infinity` are a
/// counted `null`. `None` when the bytes are not a binary numeric.
pub fn decimal_from_pg_numeric(bytes: &[u8]) -> Option<Value> {
    let word = |i: usize| -> Option<u16> {
        Some(u16::from_be_bytes([*bytes.get(i)?, *bytes.get(i + 1)?]))
    };
    let ndigits = usize::from(word(0)?);
    let weight = i64::from(word(2)? as i16);
    let sign = word(4)?;
    let dscale = usize::from(word(6)?);
    if matches!(sign, 0xC000 | 0xD000 | 0xF000) {
        record_non_finite();
        return Some(Value::Null);
    }
    if !matches!(sign, 0x0000 | 0x4000) || bytes.len() != 8 + 2 * ndigits {
        return None;
    }
    let digits: Vec<u16> = (0..ndigits)
        .map(|i| word(8 + 2 * i))
        .collect::<Option<_>>()?;
    if digits.iter().any(|d| *d > 9999) {
        return None;
    }
    let group = |g: i64| -> u16 {
        usize::try_from(g)
            .ok()
            .and_then(|i| digits.get(i).copied())
            .unwrap_or(0)
    };
    let mut text = String::new();
    if sign == 0x4000 {
        text.push('-');
    }
    if weight < 0 {
        text.push('0');
    } else {
        text.push_str(&group(0).to_string());
        for g in 1..=weight {
            text.push_str(&format!("{:04}", group(g)));
        }
    }
    if dscale > 0 {
        let mut frac = String::new();
        let mut g = weight + 1;
        while frac.len() < dscale {
            frac.push_str(&format!("{:04}", group(g)));
            g += 1;
        }
        frac.truncate(dscale);
        text.push('.');
        text.push_str(&frac);
    }
    decimal_text(&text).ok()
}

/// Whether a SQL column type holds an exact decimal (`NUMERIC(p,s)`,
/// `DECIMAL`, `MONEY`, `NUMBER`, `BIGNUMERIC`): a sink binds such a column's
/// value as text, so the server parses exactly the digits the record holds.
pub fn is_exact_decimal_type(sql_type: &str) -> bool {
    let t = sql_type.trim().to_ascii_lowercase();
    [
        "numeric",
        "decimal",
        "dec",
        "money",
        "smallmoney",
        "number",
        "bignumeric",
        "bigdecimal",
    ]
    .iter()
    .any(|p| t == *p || t.starts_with(&format!("{p}(")) || t.starts_with(&format!("{p} ")))
}

/// Bytes as standard base64 with padding.
pub fn binary(bytes: &[u8]) -> Value {
    Value::String(base64::engine::general_purpose::STANDARD.encode(bytes))
}

/// An instant as RFC 3339 with an explicit offset (`Z` for UTC) at `precision`.
pub fn timestamp<Tz: TimeZone>(dt: &DateTime<Tz>, precision: TimePrecision) -> Value {
    Value::String(
        dt.fixed_offset()
            .to_rfc3339_opts(precision.seconds_format(), true),
    )
}

/// A local date-time (no zone in the source type) without an offset, at
/// `precision`.
pub fn timestamp_naive(dt: &NaiveDateTime, precision: TimePrecision) -> Value {
    Value::String(format!(
        "{}{}",
        dt.format("%Y-%m-%dT%H:%M:%S"),
        precision.fraction(dt.and_utc().timestamp_subsec_nanos() % 1_000_000_000)
    ))
}

/// A date as `YYYY-MM-DD`.
pub fn date(d: &NaiveDate) -> Value {
    Value::String(d.format("%Y-%m-%d").to_string())
}

/// A time of day as `HH:MM:SS` plus the fraction `precision` carries.
pub fn time(t: &NaiveTime, precision: TimePrecision) -> Value {
    use chrono::Timelike;
    Value::String(format!(
        "{}{}",
        t.format("%H:%M:%S"),
        precision.fraction(t.nanosecond() % 1_000_000_000)
    ))
}

/// An epoch offset in `unit`s: with a zone, an instant ([`timestamp`]);
/// without one, a local date-time ([`timestamp_naive`]).
pub fn timestamp_from_epoch(
    value: i64,
    unit: TimePrecision,
    zone: Option<FixedOffset>,
) -> Result<Value, FidelityError> {
    let per = unit.per_second();
    let secs = value.div_euclid(per);
    let nanos = (value.rem_euclid(per) * (1_000_000_000 / per)) as u32;
    let utc = DateTime::from_timestamp(secs, nanos).ok_or(FidelityError::OutOfRange(value))?;
    Ok(match zone {
        Some(z) => timestamp(&utc.with_timezone(&z), unit),
        None => timestamp_naive(&utc.naive_utc(), unit),
    })
}

/// Parse JSON text a serializer produced (an HTTP body, a message payload, a
/// JSON Lines record) the way [`serde_json::from_str`] does, except that a
/// number literal the parsed value would not spell again — an integer beyond
/// `u64`, a decimal with more digits than a double carries — becomes a string
/// of its exact digits instead of the nearest `f64`. A 17-digit rendering of a
/// double (`0.10000000000000001`) names that double and stays a number.
pub fn from_str(text: &str) -> Result<Value, serde_json::Error> {
    let value: Value = serde_json::from_str(text)?;
    let lossy = crate::file_format::json::unfaithful_literals(text.as_bytes());
    if lossy.is_empty() {
        return Ok(value);
    }
    let mut rewritten = String::with_capacity(text.len() + 2 * lossy.len());
    let mut last = 0;
    for r in &lossy {
        rewritten.push_str(&text[last..r.start]);
        rewritten.push('"');
        rewritten.push_str(&text[r.clone()]);
        rewritten.push('"');
        last = r.end;
    }
    rewritten.push_str(&text[last..]);
    serde_json::from_str(&rewritten)
}

/// [`from_str`] over bytes, for a body that should be UTF-8.
pub fn from_slice(bytes: &[u8]) -> Result<Value, serde_json::Error> {
    match std::str::from_utf8(bytes) {
        Ok(text) => from_str(text),
        Err(_) => serde_json::from_slice(bytes),
    }
}

/// Parse JSON text a database stored (a `JSON` / `JSONB` / `VARIANT` column)
/// keeping every number exact: a number a `Value` cannot hold exactly — by
/// decimal value, so `0.10000000000000001` is not `0.1` — becomes a string of
/// its digits (see [`number_text`]).
pub fn parse_json(text: &[u8]) -> Result<Value, FidelityError> {
    let text = std::str::from_utf8(text)
        .map_err(|e| FidelityError::NotANumber(format!("invalid UTF-8 in JSON: {e}")))?;
    crate::json_numbers::parse_json_exact(text, crate::json_numbers::JsonBigNumbers::String)
        .map(|(v, _)| v)
        .map_err(|e| FidelityError::NotANumber(e.to_string()))
}

/// A per-run count of non-finite values rendered as `null`.
///
/// [`Pipeline::run`](crate::Pipeline::run) installs one around the run;
/// [`record_non_finite`] counts into the innermost installed counter. A
/// connector that converts values on a task or thread of its own carries the
/// counter there with [`NonFiniteCounter::current`] and
/// [`NonFiniteCounter::scope`] / [`NonFiniteCounter::scope_sync`].
#[derive(Debug, Clone, Default)]
pub struct NonFiniteCounter(Arc<AtomicU64>);

tokio::task_local! {
    static NON_FINITE: NonFiniteCounter;
}

static UNSCOPED: AtomicU64 = AtomicU64::new(0);
static WARNED: AtomicBool = AtomicBool::new(false);

impl NonFiniteCounter {
    /// A counter at zero.
    pub fn new() -> Self {
        Self::default()
    }

    /// Values counted so far.
    pub fn count(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }

    /// The counter installed around the current task, if any.
    pub fn current() -> Option<Self> {
        NON_FINITE.try_with(Clone::clone).ok()
    }

    /// Run `fut` with this counter installed.
    pub fn scope<F: Future>(&self, fut: F) -> impl Future<Output = F::Output> {
        NON_FINITE.scope(self.clone(), fut)
    }

    /// Run `f` (on a blocking thread, say) with this counter installed.
    pub fn scope_sync<R>(&self, f: impl FnOnce() -> R) -> R {
        NON_FINITE.sync_scope(self.clone(), f)
    }
}

/// Run `f` under the current task's counter, if any — for a connector that
/// hands conversion to `spawn_blocking`.
pub fn propagate<R>(counter: &Option<NonFiniteCounter>, f: impl FnOnce() -> R) -> R {
    match counter {
        Some(c) => c.scope_sync(f),
        None => f(),
    }
}

/// Count one non-finite value rendered as `null`. Outside any run (a library
/// call with no [`Pipeline`](crate::Pipeline)) it counts into
/// `faucet_fidelity_non_finite_total{pipeline="",row=""}`.
pub fn record_non_finite() {
    if NON_FINITE
        .try_with(|c| c.0.fetch_add(1, Ordering::Relaxed))
        .is_err()
    {
        UNSCOPED.fetch_add(1, Ordering::Relaxed);
        metrics::counter!("faucet_fidelity_non_finite_total", "pipeline" => "", "row" => "")
            .increment(1);
    }
    if !WARNED.swap(true, Ordering::Relaxed) {
        tracing::warn!(
            "a NaN or infinite value has no JSON representation and was written as null; \
             the run summary counts them as `non_finite`"
        );
    }
}

/// Non-finite values counted outside any installed counter, process-wide.
pub fn unscoped_non_finite() -> u64 {
    UNSCOPED.load(Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn decimals_render_as_exact_strings_at_their_scale() {
        for (input, out) in [
            ("1.50", "1.50"),
            ("-1.50", "-1.50"),
            ("+7", "7"),
            ("0007.10", "7.10"),
            ("1.2E+3", "1200"),
            ("1.25e-3", "0.00125"),
            (".5", "0.5"),
            ("5.", "5"),
            ("-0.00", "0.00"),
            ("0", "0"),
            ("-0", "0"),
            (
                "12345678901234567890.123456789",
                "12345678901234567890.123456789",
            ),
            ("1e2", "100"),
        ] {
            assert_eq!(decimal_text(input).unwrap(), json!(out), "{input}");
        }
        for bad in ["", "-", "1.2.3", "abc", "1e", "1e9999999999999999999", "."] {
            assert!(decimal_text(bad).is_err(), "{bad}");
        }
        assert_eq!(
            decimal_text("x").unwrap_err().to_string(),
            "\"x\" is not a decimal number"
        );
    }

    #[test]
    fn decimals_from_unscaled_parts() {
        assert_eq!(decimal_from_i128(12345, 2), json!("123.45"));
        assert_eq!(decimal_from_i128(-5, 3), json!("-0.005"));
        assert_eq!(decimal_from_i128(0, 2), json!("0.00"));
        assert_eq!(decimal_from_i128(42, 0), json!("42"));
        assert_eq!(decimal_from_i128(42, -2), json!("4200"));
        assert_eq!(decimal_from_i128(0, -2), json!("0"));
        assert_eq!(
            decimal_from_i128(i128::MIN, 10),
            json!("-17014118346046923173168730371.5884105728")
        );
    }

    #[test]
    fn integers_are_numbers_until_a_value_cannot_hold_them() {
        assert_eq!(integer_i128(i128::from(i64::MIN)), json!(i64::MIN));
        assert_eq!(integer_i128(i128::from(u64::MAX)), json!(u64::MAX));
        assert_eq!(
            integer_i128(i128::from(u64::MAX) + 1),
            json!("18446744073709551616")
        );
        assert_eq!(
            integer_i128(i128::from(i64::MIN) - 1),
            json!("-9223372036854775809")
        );
        assert_eq!(integer_u128(7), json!(7));
        assert_eq!(integer_u128(u128::MAX), json!(u128::MAX.to_string()));
        assert_eq!(integer_text(" 42 ").unwrap(), json!(42));
        assert_eq!(integer_text("+42").unwrap(), json!(42));
        assert_eq!(
            integer_text("-000123456789012345678901").unwrap(),
            json!("-123456789012345678901")
        );
        assert_eq!(integer_text("-0000000000000000000000").unwrap(), json!(0));
        assert_eq!(
            integer_text("18446744073709551615").unwrap(),
            json!(u64::MAX)
        );
        assert!(integer_text("1.5").is_err());
        assert!(integer_text("-").is_err());
        assert_eq!(
            integer_text("x").unwrap_err().to_string(),
            "\"x\" is not an integer"
        );
        const { assert!(MAX_SAFE_INTEGER == 9_007_199_254_740_991) };
    }

    #[test]
    fn unscaled_numbers_stay_numbers_only_when_exact() {
        assert_eq!(number_text("1.5").unwrap(), json!(1.5));
        assert_eq!(number_text(".25").unwrap(), json!(0.25));
        assert_eq!(number_text("-.5").unwrap(), json!(-0.5));
        assert_eq!(number_text("+2.5").unwrap(), json!(2.5));
        assert_eq!(number_text("1E+3").unwrap(), json!(1000.0));
        assert_eq!(number_text("0.1").unwrap(), json!(0.1));
        assert_eq!(number_text("42").unwrap(), json!(42));
        assert_eq!(number_text("1e2").unwrap(), json!(100.0));
        assert_eq!(number_text("0.000").unwrap(), json!(0));
        assert_eq!(
            number_text("12345678901234567890.123").unwrap(),
            json!("12345678901234567890.123")
        );
        assert_eq!(
            number_text("123456789012345678901234567890").unwrap(),
            json!("123456789012345678901234567890")
        );
        assert_eq!(number_text("1e30").unwrap(), json!(1e30));
        assert_eq!(
            number_text("1.000000000000000000001e400").unwrap(),
            json!(format!("1000000000000000000001{}", "0".repeat(379)))
        );
        assert_eq!(
            number_text("123456789012345678901234567890e3").unwrap(),
            json!("123456789012345678901234567890000")
        );
        assert!(number_text("abc").is_err());
    }

    #[test]
    fn floats_keep_their_value_and_sign() {
        assert_eq!(float64(1.5), json!(1.5));
        assert_eq!(float64(-0.0).to_string(), "-0.0");
        assert_eq!(float32(0.1), json!(0.1));
        assert_eq!(float32(-0.0).to_string(), "-0.0");
        assert_eq!(float32(3.0e38), json!(3.0e38));
        assert_eq!(float32(16_777_217.0), json!(16_777_216.0));
    }

    #[tokio::test]
    async fn non_finite_values_become_counted_nulls() {
        let counter = NonFiniteCounter::new();
        let seen = counter
            .scope(async {
                assert!(NonFiniteCounter::current().is_some());
                assert_eq!(float64(f64::NAN), Value::Null);
                assert_eq!(float64(f64::INFINITY), Value::Null);
                assert_eq!(float32(f32::NEG_INFINITY), Value::Null);
                assert_eq!(decimal_text("NaN").unwrap(), Value::Null);
                assert_eq!(number_text("-Infinity").unwrap(), Value::Null);
                let inner = NonFiniteCounter::current();
                std::thread::spawn(move || propagate(&inner, || float64(f64::NAN)))
                    .join()
                    .unwrap();
                propagate(&None, || ());
                NonFiniteCounter::current().unwrap().count()
            })
            .await;
        assert_eq!(seen, 6);
        assert_eq!(counter.count(), 6);
        assert!(NonFiniteCounter::current().is_none());
        let before = unscoped_non_finite();
        assert_eq!(float64(f64::NAN), Value::Null);
        assert!(unscoped_non_finite() > before);
        assert_eq!(counter.scope_sync(|| 1), 1);
    }

    #[test]
    fn non_integers_bind_as_their_shortest_text() {
        let n = |v: Value| match v {
            Value::Number(n) => n,
            _ => unreachable!(),
        };
        assert_eq!(non_integer_text(&n(json!(0.1))), Some("0.1".into()));
        assert_eq!(
            non_integer_text(&n(json!(-1.5e300))),
            Some("-1.5e+300".into())
        );
        assert_eq!(non_integer_text(&n(json!(7))), None);
        assert_eq!(non_integer_text(&n(json!(u64::MAX))), None);
    }

    /// `NUMERIC` in the PostgreSQL binary wire format.
    fn pg_numeric(weight: i16, sign: u16, dscale: u16, digits: &[u16]) -> Vec<u8> {
        let mut b = Vec::new();
        for w in [digits.len() as u16, weight as u16, sign, dscale] {
            b.extend_from_slice(&w.to_be_bytes());
        }
        for d in digits {
            b.extend_from_slice(&d.to_be_bytes());
        }
        b
    }

    #[test]
    fn binary_pg_numerics_keep_their_scale() {
        // 1.50 at scale 2.
        assert_eq!(
            decimal_from_pg_numeric(&pg_numeric(0, 0, 2, &[1, 5000])),
            Some(json!("1.50"))
        );
        // 12345678901234567890.123456789 at scale 9.
        assert_eq!(
            decimal_from_pg_numeric(&pg_numeric(
                4,
                0,
                9,
                &[1234, 5678, 9012, 3456, 7890, 1234, 5678, 9000]
            )),
            Some(json!("12345678901234567890.123456789"))
        );
        // -0.0005 at scale 4: weight -1 skips the units group.
        assert_eq!(
            decimal_from_pg_numeric(&pg_numeric(-1, 0x4000, 4, &[5])),
            Some(json!("-0.0005"))
        );
        // 0.00000001: weight -2.
        assert_eq!(
            decimal_from_pg_numeric(&pg_numeric(-2, 0, 8, &[1])),
            Some(json!("0.00000001"))
        );
        // 20000 (trailing zero group dropped by the server), scale 0.
        assert_eq!(
            decimal_from_pg_numeric(&pg_numeric(1, 0, 0, &[2])),
            Some(json!("20000"))
        );
        assert_eq!(
            decimal_from_pg_numeric(&pg_numeric(0, 0, 0, &[])),
            Some(json!("0"))
        );
        assert_eq!(
            decimal_from_pg_numeric(&pg_numeric(0, 0xC000, 0, &[])),
            Some(Value::Null)
        );
        assert_eq!(
            decimal_from_pg_numeric(&pg_numeric(0, 0x1234, 0, &[])),
            None
        );
        assert_eq!(
            decimal_from_pg_numeric(&pg_numeric(0, 0, 0, &[10000])),
            None
        );
        assert_eq!(decimal_from_pg_numeric(&[0, 1]), None);
        let mut short = pg_numeric(0, 0, 0, &[1, 2]);
        short.pop();
        assert_eq!(decimal_from_pg_numeric(&short), None);
    }

    #[test]
    fn exact_decimal_types_are_recognised() {
        for t in [
            "NUMERIC(38,9)",
            "numeric",
            "DECIMAL(10, 2)",
            "money",
            "NUMBER",
            "dec(5)",
            "BIGNUMERIC",
        ] {
            assert!(is_exact_decimal_type(t), "{t}");
        }
        for t in [
            "double precision",
            "real",
            "bigint",
            "text",
            "numerical_thing",
            "decimals",
        ] {
            assert!(!is_exact_decimal_type(t), "{t}");
        }
    }

    #[test]
    fn binary_is_standard_base64() {
        assert_eq!(binary(&[0, 1, 2, 253, 254, 255]), json!("AAEC/f7/"));
        assert_eq!(binary(b""), json!(""));
    }

    #[test]
    fn instants_carry_an_offset_and_native_precision() {
        let utc = DateTime::from_timestamp(1_758_198_896, 123_000_000).unwrap();
        assert_eq!(
            timestamp(&utc, TimePrecision::Micros),
            json!("2025-09-18T12:34:56.123000Z")
        );
        assert_eq!(
            timestamp(&utc, TimePrecision::Seconds),
            json!("2025-09-18T12:34:56Z")
        );
        let ist = FixedOffset::east_opt(5 * 3600 + 1800).unwrap();
        assert_eq!(
            timestamp(&utc.with_timezone(&ist), TimePrecision::Millis),
            json!("2025-09-18T18:04:56.123+05:30")
        );
        assert_eq!(
            timestamp(&utc, TimePrecision::Nanos),
            json!("2025-09-18T12:34:56.123000000Z")
        );
        assert_eq!(
            timestamp_naive(&utc.naive_utc(), TimePrecision::Micros),
            json!("2025-09-18T12:34:56.123000")
        );
        assert_eq!(
            timestamp_naive(&utc.naive_utc(), TimePrecision::Seconds),
            json!("2025-09-18T12:34:56")
        );
        assert_eq!(date(&utc.date_naive()), json!("2025-09-18"));
        assert_eq!(
            time(&utc.time(), TimePrecision::Nanos),
            json!("12:34:56.123000000")
        );
    }

    #[test]
    fn epoch_values_honour_unit_and_zone() {
        assert_eq!(
            timestamp_from_epoch(-1, TimePrecision::Micros, None).unwrap(),
            json!("1969-12-31T23:59:59.999999")
        );
        assert_eq!(
            timestamp_from_epoch(
                1_500,
                TimePrecision::Millis,
                Some(FixedOffset::east_opt(0).unwrap())
            )
            .unwrap(),
            json!("1970-01-01T00:00:01.500Z")
        );
        assert_eq!(
            timestamp_from_epoch(5, TimePrecision::Seconds, None).unwrap(),
            json!("1970-01-01T00:00:05")
        );
        assert_eq!(
            timestamp_from_epoch(1, TimePrecision::Nanos, None).unwrap(),
            json!("1970-01-01T00:00:00.000000001")
        );
        let err = timestamp_from_epoch(i64::MAX, TimePrecision::Seconds, None).unwrap_err();
        assert!(err.to_string().contains("outside"), "{err}");
    }

    #[test]
    fn precision_from_declared_digits() {
        assert_eq!(TimePrecision::from_digits(0), TimePrecision::Seconds);
        assert_eq!(TimePrecision::from_digits(2), TimePrecision::Millis);
        assert_eq!(TimePrecision::from_digits(6), TimePrecision::Micros);
        assert_eq!(TimePrecision::from_digits(7), TimePrecision::Nanos);
    }

    #[test]
    fn serialized_json_keeps_unfaithful_literals_as_strings() {
        let v = from_str(
            r#"{"id": 18446744073709551616, "amt": 0.12345678901234567890, "d": 0.10000000000000001, "s": "1234567890123456789", "n": [1, 2.5]}"#,
        )
        .unwrap();
        assert_eq!(
            v,
            json!({"id": "18446744073709551616", "amt": "0.12345678901234567890", "d": 0.1, "s": "1234567890123456789", "n": [1, 2.5]})
        );
        assert_eq!(from_slice(b"[1]").unwrap(), json!([1]));
        assert!(from_str("{").is_err());
        assert!(from_slice(&[b'"', 0xff, b'"']).is_err());
        assert_eq!(
            from_str("1e99999").is_err(),
            serde_json::from_str::<Value>("1e99999").is_err()
        );
    }

    #[test]
    fn json_text_parses_with_exact_numbers() {
        let v = parse_json(br#"{"a": 12345678901234567890.5, "b": 1.5, "c": [7]}"#).unwrap();
        assert_eq!(
            v,
            json!({"a": "12345678901234567890.5", "b": 1.5, "c": [7]})
        );
        assert!(parse_json(b"{").is_err());
        assert!(parse_json(&[0xff]).is_err());
    }
}
