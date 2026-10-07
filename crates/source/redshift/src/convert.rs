//! Row decoding and parameter binding for the Redshift source.
//!
//! Redshift is PostgreSQL wire-compatible, so this mirrors the native Postgres
//! source: values decode through `sqlx`'s Postgres row API, and JSON bind values
//! are classified before binding so large integers keep full precision.

use faucet_core::FaucetError;
use serde_json::Value;
use sqlx::{Column, Row};

/// Convert a raw Redshift/Postgres column value to a `serde_json::Value`.
///
/// A SQL `NULL` is `Value::Null`. A non-NULL cell is decoded by trying the
/// known types, then its UTF-8 text form (how SUPER, GEOMETRY and other
/// Redshift-only types arrive); a cell that is neither is an error naming the
/// column and its type — never a silent `NULL`.
pub(crate) fn pg_value_to_json(
    row: &sqlx::postgres::PgRow,
    col_name: &str,
) -> Result<Value, FaucetError> {
    use sqlx::postgres::types::{PgInterval, PgTimeTz};
    use sqlx::types::chrono::{FixedOffset, NaiveTime};
    use sqlx::{TypeInfo, ValueRef};

    let raw = row
        .try_get_raw(col_name)
        .map_err(|e| FaucetError::Source(format!("redshift: column `{col_name}`: {e}")))?;
    if raw.is_null() {
        return Ok(Value::Null);
    }
    let type_name = raw.type_info().name().to_string();
    if let Ok(v) = row.try_get::<Value, _>(col_name) {
        return Ok(v);
    }
    if let Ok(v) = row.try_get::<String, _>(col_name) {
        return Ok(Value::String(v));
    }
    if let Ok(v) = row.try_get::<i64, _>(col_name) {
        return Ok(Value::Number(v.into()));
    }
    if let Ok(v) = row.try_get::<i32, _>(col_name) {
        return Ok(Value::Number(v.into()));
    }
    if let Ok(v) = row.try_get::<i16, _>(col_name) {
        return Ok(Value::Number(v.into()));
    }
    if let Ok(v) = row.try_get::<f64, _>(col_name) {
        return Ok(float_to_json(v));
    }
    if let Ok(v) = row.try_get::<f32, _>(col_name) {
        return Ok(float_to_json(f64::from(v)));
    }
    if let Ok(v) = row.try_get::<bool, _>(col_name) {
        return Ok(Value::Bool(v));
    }
    // Timestamps → RFC3339 / ISO-8601 strings.
    if let Ok(v) =
        row.try_get::<sqlx::types::chrono::DateTime<sqlx::types::chrono::Utc>, _>(col_name)
    {
        return Ok(Value::String(v.to_rfc3339()));
    }
    if let Ok(v) = row.try_get::<sqlx::types::chrono::NaiveDateTime, _>(col_name) {
        return Ok(Value::String(v.to_string()));
    }
    if let Ok(v) = row.try_get::<sqlx::types::chrono::NaiveDate, _>(col_name) {
        return Ok(Value::String(v.to_string()));
    }
    if let Ok(v) = row.try_get::<NaiveTime, _>(col_name) {
        return Ok(Value::String(v.to_string()));
    }
    if let Ok(v) = row.try_get::<PgTimeTz<NaiveTime, FixedOffset>, _>(col_name) {
        return Ok(Value::String(format!("{}{}", v.time, v.offset)));
    }
    if let Ok(v) = row.try_get::<PgInterval, _>(col_name) {
        return Ok(Value::String(interval_to_iso(
            v.months,
            v.days,
            v.microseconds,
        )));
    }
    if let Ok(v) = row.try_get::<sqlx::types::Uuid, _>(col_name) {
        return Ok(Value::String(v.to_string()));
    }
    // NUMERIC / DECIMAL → string, preserving exact precision.
    if let Ok(v) = row.try_get::<sqlx::types::BigDecimal, _>(col_name) {
        return Ok(Value::String(v.to_string()));
    }
    // Binary (VARBYTE / bytea) → base64 so it survives the JSON round-trip.
    if let Ok(v) = row.try_get::<Vec<u8>, _>(col_name) {
        use base64::Engine as _;
        return Ok(Value::String(
            base64::engine::general_purpose::STANDARD.encode(v),
        ));
    }
    if let Ok(v) = row.try_get_unchecked::<String, _>(col_name) {
        return Ok(Value::String(v));
    }
    Err(FaucetError::Source(format!(
        "redshift: cannot decode column `{col_name}` of type {type_name}; \
         cast it in the query (e.g. `{col_name}::varchar`)"
    )))
}

/// A float as a JSON number, or its text when it is not finite (JSON has no
/// NaN/Infinity) — never `NULL`.
fn float_to_json(v: f64) -> Value {
    serde_json::Number::from_f64(v)
        .map(Value::Number)
        .unwrap_or_else(|| Value::String(v.to_string()))
}

/// An INTERVAL as an ISO 8601 duration (`P1Y2M3DT4H5M6.5S`).
pub(crate) fn interval_to_iso(months: i32, days: i32, micros: i64) -> String {
    let mut out = String::from("P");
    let (years, months) = (months / 12, months % 12);
    if years != 0 {
        out.push_str(&format!("{years}Y"));
    }
    if months != 0 {
        out.push_str(&format!("{months}M"));
    }
    if days != 0 {
        out.push_str(&format!("{days}D"));
    }
    if micros != 0 {
        out.push('T');
        let (hours, rest) = (micros / 3_600_000_000, micros % 3_600_000_000);
        let (minutes, rest) = (rest / 60_000_000, rest % 60_000_000);
        if hours != 0 {
            out.push_str(&format!("{hours}H"));
        }
        if minutes != 0 {
            out.push_str(&format!("{minutes}M"));
        }
        if rest != 0 {
            let (secs, frac) = (rest / 1_000_000, (rest % 1_000_000).abs());
            let sign = if rest < 0 && secs == 0 { "-" } else { "" };
            if frac == 0 {
                out.push_str(&format!("{sign}{secs}S"));
            } else {
                let frac = format!("{frac:06}");
                out.push_str(&format!("{sign}{secs}.{}S", frac.trim_end_matches('0')));
            }
        }
    }
    if out == "P" {
        out.push_str("T0S");
    }
    out
}

/// Convert a single row into a JSON object keyed by column name.
pub(crate) fn row_to_json(row: &sqlx::postgres::PgRow) -> Result<Value, FaucetError> {
    let mut map = serde_json::Map::new();
    for col in row.columns() {
        let name = col.name().to_string();
        let value = pg_value_to_json(row, &name)?;
        map.insert(name, value);
    }
    Ok(Value::Object(map))
}

/// How a numeric bind value should be bound onto a `sqlx` query. Classifying
/// before binding keeps any integer in `[i64::MIN, i64::MAX]` exact (binding
/// large integers as `f64` silently rounds them).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NumberBind {
    /// Exact `i64`.
    I64,
    /// Above `i64::MAX`; bind the `u64` reinterpreted as `i64` (two's complement).
    U64,
    /// Genuine floating-point value.
    F64,
}

/// Classify a JSON number into the bind category to use.
pub(crate) fn classify_number(n: &serde_json::Number) -> NumberBind {
    if n.is_i64() {
        NumberBind::I64
    } else if n.is_u64() {
        NumberBind::U64
    } else {
        NumberBind::F64
    }
}

/// Bind a slice of JSON values onto a `sqlx` query as native scalar types, in
/// positional order (`$1, $2, …`). Binding a raw `serde_json::Value` would
/// encode as `jsonb` and break comparisons against typed columns, so scalars
/// are bound as their native types.
pub(crate) fn bind_params<'q>(
    mut query: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    binds: &'q [Value],
) -> Result<sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>, FaucetError> {
    for (i, value) in binds.iter().enumerate() {
        query = match value {
            Value::String(s) => query.bind(s.clone()),
            Value::Number(n) => match classify_number(n) {
                NumberBind::I64 => query.bind(n.as_i64().unwrap()),
                // Above `i64::MAX`. Redshift's BIGINT is signed, and `as i64`
                // would *wrap* — writing a large id as a large negative number, or
                // (when this binds an incremental bookmark) comparing against a
                // negative bound and re-reading or skipping rows. Refuse (#462).
                NumberBind::U64 => query.bind(faucet_core::util::u64_to_signed(
                    n.as_u64().unwrap(),
                    &format!("bind parameter ${}", i + 1),
                )?),
                NumberBind::F64 => query.bind(n.as_f64().unwrap_or(0.0)),
            },
            Value::Bool(b) => query.bind(*b),
            Value::Null => query.bind(None::<String>),
            _ => query.bind(value.to_string()),
        };
    }
    Ok(query)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn num(v: serde_json::Value) -> serde_json::Number {
        match v {
            serde_json::Value::Number(n) => n,
            _ => panic!("not a number"),
        }
    }

    #[test]
    fn classify_small_int_is_i64() {
        assert_eq!(classify_number(&num(json!(42))), NumberBind::I64);
        assert_eq!(classify_number(&num(json!(-7))), NumberBind::I64);
    }

    #[test]
    fn classify_above_2_pow_53_stays_i64() {
        let v = 9_007_199_254_740_993i64; // 2^53 + 1
        assert_eq!(classify_number(&num(json!(v))), NumberBind::I64);
    }

    #[test]
    fn classify_above_i64_max_is_u64() {
        let v: u64 = i64::MAX as u64 + 1;
        assert_eq!(classify_number(&num(json!(v))), NumberBind::U64);
    }

    #[test]
    fn interval_renders_as_iso_8601_duration() {
        assert_eq!(interval_to_iso(0, 0, 0), "PT0S");
        assert_eq!(interval_to_iso(14, 3, 3_723_500_000), "P1Y2M3DT1H2M3.5S");
        assert_eq!(interval_to_iso(1, 0, 0), "P1M");
        assert_eq!(interval_to_iso(0, -2, -1_000_000), "P-2DT-1S");
        assert_eq!(interval_to_iso(0, 0, -500_000), "PT-0.5S");
        assert_eq!(interval_to_iso(0, 0, 60_000_000), "PT1M");
    }

    #[test]
    fn non_finite_floats_keep_their_text() {
        assert_eq!(float_to_json(f64::NAN), json!("NaN"));
        assert_eq!(float_to_json(1.5), json!(1.5));
    }

    #[test]
    fn classify_float_is_f64() {
        assert_eq!(classify_number(&num(json!(3.5))), NumberBind::F64);
    }
}

#[cfg(test)]
mod bind_overflow_tests {
    use super::*;
    use serde_json::json;

    /// #462: Redshift's BIGINT is signed; `as i64` would wrap a large unsigned
    /// id to a negative, and this same binder carries incremental bookmarks.
    #[test]
    fn u64_above_i64_max_is_refused_not_wrapped() {
        let err = match bind_params(sqlx::query("SELECT 1"), &[json!(u64::MAX)]) {
            Err(e) => e.to_string(),
            Ok(_) => panic!("u64::MAX must not bind"),
        };
        assert!(err.contains(&u64::MAX.to_string()), "{err}");
        assert!(
            !err.contains("-9223372036854775808"),
            "must not show the wrap: {err}"
        );
    }

    #[test]
    fn values_a_signed_column_can_hold_still_bind() {
        for v in [json!(0), json!(-1), json!(i64::MAX), json!(i64::MAX as u64)] {
            assert!(
                bind_params(sqlx::query("SELECT 1"), std::slice::from_ref(&v)).is_ok(),
                "{v} must still bind"
            );
        }
    }
}
