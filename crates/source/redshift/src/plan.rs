//! Per-statement decode plan.
//!
//! [`pg_value_to_json`] finds a cell's decoder by trying each one in a fixed
//! order. Which decoders *can* accept a column depends only on the column's
//! type, so the plan asks that once per statement and decodes every later
//! cell with the one decoder the cascade would have reached. A cell the
//! planned decoder cannot read is handed to the cascade unchanged.

use crate::convert::{interval_to_iso, pg_value_to_json};
use faucet_core::{FaucetError, TimePrecision};
use serde_json::Value;
use sqlx::postgres::types::{PgInterval, PgTimeTz};
use sqlx::postgres::{PgRow, PgTypeInfo, PgValueFormat, PgValueRef};
use sqlx::types::chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use sqlx::{Column, Decode, Postgres, Row, Type, TypeInfo, ValueRef};

/// The decoder a column's cells are read with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DecodeKind {
    /// Binary `numeric`, rendered from its digits.
    Numeric,
    /// `JSON` / `JSONB`, parsed exactly from the cell's text.
    Json {
        jsonb: bool,
    },
    JsonValue,
    Text,
    Int8,
    Int4,
    Int2,
    Float8,
    Float4,
    Bool,
    TimestampTz,
    Timestamp,
    Date,
    Time,
    TimeTz,
    Interval,
    Uuid,
    Decimal,
    Bytes,
    /// No single decoder is known: the cascade decides per cell.
    Probe,
}

fn fits<T: Type<Postgres>>(ty: &PgTypeInfo) -> bool {
    ty.is_null() || T::compatible(ty)
}

impl DecodeKind {
    /// The first step of the cascade that accepts a column of type `ty`.
    pub(crate) fn of(ty: &PgTypeInfo) -> Self {
        let name = ty.name();
        if name.eq_ignore_ascii_case("NUMERIC") {
            return Self::Numeric;
        }
        if name == "JSON" {
            return Self::Json { jsonb: false };
        }
        if name == "JSONB" {
            return Self::Json { jsonb: true };
        }
        if fits::<Value>(ty) {
            return Self::JsonValue;
        }
        if fits::<String>(ty) {
            return Self::Text;
        }
        if fits::<i64>(ty) {
            return Self::Int8;
        }
        if fits::<i32>(ty) {
            return Self::Int4;
        }
        if fits::<i16>(ty) {
            return Self::Int2;
        }
        if fits::<f64>(ty) {
            return Self::Float8;
        }
        if fits::<f32>(ty) {
            return Self::Float4;
        }
        if fits::<bool>(ty) {
            return Self::Bool;
        }
        if fits::<DateTime<Utc>>(ty) {
            return Self::TimestampTz;
        }
        if fits::<NaiveDateTime>(ty) {
            return Self::Timestamp;
        }
        if fits::<NaiveDate>(ty) {
            return Self::Date;
        }
        if fits::<NaiveTime>(ty) {
            return Self::Time;
        }
        if fits::<PgTimeTz<NaiveTime, FixedOffset>>(ty) {
            return Self::TimeTz;
        }
        if fits::<PgInterval>(ty) {
            return Self::Interval;
        }
        if fits::<sqlx::types::Uuid>(ty) {
            return Self::Uuid;
        }
        if fits::<sqlx::types::BigDecimal>(ty) {
            return Self::Decimal;
        }
        if fits::<Vec<u8>>(ty) {
            return Self::Bytes;
        }
        Self::Probe
    }
}

fn typed<'r, T: Decode<'r, Postgres>>(raw: PgValueRef<'r>) -> Option<T> {
    T::decode(raw).ok()
}

#[derive(Debug)]
pub(crate) struct PlannedColumn {
    pub(crate) name: String,
    pub(crate) kind: DecodeKind,
}

impl PlannedColumn {
    /// The cell as JSON.
    pub(crate) fn decode(&self, row: &PgRow, idx: usize) -> Result<Value, FaucetError> {
        if self.kind == DecodeKind::Probe {
            return pg_value_to_json(row, &self.name);
        }
        let Ok(raw) = row.try_get_raw(idx) else {
            return pg_value_to_json(row, &self.name);
        };
        if raw.is_null() {
            return Ok(Value::Null);
        }
        let planned = match self.kind {
            DecodeKind::Probe => None,
            DecodeKind::Numeric => (raw.format() == PgValueFormat::Binary)
                .then(|| raw.as_bytes().ok())
                .flatten()
                .and_then(faucet_core::fidelity::decimal_from_pg_numeric),
            DecodeKind::Json { jsonb } => raw.as_bytes().ok().and_then(|bytes| {
                let text = if jsonb && raw.format() == PgValueFormat::Binary {
                    bytes.get(1..).unwrap_or_default()
                } else {
                    bytes
                };
                faucet_core::fidelity::parse_json(text).ok()
            }),
            DecodeKind::JsonValue => typed::<Value>(raw),
            DecodeKind::Text => typed::<String>(raw).map(Value::String),
            DecodeKind::Int8 => typed::<i64>(raw).map(|v| Value::Number(v.into())),
            DecodeKind::Int4 => typed::<i32>(raw).map(|v| Value::Number(v.into())),
            DecodeKind::Int2 => typed::<i16>(raw).map(|v| Value::Number(v.into())),
            DecodeKind::Float8 => typed::<f64>(raw).map(faucet_core::fidelity::float64),
            DecodeKind::Float4 => typed::<f32>(raw).map(faucet_core::fidelity::float32),
            DecodeKind::Bool => typed::<bool>(raw).map(Value::Bool),
            DecodeKind::TimestampTz => typed::<DateTime<Utc>>(raw)
                .map(|v| faucet_core::fidelity::timestamp(&v, TimePrecision::Micros)),
            DecodeKind::Timestamp => typed::<NaiveDateTime>(raw)
                .map(|v| faucet_core::fidelity::timestamp_naive(&v, TimePrecision::Micros)),
            DecodeKind::Date => typed::<NaiveDate>(raw).map(|v| Value::String(v.to_string())),
            DecodeKind::Time => typed::<NaiveTime>(raw)
                .map(|v| faucet_core::fidelity::time(&v, TimePrecision::Micros)),
            DecodeKind::TimeTz => typed::<PgTimeTz<NaiveTime, FixedOffset>>(raw)
                .map(|v| Value::String(format!("{}{}", v.time, v.offset))),
            DecodeKind::Interval => typed::<PgInterval>(raw)
                .map(|v| Value::String(interval_to_iso(v.months, v.days, v.microseconds))),
            DecodeKind::Uuid => {
                typed::<sqlx::types::Uuid>(raw).map(|v| Value::String(v.to_string()))
            }
            DecodeKind::Decimal => {
                typed::<sqlx::types::BigDecimal>(raw).map(|v| Value::String(v.to_plain_string()))
            }
            DecodeKind::Bytes => typed::<Vec<u8>>(raw).map(|v| {
                use base64::Engine as _;
                Value::String(base64::engine::general_purpose::STANDARD.encode(v))
            }),
        };
        match planned {
            Some(v) => Ok(v),
            None => pg_value_to_json(row, &self.name),
        }
    }
}

/// How every column of one statement's rows is decoded.
#[derive(Debug)]
pub(crate) struct DecodePlan {
    shape: (usize, usize),
    pub(crate) columns: Vec<PlannedColumn>,
}

fn shape_of(row: &PgRow) -> (usize, usize) {
    let columns = row.columns();
    (columns.as_ptr() as usize, columns.len())
}

impl DecodePlan {
    /// Plan the columns `row` carries (their names are unique: the caller
    /// checked).
    pub(crate) fn for_row(row: &PgRow) -> Self {
        Self {
            shape: shape_of(row),
            columns: row
                .columns()
                .iter()
                .map(|col| PlannedColumn {
                    name: col.name().to_string(),
                    kind: DecodeKind::of(col.type_info()),
                })
                .collect(),
        }
    }

    /// `row` as a JSON object keyed by column name.
    pub(crate) fn row_to_json(&self, row: &PgRow) -> Result<Value, FaucetError> {
        let mut map = serde_json::Map::with_capacity(self.columns.len());
        for (idx, col) in self.columns.iter().enumerate() {
            map.insert(col.name.clone(), col.decode(row, idx)?);
        }
        Ok(Value::Object(map))
    }
}

/// Decode `row` with `plan`, planning on the first row and again if a row's
/// columns are not the planned ones.
pub(crate) fn decode_row(plan: &mut Option<DecodePlan>, row: &PgRow) -> Result<Value, FaucetError> {
    let planned = match plan {
        Some(p) if p.shape == shape_of(row) => p,
        _ => plan.insert(DecodePlan::for_row(row)),
    };
    planned.row_to_json(row)
}

#[cfg(test)]
mod tests;
