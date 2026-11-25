//! Per-statement decode plan.
//!
//! [`pg_value_to_json`] finds a cell's decoder by trying each one in a fixed
//! order. Which decoders *can* accept a column depends only on the column's
//! type, so the plan asks that once per statement and decodes every later
//! cell with the one decoder the cascade would have reached. A cell the
//! planned decoder cannot read is handed to the cascade unchanged.

use crate::stream::{
    JsonContext, duplicate_column, infinity_sentinel, json_cell_text, json_text_to_value,
    pg_value_to_json, time_text,
};
use faucet_core::{FaucetError, TimePrecision};
use serde_json::Value;
use sqlx::postgres::{PgRow, PgTypeInfo, PgValueFormat, PgValueRef};
use sqlx::types::chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use sqlx::{Column, Decode, Postgres, Row, Type, TypeInfo, ValueRef};

/// The decoder a column's cells are read with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DecodeKind {
    /// `json` / `jsonb`, parsed exactly from the cell's text.
    Json {
        jsonb: bool,
    },
    /// Text; `decimal` / `time` columns arrive as text and are normalised.
    Text {
        decimal: bool,
        time: bool,
    },
    Int8,
    Int4,
    Int2,
    Float8,
    Float4,
    Bool,
    /// `sentinel` is `Some(date)` when `infinity` / `-infinity` is read first.
    TimestampTz {
        sentinel: Option<bool>,
    },
    Timestamp {
        sentinel: Option<bool>,
    },
    Date {
        sentinel: Option<bool>,
    },
    Time,
    Uuid,
    /// Binary `numeric`, rendered from its digits.
    Numeric,
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
    pub(crate) fn of(ty: &PgTypeInfo, column: &str, json: &JsonContext<'_>) -> Self {
        let name = ty.name();
        if name.eq_ignore_ascii_case("JSON") {
            return Self::Json { jsonb: false };
        }
        if name.eq_ignore_ascii_case("JSONB") {
            return Self::Json { jsonb: true };
        }
        if fits::<String>(ty) {
            return Self::Text {
                decimal: json.decimal.contains(column),
                time: json.time.contains(column),
            };
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
        let sentinel = if name.eq_ignore_ascii_case("DATE") {
            Some(true)
        } else if name.eq_ignore_ascii_case("TIMESTAMP") || name.eq_ignore_ascii_case("TIMESTAMPTZ")
        {
            Some(false)
        } else {
            None
        };
        if fits::<DateTime<Utc>>(ty) {
            return Self::TimestampTz { sentinel };
        }
        if fits::<NaiveDateTime>(ty) {
            return Self::Timestamp { sentinel };
        }
        if fits::<NaiveDate>(ty) {
            return Self::Date { sentinel };
        }
        if sentinel.is_some() {
            return Self::Probe;
        }
        if fits::<NaiveTime>(ty) {
            return Self::Time;
        }
        if fits::<sqlx::types::Uuid>(ty) {
            return Self::Uuid;
        }
        if name.eq_ignore_ascii_case("NUMERIC") {
            return Self::Numeric;
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
struct PlannedColumn {
    name: String,
    kind: DecodeKind,
}

impl PlannedColumn {
    /// The cell through the planned decoder; `None` when the cascade must
    /// read it.
    fn planned(
        &self,
        row: &PgRow,
        idx: usize,
        json: &JsonContext<'_>,
    ) -> Result<Option<Value>, FaucetError> {
        let Ok(raw) = row.try_get_raw(idx) else {
            return Ok(None);
        };
        if raw.is_null() {
            return Ok(Some(Value::Null));
        }
        let temporal = |sentinel: Option<bool>, raw: &PgValueRef<'_>| {
            sentinel.and_then(|date| infinity_sentinel(raw, date))
        };
        Ok(match self.kind {
            DecodeKind::Probe => None,
            DecodeKind::Json { jsonb } => match json_cell_text(&raw, jsonb) {
                Some(text) => Some(json_text_to_value(
                    text,
                    &self.name,
                    json.mode,
                    json.warned,
                )?),
                None => None,
            },
            DecodeKind::Text { decimal, time } => match typed::<String>(raw) {
                Some(v) if decimal => {
                    Some(faucet_core::fidelity::decimal_text(&v).map_err(|e| {
                        FaucetError::Source(format!("PostgreSQL column {}: {e}", self.name))
                    })?)
                }
                Some(v) if time => Some(time_text(v)),
                Some(v) => Some(Value::String(v)),
                None => None,
            },
            DecodeKind::Int8 => typed::<i64>(raw).map(|v| Value::Number(v.into())),
            DecodeKind::Int4 => typed::<i32>(raw).map(|v| Value::Number(v.into())),
            DecodeKind::Int2 => typed::<i16>(raw).map(|v| Value::Number(v.into())),
            DecodeKind::Float8 => typed::<f64>(raw).map(faucet_core::fidelity::float64),
            DecodeKind::Float4 => typed::<f32>(raw).map(faucet_core::fidelity::float32),
            DecodeKind::Bool => typed::<bool>(raw).map(Value::Bool),
            DecodeKind::TimestampTz { sentinel } => temporal(sentinel, &raw).or_else(|| {
                typed::<DateTime<Utc>>(raw)
                    .map(|v| faucet_core::fidelity::timestamp(&v, TimePrecision::Micros))
            }),
            DecodeKind::Timestamp { sentinel } => temporal(sentinel, &raw).or_else(|| {
                typed::<NaiveDateTime>(raw)
                    .map(|v| faucet_core::fidelity::timestamp_naive(&v, TimePrecision::Micros))
            }),
            DecodeKind::Date { sentinel } => temporal(sentinel, &raw)
                .or_else(|| typed::<NaiveDate>(raw).map(|v| Value::String(v.to_string()))),
            DecodeKind::Time => typed::<NaiveTime>(raw)
                .map(|v| faucet_core::fidelity::time(&v, TimePrecision::Micros)),
            DecodeKind::Uuid => {
                typed::<sqlx::types::Uuid>(raw).map(|v| Value::String(v.to_string()))
            }
            DecodeKind::Numeric => (raw.format() == PgValueFormat::Binary)
                .then(|| raw.as_bytes().ok())
                .flatten()
                .and_then(faucet_core::fidelity::decimal_from_pg_numeric),
            DecodeKind::Decimal => {
                typed::<sqlx::types::BigDecimal>(raw).map(|v| Value::String(v.to_plain_string()))
            }
            DecodeKind::Bytes => typed::<Vec<u8>>(raw).map(|v| {
                use base64::Engine as _;
                Value::String(base64::engine::general_purpose::STANDARD.encode(v))
            }),
        })
    }
}

/// How every column of one statement's rows is decoded.
#[derive(Debug)]
pub(crate) struct DecodePlan {
    shape: (usize, usize),
    columns: Vec<PlannedColumn>,
}

fn shape_of(row: &PgRow) -> (usize, usize) {
    let columns = row.columns();
    (columns.as_ptr() as usize, columns.len())
}

impl DecodePlan {
    /// Plan the columns `row` carries. A result naming one column twice is
    /// refused.
    pub(crate) fn for_row(row: &PgRow, json: &JsonContext<'_>) -> Result<Self, FaucetError> {
        let columns = row.columns();
        let mut seen = std::collections::HashSet::with_capacity(columns.len());
        for col in columns {
            if !seen.insert(col.name()) {
                return Err(duplicate_column(col.name()));
            }
        }
        Ok(Self {
            shape: shape_of(row),
            columns: columns
                .iter()
                .map(|col| PlannedColumn {
                    name: col.name().to_string(),
                    kind: DecodeKind::of(col.type_info(), col.name(), json),
                })
                .collect(),
        })
    }

    /// The kind planned for each column, in order.
    #[cfg(test)]
    pub(crate) fn kinds(&self) -> Vec<DecodeKind> {
        self.columns.iter().map(|c| c.kind).collect()
    }

    /// `row` as a JSON object keyed by column name.
    pub(crate) fn row_to_json(
        &self,
        row: &PgRow,
        json: &JsonContext<'_>,
    ) -> Result<Value, FaucetError> {
        let mut map = serde_json::Map::with_capacity(self.columns.len());
        for (idx, col) in self.columns.iter().enumerate() {
            let value = match col.planned(row, idx, json)? {
                Some(v) => v,
                None => pg_value_to_json(row, idx, &col.name, json)?,
            };
            map.insert(col.name.clone(), value);
        }
        Ok(Value::Object(map))
    }
}

/// Decode `row` with `plan`, planning on the first row and again if a row's
/// columns are not the planned ones.
pub(crate) fn decode_row(
    plan: &mut Option<DecodePlan>,
    row: &PgRow,
    json: &JsonContext<'_>,
) -> Result<Value, FaucetError> {
    let planned = match plan {
        Some(p) if p.shape == shape_of(row) => p,
        _ => plan.insert(DecodePlan::for_row(row, json)?),
    };
    planned.row_to_json(row, json)
}

#[cfg(test)]
mod tests;
