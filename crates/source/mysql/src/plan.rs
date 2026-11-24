//! Per-statement decode plan.
//!
//! [`mysql_decoded`] finds a cell's decoder by trying each one in a fixed
//! order. Which decoders *can* accept a column depends only on the column's
//! type, so the plan asks that once per statement and decodes every later
//! cell with the one decoder the cascade would have reached. A cell the
//! planned decoder cannot read is handed to the cascade unchanged.

use crate::stream::{
    JsonContext, bytes_to_json, json_text_to_value, mysql_decoded, mysql_scalar_to_json, time_text,
};
use faucet_core::FaucetError;
use serde_json::Value;
use sqlx::mysql::{MySqlRow, MySqlTypeInfo, MySqlValueRef};
use sqlx::types::chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use sqlx::{Column, Decode, MySql, Row, Type, TypeInfo, ValueRef};
use std::collections::HashSet;

/// The decoder a column's cells are read with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DecodeKind {
    /// `JSON`, parsed exactly from the cell's text.
    Json,
    Text,
    I64,
    I32,
    I16,
    U64,
    U32,
    U16,
    U8,
    F32,
    F64,
    Bool,
    DateTimeUtc,
    DateTime,
    Date,
    Time,
    Decimal,
    /// Bytes; `text` when the column is text with a binary collation.
    Bytes {
        text: bool,
    },
    /// No single decoder is known: the cascade decides per cell.
    Probe,
}

fn fits<T: Type<MySql>>(ty: &MySqlTypeInfo) -> bool {
    ty.is_null() || T::compatible(ty)
}

impl DecodeKind {
    /// The first step of the cascade that accepts a column of type `ty`.
    pub(crate) fn of(ty: &MySqlTypeInfo, text: bool) -> Self {
        let name = ty.name();
        if name.eq_ignore_ascii_case("JSON") {
            return Self::Json;
        }
        if fits::<String>(ty) {
            return Self::Text;
        }
        if fits::<i64>(ty) {
            return Self::I64;
        }
        if fits::<i32>(ty) {
            return Self::I32;
        }
        if fits::<i16>(ty) {
            return Self::I16;
        }
        if fits::<u64>(ty) {
            return Self::U64;
        }
        if fits::<u32>(ty) {
            return Self::U32;
        }
        if fits::<u16>(ty) {
            return Self::U16;
        }
        if fits::<u8>(ty) {
            return Self::U8;
        }
        if name.eq_ignore_ascii_case("FLOAT") && fits::<f32>(ty) {
            return Self::F32;
        }
        if fits::<f64>(ty) {
            return Self::F64;
        }
        if fits::<bool>(ty) {
            return Self::Bool;
        }
        if fits::<DateTime<Utc>>(ty) {
            return Self::DateTimeUtc;
        }
        if fits::<NaiveDateTime>(ty) {
            return Self::DateTime;
        }
        if fits::<NaiveDate>(ty) {
            return Self::Date;
        }
        if fits::<sqlx::mysql::types::MySqlTime>(ty) {
            return Self::Time;
        }
        if fits::<sqlx::types::BigDecimal>(ty) {
            return Self::Decimal;
        }
        if fits::<Vec<u8>>(ty) {
            return Self::Bytes { text };
        }
        Self::Probe
    }
}

fn typed<'r, T: Decode<'r, MySql>>(raw: MySqlValueRef<'r>) -> Option<T> {
    T::decode(raw).ok()
}

#[derive(Debug)]
pub(crate) struct PlannedColumn {
    pub(crate) name: String,
    pub(crate) kind: DecodeKind,
    /// A `DATE` / `DATETIME` / `TIMESTAMP` column, whose zero dates sqlx
    /// reports as NULL.
    zero_dates: bool,
}

impl PlannedColumn {
    /// The cell as JSON.
    pub(crate) fn decode(
        &self,
        row: &MySqlRow,
        idx: usize,
        text_columns: &HashSet<String>,
        json: &JsonContext<'_>,
    ) -> Result<Value, FaucetError> {
        let Ok(raw) = row.try_get_raw(idx) else {
            return crate::stream::mysql_value_to_json(row, &self.name, text_columns, json);
        };
        if raw.is_null() {
            if self.zero_dates {
                return mysql_scalar_to_json(row, &self.name, text_columns);
            }
            return Ok(Value::Null);
        }
        let planned =
            match self.kind {
                DecodeKind::Probe => None,
                DecodeKind::Json => match typed::<String>(raw) {
                    Some(text) => return json_text_to_value(&text, &self.name, json),
                    None => None,
                },
                DecodeKind::Text => typed::<String>(raw).map(Value::String),
                DecodeKind::I64 => typed::<i64>(raw).map(|v| Value::Number(v.into())),
                DecodeKind::I32 => typed::<i32>(raw).map(|v| Value::Number(v.into())),
                DecodeKind::I16 => typed::<i16>(raw).map(|v| Value::Number(v.into())),
                DecodeKind::U64 => typed::<u64>(raw).map(|v| Value::Number(v.into())),
                DecodeKind::U32 => typed::<u32>(raw).map(|v| Value::Number(v.into())),
                DecodeKind::U16 => typed::<u16>(raw).map(|v| Value::Number(v.into())),
                DecodeKind::U8 => typed::<u8>(raw).map(|v| Value::Number(v.into())),
                DecodeKind::F32 => typed::<f32>(raw).map(faucet_core::fidelity::float32),
                DecodeKind::F64 => typed::<f64>(raw).map(faucet_core::fidelity::float64),
                DecodeKind::Bool => typed::<bool>(raw).map(Value::Bool),
                DecodeKind::DateTimeUtc => {
                    typed::<DateTime<Utc>>(raw).map(|v| Value::String(v.to_rfc3339()))
                }
                DecodeKind::DateTime => {
                    typed::<NaiveDateTime>(raw).map(|v| Value::String(v.to_string()))
                }
                DecodeKind::Date => typed::<NaiveDate>(raw).map(|v| Value::String(v.to_string())),
                DecodeKind::Time => typed::<sqlx::mysql::types::MySqlTime>(raw)
                    .map(|v| Value::String(time_text(&v))),
                DecodeKind::Decimal => typed::<sqlx::types::BigDecimal>(raw)
                    .map(|v| Value::String(v.to_plain_string())),
                DecodeKind::Bytes { text } => typed::<Vec<u8>>(raw).map(|v| bytes_to_json(v, text)),
            };
        match planned {
            Some(v) => Ok(v),
            None => mysql_decoded(row, &self.name, text_columns),
        }
    }
}

/// How every column of one statement's rows is decoded.
#[derive(Debug)]
pub(crate) struct DecodePlan {
    shape: (usize, usize),
    pub(crate) columns: Vec<PlannedColumn>,
}

fn shape_of(row: &MySqlRow) -> (usize, usize) {
    let columns = row.columns();
    (columns.as_ptr() as usize, columns.len())
}

fn zero_dates(name: &str) -> bool {
    ["DATE", "DATETIME", "TIMESTAMP"]
        .iter()
        .any(|t| name.eq_ignore_ascii_case(t))
}

impl DecodePlan {
    /// Plan the columns `row` carries (their names are unique: the caller
    /// checked).
    pub(crate) fn for_row(row: &MySqlRow, text_columns: &HashSet<String>) -> Self {
        Self {
            shape: shape_of(row),
            columns: row
                .columns()
                .iter()
                .map(|col| PlannedColumn {
                    name: col.name().to_string(),
                    kind: DecodeKind::of(col.type_info(), text_columns.contains(col.name())),
                    zero_dates: zero_dates(col.type_info().name()),
                })
                .collect(),
        }
    }

    /// `row` as a JSON object keyed by column name.
    pub(crate) fn row_to_json(
        &self,
        row: &MySqlRow,
        text_columns: &HashSet<String>,
        json: &JsonContext<'_>,
    ) -> Result<Value, FaucetError> {
        let mut map = serde_json::Map::with_capacity(self.columns.len());
        for (idx, col) in self.columns.iter().enumerate() {
            map.insert(col.name.clone(), col.decode(row, idx, text_columns, json)?);
        }
        Ok(Value::Object(map))
    }
}

/// Decode `row` with `plan`, planning on the first row and again if a row's
/// columns are not the planned ones.
pub(crate) fn decode_row(
    plan: &mut Option<DecodePlan>,
    row: &MySqlRow,
    text_columns: &HashSet<String>,
    json: &JsonContext<'_>,
) -> Result<Value, FaucetError> {
    let planned = match plan {
        Some(p) if p.shape == shape_of(row) => p,
        _ => plan.insert(DecodePlan::for_row(row, text_columns)),
    };
    planned.row_to_json(row, text_columns, json)
}

#[cfg(test)]
mod tests;
