//! Row decoding: driver values → [`Cell`] → JSON.
//!
//! [`extract_kind`] (pure, over the driver's [`OracleType`] enum) decides how a
//! column is read; [`row_to_json`] is the thin shim that performs the reads.

use chrono::{DateTime, FixedOffset, NaiveDateTime};
use faucet_core::FaucetError;
use oracle::sql_type::{IntervalDS, IntervalYM, OracleType};
use serde_json::{Map, Value};

use crate::pool::Side;
use crate::types::{Cell, cell_to_json};

/// How a column's value is read from the driver.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtractKind {
    /// Exact decimal text.
    Number,
    /// `f64`.
    Float,
    /// `String` (character data and LOB text).
    Text,
    /// `Vec<u8>`.
    Bytes,
    /// `DATE`.
    Date,
    /// `TIMESTAMP`.
    Timestamp,
    /// `TIMESTAMP WITH [LOCAL] TIME ZONE`.
    TimestampTz,
    /// `INTERVAL DAY TO SECOND`.
    IntervalDs,
    /// `INTERVAL YEAR TO MONTH`.
    IntervalYm,
    /// `BOOLEAN`.
    Bool,
}

/// Decide how to read a column of type `ty`, or explain why it cannot be read.
pub fn extract_kind(ty: &OracleType) -> Result<ExtractKind, String> {
    Ok(match ty {
        OracleType::Number(..) | OracleType::Float(_) | OracleType::Int64 | OracleType::UInt64 => {
            ExtractKind::Number
        }
        OracleType::BinaryFloat | OracleType::BinaryDouble => ExtractKind::Float,
        OracleType::Varchar2(_)
        | OracleType::NVarchar2(_)
        | OracleType::Char(_)
        | OracleType::NChar(_)
        | OracleType::Rowid
        | OracleType::Long
        | OracleType::CLOB
        | OracleType::NCLOB
        | OracleType::Xml => ExtractKind::Text,
        OracleType::Raw(_) | OracleType::LongRaw | OracleType::BLOB => ExtractKind::Bytes,
        OracleType::Date => ExtractKind::Date,
        OracleType::Timestamp(_) => ExtractKind::Timestamp,
        OracleType::TimestampTZ(_) | OracleType::TimestampLTZ(_) => ExtractKind::TimestampTz,
        OracleType::IntervalDS(..) => ExtractKind::IntervalDs,
        OracleType::IntervalYM(_) => ExtractKind::IntervalYm,
        OracleType::Boolean => ExtractKind::Bool,
        OracleType::Json => {
            return Err("native JSON columns cannot be fetched directly; select \
                        JSON_SERIALIZE(<col> RETURNING CLOB) AS <col> and list the column \
                        under `json_columns` to receive it as a JSON value"
                .into());
        }
        other => return Err(format!("unsupported Oracle column type {other}")),
    })
}

fn read_cell(v: &oracle::SqlValue, kind: ExtractKind) -> oracle::Result<Cell> {
    if v.is_null()? {
        return Ok(Cell::Null);
    }
    Ok(match kind {
        ExtractKind::Number => Cell::Number(v.get::<String>()?),
        ExtractKind::Float => Cell::Float(v.get::<f64>()?),
        ExtractKind::Text => Cell::Text(v.get::<String>()?),
        ExtractKind::Bytes => Cell::Bytes(v.get::<Vec<u8>>()?),
        ExtractKind::Date => Cell::Date(v.get::<NaiveDateTime>()?),
        ExtractKind::Timestamp => Cell::Timestamp(v.get::<NaiveDateTime>()?),
        ExtractKind::TimestampTz => Cell::TimestampTz(v.get::<DateTime<FixedOffset>>()?),
        ExtractKind::IntervalDs => {
            let i = v.get::<IntervalDS>()?;
            Cell::IntervalDs {
                days: i.days(),
                hours: i.hours(),
                minutes: i.minutes(),
                seconds: i.seconds(),
                nanos: i.nanoseconds(),
            }
        }
        ExtractKind::IntervalYm => {
            let i = v.get::<IntervalYM>()?;
            Cell::IntervalYm {
                years: i.years(),
                months: i.months(),
            }
        }
        ExtractKind::Bool => Cell::Bool(v.get::<bool>()?),
    })
}

/// Decode one row into a JSON object keyed by column name. Columns listed in
/// `json_columns` are parsed from their text into JSON values.
pub fn row_to_json(
    row: &oracle::Row,
    json_columns: &[String],
    side: Side,
) -> Result<Value, FaucetError> {
    let infos = row.column_info();
    let mut out = Map::with_capacity(infos.len());
    for (info, v) in infos.iter().zip(row.sql_values()) {
        let kind = extract_kind(info.oracle_type())
            .map_err(|m| side.err(format!("oracle column {:?}: {m}", info.name())))?;
        let cell = read_cell(v, kind)
            .map_err(|e| side.err(format!("oracle decode of column {:?}: {e}", info.name())))?;
        let value = parse_json_column(info.name(), cell_to_json(cell), json_columns);
        out.insert(info.name().to_string(), value);
    }
    Ok(Value::Object(out))
}

/// Parse a text value into JSON when its column is listed in `json_columns`
/// (case-insensitive); anything unparseable is kept as text.
pub fn parse_json_column(name: &str, value: Value, json_columns: &[String]) -> Value {
    match value {
        Value::String(s) if json_columns.iter().any(|c| c.eq_ignore_ascii_case(name)) => {
            serde_json::from_str(&s).unwrap_or(Value::String(s))
        }
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn extract_kinds() {
        use ExtractKind::*;
        let cases = [
            (OracleType::Number(10, 0), Number),
            (OracleType::Float(126), Number),
            (OracleType::Int64, Number),
            (OracleType::BinaryDouble, Float),
            (OracleType::Varchar2(10), Text),
            (OracleType::NChar(2), Text),
            (OracleType::CLOB, Text),
            (OracleType::Rowid, Text),
            (OracleType::Raw(16), Bytes),
            (OracleType::BLOB, Bytes),
            (OracleType::Date, Date),
            (OracleType::Timestamp(6), Timestamp),
            (OracleType::TimestampTZ(6), TimestampTz),
            (OracleType::TimestampLTZ(6), TimestampTz),
            (OracleType::IntervalDS(2, 6), IntervalDs),
            (OracleType::IntervalYM(2), IntervalYm),
            (OracleType::Boolean, Bool),
        ];
        for (ty, want) in cases {
            assert_eq!(extract_kind(&ty).unwrap(), want, "{ty}");
        }
        assert!(
            extract_kind(&OracleType::Json)
                .unwrap_err()
                .contains("JSON_SERIALIZE")
        );
        assert!(
            extract_kind(&OracleType::BFILE)
                .unwrap_err()
                .contains("unsupported")
        );
    }

    #[test]
    fn json_columns_parse_case_insensitively() {
        let cols = vec!["payload".to_string()];
        assert_eq!(
            parse_json_column("PAYLOAD", json!("{\"a\":1}"), &cols),
            json!({"a": 1})
        );
        assert_eq!(
            parse_json_column("PAYLOAD", json!("{bad"), &cols),
            json!("{bad")
        );
        assert_eq!(parse_json_column("OTHER", json!("{}"), &cols), json!("{}"));
        assert_eq!(parse_json_column("PAYLOAD", json!(3), &cols), json!(3));
    }
}
