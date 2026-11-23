//! The one decoder for SQL Server temporal columns, shared by the query and
//! CDC sources so a snapshot and its change stream spell a value alike.

use chrono::{FixedOffset, NaiveDate, NaiveDateTime, NaiveTime};
use faucet_core::FaucetError;
use serde_json::Value;
use tiberius::{ColumnData, Row};

/// Decode the temporal cell `data` at `idx` of `row`: `DATE` as
/// `YYYY-MM-DD`, `TIME` as chrono's text, `DATETIME` / `SMALLDATETIME` with
/// milliseconds ([`format_legacy_datetime`]), `DATETIME2` with its fraction,
/// `DATETIMEOFFSET` as RFC 3339. SQL `NULL` is `null`. `label` starts the
/// error text (`"MSSQL"`, `"mssql-cdc"`). A non-temporal `data` is an error.
pub fn decode_temporal(
    row: &Row,
    idx: usize,
    data: &ColumnData<'_>,
    label: &str,
) -> Result<Value, FaucetError> {
    let conv = |e: tiberius::error::Error| {
        FaucetError::Source(format!("{label} column {idx} temporal decode failed: {e}"))
    };
    let value = match data {
        ColumnData::Date(_) => row
            .try_get::<NaiveDate, _>(idx)
            .map_err(conv)?
            .map(|d| Value::String(d.to_string())),
        ColumnData::Time(_) => row
            .try_get::<NaiveTime, _>(idx)
            .map_err(conv)?
            .map(|t| Value::String(t.to_string())),
        ColumnData::DateTime(_) | ColumnData::SmallDateTime(_) => row
            .try_get::<NaiveDateTime, _>(idx)
            .map_err(conv)?
            .map(|dt| Value::String(format_legacy_datetime(dt))),
        ColumnData::DateTime2(_) => row
            .try_get::<NaiveDateTime, _>(idx)
            .map_err(conv)?
            .map(|dt| Value::String(dt.format("%Y-%m-%dT%H:%M:%S%.f").to_string())),
        ColumnData::DateTimeOffset(_) => row
            .try_get::<chrono::DateTime<FixedOffset>, _>(idx)
            .map_err(conv)?
            .map(|dt| Value::String(dt.to_rfc3339())),
        _ => {
            return Err(FaucetError::Source(format!(
                "{label} column {idx} is not a temporal column"
            )));
        }
    };
    Ok(value.unwrap_or(Value::Null))
}

/// Render a `DATETIME` / `SMALLDATETIME` value with millisecond precision, as
/// SQL Server itself displays it.
///
/// `DATETIME` stores 1/300-second ticks, which decode to nine-digit fractions
/// such as `.003333333`; SQL Server refuses to convert that string back to
/// `DATETIME`. The fraction is rounded to the nearest millisecond — never
/// past `.997`, the last tick of a second.
pub fn format_legacy_datetime(dt: NaiveDateTime) -> String {
    let ms = ((dt.and_utc().timestamp_subsec_nanos() + 500_000) / 1_000_000).min(999);
    format!("{}.{ms:03}", dt.format("%Y-%m-%dT%H:%M:%S"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(ns: u32) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(2026, 3, 4)
            .unwrap()
            .and_hms_nano_opt(10, 0, 0, ns)
            .unwrap()
    }

    #[test]
    fn legacy_datetimes_render_whole_milliseconds() {
        assert_eq!(
            format_legacy_datetime(at(3_333_333)),
            "2026-03-04T10:00:00.003"
        );
        assert_eq!(
            format_legacy_datetime(at(6_666_667)),
            "2026-03-04T10:00:00.007"
        );
        assert_eq!(format_legacy_datetime(at(0)), "2026-03-04T10:00:00.000");
        assert_eq!(
            format_legacy_datetime(at(996_666_667)),
            "2026-03-04T10:00:00.997"
        );
        assert_eq!(
            format_legacy_datetime(at(999_900_000)),
            "2026-03-04T10:00:00.999"
        );
    }
}
