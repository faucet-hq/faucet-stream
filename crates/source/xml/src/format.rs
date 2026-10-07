//! Tabular response-body parsing for `response_format: csv | excel` (#497).
//!
//! Turns a downloaded file body into a `Vec<Value>` of JSON objects, so an
//! authenticated file endpoint (a cloud-drive `…/content` download, a
//! signed export URL, …) can be consumed through the same REST source that
//! already owns auth, retry, and context substitution.

use faucet_core::FaucetError;
use serde_json::{Map, Value};

/// Parse CSV bytes into records. When `has_headers`, the first row supplies
/// field names; otherwise fields are named `column_0`, `column_1`, … Values are
/// strings (matching the `csv` source). Streaming RFC-4180 via `csv-async`, so
/// quoted fields with embedded newlines are handled correctly.
pub async fn parse_csv(
    bytes: &[u8],
    delimiter: u8,
    has_headers: bool,
) -> Result<Vec<Value>, FaucetError> {
    use futures::StreamExt as _;
    let mut rdr = csv_async::AsyncReaderBuilder::new()
        .has_headers(false)
        .delimiter(delimiter)
        .flexible(true)
        .create_reader(bytes);
    let mut records = rdr.records();
    let mut headers: Option<Vec<String>> = None;
    let mut out = Vec::new();
    while let Some(rec) = records.next().await {
        let rec = rec.map_err(|e| FaucetError::Source(format!("rest: CSV parse error: {e}")))?;
        if has_headers && headers.is_none() {
            headers = Some(rec.iter().map(str::to_string).collect());
            continue;
        }
        let mut obj = Map::new();
        for (i, field) in rec.iter().enumerate() {
            let key = headers
                .as_ref()
                .and_then(|h| h.get(i).cloned())
                .unwrap_or_else(|| format!("column_{i}"));
            obj.insert(key, Value::String(field.to_string()));
        }
        out.push(Value::Object(obj));
    }
    Ok(out)
}

/// Parse Excel bytes into records. Requires the `excel` feature.
#[cfg(feature = "excel")]
pub fn parse_excel(
    bytes: &[u8],
    sheet: Option<&str>,
    header_row: usize,
) -> Result<Vec<Value>, FaucetError> {
    use calamine::{Data, Reader, Xlsx};
    let cursor = std::io::Cursor::new(bytes.to_vec());
    let mut wb: Xlsx<_> = calamine::open_workbook_from_rs(cursor)
        .map_err(|e| FaucetError::Source(format!("rest: opening Excel workbook: {e}")))?;
    let names = wb.sheet_names().to_vec();
    let name = match sheet {
        Some(s) if names.iter().any(|n| n == s) => s.to_string(),
        Some(s) => match s.parse::<usize>() {
            Ok(idx) => names.get(idx).cloned().ok_or_else(|| {
                FaucetError::Source(format!("rest: Excel sheet index {idx} out of range"))
            })?,
            Err(_) => {
                return Err(FaucetError::Source(format!(
                    "rest: Excel sheet '{s}' not found (available: {})",
                    names.join(", ")
                )));
            }
        },
        None => names
            .first()
            .cloned()
            .ok_or_else(|| FaucetError::Source("rest: Excel workbook has no worksheets".into()))?,
    };
    let range = wb
        .worksheet_range(&name)
        .map_err(|e| FaucetError::Source(format!("rest: reading Excel sheet '{name}': {e}")))?;
    let rows: Vec<&[Data]> = range.rows().collect();
    let header = rows.get(header_row).ok_or_else(|| {
        FaucetError::Source(format!(
            "rest: Excel header_row {header_row} is beyond the sheet ({} rows)",
            rows.len()
        ))
    })?;
    let headers: Vec<String> = header.iter().map(cell_to_string).collect();
    let mut out = Vec::new();
    for row in rows.iter().skip(header_row + 1) {
        let mut obj = Map::new();
        for (i, cell) in row.iter().enumerate() {
            let key = headers
                .get(i)
                .cloned()
                .filter(|k| !k.is_empty())
                .unwrap_or_else(|| format!("column_{i}"));
            obj.insert(key, cell_to_value(cell));
        }
        out.push(Value::Object(obj));
    }
    Ok(out)
}

/// Stub when the `excel` feature is off — error loudly rather than mis-parse an
/// Excel blob as CSV.
#[cfg(not(feature = "excel"))]
pub fn parse_excel(
    _bytes: &[u8],
    _sheet: Option<&str>,
    _header_row: usize,
) -> Result<Vec<Value>, FaucetError> {
    Err(FaucetError::Config(
        "rest: `response_format: excel` requires the crate's `excel` feature — rebuild the CLI \
         with `--features source-rest-excel`"
            .into(),
    ))
}

#[cfg(feature = "excel")]
/// An Excel date/time cell as text: `YYYY-MM-DD` for a whole day, an ISO
/// timestamp otherwise, an ISO 8601 duration for a time span. The workbook's
/// 1904 date system is honoured. A serial outside chrono's range keeps the
/// raw number.
fn excel_datetime_text(dt: &calamine::ExcelDateTime) -> String {
    if dt.is_duration() {
        return dt
            .as_duration()
            .map_or_else(|| dt.as_f64().to_string(), |d| d.to_string());
    }
    match dt.as_datetime() {
        Some(t) if t.time() == chrono::NaiveTime::MIN => t.date().to_string(),
        Some(t) => t.format("%Y-%m-%dT%H:%M:%S%.f").to_string(),
        None => dt.as_f64().to_string(),
    }
}

#[cfg(feature = "excel")]
fn cell_to_string(cell: &calamine::Data) -> String {
    use calamine::Data;
    match cell {
        Data::String(s) => s.clone(),
        Data::Empty => String::new(),
        Data::DateTime(dt) => excel_datetime_text(dt),
        other => other.to_string(),
    }
}

#[cfg(feature = "excel")]
fn cell_to_value(cell: &calamine::Data) -> Value {
    use calamine::Data;
    match cell {
        Data::Empty => Value::Null,
        Data::String(s) => Value::String(s.clone()),
        Data::Bool(b) => Value::Bool(*b),
        Data::Int(i) => Value::from(*i),
        Data::Float(f) => serde_json::Number::from_f64(*f)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        Data::DateTime(dt) => Value::String(excel_datetime_text(dt)),
        Data::DateTimeIso(s) | Data::DurationIso(s) => Value::String(s.clone()),
        Data::Error(e) => Value::String(format!("{e:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn csv_with_headers() {
        let recs = parse_csv(b"id,name\n1,Alice\n2,Bob\n", b',', true)
            .await
            .unwrap();
        assert_eq!(recs.len(), 2);
        assert_eq!(recs[0]["id"], "1");
        assert_eq!(recs[0]["name"], "Alice");
        assert_eq!(recs[1]["name"], "Bob");
    }

    #[tokio::test]
    async fn csv_without_headers_generates_names() {
        let recs = parse_csv(b"1,Alice\n", b',', false).await.unwrap();
        assert_eq!(recs[0]["column_0"], "1");
        assert_eq!(recs[0]["column_1"], "Alice");
    }

    #[tokio::test]
    async fn csv_custom_delimiter_and_embedded_newline() {
        let recs = parse_csv(b"a;b\n1;\"x\ny\"\n", b';', true).await.unwrap();
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0]["b"], "x\ny");
    }

    #[cfg(not(feature = "excel"))]
    #[test]
    fn excel_without_feature_errors() {
        assert!(
            parse_excel(b"x", None, 0)
                .unwrap_err()
                .to_string()
                .contains("excel")
        );
    }

    #[cfg(feature = "excel")]
    #[test]
    fn cell_conversions_cover_all_variants() {
        use calamine::Data;
        assert_eq!(cell_to_value(&Data::Empty), Value::Null);
        assert_eq!(
            cell_to_value(&Data::String("s".into())),
            Value::String("s".into())
        );
        assert_eq!(cell_to_value(&Data::Bool(true)), Value::Bool(true));
        assert_eq!(cell_to_value(&Data::Int(7)), Value::from(7i64));
        assert_eq!(cell_to_value(&Data::Float(1.5)), Value::from(1.5));
        // API-40: a date cell used to come out as its serial number ("44000").
        let date = |serial: f64, kind, is_1904| {
            Data::DateTime(calamine::ExcelDateTime::new(serial, kind, is_1904))
        };
        use calamine::ExcelDateTimeType::{DateTime, TimeDelta};
        assert_eq!(
            cell_to_value(&date(44_000.0, DateTime, false)),
            "2020-06-18"
        );
        assert_eq!(
            cell_to_value(&date(44_000.5, DateTime, false)),
            "2020-06-18T12:00:00"
        );
        assert_eq!(cell_to_value(&date(0.0, DateTime, true)), "1904-01-01");
        assert_eq!(
            cell_to_string(&date(44_000.0, DateTime, false)),
            "2020-06-18"
        );
        assert!(
            cell_to_value(&date(1.5, TimeDelta, false))
                .as_str()
                .unwrap()
                .starts_with("P")
        );
        assert_eq!(cell_to_value(&date(1e12, DateTime, false)), "1000000000000");
        assert!(cell_to_value(&Data::DateTimeIso("2020".into())).is_string());
        assert!(cell_to_value(&Data::DurationIso("PT1H".into())).is_string());
        assert!(cell_to_value(&Data::Error(calamine::CellErrorType::Div0)).is_string());
        assert_eq!(cell_to_string(&Data::String("k".into())), "k");
        assert_eq!(cell_to_string(&Data::Empty), "");
        assert_eq!(cell_to_string(&Data::Int(3)), "3");
    }

    #[cfg(feature = "excel")]
    #[test]
    fn excel_sheet_selection_and_error_paths() {
        let xlsx = include_bytes!("../tests/fixtures/sample.xlsx");
        // Numeric-index sheet selection (second sheet).
        let recs = parse_excel(xlsx, Some("1"), 0).unwrap();
        assert_eq!(recs[0]["k"], "x");
        // Out-of-range numeric index.
        assert!(
            parse_excel(xlsx, Some("99"), 0)
                .unwrap_err()
                .to_string()
                .contains("out of range")
        );
        // Named sheet not found.
        assert!(
            parse_excel(xlsx, Some("Nope"), 0)
                .unwrap_err()
                .to_string()
                .contains("not found")
        );
        // header_row beyond the sheet.
        assert!(
            parse_excel(xlsx, None, 9999)
                .unwrap_err()
                .to_string()
                .contains("beyond the sheet")
        );
        // Malformed workbook bytes.
        assert!(parse_excel(b"not-a-workbook", None, 0).is_err());
    }
}
