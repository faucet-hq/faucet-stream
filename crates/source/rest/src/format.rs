//! Tabular response-body parsing for `response_format: csv | excel` (#497).
//!
//! Turns a downloaded file body into a `Vec<Value>` of JSON objects, so an
//! authenticated file endpoint (a Microsoft Graph `…/content` download, a
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
        out.push(Value::Object(csv_record_to_object(
            &rec,
            headers.as_deref(),
        )));
    }
    Ok(out)
}

/// The single definition of how a CSV record becomes a JSON object — header-
/// derived keys with the `column_<i>` fallback, all-`String` values. Shared by
/// the `Value` path ([`parse_csv`]) and both NDJSON converters so the encodings
/// can never drift apart (the parity tests additionally pin them byte-identical).
fn csv_record_to_object(
    rec: &csv_async::StringRecord,
    headers: Option<&[String]>,
) -> Map<String, Value> {
    let mut obj = Map::new();
    for (i, field) in rec.iter().enumerate() {
        let key = headers
            .and_then(|h| h.get(i).cloned())
            .unwrap_or_else(|| format!("column_{i}"));
        obj.insert(key, Value::String(field.to_string()));
    }
    obj
}

/// [`csv_record_to_object`] serialized as one NDJSON line (no trailing newline).
fn csv_record_to_ndjson_line(
    rec: &csv_async::StringRecord,
    headers: Option<&[String]>,
) -> Result<String, FaucetError> {
    serde_json::to_string(&Value::Object(csv_record_to_object(rec, headers)))
        .map_err(|e| FaucetError::Source(format!("rest: CSV→NDJSON encode error: {e}")))
}

/// Stream CSV bytes straight to newline-delimited JSON **without materializing a
/// `Vec<Value>`** — the native byte-passthrough source path (#633).
///
/// Emits the *identical* NDJSON the `Value` path would (`parse_csv` →
/// `serde_json::to_string` per record): the same header-derived keys (missing →
/// `column_<i>`) and the same all-`String` values, so switching to this path does
/// not change the bytes BigQuery loads (and hence its autodetected schema). The
/// difference is memory: one record is held at a time instead of the whole page,
/// so peak is `O(output bytes)` (~1× the data) rather than `Vec<serde_json::Value>`'s
/// ~15–20× overhead. Returns the NDJSON bytes and the data-row count.
pub async fn csv_to_ndjson(
    bytes: &[u8],
    delimiter: u8,
    has_headers: bool,
) -> Result<(Vec<u8>, u64), FaucetError> {
    use futures::StreamExt as _;
    let mut rdr = csv_async::AsyncReaderBuilder::new()
        .has_headers(false)
        .delimiter(delimiter)
        .flexible(true)
        .create_reader(bytes);
    let mut records = rdr.records();
    let mut headers: Option<Vec<String>> = None;
    let mut out: Vec<u8> = Vec::new();
    let mut count = 0u64;
    while let Some(rec) = records.next().await {
        let rec = rec.map_err(|e| FaucetError::Source(format!("rest: CSV parse error: {e}")))?;
        if has_headers && headers.is_none() {
            headers = Some(rec.iter().map(str::to_string).collect());
            continue;
        }
        let line = csv_record_to_ndjson_line(&rec, headers.as_deref())?;
        out.extend_from_slice(line.as_bytes());
        out.push(b'\n');
        count += 1;
    }
    Ok((out, count))
}

/// Chunk size at which the streaming converter yields accumulated NDJSON.
const NDJSON_STREAM_CHUNK: usize = 256 * 1024;

/// Stream CSV from an async reader straight to NDJSON **chunks**, holding only one
/// record + a bounded (~256 KiB) output buffer at a time — never the whole page
/// (#633). This is the true-streaming form of [`csv_to_ndjson`]: fed the HTTP
/// response body, it lets `load_native` push bytes into a resumable upload with
/// peak memory independent of the page/row count. Per-record encoding is
/// byte-identical to [`csv_to_ndjson`] / the `Value` path (all-`String` fields,
/// header-derived keys, `column_<i>` fallback).
pub fn csv_reader_to_ndjson_stream<R>(
    reader: R,
    delimiter: u8,
    has_headers: bool,
) -> impl futures::Stream<Item = Result<Vec<u8>, FaucetError>> + Send
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    use futures::StreamExt as _;
    async_stream::try_stream! {
        let mut rdr = csv_async::AsyncReaderBuilder::new()
            .has_headers(false)
            .delimiter(delimiter)
            .flexible(true)
            .create_reader(reader);
        let mut records = rdr.records();
        let mut headers: Option<Vec<String>> = None;
        let mut buf: Vec<u8> = Vec::with_capacity(NDJSON_STREAM_CHUNK + 4096);
        while let Some(rec) = records.next().await {
            let rec =
                rec.map_err(|e| FaucetError::Source(format!("rest: CSV parse error: {e}")))?;
            if has_headers && headers.is_none() {
                headers = Some(rec.iter().map(str::to_string).collect());
                continue;
            }
            let line = csv_record_to_ndjson_line(&rec, headers.as_deref())?;
            buf.extend_from_slice(line.as_bytes());
            buf.push(b'\n');
            if buf.len() >= NDJSON_STREAM_CHUNK {
                yield std::mem::take(&mut buf);
            }
        }
        if !buf.is_empty() {
            yield buf;
        }
    }
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
fn cell_to_string(cell: &calamine::Data) -> String {
    use calamine::Data;
    match cell {
        Data::String(s) => s.clone(),
        Data::Empty => String::new(),
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
        Data::DateTime(dt) => Value::String(dt.to_string()),
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

    /// The native NDJSON converter (#633) must produce byte-identical output to
    /// the `Value` path (`parse_csv` → one `serde_json::to_string` per record), so
    /// switching paths never changes what the sink loads.
    #[tokio::test]
    async fn csv_to_ndjson_matches_value_path_bytes() {
        let csv = b"id,name\n1,Alice\n2,Bob\n";
        let (ndjson, rows) = csv_to_ndjson(csv, b',', true).await.unwrap();
        assert_eq!(rows, 2);
        let expected: String = parse_csv(csv, b',', true)
            .await
            .unwrap()
            .iter()
            .map(|r| format!("{}\n", serde_json::to_string(r).unwrap()))
            .collect();
        assert_eq!(String::from_utf8(ndjson).unwrap(), expected);
    }

    #[tokio::test]
    async fn csv_to_ndjson_handles_quoted_fields_and_custom_delimiter() {
        // A quoted field containing the delimiter and a newline stays one record.
        let csv = b"a;b\n\"x;y\";\"line1\nline2\"\n";
        let (ndjson, rows) = csv_to_ndjson(csv, b';', true).await.unwrap();
        assert_eq!(rows, 1);
        let line = String::from_utf8(ndjson).unwrap();
        let v: serde_json::Value = serde_json::from_str(line.trim_end()).unwrap();
        assert_eq!(v["a"], "x;y");
        assert_eq!(v["b"], "line1\nline2");
    }

    #[tokio::test]
    async fn csv_reader_stream_matches_buffered_ndjson() {
        use futures::StreamExt as _;
        // A big enough input to cross the 256 KiB chunk boundary (multi-chunk path).
        let mut csv = String::from("id,name\n");
        for i in 0..20_000 {
            csv.push_str(&format!("{i},name-{i}-padding-to-grow-the-row\n"));
        }
        let reader = std::io::Cursor::new(csv.clone().into_bytes());
        let chunks: Vec<Vec<u8>> = csv_reader_to_ndjson_stream(reader, b',', true)
            .map(|r| r.unwrap())
            .collect()
            .await;
        assert!(chunks.len() > 1, "large input should yield multiple chunks");
        let streamed: Vec<u8> = chunks.concat();
        let (buffered, rows) = csv_to_ndjson(csv.as_bytes(), b',', true).await.unwrap();
        assert_eq!(rows, 20_000);
        // Byte-identical to the buffered converter (and thus the Value path).
        assert_eq!(streamed, buffered);
    }

    #[tokio::test]
    async fn csv_reader_stream_small_input_single_chunk() {
        use futures::StreamExt as _;
        let reader = std::io::Cursor::new(b"a,b\n1,2\n3,4\n".to_vec());
        let chunks: Vec<Vec<u8>> = csv_reader_to_ndjson_stream(reader, b',', true)
            .map(|r| r.unwrap())
            .collect()
            .await;
        let out = String::from_utf8(chunks.concat()).unwrap();
        assert_eq!(out.lines().count(), 2);
        let v: serde_json::Value = serde_json::from_str(out.lines().next().unwrap()).unwrap();
        assert_eq!(v["a"], "1");
        assert_eq!(v["b"], "2");
    }

    #[tokio::test]
    async fn csv_to_ndjson_missing_header_uses_column_index() {
        // A row wider than the header falls back to `column_<i>` — same as parse_csv.
        let (ndjson, rows) = csv_to_ndjson(b"a\n1,2\n", b',', true).await.unwrap();
        assert_eq!(rows, 1);
        let v: serde_json::Value =
            serde_json::from_str(String::from_utf8(ndjson).unwrap().trim_end()).unwrap();
        assert_eq!(v["a"], "1");
        assert_eq!(v["column_1"], "2");
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
        assert!(
            cell_to_value(&Data::DateTime(calamine::ExcelDateTime::new(
                44_000.0,
                calamine::ExcelDateTimeType::DateTime,
                false
            )))
            .is_string()
        );
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
