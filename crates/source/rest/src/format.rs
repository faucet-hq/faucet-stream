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

/// Decode CSV from an `AsyncRead` into **bounded pages** of records (#626).
///
/// The `Value`-path twin of [`csv_reader_to_ndjson_stream`]: the reader is fed
/// straight from the HTTP body, and records are emitted in `page_size` chunks,
/// so peak memory is one page rather than the whole result set. The buffering
/// [`parse_csv`] keeps its exact per-record semantics — `flexible(true)`,
/// header-derived keys, `column_<i>` fallback, all-`String` values — so a
/// config that switches onto this path sees identical records.
///
/// `page_size == 0` means "no batching" (the house sentinel): everything lands
/// in one page, matching [`parse_csv`].
pub fn csv_reader_to_value_pages<R>(
    reader: R,
    delimiter: u8,
    has_headers: bool,
    page_size: usize,
) -> impl futures::Stream<Item = Result<Vec<Value>, FaucetError>> + Send
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
        let mut page: Vec<Value> = Vec::with_capacity(if page_size == 0 { 1024 } else { page_size });
        while let Some(rec) = records.next().await {
            let rec =
                rec.map_err(|e| FaucetError::Source(format!("rest: CSV parse error: {e}")))?;
            if has_headers && headers.is_none() {
                headers = Some(rec.iter().map(str::to_string).collect());
                continue;
            }
            page.push(Value::Object(csv_record_to_object(&rec, headers.as_deref())));
            if page_size != 0 && page.len() >= page_size {
                yield std::mem::replace(&mut page, Vec::with_capacity(page_size));
            }
        }
        // A trailing partial page, and the empty-result case: a header-only (or
        // zero-byte) body must yield nothing rather than an error.
        if !page.is_empty() {
            yield page;
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
    use calamine::{Reader, Xlsx};
    // Borrow the body rather than copying it: `&[u8]` is `Read + Seek`, so the
    // `to_vec()` here was a second whole-file allocation before calamine made
    // its own (#624).
    let cursor = std::io::Cursor::new(bytes);
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
    // Iterate the range twice rather than collecting every row into a `Vec`
    // first (#624): `rows()` is a cheap iterator over the already-parsed
    // range, so the collect bought nothing and cost one pointer per row on
    // top of a sheet that is already fully in memory.
    let headers: Vec<String> = range
        .rows()
        .nth(header_row)
        .ok_or_else(|| {
            FaucetError::Source(format!(
                "rest: Excel header_row {header_row} is beyond the sheet ({} rows)",
                range.rows().count()
            ))
        })?
        .iter()
        .map(cell_to_string)
        .collect();
    let mut out = Vec::new();
    for row in range.rows().skip(header_row + 1) {
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

/// Stream a CSV body as Arrow [`RecordBatch`]es of `batch_size` rows (#635).
///
/// The columnar twin of [`csv_reader_to_value_pages`], and deliberately built
/// on the same `csv_async` reader rather than `arrow-csv`: `arrow-csv` is
/// synchronous and wants a whole `Read`, which would reintroduce the buffering
/// #626 removed, and its type **inference** would break column parity with the
/// `Value` path. Every column is `Utf8`, matching the all-STRING behaviour the
/// NDJSON path had to be pinned to — BigQuery autodetect on a Bulk export
/// guesses types per file and disagrees across pages.
///
/// Column names come from the header row, or `column_<i>` without one, so a
/// batch's schema is field-for-field what [`csv_record_to_object`] produces.
/// A short row is padded with nulls and a long one widens no schema — the
/// header fixes the column set for the whole stream, exactly as it fixes the
/// key set on the `Value` path.
#[cfg(feature = "arrow")]
pub fn csv_reader_to_record_batches<R>(
    reader: R,
    delimiter: u8,
    has_headers: bool,
    batch_size: usize,
) -> impl futures::Stream<Item = Result<arrow::record_batch::RecordBatch, FaucetError>> + Send
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use futures::StreamExt as _;
    use std::sync::Arc;

    async_stream::try_stream! {
        let mut rdr = csv_async::AsyncReaderBuilder::new()
            .has_headers(false)
            .delimiter(delimiter)
            .flexible(true)
            .create_reader(reader);
        let mut records = rdr.records();
        let mut headers: Option<Vec<String>> = None;
        // One `Vec<Option<String>>` per column; transposed into arrays on flush.
        let mut cols: Vec<Vec<Option<String>>> = Vec::new();
        let mut schema: Option<Arc<Schema>> = None;
        let mut rows = 0usize;
        let cap = if batch_size == 0 { 1024 } else { batch_size };

        while let Some(rec) = records.next().await {
            let rec = rec
                .map_err(|e| FaucetError::Source(format!("rest: CSV parse error: {e}")))?;
            if has_headers && headers.is_none() {
                headers = Some(rec.iter().map(str::to_string).collect());
                continue;
            }
            // The first data row fixes the column set when there is no header.
            if schema.is_none() {
                let names: Vec<String> = match &headers {
                    Some(h) => h.clone(),
                    None => (0..rec.len()).map(|i| format!("column_{i}")).collect(),
                };
                cols = vec![Vec::with_capacity(cap); names.len()];
                schema = Some(Arc::new(Schema::new(
                    names
                        .into_iter()
                        .map(|n| Field::new(n, DataType::Utf8, true))
                        .collect::<Vec<_>>(),
                )));
            }
            for (i, col) in cols.iter_mut().enumerate() {
                // A field the row does not have is null, never a silent shift.
                col.push(rec.get(i).map(str::to_string));
            }
            rows += 1;
            if batch_size != 0 && rows >= batch_size {
                let sch = schema.clone().expect("schema set above");
                let arrays: Vec<arrow::array::ArrayRef> = cols
                    .iter_mut()
                    .map(|c| Arc::new(StringArray::from(std::mem::take(c))) as arrow::array::ArrayRef)
                    .collect();
                rows = 0;
                yield arrow::record_batch::RecordBatch::try_new(sch, arrays).map_err(|e| {
                    FaucetError::Source(format!("rest: building an Arrow batch: {e}"))
                })?;
            }
        }
        // Trailing partial batch. A header-only or zero-byte body yields
        // nothing, matching `csv_reader_to_value_pages`.
        if rows > 0 {
            let sch = schema.clone().expect("schema set with the first row");
            let arrays: Vec<arrow::array::ArrayRef> = cols
                .iter_mut()
                .map(|c| Arc::new(StringArray::from(std::mem::take(c))) as arrow::array::ArrayRef)
                .collect();
            yield arrow::record_batch::RecordBatch::try_new(sch, arrays).map_err(|e| {
                FaucetError::Source(format!("rest: building an Arrow batch: {e}"))
            })?;
        }
    }
}

#[cfg(test)]
mod tests {

    /// #626 — the bounded-memory CSV page decoder.
    mod value_pages {
        use super::*;
        use futures::StreamExt as _;

        async fn pages(body: &'static str, page_size: usize) -> Vec<Vec<Value>> {
            let reader = std::io::Cursor::new(body.as_bytes());
            csv_reader_to_value_pages(reader, b',', true, page_size)
                .map(|p| p.expect("decodes"))
                .collect()
                .await
        }

        #[tokio::test]
        async fn records_are_split_into_bounded_pages() {
            // The property the whole change exists for: peak memory is a page,
            // not the result set.
            let body = "id,name\n1,a\n2,b\n3,c\n4,d\n5,e\n";
            let got = pages(body, 2).await;
            assert_eq!(
                got.iter().map(Vec::len).collect::<Vec<_>>(),
                vec![2, 2, 1],
                "pages must cap at page_size, with a partial final page"
            );
            let ids: Vec<&str> = got
                .iter()
                .flatten()
                .map(|r| r["id"].as_str().unwrap())
                .collect();
            assert_eq!(
                ids,
                vec!["1", "2", "3", "4", "5"],
                "no record lost at a boundary"
            );
        }

        #[tokio::test]
        async fn a_page_size_of_zero_means_one_page() {
            // The house "no batching" sentinel, matching `parse_csv`.
            let got = pages("id\n1\n2\n3\n", 0).await;
            assert_eq!(got.len(), 1);
            assert_eq!(got[0].len(), 3);
        }

        #[tokio::test]
        async fn the_header_is_carried_across_pages() {
            // The header arrives in the first chunk only; every later page must
            // still get named fields rather than `column_0`.
            let got = pages("id,name\n1,a\n2,b\n3,c\n", 1).await;
            assert_eq!(got.len(), 3);
            for page in &got {
                assert!(
                    page[0].get("name").is_some(),
                    "later pages lost the header: {page:?}"
                );
            }
        }

        #[tokio::test]
        async fn a_quoted_field_with_newlines_is_one_record() {
            // A record straddling a read-chunk edge must not be split. The
            // embedded newline is the case that would break a line-splitting
            // decoder.
            let body = "id,note\n1,\"line one\nline two\"\n2,plain\n";
            let got = pages(body, 10).await;
            let flat: Vec<&Value> = got.iter().flatten().collect();
            assert_eq!(flat.len(), 2, "two records, not three");
            assert_eq!(
                flat[0]["note"].as_str().unwrap(),
                "line one\nline two",
                "the embedded newline survives"
            );
        }

        #[tokio::test]
        async fn an_empty_body_yields_no_pages() {
            assert!(pages("", 100).await.is_empty());
            // Header-only is also zero records, not an error.
            assert!(pages("id,name\n", 100).await.is_empty());
        }

        #[tokio::test]
        async fn a_ragged_row_is_tolerated_like_the_buffered_decoder() {
            // `flexible(true)` parity with `parse_csv` — a short row must not
            // fail the stream.
            let got = pages("a,b\n1\n2,3\n", 100).await;
            let flat: Vec<&Value> = got.iter().flatten().collect();
            assert_eq!(flat.len(), 2);
        }

        #[tokio::test]
        async fn streamed_pages_match_the_buffered_decoder_exactly() {
            // The equivalence that lets a config switch paths silently.
            let body = "id,name\n1,ada\n2,grace\n3,\"quoted, comma\"\n";
            let streamed: Vec<Value> = pages(body, 2).await.into_iter().flatten().collect();
            let buffered = parse_csv(body.as_bytes(), b',', true)
                .await
                .expect("parses");
            assert_eq!(streamed, buffered);
        }
    }
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

    /// #635 — the Arrow-columnar CSV decoder. Every test here asserts against
    /// the `Value` path, because the only thing that makes the columnar path
    /// safe to select automatically is that it produces the *same data*.
    #[cfg(feature = "arrow")]
    mod record_batches {
        use super::super::*;
        use futures::StreamExt as _;

        async fn batches(
            csv: &str,
            has_headers: bool,
            batch_size: usize,
        ) -> Vec<arrow::record_batch::RecordBatch> {
            let r = std::io::Cursor::new(csv.as_bytes().to_vec());
            let s = csv_reader_to_record_batches(r, b',', has_headers, batch_size);
            futures::pin_mut!(s);
            let mut out = Vec::new();
            while let Some(b) = s.next().await {
                out.push(b.expect("batch"));
            }
            out
        }

        async fn values(csv: &str, has_headers: bool, page_size: usize) -> Vec<Value> {
            let r = std::io::Cursor::new(csv.as_bytes().to_vec());
            let s = csv_reader_to_value_pages(r, b',', has_headers, page_size);
            futures::pin_mut!(s);
            let mut out = Vec::new();
            while let Some(p) = s.next().await {
                out.extend(p.expect("page"));
            }
            out
        }

        /// The property the automatic path selection rests on: same rows, same
        /// columns, same strings as the `Value` decoder.
        #[tokio::test]
        async fn a_batch_carries_exactly_what_the_value_path_carries() {
            let csv = "id,name\n1,ada\n2,grace\n3,hopper\n";
            let bs = batches(csv, true, 2).await;
            let vs = values(csv, true, 2).await;

            let rows: usize = bs.iter().map(|b| b.num_rows()).sum();
            assert_eq!(rows, vs.len(), "row count must match the Value path");

            let back: Vec<Value> = bs
                .iter()
                .flat_map(|b| faucet_core::columnar::record_batch_to_values(b).expect("to values"))
                .collect();
            assert_eq!(back, vs, "columnar and Value decoders must agree");
        }

        /// Every column is Utf8 — never inferred. Inference is what made the
        /// NDJSON path disagree with itself across pages of one Bulk export.
        #[tokio::test]
        async fn every_column_is_utf8_and_named_from_the_header() {
            let bs = batches("id,amount,ok\n1,2.5,true\n", true, 0).await;
            assert_eq!(bs.len(), 1);
            let sch = bs[0].schema();
            let names: Vec<&str> = sch.fields().iter().map(|f| f.name().as_str()).collect();
            assert_eq!(names, vec!["id", "amount", "ok"]);
            for f in sch.fields() {
                assert_eq!(
                    f.data_type(),
                    &arrow::datatypes::DataType::Utf8,
                    "column `{}` must stay Utf8, not be inferred",
                    f.name()
                );
                assert!(f.is_nullable(), "a short row must be expressible as null");
            }
        }

        /// Without a header the columns are positional, matching
        /// `csv_record_to_object`'s `column_<i>`.
        #[tokio::test]
        async fn headerless_columns_are_positional_and_match_the_value_path() {
            let csv = "1,ada\n2,grace\n";
            let bs = batches(csv, false, 0).await;
            let sch = bs[0].schema();
            let names: Vec<&str> = sch.fields().iter().map(|f| f.name().as_str()).collect();
            assert_eq!(names, vec!["column_0", "column_1"]);
            let back = faucet_core::columnar::record_batch_to_values(&bs[0]).expect("values");
            assert_eq!(back, values(csv, false, 0).await);
        }

        /// `batch_size` bounds the batch, which is the whole point: peak memory
        /// is one batch, not one Bulk export.
        #[tokio::test]
        async fn batch_size_bounds_each_batch_and_zero_means_one_batch() {
            let mut csv = String::from("id\n");
            for i in 0..5 {
                csv.push_str(&format!("{i}\n"));
            }
            let bs = batches(&csv, true, 2).await;
            assert_eq!(
                bs.iter().map(|b| b.num_rows()).collect::<Vec<_>>(),
                vec![2, 2, 1],
                "the trailing partial batch must still be emitted"
            );
            let one = batches(&csv, true, 0).await;
            assert_eq!(one.len(), 1);
            assert_eq!(one[0].num_rows(), 5);
        }

        /// A header-only or empty body yields no batch at all — the caller
        /// turns that into the same empty page the `Value` path emits.
        #[tokio::test]
        async fn a_header_only_or_empty_body_yields_no_batch() {
            assert!(batches("id,name\n", true, 0).await.is_empty());
            assert!(batches("", true, 0).await.is_empty());
            assert!(values("id,name\n", true, 0).await.is_empty());
        }

        /// A ragged row must null-pad, never shift a value into the wrong
        /// column — the failure mode that silently corrupts a whole export.
        #[tokio::test]
        async fn a_short_row_pads_with_null_rather_than_shifting() {
            let csv = "a,b,c\n1,2,3\n4,5\n";
            let bs = batches(csv, true, 0).await;
            let back = faucet_core::columnar::record_batch_to_values(&bs[0]).expect("values");
            assert_eq!(back[0]["c"], Value::String("3".into()));
            assert_eq!(back[1]["a"], Value::String("4".into()));
            assert_eq!(back[1]["b"], Value::String("5".into()));
            assert_eq!(back[1]["c"], Value::Null, "the missing field is null");
        }
    }
}
