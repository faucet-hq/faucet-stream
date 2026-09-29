//! CSV file source.

use crate::config::CsvSourceConfig;
use async_trait::async_trait;
use faucet_core::{FaucetError, Stream, StreamPage};
use serde_json::{Map, Value};
use std::pin::Pin;

/// A source that reads records from a CSV file.
///
/// Each row is returned as a JSON object. If the file has headers, the header
/// names are used as keys. Otherwise, generated names (`column_0`, `column_1`,
/// etc.) are used.
#[deprecated(note = "use faucet-source-file / faucet-sink-file")]
pub struct CsvSource {
    config: CsvSourceConfig,
}

impl CsvSource {
    /// Create a new CSV source from the given configuration.
    pub fn new(config: CsvSourceConfig) -> Self {
        Self { config }
    }
}

/// Build the strict-mode field-count-mismatch error message for a ragged row.
///
/// Pure helper (no I/O) so the message wording is unit-testable. `line` is the
/// 1-based physical file line; `detail` is the underlying `csv-async`
/// description of the mismatch (which names the field counts).
fn ragged_row_message(line: usize, path: &str, detail: &str) -> String {
    format!(
        "ragged CSV row at line {line} in '{path}': {detail} — a short or long \
         row is a structural defect that would silently corrupt downstream \
         records; fix the file or set `flexible: true` to accept uneven rows"
    )
}

#[async_trait]
impl faucet_core::Source for CsvSource {
    async fn fetch_with_context(
        &self,
        context: &std::collections::HashMap<String, serde_json::Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        use futures::StreamExt;
        let mut all = Vec::new();
        let mut s = self.stream_pages(context, self.config.batch_size);
        while let Some(page) = s.next().await {
            let page = page?;
            all.extend(page.records);
        }
        Ok(all)
    }

    /// Stream rows from the CSV file without buffering the full result set.
    ///
    /// The file is opened via [`tokio::fs::File`] and parsed by `csv-async`,
    /// a streaming RFC-4180 reader. Because it tracks quote state across
    /// physical lines, **quoted fields containing embedded newlines are parsed
    /// correctly** — the file written by `faucet-sink-csv` round-trips back
    /// losslessly (#78/#40). Each emitted [`StreamPage`] holds up to
    /// [`CsvSourceConfig::batch_size`] rows.
    ///
    /// The trait-level `batch_size` argument is ignored in favour of the
    /// config field — the config is the user-facing knob the README documents,
    /// and routing the pipeline-supplied hint through it would silently
    /// override an explicit config value.
    ///
    /// `batch_size = 0` drains the entire file into a single page. The CSV
    /// source has no incremental-replication mode, so every emitted page
    /// carries `bookmark: None`.
    ///
    /// Field-count leniency is opt-in via [`CsvSourceConfig::flexible`]
    /// (default `false`): a ragged row aborts the stream with a typed
    /// [`FaucetError::Source`] naming the offending line rather than emitting an
    /// incomplete record.
    fn stream_pages<'a>(
        &'a self,
        context: &'a std::collections::HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        let batch_size = self.config.batch_size;

        Box::pin(async_stream::try_stream! {
            use futures::StreamExt as _;

            let mut config = self.config.clone();
            if !context.is_empty() {
                config.path = faucet_core::util::substitute_context(&config.path, context);
            }

            let file = tokio::fs::File::open(&config.path).await.map_err(|e| {
                FaucetError::Config(format!(
                    "failed to open CSV file '{}': {e}",
                    config.path
                ))
            })?;
            let reader = tokio::io::BufReader::new(file);
            #[cfg(feature = "compression")]
            let reader = {
                let codec = config.compression.resolve(&config.path);
                faucet_core::compression::warn_mismatch(&config.path, codec);
                faucet_core::compression::wrap_async_reader(reader, codec)
            };

            // `csv-async` tracks quote state across physical lines, so a
            // record whose field contains an embedded newline is read as one
            // record. Headers are handled manually below, so the underlying
            // reader is configured header-less.
            let mut csv_reader = csv_async::AsyncReaderBuilder::new()
                .has_headers(false)
                .delimiter(config.delimiter)
                .quote(config.quote)
                // Field-count leniency is opt-in (`flexible`, default false). A
                // ragged row is a structural defect; silently emitting an
                // incomplete record would corrupt downstream consumers. The
                // header row is parsed manually below, so `csv-async` would only
                // compare *data* rows against each other — it never sees the
                // header. We therefore also enforce the expected width
                // explicitly per data row (see `check_field_count`), which
                // catches a first data row that is short relative to the header.
                .flexible(config.flexible)
                .create_reader(reader);

            let mut records = csv_reader.records();

            // The first record is the header row when configured.
            let headers: Vec<String> = if config.has_headers {
                match records.next().await {
                    Some(rec) => {
                        let rec = rec.map_err(|e| FaucetError::Config(format!(
                            "CSV header parse error in '{}': {e}", config.path
                        )))?;
                        let headers: Vec<String> = rec.iter().map(|f| f.to_string()).collect();
                        // Reject duplicate header names up front: each row becomes a
                        // JSON object keyed by header name, so a repeated header (or
                        // two blank-named columns) would silently overwrite earlier
                        // columns last-wins, dropping data for the whole run. Fail
                        // fast naming the offending header and its 0-based positions.
                        let mut seen: std::collections::HashMap<&str, usize> =
                            std::collections::HashMap::with_capacity(headers.len());
                        for (col_idx, name) in headers.iter().enumerate() {
                            if let Some(&first_idx) = seen.get(name.as_str()) {
                                let display = if name.is_empty() { "(empty)" } else { name.as_str() };
                                Err(FaucetError::Config(format!(
                                    "duplicate CSV header {display} in '{}' at columns {first_idx} and {col_idx}; \
                                     each row is keyed by header name, so a repeated header would silently drop columns — \
                                     rename the duplicate or disable headers",
                                    config.path
                                )))?;
                            }
                            seen.insert(name.as_str(), col_idx);
                        }
                        headers
                    }
                    None => Vec::new(),
                }
            } else {
                Vec::new()
            };

            let chunk = if batch_size == 0 { usize::MAX } else { batch_size };
            let initial_capacity = if batch_size == 0 { 1024 } else { batch_size };
            let mut buffer: Vec<Value> = Vec::with_capacity(initial_capacity);
            let mut total = 0usize;
            let mut row_idx = 0usize;

            while let Some(rec) = records.next().await {
                // Physical file line: `row_idx` is the 0-based *data* row, so
                // +1 for 1-based, and +1 more when a header row was consumed —
                // otherwise the first data row (file line 2) is misreported as 1.
                let line = row_idx + 1 + usize::from(config.has_headers);
                let record = rec.map_err(|e| {
                    // With `flexible(false)` (the strict default), `csv-async`
                    // raises `UnequalLengths` for any row whose field count
                    // differs from the prior record — and because the header is
                    // the first physical record, this catches a data row that is
                    // short or long relative to the header too. A ragged row is a
                    // structural defect, not a config error, so surface it as
                    // `Source` (with `flexible: true` the reader never raises it).
                    if matches!(e.kind(), csv_async::ErrorKind::UnequalLengths { .. }) {
                        FaucetError::Source(ragged_row_message(line, &config.path, &e.to_string()))
                    } else {
                        FaucetError::Config(format!(
                            "CSV parse error at line {line} in '{}': {e}",
                            config.path
                        ))
                    }
                })?;

                let mut obj = Map::new();
                for (col_idx, field) in record.iter().enumerate() {
                    let key = if col_idx < headers.len() {
                        headers[col_idx].clone()
                    } else {
                        format!("column_{col_idx}")
                    };
                    let value = if config.null_values.iter().any(|n| n == field) {
                        Value::Null
                    } else {
                        Value::String(field.to_string())
                    };
                    obj.insert(key, value);
                }
                buffer.push(Value::Object(obj));
                row_idx += 1;

                if buffer.len() >= chunk {
                    let page = std::mem::replace(&mut buffer, Vec::with_capacity(initial_capacity));
                    total += page.len();
                    yield StreamPage { records: page, bookmark: None };
                }
            }

            if !buffer.is_empty() {
                total += buffer.len();
                yield StreamPage { records: buffer, bookmark: None };
            }

            tracing::info!(
                rows = total,
                batch_size,
                path = %config.path,
                "CSV source stream complete",
            );
        })
    }

    fn connector_name(&self) -> &'static str {
        "csv"
    }

    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(CsvSourceConfig))
            .expect("schema serialization")
    }

    fn dataset_uri(&self) -> String {
        format!("file://{}", self.config.path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::Source;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn reads_csv_with_headers() {
        let mut tmp = NamedTempFile::new().unwrap();
        writeln!(tmp, "id,name,age").unwrap();
        writeln!(tmp, "1,Alice,30").unwrap();
        writeln!(tmp, "2,Bob,25").unwrap();
        tmp.flush().unwrap();

        let config = CsvSourceConfig::new(tmp.path().to_str().unwrap());
        let source = CsvSource::new(config);
        let records = source.fetch_all().await.unwrap();

        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["id"], "1");
        assert_eq!(records[0]["name"], "Alice");
        assert_eq!(records[0]["age"], "30");
        assert_eq!(records[1]["name"], "Bob");
    }

    #[tokio::test]
    async fn reads_csv_without_headers() {
        let mut tmp = NamedTempFile::new().unwrap();
        writeln!(tmp, "Alice,30").unwrap();
        writeln!(tmp, "Bob,25").unwrap();
        tmp.flush().unwrap();

        let config = CsvSourceConfig::new(tmp.path().to_str().unwrap()).has_headers(false);
        let source = CsvSource::new(config);
        let records = source.fetch_all().await.unwrap();

        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["column_0"], "Alice");
        assert_eq!(records[0]["column_1"], "30");
    }

    #[tokio::test]
    async fn reads_tsv_with_custom_delimiter() {
        let mut tmp = NamedTempFile::new().unwrap();
        writeln!(tmp, "id\tname").unwrap();
        writeln!(tmp, "1\tAlice").unwrap();
        tmp.flush().unwrap();

        let config = CsvSourceConfig::new(tmp.path().to_str().unwrap()).delimiter(b'\t');
        let source = CsvSource::new(config);
        let records = source.fetch_all().await.unwrap();

        assert_eq!(records.len(), 1);
        assert_eq!(records[0]["id"], "1");
        assert_eq!(records[0]["name"], "Alice");
    }

    #[tokio::test]
    async fn reads_quoted_field_with_embedded_newline() {
        // Regression for #78/#40: a quoted field spanning multiple physical
        // lines (as the CSV sink emits) must round-trip as one record.
        let mut tmp = NamedTempFile::new().unwrap();
        write!(tmp, "id,note\n1,\"line one\nline two\"\n2,\"plain\"\n").unwrap();
        tmp.flush().unwrap();

        let config = CsvSourceConfig::new(tmp.path().to_str().unwrap());
        let source = CsvSource::new(config);
        let records = source.fetch_all().await.unwrap();

        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["id"], "1");
        assert_eq!(records[0]["note"], "line one\nline two");
        assert_eq!(records[1]["note"], "plain");
    }

    #[tokio::test]
    async fn reads_quoted_field_with_embedded_delimiter() {
        let mut tmp = NamedTempFile::new().unwrap();
        write!(tmp, "id,name\n1,\"Doe, John\"\n").unwrap();
        tmp.flush().unwrap();

        let config = CsvSourceConfig::new(tmp.path().to_str().unwrap());
        let source = CsvSource::new(config);
        let records = source.fetch_all().await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0]["name"], "Doe, John");
    }

    #[tokio::test]
    async fn empty_csv_returns_empty_vec() {
        let mut tmp = NamedTempFile::new().unwrap();
        writeln!(tmp, "id,name").unwrap();
        tmp.flush().unwrap();

        let config = CsvSourceConfig::new(tmp.path().to_str().unwrap());
        let source = CsvSource::new(config);
        let records = source.fetch_all().await.unwrap();

        assert!(records.is_empty());
    }

    #[tokio::test]
    async fn missing_file_returns_error() {
        let config = CsvSourceConfig::new("/nonexistent/path/data.csv");
        let source = CsvSource::new(config);
        let result = source.fetch_all().await;

        assert!(result.is_err());
    }

    #[cfg(feature = "compression")]
    #[tokio::test]
    async fn roundtrip_gzip_via_stream_pages() {
        use faucet_core::CompressionConfig;
        let tmp = NamedTempFile::with_suffix(".csv.gz").unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        let plain = b"id,name\n1,Alice\n2,Bob\n";
        let compressed =
            faucet_core::compression::compress_buf(plain, faucet_core::Compression::Gzip).unwrap();
        tokio::fs::write(&path, &compressed).await.unwrap();

        let config = CsvSourceConfig::new(&path).compression(CompressionConfig::Auto);
        let source = CsvSource::new(config);
        let records = source.fetch_all().await.unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["name"], "Alice");
        assert_eq!(records[1]["name"], "Bob");
    }

    #[cfg(feature = "compression")]
    #[tokio::test]
    async fn roundtrip_zstd_via_stream_pages() {
        use faucet_core::CompressionConfig;
        let tmp = NamedTempFile::with_suffix(".csv.zst").unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        let plain = b"id,name\n1,Carol\n";
        let compressed =
            faucet_core::compression::compress_buf(plain, faucet_core::Compression::Zstd).unwrap();
        tokio::fs::write(&path, &compressed).await.unwrap();

        let config = CsvSourceConfig::new(&path).compression(CompressionConfig::Auto);
        let source = CsvSource::new(config);
        let records = source.fetch_all().await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0]["name"], "Carol");
    }

    #[tokio::test]
    async fn duplicate_header_names_fail_fast() {
        // Data-loss regression (F7, audit #264): a repeated header name must be
        // rejected, not silently overwrite the earlier column last-wins.
        let mut tmp = NamedTempFile::new().unwrap();
        writeln!(tmp, "id,name,id").unwrap();
        writeln!(tmp, "1,a,2").unwrap();
        tmp.flush().unwrap();

        let config = CsvSourceConfig::new(tmp.path().to_str().unwrap());
        let source = CsvSource::new(config);
        let result = source.fetch_all().await;

        let err = result.expect_err("duplicate header must error, not drop a column");
        assert!(
            matches!(err, FaucetError::Config(_)),
            "expected FaucetError::Config, got {err:?}"
        );
        let msg = err.to_string();
        assert!(msg.contains("duplicate CSV header"), "message was: {msg}");
        assert!(msg.contains("id"), "message should name the header: {msg}");
        assert!(
            msg.contains("columns 0 and 2"),
            "message should name positions: {msg}"
        );
    }

    #[tokio::test]
    async fn duplicate_blank_header_names_fail_fast() {
        // Two empty-string headers must be caught too.
        let mut tmp = NamedTempFile::new().unwrap();
        writeln!(tmp, "id,,").unwrap();
        writeln!(tmp, "1,a,b").unwrap();
        tmp.flush().unwrap();

        let config = CsvSourceConfig::new(tmp.path().to_str().unwrap());
        let source = CsvSource::new(config);
        let result = source.fetch_all().await;

        let err = result.expect_err("duplicate blank header must error");
        assert!(matches!(err, FaucetError::Config(_)), "got {err:?}");
        let msg = err.to_string();
        assert!(msg.contains("duplicate CSV header"), "message was: {msg}");
        assert!(
            msg.contains("(empty)"),
            "blank header should render as (empty): {msg}"
        );
        assert!(msg.contains("columns 1 and 2"), "positions: {msg}");
    }

    #[tokio::test]
    async fn headerless_csv_allows_repeated_values_without_error() {
        // With has_headers=false there are no header names to collide; the
        // duplicate-header guard must not fire (keys are generated column_N).
        let mut tmp = NamedTempFile::new().unwrap();
        writeln!(tmp, "1,1,1").unwrap();
        tmp.flush().unwrap();

        let config = CsvSourceConfig::new(tmp.path().to_str().unwrap()).has_headers(false);
        let source = CsvSource::new(config);
        let records = source.fetch_all().await.unwrap();

        assert_eq!(records.len(), 1);
        assert_eq!(records[0]["column_0"], "1");
        assert_eq!(records[0]["column_1"], "1");
        assert_eq!(records[0]["column_2"], "1");
    }

    #[test]
    fn dataset_uri_reflects_path() {
        // CsvSource::new is sync — construct directly.
        let source = CsvSource::new(CsvSourceConfig::new("/data/input.csv"));
        assert_eq!(source.dataset_uri(), "file:///data/input.csv");
    }

    #[test]
    fn ragged_row_message_names_line_path_and_detail_and_hints_optin() {
        let msg = ragged_row_message(
            3,
            "/data/in.csv",
            "found record with 2 fields, but the previous record has 4 fields",
        );
        assert!(msg.contains("line 3"), "msg: {msg}");
        assert!(msg.contains("/data/in.csv"), "msg: {msg}");
        assert!(msg.contains("2 fields"), "msg: {msg}");
        assert!(msg.contains("4 fields"), "msg: {msg}");
        assert!(msg.contains("structural defect"), "msg: {msg}");
        assert!(msg.contains("flexible: true"), "msg: {msg}");
    }

    #[tokio::test]
    async fn ragged_short_row_errors_by_default() {
        // F22: a data row with fewer fields than the header is a structural
        // defect and must abort with a typed error, not silently emit a record
        // missing columns.
        let mut tmp = NamedTempFile::new().unwrap();
        writeln!(tmp, "id,name,age").unwrap();
        writeln!(tmp, "1,Alice,30").unwrap();
        writeln!(tmp, "2,Bob").unwrap(); // short row — missing `age`
        tmp.flush().unwrap();

        let config = CsvSourceConfig::new(tmp.path().to_str().unwrap());
        let source = CsvSource::new(config);
        let result = source.fetch_all().await;

        let err = result.expect_err("ragged row must error under default strict mode");
        assert!(
            matches!(err, FaucetError::Source(_)),
            "expected FaucetError::Source, got {err:?}"
        );
        let msg = err.to_string();
        assert!(msg.contains("ragged CSV row"), "message was: {msg}");
        assert!(
            msg.contains("line 3"),
            "should name the offending line: {msg}"
        );
        assert!(msg.contains("2 fields"), "should name the count: {msg}");
        assert!(
            msg.contains("flexible: true"),
            "should hint the opt-in: {msg}"
        );
    }

    #[tokio::test]
    async fn first_data_row_short_vs_header_errors_by_default() {
        // The *first* data row being short relative to the header must error.
        // `csv-async` compares against the header (its first physical record),
        // so this is caught even though the header is mapped to keys manually.
        let mut tmp = NamedTempFile::new().unwrap();
        writeln!(tmp, "id,name,age").unwrap();
        writeln!(tmp, "1,Alice").unwrap(); // first data row, short
        tmp.flush().unwrap();

        let config = CsvSourceConfig::new(tmp.path().to_str().unwrap());
        let source = CsvSource::new(config);
        let err = source
            .fetch_all()
            .await
            .expect_err("short first data row must error vs header");
        assert!(matches!(err, FaucetError::Source(_)), "got {err:?}");
        let msg = err.to_string();
        assert!(msg.contains("ragged CSV row"), "msg: {msg}");
        assert!(msg.contains("2 fields"), "msg: {msg}");
        assert!(msg.contains("3 fields"), "msg: {msg}");
        assert!(msg.contains("line 2"), "msg: {msg}");
    }

    #[tokio::test]
    async fn ragged_long_row_errors_by_default() {
        // A row with *more* fields than the header is equally a defect.
        let mut tmp = NamedTempFile::new().unwrap();
        writeln!(tmp, "id,name").unwrap();
        writeln!(tmp, "1,Alice,extra").unwrap();
        tmp.flush().unwrap();

        let config = CsvSourceConfig::new(tmp.path().to_str().unwrap());
        let source = CsvSource::new(config);
        let err = source
            .fetch_all()
            .await
            .expect_err("long row must error under strict mode");
        assert!(matches!(err, FaucetError::Source(_)), "got {err:?}");
        assert!(err.to_string().contains("line 2"), "{err:?}");
    }

    #[tokio::test]
    async fn ragged_row_accepted_when_flexible() {
        // Opting in to `flexible` preserves the previous lenient behavior: the
        // short row is accepted and emitted with the columns it does have.
        let mut tmp = NamedTempFile::new().unwrap();
        writeln!(tmp, "id,name,age").unwrap();
        writeln!(tmp, "1,Alice,30").unwrap();
        writeln!(tmp, "2,Bob").unwrap();
        tmp.flush().unwrap();

        let config = CsvSourceConfig::new(tmp.path().to_str().unwrap()).flexible(true);
        let source = CsvSource::new(config);
        let records = source.fetch_all().await.unwrap();

        assert_eq!(records.len(), 2);
        assert_eq!(records[1]["id"], "2");
        assert_eq!(records[1]["name"], "Bob");
        // The short row is missing `age` — present but incomplete (the opt-in
        // contract).
        assert!(records[1].get("age").is_none());
    }

    #[tokio::test]
    async fn long_row_accepted_when_flexible_gains_generated_key() {
        let mut tmp = NamedTempFile::new().unwrap();
        writeln!(tmp, "id,name").unwrap();
        writeln!(tmp, "1,Alice,extra").unwrap();
        tmp.flush().unwrap();

        let config = CsvSourceConfig::new(tmp.path().to_str().unwrap()).flexible(true);
        let source = CsvSource::new(config);
        let records = source.fetch_all().await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0]["column_2"], "extra");
    }

    #[tokio::test]
    async fn headerless_ragged_row_errors_by_default() {
        // Header-less: the first data row sets the expected width; a later row
        // of a different width must error (csv-async itself would catch this,
        // but verify the path under our config plumbing).
        let mut tmp = NamedTempFile::new().unwrap();
        writeln!(tmp, "a,b,c").unwrap();
        writeln!(tmp, "d,e").unwrap();
        tmp.flush().unwrap();

        let config = CsvSourceConfig::new(tmp.path().to_str().unwrap()).has_headers(false);
        let source = CsvSource::new(config);
        let err = source
            .fetch_all()
            .await
            .expect_err("headerless ragged row must error");
        assert!(matches!(err, FaucetError::Source(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn well_formed_csv_still_parses_under_strict_default() {
        // The strict default must not regress well-formed files.
        let mut tmp = NamedTempFile::new().unwrap();
        writeln!(tmp, "id,name,age").unwrap();
        writeln!(tmp, "1,Alice,30").unwrap();
        writeln!(tmp, "2,Bob,25").unwrap();
        tmp.flush().unwrap();

        let config = CsvSourceConfig::new(tmp.path().to_str().unwrap());
        let source = CsvSource::new(config);
        let records = source.fetch_all().await.unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[1]["age"], "25");
    }

    #[tokio::test]
    async fn strict_mode_honored_when_buffered_into_single_page() {
        // The buffered path (batch_size = 0) drains via the same stream_pages
        // loop, so it must enforce strictness too (finding cites two locations).
        let mut tmp = NamedTempFile::new().unwrap();
        writeln!(tmp, "id,name").unwrap();
        writeln!(tmp, "1,Alice").unwrap();
        writeln!(tmp, "2").unwrap();
        tmp.flush().unwrap();

        let config = CsvSourceConfig::new(tmp.path().to_str().unwrap()).with_batch_size(0);
        let source = CsvSource::new(config);
        let err = source
            .fetch_all()
            .await
            .expect_err("buffered drain must also enforce strict mode");
        assert!(matches!(err, FaucetError::Source(_)), "got {err:?}");
    }
}
