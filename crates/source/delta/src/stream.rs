//! Delta Lake source stream executor.
//!
//! Reads a Delta table's active data files at the latest version (or a pinned
//! `version` / `timestamp`) and yields each row as a `serde_json::Value`
//! object. No datafusion: the active file set comes from the Delta log
//! (`get_active_add_actions_by_partitions`) and each parquet file is streamed through the async
//! Arrow reader faucet's Parquet source uses. Partition-column values (which
//! live in the Hive-style path, not the file) are reconstructed and merged
//! back into every row, typed against the table schema.

use std::collections::HashMap;
use std::pin::Pin;

use arrow::datatypes::{DataType, SchemaRef};
use async_trait::async_trait;
use faucet_common_delta::convert::record_batch_to_json;
use faucet_core::{FaucetError, Stream, StreamPage};
use futures::StreamExt;
use object_store::path::Path as ObjPath;
use parquet::arrow::ProjectionMask;
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};
use serde_json::Value;

use crate::config::DeltaSourceConfig;

/// A source that reads an Apache Delta Lake table into JSON records.
pub struct DeltaSource {
    config: DeltaSourceConfig,
}

/// One active data file plus the partition values encoded in its path.
struct DataFile {
    path: ObjPath,
    /// `col -> JSON value` for every partition column, typed against the table
    /// schema. `null` for the Hive default-partition sentinel.
    partitions: HashMap<String, Value>,
}

impl DeltaSource {
    /// Build a new Delta source. Validates config eagerly; the table is opened
    /// on each read so time-travel/version pins re-resolve.
    pub async fn new(config: DeltaSourceConfig) -> Result<Self, FaucetError> {
        config
            .validate()
            .map_err(|e| FaucetError::Config(format!("invalid delta source config: {e}")))?;
        config.connection.register_handlers();
        Ok(Self { config })
    }

    /// Open the table at the configured version / timestamp / latest.
    async fn open(&self) -> Result<deltalake::DeltaTable, FaucetError> {
        match (self.config.version, &self.config.timestamp) {
            (Some(v), _) => self.config.connection.open_at_version(v).await,
            (None, Some(ts)) => self.config.connection.open_at_timestamp(ts).await,
            (None, None) => self.config.connection.open().await,
        }
    }

    /// Resolve the active files + their partition values, and the table's Arrow
    /// schema (used to type partition values and validate projection).
    async fn resolve(
        &self,
        table: &deltalake::DeltaTable,
    ) -> Result<(Vec<DataFile>, SchemaRef, Vec<String>), FaucetError> {
        let state = table
            .snapshot()
            .map_err(|e| FaucetError::Source(format!("delta: table has no snapshot: {e}")))?;
        let arrow_schema =
            faucet_common_delta::arrow_bridge::schema_from_delta(&state.snapshot().arrow_schema())?;
        let partition_cols = state.metadata().partition_columns().to_vec();

        let views: Vec<_> =
            futures::TryStreamExt::try_collect(table.get_active_add_actions_by_partitions(&[]))
                .await
                .map_err(|e| {
                    FaucetError::Source(format!("delta: could not list table files: {e}"))
                })?;
        refuse_deletion_vectors(
            views
                .iter()
                .map(|v| (v.path(), v.deletion_vector_descriptor().is_some())),
        )?;
        let paths: Vec<ObjPath> = views.iter().map(|v| v.object_store_path()).collect();

        let files = paths
            .into_iter()
            .map(|path| {
                let partitions =
                    parse_partition_values(path.as_ref(), &partition_cols, &arrow_schema);
                DataFile { path, partitions }
            })
            .collect();
        Ok((files, arrow_schema, partition_cols))
    }

    /// The projection over the *data* file columns: the requested columns minus
    /// any partition columns (which are not stored in the file). `None` (read
    /// all file columns) when no projection is configured.
    fn data_projection(&self, partition_cols: &[String]) -> Option<Vec<String>> {
        if self.config.columns.is_empty() {
            return None;
        }
        Some(
            self.config
                .columns
                .iter()
                .filter(|c| !partition_cols.contains(c))
                .cloned()
                .collect(),
        )
    }
}

/// Refuse a table whose active files carry deletion vectors: reading such a
/// file whole would return rows the table has deleted or superseded.
fn refuse_deletion_vectors<P: std::fmt::Display>(
    files: impl Iterator<Item = (P, bool)>,
) -> Result<(), FaucetError> {
    let marked: Vec<String> = files
        .filter(|(_, dv)| *dv)
        .map(|(p, _)| p.to_string())
        .collect();
    match marked.first() {
        None => Ok(()),
        Some(first) => Err(FaucetError::Source(format!(
            "delta: {} active file(s) carry deletion vectors (first: {first}), which this \
             source cannot apply; reading them would return deleted rows. Purge them first \
             (`REORG TABLE … APPLY (PURGE)`) or disable `delta.enableDeletionVectors` on \
             the table",
            marked.len()
        ))),
    }
}

#[async_trait]
impl faucet_core::Source for DeltaSource {
    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(DeltaSourceConfig))
            .expect("schema serialization")
    }

    fn connector_name(&self) -> &'static str {
        "delta"
    }

    fn dataset_uri(&self) -> String {
        self.config.connection.redacted_uri()
    }

    async fn check(
        &self,
        ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::{CheckReport, Probe};
        let started = std::time::Instant::now();
        // Metadata-only open (no data scan). The source needs the table to
        // exist, so an absent table fails the probe.
        let probe =
            match tokio::time::timeout(ctx.timeout, self.config.connection.open_optional()).await {
                Ok(Ok(Some(_))) => Probe::pass("table", started.elapsed()),
                Ok(Ok(None)) => Probe::fail_hint(
                    "table",
                    started.elapsed(),
                    format!(
                        "delta source: no Delta table at '{}'",
                        self.config.connection.redacted_uri()
                    ),
                    "Verify table_uri points at an existing Delta table.",
                ),
                Ok(Err(e)) => Probe::fail_hint(
                    "table",
                    started.elapsed(),
                    format!("delta source probe failed: {e}"),
                    "Verify table_uri, credentials, and object-store reachability.",
                ),
                Err(_) => Probe::fail_hint(
                    "table",
                    started.elapsed(),
                    format!("delta source probe timed out after {:?}", ctx.timeout),
                    "Check object-store network reachability.",
                ),
            };
        Ok(CheckReport::single(probe))
    }

    async fn fetch_with_context(
        &self,
        _context: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        let mut out = Vec::new();
        let mut stream = self.stream_pages(_context, self.config.batch_size);
        while let Some(page) = stream.next().await {
            out.extend(page?.records);
        }
        Ok(out)
    }

    fn stream_pages<'a>(
        &'a self,
        _context: &'a HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        Box::pin(async_stream::try_stream! {
            let table = self.open().await?;
            let (files, _schema, partition_cols) = self.resolve(&table).await?;
            let store = table.object_store();
            let data_projection = self.data_projection(&partition_cols);
            let requested: Option<&[String]> =
                if self.config.columns.is_empty() { None } else { Some(&self.config.columns) };

            tracing::info!(
                files = files.len(),
                uri = %self.config.connection.redacted_uri(),
                "delta source resolved active files",
            );

            for file in &files {
                let reader = ParquetObjectReader::new(store.clone(), file.path.clone());
                let mut builder = ParquetRecordBatchStreamBuilder::new(reader).await.map_err(|e| {
                    FaucetError::Source(format!(
                        "delta: could not open data file '{}': {e}",
                        file.path
                    ))
                })?;

                if self.config.batch_size > 0 {
                    builder = builder.with_batch_size(self.config.batch_size);
                }
                if let Some(cols) = &data_projection {
                    // Only project columns actually present in this file. A
                    // requested column that is neither a data column here nor a
                    // partition column is genuinely absent → surface it.
                    let pq = builder.parquet_schema();
                    let present: Vec<&str> = cols
                        .iter()
                        .filter(|c| pq.columns().iter().any(|col| col.name() == c.as_str()))
                        .map(String::as_str)
                        .collect();
                    let mask = ProjectionMask::columns(pq, present.iter().copied());
                    builder = builder.with_projection(mask);
                }

                let mut batches = builder.build().map_err(|e| {
                    FaucetError::Source(format!(
                        "delta: could not build reader for '{}': {e}",
                        file.path
                    ))
                })?;

                while let Some(batch) = batches.next().await {
                    let batch = batch.map_err(|e| {
                        FaucetError::Source(format!("delta: read error in '{}': {e}", file.path))
                    })?;
                    let mut rows = record_batch_to_json(&batch)?;
                    if !rows.is_empty() {
                        for row in &mut rows {
                            merge_partitions(row, &file.partitions, requested);
                        }
                        yield StreamPage { records: rows, bookmark: None };
                    }
                }
            }
        })
    }

    /// Delta reads are natively Arrow (each data file is a parquet stream), so
    /// the source participates in the opt-in columnar fast path (#375): a
    /// `delta → parquet` / `delta → delta` chain never materializes `Value`.
    #[cfg(feature = "arrow")]
    fn supports_columnar(&self) -> bool {
        true
    }

    /// Stream the table as Arrow [`ColumnarPage`](faucet_core::ColumnarPage)s.
    /// Mirrors `stream_pages` but yields each parquet
    /// `RecordBatch` directly; Hive partition-column values (which live in the
    /// file path, not the parquet data) are appended as constant Arrow columns
    /// so the columnar output matches the row-wise output field-for-field.
    #[cfg(feature = "arrow")]
    fn stream_batches<'a>(
        &'a self,
        _context: &'a HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<faucet_core::ColumnarPage, FaucetError>> + Send + 'a>>
    {
        Box::pin(async_stream::try_stream! {
            let table = self.open().await?;
            let (files, schema, partition_cols) = self.resolve(&table).await?;
            let store = table.object_store();
            let data_projection = self.data_projection(&partition_cols);
            let requested: Option<&[String]> =
                if self.config.columns.is_empty() { None } else { Some(&self.config.columns) };

            for file in &files {
                let reader = ParquetObjectReader::new(store.clone(), file.path.clone());
                let mut builder = ParquetRecordBatchStreamBuilder::new(reader).await.map_err(|e| {
                    FaucetError::Source(format!("delta: could not open data file '{}': {e}", file.path))
                })?;
                if self.config.batch_size > 0 {
                    builder = builder.with_batch_size(self.config.batch_size);
                }
                if let Some(cols) = &data_projection {
                    let pq = builder.parquet_schema();
                    let present: Vec<&str> = cols
                        .iter()
                        .filter(|c| pq.columns().iter().any(|col| col.name() == c.as_str()))
                        .map(String::as_str)
                        .collect();
                    let mask = ProjectionMask::columns(pq, present.iter().copied());
                    builder = builder.with_projection(mask);
                }
                let mut batches = builder.build().map_err(|e| {
                    FaucetError::Source(format!("delta: could not build reader for '{}': {e}", file.path))
                })?;
                while let Some(batch) = batches.next().await {
                    let batch = batch.map_err(|e| {
                        FaucetError::Source(format!("delta: read error in '{}': {e}", file.path))
                    })?;
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    let batch = append_partition_columns(batch, &file.partitions, &schema, requested)?;
                    yield faucet_core::ColumnarPage { batch, bookmark: None };
                }
            }
        })
    }
}

/// Append Hive partition columns to a data-file `RecordBatch` as constant
/// columns, honoring the same `requested`-projection semantics as
/// [`merge_partitions`] (add a partition column only when unprojected-away, and
/// never shadow a real data column of the same name). Each constant column is
/// built through the core `Value → RecordBatch` shim with the table's declared
/// Arrow type, so a partition value round-trips identically to the row path.
#[cfg(feature = "arrow")]
fn append_partition_columns(
    batch: arrow::array::RecordBatch,
    partitions: &HashMap<String, Value>,
    table_schema: &SchemaRef,
    requested: Option<&[String]>,
) -> Result<arrow::array::RecordBatch, FaucetError> {
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    if partitions.is_empty() {
        return Ok(batch);
    }
    let in_schema = batch.schema();
    let n = batch.num_rows();
    let mut fields: Vec<Arc<Field>> = in_schema.fields().iter().cloned().collect();
    let mut columns = batch.columns().to_vec();

    // Deterministic order so the output schema is stable run-to-run.
    let mut keys: Vec<&String> = partitions.keys().collect();
    keys.sort();
    for k in keys {
        if let Some(cols) = requested
            && !cols.iter().any(|c| c == k)
        {
            continue;
        }
        if in_schema.field_with_name(k).is_ok() {
            continue; // a real data column of this name wins (merge_partitions)
        }
        let field = table_schema
            .field_with_name(k)
            .cloned()
            .unwrap_or_else(|_| Field::new(k, arrow::datatypes::DataType::Utf8, true));
        let one_schema = Arc::new(Schema::new(vec![field.clone().with_nullable(true)]));
        let mut obj = serde_json::Map::new();
        obj.insert(k.clone(), partitions[k].clone());
        let rows = vec![Value::Object(obj); n];
        let col_batch = faucet_core::values_to_record_batch(&rows, one_schema)?;
        fields.push(Arc::new(field));
        columns.push(col_batch.column(0).clone());
    }

    arrow::array::RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|e| FaucetError::Source(format!("delta: assembling columnar batch failed: {e}")))
}

/// Parse Hive-style `col=value` segments out of a data file path, typing each
/// value against the table's Arrow schema. Only the declared partition columns
/// are extracted; unknown segments are ignored.
fn parse_partition_values(
    path: &str,
    partition_cols: &[String],
    schema: &SchemaRef,
) -> HashMap<String, Value> {
    let mut out = HashMap::new();
    if partition_cols.is_empty() {
        return out;
    }
    for segment in path.split('/') {
        if let Some((k, v)) = segment.split_once('=')
            && partition_cols.iter().any(|c| c == k)
        {
            let decoded = percent_decode(v);
            let dt = schema
                .field_with_name(k)
                .ok()
                .map(|f| f.data_type().clone())
                .unwrap_or(DataType::Utf8);
            out.insert(k.to_string(), coerce_partition_value(&decoded, &dt));
        }
    }
    out
}

/// The Delta Hive-default-partition sentinel — represents a NULL partition
/// value.
const HIVE_NULL: &str = "__HIVE_DEFAULT_PARTITION__";

/// Coerce a string partition value to JSON, typed by the column's Arrow type.
fn coerce_partition_value(raw: &str, dt: &DataType) -> Value {
    if raw == HIVE_NULL || raw.is_empty() {
        return Value::Null;
    }
    match dt {
        DataType::Boolean => match raw {
            "true" => Value::Bool(true),
            "false" => Value::Bool(false),
            _ => Value::String(raw.to_string()),
        },
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => raw
            .parse::<i64>()
            .map(|n| Value::Number(n.into()))
            .unwrap_or_else(|_| Value::String(raw.to_string())),
        DataType::Float32 | DataType::Float64 => {
            serde_json::Number::from_f64(raw.parse::<f64>().unwrap_or(f64::NAN))
                .map(Value::Number)
                .unwrap_or_else(|| Value::String(raw.to_string()))
        }
        // Dates/timestamps/strings/decimals: keep the logical string form.
        _ => Value::String(raw.to_string()),
    }
}

/// Merge partition values into a data row, then narrow to `requested` columns
/// (when a projection is configured). Partition values fill keys not present in
/// the data (the file never stores them).
fn merge_partitions(
    row: &mut Value,
    partitions: &HashMap<String, Value>,
    requested: Option<&[String]>,
) {
    if let Value::Object(map) = row {
        for (k, v) in partitions {
            match requested {
                Some(cols) if !cols.iter().any(|c| c == k) => continue,
                _ => {
                    map.entry(k.clone()).or_insert_with(|| v.clone());
                }
            }
        }
        if let Some(cols) = requested {
            map.retain(|k, _| cols.iter().any(|c| c == k));
        }
    }
}

/// Minimal `%XX` percent-decoder for Hive-encoded partition path segments.
/// Leaves malformed escapes untouched.
fn percent_decode(s: &str) -> String {
    if !s.contains('%') {
        return s.to_string();
    }
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hi = (bytes[i + 1] as char).to_digit(16);
            let lo = (bytes[i + 2] as char).to_digit(16);
            if let (Some(h), Some(l)) = (hi, lo) {
                out.push((h * 16 + l) as u8);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, Schema};
    use serde_json::json;
    use std::sync::Arc;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("dt", DataType::Utf8, true),
            Field::new("region", DataType::Utf8, true),
            Field::new("part", DataType::Int64, true),
        ]))
    }

    #[test]
    fn parses_typed_partition_values() {
        let s = schema();
        let cols = vec!["dt".to_string(), "part".to_string()];
        let m = parse_partition_values("t/dt=2026-01-01/part=7/file.parquet", &cols, &s);
        assert_eq!(m["dt"], json!("2026-01-01"));
        assert_eq!(m["part"], json!(7));
    }

    #[test]
    fn hive_null_becomes_json_null() {
        let s = schema();
        let cols = vec!["region".to_string()];
        let m = parse_partition_values("t/region=__HIVE_DEFAULT_PARTITION__/f.parquet", &cols, &s);
        assert_eq!(m["region"], Value::Null);
    }

    #[test]
    fn percent_decoding_of_partition_values() {
        let s = schema();
        let cols = vec!["region".to_string()];
        let m = parse_partition_values("t/region=a%2Fb/f.parquet", &cols, &s);
        assert_eq!(m["region"], json!("a/b"));
    }

    #[test]
    fn no_partition_columns_is_empty() {
        let s = schema();
        assert!(parse_partition_values("t/f.parquet", &[], &s).is_empty());
    }

    #[test]
    fn merge_injects_and_projects() {
        let mut row = json!({"id": 1});
        let mut parts = HashMap::new();
        parts.insert("dt".to_string(), json!("2026-01-01"));
        merge_partitions(&mut row, &parts, None);
        assert_eq!(row["dt"], json!("2026-01-01"));
        assert_eq!(row["id"], json!(1));

        // With projection, only requested keys survive.
        let mut row2 = json!({"id": 1, "name": "x"});
        let cols = vec!["id".to_string(), "dt".to_string()];
        merge_partitions(&mut row2, &parts, Some(&cols));
        assert_eq!(row2["id"], json!(1));
        assert_eq!(row2["dt"], json!("2026-01-01"));
        assert!(row2.get("name").is_none());
    }

    #[test]
    fn coerce_bool_and_float() {
        assert_eq!(
            coerce_partition_value("true", &DataType::Boolean),
            json!(true)
        );
        assert_eq!(
            coerce_partition_value("1.5", &DataType::Float64),
            json!(1.5)
        );
        assert_eq!(coerce_partition_value("x", &DataType::Int64), json!("x"));
        // Non-parseable values for bool/float columns fall back to a string.
        assert_eq!(
            coerce_partition_value("maybe", &DataType::Boolean),
            json!("maybe")
        );
        assert_eq!(
            coerce_partition_value("nan-ish", &DataType::Float32),
            json!("nan-ish")
        );
        // Empty and the Hive sentinel both become JSON null.
        assert_eq!(coerce_partition_value("", &DataType::Utf8), Value::Null);
        assert_eq!(
            coerce_partition_value(HIVE_NULL, &DataType::Int64),
            Value::Null
        );
        // A date column keeps the logical string form.
        assert_eq!(
            coerce_partition_value("2026-01-01", &DataType::Date32),
            json!("2026-01-01")
        );
    }

    #[tokio::test]
    async fn source_trait_metadata_methods() {
        use faucet_core::Source;
        let src = DeltaSource::new(DeltaSourceConfig::new("file:///tmp/delta_src_meta"))
            .await
            .unwrap();
        assert_eq!(src.connector_name(), "delta");
        assert_eq!(src.dataset_uri(), "file:///tmp/delta_src_meta");
        assert!(src.config_schema().is_object());
    }

    #[tokio::test]
    async fn fetch_missing_table_errors() {
        use faucet_core::Source;
        let dir = tempfile::tempdir().unwrap();
        let uri = dir
            .path()
            .join("no_such_table")
            .to_string_lossy()
            .into_owned();
        let src = DeltaSource::new(DeltaSourceConfig::new(&uri))
            .await
            .unwrap();
        // `open()` fails (not a Delta table) → mapped to FaucetError::Source.
        let err = src.fetch_with_context(&HashMap::new()).await.unwrap_err();
        assert!(matches!(err, FaucetError::Source(_)), "{err}");
    }
}
