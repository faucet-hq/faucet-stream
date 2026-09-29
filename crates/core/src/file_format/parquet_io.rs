//! Apache Parquet for object-store connectors (#777).
//!
//! The option types are always compiled so a connector config can carry a
//! `parquet:` block without cfg-gating it. The encoder and the read-side
//! projection helpers need the `file-format-parquet` feature.
//!
//! **Writing.** [`ParquetObjects`] turns pages (records or Arrow batches) into
//! complete, self-contained Parquet objects in memory: the footer is written
//! before an object is handed back, so an upload of the returned bytes is
//! atomic. The schema is inferred from the object's first page (every field
//! nullable) and widened by later pages — a new column is added (null in the
//! rows already written) and an integer column that later holds fractions
//! becomes a float. A field that changes to an incompatible type is an error
//! naming the field. With an explicit schema nothing is inferred; fields
//! outside it follow [`ParquetUnknownField`].
//!
//! **Reading.** [`ParquetReadOptions::columns`] projects top-level columns
//! before any row group is decoded.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Default rows per row group — the Arrow writer's own default.
pub const DEFAULT_ROW_GROUP_SIZE: usize = 1024 * 1024;

/// Parquet column compression codec.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ParquetCompression {
    /// No compression.
    Uncompressed,
    /// Snappy.
    Snappy,
    /// Gzip at the default level.
    Gzip,
    /// Zstandard at the default level (the default).
    #[default]
    Zstd,
    /// LZ4 (raw framing).
    Lz4,
}

/// What to do with a record field that the object's schema does not hold.
///
/// Only reachable with an explicit schema, or an inferred one limited by
/// `sample_size`: a fully inferred schema widens to every field it sees.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ParquetUnknownField {
    /// Drop the field and log a warning once per field per object.
    #[default]
    Warn,
    /// Fail the write, naming the field.
    Error,
}

/// A column type in an explicit Parquet schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ParquetFieldType {
    /// Boolean.
    Boolean,
    /// 32-bit signed integer.
    Int32,
    /// 64-bit signed integer.
    Int64,
    /// 32-bit float.
    Float32,
    /// 64-bit float.
    Float64,
    /// UTF-8 string.
    String,
    /// Calendar date, from `YYYY-MM-DD` strings.
    Date,
    /// Microsecond UTC timestamp, from RFC 3339 strings.
    Timestamp,
}

/// One column of an explicit Parquet schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ParquetField {
    /// Column name (a top-level record field).
    pub name: String,
    /// Column type.
    #[serde(rename = "type")]
    pub data_type: ParquetFieldType,
    /// Whether the column may be null (default `true`). A record missing a
    /// non-nullable column fails the write.
    #[serde(default = "default_true")]
    pub nullable: bool,
}

fn default_true() -> bool {
    true
}

/// Where an object's Parquet schema comes from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ParquetSchema {
    /// Inferred from the records of each object, widened page by page.
    Inferred {
        /// Infer from only the first N records of each page; fields that first
        /// appear later follow `on_unknown_field`. Unset: every record.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        sample_size: Option<usize>,
    },
    /// Declared up front; every object has exactly these columns.
    Explicit {
        /// The columns, in order.
        fields: Vec<ParquetField>,
    },
}

/// Parquet writer options (the `parquet:` block of an object-store sink).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ParquetWriteOptions {
    /// Column compression codec (default `zstd`).
    #[serde(default)]
    pub compression: ParquetCompression,
    /// Maximum rows per row group (default 1048576).
    #[serde(default = "default_row_group_size")]
    pub row_group_size: usize,
    /// Schema source (default: inferred from every record).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<ParquetSchema>,
    /// Policy for record fields outside the schema (default `warn`).
    #[serde(default)]
    pub on_unknown_field: ParquetUnknownField,
}

fn default_row_group_size() -> usize {
    DEFAULT_ROW_GROUP_SIZE
}

impl Default for ParquetWriteOptions {
    fn default() -> Self {
        Self {
            compression: ParquetCompression::default(),
            row_group_size: DEFAULT_ROW_GROUP_SIZE,
            schema: None,
            on_unknown_field: ParquetUnknownField::default(),
        }
    }
}

impl ParquetWriteOptions {
    /// Check the options, naming the offending field.
    pub fn validate(&self) -> Result<(), crate::FaucetError> {
        let bad = |m: &str| Err(crate::FaucetError::Config(format!("parquet: {m}")));
        if self.row_group_size == 0 {
            return bad("`row_group_size` must be greater than 0");
        }
        match &self.schema {
            Some(ParquetSchema::Inferred {
                sample_size: Some(0),
            }) => bad("`schema.sample_size` must be greater than 0"),
            Some(ParquetSchema::Explicit { fields }) => {
                if fields.is_empty() {
                    return bad("an explicit `schema` needs at least one field");
                }
                let mut seen = std::collections::HashSet::new();
                for f in fields {
                    if f.name.is_empty() {
                        return bad("an explicit schema field has an empty `name`");
                    }
                    if !seen.insert(f.name.as_str()) {
                        return bad(&format!("explicit schema field `{}` is repeated", f.name));
                    }
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

/// Parquet reader options (the `parquet:` block of an object-store source).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ParquetReadOptions {
    /// Decode only these top-level columns. A name missing from an object is
    /// an error naming the object and its columns. Unset: every column.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub columns: Option<Vec<String>>,
}

#[cfg(feature = "file-format-parquet")]
pub use imp::{ParquetObjects, projection_mask, read_bytes};

#[cfg(feature = "file-format-parquet")]
mod imp {
    use super::{
        ParquetCompression, ParquetFieldType, ParquetReadOptions, ParquetSchema,
        ParquetUnknownField, ParquetWriteOptions,
    };
    use crate::FaucetError;
    use ::parquet::arrow::ArrowWriter;
    use ::parquet::arrow::ProjectionMask;
    use ::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use ::parquet::file::properties::WriterProperties;
    use ::parquet::schema::types::SchemaDescriptor;
    use arrow::array::{Array, ArrayRef, RecordBatch, StructArray, new_null_array};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
    use serde_json::Value;
    use std::collections::BTreeSet;
    use std::sync::Arc;

    fn sink_err(m: impl std::fmt::Display) -> FaucetError {
        FaucetError::Sink(format!("parquet: {m}"))
    }

    impl ParquetCompression {
        /// The Parquet writer codec.
        pub fn codec(self) -> ::parquet::basic::Compression {
            use ::parquet::basic::{Compression as C, GzipLevel, ZstdLevel};
            match self {
                Self::Uncompressed => C::UNCOMPRESSED,
                Self::Snappy => C::SNAPPY,
                Self::Gzip => C::GZIP(GzipLevel::default()),
                Self::Zstd => C::ZSTD(ZstdLevel::default()),
                Self::Lz4 => C::LZ4_RAW,
            }
        }
    }

    impl ParquetFieldType {
        /// The Arrow type a column of this kind is written as.
        pub fn arrow_type(self) -> DataType {
            match self {
                Self::Boolean => DataType::Boolean,
                Self::Int32 => DataType::Int32,
                Self::Int64 => DataType::Int64,
                Self::Float32 => DataType::Float32,
                Self::Float64 => DataType::Float64,
                Self::String => DataType::Utf8,
                Self::Date => DataType::Date32,
                Self::Timestamp => {
                    DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into()))
                }
            }
        }
    }

    impl ParquetWriteOptions {
        /// Writer properties for the configured codec and row-group size.
        pub fn writer_properties(&self) -> WriterProperties {
            WriterProperties::builder()
                .set_compression(self.compression.codec())
                .set_max_row_group_row_count(Some(self.row_group_size))
                .build()
        }

        /// The declared schema, when `schema.type` is `explicit`.
        pub fn explicit_schema(&self) -> Option<SchemaRef> {
            match &self.schema {
                Some(ParquetSchema::Explicit { fields }) => Some(Arc::new(Schema::new(
                    fields
                        .iter()
                        .map(|f| Field::new(&f.name, f.data_type.arrow_type(), f.nullable))
                        .collect::<Vec<_>>(),
                ))),
                _ => None,
            }
        }

        fn sample_size(&self) -> Option<usize> {
            match &self.schema {
                Some(ParquetSchema::Inferred { sample_size }) => *sample_size,
                _ => None,
            }
        }
    }

    /// Pages in, complete Parquet objects out.
    ///
    /// Objects close on the row cap, the encoded-byte cap, or [`finish`](Self::finish);
    /// each finished object waits in [`take_ready`](Self::take_ready) until the
    /// caller uploads it. An error leaves every object finished before it
    /// ready, and the rows written before it in the open object.
    pub struct ParquetObjects {
        options: ParquetWriteOptions,
        explicit: Option<SchemaRef>,
        max_rows: Option<usize>,
        max_bytes: Option<usize>,
        schema: Option<SchemaRef>,
        writer: Option<ArrowWriter<Vec<u8>>>,
        rows: usize,
        warned: BTreeSet<String>,
        ready: Vec<Vec<u8>>,
    }

    impl std::fmt::Debug for ParquetObjects {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("ParquetObjects")
                .field("rows", &self.rows)
                .field("ready", &self.ready.len())
                .finish_non_exhaustive()
        }
    }

    impl ParquetObjects {
        /// A writer rolling objects at `max_rows` rows or `max_bytes` encoded
        /// bytes (either `None` or `0`: no cap).
        pub fn new(
            options: &ParquetWriteOptions,
            max_rows: Option<usize>,
            max_bytes: Option<usize>,
        ) -> Result<Self, FaucetError> {
            options.validate()?;
            Ok(Self {
                explicit: options.explicit_schema(),
                options: options.clone(),
                max_rows: max_rows.filter(|n| *n > 0),
                max_bytes: max_bytes.filter(|n| *n > 0),
                schema: None,
                writer: None,
                rows: 0,
                warned: BTreeSet::new(),
                ready: Vec::new(),
            })
        }

        /// Rows in the open object.
        pub fn rows(&self) -> usize {
            self.rows
        }

        /// Encoded bytes of the open object so far (flushed row groups plus the
        /// buffered one).
        pub fn bytes(&self) -> usize {
            self.writer
                .as_ref()
                .map_or(0, |w| w.bytes_written() + w.in_progress_size())
        }

        /// The open object's schema, once a page has been written.
        pub fn schema(&self) -> Option<&SchemaRef> {
            self.schema.as_ref()
        }

        /// Objects finished and not yet taken.
        pub fn take_ready(&mut self) -> Vec<Vec<u8>> {
            std::mem::take(&mut self.ready)
        }

        /// Write `records` (JSON objects), rolling objects at the caps.
        pub fn push_records(&mut self, records: &[Value]) -> Result<(), FaucetError> {
            let mut offset = 0;
            while offset < records.len() {
                let take = self.capacity().min(records.len() - offset);
                let slice = &records[offset..offset + take];
                let batch = self.records_to_batch(slice)?;
                self.write(&batch)?;
                offset += take;
                self.roll_if_full()?;
            }
            Ok(())
        }

        /// Write an Arrow batch, rolling objects at the caps.
        pub fn push_batch(&mut self, batch: &RecordBatch) -> Result<(), FaucetError> {
            let n = batch.num_rows();
            let mut offset = 0;
            while offset < n {
                let take = self.capacity().min(n - offset);
                let slice = batch.slice(offset, take);
                let slice = match self.explicit.clone() {
                    Some(target) => self.conform(&slice, &target)?,
                    None => slice,
                };
                self.write(&slice)?;
                offset += take;
                self.roll_if_full()?;
            }
            Ok(())
        }

        /// Close the open object (if any rows were written) into the ready list.
        pub fn finish(&mut self) -> Result<(), FaucetError> {
            if let Some(writer) = self.writer.take() {
                let bytes = writer.into_inner().map_err(sink_err)?;
                self.ready.push(bytes);
            }
            self.schema = None;
            self.rows = 0;
            self.warned.clear();
            Ok(())
        }

        fn capacity(&self) -> usize {
            self.max_rows
                .map_or(usize::MAX, |m| m.saturating_sub(self.rows).max(1))
        }

        fn roll_if_full(&mut self) -> Result<(), FaucetError> {
            let rows_full = self.max_rows.is_some_and(|m| self.rows >= m);
            let bytes_full = self.max_bytes.is_some_and(|m| self.bytes() >= m);
            if rows_full || bytes_full {
                self.finish()?;
            }
            Ok(())
        }
    }

    impl ParquetObjects {
        fn records_to_batch(&mut self, records: &[Value]) -> Result<RecordBatch, FaucetError> {
            let target = match self.explicit.clone() {
                Some(schema) => schema,
                None => {
                    let n = self
                        .options
                        .sample_size()
                        .map_or(records.len(), |s| s.min(records.len()));
                    let inferred =
                        crate::columnar::infer_arrow_schema(&records[..n]).map_err(sink_err)?;
                    match &self.schema {
                        Some(current) => merge(current, &inferred)?,
                        None => nullable(&inferred),
                    }
                }
            };
            let unknown: BTreeSet<&str> = records
                .iter()
                .filter_map(Value::as_object)
                .flat_map(|m| m.keys())
                .filter(|k| target.field_with_name(k).is_err())
                .map(String::as_str)
                .collect();
            self.unknown_fields(unknown.into_iter())?;
            crate::columnar::values_to_record_batch(records, target).map_err(|e| {
                sink_err(format!(
                    "a record does not fit the object's schema — a field holds a value of \
                     another type than earlier records (cast it with a `cast` transform): {e}"
                ))
            })
        }

        fn unknown_fields<'a>(
            &mut self,
            names: impl Iterator<Item = &'a str>,
        ) -> Result<(), FaucetError> {
            for name in names {
                match self.options.on_unknown_field {
                    ParquetUnknownField::Error => {
                        return Err(sink_err(format!(
                            "field `{name}` is not in the schema (on_unknown_field: error)"
                        )));
                    }
                    ParquetUnknownField::Warn => {
                        if self.warned.insert(name.to_string()) {
                            tracing::warn!(
                                field = name,
                                "parquet: dropping a field that is not in the object's schema"
                            );
                        }
                    }
                }
            }
            Ok(())
        }

        fn conform(
            &mut self,
            batch: &RecordBatch,
            target: &SchemaRef,
        ) -> Result<RecordBatch, FaucetError> {
            let schema = batch.schema();
            let unknown: Vec<String> = schema
                .fields()
                .iter()
                .filter(|f| target.field_with_name(f.name()).is_err())
                .map(|f| f.name().clone())
                .collect();
            self.unknown_fields(unknown.iter().map(String::as_str))?;
            project(batch, target)
        }

        fn write(&mut self, batch: &RecordBatch) -> Result<(), FaucetError> {
            let target = match (&self.explicit, &self.schema) {
                (Some(explicit), _) => explicit.clone(),
                (None, Some(current)) => merge(current, batch.schema_ref())?,
                (None, None) => nullable(batch.schema_ref()),
            };
            if target.fields().is_empty() {
                return Err(sink_err("a page has no fields to write as columns"));
            }
            let batch = project(batch, &target)?;
            let widened = self
                .schema
                .as_ref()
                .is_some_and(|s| s.fields() != target.fields());
            if self.writer.is_none() {
                self.writer = Some(self.open(&target)?);
            } else if widened {
                self.rewrite(&target)?;
            }
            self.writer
                .as_mut()
                .ok_or_else(|| sink_err("no open writer"))?
                .write(&batch)
                .map_err(sink_err)?;
            self.rows += batch.num_rows();
            self.schema = Some(target);
            Ok(())
        }

        fn open(&self, schema: &SchemaRef) -> Result<ArrowWriter<Vec<u8>>, FaucetError> {
            ArrowWriter::try_new(
                Vec::new(),
                schema.clone(),
                Some(self.options.writer_properties()),
            )
            .map_err(sink_err)
        }

        fn rewrite(&mut self, target: &SchemaRef) -> Result<(), FaucetError> {
            let Some(old) = self.writer.take() else {
                return Ok(());
            };
            let bytes = old.into_inner().map_err(sink_err)?;
            let mut writer = self.open(target)?;
            let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
                .and_then(|b| b.build())
                .map_err(sink_err)?;
            for batch in reader {
                let batch = batch.map_err(sink_err)?;
                writer.write(&project(&batch, target)?).map_err(sink_err)?;
            }
            self.writer = Some(writer);
            Ok(())
        }
    }

    /// `current` widened by `incoming`: new fields appended, integer columns
    /// that meet a float become float, null-typed columns take the other type.
    fn merge(current: &SchemaRef, incoming: &Schema) -> Result<SchemaRef, FaucetError> {
        let mut fields: Vec<Field> = current
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        for f in incoming.fields() {
            match fields.iter().position(|c| c.name() == f.name()) {
                Some(i) => {
                    let dt = merge_type(f.name(), fields[i].data_type(), f.data_type())?;
                    fields[i] = Field::new(f.name(), dt, true);
                }
                None => fields.push(Field::new(f.name(), f.data_type().clone(), true)),
            }
        }
        Ok(Arc::new(Schema::new(fields)))
    }

    fn merge_type(name: &str, a: &DataType, b: &DataType) -> Result<DataType, FaucetError> {
        use DataType as D;
        let int = |t: &D| matches!(t, D::Int8 | D::Int16 | D::Int32 | D::Int64);
        let float = |t: &D| matches!(t, D::Float16 | D::Float32 | D::Float64);
        Ok(match (a, b) {
            _ if a == b => a.clone(),
            (D::Null, t) | (t, D::Null) => t.clone(),
            (x, y) if int(x) && int(y) => D::Int64,
            (x, y) if (int(x) || float(x)) && (int(y) || float(y)) => D::Float64,
            (D::Struct(x), D::Struct(y)) => {
                let merged = merge(&Arc::new(Schema::new(x.clone())), &Schema::new(y.clone()))?;
                D::Struct(merged.fields().clone())
            }
            (D::List(x), D::List(y)) => {
                let item = merge_type(name, x.data_type(), y.data_type())?;
                D::List(Arc::new(Field::new(x.name(), item, true)))
            }
            _ => {
                return Err(sink_err(format!(
                    "field `{name}` changed type from {a} to {b}, which one Parquet object \
                     cannot hold — cast it to one type (a `cast` transform)"
                )));
            }
        })
    }

    /// Every top-level field made nullable, so a later page may omit it.
    fn nullable(schema: &Schema) -> SchemaRef {
        Arc::new(Schema::new(
            schema
                .fields()
                .iter()
                .map(|f| f.as_ref().clone().with_nullable(true))
                .collect::<Vec<_>>(),
        ))
    }

    fn conform_array(
        name: &str,
        col: &ArrayRef,
        target: &DataType,
    ) -> Result<ArrayRef, FaucetError> {
        match (col.data_type(), target) {
            (a, b) if a == b => Ok(col.clone()),
            (DataType::Struct(_), DataType::Struct(fields)) => {
                let s = col
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(|| sink_err("struct column"))?;
                let children = fields
                    .iter()
                    .map(|f| match s.column_by_name(f.name()) {
                        Some(c) => conform_array(f.name(), c, f.data_type()),
                        None => Ok(new_null_array(f.data_type(), s.len())),
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                StructArray::try_new(fields.clone(), children, s.nulls().cloned())
                    .map(|a| Arc::new(a) as ArrayRef)
                    .map_err(sink_err)
            }
            _ => arrow::compute::cast(col, target).map_err(|e| {
                sink_err(format!(
                    "column `{name}` cannot be converted to {target}: {e}"
                ))
            }),
        }
    }

    /// `batch` reshaped to `schema`: columns by name, cast where the type
    /// widened, null where the batch lacks a nullable column.
    fn project(batch: &RecordBatch, schema: &SchemaRef) -> Result<RecordBatch, FaucetError> {
        let columns = schema
            .fields()
            .iter()
            .map(|f| match batch.column_by_name(f.name()) {
                Some(c) => conform_array(f.name(), c, f.data_type()),
                None if f.is_nullable() => Ok(new_null_array(f.data_type(), batch.num_rows())),
                None => Err(sink_err(format!(
                    "required column `{}` is missing from the page",
                    f.name()
                ))),
            })
            .collect::<Result<Vec<_>, _>>()?;
        RecordBatch::try_new(schema.clone(), columns).map_err(sink_err)
    }

    /// The projection `opts.columns` selects in an object with `schema`, or
    /// `None` to read every column. A requested name the object lacks is an
    /// error naming the object (`display`) and its columns.
    pub fn projection_mask(
        opts: &ParquetReadOptions,
        schema: &SchemaDescriptor,
        display: &str,
    ) -> Result<Option<ProjectionMask>, FaucetError> {
        let Some(columns) = opts.columns.as_deref() else {
            return Ok(None);
        };
        let roots = schema.root_schema().get_fields();
        let mut indices = Vec::with_capacity(columns.len());
        for name in columns {
            match roots.iter().position(|f| f.name() == name) {
                Some(i) => indices.push(i),
                None => {
                    let available: Vec<&str> = roots.iter().map(|f| f.name()).collect();
                    return Err(FaucetError::Source(format!(
                        "parquet: column `{name}` is not in '{display}' (columns: {})",
                        available.join(", ")
                    )));
                }
            }
        }
        Ok(Some(ProjectionMask::roots(schema, indices)))
    }

    /// Decode a whole in-memory Parquet object (projected by `opts`) into its
    /// Arrow schema and batches of at most `batch_size` rows (`0`: the
    /// reader's default).
    pub fn read_bytes(
        data: bytes::Bytes,
        opts: &ParquetReadOptions,
        batch_size: usize,
        display: &str,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), FaucetError> {
        let src = |e: &dyn std::fmt::Display| {
            FaucetError::Source(format!("parquet: failed to read '{display}': {e}"))
        };
        let mut builder = ParquetRecordBatchReaderBuilder::try_new(data).map_err(|e| src(&e))?;
        if let Some(mask) = projection_mask(opts, builder.parquet_schema(), display)? {
            builder = builder.with_projection(mask);
        }
        if batch_size > 0 {
            builder = builder.with_batch_size(batch_size);
        }
        let reader = builder.build().map_err(|e| src(&e))?;
        let schema = arrow::array::RecordBatchReader::schema(&reader);
        let batches = reader.collect::<Result<Vec<_>, _>>().map_err(|e| src(&e))?;
        Ok((schema, batches))
    }
}

#[cfg(test)]
mod option_tests {
    use super::*;

    #[test]
    fn defaults_and_serde() {
        let o: ParquetWriteOptions = serde_json::from_value(serde_json::json!({})).unwrap();
        assert_eq!(o, ParquetWriteOptions::default());
        assert_eq!(o.compression, ParquetCompression::Zstd);
        assert_eq!(o.row_group_size, DEFAULT_ROW_GROUP_SIZE);
        let o: ParquetWriteOptions = serde_json::from_value(serde_json::json!({
            "compression": "snappy", "row_group_size": 10, "on_unknown_field": "error",
            "schema": {"type": "explicit", "fields": [{"name": "id", "type": "int64", "nullable": false}]}
        }))
        .unwrap();
        assert_eq!(o.compression, ParquetCompression::Snappy);
        assert_eq!(o.on_unknown_field, ParquetUnknownField::Error);
        assert!(
            matches!(o.schema, Some(ParquetSchema::Explicit { ref fields }) if !fields[0].nullable)
        );
        let r: ParquetReadOptions =
            serde_json::from_value(serde_json::json!({"columns": ["a"]})).unwrap();
        assert_eq!(r.columns.as_deref(), Some(&["a".to_string()][..]));
        assert!(
            serde_json::from_value::<ParquetWriteOptions>(serde_json::json!({"nope": 1})).is_err()
        );
    }

    #[test]
    fn validate_names_each_problem() {
        let mut o = ParquetWriteOptions {
            row_group_size: 0,
            ..Default::default()
        };
        assert!(
            o.validate()
                .unwrap_err()
                .to_string()
                .contains("row_group_size")
        );
        o.row_group_size = 5;
        o.schema = Some(ParquetSchema::Inferred {
            sample_size: Some(0),
        });
        assert!(
            o.validate()
                .unwrap_err()
                .to_string()
                .contains("sample_size")
        );
        o.schema = Some(ParquetSchema::Explicit { fields: vec![] });
        assert!(
            o.validate()
                .unwrap_err()
                .to_string()
                .contains("at least one")
        );
        let f = |n: &str| ParquetField {
            name: n.into(),
            data_type: ParquetFieldType::Int64,
            nullable: true,
        };
        o.schema = Some(ParquetSchema::Explicit {
            fields: vec![f("")],
        });
        assert!(o.validate().unwrap_err().to_string().contains("empty"));
        o.schema = Some(ParquetSchema::Explicit {
            fields: vec![f("a"), f("a")],
        });
        assert!(o.validate().unwrap_err().to_string().contains("repeated"));
        o.schema = Some(ParquetSchema::Explicit {
            fields: vec![f("a")],
        });
        assert!(o.validate().is_ok());
        o.schema = Some(ParquetSchema::Inferred { sample_size: None });
        assert!(o.validate().is_ok());
    }
}

#[cfg(all(test, feature = "file-format-parquet"))]
mod encoder_tests {
    use super::*;
    use arrow::datatypes::DataType;
    use serde_json::{Value, json};

    fn read(bytes: &[u8]) -> (arrow::datatypes::SchemaRef, Vec<Value>) {
        read_with(bytes, &ParquetReadOptions::default())
    }

    fn read_with(
        bytes: &[u8],
        opts: &ParquetReadOptions,
    ) -> (arrow::datatypes::SchemaRef, Vec<Value>) {
        let (schema, batches) =
            read_bytes(bytes::Bytes::copy_from_slice(bytes), opts, 0, "t").unwrap();
        let rows = batches
            .iter()
            .flat_map(|b| crate::columnar::record_batch_to_values(b).unwrap())
            .collect();
        (schema, rows)
    }

    fn objects(
        o: ParquetWriteOptions,
        rows: Option<usize>,
        bytes: Option<usize>,
    ) -> ParquetObjects {
        ParquetObjects::new(&o, rows, bytes).unwrap()
    }

    #[test]
    fn records_round_trip_and_finish_resets() {
        let mut w = objects(ParquetWriteOptions::default(), None, None);
        w.push_records(&[json!({"id": 1, "name": "a"}), json!({"id": 2, "name": "b"})])
            .unwrap();
        assert_eq!(w.rows(), 2);
        assert!(w.bytes() > 0);
        assert!(w.schema().is_some());
        assert!(w.take_ready().is_empty());
        w.finish().unwrap();
        assert_eq!(w.rows(), 0);
        assert!(w.schema().is_none());
        let ready = w.take_ready();
        assert_eq!(ready.len(), 1);
        assert_eq!(&ready[0][..4], b"PAR1");
        let (schema, rows) = read(&ready[0]);
        assert!(schema.fields().iter().all(|f| f.is_nullable()));
        assert_eq!(
            rows,
            vec![json!({"id": 1, "name": "a"}), json!({"id": 2, "name": "b"})]
        );
        w.finish().unwrap();
        assert!(w.take_ready().is_empty(), "finishing nothing emits nothing");
        assert!(format!("{w:?}").contains("ParquetObjects"));
    }

    #[test]
    fn later_pages_widen_the_object() {
        let mut w = objects(ParquetWriteOptions::default(), None, None);
        w.push_records(&[json!({"id": 1})]).unwrap();
        w.push_records(&[json!({"id": 2.5, "extra": "x"})]).unwrap();
        w.push_records(&[json!({"id": null})]).unwrap();
        w.finish().unwrap();
        let (schema, rows) = read(&w.take_ready()[0]);
        assert_eq!(
            schema.field_with_name("id").unwrap().data_type(),
            &DataType::Float64
        );
        assert_eq!(rows[0], json!({"id": 1.0, "extra": null}));
        assert_eq!(rows[1], json!({"id": 2.5, "extra": "x"}));
        assert_eq!(rows[2], json!({"id": null, "extra": null}));
    }

    #[test]
    fn a_type_change_names_the_field() {
        let mut w = objects(ParquetWriteOptions::default(), None, None);
        w.push_records(&[json!({"id": 1})]).unwrap();
        let err = w
            .push_records(&[json!({"id": "one"})])
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("`id`") && err.contains("changed type"),
            "{err}"
        );
        assert_eq!(w.rows(), 1, "the object keeps what was written");
    }

    #[test]
    fn objects_roll_on_rows_and_bytes() {
        let mut w = objects(ParquetWriteOptions::default(), Some(2), None);
        let recs: Vec<Value> = (0..5).map(|i| json!({"i": i})).collect();
        w.push_records(&recs).unwrap();
        assert_eq!(w.take_ready().len(), 2);
        assert_eq!(w.rows(), 1);
        let mut w = objects(ParquetWriteOptions::default(), Some(0), Some(1));
        w.push_records(&recs).unwrap();
        assert_eq!(
            w.take_ready().len(),
            1,
            "the byte cap closes after the first write"
        );
    }

    #[test]
    fn explicit_schema_types_and_unknown_fields() {
        let o: ParquetWriteOptions = serde_json::from_value(json!({
            "schema": {"type": "explicit", "fields": [
                {"name": "id", "type": "int32", "nullable": false},
                {"name": "d", "type": "date"},
                {"name": "ts", "type": "timestamp"}
            ]}
        }))
        .unwrap();
        let mut w = objects(o.clone(), None, None);
        w.push_records(&[
            json!({"id": 7, "d": "2024-01-02", "ts": "2024-01-02T03:04:05Z", "junk": 1}),
        ])
        .unwrap();
        w.finish().unwrap();
        let (schema, _) = read(&w.take_ready()[0]);
        assert_eq!(schema.field(0).data_type(), &DataType::Int32);
        assert!(!schema.field(0).is_nullable());
        assert_eq!(schema.field(1).data_type(), &DataType::Date32);
        assert!(matches!(
            schema.field(2).data_type(),
            DataType::Timestamp(_, Some(_))
        ));
        assert!(schema.field_with_name("junk").is_err());

        let strict = ParquetWriteOptions {
            on_unknown_field: ParquetUnknownField::Error,
            ..o
        };
        let mut w = objects(strict, None, None);
        let err = w
            .push_records(&[json!({"id": 1, "junk": 1})])
            .unwrap_err()
            .to_string();
        assert!(err.contains("`junk`"), "{err}");
        let err = w
            .push_records(&[json!({"id": "x"})])
            .unwrap_err()
            .to_string();
        assert!(err.contains("does not fit"), "{err}");
    }

    #[test]
    fn sample_size_limits_inference() {
        let o = ParquetWriteOptions {
            schema: Some(ParquetSchema::Inferred {
                sample_size: Some(1),
            }),
            ..Default::default()
        };
        let mut w = objects(o, None, None);
        w.push_records(&[json!({"a": 1}), json!({"a": 2, "late": true})])
            .unwrap();
        w.finish().unwrap();
        let (schema, _) = read(&w.take_ready()[0]);
        assert!(schema.field_with_name("late").is_err());
    }

    #[test]
    fn empty_records_are_refused() {
        let mut w = objects(ParquetWriteOptions::default(), None, None);
        let err = w.push_records(&[json!({})]).unwrap_err().to_string();
        assert!(err.contains("no fields"), "{err}");
    }
}

#[cfg(all(test, feature = "file-format-parquet"))]
mod columnar_tests {
    use super::*;
    use arrow::array::{Array, Int32Array, Int64Array, RecordBatch, StringArray, StructArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn batch(ids: Vec<i64>, names: Option<Vec<&str>>) -> RecordBatch {
        let mut fields = vec![Field::new("id", DataType::Int64, false)];
        let mut cols: Vec<Arc<dyn Array>> = vec![Arc::new(Int64Array::from(ids))];
        if let Some(n) = names {
            fields.push(Field::new("name", DataType::Utf8, false));
            cols.push(Arc::new(StringArray::from(n)));
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).unwrap()
    }

    fn decode(bytes: Vec<u8>, opts: &ParquetReadOptions) -> (arrow::datatypes::SchemaRef, usize) {
        let (s, b) = read_bytes(bytes::Bytes::from(bytes), opts, 2, "obj").unwrap();
        (s, b.iter().map(RecordBatch::num_rows).sum())
    }

    #[test]
    fn batches_slice_widen_and_rewrite() {
        let o = ParquetWriteOptions {
            row_group_size: 1,
            ..Default::default()
        };
        let mut w = ParquetObjects::new(&o, Some(3), None).unwrap();
        w.push_batch(&batch(vec![1, 2], None)).unwrap();
        w.push_batch(&batch(vec![3, 4], Some(vec!["c", "d"])))
            .unwrap();
        let ready = w.take_ready();
        assert_eq!(ready.len(), 1);
        let (schema, rows) = decode(ready[0].clone(), &ParquetReadOptions::default());
        assert_eq!(rows, 3);
        assert!(
            schema.field_with_name("name").is_ok(),
            "the widened column was kept"
        );
        w.finish().unwrap();
        let (_, rows) = decode(w.take_ready().remove(0), &ParquetReadOptions::default());
        assert_eq!(rows, 1);
    }

    #[test]
    fn explicit_schema_conforms_batches() {
        let o: ParquetWriteOptions = serde_json::from_value(serde_json::json!({
            "schema": {"type": "explicit", "fields": [
                {"name": "id", "type": "int32"}, {"name": "missing", "type": "string"}
            ]}
        }))
        .unwrap();
        let mut w = ParquetObjects::new(&o, None, None).unwrap();
        w.push_batch(&batch(vec![1], Some(vec!["x"]))).unwrap();
        w.finish().unwrap();
        let (schema, rows) = decode(w.take_ready().remove(0), &ParquetReadOptions::default());
        assert_eq!(rows, 1);
        assert_eq!(schema.field(0).data_type(), &DataType::Int32);
        assert_eq!(schema.fields().len(), 2);

        let strict: ParquetWriteOptions = serde_json::from_value(serde_json::json!({
            "on_unknown_field": "error",
            "schema": {"type": "explicit", "fields": [{"name": "id", "type": "int64", "nullable": false},
                                                      {"name": "req", "type": "int64", "nullable": false}]}
        }))
        .unwrap();
        let mut w = ParquetObjects::new(&strict, None, None).unwrap();
        let err = w
            .push_batch(&batch(vec![1], Some(vec!["x"])))
            .unwrap_err()
            .to_string();
        assert!(err.contains("`name`"), "{err}");
        let err = w.push_batch(&batch(vec![1], None)).unwrap_err().to_string();
        assert!(err.contains("required column `req`"), "{err}");
    }

    #[test]
    fn merge_rules() {
        let s = |dt: DataType| Arc::new(Schema::new(vec![Field::new("x", dt, true)]));
        let m = |a: DataType, b: DataType| imp_merge(&s(a), &s(b));
        assert_eq!(
            m(DataType::Int32, DataType::Int64).unwrap(),
            DataType::Int64
        );
        assert_eq!(m(DataType::Null, DataType::Utf8).unwrap(), DataType::Utf8);
        assert_eq!(
            m(DataType::Int64, DataType::Float32).unwrap(),
            DataType::Float64
        );
        let list = |t| DataType::List(Arc::new(Field::new("item", t, true)));
        assert_eq!(
            m(list(DataType::Int32), list(DataType::Int64)).unwrap(),
            list(DataType::Int64)
        );
        let st = |f: Vec<Field>| DataType::Struct(f.into());
        let merged = m(
            st(vec![Field::new("a", DataType::Int64, true)]),
            st(vec![Field::new("b", DataType::Utf8, true)]),
        )
        .unwrap();
        assert!(matches!(merged, DataType::Struct(ref f) if f.len() == 2));
        assert!(m(DataType::Utf8, DataType::Boolean).is_err());
    }

    fn imp_merge(
        a: &arrow::datatypes::SchemaRef,
        b: &arrow::datatypes::SchemaRef,
    ) -> Result<DataType, crate::FaucetError> {
        let mut w = ParquetObjects::new(&ParquetWriteOptions::default(), None, None).unwrap();
        let col = |s: &arrow::datatypes::SchemaRef| {
            arrow::array::new_null_array(s.field(0).data_type(), 1)
        };
        let first = RecordBatch::try_new(a.clone(), vec![col(a)]).unwrap();
        let second = RecordBatch::try_new(b.clone(), vec![col(b)]).unwrap();
        w.push_batch(&first)?;
        w.push_batch(&second)?;
        Ok(w.schema().unwrap().field(0).data_type().clone())
    }

    #[test]
    fn projection_selects_and_names_missing_columns() {
        let mut w = ParquetObjects::new(&ParquetWriteOptions::default(), None, None).unwrap();
        w.push_batch(&batch(vec![1, 2, 3], Some(vec!["a", "b", "c"])))
            .unwrap();
        w.finish().unwrap();
        let bytes = w.take_ready().remove(0);
        let opts = ParquetReadOptions {
            columns: Some(vec!["name".into()]),
        };
        let (schema, rows) = decode(bytes.clone(), &opts);
        assert_eq!(rows, 3);
        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0).name(), "name");
        let bad = ParquetReadOptions {
            columns: Some(vec!["nope".into()]),
        };
        let err = read_bytes(bytes::Bytes::from(bytes), &bad, 0, "obj")
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("`nope`") && err.contains("'obj'") && err.contains("id, name"),
            "{err}"
        );
        let err = read_bytes(bytes::Bytes::from_static(b"nope"), &opts, 0, "obj").unwrap_err();
        assert!(err.to_string().contains("failed to read 'obj'"));
    }

    #[test]
    fn every_codec_writes_a_readable_object() {
        for c in [
            ParquetCompression::Uncompressed,
            ParquetCompression::Snappy,
            ParquetCompression::Gzip,
            ParquetCompression::Zstd,
            ParquetCompression::Lz4,
        ] {
            let o = ParquetWriteOptions {
                compression: c,
                ..Default::default()
            };
            let mut w = ParquetObjects::new(&o, None, None).unwrap();
            w.push_batch(&batch(vec![1], None)).unwrap();
            w.finish().unwrap();
            let (_, rows) = decode(w.take_ready().remove(0), &ParquetReadOptions::default());
            assert_eq!(rows, 1, "{c:?}");
        }
        let _ = (
            Int32Array::from(vec![1]),
            StructArray::new_empty_fields(0, None),
        );
    }
}
