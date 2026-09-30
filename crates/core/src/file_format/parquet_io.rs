//! Apache Parquet read helpers for the object-store sources (#777).
//!
//! [`ParquetReadOptions`] is always compiled so a source config can carry a
//! `parquet:` block without cfg-gating it; [`projection_mask`] and
//! [`read_bytes`] need the `file-format-parquet` feature, as does the shared
//! ranged reader ([`RangedParquetReader`] over a [`RangeRead`] transport,
//! opened with [`range_stream`]) that decodes an object one row group at a
//! time instead of buffering it whole. `parquet.columns`
//! projects top-level columns before any row group is decoded, on both the
//! ranged (row group at a time) and the buffered path.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Parquet reader options (the `parquet:` block of an object-store source).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ParquetReadOptions {
    /// Decode only these top-level columns. A name missing from an object is
    /// an error naming the object and its columns. Unset: every column.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub columns: Option<Vec<String>>,
}

impl ParquetReadOptions {
    /// Refuse options that would silently read nothing: an empty `columns`
    /// list (it projects no column, so every record would be empty) or a
    /// blank column name. Sources call this at construction.
    pub fn validate(&self) -> Result<(), crate::FaucetError> {
        let Some(columns) = self.columns.as_deref() else {
            return Ok(());
        };
        if columns.is_empty() {
            return Err(crate::FaucetError::Config(
                "parquet.columns must name at least one column; omit it to read every column"
                    .into(),
            ));
        }
        if columns.iter().any(|c| c.trim().is_empty()) {
            return Err(crate::FaucetError::Config(
                "parquet.columns must not contain an empty column name".into(),
            ));
        }
        Ok(())
    }
}

/// The byte buffer a [`RangeRead`] returns, re-exported so a connector
/// needs no direct `bytes` dependency.
#[cfg(feature = "file-format-parquet")]
pub use bytes::Bytes;
#[cfg(feature = "file-format-parquet")]
pub use imp::{
    BatchStream, RangeRead, RangedParquetReader, open_ranged, projection_mask, range_stream,
    read_bytes,
};

#[cfg(feature = "file-format-parquet")]
mod imp {
    use super::ParquetReadOptions;
    use crate::FaucetError;
    use ::parquet::arrow::ProjectionMask;
    use ::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use ::parquet::schema::types::SchemaDescriptor;
    use arrow::array::RecordBatch;
    use arrow::datatypes::SchemaRef;
    use bytes::Bytes;
    use futures::FutureExt;
    use futures::future::BoxFuture;
    use ::parquet::arrow::ParquetRecordBatchStreamBuilder;
    use ::parquet::arrow::arrow_reader::ArrowReaderOptions;
    use ::parquet::arrow::async_reader::{AsyncFileReader, ParquetRecordBatchStream};
    use ::parquet::errors::ParquetError;
    use ::parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
    use std::ops::Range;
    use std::sync::Arc;

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
        opts.validate()?;
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

    /// Random access to one stored object: the transport half of
    /// [`RangedParquetReader`]. An implementation fetches `range` and may
    /// return fewer bytes than asked for; the reader turns a short read into
    /// an error instead of handing truncated bytes to the decoder.
    pub trait RangeRead: Send + Unpin + 'static {
        /// The bytes of `range` (end exclusive).
        fn read_range(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes, FaucetError>>;
    }

    /// A Parquet [`AsyncFileReader`] over byte ranges (#619, #783): the
    /// footer locates every row group, so an object is decoded one row group
    /// at a time and never buffered whole.
    pub struct RangedParquetReader<R> {
        inner: R,
        len: u64,
        display: String,
    }

    impl<R: RangeRead> RangedParquetReader<R> {
        /// A reader over an object of `len` bytes, named `display` in errors.
        pub fn new(inner: R, len: u64, display: impl Into<String>) -> Self {
            Self {
                inner,
                len,
                display: display.into(),
            }
        }

        /// The object's length in bytes.
        pub fn len(&self) -> u64 {
            self.len
        }

        /// Whether the object is empty.
        pub fn is_empty(&self) -> bool {
            self.len == 0
        }

        async fn fetch(&mut self, range: Range<u64>) -> Result<Bytes, ParquetError> {
            let wanted = range.end.saturating_sub(range.start);
            if wanted == 0 {
                return Ok(Bytes::new());
            }
            let (start, end) = (range.start, range.end);
            let bytes = self.inner.read_range(range).await.map_err(|e| {
                external(format!(
                    "ranged read of '{}' (bytes {start}..{end}) failed: {e}",
                    self.display
                ))
            })?;
            if bytes.len() as u64 != wanted {
                return Err(external(format!(
                    "'{}' returned {} bytes for range {start}..{end} but {wanted} were \
                     requested; the object is truncated or was replaced mid-read",
                    self.display,
                    bytes.len()
                )));
            }
            Ok(bytes)
        }
    }

    fn external(msg: String) -> ParquetError {
        ParquetError::External(Box::new(std::io::Error::other(msg)))
    }

    impl<R: RangeRead> AsyncFileReader for RangedParquetReader<R> {
        fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes, ParquetError>> {
            self.fetch(range).boxed()
        }

        fn get_metadata<'a>(
            &'a mut self,
            options: Option<&'a ArrowReaderOptions>,
        ) -> BoxFuture<'a, Result<Arc<ParquetMetaData>, ParquetError>> {
            async move {
                let mut reader = ParquetMetaDataReader::new();
                if let Some(opts) = options {
                    reader = reader
                        .with_column_index_policy(opts.column_index_policy())
                        .with_offset_index_policy(opts.offset_index_policy());
                }
                let len = self.len;
                Ok(Arc::new(reader.load_and_finish(self, len).await?))
            }
            .boxed()
        }
    }

    /// Open `reader` as a stream of Arrow batches projected by `opts`, each
    /// at most `batch_size` rows (`0`: the reader's default), so a single
    /// huge row group is still decoded in bounded steps.
    pub async fn range_stream<R: RangeRead>(
        reader: RangedParquetReader<R>,
        opts: &ParquetReadOptions,
        batch_size: usize,
    ) -> Result<ParquetRecordBatchStream<RangedParquetReader<R>>, FaucetError> {
        let display = reader.display.clone();
        let mut builder = ParquetRecordBatchStreamBuilder::new(reader)
            .await
            .map_err(|e| {
                FaucetError::Source(format!(
                    "parquet: failed to read the metadata of '{display}': {e}"
                ))
            })?;
        if let Some(mask) = projection_mask(opts, builder.parquet_schema(), &display)? {
            builder = builder.with_projection(mask);
        }
        if batch_size > 0 {
            builder = builder.with_batch_size(batch_size);
        }
        builder.build().map_err(|e| {
            FaucetError::Source(format!(
                "parquet: failed to build a reader for '{display}': {e}"
            ))
        })
    }

    /// A boxed stream of Arrow batches, the shape [`open_ranged`] returns.
    pub type BatchStream = futures::stream::BoxStream<'static, Result<RecordBatch, FaucetError>>;

    /// [`range_stream`] as the object's Arrow schema and a boxed batch stream
    /// whose decode errors name the object — the one shape a source's row and
    /// columnar paths both consume.
    pub async fn open_ranged<R: RangeRead>(
        reader: RangedParquetReader<R>,
        opts: &ParquetReadOptions,
        batch_size: usize,
    ) -> Result<(SchemaRef, BatchStream), FaucetError> {
        use futures::{StreamExt, TryStreamExt};
        let display = reader.display.clone();
        let stream = range_stream(reader, opts, batch_size).await?;
        let schema = stream.schema().clone();
        let batches = stream
            .map_err(move |e| {
                FaucetError::Source(format!("parquet: failed to decode '{display}': {e}"))
            })
            .boxed();
        Ok((schema, batches))
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
mod tests {
    use super::*;

    #[test]
    fn read_options_serde() {
        let r: ParquetReadOptions =
            serde_json::from_value(serde_json::json!({"columns": ["a"]})).unwrap();
        assert_eq!(r.columns.as_deref(), Some(&["a".to_string()][..]));
        let empty: ParquetReadOptions = serde_json::from_value(serde_json::json!({})).unwrap();
        assert_eq!(empty, ParquetReadOptions::default());
        assert!(
            serde_json::from_value::<ParquetReadOptions>(serde_json::json!({"nope": 1})).is_err()
        );
    }

    #[cfg(feature = "file-format-parquet")]
    fn object() -> bytes::Bytes {
        use arrow::array::{Int64Array, RecordBatch, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("a.b", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["x", "y", "z"])),
            ],
        )
        .unwrap();
        let mut buf = Vec::new();
        let mut w = ::parquet::arrow::ArrowWriter::try_new(&mut buf, schema, None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        buf.into()
    }

    #[cfg(feature = "file-format-parquet")]
    #[test]
    fn projection_selects_top_level_columns_even_with_dots() {
        let (schema, batches) =
            read_bytes(object(), &ParquetReadOptions::default(), 2, "o").unwrap();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(batches.len(), 2, "batch_size caps each batch");
        let opts = ParquetReadOptions {
            columns: Some(vec!["a.b".into()]),
        };
        let (schema, batches) = read_bytes(object(), &opts, 0, "o").unwrap();
        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0).name(), "a.b");
        assert_eq!(batches[0].num_columns(), 1);
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
    }

    #[cfg(feature = "file-format-parquet")]
    #[test]
    fn a_missing_column_or_a_bad_object_is_named() {
        let bad = ParquetReadOptions {
            columns: Some(vec!["nope".into()]),
        };
        let err = read_bytes(object(), &bad, 0, "obj")
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("`nope`") && err.contains("'obj'") && err.contains("id, a.b"),
            "{err}"
        );
        let err = read_bytes(bytes::Bytes::from_static(b"nope"), &bad, 0, "obj").unwrap_err();
        assert!(matches!(err, crate::FaucetError::Source(ref m) if m.contains("'obj'")));
    }

    #[test]
    fn validate_refuses_an_empty_or_blank_projection() {
        assert!(ParquetReadOptions::default().validate().is_ok());
        let ok = ParquetReadOptions {
            columns: Some(vec!["a".into()]),
        };
        assert!(ok.validate().is_ok());
        let empty = ParquetReadOptions {
            columns: Some(vec![]),
        };
        let e = empty.validate().unwrap_err();
        assert!(
            matches!(e, crate::FaucetError::Config(ref m) if m.contains("at least one column")),
            "{e}"
        );
        let blank = ParquetReadOptions {
            columns: Some(vec!["a".into(), " ".into()]),
        };
        assert!(matches!(
            blank.validate(),
            Err(crate::FaucetError::Config(_))
        ));
    }

    #[cfg(feature = "file-format-parquet")]
    #[test]
    fn projection_mask_refuses_an_empty_projection() {
        let opts = ParquetReadOptions {
            columns: Some(vec![]),
        };
        assert!(read_bytes(object(), &opts, 0, "o").is_err());
    }

    #[cfg(feature = "file-format-parquet")]
    struct Mem {
        data: bytes::Bytes,
        truncate: bool,
        reads: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }

    #[cfg(feature = "file-format-parquet")]
    impl RangeRead for Mem {
        fn read_range(
            &mut self,
            range: std::ops::Range<u64>,
        ) -> futures::future::BoxFuture<'_, Result<bytes::Bytes, crate::FaucetError>> {
            use futures::FutureExt;
            self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let end = if self.truncate {
                range.end - 1
            } else {
                range.end
            };
            let out = self.data.slice(range.start as usize..end as usize);
            async move { Ok(out) }.boxed()
        }
    }

    #[cfg(feature = "file-format-parquet")]
    #[tokio::test]
    async fn ranged_reader_streams_projected_capped_batches() {
        use futures::TryStreamExt;
        let data = object();
        let reads = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mem = Mem {
            data: data.clone(),
            truncate: false,
            reads: reads.clone(),
        };
        let reader = RangedParquetReader::new(mem, data.len() as u64, "o");
        assert_eq!(reader.len(), data.len() as u64);
        assert!(!reader.is_empty());
        let opts = ParquetReadOptions {
            columns: Some(vec!["id".into()]),
        };
        let batches: Vec<_> = range_stream(reader, &opts, 2)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(batches.len(), 2, "batch_size caps each batch");
        assert!(batches.iter().all(|b| b.num_columns() == 1));
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
        assert!(reads.load(std::sync::atomic::Ordering::SeqCst) >= 2);

        let mem = Mem {
            data: data.clone(),
            truncate: false,
            reads: reads.clone(),
        };
        let (schema, batches) = open_ranged(
            RangedParquetReader::new(mem, data.len() as u64, "o"),
            &ParquetReadOptions::default(),
            0,
        )
        .await
        .unwrap();
        assert_eq!(schema.fields().len(), 2);
        let batches: Vec<_> = batches.try_collect().await.unwrap();
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
    }

    #[cfg(feature = "file-format-parquet")]
    #[tokio::test]
    async fn a_short_range_is_an_error_naming_the_object() {
        let data = object();
        let mem = Mem {
            data: data.clone(),
            truncate: true,
            reads: Default::default(),
        };
        let reader = RangedParquetReader::new(mem, data.len() as u64, "obj.parquet");
        let err = range_stream(reader, &ParquetReadOptions::default(), 0)
            .await
            .err()
            .expect("a short read fails")
            .to_string();
        assert!(err.contains("obj.parquet") && err.contains("truncated"), "{err}");
    }

    #[cfg(feature = "file-format-parquet")]
    #[tokio::test]
    async fn ranged_reader_names_a_missing_column_and_a_failed_read() {
        struct Failing;
        impl RangeRead for Failing {
            fn read_range(
                &mut self,
                _range: std::ops::Range<u64>,
            ) -> futures::future::BoxFuture<'_, Result<bytes::Bytes, crate::FaucetError>>
            {
                use futures::FutureExt;
                async { Err(crate::FaucetError::Source("boom".into())) }.boxed()
            }
        }
        let e = range_stream(
            RangedParquetReader::new(Failing, 100, "f"),
            &ParquetReadOptions::default(),
            0,
        )
        .await
        .err()
        .unwrap()
        .to_string();
        assert!(e.contains("'f'") && e.contains("boom"), "{e}");
        let data = object();
        let mem = Mem {
            data: data.clone(),
            truncate: false,
            reads: Default::default(),
        };
        let opts = ParquetReadOptions {
            columns: Some(vec!["nope".into()]),
        };
        let e = range_stream(RangedParquetReader::new(mem, data.len() as u64, "o"), &opts, 0)
            .await
            .err()
            .unwrap()
            .to_string();
        assert!(e.contains("`nope`"), "{e}");
        let empty = RangedParquetReader::new(Failing, 0, "e");
        assert!(empty.is_empty());
    }
}
