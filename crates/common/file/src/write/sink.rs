//! [`FileSink`]: the one `Sink` implementation over a [`FileWriter`],
//! shared by every file-writing sink and generic over the
//! [`StorageBackend`] it writes to. A connector supplies only what is its
//! own — its name, config schema, dataset URI and readback source — through
//! [`SinkIdentity`], and forwards its `Sink` impl with
//! [`delegate_sink!`](crate::delegate_sink).

use super::backend::StorageBackend;
use super::layout::NameTemplate;
use super::writer::{FileWriter, WriteSettings};
use faucet_core::check::{CheckContext, CheckReport};
use faucet_core::observability::RoundtripRecorder;
use faucet_core::{Capabilities, FaucetError, FileFormat, SchemaReport, Sink, WriteMode};
use serde_json::Value;
use std::sync::Arc;

/// What a file-writing sink supplies besides its writer.
///
/// **Experimental** (PRINCIPLES.md §3): this block's shape may change in a
/// minor release; any change is called out in the changelog.
#[faucet_core::async_trait]
pub trait SinkIdentity: Send + Sync {
    /// The connector's name (`"s3"`).
    fn connector_name(&self) -> &'static str;
    /// The JSON Schema of the connector's config.
    fn config_schema(&self) -> Value;
    /// The dataset the sink writes, for lineage.
    fn dataset_uri(&self) -> String;
    /// A non-mutating preflight probe, used when the backend has none.
    /// Default: not implemented.
    async fn check(&self, _ctx: &CheckContext) -> Result<CheckReport, FaucetError> {
        Ok(CheckReport::not_implemented())
    }
    /// A `(kind, config)` source that reads the output back. Default: none.
    fn readback_source(&self) -> Option<(String, Value)> {
        None
    }
    /// Install the round-trip recorder the connector's client reports to,
    /// beside the backend's. Default: the connector counts nothing.
    fn set_roundtrip_recorder(&self, _recorder: Arc<RoundtripRecorder>) {}
    /// Install the run's call context (#880) the connector's requests run
    /// through, beside the backend's. Default: the connector has no calls of
    /// its own.
    fn set_call_context(&self, _ctx: faucet_core::io::CallContext) {}
}

/// The one [`Sink`] over a [`FileWriter`], generic over the storage it
/// writes to (RFC 0013, #886): batch writes, flush, the overwrite lifecycle,
/// the end-of-run prune and the columnar path, in one place. The local file
/// sink is a `FileSink<LocalBackend>`; the S3, GCS, Azure Blob and SFTP
/// sinks are `FileSink`s over their stores' backends.
///
/// Requests the backend makes run through the run's call context and report
/// to its round-trip recorder ([`StorageBackend::set_call_context`],
/// [`StorageBackend::set_roundtrip_recorder`]); the preflight probe is the
/// backend's ([`StorageBackend::check`]) and falls back to the identity's.
///
/// **Experimental** (PRINCIPLES.md §3): this block's shape may change in a
/// minor release; any change is called out in the changelog.
pub struct FileSink<B: StorageBackend + ?Sized + 'static = dyn StorageBackend> {
    writer: FileWriter,
    backend: Arc<B>,
    identity: Box<dyn SinkIdentity>,
}

impl<B: StorageBackend + 'static> FileSink<B> {
    /// A sink writing `settings` / `template` to `backend`, described by
    /// `identity`.
    pub fn build(
        settings: WriteSettings,
        template: NameTemplate,
        backend: Arc<B>,
        identity: impl SinkIdentity + 'static,
    ) -> Result<Self, FaucetError> {
        let dynamic: Arc<dyn StorageBackend> = backend.clone();
        let writer = FileWriter::new(settings, template, dynamic)?;
        Ok(Self {
            writer,
            backend,
            identity: Box::new(identity),
        })
    }
}

impl FileSink<dyn StorageBackend> {
    /// A sink over an already-built `writer`, described by `identity`.
    pub fn from_writer(writer: FileWriter, identity: impl SinkIdentity + 'static) -> Self {
        Self {
            backend: writer.backend().clone(),
            writer,
            identity: Box::new(identity),
        }
    }
}

impl<B: StorageBackend + ?Sized + 'static> FileSink<B> {
    /// The writer.
    pub fn writer(&self) -> &FileWriter {
        &self.writer
    }

    /// The storage backend.
    pub fn backend(&self) -> &Arc<B> {
        &self.backend
    }

    /// The format the output is written in.
    pub fn format(&self) -> FileFormat {
        self.writer.settings().format
    }
}

#[faucet_core::async_trait]
impl<B: StorageBackend + ?Sized + 'static> Sink for FileSink<B> {
    fn connector_name(&self) -> &'static str {
        self.identity.connector_name()
    }

    fn config_schema(&self) -> Value {
        self.identity.config_schema()
    }

    fn dataset_uri(&self) -> String {
        self.identity.dataset_uri()
    }

    fn set_roundtrip_recorder(&self, recorder: Arc<RoundtripRecorder>) {
        self.backend.set_roundtrip_recorder(recorder.clone());
        self.identity.set_roundtrip_recorder(recorder);
    }

    fn set_call_context(&self, ctx: faucet_core::io::CallContext) {
        self.backend.set_call_context(ctx.clone());
        self.identity.set_call_context(ctx);
    }

    async fn check(&self, ctx: &CheckContext) -> Result<CheckReport, FaucetError> {
        match self.backend.check(ctx).await? {
            Some(report) => Ok(report),
            None => self.identity.check(ctx).await,
        }
    }

    fn readback_source(&self) -> Option<(String, Value)> {
        self.identity.readback_source()
    }

    fn batch_atomicity(&self) -> faucet_core::BatchAtomicity {
        self.writer.settings().batch_atomicity()
    }

    fn supported_write_modes(&self) -> &'static [WriteMode] {
        &[WriteMode::Append, WriteMode::Overwrite]
    }

    /// Derived, plus [`SchemaReport::DropsUnknownFields`] for CSV under
    /// `csv.on_unknown_field: warn`, whose frozen header drops new fields.
    fn capabilities(&self) -> Capabilities {
        let settings = self.writer.settings();
        let base = Capabilities::of_sink(self);
        if crate::capabilities::drops_unknown_fields(
            settings.format,
            settings.opts.csv.on_unknown_field,
        ) {
            base.to_builder()
                .schema_report(SchemaReport::DropsUnknownFields)
                .build()
        } else {
            base
        }
    }

    fn is_overwrite(&self) -> bool {
        self.writer.overwriting()
    }

    async fn begin_overwrite(&self) -> Result<(), FaucetError> {
        self.writer.begin_overwrite().await
    }

    async fn commit_overwrite(&self) -> Result<(), FaucetError> {
        self.writer.commit_overwrite().await
    }

    async fn abort_overwrite(&self) -> Result<(), FaucetError> {
        self.writer.abort_overwrite().await
    }

    async fn overwrite_staging_exists(&self) -> Result<Option<bool>, FaucetError> {
        self.writer.swap_area_exists().await.map(Some)
    }

    async fn complete_run(&self) -> Result<(), FaucetError> {
        self.writer.complete().await
    }

    async fn local_outputs(&self) -> Vec<faucet_core::LocalOutput> {
        self.writer.local_outputs()
    }

    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }
        self.writer.write_rows(records).await
    }

    /// Publish the open file (#618): the pipeline calls `flush` at every
    /// bookmark-carrying page and at the end, so a bookmark never advances
    /// past records that are not in storage.
    async fn flush(&self) -> Result<(), FaucetError> {
        self.writer.flush().await
    }

    #[cfg(feature = "arrow")]
    fn supports_columnar(&self) -> bool {
        matches!(self.format(), FileFormat::Parquet | FileFormat::Avro)
    }

    #[cfg(feature = "arrow")]
    async fn write_batch_columnar(
        &self,
        batch: &arrow::array::RecordBatch,
    ) -> Result<usize, FaucetError> {
        if batch.num_rows() == 0 {
            return Ok(0);
        }
        #[cfg(feature = "file-format-parquet")]
        if self.format() == FileFormat::Parquet {
            return self.writer.write_batch(batch).await;
        }
        let rows = faucet_core::columnar::record_batch_to_values(batch)?;
        self.writer.write_rows(&rows).await
    }
}

/// A [`FileSink`] whose backend type is not named, built from a writer.
///
/// **Experimental** (PRINCIPLES.md §3): this block's shape may change in a
/// minor release; any change is called out in the changelog.
pub struct WriterSink {
    inner: FileSink,
}

impl WriterSink {
    /// A sink writing through `writer`, described by `identity`.
    pub fn new(writer: FileWriter, identity: impl SinkIdentity + 'static) -> Self {
        Self {
            inner: FileSink::from_writer(writer, identity),
        }
    }

    /// The writer.
    pub fn writer(&self) -> &FileWriter {
        self.inner.writer()
    }

    /// The format the output is written in.
    pub fn format(&self) -> FileFormat {
        self.inner.format()
    }
}

#[faucet_core::async_trait]
impl Sink for WriterSink {
    faucet_core::forward_sink!(inner);
}

/// Implement `Sink` for a connector type by forwarding every method to its
/// [`FileSink`] (or [`WriterSink`]) field: `delegate_sink!(S3Sink, inner);`.
/// Every hook is forwarded ([`faucet_core::forward_sink!`]), so a hook the
/// trait gains later reaches the shared sink without touching the connector.
#[macro_export]
macro_rules! delegate_sink {
    ($ty:ty, $field:ident) => {
        #[$crate::__private::async_trait]
        impl $crate::__private::Sink for $ty {
            $crate::__private::forward_sink!($field);
        }
    };
}
