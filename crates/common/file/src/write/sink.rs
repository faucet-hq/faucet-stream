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
/// The columnar hooks follow the calling crate's `arrow` feature, so the
/// connector never claims the columnar path its own build leaves out.
#[macro_export]
macro_rules! delegate_sink {
    ($ty:ty, $field:ident) => {
        #[$crate::__private::async_trait]
        impl $crate::__private::Sink for $ty {
            $crate::__private::forward_sink!($field;
                except capabilities, supports_columnar, write_batch_columnar);
            fn capabilities(&self) -> $crate::__private::Capabilities {
                let caps = $crate::__private::Sink::capabilities(&self.$field);
                if cfg!(feature = "arrow") {
                    caps
                } else {
                    caps.to_builder().columnar(false).build()
                }
            }
            #[cfg(feature = "arrow")]
            fn supports_columnar(&self) -> bool {
                $crate::__private::Sink::supports_columnar(&self.$field)
            }
            #[cfg(feature = "arrow")]
            async fn write_batch_columnar(
                &self,
                batch: &::arrow::array::RecordBatch,
            ) -> ::std::result::Result<usize, $crate::__private::FaucetError> {
                $crate::__private::Sink::write_batch_columnar(&self.$field, batch).await
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::write::remote::tests::Mem;
    use crate::write::{Area, LocalBackend, ObjectClient, RemoteBackend, object_layout};
    use faucet_core::check::{Probe, ProbeStatus};
    use faucet_core::io::CallContext;
    use faucet_core::observability::RoundtripSide;
    use faucet_core::{Compression, FileFormat};
    use std::path::Path;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Default)]
    struct Identity {
        installs: Arc<AtomicUsize>,
    }

    #[faucet_core::async_trait]
    impl SinkIdentity for Identity {
        fn connector_name(&self) -> &'static str {
            "test"
        }
        fn config_schema(&self) -> Value {
            serde_json::json!({"type": "object"})
        }
        fn dataset_uri(&self) -> String {
            "test://out".into()
        }
        async fn check(&self, _ctx: &CheckContext) -> Result<CheckReport, FaucetError> {
            Ok(CheckReport::single(Probe::pass(
                "identity",
                std::time::Duration::ZERO,
            )))
        }
        fn set_roundtrip_recorder(&self, _recorder: Arc<RoundtripRecorder>) {
            self.installs.fetch_add(1, Ordering::SeqCst);
        }
        fn set_call_context(&self, _ctx: CallContext) {
            self.installs.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// A store client that answers the preflight probe and counts installs.
    struct Probed {
        mem: Arc<Mem>,
        installs: Arc<AtomicUsize>,
    }

    #[faucet_core::async_trait]
    impl ObjectClient for Probed {
        fn describe(&self, key: &str) -> String {
            self.mem.describe(key)
        }
        async fn list(&self, prefix: &str) -> Result<Vec<String>, FaucetError> {
            self.mem.list(prefix).await
        }
        async fn exists(&self, key: &str) -> Result<bool, FaucetError> {
            self.mem.exists(key).await
        }
        async fn download(&self, key: &str, to: &Path) -> Result<(), FaucetError> {
            self.mem.download(key, to).await
        }
        async fn upload(&self, from: &Path, key: &str) -> Result<(), FaucetError> {
            self.mem.upload(from, key).await
        }
        async fn delete(&self, key: &str) -> Result<(), FaucetError> {
            self.mem.delete(key).await
        }
        fn set_roundtrip_recorder(&self, _recorder: Arc<RoundtripRecorder>) {
            self.installs.fetch_add(1, Ordering::SeqCst);
        }
        fn set_call_context(&self, _ctx: CallContext) {
            self.installs.fetch_add(1, Ordering::SeqCst);
        }
        async fn check(&self, _ctx: &CheckContext) -> Result<Option<CheckReport>, FaucetError> {
            Ok(Some(CheckReport::single(Probe::fail(
                "store",
                std::time::Duration::ZERO,
                "unreachable",
            ))))
        }
    }

    /// A store's own backend type, as a common crate defines one.
    struct Named {
        remote: RemoteBackend,
    }

    crate::delegate_storage_backend!(Named, remote);

    fn recorder() -> Arc<RoundtripRecorder> {
        Arc::new(RoundtripRecorder::new(
            RoundtripSide::Sink,
            "p",
            "r",
            "test",
        ))
    }

    fn jsonl() -> WriteSettings {
        WriteSettings::new(FileFormat::JsonLines, Compression::None)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_backend_without_a_probe_falls_back_to_the_identity() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.jsonl");
        let (d, t) = NameTemplate::from_path(
            path.to_str().unwrap(),
            FileFormat::JsonLines,
            Compression::None,
            false,
        )
        .unwrap();
        let identity = Identity::default();
        let installs = identity.installs.clone();
        let backend = Arc::new(LocalBackend::new(&d, &t, true));
        let sink = FileSink::build(jsonl(), t, backend, identity).unwrap();
        assert_eq!(sink.format(), FileFormat::JsonLines);
        assert!(
            sink.backend()
                .local_path(Area::Destination, "out.jsonl")
                .is_some()
        );
        let report = sink.check(&CheckContext::default()).await.unwrap();
        assert_eq!(report.probes[0].name, "identity");
        sink.set_call_context(CallContext::new());
        sink.set_roundtrip_recorder(recorder());
        assert_eq!(installs.load(Ordering::SeqCst), 2);
        let rows = [serde_json::json!({"a": 1})];
        assert_eq!(sink.write_batch(&rows).await.unwrap(), 1);
        sink.flush().await.unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "{\"a\":1}\n");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_backends_probe_and_hooks_win_through_a_named_backend() {
        let installs = Arc::new(AtomicUsize::new(0));
        let client = Arc::new(Probed {
            mem: Arc::new(Mem::default()),
            installs: installs.clone(),
        });
        let (base, t) = object_layout(
            "pre/",
            Some("x.jsonl"),
            ".jsonl",
            FileFormat::JsonLines,
            Compression::None,
            false,
        )
        .unwrap();
        let remote = RemoteBackend::new(client, base, &t, None).unwrap();
        let backend = Arc::new(Named { remote });
        assert!(
            backend
                .describe(Area::Destination, "x.jsonl")
                .ends_with("pre/x.jsonl")
        );
        let sink = FileSink::build(jsonl(), t, backend, Identity::default()).unwrap();
        let report = sink.check(&CheckContext::default()).await.unwrap();
        assert!(matches!(report.probes[0].status, ProbeStatus::Fail { .. }));
        sink.set_call_context(CallContext::new());
        sink.set_roundtrip_recorder(recorder());
        assert_eq!(installs.load(Ordering::SeqCst), 2);
        let rows = [serde_json::json!({"a": 1})];
        assert_eq!(sink.write_batch(&rows).await.unwrap(), 1);
        sink.flush().await.unwrap();
        sink.complete_run().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_writer_sink_is_the_file_sink_with_the_backend_unnamed() {
        let mem = Arc::new(Mem::default());
        let (base, t) = object_layout(
            "pre/",
            None,
            ".jsonl",
            FileFormat::JsonLines,
            Compression::None,
            false,
        )
        .unwrap();
        let remote = RemoteBackend::new(mem.clone(), base, &t, None).unwrap();
        let writer = FileWriter::new(jsonl(), t, Arc::new(remote)).unwrap();
        let sink = WriterSink::new(writer, Identity::default());
        assert_eq!(sink.format(), FileFormat::JsonLines);
        assert_eq!(sink.writer().settings().format, FileFormat::JsonLines);
        assert_eq!(sink.connector_name(), "test");
        assert_eq!(sink.dataset_uri(), "test://out");
        assert!(sink.config_schema().is_object());
        assert!(sink.readback_source().is_none());
        let report = sink.check(&CheckContext::default()).await.unwrap();
        assert_eq!(report.probes[0].name, "identity");
        let rows = [serde_json::json!({"a": 1})];
        assert_eq!(sink.write_batch(&rows).await.unwrap(), 1);
        sink.flush().await.unwrap();
        assert_eq!(mem.keys().len(), 1);
    }
}
