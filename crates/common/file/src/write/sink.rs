//! [`WriterSink`]: the one `Sink` implementation over a [`FileWriter`],
//! shared by every file-writing sink. A connector supplies only what is its
//! own — its name, config schema, dataset URI and preflight probe — through
//! [`SinkIdentity`], and forwards its `Sink` impl with
//! [`delegate_sink!`](crate::delegate_sink).

use super::writer::FileWriter;
use faucet_core::check::{CheckContext, CheckReport};
use faucet_core::observability::RoundtripRecorder;
use faucet_core::{FaucetError, FileFormat, Sink, WriteMode};
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
    /// A non-mutating preflight probe. Default: not implemented.
    async fn check(&self, _ctx: &CheckContext) -> Result<CheckReport, FaucetError> {
        Ok(CheckReport::not_implemented())
    }
    /// A `(kind, config)` source that reads the output back. Default: none.
    fn readback_source(&self) -> Option<(String, Value)> {
        None
    }
    /// Install the round-trip recorder the connector's client reports to.
    /// Default: the connector counts nothing.
    fn set_roundtrip_recorder(&self, _recorder: Arc<RoundtripRecorder>) {}
}

/// A [`Sink`] over a [`FileWriter`]: batch writes, flush, the overwrite
/// lifecycle, the end-of-run prune and the columnar path, in one place.
///
/// **Experimental** (PRINCIPLES.md §3): this block's shape may change in a
/// minor release; any change is called out in the changelog.
pub struct WriterSink {
    writer: FileWriter,
    identity: Box<dyn SinkIdentity>,
}

impl WriterSink {
    /// A sink writing through `writer`, described by `identity`.
    pub fn new(writer: FileWriter, identity: impl SinkIdentity + 'static) -> Self {
        Self {
            writer,
            identity: Box::new(identity),
        }
    }

    /// The writer.
    pub fn writer(&self) -> &FileWriter {
        &self.writer
    }

    /// The format the output is written in.
    pub fn format(&self) -> FileFormat {
        self.writer.settings().format
    }
}

#[faucet_core::async_trait]
impl Sink for WriterSink {
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
        self.identity.set_roundtrip_recorder(recorder);
    }

    async fn check(&self, ctx: &CheckContext) -> Result<CheckReport, FaucetError> {
        self.identity.check(ctx).await
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

/// Implement `Sink` for a connector type by forwarding every method to its
/// [`WriterSink`] field: `delegate_sink!(S3Sink, inner);`. The columnar
/// methods are forwarded when the calling crate's `arrow` feature is on.
#[macro_export]
macro_rules! delegate_sink {
    ($ty:ty, $field:ident) => {
        #[$crate::__private::async_trait]
        impl $crate::__private::Sink for $ty {
            fn connector_name(&self) -> &'static str {
                $crate::__private::Sink::connector_name(&self.$field)
            }
            fn config_schema(&self) -> $crate::__private::Value {
                $crate::__private::Sink::config_schema(&self.$field)
            }
            fn dataset_uri(&self) -> ::std::string::String {
                $crate::__private::Sink::dataset_uri(&self.$field)
            }
            fn set_roundtrip_recorder(
                &self,
                recorder: ::std::sync::Arc<$crate::__private::RoundtripRecorder>,
            ) {
                $crate::__private::Sink::set_roundtrip_recorder(&self.$field, recorder)
            }
            async fn check(
                &self,
                ctx: &$crate::__private::CheckContext,
            ) -> ::std::result::Result<$crate::__private::CheckReport, $crate::__private::FaucetError>
            {
                $crate::__private::Sink::check(&self.$field, ctx).await
            }
            fn readback_source(
                &self,
            ) -> ::std::option::Option<(::std::string::String, $crate::__private::Value)> {
                $crate::__private::Sink::readback_source(&self.$field)
            }
            fn batch_atomicity(&self) -> $crate::__private::BatchAtomicity {
                $crate::__private::Sink::batch_atomicity(&self.$field)
            }
            fn supported_write_modes(&self) -> &'static [$crate::__private::WriteMode] {
                $crate::__private::Sink::supported_write_modes(&self.$field)
            }
            fn is_overwrite(&self) -> bool {
                $crate::__private::Sink::is_overwrite(&self.$field)
            }
            async fn begin_overwrite(
                &self,
            ) -> ::std::result::Result<(), $crate::__private::FaucetError> {
                $crate::__private::Sink::begin_overwrite(&self.$field).await
            }
            async fn commit_overwrite(
                &self,
            ) -> ::std::result::Result<(), $crate::__private::FaucetError> {
                $crate::__private::Sink::commit_overwrite(&self.$field).await
            }
            async fn abort_overwrite(
                &self,
            ) -> ::std::result::Result<(), $crate::__private::FaucetError> {
                $crate::__private::Sink::abort_overwrite(&self.$field).await
            }
            async fn overwrite_staging_exists(
                &self,
            ) -> ::std::result::Result<::std::option::Option<bool>, $crate::__private::FaucetError>
            {
                $crate::__private::Sink::overwrite_staging_exists(&self.$field).await
            }
            async fn complete_run(
                &self,
            ) -> ::std::result::Result<(), $crate::__private::FaucetError> {
                $crate::__private::Sink::complete_run(&self.$field).await
            }
            async fn local_outputs(&self) -> ::std::vec::Vec<$crate::__private::LocalOutput> {
                $crate::__private::Sink::local_outputs(&self.$field).await
            }
            async fn write_batch(
                &self,
                records: &[$crate::__private::Value],
            ) -> ::std::result::Result<usize, $crate::__private::FaucetError> {
                $crate::__private::Sink::write_batch(&self.$field, records).await
            }
            async fn flush(&self) -> ::std::result::Result<(), $crate::__private::FaucetError> {
                $crate::__private::Sink::flush(&self.$field).await
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
