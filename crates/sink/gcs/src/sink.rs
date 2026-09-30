//! GCS sink executor: the shared file writer (#777) over a GCS backend.

use crate::config::GcsSinkConfig;
use crate::object::GcsObjects;
use faucet_common_file::write::{FileWriter, RemoteBackend, SinkIdentity, WriterSink};
use faucet_common_gcs::{build_storage, build_storage_control};
use faucet_core::{FaucetError, FileFormat};
use google_cloud_storage::client::StorageControl;
use serde_json::Value;
use std::sync::Arc;

/// A sink that writes records to GCS objects in any format the local file
/// sink writes — JSON Lines, JSON, CSV, XML, Excel, Avro, Parquet or raw
/// text — through the shared file writer.
///
/// Each object is built in a local scratch file (see `scratch_dir`) and
/// published with one upload (resumable past the client's threshold) when it
/// closes: at the row / byte cap or at `flush`. Uploads run in the
/// background, up to `concurrency` at a time, while the next object is
/// encoded; `flush` waits for all of them, so a bookmark never advances past
/// an object that is not in the bucket.
pub struct GcsSink {
    inner: WriterSink,
}

impl GcsSink {
    /// Create a new GCS sink, building the clients eagerly.
    pub async fn new(config: GcsSinkConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        let storage = build_storage(&config.auth, config.storage_host.as_deref()).await?;
        let control = build_storage_control(&config.auth, config.storage_host.as_deref()).await?;
        let write = config.write_config();
        let settings = write.settings()?;
        let (base, template) = write.object_layout(&settings)?;
        let roundtrips = Arc::new(faucet_core::observability::RecorderSlot::new());
        let objects = Arc::new(GcsObjects {
            storage,
            control: control.clone(),
            bucket: config.bucket.clone(),
            roundtrips: roundtrips.clone(),
        });
        let backend = RemoteBackend::new(
            objects,
            base,
            &template,
            config.scratch_dir.as_deref().map(std::path::Path::new),
        )?
        .with_upload_concurrency(config.concurrency);
        let writer = FileWriter::new(settings, template, Arc::new(backend))?;
        let identity = GcsIdentity {
            control,
            bucket: config.bucket.clone(),
            prefix: config.prefix.clone(),
            roundtrips,
        };
        Ok(Self {
            inner: WriterSink::new(writer, identity),
        })
    }

    /// The format objects are written in.
    pub fn format(&self) -> FileFormat {
        self.inner.format()
    }
}

faucet_common_file::delegate_sink!(GcsSink, inner);

/// What the GCS sink supplies to the shared sink.
struct GcsIdentity {
    control: StorageControl,
    bucket: String,
    prefix: String,
    roundtrips: Arc<faucet_core::observability::RecorderSlot>,
}

#[faucet_core::async_trait]
impl SinkIdentity for GcsIdentity {
    fn connector_name(&self) -> &'static str {
        "gcs"
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(GcsSinkConfig)).expect("schema serialization")
    }

    fn dataset_uri(&self) -> String {
        format!("gs://{}/{}", self.bucket, self.prefix)
    }

    fn set_roundtrip_recorder(&self, recorder: Arc<faucet_core::observability::RoundtripRecorder>) {
        self.roundtrips.install(recorder);
    }

    /// Preflight probe: a non-mutating `list_objects` call capped at one
    /// result confirms the bucket is reachable and the credentials work.
    async fn check(
        &self,
        ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::{CheckReport, Probe};
        let started = std::time::Instant::now();
        let request = self
            .control
            .list_objects()
            .set_parent(format!("projects/_/buckets/{}", self.bucket))
            .set_page_size(1_i32);
        let probe = match tokio::time::timeout(ctx.timeout, request.send()).await {
            Ok(Ok(_)) => Probe::pass("auth", started.elapsed()),
            Ok(Err(e)) => Probe::fail_hint(
                "auth",
                started.elapsed(),
                e.to_string(),
                "check bucket name, credentials, and network",
            ),
            Err(_) => Probe::fail("network", started.elapsed(), "timed out"),
        };
        Ok(CheckReport::single(probe))
    }
}
