//! GCS sink executor: the shared [`FileSink`] over a [`GcsBackend`]
//! (RFC 0013, #886).

use crate::config::GcsSinkConfig;
use faucet_common_file::write::{FileSink, SinkIdentity};
use faucet_common_gcs::{GcsBackend, build_storage, build_storage_control};
use faucet_core::{FaucetError, FileFormat};
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
    inner: FileSink<GcsBackend>,
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
        let backend = GcsBackend::builder(storage, control, config.bucket.clone())
            .concurrency(config.concurrency)
            .scratch_dir(config.scratch_dir.as_deref().map(std::path::Path::new))
            .build(base, &template)?;
        let identity = GcsIdentity {
            bucket: config.bucket.clone(),
            key: object_key_layout(&config.prefix, config.path.as_deref()),
        };
        Ok(Self {
            inner: FileSink::build(settings, template, Arc::new(backend), identity)?,
        })
    }

    /// The format objects are written in.
    pub fn format(&self) -> FileFormat {
        self.inner.format()
    }
}

impl GcsSink {
    /// Join the overwrite run `token`
    /// ([`faucet_core::idempotency::OVERWRITE_RUN_KEY`]): every instance of
    /// one run (begin, writers, commit) stages in that run's own swap area,
    /// so a concurrent overwrite run of the same output neither drops nor
    /// moves in the other's files.
    pub fn with_overwrite_run(mut self, token: &str) -> Result<Self, FaucetError> {
        self.inner = self.inner.with_overwrite_run(token)?;
        Ok(self)
    }
}

faucet_common_file::delegate_sink!(GcsSink, inner);

/// What the GCS sink supplies to the shared sink; requests, metering and the
/// preflight probe are the backend's.
struct GcsIdentity {
    bucket: String,
    key: String,
}

impl SinkIdentity for GcsIdentity {
    fn connector_name(&self) -> &'static str {
        "gcs"
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(GcsSinkConfig)).expect("schema serialization")
    }

    fn dataset_uri(&self) -> String {
        format!("gs://{}/{}", self.bucket, self.key)
    }
}

/// The object-key layout a run writes under: `prefix` followed by the `path`
/// template, so two pipelines writing different paths are different datasets.
fn object_key_layout(prefix: &str, path: Option<&str>) -> String {
    format!("{prefix}{}", path.unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::Sink as _;

    #[tokio::test]
    async fn the_sink_is_a_file_sink_over_gcs() {
        let config: GcsSinkConfig = serde_json::from_value(serde_json::json!({
            "bucket": "b", "prefix": "p/", "storage_host": "http://127.0.0.1:9",
            "auth": {"type": "anonymous"}
        }))
        .unwrap();
        let sink = GcsSink::new(config).await.unwrap();
        assert_eq!(sink.connector_name(), "gcs");
        assert_eq!(sink.dataset_uri(), "gs://b/p/");
        assert_eq!(sink.format(), FileFormat::JsonLines);
        assert_eq!(sink.inner.backend().bucket(), "b");
        assert!(sink.config_schema().is_object());
    }

    #[tokio::test]
    async fn dataset_uri_names_the_path_template() {
        let uri = |path: &str| {
            let config: GcsSinkConfig = serde_json::from_value(serde_json::json!({
                "bucket": "b", "prefix": "p/", "path": path, "storage_host": "http://127.0.0.1:9", "auth": {"type": "anonymous"}
            }))
            .unwrap();
            async move { GcsSink::new(config).await.unwrap().dataset_uri() }
        };
        let orders = uri("orders/part-{part}.parquet").await;
        let users = uri("users.csv").await;
        assert_eq!(orders, "gs://b/p/orders/part-{part}.parquet");
        assert_eq!(users, "gs://b/p/users.csv");
    }
}
