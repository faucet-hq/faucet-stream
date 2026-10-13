//! S3 sink executor: the shared [`FileSink`] over an [`S3Backend`]
//! (RFC 0013, #886).

use crate::config::S3SinkConfig;
use faucet_common_file::write::{FileSink, SinkIdentity};
use faucet_common_s3::S3Backend;
use faucet_core::{FaucetError, FileFormat};
use serde_json::Value;
use std::sync::Arc;

/// A sink that writes records to S3 objects in any format the local file
/// sink writes — JSON Lines, JSON, CSV, XML, Excel, Avro, Parquet or raw
/// text — through the shared file writer.
///
/// JSON Lines and raw text go up as a multipart upload while they are
/// written, so neither memory nor local disk grows with the object; every
/// other format is built in a local scratch file and published with one
/// upload (multipart past 8 MiB) when it closes: at the row / byte cap or at
/// `flush`. Up to `concurrency` uploads (objects and parts) run at once,
/// while the next data is encoded; `flush` waits for all of them, so a
/// bookmark never advances past an object that is not in the bucket.
pub struct S3Sink {
    inner: FileSink<S3Backend>,
}

impl S3Sink {
    /// Create a new S3 sink from the given configuration.
    ///
    /// Builds the S3 client eagerly so it is reused across calls.
    pub async fn new(config: S3SinkConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        let client = faucet_common_s3::build_client(
            config.region.as_deref(),
            config.endpoint_url.as_deref(),
        )
        .await;
        Self::with_client(config, client)
    }

    fn with_client(config: S3SinkConfig, client: aws_sdk_s3::Client) -> Result<Self, FaucetError> {
        let write = config.write_config();
        let settings = write.settings()?;
        let (base, template) = write.object_layout(&settings)?;
        let backend = S3Backend::builder(client, config.bucket.clone())
            .concurrency(config.concurrency)
            .scratch_dir(config.scratch_dir.as_deref().map(std::path::Path::new))
            .build(base, &template)?;
        let identity = S3Identity {
            bucket: config.bucket.clone(),
            prefix: config.prefix.clone(),
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

faucet_common_file::delegate_sink!(S3Sink, inner);

/// What the S3 sink supplies to the shared sink; requests, metering and the
/// preflight probe are the backend's.
struct S3Identity {
    bucket: String,
    prefix: String,
}

impl SinkIdentity for S3Identity {
    fn connector_name(&self) -> &'static str {
        "s3"
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(S3SinkConfig)).expect("schema serialization")
    }

    fn dataset_uri(&self) -> String {
        format!("s3://{}/{}", self.bucket, self.prefix)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::Sink as _;

    #[tokio::test]
    async fn the_sink_is_a_file_sink_over_s3() {
        let config: S3SinkConfig = serde_json::from_value(serde_json::json!({
            "bucket": "b", "prefix": "p/", "region": "us-east-1",
            "endpoint_url": "http://127.0.0.1:9"
        }))
        .unwrap();
        let sink = S3Sink::new(config).await.unwrap();
        assert_eq!(sink.connector_name(), "s3");
        assert_eq!(sink.dataset_uri(), "s3://b/p/");
        assert_eq!(sink.format(), FileFormat::JsonLines);
        assert_eq!(sink.inner.backend().bucket(), "b");
        assert!(sink.config_schema().is_object());
    }
}
