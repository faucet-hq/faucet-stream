#![cfg_attr(docsrs, feature(doc_cfg))]
//! Shared S3 pieces for the faucet-stream connectors that write to S3: the
//! client builder and [`S3Backend`], the
//! [`StorageBackend`](faucet_common_file::write::StorageBackend) the shared
//! file writer publishes objects through (RFC 0013, #886).
//!
//! The S3 sink is a `FileSink<S3Backend>`, and so is the deprecated Parquet
//! sink's S3 destination, so both write every format, compression,
//! encryption, rollover and overwrite mode the local file sink does, with
//! one implementation.
//!
//! **Experimental** (PRINCIPLES.md §3): this block's shape may change in a
//! minor release; any change is called out in the changelog.

mod backend;
mod objects;
mod parts;

pub use backend::{S3Backend, S3BackendBuilder};
pub use objects::{MAX_COPY_BYTES, MAX_PARTS, PART_BYTES};

/// An S3 client from the default AWS credential chain, in `region` (else the
/// chain's region) and against `endpoint_url` (an S3-compatible store) when
/// set.
pub async fn build_client(region: Option<&str>, endpoint_url: Option<&str>) -> aws_sdk_s3::Client {
    let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
    if let Some(region) = region {
        loader = loader.region(aws_config::Region::new(region.to_string()));
    }
    if let Some(endpoint) = endpoint_url {
        loader = loader.endpoint_url(endpoint);
    }
    aws_sdk_s3::Client::new(&loader.load().await)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn the_client_takes_the_region_and_endpoint() {
        let client = build_client(Some("eu-west-1"), Some("http://127.0.0.1:9")).await;
        let conf = client.config();
        assert_eq!(conf.region().map(|r| r.as_ref()), Some("eu-west-1"));
        let _ = build_client(None, None).await;
    }
}
