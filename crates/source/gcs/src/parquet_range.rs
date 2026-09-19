//! Range-reading Parquet adapter (#619).
//!
//! The mirror of the s3 source's adapter, and for the same reason: Parquet
//! objects used to be buffered whole before decoding, so peak memory was the
//! size of the largest object. Parquet's footer locates every row group, so a
//! reader that can fetch byte ranges decodes one row group at a time.
//!
//! `object_store`'s `ParquetObjectReader` does this, but this crate talks to
//! GCS through `google-cloud-storage` — with its own credentials and
//! host override — so adopting `object_store` here would mean a second,
//! divergent way to authenticate. A ranged `read_object` is the same primitive
//! without that cost.
//!
//! **A short range is an error, never a silent truncation:** every fetch
//! asserts it received exactly the bytes it asked for, which is the same guard
//! `verify_length` gives the whole-object path.

use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{FutureExt, TryStreamExt};
use google_cloud_storage::client::Storage;
use google_cloud_storage::model_ext::ReadRange;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::errors::ParquetError;
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};

/// Reads one GCS object's byte ranges on demand.
pub(crate) struct GcsRangeReader {
    storage: Storage,
    bucket_path: String,
    key: String,
    len: u64,
}

impl GcsRangeReader {
    /// Resolve the object's length (one zero-length read, which still returns
    /// the object metadata) so the Parquet footer can be located.
    pub(crate) async fn open(
        storage: &Storage,
        bucket_path: &str,
        key: &str,
        len: u64,
    ) -> Result<Self, faucet_core::FaucetError> {
        if len == 0 {
            return Err(faucet_core::FaucetError::Source(format!(
                "GCS object '{key}' reports a zero size, so its Parquet footer cannot be \
                 located"
            )));
        }
        let _ = storage;
        Ok(Self {
            storage: storage.clone(),
            bucket_path: bucket_path.to_string(),
            key: key.to_string(),
            len,
        })
    }

    pub(crate) fn len(&self) -> u64 {
        self.len
    }

    async fn fetch(&self, range: Range<u64>) -> Result<Bytes, ParquetError> {
        let wanted = range.end.saturating_sub(range.start);
        if wanted == 0 {
            return Ok(Bytes::new());
        }
        let resp = self
            .storage
            .read_object(&self.bucket_path, self.key.clone())
            .set_read_range(ReadRange::segment(range.start, wanted))
            .send()
            .await
            .map_err(|e| failed(&self.key, &range, "", e))?;

        let mut buf = Vec::with_capacity(wanted as usize);
        let mut stream = resp.into_stream();
        while let Some(chunk) = stream
            .try_next()
            .await
            .map_err(|e| failed(&self.key, &range, " mid-stream", e))?
        {
            buf.extend_from_slice(&chunk);
        }
        if buf.len() as u64 != wanted {
            return Err(short(&self.key, &range, buf.len() as u64));
        }
        Ok(Bytes::from(buf))
    }
}

/// A range read that failed outright.
fn failed(
    key: &str,
    range: &Range<u64>,
    when: &str,
    cause: impl std::fmt::Display,
) -> ParquetError {
    ParquetError::External(Box::new(std::io::Error::other(format!(
        "GCS ranged read for '{key}' ({}..{}) failed{when}: {cause}",
        range.start, range.end
    ))))
}

/// A range read that returned fewer bytes than were asked for.
///
/// Its own error rather than a truncated buffer handed to the Parquet decoder:
/// silently short bytes read as corrupt column data, which is a wrong-value
/// bug rather than a failure.
fn short(key: &str, range: &Range<u64>, got: u64) -> ParquetError {
    let wanted = range.end.saturating_sub(range.start);
    ParquetError::External(Box::new(std::io::Error::other(format!(
        "GCS returned {got} bytes for '{key}' range {}..{} but {wanted} were requested — the \
         object is truncated or was replaced mid-read",
        range.start, range.end
    ))))
}

impl AsyncFileReader for GcsRangeReader {
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

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_common_gcs::{GcsCredentials, build_storage};

    /// A client pointed at a closed port. Building one contacts nothing, which
    /// is enough to drive the reader's non-I/O paths.
    ///
    /// This crate's emulator suite is `#[ignore]`d because `fake-gcs-server`
    /// speaks REST while the connector uses the gRPC data plane, so these unit
    /// tests are the only automated cover this adapter gets. The I/O itself is
    /// kept a thin shim over `read_object` for exactly that reason: what is
    /// worth asserting — the failure messages and the range arithmetic — is
    /// pure.
    async fn offline_storage() -> Storage {
        build_storage(&GcsCredentials::Anonymous, Some("http://127.0.0.1:1"))
            .await
            .expect("an anonymous client builds without contacting GCS")
    }

    #[tokio::test]
    async fn a_zero_length_object_is_refused_by_name() {
        let storage = offline_storage().await;
        let msg = match GcsRangeReader::open(&storage, "projects/_/buckets/b", "empty.parquet", 0)
            .await
        {
            Err(e) => e.to_string(),
            Ok(_) => panic!("a zero-length object has no Parquet footer to read"),
        };
        assert!(
            msg.contains("empty.parquet") && msg.contains("footer"),
            "the error must name the object and the reason: {msg}"
        );
    }

    #[tokio::test]
    async fn an_empty_range_needs_no_request() {
        // The Parquet reader asks for zero-length ranges at file boundaries; a
        // round trip for nothing would be wasteful, and against a closed port
        // it would also be an error — so this doubles as proof no request is
        // made.
        let storage = offline_storage().await;
        let reader = GcsRangeReader::open(&storage, "projects/_/buckets/b", "o.parquet", 1_024)
            .await
            .expect("opens");
        assert_eq!(reader.len(), 1_024);
        assert!(reader.fetch(10..10).await.expect("no request").is_empty());
    }

    #[test]
    fn a_failed_range_names_the_object_and_the_bytes() {
        // Without both, a partial-read bug in a multi-gigabyte scan is
        // undiagnosable.
        let msg = failed("o.parquet", &(128..256), "", "connection refused").to_string();
        assert!(msg.contains("o.parquet"), "{msg}");
        assert!(msg.contains("128..256"), "{msg}");
        assert!(msg.contains("connection refused"), "{msg}");

        let mid = failed("o.parquet", &(0..64), " mid-stream", "reset").to_string();
        assert!(
            mid.contains("mid-stream"),
            "a failure part-way through must say so: {mid}"
        );
    }

    #[test]
    fn a_short_range_reports_both_counts() {
        let msg = short("o.parquet", &(1_000..2_000), 400).to_string();
        assert!(msg.contains("400 bytes"), "{msg}");
        assert!(msg.contains("1000 were requested"), "{msg}");
        assert!(
            msg.contains("truncated"),
            "the message must say what it means: {msg}"
        );
    }
}
