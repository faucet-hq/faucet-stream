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
            .map_err(|e| {
                ParquetError::External(Box::new(std::io::Error::other(format!(
                    "GCS ranged read for '{}' ({}..{}) failed: {e}",
                    self.key, range.start, range.end
                ))))
            })?;

        let mut buf = Vec::with_capacity(wanted as usize);
        let mut stream = resp.into_stream();
        while let Some(chunk) = stream.try_next().await.map_err(|e| {
            ParquetError::External(Box::new(std::io::Error::other(format!(
                "GCS ranged read for '{}' ({}..{}) failed mid-stream: {e}",
                self.key, range.start, range.end
            ))))
        })? {
            buf.extend_from_slice(&chunk);
        }
        if buf.len() as u64 != wanted {
            return Err(ParquetError::External(Box::new(std::io::Error::other(
                format!(
                    "GCS returned {} bytes for '{}' range {}..{} but {wanted} were requested — \
                     the object is truncated or was replaced mid-read",
                    buf.len(),
                    self.key,
                    range.start,
                    range.end
                ),
            ))));
        }
        Ok(Bytes::from(buf))
    }
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
