//! Range-reading Parquet adapter (#619).
//!
//! The Parquet formats used to buffer a whole object into `Bytes` before
//! decoding, so peak memory was the size of the largest object — on a
//! multi-gigabyte file, an OOM. Parquet is random-access by design: its footer
//! locates every row group, so a reader that can fetch byte ranges decodes one
//! row group at a time.
//!
//! `object_store`'s `ParquetObjectReader` does this, but this crate talks to S3
//! through `aws-sdk-s3` — with its own credential chain, `endpoint_url`, and
//! checksum options — so adopting `object_store` here would mean a second,
//! divergent way to authenticate. A ranged `GetObject` is the same primitive
//! with none of that cost.
//!
//! **A short range is an error, never a silent truncation.** S3 answers a
//! range request with only the bytes it has, so a truncated object would
//! otherwise surface as a corrupt-Parquet error at best and a wrong value at
//! worst. Every fetch asserts it got exactly the bytes it asked for.

use std::ops::Range;
use std::sync::Arc;

use aws_sdk_s3::Client;
use bytes::Bytes;
use futures::FutureExt;
use futures::future::BoxFuture;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::errors::ParquetError;
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};

/// Reads one S3 object's byte ranges on demand.
pub(crate) struct S3RangeReader {
    client: Client,
    bucket: String,
    key: String,
    len: u64,
}

impl S3RangeReader {
    /// Resolve the object's length (one `HeadObject`) so the Parquet footer can
    /// be located. Cheaper than the whole-object `GetObject` it replaces even
    /// counting the extra round trip.
    pub(crate) async fn open(
        client: &Client,
        bucket: &str,
        key: &str,
    ) -> Result<Self, faucet_core::FaucetError> {
        let head = client
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| {
                faucet_core::FaucetError::Source(format!(
                    "S3 head object error for key '{key}': {e}"
                ))
            })?;
        let len = head.content_length().ok_or_else(|| {
            faucet_core::FaucetError::Source(format!(
                "S3 object '{key}' reports no Content-Length, so its Parquet footer cannot be \
                 located; set verify_checksum to read it as a whole object instead"
            ))
        })?;
        let len = u64::try_from(len).map_err(|_| {
            faucet_core::FaucetError::Source(format!(
                "S3 object '{key}' reports a negative Content-Length ({len})"
            ))
        })?;
        Ok(Self {
            client: client.clone(),
            bucket: bucket.to_string(),
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
        let header = range_header(&range);
        let response = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&self.key)
            .range(&header)
            .send()
            .await
            .map_err(|e| failed(&self.key, &header, "", e))?;
        let data = response
            .body
            .collect()
            .await
            .map_err(|e| failed(&self.key, &header, " mid-stream", e))?;
        let bytes = data.into_bytes();
        if bytes.len() as u64 != wanted {
            return Err(short(&self.key, &header, bytes.len() as u64, wanted));
        }
        Ok(bytes)
    }
}

/// The inclusive-on-both-ends HTTP range header for `range`.
///
/// Its own function because an off-by-one here reads the wrong bytes rather
/// than failing — the one arithmetic mistake in this file that would corrupt
/// data instead of erroring.
fn range_header(range: &Range<u64>) -> String {
    format!("bytes={}-{}", range.start, range.end.saturating_sub(1))
}

/// A range read that failed outright.
fn failed(key: &str, header: &str, when: &str, cause: impl std::fmt::Display) -> ParquetError {
    ParquetError::External(Box::new(std::io::Error::other(format!(
        "S3 ranged read for '{key}' ({header}) failed{when}: {cause}"
    ))))
}

/// A range read that returned fewer bytes than were asked for.
///
/// Its own error rather than a truncated buffer handed to the Parquet decoder:
/// silently short bytes read as corrupt column data, which is a wrong-value bug
/// rather than a failure.
fn short(key: &str, header: &str, got: u64, wanted: u64) -> ParquetError {
    ParquetError::External(Box::new(std::io::Error::other(format!(
        "S3 returned {got} bytes for '{key}' range {header} but {wanted} were requested — the \
         object is truncated or was replaced mid-read"
    ))))
}

impl AsyncFileReader for S3RangeReader {
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

    #[test]
    fn a_range_header_is_inclusive_on_both_ends() {
        // `0..4` is four bytes: 0,1,2,3. Emitting `bytes=0-4` would read a
        // fifth byte and shift every subsequent column value.
        assert_eq!(range_header(&(0..4)), "bytes=0-3");
        assert_eq!(range_header(&(1_000..1_001)), "bytes=1000-1000");
    }

    #[test]
    fn a_failed_range_names_the_object_and_the_bytes() {
        // Without both, a partial-read bug in a multi-gigabyte scan is
        // undiagnosable.
        let msg = failed("o.parquet", "bytes=128-255", "", "connection refused").to_string();
        assert!(msg.contains("o.parquet"), "{msg}");
        assert!(msg.contains("bytes=128-255"), "{msg}");
        assert!(msg.contains("connection refused"), "{msg}");

        let mid = failed("o.parquet", "bytes=0-63", " mid-stream", "reset").to_string();
        assert!(
            mid.contains("mid-stream"),
            "a failure part-way through must say so: {mid}"
        );
    }

    #[test]
    fn a_short_range_reports_both_counts() {
        let msg = short("o.parquet", "bytes=1000-1999", 400, 1_000).to_string();
        assert!(msg.contains("400 bytes"), "{msg}");
        assert!(msg.contains("1000 were requested"), "{msg}");
        assert!(
            msg.contains("truncated"),
            "the message must say what it means: {msg}"
        );
    }
}
