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
    generation: Option<i64>,
    recorder: Option<Arc<faucet_core::observability::RoundtripRecorder>>,
    calls: faucet_core::CallContext,
}

impl GcsRangeReader {
    /// Resolve the object's length (one zero-length read, which still returns
    /// the object metadata) so the Parquet footer can be located.
    pub(crate) async fn open(
        storage: &Storage,
        bucket_path: &str,
        key: &str,
        len: u64,
        recorder: Option<Arc<faucet_core::observability::RoundtripRecorder>>,
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
            generation: None,
            recorder,
            calls: faucet_core::CallContext::new(),
        })
    }

    /// Make every range read through `calls` (#880).
    pub(crate) fn with_calls(mut self, calls: faucet_core::CallContext) -> Self {
        self.calls = calls;
        self
    }

    /// Read every range from object generation `generation` (the one whose
    /// size located the footer), so a replacement mid-read fails the read
    /// instead of mixing row groups from two versions. `0` (unknown) pins
    /// nothing.
    pub(crate) fn pinned_to(mut self, generation: i64) -> Self {
        self.generation = (generation != 0).then_some(generation);
        self
    }

    pub(crate) fn len(&self) -> u64 {
        self.len
    }

    async fn fetch(&self, range: Range<u64>) -> Result<Bytes, ParquetError> {
        check_range(&self.key, &range, self.len)?;
        let wanted = range.end - range.start;
        if wanted == 0 {
            return Ok(Bytes::new());
        }
        if let Some(r) = &self.recorder {
            r.record("get");
        }
        let mut request = self
            .storage
            .read_object(&self.bucket_path, self.key.clone())
            .set_read_range(ReadRange::segment(range.start, wanted));
        if let Some(generation) = self.generation {
            request = request.set_generation(generation);
        }
        let fetched = self
            .calls
            .call_once("get", async {
                let resp = match request.send().await {
                    Ok(r) => r,
                    Err(e) => return Ok(Err(failed(&self.key, &range, "", e))),
                };
                let mut buf = Vec::with_capacity(initial_capacity(wanted));
                let mut stream = resp.into_stream();
                loop {
                    match stream.try_next().await {
                        Ok(Some(chunk)) => {
                            buf.extend_from_slice(&chunk);
                            if buf.len() as u64 > wanted {
                                // More than asked for: stop rather than buffer
                                // whatever the server keeps sending.
                                break;
                            }
                        }
                        Ok(None) => break,
                        Err(e) => return Ok(Err(failed(&self.key, &range, " mid-stream", e))),
                    }
                }
                Ok(Ok(buf))
            })
            .await
            .map_err(|e| ParquetError::External(Box::new(e)))?;
        let buf = fetched?;
        if buf.len() as u64 != wanted {
            return Err(short(&self.key, &range, buf.len() as u64));
        }
        Ok(Bytes::from(buf))
    }
}

/// The most a range read reserves up front; a larger range grows as it arrives.
const MAX_INITIAL_CAPACITY: u64 = 8 * 1024 * 1024;

fn initial_capacity(wanted: u64) -> usize {
    usize::try_from(wanted.min(MAX_INITIAL_CAPACITY)).unwrap_or(0)
}

/// Refuse a range outside the object before reading or allocating anything.
/// The ranges come from the object's own footer, so a corrupt or hostile file
/// could otherwise ask for an allocation far larger than the object.
fn check_range(key: &str, range: &Range<u64>, len: u64) -> Result<(), ParquetError> {
    if range.start > range.end || range.end > len {
        return Err(ParquetError::General(format!(
            "'{key}' asks for bytes {}..{} but the object is {len} bytes — its Parquet \
             metadata is corrupt",
            range.start, range.end
        )));
    }
    Ok(())
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
    #[test]
    fn a_range_outside_the_object_is_refused_before_reading() {
        assert!(super::check_range("k", &(0..10), 10).is_ok());
        assert!(super::check_range("k", &(10..10), 10).is_ok());
        let err = super::check_range("k", &(0..(1 << 40)), 1_000_000).unwrap_err();
        assert!(err.to_string().contains("corrupt"), "{err}");
        #[allow(clippy::reversed_empty_ranges)]
        let reversed = 9..3;
        assert!(super::check_range("k", &reversed, 10).is_err());
        assert_eq!(super::initial_capacity(1 << 40), 8 * 1024 * 1024);
        assert_eq!(super::initial_capacity(100), 100);
    }

    use super::*;
    use faucet_common_gcs::{GcsCredentials, build_storage};

    /// A client pointed at a closed port. Building one contacts nothing, which
    /// is enough to drive the reader's non-I/O paths.
    ///
    /// The emulator suite covers the ranged reads end to end; these tests pin
    /// the failure messages and the range arithmetic, which are pure.
    async fn offline_storage() -> Storage {
        build_storage(&GcsCredentials::Anonymous, Some("http://127.0.0.1:1"))
            .await
            .expect("an anonymous client builds without contacting GCS")
    }

    #[tokio::test]
    async fn a_zero_length_object_is_refused_by_name() {
        let storage = offline_storage().await;
        let msg =
            match GcsRangeReader::open(&storage, "projects/_/buckets/b", "empty.parquet", 0, None)
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
        let reader =
            GcsRangeReader::open(&storage, "projects/_/buckets/b", "o.parquet", 1_024, None)
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

    /// A server that sends more than the range asked for: the read stops
    /// instead of buffering the rest, and the oversize is refused.
    #[test]
    fn an_overlong_range_response_is_cut_off_and_refused() {
        faucet_conformance::block_on_large_stack(async {
            use faucet_conformance::hung::{Reply, StallingBodyServer};
            let server = StallingBodyServer::start(|_method, _target| {
                Reply::full("application/octet-stream", vec![7u8; 64])
                    .header("x-goog-generation", "1")
                    .header("x-goog-metageneration", "1")
                    .header("x-goog-stored-content-length", "64")
                    .header("x-goog-storage-class", "STANDARD")
            })
            .await
            .expect("server");
            let storage = build_storage(&GcsCredentials::Anonymous, Some(&server.http_url()))
                .await
                .expect("storage");
            let reader =
                GcsRangeReader::open(&storage, "projects/_/buckets/b", "o.parquet", 1_024, None)
                    .await
                    .expect("opens");
            let msg = reader.fetch(0..10).await.unwrap_err().to_string();
            assert!(
                msg.contains("64 bytes") && msg.contains("o.parquet"),
                "{msg}"
            );
        });
    }
}
