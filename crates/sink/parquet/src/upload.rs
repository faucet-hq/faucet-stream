//! The object upload behind the Parquet writer, abortable after any failure.
//!
//! `object_store`'s `BufWriter` cannot abort once shutdown has begun, and its
//! shutdown does not abort when an in-flight part fails, so a failed close
//! could leave an S3 multipart's parts behind (billed, invisible). This writer
//! keeps the multipart in hand until it completes, so it can always abort.

use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt as _;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use object_store::path::Path as ObjPath;
use object_store::{MultipartUpload, ObjectStore, ObjectStoreExt as _, UploadPart};
use parquet::arrow::async_writer::AsyncFileWriter;
use parquet::errors::ParquetError;

/// Bytes buffered before a multipart upload starts (smaller files go up as one
/// `put`), and the size of each part after that.
const PART_SIZE: usize = 10 * 1024 * 1024;
/// Parts in flight at once.
const MAX_CONCURRENT_PARTS: usize = 8;

struct Multipart {
    upload: Box<dyn MultipartUpload>,
    buffer: Vec<u8>,
    in_flight: FuturesUnordered<UploadPart>,
}

impl Multipart {
    fn send_part(&mut self) {
        let part = std::mem::take(&mut self.buffer);
        self.in_flight.push(self.upload.put_part(part.into()));
    }

    async fn wait_until_below(&mut self, limit: usize) -> Result<(), object_store::Error> {
        while self.in_flight.len() > limit {
            if let Some(done) = self.in_flight.next().await {
                done?;
            }
        }
        Ok(())
    }

    async fn abort(mut self) -> Result<(), object_store::Error> {
        self.in_flight.clear();
        self.upload.abort().await
    }
}

enum State {
    Buffer(Vec<u8>),
    Multipart(Multipart),
    Done,
}

/// An [`AsyncFileWriter`] over one object that can always be aborted.
pub(crate) struct AbortableUpload {
    store: Arc<dyn ObjectStore>,
    path: ObjPath,
    part_size: usize,
    state: State,
}

fn external(e: object_store::Error) -> ParquetError {
    ParquetError::External(Box::new(e))
}

impl AbortableUpload {
    pub(crate) fn new(store: Arc<dyn ObjectStore>, path: ObjPath) -> Self {
        Self::with_part_size(store, path, PART_SIZE)
    }

    pub(crate) fn with_part_size(
        store: Arc<dyn ObjectStore>,
        path: ObjPath,
        part_size: usize,
    ) -> Self {
        Self {
            store,
            path,
            part_size,
            state: State::Buffer(Vec::new()),
        }
    }

    /// Discard the upload: abort a started multipart, drop a buffer. A no-op
    /// once the object completed or was already aborted.
    pub(crate) async fn abort(&mut self) -> Result<(), object_store::Error> {
        match std::mem::replace(&mut self.state, State::Done) {
            State::Multipart(upload) => upload.abort().await,
            State::Buffer(_) | State::Done => Ok(()),
        }
    }
}

impl AsyncFileWriter for AbortableUpload {
    fn write(&mut self, bs: Bytes) -> BoxFuture<'_, parquet::errors::Result<()>> {
        Box::pin(async move {
            match &mut self.state {
                State::Buffer(buf) => {
                    buf.extend_from_slice(&bs);
                    if buf.len() >= self.part_size {
                        let buffer = std::mem::take(buf);
                        let upload = self
                            .store
                            .put_multipart(&self.path)
                            .await
                            .map_err(external)?;
                        let mut multipart = Multipart {
                            upload,
                            buffer,
                            in_flight: FuturesUnordered::new(),
                        };
                        multipart.send_part();
                        self.state = State::Multipart(multipart);
                    }
                    Ok(())
                }
                State::Multipart(upload) => {
                    upload.buffer.extend_from_slice(&bs);
                    if upload.buffer.len() >= self.part_size {
                        upload
                            .wait_until_below(MAX_CONCURRENT_PARTS - 1)
                            .await
                            .map_err(external)?;
                        upload.send_part();
                    }
                    Ok(())
                }
                State::Done => Err(ParquetError::General(
                    "parquet upload already finished".into(),
                )),
            }
        })
    }

    fn complete(&mut self) -> BoxFuture<'_, parquet::errors::Result<()>> {
        Box::pin(async move {
            match std::mem::replace(&mut self.state, State::Done) {
                State::Buffer(buf) => {
                    self.store
                        .put(&self.path, buf.into())
                        .await
                        .map_err(external)?;
                    Ok(())
                }
                State::Multipart(mut upload) => {
                    if !upload.buffer.is_empty() {
                        upload.send_part();
                    }
                    let finished = match upload.wait_until_below(0).await {
                        Ok(()) => upload.upload.complete().await.map(|_| ()),
                        Err(e) => Err(e),
                    };
                    if let Err(e) = finished {
                        if let Err(abort) = upload.abort().await {
                            tracing::warn!(error = %abort, "parquet sink: aborting the failed upload also failed");
                        }
                        return Err(external(e));
                    }
                    Ok(())
                }
                State::Done => Ok(()),
            }
        })
    }
}
