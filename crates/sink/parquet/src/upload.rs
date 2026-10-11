//! The object upload behind the Parquet writer, abortable after any failure.
//!
//! `object_store`'s `BufWriter` cannot abort once shutdown has begun, and its
//! shutdown does not abort when an in-flight part fails, so a failed close
//! could leave an S3 multipart's parts behind (billed, invisible). This writer
//! keeps the multipart in hand until it completes, so it can always abort.

use std::sync::Arc;

use bytes::Bytes;
use futures::future::BoxFuture;
use object_store::path::Path as ObjPath;
use object_store::{MultipartUpload, ObjectStore, ObjectStoreExt as _};
use parquet::arrow::async_writer::AsyncFileWriter;
use parquet::errors::ParquetError;
use tokio::task::JoinSet;

/// Bytes buffered before a multipart upload starts (smaller files go up as one
/// `put`), and the size of each part after that.
const PART_SIZE: usize = 10 * 1024 * 1024;
/// Parts in flight at once.
const MAX_CONCURRENT_PARTS: usize = 8;

struct Multipart {
    upload: Box<dyn MultipartUpload>,
    buffer: Vec<u8>,
    /// Parts uploading in the background while encoding continues; part
    /// futures are lazy, so each is spawned rather than merely queued.
    in_flight: JoinSet<Result<(), object_store::Error>>,
}

fn joined(
    done: Result<Result<(), object_store::Error>, tokio::task::JoinError>,
) -> Result<(), object_store::Error> {
    done.unwrap_or_else(|e| {
        Err(object_store::Error::Generic {
            store: "parquet sink",
            source: Box::new(e),
        })
    })
}

impl Multipart {
    fn send_part(&mut self) {
        let part = std::mem::take(&mut self.buffer);
        self.in_flight.spawn(self.upload.put_part(part.into()));
    }

    /// Surface a part that already failed without waiting for the others.
    fn reap_finished(&mut self) -> Result<(), object_store::Error> {
        while let Some(done) = self.in_flight.try_join_next() {
            joined(done)?;
        }
        Ok(())
    }

    async fn wait_until_below(&mut self, limit: usize) -> Result<(), object_store::Error> {
        while self.in_flight.len() > limit {
            if let Some(done) = self.in_flight.join_next().await {
                joined(done)?;
            }
        }
        Ok(())
    }

    async fn abort(mut self) -> Result<(), object_store::Error> {
        self.in_flight.abort_all();
        while self.in_flight.join_next().await.is_some() {}
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
                            in_flight: JoinSet::new(),
                        };
                        multipart.send_part();
                        self.state = State::Multipart(multipart);
                    }
                    Ok(())
                }
                State::Multipart(upload) => {
                    upload.reap_finished().map_err(external)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Debug, Default)]
    struct Counting {
        started: Arc<AtomicUsize>,
        fail: bool,
    }

    #[async_trait::async_trait]
    impl MultipartUpload for Counting {
        fn put_part(&mut self, _data: object_store::PutPayload) -> object_store::UploadPart {
            let started = self.started.clone();
            let fail = self.fail;
            Box::pin(async move {
                started.fetch_add(1, Ordering::SeqCst);
                if fail {
                    Err(object_store::Error::NotImplemented {
                        operation: "put_part".into(),
                        implementer: "test".into(),
                    })
                } else {
                    Ok(())
                }
            })
        }
        async fn complete(&mut self) -> object_store::Result<object_store::PutResult> {
            Ok(object_store::PutResult {
                e_tag: None,
                version: None,
            })
        }
        async fn abort(&mut self) -> object_store::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn parts_upload_in_the_background_and_failures_surface_early() {
        let started = Arc::new(AtomicUsize::new(0));
        let mut multipart = Multipart {
            upload: Box::new(Counting {
                started: started.clone(),
                fail: false,
            }),
            buffer: b"part".to_vec(),
            in_flight: JoinSet::new(),
        };
        multipart.send_part();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while started.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("a part starts uploading without being awaited");
        multipart.wait_until_below(0).await.unwrap();

        let mut failing = Multipart {
            upload: Box::new(Counting {
                started: Arc::new(AtomicUsize::new(0)),
                fail: true,
            }),
            buffer: b"part".to_vec(),
            in_flight: JoinSet::new(),
        };
        failing.send_part();
        let err = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if let Err(e) = failing.reap_finished() {
                    break e;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the failed part is reaped");
        assert!(err.to_string().contains("put_part"), "{err}");
        failing.send_part();
        failing.abort().await.unwrap();
    }

    #[test]
    fn a_panicked_part_is_an_upload_error() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let err = rt.block_on(async {
            let mut set: JoinSet<Result<(), object_store::Error>> = JoinSet::new();
            set.spawn(async { panic!("boom") });
            joined(set.join_next().await.unwrap()).unwrap_err()
        });
        assert!(err.to_string().contains("parquet sink"), "{err}");
    }
}
