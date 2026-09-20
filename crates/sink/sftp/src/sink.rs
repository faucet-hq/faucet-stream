//! SFTP sink executor.
//!
//! Writes each `write_batch` chunk as a JSON Lines object under a remote
//! directory. Writes are **atomic**: each object is uploaded to a hidden
//! temporary name and then renamed to its final name, so a consumer watching
//! the directory never observes a partially-written file. Construction is lazy
//! — [`SftpSink::new`] performs no I/O; the SSH transport is opened on the
//! first `write_batch` and reused for the life of the sink.

use crate::config::SftpSinkConfig;
use async_trait::async_trait;
use faucet_common_sftp::{SftpSession, connect};
use faucet_core::FaucetError;
use russh_sftp::protocol::OpenFlags;
use serde_json::Value;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

/// A sink that writes JSON records to an SFTP server as JSON Lines objects.
pub struct SftpSink {
    config: SftpSinkConfig,
    /// Reused SFTP session, opened on first write. Behind a `Mutex` so writes
    /// share one SSH connection instead of reconnecting per page.
    session: Mutex<Option<SftpSession>>,
    /// Rows accumulated across `write_batch` calls for the open file (#618).
    ///
    /// Without this the sink wrote one file per upstream page, so a small
    /// `batch_size` produced a directory of tiny files.
    open: Mutex<faucet_core::ObjectAccumulator>,
    /// Records held for a **whole-file** format (#604). CSV has a header, XML
    /// a document element, a workbook a container index and a JSON array its
    /// brackets — none can be appended a record at a time, so their records
    /// are buffered and encoded together at the rollover. Always empty for
    /// JSON Lines, which streams through `open`.
    pending: Mutex<faucet_core::object_rollover::PageAccumulator>,
}

impl SftpSink {
    /// Create a new SFTP sink. Lazy: performs no I/O and never connects here.
    /// The batch size is validated up front so a bad config fails fast.
    pub fn new(config: SftpSinkConfig) -> Result<Self, FaucetError> {
        faucet_core::validate_batch_size(config.batch_size)?;
        let open = Mutex::new(faucet_core::ObjectAccumulator::new(
            config.max_records_per_file.or(match config.batch_size {
                0 => None,
                n => Some(n),
            }),
            config.max_bytes_per_file,
        ));
        let pending = Mutex::new(faucet_core::object_rollover::PageAccumulator::new(
            config.max_records_per_file.or(match config.batch_size {
                0 => None,
                n => Some(n),
            }),
            config.max_bytes_per_file,
        ));
        Ok(Self {
            config,
            session: Mutex::new(None),
            open,
            pending,
        })
    }

    /// Join the configured directory prefix with a file name using POSIX `/`.
    fn join_path(&self, name: &str) -> String {
        let dir = &self.config.path;
        if dir.is_empty() {
            name.to_string()
        } else if dir.ends_with('/') {
            format!("{dir}{name}")
        } else {
            format!("{dir}/{name}")
        }
    }

    /// Generate the final object key: `{path}/{uuid}{ext}`.
    fn final_key(&self) -> String {
        let id = uuid::Uuid::new_v4();
        self.join_path(&format!("{id}{}", self.config.file_extension))
    }

    /// Encode one buffered group in the configured whole-file format and
    /// write it as a single file (#604).
    async fn write_encoded_file(&self, group: Vec<Value>) -> Result<(), FaucetError> {
        if group.is_empty() {
            return Ok(());
        }
        let format = self.config.format.shared();
        let rows = group.len();
        let body = faucet_core::file_format::encode(&group, format, &self.config.format_options())?;
        let mut guard = self.session.lock().await;
        if guard.is_none() {
            let sftp = connect(&self.config.connection).await?;
            if let Err(e) = sftp.create_dir(self.config.path.as_str()).await {
                tracing::debug!(path = %self.config.path, error = %e, "SFTP create_dir (best-effort)");
            }
            *guard = Some(sftp);
        }
        let sftp = guard.as_ref().expect("session initialized above");
        let key = self.final_key();
        Self::upload_atomic(sftp, &key, &body).await?;
        tracing::info!(path = %key, records = rows, format = format.as_str(), "SFTP file written");
        Ok(())
    }

    /// Upload `body` to `final_key` atomically: write a temporary object and
    /// rename it into place, so consumers never see a partial file.
    async fn upload_atomic(
        sftp: &SftpSession,
        final_key: &str,
        body: &[u8],
    ) -> Result<(), FaucetError> {
        let temp_key = format!("{final_key}.tmp");

        let mut file = sftp
            .open_with_flags(
                temp_key.as_str(),
                OpenFlags::CREATE | OpenFlags::WRITE | OpenFlags::TRUNCATE,
            )
            .await
            .map_err(|e| {
                FaucetError::Sink(format!("SFTP open '{temp_key}' for write failed: {e}"))
            })?;

        file.write_all(body)
            .await
            .map_err(|e| FaucetError::Sink(format!("SFTP write to '{temp_key}' failed: {e}")))?;
        file.flush()
            .await
            .map_err(|e| FaucetError::Sink(format!("SFTP flush of '{temp_key}' failed: {e}")))?;
        file.shutdown()
            .await
            .map_err(|e| FaucetError::Sink(format!("SFTP close of '{temp_key}' failed: {e}")))?;

        sftp.rename(temp_key.as_str(), final_key)
            .await
            .map_err(|e| {
                FaucetError::Sink(format!(
                    "SFTP rename '{temp_key}' -> '{final_key}' failed: {e}"
                ))
            })?;

        tracing::debug!(key = %final_key, "wrote SFTP object");
        Ok(())
    }
}

#[async_trait]
impl faucet_core::Sink for SftpSink {
    /// Close the open file (#618).
    ///
    /// The pipeline calls `flush` at every bookmark-carrying page and once at
    /// the end, so the remainder is uploaded before the bookmark advances — a
    /// file left unwritten after a "successful" run is data loss with a green
    /// exit code.
    async fn flush(&self) -> Result<(), FaucetError> {
        let finished = {
            let mut open = self.open.lock().await;
            open.finish()
        };
        let group = {
            let mut pending = self.pending.lock().await;
            pending.finish()
        };
        if let Some(group) = group {
            self.write_encoded_file(group).await?;
        }
        let Some(obj) = finished else {
            return Ok(());
        };
        let mut guard = self.session.lock().await;
        if guard.is_none() {
            let sftp = connect(&self.config.connection).await?;
            if let Err(e) = sftp.create_dir(self.config.path.as_str()).await {
                tracing::debug!(path = %self.config.path, error = %e, "SFTP create_dir (best-effort)");
            }
            *guard = Some(sftp);
        }
        let sftp = guard.as_ref().expect("session initialized above");
        let key = self.final_key();
        Self::upload_atomic(sftp, &key, &obj.body).await?;
        tracing::info!(path = %key, records = obj.rows, "SFTP file closed");
        Ok(())
    }

    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }

        // Whole-file formats (#604): a CSV header, an XML document element, a
        // workbook index and a JSON array's brackets all need every record
        // before any byte is final, so records are buffered and encoded
        // together. The same row/byte caps decide the rollover, so file sizing
        // means the same thing whatever the format.
        if !self.config.format.appends_per_record() {
            let group = {
                let mut pending = self.pending.lock().await;
                pending.push_page(records)
            };
            if let Some(group) = group {
                self.write_encoded_file(group).await?;
            }
            return Ok(records.len());
        }

        let mut guard = self.session.lock().await;
        if guard.is_none() {
            let sftp = connect(&self.config.connection).await?;
            // Best-effort: ensure the target directory exists. Ignore errors
            // (it usually already exists; a real permission/path problem
            // surfaces on the first write with a clear message).
            if let Err(e) = sftp.create_dir(self.config.path.as_str()).await {
                tracing::debug!(path = %self.config.path, error = %e, "SFTP create_dir (best-effort)");
            }
            *guard = Some(sftp);
        }
        let sftp = guard.as_ref().expect("session initialized above");

        // Accumulate across calls and roll on the record/byte cap (#618). A
        // page smaller than the cap joins the open file rather than becoming a
        // file of its own. `batch_size` still sizes files when no explicit
        // `max_records_per_file` is given, so an existing config keeps the
        // file size it asked for.
        let mut files = 0usize;
        {
            let mut open = self.open.lock().await;
            for record in records {
                if let faucet_core::object_rollover::Emit::Object(obj) = open.push_record(record)? {
                    let key = self.final_key();
                    Self::upload_atomic(sftp, &key, &obj.body).await?;
                    files += 1;
                }
            }
        }

        tracing::debug!(records = records.len(), files, "SFTP batch accumulated");
        Ok(records.len())
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(SftpSinkConfig))
            .expect("schema serialization")
    }

    fn connector_name(&self) -> &'static str {
        "sftp"
    }

    fn dataset_uri(&self) -> String {
        format!(
            "sftp://{}:{}/{}",
            self.config.connection.host,
            self.config.connection.port,
            self.config.path.trim_start_matches('/')
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_common_sftp::SftpConnectionConfig;
    use faucet_core::Sink;

    fn cfg() -> SftpSinkConfig {
        SftpSinkConfig::new(SftpConnectionConfig::with_password("h", "u", "p"), "/out")
    }

    #[test]
    fn new_is_lazy_and_validates_batch_size() {
        assert!(SftpSink::new(cfg()).is_ok());
        let bad = cfg().with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        assert!(matches!(SftpSink::new(bad), Err(FaucetError::Config(_))));
    }

    /// The NDJSON encoding moved into `faucet_core::ObjectAccumulator` with
    /// the cross-page accumulation (#618) — pinned here too, because this is
    /// what lands on the remote filesystem.
    #[test]
    fn records_write_as_newline_delimited_json() {
        let mut acc = faucet_core::ObjectAccumulator::new(Some(2), None);
        acc.push_record(&serde_json::json!({"id": 1, "name": "Alice"}))
            .unwrap();
        let faucet_core::object_rollover::Emit::Object(obj) = acc
            .push_record(&serde_json::json!({"id": 2, "name": "Bob"}))
            .unwrap()
        else {
            panic!("rolled at 2 records");
        };
        let text = String::from_utf8(obj.body).unwrap();
        let lines: Vec<&str> = text.trim().split('\n').collect();
        assert_eq!(lines.len(), 2);
        let first: Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(first["id"], 1);
    }

    #[test]
    fn an_empty_accumulator_writes_no_file() {
        // An empty page must not create an empty file on the remote host.
        let mut acc = faucet_core::ObjectAccumulator::new(Some(2), None);
        assert!(acc.finish().is_none());
    }

    #[test]
    fn join_path_handles_trailing_slash() {
        let sink = SftpSink::new(cfg()).unwrap();
        assert_eq!(sink.join_path("f.jsonl"), "/out/f.jsonl");

        let sink2 = SftpSink::new(SftpSinkConfig::new(
            SftpConnectionConfig::with_password("h", "u", "p"),
            "/out/",
        ))
        .unwrap();
        assert_eq!(sink2.join_path("f.jsonl"), "/out/f.jsonl");
    }

    #[test]
    fn final_key_uses_prefix_and_extension() {
        let sink = SftpSink::new(cfg()).unwrap();
        let key = sink.final_key();
        assert!(key.starts_with("/out/"), "got {key}");
        assert!(key.ends_with(".jsonl"), "got {key}");
    }

    #[test]
    fn connector_name_is_sftp() {
        let sink = SftpSink::new(cfg()).unwrap();
        assert_eq!(sink.connector_name(), "sftp");
    }

    #[test]
    fn dataset_uri_has_no_credentials() {
        let sink = SftpSink::new(cfg()).unwrap();
        assert_eq!(sink.dataset_uri(), "sftp://h:22/out");
    }

    #[test]
    fn append_only_capabilities() {
        let sink = SftpSink::new(cfg()).unwrap();
        assert!(!sink.supports_idempotent_writes());
        assert!(!sink.dedups_by_key());
        assert!(
            sink.supported_write_modes()
                .contains(&faucet_core::write_mode::WriteMode::Append)
        );
    }
}
