//! The local file sink.

use crate::config::{FileMode, FileSinkConfig, FileWriteMode};
use crate::layout::{Layout, io_err};
use crate::writer::{Ctx, OpenFile, sync_dir};
use async_trait::async_trait;
use faucet_core::{Compression, FaucetError, FileFormat, FormatOptions, Sink, WriteMode};
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// Writes records to local files in any writable format.
///
/// Each file is written to `<name>.faucet-tmp` and renamed into place by
/// [`flush`](Sink::flush) (or by a rollover), so a file is complete or absent;
/// the pipeline advances a bookmark only after that flush. A sink dropped
/// without a flush removes its temporary files and leaves no final file.
pub struct FileSink {
    config: FileSinkConfig,
    format: FileFormat,
    codec: Compression,
    opts: FormatOptions,
    layout: Layout,
    state: Mutex<State>,
    outputs: faucet_core::LocalOutputLog,
}

#[derive(Default)]
struct State {
    current: Option<OpenFile>,
    /// Part number the next file opens with; `0` until the first open.
    next_part: u64,
}

impl FileSink {
    /// Build a sink, validating the config.
    pub fn new(config: FileSinkConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        let format = config.resolved_format()?;
        let codec = config.resolved_compression();
        if codec != Compression::None {
            faucet_core::warn_mismatch(&config.path, codec);
        }
        let layout = Layout::new(&config, format, codec)?;
        Ok(Self {
            opts: config.format_options(),
            format,
            codec,
            layout,
            config,
            state: Mutex::new(State::default()),
            outputs: faucet_core::LocalOutputLog::new(),
        })
    }

    /// The format this sink writes.
    pub fn format(&self) -> FileFormat {
        self.format
    }

    /// The config this sink was built from.
    pub fn config(&self) -> &FileSinkConfig {
        &self.config
    }

    fn ctx(&self) -> Ctx<'_> {
        Ctx {
            format: self.format,
            codec: self.codec,
            opts: &self.opts,
            parquet: &self.config.parquet,
        }
    }

    fn overwriting(&self) -> bool {
        self.config.write_mode == FileWriteMode::Overwrite
    }

    /// Directory this instance writes into: the destination, or the staging
    /// directory during an overwrite run.
    fn target_dir(&self) -> PathBuf {
        if self.overwriting() {
            self.layout.staging_dir()
        } else {
            self.layout.dir.clone()
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Make sure `dir` exists, creating it when allowed.
    fn ensure_dir(&self, dir: &Path, always_create: bool) -> Result<(), FaucetError> {
        if dir.is_dir() {
            return Ok(());
        }
        if !(self.config.create_dirs || always_create) {
            return Err(FaucetError::Sink(format!(
                "file sink: directory '{}' does not exist and `create_dirs` is off",
                dir.display()
            )));
        }
        std::fs::create_dir_all(dir).map_err(|e| io_err("creating directory", dir, e))
    }

    /// Open the next output file.
    fn open_next(&self, st: &mut State) -> Result<(), FaucetError> {
        let dir = self.target_dir();
        if st.next_part == 0 {
            self.ensure_dir(&self.layout.dir, false)?;
            if self.overwriting() {
                self.ensure_dir(&dir, true)?;
            }
            self.layout.remove_stale_temps(&dir);
            st.next_part = if self.config.mode == FileMode::Append && self.layout.numbered() {
                self.layout.existing(&dir)?.last().map_or(1, |(n, _)| n + 1)
            } else {
                1
            };
        } else {
            self.ensure_dir(&dir, self.overwriting())?;
        }
        let name = self.layout.file_name(st.next_part);
        let final_path = dir.join(&name);
        let exists = final_path.exists();
        if exists && self.config.mode == FileMode::ErrorIfExists {
            return Err(FaucetError::Sink(format!(
                "file sink: '{}' already exists and `mode` is `error_if_exists`",
                final_path.display()
            )));
        }
        self.outputs.record_open_probing_with(
            self.layout.dir.join(&name),
            self.config.mode != FileMode::Append,
        );
        let resume = exists && self.config.mode == FileMode::Append;
        st.current = Some(OpenFile::create(&self.ctx(), final_path, resume)?);
        Ok(())
    }

    fn cap_reached(&self, records: usize, bytes: usize) -> bool {
        self.config
            .max_records_per_file
            .is_some_and(|m| records >= m)
            || self.config.max_bytes_per_file.is_some_and(|m| bytes >= m)
    }

    /// Finalise and close the current file; the next write opens a new part.
    fn roll(&self, st: &mut State) -> Result<(), FaucetError> {
        if let Some(mut f) = st.current.take() {
            let r = f.finalize(&self.ctx());
            f.discard();
            r?;
        }
        st.next_part += 1;
        Ok(())
    }

    /// Write `rows` into as many files as the rollover caps require.
    fn write_rows(&self, rows: &[Value]) -> Result<usize, FaucetError> {
        let mut st = self.lock();
        let track_bytes = self.config.max_bytes_per_file.is_some();
        let mut i = 0;
        while i < rows.len() {
            if st.current.is_none() {
                self.open_next(&mut st)?;
            }
            let cur = st.current.as_mut().expect("opened above");
            let (mut records, mut bytes) = (cur.records, cur.bytes);
            let mut end = i;
            while end < rows.len() && !(records > 0 && self.cap_reached(records, bytes)) {
                if track_bytes {
                    bytes += estimate(&rows[end]);
                }
                records += 1;
                end += 1;
            }
            if end > i {
                cur.write(&self.ctx(), &rows[i..end])?;
                cur.bytes = bytes;
            }
            i = end;
            if self.cap_reached(cur.records, cur.bytes) {
                self.roll(&mut st)?;
            }
        }
        Ok(rows.len())
    }

    #[cfg(feature = "file-format-parquet")]
    fn write_columnar(&self, batch: &arrow::array::RecordBatch) -> Result<usize, FaucetError> {
        let rows = batch.num_rows();
        if rows == 0 {
            return Ok(0);
        }
        let mut st = self.lock();
        let mut offset = 0;
        while offset < rows {
            if st.current.is_none() {
                self.open_next(&mut st)?;
            }
            let cur = st.current.as_mut().expect("opened above");
            let room = self
                .config
                .max_records_per_file
                .map_or(rows - offset, |m| m.saturating_sub(cur.records).max(1));
            let len = room.min(rows - offset);
            let slice = batch.slice(offset, len);
            cur.write_batch(&self.ctx(), &slice)?;
            if self.config.max_bytes_per_file.is_some() {
                cur.bytes += slice.get_array_memory_size();
            }
            offset += len;
            if self.cap_reached(cur.records, cur.bytes) {
                self.roll(&mut st)?;
            }
        }
        Ok(rows)
    }

    fn flush_blocking(&self) -> Result<(), FaucetError> {
        let mut st = self.lock();
        if let Some(f) = st.current.as_mut() {
            f.finalize(&self.ctx())?;
        }
        Ok(())
    }

    fn begin_blocking(&self) -> Result<(), FaucetError> {
        let staging = self.layout.staging_dir();
        self.ensure_dir(&self.layout.dir, false)?;
        if staging.exists() {
            std::fs::remove_dir_all(&staging)
                .map_err(|e| io_err("clearing staging directory", &staging, e))?;
        }
        std::fs::create_dir_all(&staging)
            .map_err(|e| io_err("creating staging directory", &staging, e))
    }

    fn commit_blocking(&self) -> Result<(), FaucetError> {
        let staging = self.layout.staging_dir();
        if !staging.is_dir() {
            return Err(FaucetError::Sink(format!(
                "file sink: overwrite staging directory '{}' is missing, so there is nothing \
                 to swap in; the destination is unchanged",
                staging.display()
            )));
        }
        let staged = self.layout.existing(&staging)?;
        let mut kept = std::collections::HashSet::new();
        for (_, path) in &staged {
            let name = path.file_name().expect("listed file").to_owned();
            let dest = self.layout.dir.join(&name);
            std::fs::rename(path, &dest).map_err(|e| io_err("moving into place", &dest, e))?;
            kept.insert(name);
        }
        for (_, path) in self.layout.existing(&self.layout.dir)? {
            if !path.file_name().is_some_and(|n| kept.contains(n)) {
                std::fs::remove_file(&path)
                    .map_err(|e| io_err("removing earlier output", &path, e))?;
            }
        }
        std::fs::remove_dir_all(&staging)
            .map_err(|e| io_err("removing staging directory", &staging, e))?;
        sync_dir(&self.layout.dir.join("x"));
        Ok(())
    }

    fn abort_blocking(&self) -> Result<(), FaucetError> {
        let mut st = self.lock();
        if let Some(mut f) = st.current.take() {
            f.discard();
        }
        drop(st);
        let staging = self.layout.staging_dir();
        if staging.exists() {
            std::fs::remove_dir_all(&staging)
                .map_err(|e| io_err("removing staging directory", &staging, e))?;
        }
        Ok(())
    }

    /// The source config that reads this sink's output back.
    fn readback_config(&self) -> Value {
        let path = if self.layout.numbered() {
            self.layout
                .dir
                .join(self.layout.name.replace(crate::config::PART_TOKEN, "*"))
        } else {
            self.layout.dir.join(&self.layout.name)
        };
        let mut cfg = serde_json::json!({
            "path": path.to_string_lossy(),
            "format": self.format.as_str(),
        });
        if self.format == FileFormat::Csv {
            cfg["csv"] = serde_json::to_value(&self.config.csv).unwrap_or(Value::Null);
        }
        if self.format == FileFormat::Xml {
            cfg["xml"] = serde_json::to_value(&self.config.xml).unwrap_or(Value::Null);
        }
        if self.format == FileFormat::Xlsx {
            cfg["excel"] = serde_json::to_value(&self.config.excel).unwrap_or(Value::Null);
        }
        cfg
    }
}

impl Drop for FileSink {
    fn drop(&mut self) {
        if let Some(mut f) = self.lock().current.take() {
            f.discard();
        }
    }
}

/// A record's JSON length plus a newline: the unit the byte cap counts in.
fn estimate(v: &Value) -> usize {
    serde_json::to_vec(v).map_or(0, |b| b.len()) + 1
}

/// Run blocking filesystem work off the async worker when the runtime allows,
/// inline otherwise (a current-thread runtime cannot block in place).
fn blocking<T>(f: impl FnOnce() -> T) -> T {
    match tokio::runtime::Handle::try_current() {
        Ok(h) if h.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(f)
        }
        _ => f(),
    }
}

#[async_trait]
impl Sink for FileSink {
    fn connector_name(&self) -> &'static str {
        "file"
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(FileSinkConfig))
            .expect("schema serialization")
    }

    fn dataset_uri(&self) -> String {
        let p = Path::new(&self.config.path);
        let abs = std::path::absolute(p).unwrap_or_else(|_| p.to_path_buf());
        format!("file://{}", abs.display())
    }

    fn batch_atomicity(&self) -> faucet_core::BatchAtomicity {
        self.config.batch_atomicity()
    }

    fn supported_write_modes(&self) -> &'static [WriteMode] {
        &[WriteMode::Append, WriteMode::Overwrite]
    }

    fn is_overwrite(&self) -> bool {
        self.overwriting()
    }

    async fn begin_overwrite(&self) -> Result<(), FaucetError> {
        blocking(|| self.begin_blocking())
    }

    async fn commit_overwrite(&self) -> Result<(), FaucetError> {
        blocking(|| self.commit_blocking())
    }

    async fn abort_overwrite(&self) -> Result<(), FaucetError> {
        blocking(|| self.abort_blocking())
    }

    fn readback_source(&self) -> Option<(String, Value)> {
        Some(("file".into(), self.readback_config()))
    }

    async fn local_outputs(&self) -> Vec<faucet_core::LocalOutput> {
        self.outputs.snapshot()
    }

    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }
        blocking(|| self.write_rows(records))
    }

    #[cfg(feature = "arrow")]
    fn supports_columnar(&self) -> bool {
        matches!(self.format, FileFormat::Parquet | FileFormat::Avro)
    }

    #[cfg(feature = "arrow")]
    async fn write_batch_columnar(
        &self,
        batch: &arrow::array::RecordBatch,
    ) -> Result<usize, FaucetError> {
        #[cfg(feature = "file-format-parquet")]
        if self.format == FileFormat::Parquet {
            return blocking(|| self.write_columnar(batch));
        }
        let rows = faucet_core::columnar::record_batch_to_values(batch)?;
        self.write_batch(&rows).await
    }

    async fn flush(&self) -> Result<(), FaucetError> {
        blocking(|| self.flush_blocking())
    }

    /// Preflight probe: the destination directory (or, when it will be
    /// created, its nearest existing ancestor) accepts a new file.
    async fn check(
        &self,
        _ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::{CheckReport, Probe};
        let start = std::time::Instant::now();
        let dir = self.layout.dir.clone();
        let probe = blocking(|| {
            let mut target = dir.clone();
            if !target.is_dir() {
                if !self.config.create_dirs {
                    return Probe::fail_hint(
                        "io",
                        start.elapsed(),
                        format!("directory {} does not exist", dir.display()),
                        "create it, or set `create_dirs: true`",
                    );
                }
                while !target.is_dir() {
                    match target.parent() {
                        Some(p) if !p.as_os_str().is_empty() => target = p.to_path_buf(),
                        _ => {
                            target = PathBuf::from(".");
                            break;
                        }
                    }
                }
            }
            let probe_file = target.join(format!(".faucet_doctor_probe-{}", std::process::id()));
            match std::fs::write(&probe_file, b"") {
                Ok(()) => {
                    let _ = std::fs::remove_file(&probe_file);
                    Probe::pass("io", start.elapsed())
                }
                Err(e) => Probe::fail_hint(
                    "io",
                    start.elapsed(),
                    format!("cannot write to directory {}: {e}", target.display()),
                    "make the directory writable by the current user",
                ),
            }
        });
        Ok(CheckReport::single(probe))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sink(dir: &Path, v: Value) -> FileSink {
        let mut v = v;
        let rel = v["path"].as_str().unwrap().to_string();
        v["path"] = Value::String(format!("{}/{rel}", dir.display()));
        FileSink::new(serde_json::from_value(v).unwrap()).unwrap()
    }

    fn lines(p: &Path) -> Vec<Value> {
        std::fs::read_to_string(p)
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    }

    #[tokio::test]
    async fn jsonl_appears_only_on_flush() {
        let d = tempfile::tempdir().unwrap();
        let s = sink(d.path(), json!({"path": "x.jsonl"}));
        assert_eq!(s.write_batch(&[]).await.unwrap(), 0);
        s.write_batch(&[json!({"a": 1})]).await.unwrap();
        let fin = d.path().join("x.jsonl");
        assert!(!fin.exists());
        assert!(d.path().join("x.jsonl.faucet-tmp").exists());
        s.flush().await.unwrap();
        assert_eq!(lines(&fin), vec![json!({"a": 1})]);
        s.write_batch(&[json!({"a": 2})]).await.unwrap();
        assert_eq!(lines(&fin).len(), 1, "second page invisible until flushed");
        s.flush().await.unwrap();
        s.flush().await.unwrap();
        assert_eq!(lines(&fin), vec![json!({"a": 1}), json!({"a": 2})]);
        assert!(!d.path().join("x.jsonl.faucet-tmp").exists());
        assert_eq!(s.local_outputs().await.len(), 1);
    }

    #[tokio::test]
    async fn dropping_without_flush_leaves_nothing() {
        let d = tempfile::tempdir().unwrap();
        let s = sink(d.path(), json!({"path": "x.jsonl"}));
        s.write_batch(&[json!({"a": 1})]).await.unwrap();
        drop(s);
        assert_eq!(std::fs::read_dir(d.path()).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn rollover_by_records_and_bytes() {
        let d = tempfile::tempdir().unwrap();
        let s = sink(
            d.path(),
            json!({"path": "r.jsonl", "max_records_per_file": 2}),
        );
        let rows: Vec<Value> = (0..5).map(|i| json!({ "i": i })).collect();
        s.write_batch(&rows).await.unwrap();
        s.flush().await.unwrap();
        for (n, want) in [(1, 2), (2, 2), (3, 1)] {
            assert_eq!(
                lines(&d.path().join(format!("r-0000{n}.jsonl"))).len(),
                want
            );
        }
        let s = sink(
            d.path(),
            json!({"path": "b/", "format": "json_lines", "max_bytes_per_file": 20}),
        );
        s.write_batch(&rows).await.unwrap();
        s.flush().await.unwrap();
        let n = std::fs::read_dir(d.path().join("b")).unwrap().count();
        assert!(n >= 2, "{n}");
    }

    #[tokio::test]
    async fn append_mode_continues_a_file_and_numbering() {
        let d = tempfile::tempdir().unwrap();
        std::fs::write(d.path().join("a.jsonl"), "{\"a\":0}\n").unwrap();
        let s = sink(d.path(), json!({"path": "a.jsonl", "mode": "append"}));
        s.write_batch(&[json!({"a": 1})]).await.unwrap();
        s.flush().await.unwrap();
        assert_eq!(lines(&d.path().join("a.jsonl")).len(), 2);
        assert!(s.local_outputs().await[0].pre_existing);

        std::fs::write(d.path().join("p-00004.jsonl"), "{}\n").unwrap();
        let s = sink(
            d.path(),
            json!({"path": "p-{part}.jsonl", "mode": "append", "max_records_per_file": 10}),
        );
        s.write_batch(&[json!({"a": 1})]).await.unwrap();
        s.flush().await.unwrap();
        assert!(d.path().join("p-00005.jsonl").exists());
    }

    #[tokio::test]
    async fn error_if_exists_and_missing_dirs() {
        let d = tempfile::tempdir().unwrap();
        std::fs::write(d.path().join("e.jsonl"), "").unwrap();
        let s = sink(
            d.path(),
            json!({"path": "e.jsonl", "mode": "error_if_exists"}),
        );
        let e = s.write_batch(&[json!({})]).await.unwrap_err();
        assert!(e.to_string().contains("already exists"), "{e}");
        let s = sink(
            d.path(),
            json!({"path": "no/such/x.jsonl", "create_dirs": false}),
        );
        let e = s.write_batch(&[json!({})]).await.unwrap_err();
        assert!(e.to_string().contains("create_dirs"), "{e}");
        let s = sink(d.path(), json!({"path": "made/here/x.jsonl"}));
        s.write_batch(&[json!({})]).await.unwrap();
        s.flush().await.unwrap();
        assert!(d.path().join("made/here/x.jsonl").exists());
    }

    #[tokio::test]
    async fn overwrite_lifecycle_across_instances() {
        let d = tempfile::tempdir().unwrap();
        let cfg =
            json!({"path": "o-{part}.jsonl", "write_mode": "overwrite", "max_records_per_file": 1});
        for n in 1..=3 {
            std::fs::write(d.path().join(format!("o-0000{n}.jsonl")), "{\"old\":1}\n").unwrap();
        }
        let life = sink(d.path(), cfg.clone());
        assert!(life.is_overwrite());
        life.begin_overwrite().await.unwrap();
        let w = sink(d.path(), cfg.clone());
        w.write_batch(&[json!({"n": 1})]).await.unwrap();
        w.flush().await.unwrap();
        assert!(
            d.path().join("o-00003.jsonl").exists(),
            "untouched before commit"
        );
        sink(d.path(), cfg.clone())
            .commit_overwrite()
            .await
            .unwrap();
        assert_eq!(
            lines(&d.path().join("o-00001.jsonl")),
            vec![json!({"n": 1})]
        );
        assert!(!d.path().join("o-00002.jsonl").exists());
        assert!(!d.path().join("o-00003.jsonl").exists());
        let e = sink(d.path(), cfg.clone())
            .commit_overwrite()
            .await
            .unwrap_err();
        assert!(e.to_string().contains("missing"), "{e}");

        let life = sink(d.path(), cfg.clone());
        life.begin_overwrite().await.unwrap();
        life.begin_overwrite().await.unwrap();
        let w = sink(d.path(), cfg.clone());
        w.write_batch(&[json!({"n": 2})]).await.unwrap();
        w.abort_overwrite().await.unwrap();
        sink(d.path(), cfg.clone()).abort_overwrite().await.unwrap();
        assert_eq!(
            lines(&d.path().join("o-00001.jsonl")),
            vec![json!({"n": 1})]
        );
        assert!(!life.layout.staging_dir().exists());
    }

    #[tokio::test]
    async fn check_and_metadata() {
        let d = tempfile::tempdir().unwrap();
        let s = sink(d.path(), json!({"path": "sub/x.jsonl"}));
        let ctx = faucet_core::check::CheckContext::default();
        assert_eq!(s.check(&ctx).await.unwrap().failed_count(), 0);
        assert!(!d.path().join("sub").exists());
        let s = sink(
            d.path(),
            json!({"path": "sub/x.jsonl", "create_dirs": false}),
        );
        assert_eq!(s.check(&ctx).await.unwrap().failed_count(), 1);
        assert_eq!(s.connector_name(), "file");
        assert!(s.dataset_uri().starts_with("file:///"));
        assert!(s.config_schema().is_object());
        assert_eq!(s.format(), FileFormat::JsonLines);
        assert!(!s.config().create_dirs);
        assert_eq!(
            s.supported_write_modes(),
            &[WriteMode::Append, WriteMode::Overwrite]
        );
        assert_eq!(s.batch_atomicity(), faucet_core::BatchAtomicity::Atomic);
        let (kind, cfg) = s.readback_source().unwrap();
        assert_eq!(kind, "file");
        assert!(cfg["path"].as_str().unwrap().ends_with("sub/x.jsonl"));
        let r = sink(d.path(), json!({"path": "p-{part}.csv"})).readback_config();
        assert!(r["path"].as_str().unwrap().ends_with("p-*.csv"));
        assert!(r["csv"].is_object());
        assert!(sink(d.path(), json!({"path": "a.xml"})).readback_config()["xml"].is_object());
        assert!(sink(d.path(), json!({"path": "a.xlsx"})).readback_config()["excel"].is_object());
        let rel = FileSink::new(FileSinkConfig::new("rel.jsonl")).unwrap();
        assert!(rel.dataset_uri().ends_with("/rel.jsonl"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn unwritable_directory_is_reported_with_the_path() {
        use std::os::unix::fs::PermissionsExt;
        let d = tempfile::tempdir().unwrap();
        let ro = d.path().join("ro");
        std::fs::create_dir(&ro).unwrap();
        std::fs::set_permissions(&ro, std::fs::Permissions::from_mode(0o500)).unwrap();
        let s = sink(d.path(), json!({"path": "ro/x.jsonl"}));
        let ctx = faucet_core::check::CheckContext::default();
        let report = s.check(&ctx).await.unwrap();
        let writable = std::fs::write(ro.join("t"), b"").is_ok();
        if !writable {
            assert_eq!(report.failed_count(), 1);
            let e = s.write_batch(&[json!({})]).await.unwrap_err();
            assert!(e.to_string().contains("ro/x.jsonl"), "{e}");
        }
        std::fs::set_permissions(&ro, std::fs::Permissions::from_mode(0o700)).unwrap();
    }

    #[test]
    fn blocking_runs_without_a_runtime() {
        assert_eq!(blocking(|| 7), 7);
        assert_eq!(estimate(&json!({"a": 1})), 8);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blocking_in_place_on_a_multi_thread_runtime() {
        assert_eq!(blocking(|| 3), 3);
    }
}
