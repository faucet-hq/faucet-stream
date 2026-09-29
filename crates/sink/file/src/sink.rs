//! The local file sink: the shared file writer over the local backend.

use crate::config::{FileSinkConfig, FileWriteMode};
use async_trait::async_trait;
use faucet_common_file::write::{FileWriter, LocalBackend, NameTemplate, blocking};
use faucet_core::{FaucetError, FileFormat, Sink, WriteMode};
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Writes records to local files. See the crate docs.
pub struct FileSink {
    config: FileSinkConfig,
    local: Arc<LocalBackend>,
    writer: FileWriter,
    name: &'static str,
    advertised: Option<(&'static [WriteMode], faucet_core::BatchAtomicity)>,
}

impl FileSink {
    /// Build the sink, validating the config.
    pub fn new(config: FileSinkConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        let settings = config.settings()?;
        if settings.codec != faucet_core::Compression::None {
            faucet_core::warn_mismatch(&config.path, settings.codec);
        }
        let (dir, template) = NameTemplate::from_path(
            &config.path,
            settings.format,
            settings.codec,
            settings.rolls_over(),
        )
        .map_err(|e| match e {
            FaucetError::Config(m) => FaucetError::Config(format!("file sink: {m}")),
            other => other,
        })?;
        let local = Arc::new(LocalBackend::new(
            &dir,
            &template.staging_name(),
            config.create_dirs,
        ));
        let writer = FileWriter::new(settings, template, local.clone())?;
        Ok(Self {
            config,
            local,
            writer,
            name: "file",
            advertised: None,
        })
    }

    /// Report `name` as the connector name (metric labels, logs) instead of
    /// `file`, for a deprecated kind the CLI builds as this sink.
    pub fn with_connector_name(mut self, name: &'static str) -> Self {
        self.name = name;
        self
    }

    /// Advertise what a deprecated kind built as this sink always advertised:
    /// these write modes, this batch atomicity, and no read-back source.
    pub fn with_legacy_surface(
        mut self,
        write_modes: &'static [WriteMode],
        atomicity: faucet_core::BatchAtomicity,
    ) -> Self {
        self.advertised = Some((write_modes, atomicity));
        self
    }

    /// The resolved format.
    pub fn format(&self) -> FileFormat {
        self.writer.settings().format
    }

    /// The config the sink was built with.
    pub fn config(&self) -> &FileSinkConfig {
        &self.config
    }

    fn overwriting(&self) -> bool {
        self.config.write_mode == FileWriteMode::Overwrite
    }

    /// The config of a `file` source that reads this sink's output back.
    fn readback_config(&self) -> Value {
        let template = self.writer.template();
        let path = if template.numbered() {
            self.local
                .dir()
                .join(template.name.replace(crate::config::PART_TOKEN, "*"))
        } else {
            self.local.dir().join(&template.name)
        };
        let format = self.format();
        let mut cfg = serde_json::json!({
            "path": path.to_string_lossy(),
            "format": format.as_str(),
        });
        if format == FileFormat::Csv {
            cfg["csv"] = serde_json::to_value(&self.config.csv).unwrap_or(Value::Null);
        }
        if format == FileFormat::Xml {
            cfg["xml"] = serde_json::to_value(&self.config.xml).unwrap_or(Value::Null);
        }
        if format == FileFormat::Xlsx {
            cfg["excel"] = serde_json::to_value(&self.config.excel).unwrap_or(Value::Null);
        }
        #[cfg(feature = "encryption")]
        if let Some(spec) = &self.config.encryption {
            cfg["encryption"] = serde_json::to_value(spec).unwrap_or(Value::Null);
        }
        cfg
    }
}

#[async_trait]
impl Sink for FileSink {
    fn connector_name(&self) -> &'static str {
        self.name
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
        self.advertised
            .map_or_else(|| self.config.batch_atomicity(), |(_, a)| a)
    }

    fn supported_write_modes(&self) -> &'static [WriteMode] {
        self.advertised
            .map_or(&[WriteMode::Append, WriteMode::Overwrite], |(m, _)| m)
    }

    fn is_overwrite(&self) -> bool {
        self.overwriting()
    }

    async fn begin_overwrite(&self) -> Result<(), FaucetError> {
        blocking(|| self.writer.begin_overwrite())
    }

    async fn commit_overwrite(&self) -> Result<(), FaucetError> {
        blocking(|| self.writer.commit_overwrite())
    }

    async fn abort_overwrite(&self) -> Result<(), FaucetError> {
        blocking(|| self.writer.abort_overwrite())
    }

    async fn complete_run(&self) -> Result<(), FaucetError> {
        blocking(|| self.writer.complete())
    }

    fn readback_source(&self) -> Option<(String, Value)> {
        match self.advertised {
            Some(_) => None,
            None => Some(("file".into(), self.readback_config())),
        }
    }

    async fn local_outputs(&self) -> Vec<faucet_core::LocalOutput> {
        self.writer.local_outputs()
    }

    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }
        blocking(|| self.writer.write_rows(records))
    }

    #[cfg(feature = "arrow")]
    fn supports_columnar(&self) -> bool {
        matches!(self.format(), FileFormat::Parquet | FileFormat::Avro)
    }

    #[cfg(feature = "arrow")]
    async fn write_batch_columnar(
        &self,
        batch: &arrow::array::RecordBatch,
    ) -> Result<usize, FaucetError> {
        #[cfg(feature = "file-format-parquet")]
        if self.format() == FileFormat::Parquet {
            return blocking(|| self.writer.write_batch(batch));
        }
        let rows = faucet_core::columnar::record_batch_to_values(batch)?;
        self.write_batch(&rows).await
    }

    async fn flush(&self) -> Result<(), FaucetError> {
        blocking(|| self.writer.flush())
    }

    async fn check(
        &self,
        _ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::{CheckReport, Probe};
        let start = std::time::Instant::now();
        let dir = self.local.dir().to_path_buf();
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

    #[test]
    fn legacy_surface_replaces_the_advertised_capabilities() {
        let dir = tempfile::tempdir().unwrap();
        let s = sink(dir.path(), json!({ "path": "out.jsonl" }));
        assert_eq!(
            s.supported_write_modes(),
            &[WriteMode::Append, WriteMode::Overwrite]
        );
        assert_eq!(s.batch_atomicity(), faucet_core::BatchAtomicity::Atomic);
        assert!(s.readback_source().is_some());
        let s = s.with_legacy_surface(
            &[WriteMode::Append],
            faucet_core::BatchAtomicity::BestEffort,
        );
        assert_eq!(s.supported_write_modes(), &[WriteMode::Append]);
        assert_eq!(s.batch_atomicity(), faucet_core::BatchAtomicity::BestEffort);
        assert!(s.readback_source().is_none());
    }

    fn sink(dir: &Path, v: Value) -> FileSink {
        let mut v = v;
        let rel = v["path"].as_str().unwrap().to_string();
        v["path"] = Value::String(format!("{}/{rel}", dir.display()));
        FileSink::new(serde_json::from_value(v).unwrap()).unwrap()
    }

    #[test]
    fn connector_name_defaults_to_file_and_can_be_renamed() {
        let d = tempfile::tempdir().unwrap();
        let s = sink(d.path(), json!({"path": "a.jsonl"}));
        assert_eq!(s.connector_name(), "file");
        assert_eq!(s.with_connector_name("jsonl").connector_name(), "jsonl");
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
        assert!(!life.local.staging_dir().exists());
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
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blocking_in_place_on_a_multi_thread_runtime() {
        assert_eq!(blocking(|| 3), 3);
    }
}
