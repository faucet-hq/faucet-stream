//! The local file sink: the shared file writer over the local backend.

use crate::config::FileSinkConfig;
use faucet_common_file::write::{FileWriter, LocalBackend, SinkIdentity, WriterSink};
use faucet_core::{FaucetError, FileFormat};
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Writes records to local files. See the crate docs.
pub struct FileSink {
    config: FileSinkConfig,
    inner: WriterSink,
}

impl FileSink {
    /// Build the sink, validating the config.
    pub fn new(config: FileSinkConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        let write = config.write_config();
        let settings = write.settings()?;
        if settings.codec != faucet_core::Compression::None {
            faucet_core::warn_mismatch(&config.path, settings.codec);
        }
        let (dir, template) = write.local_layout(&settings)?;
        let local = Arc::new(LocalBackend::new(&dir, &template, config.create_dirs));
        let writer = FileWriter::new(settings, template, local.clone())?;
        let identity = FileIdentity {
            dataset_uri: dataset_uri(&config.path),
            dir: local.dir().to_path_buf(),
            create_dirs: config.create_dirs,
            readback: readback_config(&config, &writer, local.dir()),
        };
        Ok(Self {
            config,
            inner: WriterSink::new(writer, identity),
        })
    }

    /// The resolved format.
    pub fn format(&self) -> FileFormat {
        self.inner.format()
    }

    /// The config the sink was built with.
    pub fn config(&self) -> &FileSinkConfig {
        &self.config
    }
}

faucet_common_file::delegate_sink!(FileSink, inner);

fn dataset_uri(path: &str) -> String {
    let p = Path::new(path);
    let abs = std::path::absolute(p).unwrap_or_else(|_| p.to_path_buf());
    format!("file://{}", abs.display())
}

/// The config of a `file` source that reads the output back, or `None` when
/// the source cannot: pretty-printed JSON Lines is not one record per line.
fn readback_config(config: &FileSinkConfig, writer: &FileWriter, dir: &Path) -> Option<Value> {
    let settings = writer.settings();
    if settings.format == FileFormat::JsonLines && settings.json_lines.pretty {
        return None;
    }
    let template = writer.template();
    let path = if template.numbered() {
        dir.join(template.name.replace(crate::config::PART_TOKEN, "*"))
    } else {
        dir.join(&template.name)
    };
    let mut cfg = serde_json::json!({
        "path": path.to_string_lossy(),
        "format": settings.format.as_str(),
        "compression": serde_json::to_value(config.compression).ok()?,
    });
    match settings.format {
        FileFormat::Csv => cfg["csv"] = serde_json::to_value(&config.csv).ok()?,
        FileFormat::Xml => cfg["xml"] = serde_json::to_value(&config.xml).ok()?,
        FileFormat::Xlsx => cfg["excel"] = serde_json::to_value(&config.excel).ok()?,
        _ => {}
    }
    #[cfg(feature = "encryption")]
    if let Some(spec) = &config.encryption {
        cfg["encryption"] = serde_json::to_value(spec).ok()?;
    }
    Some(cfg)
}

/// What the local file sink supplies to the shared sink.
struct FileIdentity {
    dataset_uri: String,
    dir: PathBuf,
    create_dirs: bool,
    readback: Option<Value>,
}

#[faucet_core::async_trait]
impl SinkIdentity for FileIdentity {
    fn connector_name(&self) -> &'static str {
        "file"
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(FileSinkConfig))
            .expect("schema serialization")
    }

    fn dataset_uri(&self) -> String {
        self.dataset_uri.clone()
    }

    fn readback_source(&self) -> Option<(String, Value)> {
        self.readback.clone().map(|cfg| ("file".into(), cfg))
    }

    async fn check(
        &self,
        _ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::CheckReport;
        let (dir, create_dirs) = (self.dir.clone(), self.create_dirs);
        let probe = tokio::task::spawn_blocking(move || probe_dir(&dir, create_dirs))
            .await
            .map_err(|e| FaucetError::Sink(format!("file sink: the probe did not finish: {e}")))?;
        Ok(CheckReport::single(probe))
    }
}

fn probe_dir(dir: &Path, create_dirs: bool) -> faucet_core::check::Probe {
    use faucet_core::check::Probe;
    let start = std::time::Instant::now();
    let mut target = dir.to_path_buf();
    if !target.is_dir() {
        if !create_dirs {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::{Sink, WriteMode};
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
        assert!(!d.path().join(".faucet-overwrite-o-_part_.jsonl").exists());
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
        let back = |v: Value| sink(d.path(), v).readback_source().map(|(_, c)| c);
        let r = back(json!({"path": "p-{part}.csv.gz"})).unwrap();
        assert_eq!(r["compression"], json!("auto"));
        assert!(
            back(json!({"path": "p.jsonl", "json_lines": {"pretty": true}})).is_none(),
            "pretty JSON Lines cannot be read back one record per line"
        );
        assert!(r["path"].as_str().unwrap().ends_with("p-*.csv.gz"));
        assert!(r["csv"].is_object());
        assert!(back(json!({"path": "a.xml"})).unwrap()["xml"].is_object());
        assert!(back(json!({"path": "a.xlsx"})).unwrap()["excel"].is_object());
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
}
