//! [`WriteConfig`]: the one mapping from a file-writing sink's config to
//! [`WriteSettings`] and its output layout, shared by the local file sink and
//! the object-store and SFTP sinks so their rules cannot drift apart.

use super::layout::NameTemplate;
use super::options::{
    FileWriteMode, IfExists, JsonLinesOptions, PART_TOKEN, ParquetCodec, ParquetOptions,
};
use super::remote::object_layout;
use super::writer::WriteSettings;
use faucet_core::{CompressionConfig, FaucetError, FileFormat, FormatOptions};

/// A sink config's write fields, in one shape.
///
/// Build it with every field the sink has and `..WriteConfig::default()`
/// for the rest, then call [`settings`](Self::settings) and
/// [`object_layout`](Self::object_layout).
///
/// **Experimental** (PRINCIPLES.md §3): this block's shape may change in a
/// minor release; any change is called out in the changelog.
#[derive(Debug, Clone)]
pub struct WriteConfig {
    /// The sink's name in messages (`"S3 sink"`).
    pub connector: &'static str,
    /// The config key that holds the path template (`"path"`).
    pub path_field: &'static str,
    /// Key prefix (or directory) the path is relative to.
    pub prefix: String,
    /// The path template. `None` names every run's files uniquely (an
    /// object-store sink without `path`).
    pub path: Option<String>,
    /// Extension of uniquely named files (no `path`).
    pub file_extension: String,
    /// The explicit format, or `None` to take it from the name's extension.
    pub format: Option<FileFormat>,
    /// File-level compression.
    pub compression: CompressionConfig,
    /// Per-format encoder options.
    pub opts: FormatOptions,
    /// Parquet options.
    pub parquet: ParquetOptions,
    /// This sink's Parquet codec when `parquet.compression` is unset.
    pub default_parquet_codec: ParquetCodec,
    /// JSON Lines options.
    pub json_lines: JsonLinesOptions,
    /// What happens to an existing file.
    pub if_exists: IfExists,
    /// Pipeline write mode.
    pub write_mode: FileWriteMode,
    /// Record cap per file. On an object-store sink `0` means none.
    pub max_records_per_file: Option<usize>,
    /// Byte cap per file.
    pub max_bytes_per_file: Option<usize>,
    /// An object-store sink's `batch_size`: records per uniquely named file
    /// (`0` = no cap). `None` for a sink without it.
    pub batch_size: Option<usize>,
    /// Close the file at every flush (object stores).
    pub object_per_flush: bool,
    /// Encrypt the output at rest.
    #[cfg(feature = "encryption")]
    pub encryption: Option<faucet_core::EncryptionSpec>,
}

impl Default for WriteConfig {
    fn default() -> Self {
        Self {
            connector: "file sink",
            path_field: "path",
            prefix: String::new(),
            path: None,
            file_extension: String::new(),
            format: None,
            compression: CompressionConfig::None,
            opts: FormatOptions::default(),
            parquet: ParquetOptions::default(),
            default_parquet_codec: ParquetCodec::Snappy,
            json_lines: JsonLinesOptions::default(),
            if_exists: IfExists::default(),
            write_mode: FileWriteMode::default(),
            max_records_per_file: None,
            max_bytes_per_file: None,
            batch_size: None,
            object_per_flush: false,
            #[cfg(feature = "encryption")]
            encryption: None,
        }
    }
}

impl WriteConfig {
    fn err(&self, msg: impl std::fmt::Display) -> FaucetError {
        FaucetError::Config(format!("{}: {msg}", self.connector))
    }

    /// The name format and compression are resolved from: the path with
    /// part 1 filled in, or the extension of uniquely named files.
    fn resolution_name(&self) -> String {
        match &self.path {
            Some(p) => format!("{}{p}", self.prefix).replace(PART_TOKEN, "00001"),
            None => self.file_extension.clone(),
        }
    }

    /// The record cap: `max_records_per_file`, and without a path on an
    /// object-store sink also `batch_size`, whichever is smaller.
    fn record_cap(&self) -> Option<usize> {
        let Some(batch_size) = self.batch_size else {
            return self.max_records_per_file;
        };
        let max = self.max_records_per_file.filter(|n| *n > 0);
        if self.path.is_some() {
            return max;
        }
        let bs = (batch_size > 0).then_some(batch_size);
        match (bs, max) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }

    /// The storage-independent write settings.
    pub fn settings(&self) -> Result<WriteSettings, FaucetError> {
        if let Some(path) = &self.path {
            crate::require_path(self.connector, path)?;
            if path.matches(PART_TOKEN).count() > 1 {
                return Err(self.err(format!("'{path}' has more than one `{{part}}`")));
            }
            if crate::is_directory_path(path) && self.format.is_none() {
                return Err(self.err(format!(
                    "'{path}' is a directory, so there is no extension to take the format \
                     from — set `format`"
                )));
            }
        } else if self.write_mode == FileWriteMode::Overwrite || self.if_exists != IfExists::Replace
        {
            return Err(self.err(format!(
                "`write_mode: overwrite` and `if_exists: append` / `error` need `{}` — \
                 without it every run writes new, uniquely named files",
                self.path_field
            )));
        }
        if let Some(bs) = self.batch_size {
            faucet_core::validate_batch_size(bs)?;
        }
        let name = self.resolution_name();
        let format = match self.format {
            Some(f) => f,
            None => crate::FileFormatChoice::Auto
                .resolve_writable(&name)
                .map_err(|e| crate::config_context(self.connector, e))?,
        };
        let codec = crate::resolve_compression(self.compression, &name);
        let mut s = WriteSettings::new(format, codec);
        s.opts = self.opts.clone();
        s.parquet = self.parquet.clone();
        s.default_parquet_codec = self.default_parquet_codec;
        s.json_lines = self.json_lines.clone();
        s.if_exists = self.if_exists;
        s.write_mode = self.write_mode;
        s.max_records_per_file = self.record_cap();
        s.max_bytes_per_file = self.max_bytes_per_file;
        #[cfg(feature = "encryption")]
        {
            s.encryption = self.encryption.clone();
        }
        s.object_per_flush = self.object_per_flush;
        s.object_per_write = format == FileFormat::Parquet
            && self.path.is_none()
            && s.max_records_per_file.is_none()
            && s.max_bytes_per_file.is_none();
        s.prune_stale = self.path.is_some();
        Ok(s)
    }

    /// The key prefix and file-name template of an object-store or SFTP
    /// sink's output (see [`object_layout`]).
    pub fn object_layout(
        &self,
        settings: &WriteSettings,
    ) -> Result<(String, NameTemplate), FaucetError> {
        object_layout(
            &self.prefix,
            self.path.as_deref(),
            &self.file_extension,
            settings.format,
            settings.codec,
            settings.rolls_over(),
        )
        .map_err(|e| crate::config_context(self.connector, e))
    }

    /// The directory and file-name template of a local path.
    pub fn local_layout(
        &self,
        settings: &WriteSettings,
    ) -> Result<(String, NameTemplate), FaucetError> {
        let path = format!("{}{}", self.prefix, self.path.as_deref().unwrap_or(""));
        NameTemplate::from_path(
            &path,
            settings.format,
            settings.codec,
            settings.rolls_over(),
        )
        .map_err(|e| crate::config_context(self.connector, e))
    }

    /// Refuse every combination that would otherwise fail mid-run.
    pub fn validate(&self) -> Result<(), FaucetError> {
        let settings = self.settings()?;
        let (_, template) = if self.path.is_some() && self.batch_size.is_none() {
            self.local_layout(&settings)?
        } else {
            self.object_layout(&settings)?
        };
        settings.validate_for(&template)
    }

    /// What a failed batch write leaves behind (#737).
    pub fn batch_atomicity(&self) -> faucet_core::BatchAtomicity {
        match self.settings() {
            Ok(s) => s.batch_atomicity(),
            Err(_) => faucet_core::BatchAtomicity::BestEffort,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::Compression;

    fn object(path: Option<&str>) -> WriteConfig {
        WriteConfig {
            connector: "S3 sink",
            prefix: "pre/".into(),
            path: path.map(String::from),
            file_extension: ".jsonl".into(),
            format: Some(FileFormat::JsonLines),
            compression: CompressionConfig::Auto,
            default_parquet_codec: ParquetCodec::Zstd,
            batch_size: Some(1000),
            object_per_flush: true,
            ..WriteConfig::default()
        }
    }

    #[test]
    fn caps_combine_the_same_way_for_every_object_sink() {
        let mut c = object(None);
        assert_eq!(c.record_cap(), Some(1000));
        c.max_records_per_file = Some(10);
        assert_eq!(c.record_cap(), Some(10));
        c.max_records_per_file = Some(5000);
        assert_eq!(c.record_cap(), Some(1000));
        c.batch_size = Some(0);
        assert_eq!(c.record_cap(), Some(5000));
        c.max_records_per_file = Some(0);
        assert_eq!(c.record_cap(), None);
        c.path = Some("x.jsonl".into());
        c.max_records_per_file = Some(7);
        c.batch_size = Some(3);
        assert_eq!(
            c.record_cap(),
            Some(7),
            "batch_size does not size a named file"
        );
        let mut f = WriteConfig {
            path: Some("x.jsonl".into()),
            ..WriteConfig::default()
        };
        f.max_records_per_file = Some(0);
        let e = f.validate().unwrap_err().to_string();
        assert!(e.contains("at least 1"), "{e}");
    }

    #[test]
    fn settings_follow_the_path_and_mode_rules() {
        let s = object(None).settings().unwrap();
        assert!(!s.prune_stale, "unique names have nothing to prune");
        assert!(s.object_per_flush);
        assert_eq!(s.parquet_codec(), ParquetCodec::Zstd);
        let s = object(Some("d/x.jsonl.gz")).settings().unwrap();
        assert!(s.prune_stale);
        assert_eq!(s.codec, Compression::Gzip);
        for (mode, if_exists) in [
            (FileWriteMode::Overwrite, IfExists::Replace),
            (FileWriteMode::Append, IfExists::Append),
            (FileWriteMode::Append, IfExists::Error),
        ] {
            let mut c = object(None);
            c.write_mode = mode;
            c.if_exists = if_exists;
            let e = c.settings().unwrap_err().to_string();
            assert!(
                e.starts_with("Config error: S3 sink: ") || e.contains("S3 sink: "),
                "{e}"
            );
            assert!(e.contains("need `path`"), "{e}");
        }
        let mut c = object(Some("{part}-{part}.jsonl"));
        assert!(
            c.settings()
                .unwrap_err()
                .to_string()
                .contains("more than one")
        );
        c.path = Some(" ".into());
        assert!(c.settings().unwrap_err().to_string().contains("empty"));
        c.path = Some("d/".into());
        c.format = None;
        assert!(c.settings().unwrap_err().to_string().contains("directory"));
        c.path = Some("x.dat".into());
        assert!(c.settings().unwrap_err().to_string().contains("x.dat"));
        c.path = Some("x.csv.zst".into());
        let s = c.settings().unwrap();
        assert_eq!((s.format, s.codec), (FileFormat::Csv, Compression::Zstd));
        c.batch_size = Some(2_000_000);
        assert!(c.settings().is_err());
    }

    #[test]
    fn parquet_without_a_path_or_cap_is_one_object_per_write() {
        let mut c = object(None);
        c.format = Some(FileFormat::Parquet);
        c.batch_size = Some(0);
        assert!(c.settings().unwrap().object_per_write);
        c.max_bytes_per_file = Some(10);
        assert!(!c.settings().unwrap().object_per_write);
    }

    #[test]
    fn layouts_and_atomicity() {
        let c = object(Some("d/x-{part}.jsonl"));
        let s = c.settings().unwrap();
        let (base, t) = c.object_layout(&s).unwrap();
        assert_eq!(
            (base.as_str(), t.name.as_str()),
            ("pre/d/", "x-{part}.jsonl")
        );
        let f = WriteConfig {
            path: Some("/tmp/out/x.jsonl".into()),
            ..WriteConfig::default()
        };
        let s = f.settings().unwrap();
        let (dir, t) = f.local_layout(&s).unwrap();
        assert_eq!((dir.as_str(), t.name.as_str()), ("/tmp/out", "x.jsonl"));
        assert_eq!(f.batch_atomicity(), faucet_core::BatchAtomicity::Atomic);
        let mut bad = f.clone();
        bad.path = None;
        bad.write_mode = FileWriteMode::Overwrite;
        assert_eq!(
            bad.batch_atomicity(),
            faucet_core::BatchAtomicity::BestEffort
        );
        let mut append = f.clone();
        append.path = Some("x.json".into());
        append.if_exists = IfExists::Append;
        let e = append.validate().unwrap_err().to_string();
        assert!(e.contains("file sink: `if_exists: append`"), "{e}");
        let mut named = append.clone();
        named.batch_size = Some(10);
        let e = named.validate().unwrap_err().to_string();
        assert!(e.contains("`if_exists: append`"), "{e}");
    }
}
