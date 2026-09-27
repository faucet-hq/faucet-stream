//! Where the sink's files go: the directory, the file-name template, part
//! numbering, temporary names and the overwrite staging directory.

use crate::config::{FileSinkConfig, PART_TOKEN};
use faucet_core::{Compression, FaucetError, FileFormat};
use std::path::{Path, PathBuf};

/// Suffix of a file that is still being written.
pub(crate) const TMP_SUFFIX: &str = ".faucet-tmp";

/// Suffix of a CSV body that is waiting for its header.
pub(crate) const BODY_SUFFIX: &str = ".faucet-tmp-body";

/// Prefix of the staging directory an overwrite run writes into.
const STAGING_PREFIX: &str = ".faucet-overwrite-";

/// The resolved output layout of one sink config.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Layout {
    /// Directory the files land in.
    pub dir: PathBuf,
    /// File-name template; `{part}` present when the output is numbered.
    pub name: String,
}

impl Layout {
    /// Resolve the layout of `cfg` writing `format` with `codec`.
    pub fn new(
        cfg: &FileSinkConfig,
        format: FileFormat,
        codec: Compression,
    ) -> Result<Self, FaucetError> {
        if cfg.is_directory() {
            let dir = cfg.path.trim_end_matches(['/', '\\']);
            let dir = if dir.is_empty() { "/" } else { dir };
            return Ok(Self {
                dir: PathBuf::from(dir),
                name: format!(
                    "part-{PART_TOKEN}{}{}",
                    format.extension(),
                    codec_suffix(codec)
                ),
            });
        }
        let path = Path::new(&cfg.path);
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| {
                FaucetError::Config(format!("file sink: '{}' has no file name", cfg.path))
            })?
            .to_string();
        let dir = match path.parent() {
            Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
            _ => PathBuf::from("."),
        };
        if dir.to_string_lossy().contains(PART_TOKEN) {
            return Err(FaucetError::Config(format!(
                "file sink: `{{part}}` may appear only in the file name, not the directory, \
                 of '{}'",
                cfg.path
            )));
        }
        let name = if cfg.rolls_over() && !name.contains(PART_TOKEN) {
            insert_part(&name)
        } else {
            name
        };
        Ok(Self { dir, name })
    }

    /// Whether the output is numbered.
    pub fn numbered(&self) -> bool {
        self.name.contains(PART_TOKEN)
    }

    /// The file name of part `n` (1-based).
    pub fn file_name(&self, n: u64) -> String {
        self.name.replace(PART_TOKEN, &format!("{n:05}"))
    }

    /// The part number `name` carries, or `None` when it is not one of this
    /// layout's files. An unnumbered layout's single file is part 1.
    pub fn part_of(&self, name: &str) -> Option<u64> {
        match self.name.split_once(PART_TOKEN) {
            None => (name == self.name).then_some(1),
            Some((pre, post)) => {
                let digits = name.strip_prefix(pre)?.strip_suffix(post)?;
                if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
                    return None;
                }
                digits.parse().ok()
            }
        }
    }

    /// The hidden staging directory an overwrite run writes into, beside the
    /// destination so the final move is a same-filesystem rename.
    pub fn staging_dir(&self) -> PathBuf {
        let tag: String = self
            .name
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '.' || c == '-' || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        self.dir.join(format!("{STAGING_PREFIX}{tag}"))
    }

    /// Existing files in `dir` that belong to this layout, with their part.
    pub fn existing(&self, dir: &Path) -> Result<Vec<(u64, PathBuf)>, FaucetError> {
        let entries = match std::fs::read_dir(dir) {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(io_err("listing", dir, e)),
        };
        let mut out = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|e| io_err("listing", dir, e))?;
            if let Some(name) = entry.file_name().to_str()
                && let Some(n) = self.part_of(name)
                && entry.file_type().map(|t| t.is_file()).unwrap_or(false)
            {
                out.push((n, entry.path()));
            }
        }
        out.sort();
        Ok(out)
    }

    /// Remove temporary files a killed run left behind in `dir`.
    pub fn remove_stale_temps(&self, dir: &Path) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let Some(name) = entry.file_name().to_str().map(str::to_string) else {
                continue;
            };
            let base = name
                .strip_suffix(BODY_SUFFIX)
                .or_else(|| name.strip_suffix(TMP_SUFFIX));
            if let Some(base) = base
                && self.part_of(base).is_some()
            {
                let _ = std::fs::remove_file(entry.path());
            }
        }
    }
}

/// `contacts.jsonl.gz` → `contacts-{part}.jsonl.gz`.
fn insert_part(name: &str) -> String {
    let search_from = usize::from(name.starts_with('.'));
    match name[search_from..].find('.') {
        Some(i) => {
            let i = i + search_from;
            format!("{}-{PART_TOKEN}{}", &name[..i], &name[i..])
        }
        None => format!("{name}-{PART_TOKEN}"),
    }
}

fn codec_suffix(codec: Compression) -> &'static str {
    match codec {
        Compression::None => "",
        Compression::Gzip => ".gz",
        Compression::Zstd => ".zst",
    }
}

/// The temporary sibling of `path`.
pub(crate) fn tmp_path(path: &Path, suffix: &str) -> PathBuf {
    let mut s = path.as_os_str().to_owned();
    s.push(suffix);
    PathBuf::from(s)
}

/// An I/O error naming the file.
pub(crate) fn io_err(what: &str, path: &Path, e: std::io::Error) -> FaucetError {
    FaucetError::Sink(format!("file sink: {what} '{}': {e}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn layout(v: serde_json::Value) -> Layout {
        let cfg: FileSinkConfig = serde_json::from_value(v).unwrap();
        let f = cfg.resolved_format().unwrap();
        Layout::new(&cfg, f, cfg.resolved_compression()).unwrap()
    }

    #[test]
    fn single_file_and_numbered_layouts() {
        let l = layout(json!({"path": "out/x.jsonl"}));
        assert_eq!(l.dir, PathBuf::from("out"));
        assert_eq!(l.name, "x.jsonl");
        assert!(!l.numbered());
        assert_eq!(l.file_name(3), "x.jsonl");
        assert_eq!(l.part_of("x.jsonl"), Some(1));
        assert_eq!(l.part_of("y.jsonl"), None);

        let l = layout(json!({"path": "x.jsonl.gz", "max_records_per_file": 2}));
        assert_eq!(l.dir, PathBuf::from("."));
        assert_eq!(l.name, "x-{part}.jsonl.gz");
        assert_eq!(l.file_name(12), "x-00012.jsonl.gz");
        assert_eq!(l.part_of("x-00012.jsonl.gz"), Some(12));
        assert_eq!(l.part_of("x-.jsonl.gz"), None);
        assert_eq!(l.part_of("x-1a.jsonl.gz"), None);
        assert_eq!(l.part_of("z-1.jsonl.gz"), None);

        let l = layout(json!({"path": "d/{part}_rows.csv"}));
        assert_eq!(l.file_name(1), "00001_rows.csv");
    }

    #[test]
    fn directory_layout_names_parts_by_format() {
        let l = layout(json!({"path": "out/", "format": "json_lines", "compression": "zstd"}));
        assert_eq!(l.dir, PathBuf::from("out"));
        assert_eq!(l.name, "part-{part}.jsonl.zst");
        let l = layout(json!({"path": "/", "format": "json_array"}));
        assert_eq!(l.dir, PathBuf::from("/"));
    }

    #[test]
    fn part_in_the_directory_is_refused() {
        let cfg: FileSinkConfig =
            serde_json::from_value(json!({"path": "a{part}/x.jsonl"})).unwrap();
        let e = Layout::new(&cfg, FileFormat::JsonLines, Compression::None).unwrap_err();
        assert!(e.to_string().contains("directory"), "{e}");
    }

    #[test]
    fn insert_part_handles_dotless_and_hidden_names() {
        assert_eq!(insert_part("data"), "data-{part}");
        assert_eq!(insert_part(".hidden.jsonl"), ".hidden-{part}.jsonl");
        assert_eq!(insert_part("a.b.c"), "a-{part}.b.c");
    }

    #[test]
    fn staging_dir_is_hidden_and_sanitized() {
        let l = layout(json!({"path": "out/x y-{part}.jsonl"}));
        assert_eq!(
            l.staging_dir(),
            PathBuf::from("out/.faucet-overwrite-x_y-_part_.jsonl")
        );
    }

    #[test]
    fn existing_and_stale_temps() {
        let dir = tempfile::tempdir().unwrap();
        let l = Layout {
            dir: dir.path().to_path_buf(),
            name: "p-{part}.jsonl".into(),
        };
        assert!(l.existing(&dir.path().join("missing")).unwrap().is_empty());
        for n in ["p-00002.jsonl", "p-00001.jsonl", "other.jsonl"] {
            std::fs::write(dir.path().join(n), b"").unwrap();
        }
        std::fs::create_dir(dir.path().join("p-00009.jsonl")).unwrap();
        for n in [
            "p-00003.jsonl.faucet-tmp",
            "p-00004.jsonl.faucet-tmp-body",
            "q.jsonl.faucet-tmp",
        ] {
            std::fs::write(dir.path().join(n), b"").unwrap();
        }
        let got: Vec<u64> = l
            .existing(dir.path())
            .unwrap()
            .into_iter()
            .map(|(n, _)| n)
            .collect();
        assert_eq!(got, vec![1, 2]);
        l.remove_stale_temps(dir.path());
        assert!(!dir.path().join("p-00003.jsonl.faucet-tmp").exists());
        assert!(!dir.path().join("p-00004.jsonl.faucet-tmp-body").exists());
        assert!(dir.path().join("q.jsonl.faucet-tmp").exists());
        l.remove_stale_temps(&dir.path().join("missing"));
        let file = dir.path().join("p-00001.jsonl");
        let e = l.existing(&file).unwrap_err();
        assert!(e.to_string().contains("listing"), "{e}");
    }

    #[test]
    fn tmp_path_appends_the_suffix() {
        assert_eq!(
            tmp_path(Path::new("a/b.csv"), TMP_SUFFIX),
            PathBuf::from("a/b.csv.faucet-tmp")
        );
    }
}
