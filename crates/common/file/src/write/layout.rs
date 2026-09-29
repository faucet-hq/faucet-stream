//! File names: the `{part}` template, part numbering and scratch names.

use super::options::PART_TOKEN;
use faucet_core::{Compression, FaucetError, FileFormat};
use std::path::{Path, PathBuf};

/// Suffix of a file that is still being written.
pub const TMP_SUFFIX: &str = ".faucet-tmp";
/// Suffix of a CSV body that is waiting for its header.
pub const BODY_SUFFIX: &str = ".faucet-tmp-body";
/// Prefix of the staging area an overwrite run writes into.
pub const STAGING_PREFIX: &str = ".faucet-overwrite-";

/// The file-name template of one output set. `{part}` is present when the
/// output is numbered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NameTemplate {
    /// The template, e.g. `part-{part}.jsonl.gz` or `export.csv`.
    pub name: String,
}

impl NameTemplate {
    /// Split an output `path` into the directory (or key prefix) it names and
    /// the file-name template. A path ending in `/` is a directory, whose
    /// files are `part-{part}` plus the format's extension and codec suffix;
    /// with `rolls_over` and no `{part}`, `-{part}` is inserted before the
    /// extension. The directory is `""` when the path has none.
    pub fn from_path(
        path: &str,
        format: FileFormat,
        codec: Compression,
        rolls_over: bool,
    ) -> Result<(String, Self), FaucetError> {
        if crate::is_directory_path(path) {
            let dir = path.trim_end_matches(['/', '\\']);
            let dir = if dir.is_empty() { "/" } else { dir };
            let name = format!(
                "part-{PART_TOKEN}{}{}",
                format.extension(),
                codec_suffix(codec)
            );
            return Ok((dir.to_string(), Self { name }));
        }
        let p = Path::new(path);
        let name = p
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| FaucetError::Config(format!("'{path}' has no file name")))?
            .to_string();
        let dir = match p.parent() {
            Some(d) if !d.as_os_str().is_empty() => d.to_string_lossy().into_owned(),
            _ => String::new(),
        };
        if dir.contains(PART_TOKEN) {
            return Err(FaucetError::Config(format!(
                "`{{part}}` may appear only in the file name, not the directory, of '{path}'"
            )));
        }
        let name = if rolls_over && !name.contains(PART_TOKEN) {
            insert_part(&name)
        } else {
            name
        };
        Ok((dir, Self { name }))
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
    /// template's files. An unnumbered template's single file is part 1.
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

    /// Whether `name` is a scratch file of one of this template's files.
    pub fn owns_scratch(&self, name: &str) -> bool {
        let Some(at) = name.rfind(TMP_SUFFIX) else {
            return false;
        };
        let (base, rest) = (&name[..at], &name[at + TMP_SUFFIX.len()..]);
        let tail_ok = rest.is_empty()
            || rest
                .strip_prefix('-')
                .is_some_and(|t| !t.is_empty() && t.chars().all(|c| c.is_ascii_alphanumeric() || c == '-'));
        tail_ok && self.part_of(base).is_some()
    }

    /// The staging area's name for an overwrite run of this template: hidden
    /// and made of path-safe characters.
    pub fn staging_name(&self) -> String {
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
        format!("{STAGING_PREFIX}{tag}")
    }

    /// `(part, name)` of every name in `names` that belongs to this template,
    /// sorted by part.
    pub fn select(&self, names: Vec<String>) -> Vec<(u64, String)> {
        let mut out: Vec<(u64, String)> = names
            .into_iter()
            .filter_map(|n| self.part_of(&n).map(|p| (p, n)))
            .collect();
        out.sort();
        out
    }
}

pub(crate) fn insert_part(name: &str) -> String {
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

/// `path` with `suffix` appended to its last component.
pub fn tmp_path(path: &Path, suffix: &str) -> PathBuf {
    let mut s = path.as_os_str().to_owned();
    s.push(suffix);
    PathBuf::from(s)
}

/// An I/O error on `path`, as a typed sink error.
pub fn io_err(what: &str, path: &Path, e: std::io::Error) -> FaucetError {
    FaucetError::Sink(format!("file sink: {what} '{}': {e}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(path: &str, rolls: bool) -> (String, NameTemplate) {
        NameTemplate::from_path(path, FileFormat::JsonLines, Compression::None, rolls).unwrap()
    }

    #[test]
    fn single_file_and_numbered_templates() {
        let (dir, l) = t("out/x.jsonl", false);
        assert_eq!(dir, "out");
        assert_eq!(l.name, "x.jsonl");
        assert!(!l.numbered());
        assert_eq!(l.file_name(3), "x.jsonl");
        assert_eq!(l.part_of("x.jsonl"), Some(1));
        assert_eq!(l.part_of("y.jsonl"), None);

        let (dir, l) = t("x.jsonl.gz", true);
        assert_eq!(dir, "");
        assert_eq!(l.name, "x-{part}.jsonl.gz");
        assert_eq!(l.file_name(12), "x-00012.jsonl.gz");
        assert_eq!(l.part_of("x-00012.jsonl.gz"), Some(12));
        assert_eq!(l.part_of("x-.jsonl.gz"), None);
        assert_eq!(l.part_of("x-1a.jsonl.gz"), None);
        assert_eq!(l.part_of("z-1.jsonl.gz"), None);
        assert_eq!(
            t("d/{part}_rows.csv", false).1.file_name(1),
            "00001_rows.csv"
        );
    }

    #[test]
    fn directory_templates_name_parts_by_format() {
        let (dir, l) =
            NameTemplate::from_path("out/", FileFormat::JsonLines, Compression::Zstd, false)
                .unwrap();
        assert_eq!(dir, "out");
        assert_eq!(l.name, "part-{part}.jsonl.zst");
        let (dir, l) =
            NameTemplate::from_path("/", FileFormat::JsonArray, Compression::Gzip, false).unwrap();
        assert_eq!(dir, "/");
        assert_eq!(l.name, "part-{part}.json.gz");
    }

    #[test]
    fn part_in_the_directory_and_a_missing_name_are_refused() {
        let e = NameTemplate::from_path(
            "a{part}/x.jsonl",
            FileFormat::JsonLines,
            Compression::None,
            false,
        )
        .unwrap_err();
        assert!(e.to_string().contains("directory"), "{e}");
        assert!(
            NameTemplate::from_path("..", FileFormat::JsonLines, Compression::None, false).is_err()
        );
    }

    #[test]
    fn insert_part_handles_dotless_and_hidden_names() {
        assert_eq!(insert_part("data"), "data-{part}");
        assert_eq!(insert_part(".hidden.jsonl"), ".hidden-{part}.jsonl");
        assert_eq!(insert_part("a.b.c"), "a-{part}.b.c");
    }

    #[test]
    fn staging_names_scratch_and_selection() {
        let (_, l) = t("out/x y-{part}.jsonl", false);
        assert_eq!(l.staging_name(), ".faucet-overwrite-x_y-_part_.jsonl");
        assert!(l.owns_scratch("x y-00003.jsonl.faucet-tmp"));
        assert!(l.owns_scratch("x y-00003.jsonl.faucet-tmp-body"));
        assert!(l.owns_scratch("x y-00003.jsonl.faucet-tmp-123-0a1b2c3d4e5f"));
        assert!(l.owns_scratch("x y-00003.jsonl.faucet-tmp-123-0a1b2c3d4e5f-body"));
        assert!(!l.owns_scratch("x y-00003.jsonl.faucet-tmp-"));
        assert!(!l.owns_scratch("x y-00003.jsonl.faucet-tmp.bak"));
        assert!(!l.owns_scratch("x y-00003.jsonl"));
        assert!(l.owns_scratch("x y-00004.jsonl.faucet-tmp-body"));
        assert!(!l.owns_scratch("q.jsonl.faucet-tmp"));
        assert!(!l.owns_scratch("x y-00001.jsonl"));
        let got = l.select(vec![
            "x y-00002.jsonl".into(),
            "x y-00001.jsonl".into(),
            "other".into(),
        ]);
        assert_eq!(got.iter().map(|(n, _)| *n).collect::<Vec<_>>(), vec![1, 2]);
    }

    #[test]
    fn tmp_path_appends_the_suffix() {
        assert_eq!(
            tmp_path(Path::new("a/b.csv"), TMP_SUFFIX),
            PathBuf::from("a/b.csv.faucet-tmp")
        );
    }
}
