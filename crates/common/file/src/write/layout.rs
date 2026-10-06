//! File names: the `{part}` template, part numbering and scratch names.

use super::options::PART_TOKEN;
use faucet_core::{Compression, FaucetError, FileFormat};
use std::path::{Path, PathBuf};

/// Suffix of a file that is still being written. Every other scratch file
/// the writer makes for the same output adds one of [`SCRATCH_ROLES`] to it.
pub const TMP_SUFFIX: &str = ".faucet-tmp";
/// Suffix of a CSV body that is waiting for its header.
pub const BODY_SUFFIX: &str = ".faucet-tmp-body";
/// What follows [`TMP_SUFFIX`] on a scratch file's siblings: the CSV body
/// waiting for its header, the earlier contents while a Parquet file is
/// rewritten under a wider schema or an existing file is read back, the
/// sealed copy being written, and the copy of the last published version
/// kept to continue from.
pub const SCRATCH_ROLES: &[&str] = &[BODY_ROLE, OLD_ROLE, SEAL_ROLE, PREV_ROLE];
pub(crate) const BODY_ROLE: &str = "-body";
pub(crate) const OLD_ROLE: &str = "-old";
pub(crate) const SEAL_ROLE: &str = "-seal";
pub(crate) const PREV_ROLE: &str = "-prev";
/// What follows [`TMP_SUFFIX`] on an upload in flight to a store that has no
/// atomic put (SFTP), before a unique id: `<name>.faucet-tmp-upload-<id>`.
pub const UPLOAD_ROLE: &str = "-upload-";
/// Prefix of the swap area an overwrite run (`write_mode: overwrite`) writes
/// into before its files are moved into place.
pub const SWAP_PREFIX: &str = ".faucet-overwrite-";
/// Digits in a part number (`00001`).
pub const PART_WIDTH: usize = 5;

/// Whether `name` (a file name, no directory) is a scratch file of the
/// shared writer: a file still being written, or one of its siblings. A
/// reader of the output directory must skip it.
pub fn is_scratch_name(name: &str) -> bool {
    scratch_base(name).is_some()
}

/// The temporary name an upload of `key` is written under before it is
/// renamed into place. Readers skip it like any other scratch file.
pub fn upload_scratch_key(key: &str, id: &str) -> String {
    format!("{key}{TMP_SUFFIX}{UPLOAD_ROLE}{id}")
}

/// Whether the object `key` (`/`-separated) is unfinished output of a file
/// sink: a scratch file, or anything inside the swap area of an overwrite run
/// that has not committed. A listing of a sink's output must skip it.
pub fn is_unfinished_output_key(key: &str) -> bool {
    let mut parts = key.split('/').filter(|p| !p.is_empty()).collect::<Vec<_>>();
    let Some(name) = parts.pop() else {
        return false;
    };
    is_scratch_name(name) || parts.into_iter().any(is_swap_dir_name)
}

/// [`is_unfinished_output_key`] for a local path.
pub fn is_unfinished_output_path(path: &Path) -> bool {
    path.file_name()
        .and_then(|n| n.to_str())
        .is_some_and(is_scratch_name)
        || path
            .parent()
            .into_iter()
            .flat_map(Path::components)
            .filter_map(|c| c.as_os_str().to_str())
            .any(is_swap_dir_name)
}

/// Whether `name` (a directory name) is the swap area of an overwrite run,
/// whose files are not part of the output until the run commits.
pub fn is_swap_dir_name(name: &str) -> bool {
    name.starts_with(SWAP_PREFIX)
}

/// The output file a scratch file belongs to, or `None` when `name` is not a
/// scratch file.
fn scratch_base(name: &str) -> Option<&str> {
    let i = name.rfind(TMP_SUFFIX)?;
    let role = &name[i + TMP_SUFFIX.len()..];
    (role.is_empty()
        || SCRATCH_ROLES.contains(&role)
        || role.len() > UPLOAD_ROLE.len() && role.starts_with(UPLOAD_ROLE))
    .then_some(&name[..i])
}

/// The file-name template of one output set. `{part}` is present when the
/// output is numbered.
///
/// **Experimental** (PRINCIPLES.md §3): this block's shape may change in a
/// minor release; any change is called out in the changelog.
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
        self.name
            .replace(PART_TOKEN, &format!("{n:0width$}", width = PART_WIDTH))
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
        scratch_base(name).is_some_and(|base| self.part_of(base).is_some())
    }

    /// The swap area's name for an overwrite run of this template: hidden
    /// and made of path-safe characters.
    pub fn swap_dir_name(&self) -> String {
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
        format!("{SWAP_PREFIX}{tag}")
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
pub(crate) fn tmp_path(path: &Path, suffix: &str) -> PathBuf {
    let mut s = path.as_os_str().to_owned();
    s.push(suffix);
    PathBuf::from(s)
}

/// An I/O error on `path`, as a typed sink error.
pub(crate) fn io_err(what: &str, path: &Path, e: std::io::Error) -> FaucetError {
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
    fn swap_names_scratch_and_selection() {
        let (_, l) = t("out/x y-{part}.jsonl", false);
        assert_eq!(l.swap_dir_name(), ".faucet-overwrite-x_y-_part_.jsonl");
        assert!(is_swap_dir_name(&l.swap_dir_name()));
        assert!(!is_swap_dir_name("x y-00001.jsonl"));
        for role in ["", "-body", "-old", "-seal", "-prev"] {
            let n = format!("x y-00003.jsonl.faucet-tmp{role}");
            assert!(l.owns_scratch(&n), "{n}");
            assert!(is_scratch_name(&n), "{n}");
        }
        assert!(!l.owns_scratch("x y-00003.jsonl.faucet-tmp-other"));
        assert!(!is_scratch_name("x y-00003.jsonl.faucet-tmp-other"));
        assert!(!l.owns_scratch("q.jsonl.faucet-tmp"));
        assert!(is_scratch_name("q.jsonl.faucet-tmp"));
        assert!(!l.owns_scratch("x y-00001.jsonl"));
        assert!(!is_scratch_name("x y-00001.jsonl"));
        assert!(is_scratch_name(BODY_SUFFIX));
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

    #[test]
    fn unfinished_output_keys_and_upload_scratch() {
        let up = upload_scratch_key("out/a.csv", "abc");
        assert_eq!(up, "out/a.csv.faucet-tmp-upload-abc");
        assert!(is_unfinished_output_key(&up));
        assert!(!is_scratch_name("a.csv.faucet-tmp-upload-"));
        assert!(is_unfinished_output_key(
            "out/.faucet-overwrite-a_.csv/a.csv"
        ));
        assert!(is_unfinished_output_key(
            "out/.faucet-overwrite-a_.csv/.faucet-swap"
        ));
        assert!(!is_unfinished_output_key("out/a.csv"));
        assert!(!is_unfinished_output_key(""));
        assert!(is_unfinished_output_path(Path::new(
            "o/.faucet-overwrite-x/a"
        )));
        assert!(is_unfinished_output_path(Path::new("o/a.faucet-tmp")));
        assert!(!is_unfinished_output_path(Path::new("o/a.csv")));
        let t = NameTemplate {
            name: "a.csv".into(),
        };
        assert!(t.owns_scratch("a.csv.faucet-tmp-upload-1f"));
    }
}
