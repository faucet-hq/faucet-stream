//! Which files a run reads, in which order — listing, the `stable_for_secs`
//! guard, and the incremental bookmark. Pure apart from [`list_local`].

use crate::config::IncrementalBy;
use faucet_core::FaucetError;
use serde_json::{Value, json};
use std::path::Path;

/// One file a run may read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Candidate {
    /// The path (or URL) as the rest of the connector names it.
    pub path: String,
    /// Modification time in nanoseconds since the Unix epoch, when known.
    pub mtime_ns: Option<i64>,
    /// Inode change time (nanoseconds since the epoch) where the platform has
    /// one. A rename or a timestamp-preserving copy moves it but not `mtime`.
    pub ctime_ns: Option<i64>,
}

/// The incremental position: what the previous runs have read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Bookmark {
    /// The newest modification time read, and every path read at exactly that
    /// time — a later file with the same timestamp is still new.
    Mtime { mtime_ns: i64, paths: Vec<String> },
    /// The last path read.
    Name { last: String },
}

impl Bookmark {
    /// The JSON the state store keeps.
    pub fn to_value(&self) -> Value {
        match self {
            Self::Mtime { mtime_ns, paths } => {
                json!({"by": "mtime", "mtime_ns": mtime_ns, "paths": paths})
            }
            Self::Name { last } => json!({"by": "name", "last": last}),
        }
    }

    /// Read a stored bookmark, refusing one written under the other mode —
    /// reinterpreting a name as a timestamp would silently skip or re-read
    /// files.
    pub fn from_value(v: &Value, by: IncrementalBy) -> Result<Self, FaucetError> {
        let bad = || FaucetError::State(format!("file source: unreadable bookmark {v}"));
        let stored = v.get("by").and_then(Value::as_str).ok_or_else(bad)?;
        match (by, stored) {
            (IncrementalBy::Mtime, "mtime") => Ok(Self::Mtime {
                mtime_ns: v.get("mtime_ns").and_then(Value::as_i64).ok_or_else(bad)?,
                paths: v
                    .get("paths")
                    .and_then(Value::as_array)
                    .ok_or_else(bad)?
                    .iter()
                    .map(|p| p.as_str().map(str::to_string).ok_or_else(bad))
                    .collect::<Result<_, _>>()?,
            }),
            (IncrementalBy::Name, "name") => Ok(Self::Name {
                last: v
                    .get("last")
                    .and_then(Value::as_str)
                    .ok_or_else(bad)?
                    .to_string(),
            }),
            (_, other) => Err(FaucetError::State(format!(
                "file source: the stored bookmark was written with `incremental.by: {other}`; \
                 clear the state to switch modes"
            ))),
        }
    }
}

/// Whether `c` is new relative to `bookmark`.
pub fn is_new(bookmark: Option<&Bookmark>, c: &Candidate) -> Result<bool, FaucetError> {
    Ok(match bookmark {
        None => true,
        Some(Bookmark::Name { last }) => c.path.as_str() > last.as_str(),
        Some(Bookmark::Mtime { mtime_ns, paths }) => {
            let m = mtime_of(c)?;
            m > *mtime_ns || (m == *mtime_ns && !paths.contains(&c.path))
        }
    })
}

/// A skipped file whose inode changed after the watermark: it landed after
/// the newest file read, with a preserved (older) modification time.
pub fn arrived_after_watermark(bookmark: Option<&Bookmark>, c: &Candidate) -> bool {
    match (bookmark, c.mtime_ns, c.ctime_ns) {
        (Some(Bookmark::Mtime { mtime_ns, .. }), Some(m), Some(ct)) => {
            m < *mtime_ns && ct > *mtime_ns
        }
        _ => false,
    }
}

fn mtime_of(c: &Candidate) -> Result<i64, FaucetError> {
    c.mtime_ns.ok_or_else(|| {
        FaucetError::Source(format!(
            "file source: '{}' has no modification time, which this mode needs",
            c.path
        ))
    })
}

/// The bookmark after reading `c`.
pub fn advance(
    bookmark: Option<Bookmark>,
    by: IncrementalBy,
    c: &Candidate,
) -> Result<Bookmark, FaucetError> {
    Ok(match by {
        IncrementalBy::Name => Bookmark::Name {
            last: c.path.clone(),
        },
        IncrementalBy::Mtime => {
            let m = mtime_of(c)?;
            match bookmark {
                Some(Bookmark::Mtime {
                    mtime_ns,
                    mut paths,
                }) if mtime_ns == m => {
                    paths.push(c.path.clone());
                    Bookmark::Mtime { mtime_ns, paths }
                }
                Some(Bookmark::Mtime { mtime_ns, paths }) if mtime_ns > m => {
                    Bookmark::Mtime { mtime_ns, paths }
                }
                _ => Bookmark::Mtime {
                    mtime_ns: m,
                    paths: vec![c.path.clone()],
                },
            }
        }
    })
}

/// Order and filter the listing: settled files only, new ones only, sorted by
/// modification time then path in `mtime` mode (so the bookmark only moves
/// forward) and by path otherwise.
pub fn select(
    mut files: Vec<Candidate>,
    by: Option<IncrementalBy>,
    bookmark: Option<&Bookmark>,
    stable_for_secs: Option<u64>,
    now_ns: i64,
) -> Result<Vec<Candidate>, FaucetError> {
    if let Some(secs) = stable_for_secs {
        let cutoff = now_ns.saturating_sub((secs as i64).saturating_mul(1_000_000_000));
        let mut kept = Vec::with_capacity(files.len());
        for f in files {
            if mtime_of(&f)? <= cutoff {
                kept.push(f);
            } else {
                tracing::info!(path = %f.path, "file source: skipping a file modified within stable_for_secs");
            }
        }
        files = kept;
    }
    let mut out = Vec::with_capacity(files.len());
    let mut late = Vec::new();
    for f in files {
        if is_new(bookmark, &f)? {
            out.push(f);
        } else if arrived_after_watermark(bookmark, &f) {
            late.push(f.path);
        }
    }
    if !late.is_empty() {
        tracing::warn!(
            count = late.len(),
            files = ?late.iter().take(10).collect::<Vec<_>>(),
            "file source: files arrived after the incremental watermark but carry an older \
             modification time (a rename, `cp -p` or `rsync -t`), so `by: mtime` skips them; \
             touch them, or use `by: name` with sortable names"
        );
    }
    match by {
        Some(IncrementalBy::Mtime) => {
            for f in &out {
                mtime_of(f)?;
            }
            out.sort_by(|a, b| {
                a.mtime_ns
                    .cmp(&b.mtime_ns)
                    .then_with(|| a.path.cmp(&b.path))
            });
        }
        _ => out.sort_by(|a, b| a.path.cmp(&b.path)),
    }
    Ok(out)
}

/// Whether `path` is a glob pattern rather than a literal path.
pub fn is_glob(path: &str) -> bool {
    path.contains(['*', '?', '['])
}

fn mtime_ns(meta: &std::fs::Metadata) -> Option<i64> {
    let t = meta.modified().ok()?;
    let d = t.duration_since(std::time::UNIX_EPOCH).ok()?;
    i64::try_from(d.as_nanos()).ok()
}

#[cfg(unix)]
fn ctime_ns(meta: &std::fs::Metadata) -> Option<i64> {
    use std::os::unix::fs::MetadataExt;
    meta.ctime()
        .checked_mul(1_000_000_000)?
        .checked_add(meta.ctime_nsec())
}

#[cfg(not(unix))]
fn ctime_ns(_meta: &std::fs::Metadata) -> Option<i64> {
    None
}

fn io_err(path: &Path, e: impl std::fmt::Display) -> FaucetError {
    FaucetError::Source(format!("file source: '{}': {e}", path.display()))
}

fn unfinished_output(path: &Path) -> bool {
    faucet_common_file::write::is_unfinished_output_path(path)
}

/// List the regular files `path` names: itself, a directory's files
/// (recursively when asked), or a glob's matches. Symlinks are followed; an
/// unreadable entry fails the listing with its path. A directory or glob
/// listing skips a file sink's unfinished output (scratch files and the
/// swap area of an uncommitted overwrite).
pub fn list_local(path: &str, recursive: bool) -> Result<Vec<Candidate>, FaucetError> {
    let mut out = Vec::new();
    if is_glob(path) {
        let matches = glob::glob(path)
            .map_err(|e| FaucetError::Config(format!("file source: bad glob {path:?}: {e}")))?;
        for entry in matches {
            let p = entry.map_err(|e| io_err(e.path(), e.error()))?;
            if unfinished_output(&p) {
                continue;
            }
            let meta = std::fs::metadata(&p).map_err(|e| io_err(&p, e))?;
            if meta.is_file() {
                out.push(Candidate {
                    path: p.to_string_lossy().into_owned(),
                    mtime_ns: mtime_ns(&meta),
                    ctime_ns: ctime_ns(&meta),
                });
            }
        }
        return Ok(out);
    }
    let root = Path::new(path);
    let meta = std::fs::metadata(root).map_err(|e| io_err(root, e))?;
    if meta.is_file() {
        return Ok(vec![Candidate {
            path: path.to_string(),
            mtime_ns: mtime_ns(&meta),
            ctime_ns: ctime_ns(&meta),
        }]);
    }
    walk(root, recursive, &mut out)?;
    Ok(out)
}

fn walk(dir: &Path, recursive: bool, out: &mut Vec<Candidate>) -> Result<(), FaucetError> {
    for entry in std::fs::read_dir(dir).map_err(|e| io_err(dir, e))? {
        let entry = entry.map_err(|e| io_err(dir, e))?;
        let p = entry.path();
        if unfinished_output(&p) {
            continue;
        }
        let meta = std::fs::metadata(&p).map_err(|e| io_err(&p, e))?;
        if meta.is_dir() {
            if recursive {
                walk(&p, recursive, out)?;
            }
        } else if meta.is_file() {
            out.push(Candidate {
                path: p.to_string_lossy().into_owned(),
                mtime_ns: mtime_ns(&meta),
                ctime_ns: ctime_ns(&meta),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn c(path: &str, m: i64) -> Candidate {
        Candidate {
            path: path.into(),
            mtime_ns: Some(m),
            ctime_ns: Some(m),
        }
    }

    #[test]
    fn a_file_landing_with_a_preserved_old_mtime_is_reported() {
        let bm = Bookmark::Mtime {
            mtime_ns: 100,
            paths: vec!["new".into()],
        };
        let copied = Candidate {
            path: "copied".into(),
            mtime_ns: Some(50),
            ctime_ns: Some(200),
        };
        assert!(arrived_after_watermark(Some(&bm), &copied));
        assert!(!arrived_after_watermark(Some(&bm), &c("old", 50)));
        assert!(!arrived_after_watermark(None, &copied));
        let name = Bookmark::Name { last: "a".into() };
        assert!(!arrived_after_watermark(Some(&name), &copied));
        let sel = select(
            vec![copied, c("old", 50), c("newer", 150)],
            Some(IncrementalBy::Mtime),
            Some(&bm),
            None,
            0,
        )
        .unwrap();
        assert_eq!(sel, vec![c("newer", 150)]);
    }

    #[cfg(unix)]
    #[test]
    fn local_listing_reports_the_inode_change_time() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("a.jsonl");
        std::fs::write(&p, "{}\n").unwrap();
        let f = std::fs::File::options().write(true).open(&p).unwrap();
        f.set_modified(std::time::UNIX_EPOCH + std::time::Duration::from_secs(1))
            .unwrap();
        let listed = list_local(&p.to_string_lossy(), false).unwrap();
        assert_eq!(listed[0].mtime_ns, Some(1_000_000_000));
        assert!(listed[0].ctime_ns.unwrap() > 1_000_000_000);
    }

    #[test]
    fn bookmarks_round_trip_and_refuse_the_other_mode() {
        for (b, by) in [
            (
                Bookmark::Mtime {
                    mtime_ns: 5,
                    paths: vec!["a".into()],
                },
                IncrementalBy::Mtime,
            ),
            (Bookmark::Name { last: "x".into() }, IncrementalBy::Name),
        ] {
            assert_eq!(Bookmark::from_value(&b.to_value(), by).unwrap(), b);
        }
        let err = Bookmark::from_value(&json!({"by": "name", "last": "x"}), IncrementalBy::Mtime)
            .expect_err("mode switch");
        assert!(err.to_string().contains("clear the state"), "{err}");
        for bad in [
            json!({}),
            json!({"by": "mtime"}),
            json!({"by": "mtime", "mtime_ns": 1, "paths": [1]}),
            json!({"by": "name"}),
        ] {
            let by = if bad.get("by") == Some(&json!("name")) {
                IncrementalBy::Name
            } else {
                IncrementalBy::Mtime
            };
            assert!(Bookmark::from_value(&bad, by).is_err(), "{bad}");
        }
    }

    #[test]
    fn mtime_mode_orders_by_time_and_keeps_ties_new() {
        let files = vec![c("b", 2), c("a", 2), c("z", 1)];
        let sel = select(files.clone(), Some(IncrementalBy::Mtime), None, None, 0).unwrap();
        assert_eq!(
            sel.iter().map(|f| f.path.as_str()).collect::<Vec<_>>(),
            ["z", "a", "b"]
        );
        let mut bm = None;
        for f in &sel[..2] {
            bm = Some(advance(bm, IncrementalBy::Mtime, f).unwrap());
        }
        assert_eq!(
            bm,
            Some(Bookmark::Mtime {
                mtime_ns: 2,
                paths: vec!["a".into()]
            })
        );
        let again = select(files, Some(IncrementalBy::Mtime), bm.as_ref(), None, 0).unwrap();
        assert_eq!(
            again,
            vec![c("b", 2)],
            "a tie at the watermark is still new"
        );
        let tied = advance(bm.clone(), IncrementalBy::Mtime, &c("b", 2)).unwrap();
        assert_eq!(
            tied,
            Bookmark::Mtime {
                mtime_ns: 2,
                paths: vec!["a".into(), "b".into()]
            }
        );
        let older = advance(bm, IncrementalBy::Mtime, &c("q", 1)).unwrap();
        assert_eq!(
            older,
            Bookmark::Mtime {
                mtime_ns: 2,
                paths: vec!["a".into()]
            }
        );
        assert!(
            advance(
                None,
                IncrementalBy::Mtime,
                &Candidate {
                    path: "n".into(),
                    mtime_ns: None,
                    ctime_ns: None,
                }
            )
            .is_err()
        );
    }

    #[test]
    fn name_mode_reads_paths_after_the_last() {
        let bm = advance(None, IncrementalBy::Name, &c("2026-01", 0)).unwrap();
        let sel = select(
            vec![c("2026-02", 0), c("2025-12", 9), c("2026-01", 0)],
            Some(IncrementalBy::Name),
            Some(&bm),
            None,
            0,
        )
        .unwrap();
        assert_eq!(sel, vec![c("2026-02", 0)]);
    }

    #[test]
    fn unstable_files_are_skipped_and_an_unknown_mtime_is_an_error() {
        let now = 100 * 1_000_000_000;
        let sel = select(
            vec![c("old", 10 * 1_000_000_000), c("fresh", 99 * 1_000_000_000)],
            None,
            None,
            Some(5),
            now,
        )
        .unwrap();
        assert_eq!(sel.len(), 1);
        assert_eq!(sel[0].path, "old");
        let none = Candidate {
            path: "u".into(),
            mtime_ns: None,
            ctime_ns: None,
        };
        assert!(select(vec![none.clone()], None, None, Some(1), now).is_err());
        assert!(select(vec![none], Some(IncrementalBy::Mtime), None, None, now).is_err());
    }

    #[test]
    fn local_listing_covers_files_dirs_globs_and_errors() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::write(root.join("a.jsonl"), "{}\n").unwrap();
        std::fs::create_dir(root.join("sub")).unwrap();
        std::fs::write(root.join("sub/b.csv"), "x\n1\n").unwrap();
        let r = root.to_string_lossy().to_string();
        assert_eq!(list_local(&r, false).unwrap().len(), 1);
        assert_eq!(list_local(&r, true).unwrap().len(), 2);
        let one = format!("{r}/a.jsonl");
        let f = list_local(&one, false).unwrap();
        assert_eq!(f[0].path, one);
        assert!(f[0].mtime_ns.is_some());
        assert_eq!(
            list_local(&format!("{r}/**/*.csv"), false).unwrap().len(),
            1
        );
        assert_eq!(
            list_local(&format!("{r}/*"), false).unwrap().len(),
            1,
            "globs match files only"
        );
        assert!(
            list_local(&format!("{r}/missing"), false)
                .unwrap_err()
                .to_string()
                .contains("missing")
        );
        assert!(matches!(
            list_local("[", false),
            Err(FaucetError::Config(_))
        ));
        assert!(is_glob("a/*.csv") && !is_glob("a/b.csv"));
    }
}
