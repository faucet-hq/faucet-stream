//! Pluggable state store for incremental replication bookmarks.
//!
//! Sources that support incremental replication need to remember where they
//! left off across runs. This module defines the [`StateStore`] trait that
//! the pipeline orchestrator uses to read and persist that progress, plus two
//! ready-to-use implementations:
//!
//! - [`MemoryStateStore`] — in-process map. Useful for tests and for runs
//!   that intentionally start fresh each time.
//! - [`FileStateStore`] — one JSON file per key, written via atomic rename
//!   so a crash mid-update never leaves the bookmark torn.
//!
//! Heavier backends (Redis, PostgreSQL) live in their own crates so
//! `faucet-core` stays dependency-light. They implement the same trait.

use crate::error::FaucetError;
use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

/// Persistent key/value store for replication bookmarks and pipeline checkpoints.
///
/// Implementations must be safe to call from multiple tasks at once. The
/// trait is intentionally minimal — three operations cover every bookmark
/// flow the pipeline orchestrator needs.
#[async_trait]
pub trait StateStore: Send + Sync {
    /// Return the value stored under `key`, or `None` if no entry exists.
    async fn get(&self, key: &str) -> Result<Option<Value>, FaucetError>;

    /// Store `value` under `key`, replacing any previous entry.
    ///
    /// Implementations should make the update durable before returning so a
    /// crash immediately after `put` does not lose the bookmark.
    async fn put(&self, key: &str, value: &Value) -> Result<(), FaucetError>;

    /// Remove the entry for `key`. A missing key is not an error.
    async fn delete(&self, key: &str) -> Result<(), FaucetError>;

    /// Run a fast, non-mutating preflight probe (used by `faucet doctor`).
    ///
    /// The default returns
    /// [`CheckReport::not_implemented`](crate::check::CheckReport::not_implemented).
    /// Built-in stores override this with a reachability + sentinel
    /// get/put/delete probe that leaves no residue.
    async fn check(
        &self,
        _ctx: &crate::check::CheckContext,
    ) -> Result<crate::check::CheckReport, FaucetError> {
        Ok(crate::check::CheckReport::not_implemented())
    }
}

/// Sentinel key used by state-store `check()` probes. Valid per
/// [`validate_state_key`] and deleted after the probe so it leaves no residue.
pub const DOCTOR_SENTINEL_KEY: &str = "faucet_doctor_probe";

/// Reject keys that could escape the storage namespace or break filename
/// rules on common filesystems. Allowed: ASCII letters, digits, `_`, `-`,
/// `:`, `.`. Empty keys are rejected.
pub fn validate_state_key(key: &str) -> Result<(), FaucetError> {
    if key.is_empty() {
        return Err(FaucetError::State("state key must not be empty".into()));
    }
    if key.len() > 256 {
        return Err(FaucetError::State(format!(
            "state key '{key}' exceeds 256 characters"
        )));
    }
    for (i, c) in key.char_indices() {
        let ok = c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | ':' | '.' | '/');
        if !ok {
            return Err(FaucetError::State(format!(
                "state key '{key}' contains illegal character {c:?} at byte {i}"
            )));
        }
    }
    if key == "." || key == ".." || key.starts_with('.') {
        return Err(FaucetError::State(format!(
            "state key '{key}' must not begin with a dot"
        )));
    }
    // `/` separates a hub owner namespace from a name (`acme/netsuite::…`);
    // it must never read as a path: no empty, `.` or `..` segments, no
    // leading or trailing separator.
    if key.contains('/')
        && key
            .split('/')
            .any(|seg| seg.is_empty() || seg == "." || seg == ".." || seg.starts_with('.'))
    {
        return Err(FaucetError::State(format!(
            "state key '{key}' has a path-like segment — `/` may only separate non-empty, non-dot segments"
        )));
    }
    Ok(())
}

// ── MemoryStateStore ────────────────────────────────────────────────────────

/// In-memory `StateStore` for tests and ephemeral pipelines.
#[derive(Default)]
pub struct MemoryStateStore {
    inner: Mutex<HashMap<String, Value>>,
}

impl MemoryStateStore {
    /// Create an empty in-memory store.
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl StateStore for MemoryStateStore {
    async fn get(&self, key: &str) -> Result<Option<Value>, FaucetError> {
        validate_state_key(key)?;
        Ok(self.inner.lock().await.get(key).cloned())
    }

    async fn put(&self, key: &str, value: &Value) -> Result<(), FaucetError> {
        validate_state_key(key)?;
        self.inner
            .lock()
            .await
            .insert(key.to_owned(), value.clone());
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), FaucetError> {
        validate_state_key(key)?;
        self.inner.lock().await.remove(key);
        Ok(())
    }

    async fn check(
        &self,
        _ctx: &crate::check::CheckContext,
    ) -> Result<crate::check::CheckReport, FaucetError> {
        // In-process map — always reachable.
        Ok(crate::check::CheckReport::single(
            crate::check::Probe::pass("sentinel", std::time::Duration::ZERO),
        ))
    }
}

// ── FileStateStore ──────────────────────────────────────────────────────────

/// Map a logical state key to a filesystem-safe filename stem. The key
/// grammar allows `:` (used by the documented `pipeline:rest:issues`
/// convention), but `:` is illegal in a filename on Windows/NTFS, so it is
/// percent-encoded as `%3A`. This is collision-free because `%` can never
/// appear in a valid key (see [`validate_state_key`]) (#78 LOW).
fn safe_filename(key: &str) -> String {
    key.replace(':', "%3A").replace('/', "%2F")
}

/// File-backed `StateStore`. Each key maps to a JSON file at
/// `{root}/{safe_filename(key)}.json`, written via atomic rename. The
/// filename stem percent-encodes `:` as `%3A` so keys using the
/// `pipeline:rest:issues` convention are valid on Windows.
///
/// Each `put` writes a temp file, fsyncs it, renames it over the final path,
/// and (on Unix) fsyncs the parent directory — so once `put` returns the
/// bookmark is durable across a crash or power loss, not merely sitting in the
/// page cache.
///
/// The store creates its root directory on first use. All writes are
/// serialized through a per-store mutex so two concurrent `put`s for the
/// same key cannot race on the temp filename. Different processes pointing
/// at the same directory rely on the filesystem's atomic rename guarantee.
pub struct FileStateStore {
    root: PathBuf,
    write_lock: Mutex<()>,
    /// Optional at-rest encryption (#207). When set, `put` seals the JSON
    /// bytes before the atomic write and `get` unseals; plaintext files
    /// remain readable (and are sealed on their next write).
    #[cfg(feature = "encryption")]
    encryption: Option<crate::encryption::CompiledEncryption>,
}

impl FileStateStore {
    /// Open or create a file-backed state store rooted at `root`.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            write_lock: Mutex::new(()),
            #[cfg(feature = "encryption")]
            encryption: None,
        }
    }

    /// Seal bookmark files at rest with the given policy (#207). Reads accept
    /// both sealed and (legacy) plaintext files; every write seals.
    #[cfg(feature = "encryption")]
    pub fn with_encryption(mut self, encryption: crate::encryption::CompiledEncryption) -> Self {
        self.encryption = Some(encryption);
        self
    }

    fn entry_path(&self, key: &str) -> PathBuf {
        self.root.join(format!("{}.json", safe_filename(key)))
    }

    fn temp_path(&self, key: &str) -> PathBuf {
        // Unique per write: a per-process random token + a monotonic counter, so
        // two writers (different processes sharing the directory, or two store
        // instances in one process) never share a temp file. A *fixed* temp name
        // let a second writer `File::create`-truncate the first's half-written
        // temp, which the first could then `rename` over the final path —
        // yielding torn/truncated state JSON that breaks resume (audit #146 H10).
        //
        // The token MUST NOT be derived from the process id: containers on a
        // shared NFS/EFS/host volume routinely reuse the same PID (each starts at
        // PID 1), so a PID + per-process counter collides across processes and
        // reintroduces exactly that torn-bookmark corruption (F50). A random v4
        // UUID minted once per process is unguessable and collision-free across
        // processes regardless of PID; the counter keeps writers within one
        // process distinct. The per-store `write_lock` only serializes writers
        // within one process. Orphaned `.tmp` files from an interrupted write are
        // harmless: `get` only ever reads the final `.json` path.
        use std::sync::OnceLock;
        use std::sync::atomic::{AtomicU64, Ordering};
        static PROC_TOKEN: OnceLock<String> = OnceLock::new();
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let token = PROC_TOKEN.get_or_init(|| uuid::Uuid::new_v4().simple().to_string());
        let seq = SEQ.fetch_add(1, Ordering::Relaxed);
        self.root
            .join(format!("{}.{}.{}.json.tmp", safe_filename(key), token, seq))
    }

    async fn ensure_root(&self) -> Result<(), FaucetError> {
        tokio::fs::create_dir_all(&self.root).await.map_err(|e| {
            FaucetError::State(format!(
                "failed to create state dir {}: {e}",
                self.root.display()
            ))
        })
    }

    /// Returns the root directory this store writes into.
    pub fn root(&self) -> &Path {
        &self.root
    }
}

#[async_trait]
impl StateStore for FileStateStore {
    async fn get(&self, key: &str) -> Result<Option<Value>, FaucetError> {
        validate_state_key(key)?;
        let path = self.entry_path(key);
        match tokio::fs::read(&path).await {
            Ok(bytes) => {
                #[cfg(feature = "encryption")]
                let bytes: Vec<u8> = if crate::encryption::is_encrypted(&bytes) {
                    match &self.encryption {
                        Some(enc) => enc.decrypt(&bytes).map_err(|e| {
                            // Wrong/rotated key must be a loud, typed error —
                            // treating it as "no bookmark" would silently
                            // trigger a full re-sync.
                            FaucetError::State(format!(
                                "state file {} could not be decrypted: {e}",
                                path.display()
                            ))
                        })?,
                        None => {
                            return Err(FaucetError::State(format!(
                                "state file {} is encrypted but no `encryption` block is \
                                 configured on the file state store — add \
                                 `state.config.encryption` with the original key",
                                path.display()
                            )));
                        }
                    }
                } else {
                    bytes
                };
                #[cfg(not(feature = "encryption"))]
                let bytes = {
                    // Built without the `encryption` feature: refuse to parse a
                    // sealed file as JSON garbage.
                    if bytes.starts_with(b"FCT1") {
                        return Err(FaucetError::State(format!(
                            "state file {} is encrypted but this build of faucet has no \
                             `encryption` feature",
                            path.display()
                        )));
                    }
                    bytes
                };
                let value: Value = serde_json::from_slice(&bytes).map_err(|e| {
                    FaucetError::State(format!(
                        "failed to parse state file {}: {e}",
                        path.display()
                    ))
                })?;
                Ok(Some(value))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(FaucetError::State(format!(
                "failed to read state file {}: {e}",
                path.display()
            ))),
        }
    }

    async fn put(&self, key: &str, value: &Value) -> Result<(), FaucetError> {
        validate_state_key(key)?;
        let _guard = self.write_lock.lock().await;
        self.ensure_root().await?;
        let bytes = serde_json::to_vec(value).map_err(|e| {
            FaucetError::State(format!("failed to serialize state for key '{key}': {e}"))
        })?;
        #[cfg(feature = "encryption")]
        let bytes = match &self.encryption {
            Some(enc) => enc.encrypt(&bytes),
            None => bytes,
        };
        let final_path = self.entry_path(key);
        let tmp_path = self.temp_path(key);

        // Write the temp file and fsync it BEFORE the rename. `fs::write`
        // alone leaves the bytes in the page cache: a crash after `put`
        // returns could surface a zero-length or stale file, breaking the
        // durability guarantee this store documents (#78/#8). `sync_all`
        // flushes the file's data and metadata to disk.
        {
            let mut file = tokio::fs::File::create(&tmp_path).await.map_err(|e| {
                FaucetError::State(format!(
                    "failed to create temp state file {}: {e}",
                    tmp_path.display()
                ))
            })?;
            file.write_all(&bytes).await.map_err(|e| {
                FaucetError::State(format!(
                    "failed to write temp state file {}: {e}",
                    tmp_path.display()
                ))
            })?;
            file.sync_all().await.map_err(|e| {
                FaucetError::State(format!(
                    "failed to fsync temp state file {}: {e}",
                    tmp_path.display()
                ))
            })?;
        }

        tokio::fs::rename(&tmp_path, &final_path)
            .await
            .map_err(|e| {
                FaucetError::State(format!(
                    "failed to commit state file {}: {e}",
                    final_path.display()
                ))
            })?;

        // fsync the parent directory so the rename itself is durable — an
        // atomic rename can still be lost on crash if the directory entry was
        // never flushed. Directory fsync is a POSIX concept; on platforms that
        // don't allow opening a directory as a file (e.g. Windows) it is
        // skipped.
        #[cfg(unix)]
        {
            let dir = tokio::fs::File::open(&self.root).await.map_err(|e| {
                FaucetError::State(format!(
                    "failed to open state dir {} for fsync: {e}",
                    self.root.display()
                ))
            })?;
            dir.sync_all().await.map_err(|e| {
                FaucetError::State(format!(
                    "failed to fsync state dir {}: {e}",
                    self.root.display()
                ))
            })?;
        }

        tracing::debug!(
            key,
            path = %final_path.display(),
            "state file written"
        );
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), FaucetError> {
        validate_state_key(key)?;
        let path = self.entry_path(key);
        match tokio::fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(FaucetError::State(format!(
                "failed to delete state file {}: {e}",
                path.display()
            ))),
        }
    }

    async fn check(
        &self,
        _ctx: &crate::check::CheckContext,
    ) -> Result<crate::check::CheckReport, FaucetError> {
        use crate::check::{CheckReport, Probe};
        // Exercise the real put → get → delete cycle on a sentinel key. `put`
        // creates the root dir if needed, so this validates "dir exists +
        // writable" via the actual code path and leaves no residue.
        let start = std::time::Instant::now();
        let probe = match self.sentinel_roundtrip().await {
            Ok(()) => Probe::pass("sentinel", start.elapsed()),
            Err(e) => Probe::fail_hint(
                "sentinel",
                start.elapsed(),
                e.to_string(),
                format!("ensure {} exists and is writable", self.root.display()),
            ),
        };
        Ok(CheckReport::single(probe))
    }
}

impl FileStateStore {
    /// Write, read back, and delete a sentinel key — the body of the `check()`
    /// probe, factored out so the happy path stays linear.
    async fn sentinel_roundtrip(&self) -> Result<(), FaucetError> {
        let probe = serde_json::json!({ "faucet_doctor": true });
        self.put(DOCTOR_SENTINEL_KEY, &probe).await?;
        let got = self.get(DOCTOR_SENTINEL_KEY).await?;
        // Best-effort cleanup regardless of the read result.
        let _ = self.delete(DOCTOR_SENTINEL_KEY).await;
        match got {
            Some(v) if v == probe => Ok(()),
            _ => Err(FaucetError::State(
                "sentinel readback did not match what was written".into(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::Arc;
    use tempfile::TempDir;

    // ── key validation ─────────────────────────────────────────────────────

    #[test]
    fn rejects_empty_key() {
        let err = validate_state_key("").unwrap_err();
        assert!(matches!(err, FaucetError::State(_)));
    }

    #[test]
    fn rejects_path_traversal_segments() {
        for k in [
            "../etc/passwd",
            "a/../b",
            "a/./b",
            "a//b",
            "/a",
            "a/",
            "a/.x",
            "a\\b",
            "..",
            ".",
        ] {
            assert!(validate_state_key(k).is_err(), "expected reject for {k:?}");
        }
        // A `/` that separates a hub owner from a name is fine (#682).
        assert!(validate_state_key("acme/netsuite::invoices").is_ok());
    }

    #[test]
    fn rejects_leading_dot() {
        assert!(validate_state_key(".hidden").is_err());
    }

    #[test]
    fn rejects_over_long_key() {
        let k = "a".repeat(257);
        assert!(validate_state_key(&k).is_err());
    }

    #[test]
    fn accepts_typical_keys() {
        for k in [
            "github_issues",
            "pipeline:rest:issues",
            "with.dot",
            "with-dash_and_underscore",
            "lower-Case_99",
        ] {
            validate_state_key(k).unwrap_or_else(|e| panic!("expected ok for {k:?}: {e}"));
        }
    }

    // ── MemoryStateStore ────────────────────────────────────────────────────

    #[tokio::test]
    async fn memory_get_returns_none_for_missing_key() {
        let s = MemoryStateStore::new();
        assert!(s.get("nope").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn memory_put_then_get_round_trips() {
        let s = MemoryStateStore::new();
        s.put("k", &json!({"cursor": "abc", "n": 7})).await.unwrap();
        let got = s.get("k").await.unwrap().unwrap();
        assert_eq!(got["cursor"], "abc");
        assert_eq!(got["n"], 7);
    }

    #[tokio::test]
    async fn memory_put_overwrites_previous_value() {
        let s = MemoryStateStore::new();
        s.put("k", &json!(1)).await.unwrap();
        s.put("k", &json!(2)).await.unwrap();
        assert_eq!(s.get("k").await.unwrap().unwrap(), json!(2));
    }

    #[tokio::test]
    async fn memory_delete_makes_get_return_none() {
        let s = MemoryStateStore::new();
        s.put("k", &json!("v")).await.unwrap();
        s.delete("k").await.unwrap();
        assert!(s.get("k").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn memory_delete_missing_key_is_ok() {
        let s = MemoryStateStore::new();
        s.delete("absent").await.unwrap();
    }

    #[tokio::test]
    async fn memory_rejects_invalid_keys() {
        let s = MemoryStateStore::new();
        assert!(s.get("a b").await.is_err());
        assert!(s.put("a b", &json!(1)).await.is_err());
        assert!(s.delete("a b").await.is_err());
    }

    // ── FileStateStore ──────────────────────────────────────────────────────

    #[tokio::test]
    async fn file_get_returns_none_for_missing_key() {
        let dir = TempDir::new().unwrap();
        let s = FileStateStore::new(dir.path());
        assert!(s.get("nope").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn file_put_creates_root_directory_lazily() {
        let dir = TempDir::new().unwrap();
        let root = dir.path().join("nested/state");
        let s = FileStateStore::new(&root);
        s.put("k", &json!("v")).await.unwrap();
        assert!(root.is_dir(), "root dir should be created on first put");
    }

    #[tokio::test]
    async fn file_put_then_get_round_trips() {
        let dir = TempDir::new().unwrap();
        let s = FileStateStore::new(dir.path());
        let value = json!({"cursor": "abc", "n": 42, "nested": {"flag": true}});
        s.put("github_issues", &value).await.unwrap();
        let got = s.get("github_issues").await.unwrap().unwrap();
        assert_eq!(got, value);
    }

    #[test]
    fn temp_path_is_unique_and_not_pid_derived() {
        // Regression for F50: the temp filename must not embed the process id.
        // Containers on a shared volume reuse PIDs (each starts at PID 1), so a
        // PID + per-process counter collides across processes and reintroduces
        // the torn-bookmark corruption the unique-temp scheme prevents. The
        // token is a per-process random UUID instead.
        let dir = TempDir::new().unwrap();
        let s = FileStateStore::new(dir.path());

        let a = s.temp_path("k");
        let b = s.temp_path("k");
        // Distinct temp paths within one process (the monotonic counter differs).
        assert_ne!(a, b);

        let name_a = a.file_name().unwrap().to_str().unwrap();
        let pid = std::process::id().to_string();
        // The PID must NOT appear as a dotted segment of the temp name.
        assert!(
            !name_a.split('.').any(|seg| seg == pid),
            "temp filename {name_a} must not embed the process id ({pid})"
        );
        assert!(name_a.ends_with(".json.tmp"));
    }

    #[test]
    fn safe_filename_percent_encodes_colon() {
        assert_eq!(
            safe_filename("pipeline:rest:issues"),
            "pipeline%3Arest%3Aissues"
        );
        assert_eq!(safe_filename("plain_key-1.v2"), "plain_key-1.v2");
        // Owner-scoped hub ids (`acme/netsuite::invoices`) must not become
        // nested directories.
        assert_eq!(
            safe_filename("acme/netsuite::invoices"),
            "acme%2Fnetsuite%3A%3Ainvoices"
        );
        assert!(validate_state_key("acme/netsuite::invoices").is_ok());
        assert!(validate_state_key("a b").is_err());
    }

    #[tokio::test]
    async fn file_round_trips_colon_keys_with_safe_filename() {
        // Regression for #78 LOW: the documented `pipeline:rest:issues` key
        // convention must round-trip and produce a Windows-legal filename
        // (no `:` on disk).
        let dir = TempDir::new().unwrap();
        let s = FileStateStore::new(dir.path());
        let value = json!({"cursor": "z"});
        s.put("pipeline:rest:issues", &value).await.unwrap();
        assert_eq!(s.get("pipeline:rest:issues").await.unwrap().unwrap(), value);
        // On-disk filename must not contain a colon.
        assert!(dir.path().join("pipeline%3Arest%3Aissues.json").exists());
        let mut has_colon = false;
        for entry in std::fs::read_dir(dir.path()).unwrap() {
            if entry.unwrap().file_name().to_string_lossy().contains(':') {
                has_colon = true;
            }
        }
        assert!(!has_colon, "no state filename may contain ':'");
    }

    /// True if any `.json.tmp` residue remains in `dir`. Temp names are unique
    /// per write since #146 H10, so we glob for the suffix rather than check a
    /// fixed name (which would never exist now and pass vacuously).
    fn has_tmp_residue(dir: &std::path::Path) -> bool {
        std::fs::read_dir(dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .any(|e| e.file_name().to_string_lossy().ends_with(".json.tmp"))
    }

    #[tokio::test]
    async fn file_put_overwrites_previous_value_atomically() {
        let dir = TempDir::new().unwrap();
        let s = FileStateStore::new(dir.path());
        s.put("k", &json!({"v": 1})).await.unwrap();
        s.put("k", &json!({"v": 2})).await.unwrap();
        assert_eq!(s.get("k").await.unwrap().unwrap(), json!({"v": 2}));
        // No temp file left behind.
        assert!(!has_tmp_residue(dir.path()), "no temp residue after put");
    }

    #[test]
    fn file_temp_paths_are_unique_per_write() {
        // H10 (audit #146): the temp path must be unique per write so two
        // writers (different processes, or two store instances in one process
        // with independent write_locks) never `File::create`-truncate a shared
        // temp that the other then renames over the final file (torn state).
        let dir = TempDir::new().unwrap();
        let s = FileStateStore::new(dir.path());
        let a = s.temp_path("k");
        let b = s.temp_path("k");
        assert_ne!(a, b, "each write must get a distinct temp path");
        // The committed (final) path stays stable across writes.
        assert_eq!(s.entry_path("k"), s.entry_path("k"));
    }

    #[tokio::test]
    async fn file_put_writes_complete_durable_file_with_no_temp_residue() {
        // Regression for #78/#8. `put` must produce a fully-written, parseable
        // file and leave no temp file behind. (The fsync that makes this
        // durable across a power loss can't be observed on a healthy
        // filesystem, but a regression in the write/rename path — truncation,
        // a leftover .tmp, or an unwritten file — is caught here.) A large
        // payload makes a partial/unflushed write detectable on read-back.
        let dir = TempDir::new().unwrap();
        let s = FileStateStore::new(dir.path());
        let big: Vec<Value> = (0..1_000)
            .map(|i| json!({"i": i, "s": "x".repeat(20)}))
            .collect();
        let value = json!({"cursor": "abc", "rows": big});

        s.put("github_issues", &value).await.unwrap();

        // Read the raw file directly (bypassing get) to confirm it is complete.
        let raw = tokio::fs::read(dir.path().join("github_issues.json"))
            .await
            .expect("state file must exist after put");
        assert!(!raw.is_empty(), "state file must not be zero-length");
        let parsed: Value = serde_json::from_slice(&raw).expect("state file must be valid JSON");
        assert_eq!(parsed, value);

        // No temp file left behind.
        assert!(!has_tmp_residue(dir.path()), "no temp residue after put");
    }

    #[tokio::test]
    async fn file_delete_removes_file() {
        let dir = TempDir::new().unwrap();
        let s = FileStateStore::new(dir.path());
        s.put("k", &json!("v")).await.unwrap();
        s.delete("k").await.unwrap();
        assert!(s.get("k").await.unwrap().is_none());
        assert!(!dir.path().join("k.json").exists());
    }

    #[tokio::test]
    async fn file_delete_missing_key_is_ok() {
        let dir = TempDir::new().unwrap();
        let s = FileStateStore::new(dir.path());
        s.delete("absent").await.unwrap();
    }

    #[tokio::test]
    async fn file_get_returns_error_for_corrupt_json() {
        let dir = TempDir::new().unwrap();
        let s = FileStateStore::new(dir.path());
        tokio::fs::create_dir_all(dir.path()).await.unwrap();
        tokio::fs::write(dir.path().join("bad.json"), b"not json")
            .await
            .unwrap();
        let err = s.get("bad").await.unwrap_err();
        match err {
            FaucetError::State(msg) => assert!(msg.contains("bad.json")),
            other => panic!("expected State error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn file_concurrent_puts_do_not_corrupt_or_leak_temp() {
        let dir = TempDir::new().unwrap();
        let s = Arc::new(FileStateStore::new(dir.path()));
        let mut handles = vec![];
        for i in 0..50 {
            let s = Arc::clone(&s);
            handles.push(tokio::spawn(async move {
                s.put("k", &json!({"i": i})).await.unwrap();
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        // The final value must be one of the 50 we wrote.
        let got = s.get("k").await.unwrap().unwrap();
        let i = got["i"].as_i64().unwrap();
        assert!((0..50).contains(&i));
        // And no temp file should remain.
        assert!(
            !has_tmp_residue(dir.path()),
            "no temp residue after concurrent puts"
        );
    }

    #[tokio::test]
    async fn file_store_works_through_trait_object() {
        let dir = TempDir::new().unwrap();
        let s: Box<dyn StateStore> = Box::new(FileStateStore::new(dir.path()));
        s.put("k", &json!(1)).await.unwrap();
        assert_eq!(s.get("k").await.unwrap().unwrap(), json!(1));
    }

    // ── check() ──────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn memory_check_passes() {
        let s = MemoryStateStore::new();
        let report = s
            .check(&crate::check::CheckContext::default())
            .await
            .unwrap();
        assert_eq!(report.failed_count(), 0);
        assert!(
            report
                .probes
                .iter()
                .all(|p| matches!(p.status, crate::check::ProbeStatus::Pass))
        );
    }

    #[tokio::test]
    async fn file_check_passes_for_writable_root() {
        let dir = TempDir::new().unwrap();
        let s = FileStateStore::new(dir.path());
        let report = s
            .check(&crate::check::CheckContext::default())
            .await
            .unwrap();
        assert_eq!(report.failed_count(), 0, "writable root should pass");
        // The sentinel probe must leave no residue.
        let leftovers: Vec<_> = std::fs::read_dir(dir.path()).unwrap().collect();
        assert!(leftovers.is_empty(), "check() must not leave files behind");
    }

    #[tokio::test]
    async fn file_store_root_returns_configured_directory() {
        let dir = TempDir::new().unwrap();
        let s = FileStateStore::new(dir.path());
        assert_eq!(s.root(), dir.path());
    }

    #[tokio::test]
    async fn default_check_reports_not_implemented() {
        // A StateStore that does not override `check()` falls back to the
        // trait default, which yields a single not-implemented (skipped) probe.
        struct BareStore;
        #[async_trait]
        impl StateStore for BareStore {
            async fn get(&self, _key: &str) -> Result<Option<Value>, FaucetError> {
                Ok(None)
            }
            async fn put(&self, _key: &str, _value: &Value) -> Result<(), FaucetError> {
                Ok(())
            }
            async fn delete(&self, _key: &str) -> Result<(), FaucetError> {
                Ok(())
            }
        }
        let s = BareStore;
        let report = s
            .check(&crate::check::CheckContext::default())
            .await
            .unwrap();
        // not_implemented() reports no failures and is not a pass.
        assert_eq!(report.failed_count(), 0);
        assert!(
            report
                .probes
                .iter()
                .any(|p| matches!(p.status, crate::check::ProbeStatus::Skip { .. })),
            "default check must surface a skipped (not-implemented) probe"
        );
    }

    #[tokio::test]
    async fn file_check_fails_when_root_unusable() {
        // Root whose parent is a regular file → create_dir_all fails.
        let dir = TempDir::new().unwrap();
        let file = dir.path().join("not_a_dir");
        std::fs::write(&file, b"x").unwrap();
        let s = FileStateStore::new(file.join("state"));
        let report = s
            .check(&crate::check::CheckContext::default())
            .await
            .unwrap();
        assert_eq!(report.failed_count(), 1, "unusable root should fail");
    }

    #[cfg(feature = "encryption")]
    mod encryption_at_rest {
        use super::*;
        use crate::encryption::{CompiledEncryption, EncryptionSpec, is_encrypted};
        use serde_json::json;

        fn enc(key: &str) -> CompiledEncryption {
            CompiledEncryption::compile(&EncryptionSpec {
                key: key.into(),
                previous_keys: vec![],
                algorithm: Default::default(),
            })
            .unwrap()
        }

        #[tokio::test]
        async fn encrypted_round_trip_and_ciphertext_on_disk() {
            let dir = tempfile::tempdir().unwrap();
            let store = FileStateStore::new(dir.path()).with_encryption(enc("k1"));
            store.put("bk", &json!({"lsn": 42})).await.unwrap();
            assert_eq!(store.get("bk").await.unwrap(), Some(json!({"lsn": 42})));

            // On disk it is ciphertext, not JSON.
            let raw = std::fs::read(dir.path().join("bk.json")).unwrap();
            assert!(is_encrypted(&raw));
            assert!(serde_json::from_slice::<Value>(&raw).is_err());

            store.delete("bk").await.unwrap();
            assert_eq!(store.get("bk").await.unwrap(), None);
        }

        #[tokio::test]
        async fn plaintext_file_stays_readable_and_is_sealed_on_next_write() {
            let dir = tempfile::tempdir().unwrap();
            // A pre-encryption bookmark written by an older config.
            let plain = FileStateStore::new(dir.path());
            plain.put("bk", &json!("legacy")).await.unwrap();
            let before = std::fs::read(dir.path().join("bk.json")).unwrap();
            assert!(!is_encrypted(&before));

            let sealed = FileStateStore::new(dir.path()).with_encryption(enc("k1"));
            assert_eq!(sealed.get("bk").await.unwrap(), Some(json!("legacy")));
            sealed.put("bk", &json!("updated")).await.unwrap();
            let after = std::fs::read(dir.path().join("bk.json")).unwrap();
            assert!(is_encrypted(&after), "next write must seal the file");
            assert_eq!(sealed.get("bk").await.unwrap(), Some(json!("updated")));
        }

        #[tokio::test]
        async fn wrong_key_is_a_typed_error_not_a_missing_bookmark() {
            let dir = tempfile::tempdir().unwrap();
            let a = FileStateStore::new(dir.path()).with_encryption(enc("right"));
            a.put("bk", &json!(1)).await.unwrap();

            let b = FileStateStore::new(dir.path()).with_encryption(enc("wrong"));
            let err = b.get("bk").await.unwrap_err();
            assert!(matches!(err, FaucetError::State(_)));
            assert!(err.to_string().contains("could not be decrypted"), "{err}");
        }

        #[tokio::test]
        async fn encrypted_file_with_unconfigured_store_is_a_typed_error() {
            let dir = tempfile::tempdir().unwrap();
            let sealed = FileStateStore::new(dir.path()).with_encryption(enc("k1"));
            sealed.put("bk", &json!(1)).await.unwrap();

            let plain = FileStateStore::new(dir.path());
            let err = plain.get("bk").await.unwrap_err();
            assert!(err.to_string().contains("no `encryption` block"), "{err}");
        }

        #[tokio::test]
        async fn rotation_reads_old_key_files() {
            let dir = tempfile::tempdir().unwrap();
            let old = FileStateStore::new(dir.path()).with_encryption(enc("old"));
            old.put("bk", &json!("v1")).await.unwrap();

            let rotated = FileStateStore::new(dir.path()).with_encryption(
                CompiledEncryption::compile(&EncryptionSpec {
                    key: "new".into(),
                    previous_keys: vec!["old".into()],
                    algorithm: Default::default(),
                })
                .unwrap(),
            );
            assert_eq!(rotated.get("bk").await.unwrap(), Some(json!("v1")));
            // Re-seal under the new key; a store knowing only "new" can read it.
            rotated.put("bk", &json!("v2")).await.unwrap();
            let new_only = FileStateStore::new(dir.path()).with_encryption(enc("new"));
            assert_eq!(new_only.get("bk").await.unwrap(), Some(json!("v2")));
        }

        #[tokio::test]
        async fn no_temp_files_left_behind() {
            let dir = tempfile::tempdir().unwrap();
            let store = FileStateStore::new(dir.path()).with_encryption(enc("k1"));
            store.put("bk", &json!(1)).await.unwrap();
            let leftovers: Vec<_> = std::fs::read_dir(dir.path())
                .unwrap()
                .filter_map(Result::ok)
                .filter(|e| e.path().to_string_lossy().ends_with(".tmp"))
                .collect();
            assert!(
                leftovers.is_empty(),
                "atomic write must leave no temp files"
            );
        }
    }
}
