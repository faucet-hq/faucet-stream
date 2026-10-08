//! Shared preflight-probe helper for the JSONL file sink (`faucet doctor`).

use std::path::Path;
use std::time::Instant;

use faucet_core::check::Probe;

/// Probe whether the parent directory of `path` exists and is writable.
///
/// Idempotent: creates a uniquely-named temp file in the parent directory and
/// removes it immediately. Never touches the configured output file itself.
///
/// - Parent exists and a temp file can be created + removed → [`Probe::pass`].
/// - Parent directory is missing → the nearest existing ancestor is probed
///   instead, since the sink creates the missing directories.
/// - That ancestor is not a directory → [`Probe::fail_hint`] naming it.
/// - Parent exists but the temp file cannot be created → [`Probe::fail_hint`]
///   surfacing the I/O error (e.g. permission denied, read-only filesystem).
pub async fn probe_parent_writable(path: &Path, start: Instant) -> Probe {
    // A path with no parent component (e.g. a bare filename) targets the
    // current working directory.
    let parent = match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
        _ => std::path::PathBuf::from("."),
    };

    // The sink creates missing parent directories, so a missing parent passes
    // when its nearest existing ancestor is a writable directory.
    let mut parent = parent;
    while !tokio::fs::try_exists(&parent).await.unwrap_or(false) {
        match parent.parent() {
            Some(up) if !up.as_os_str().is_empty() => parent = up.to_path_buf(),
            _ => {
                parent = std::path::PathBuf::from(".");
                break;
            }
        }
    }
    if !tokio::fs::metadata(&parent).await.is_ok_and(|m| m.is_dir()) {
        return Probe::fail_hint(
            "io",
            start.elapsed(),
            format!("{} is not a directory", parent.display()),
            format!(
                "point the sink at a path under a directory, not under the file {}",
                parent.display()
            ),
        );
    }

    // Unique temp file name so concurrent probes don't collide.
    let probe_path = parent.join(format!(".faucet_doctor_probe-{}", uuid_like()));
    match tokio::fs::write(&probe_path, b"").await {
        Ok(()) => {
            // Best-effort cleanup; a leftover empty probe file is harmless but
            // we remove it to keep the directory clean.
            let _ = tokio::fs::remove_file(&probe_path).await;
            Probe::pass("io", start.elapsed())
        }
        Err(e) => Probe::fail_hint(
            "io",
            start.elapsed(),
            format!("cannot write to directory {}: {e}", parent.display()),
            "ensure the directory is writable by the current user",
        ),
    }
}

/// Cheap, dependency-free unique token for the temp-file name. Combines the
/// process id, a monotonic counter, and the current nanosecond timestamp so
/// concurrent probes within and across processes don't collide.
fn uuid_like() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{}-{}-{}", std::process::id(), n, nanos)
}
