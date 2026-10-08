//! Shared preflight-probe helper for the CSV file sink (`faucet doctor`).

use std::path::{Path, PathBuf};
use std::time::Instant;

use faucet_core::check::Probe;

/// Probe whether the parent directory of `path` exists and is writable.
///
/// Idempotent: creates a uniquely-named temp file in the parent directory and
/// removes it immediately. Never touches the configured output file itself.
///
/// The filesystem work runs on the synchronous `std::fs` API so the probe does
/// not depend on tokio's `fs` feature being enabled in this crate's non-dev
/// build (the CSV sink already does all its I/O via `spawn_blocking`).
///
/// - Parent exists and a temp file can be created + removed → [`Probe::pass`].
/// - Parent directory is missing → the nearest existing ancestor is probed
///   instead, since the sink creates the missing directories.
/// - That ancestor is not a directory → [`Probe::fail_hint`] naming it.
/// - Parent exists but the temp file cannot be created → [`Probe::fail_hint`]
///   surfacing the I/O error (e.g. permission denied, read-only filesystem).
pub fn probe_parent_writable(path: &str, start: Instant) -> Probe {
    // A path with no parent component (e.g. a bare filename) targets the
    // current working directory.
    let parent = match Path::new(path).parent() {
        Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
        _ => PathBuf::from("."),
    };

    // The sink creates missing parent directories, so a missing parent passes
    // when its nearest existing ancestor is a writable directory.
    let mut parent = parent;
    while !parent.exists() {
        match parent.parent() {
            Some(up) if !up.as_os_str().is_empty() => parent = up.to_path_buf(),
            _ => {
                parent = PathBuf::from(".");
                break;
            }
        }
    }
    if !parent.is_dir() {
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
    match std::fs::File::create(&probe_path) {
        Ok(_) => {
            // Best-effort cleanup; a leftover empty probe file is harmless but
            // we remove it to keep the directory clean.
            let _ = std::fs::remove_file(&probe_path);
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
