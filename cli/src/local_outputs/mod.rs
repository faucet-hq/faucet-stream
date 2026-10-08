//! Retention GC for local sink outputs (#587).
//!
//! Local runs write real files — `out.jsonl`, `rows.csv`, a directory of rolled
//! parquet parts. Repeated local iteration, or a long-running `faucet serve`
//! used for local testing, accumulates them until someone remembers to `rm`.
//! This module bounds that footprint: faucet records every local file its sinks
//! open, then deletes the ones past a retention window (**7 days** by default),
//! plus on-demand *immediate* (one output), *bulk* (older than N days), and
//! *clean-all* sweeps.
//!
//! ## The guardrail is the whole design
//!
//! **The sweeper may only delete files faucet recorded as its own local sink
//! outputs — never a glob, never a directory wipe, not even for "clean all".**
//! Everything here follows from that:
//!
//! - Paths come from the sink that opened them
//!   ([`Sink::local_outputs`](faucet_core::Sink::local_outputs)) and are stored in
//!   the ledger. A sweep iterates ledger rows and calls `remove_file` on each —
//!   there is no code path in this module that expands a pattern, walks a
//!   directory, or removes one.
//! - A file faucet *wrote* but did not *create* (`append:` onto an existing
//!   export, a mistyped path landing on real data) carries
//!   [`pre_existing`](faucet_core::LocalOutput::pre_existing) and is **never**
//!   deleted — not by the sweeper, not by an explicit single-path request. See
//!   [`SkipReason::PreExisting`].
//! - A parquet run in rollover mode records each UUID-named part as its own row,
//!   so "delete this dataset's outputs" is still a list of individual files.
//!
//! ## What it does *not* touch
//!
//! Run-history rows, catalog entries, and lineage are the durable record; this
//! GC removes **data files only**. A swept run keeps its history row, and its
//! ledger row survives too — marked `deleted_at`, which is what renders the
//! output as **expired** in the console rather than as a broken link. That
//! asymmetry is deliberate: *data artifacts are disposable, the record of what
//! ran is not*.
//!
//! ## Layout
//!
//! - [`ledger`] — the stored record ([`LocalOutputRecord`]) + the filter/report
//!   types the storage backends and HTTP handlers share.
//! - [`spec`] — the top-level `local_outputs:` config block.
//! - [`sweep`] — the engine: pure selection ([`sweep::select`]) separated from
//!   the I/O that acts on it ([`sweep::run`]).
//! - [`record`](mod@record) — the write path the executor calls after an invocation.
//! - [`metrics`] — the `faucet_local_outputs_*` counters.
//!
//! ## Feature gating
//!
//! Compiled with `serve`, not `catalog`: the ledger's storage methods are part of
//! the [`RunHistory`](crate::serve::history::RunHistory) surface, so a
//! `serve`-only build has to compile them (they are inert defaults there). The
//! *surfaces* need more — the executor's write path and `faucet cleanup` need a
//! `catalog:` store to record into, and the console's Datasets page is a catalog
//! view — so those stay behind `catalog`.

pub mod ledger;
pub mod metrics;
pub mod record;
pub mod spec;
pub mod sweep;

pub use ledger::{
    LocalOutputFilter, LocalOutputObservation, LocalOutputRecord, LocalOutputState, SkipReason,
    SweepOutcome, SweepReport, SweepScope,
};
pub use record::{RecordContext, record};
pub use spec::LocalOutputsSpec;
pub use sweep::{SweepOptions, select};

/// Default retention window for local sink outputs, in days.
///
/// Seven days matches `--retain-terminal-runs-secs`, so a run record and the
/// files it produced age out on the same clock by default.
pub const DEFAULT_RETENTION_DAYS: u32 = 7;

/// This machine's host name, stamped on every ledger row so a sweeper never
/// deletes a path another host wrote (#789 CLI-45). `None` when the platform
/// cannot say.
pub fn local_host() -> Option<String> {
    static HOST: std::sync::OnceLock<Option<String>> = std::sync::OnceLock::new();
    HOST.get_or_init(read_host).clone()
}

#[cfg(unix)]
fn read_host() -> Option<String> {
    let mut buf = [0u8; 256];
    // SAFETY: `gethostname` writes at most `buf.len()` bytes into a buffer we own.
    let rc = unsafe { libc::gethostname(buf.as_mut_ptr().cast(), buf.len()) };
    if rc != 0 {
        return None;
    }
    let end = buf.iter().position(|b| *b == 0).unwrap_or(buf.len());
    let name = String::from_utf8_lossy(&buf[..end]).trim().to_string();
    (!name.is_empty()).then_some(name)
}

#[cfg(not(unix))]
fn read_host() -> Option<String> {
    std::env::var("COMPUTERNAME").ok().filter(|h| !h.is_empty())
}
