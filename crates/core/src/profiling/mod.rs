//! Learned column profiles with drift detection (#708).
//!
//! A run's written records are folded into one [`RunProfile`] by a
//! [`Profiler`] — per top-level column: null rate, type mix, distinct-count
//! estimate, numeric / string summaries and the most frequent values, all in
//! bounded memory (see [`sketch`]). [`detect_drift`] then compares the
//! profile against a rolling baseline of earlier runs and reports
//! statistically significant changes per column, with no thresholds to
//! write. [`ProfilingSink`] is the decorator that feeds a pipeline's sink
//! writes into the profiler.
//!
//! The baseline store, the post-run evaluation, the catalog surfaces and the
//! `faucet profiling` verbs live in the CLI; this module is the pure core.
//!
//! Vocabulary: *profiling* is this feature. A config *profile* (`profiles:`,
//! `--profile`) is an unrelated config overlay.

pub mod column;
pub mod drift;
pub mod sink;
pub mod sketch;
pub mod spec;

pub use column::{
    ColumnProfile, NumericSummary, Profiler, RunProfile, StringSummary, TopValue, TypeCounts,
};
pub use drift::{DriftMetric, ProfileDrift, detect_drift, population_stability_index};
pub use sink::ProfilingSink;
pub use spec::{OnProfileDrift, ProfilingSpec};
