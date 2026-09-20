//! Parameter-combination test suites for pipeline templates (#648).
//!
//! A template fans out across a parameter space, and a change to one version
//! can silently break one corner of it. This turns "did I break a param
//! combination?" from a production incident into a red/green check.
//!
//! - [`spec`] — the suite grammar (`faucet schema template-test`).
//! - [`combine`] — pure case generation: explicit, cartesian/all-pairs, and
//!   cases derived from the template's own `params:`.
//! - [`runner`] — materialize per combination, then validate or run fixtures.
//! - [`report`] — human checklist + `--json`.

pub mod combine;
pub mod report;
pub mod runner;
pub mod spec;

pub use runner::{CaseOutcome, SuiteOutcome, Target, resolve_target_version, run};
pub use spec::SuiteFile;
