//! Template tests: parameter-combination suites (#648) and the test bundle a
//! template version carries (#856).
//!
//! A template fans out across a parameter space, and a change to one version
//! can silently break one corner of it. This turns "did I break a param
//! combination?" from a production incident into a red/green check — and, with
//! a bundle, into the gate a version must pass before it is launched.
//!
//! - [`spec`] — the suite grammar (`faucet schema template-test`).
//! - [`bundle`] — the `tests:` block and `kind: test-suite` documents.
//! - [`combine`] — pure case generation: explicit, cartesian/all-pairs, and
//!   cases derived from the template's own `params:`.
//! - [`runner`] — materialize per combination, then validate or run fixtures.
//! - [`run`] — a whole bundle, shared suites included.
//! - [`result`] / [`gate`] — what a run records and what a launch requires.
//! - [`report`] — human checklist + `--json`.

pub mod bundle;
pub mod combine;
pub mod gate;
pub mod report;
pub mod result;
pub mod run;
pub mod runner;
pub mod selector;
pub mod spec;

pub use bundle::{Fixture, SuiteRequirement, TestBundle, TestSuiteTemplate};
pub use gate::{GateStatus, GateVerdict, LaunchGate};
pub use result::{BundleOutcome, TemplateTestResult};
pub use runner::{CaseOutcome, SuiteOutcome, Target, run};
#[cfg(feature = "templates")]
pub use runner::resolve_target_version;
pub use spec::SuiteFile;
