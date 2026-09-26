//! Data-flow policies (#702): organisation-wide rules about where a labelled
//! column may land, declared once and enforced on every pipeline.
//!
//! - [`spec`] — the `policy:` grammar: [`Classification`]s tag columns with
//!   labels (by name, by name pattern, or by value detector), [`PolicyRule`]s
//!   say what a label requires of the sink it lands in (`require` sink
//!   attributes, `mask` actions that satisfy the rule, or `deny`).
//! - [`compile`] — [`CompiledPolicy`]: regexes built once; labels resolvable
//!   by column name and by value (the masking detectors, reused).
//! - [`evaluate()`] — the pure function: sink facts × labelled columns →
//!   [`Violation`]s. The CLI's static pass and the runtime backstop both call
//!   it, so a config-time verdict and a run-time verdict can never disagree.
//! - [`sink`] — [`PolicySink`], the runtime backstop: a sink decorator that
//!   classifies every record about to be written and fails the run or
//!   quarantines the record per the rule's `on_runtime`.
//!
//! Sink attributes (`residency`, `environment`, `region`, …) are declared on
//! the sink template in the CLI config; label propagation through transforms
//! (column lineage) is the CLI's job too — this module is the pure core.

pub mod compile;
pub mod evaluate;
pub mod sink;
pub mod spec;

pub use compile::{CompiledClassification, CompiledPolicy};
pub use evaluate::{ColumnFacts, SinkFacts, Violation, ViolationKind, evaluate, rule_applies};
pub use sink::{PolicyScope, PolicySink, classify_record};
pub use spec::{Classification, MASK_ACTIONS, PolicyRule, PolicySpec, RuleWhen, RuntimeAction};
