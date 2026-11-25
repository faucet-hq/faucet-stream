//! `faucet schedule` — built-in cron scheduler.
//!
//! The runtime loop lives in `crate::commands::schedule`; this module holds
//! the pure, unit-tested pieces it drives: the config types ([`spec`]),
//! the validated/compiled schedule (compiled — added in Task 3), the decision
//! state machine (state — added in Task 4), and the metric emitters
//! (metrics — added in Task 5).

pub mod compiled;
pub mod metrics;
pub mod spec;
pub mod state;

#[cfg(test)]
mod dst_oracle;
#[cfg(test)]
mod golden_ticks;
