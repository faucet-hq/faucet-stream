//! Unified resilience policy: retry, backoff classification, circuit breaker,
//! and poison-pill row handling. See
//! `docs/superpowers/specs/2026-06-17-resilience-policy-design.md`.

mod breaker;
mod classify;
mod execute;
mod policy;
pub mod response_matcher;

pub use breaker::CircuitBreaker;
pub use classify::{RetryClass, RetryClassSet, classify};
pub use execute::{
    RetryMetrics, execute_with_policy, execute_with_policy_metered, execute_with_policy_recorded,
};
pub use policy::{
    BackoffKind, CircuitBreakerConfig, PoisonAction, PoisonPolicy, ResiliencePolicy, RetryPolicy,
};
pub use response_matcher::{
    BackoffFrom, DEFAULT_MAX_WAIT_SECS, RetryMatcher, WaitUnit, find_match,
};
