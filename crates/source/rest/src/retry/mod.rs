//! Retry logic with exponential backoff.

pub mod backoff;

pub use backoff::{execute_with_retry, execute_with_retry_recorded};
