//! Retry logic with exponential backoff.

pub mod backoff;
pub mod matcher;

pub use backoff::{execute_with_retry, execute_with_retry_recorded};
pub use matcher::RetryMatcher;
