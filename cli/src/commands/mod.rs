//! Subcommand implementations. Each module owns its `run(...)` entry point
//! and stays free of clap so the integration tests can drive it directly.

pub mod backfill;
#[cfg(feature = "catalog")]
pub mod catalog;
#[cfg(feature = "catalog")]
pub mod cleanup;
pub mod completions;
pub mod conformance;
#[cfg(feature = "contract")]
pub mod contract;
#[cfg(feature = "cli-dev")]
pub mod dev;
pub mod discover;
pub mod dlq;
pub mod doctor;
pub mod explain;
pub mod fmt;
#[cfg(feature = "catalog")]
pub mod history;
pub mod hub;
pub mod init;
pub mod install;
pub mod list;
#[cfg(feature = "masking")]
pub mod masking;
#[cfg(feature = "mcp")]
pub mod mcp;
pub mod migrate;
pub mod new;
#[cfg(feature = "notify")]
pub mod notify;
pub mod plan;
pub mod preview;
pub mod profiling;
pub mod replicate;
pub mod rollback;
pub mod run;
#[cfg(feature = "schedule")]
pub mod schedule;
pub mod schema;
pub mod search;
#[cfg(feature = "serve")]
pub mod serve;
#[cfg(feature = "templates")]
pub mod template;
pub mod test;
pub mod validate;
pub mod verify;

use crate::error::CliError;

/// Render any [`CliError`] to a single line on stderr, scrubbing any resolved
/// secret values that may have reached the error chain.
pub fn report(err: &CliError) {
    use crate::secrets::registry::redact;
    eprintln!("error: {}", redact(&err.to_string()));
    let mut src = std::error::Error::source(err);
    while let Some(s) = src {
        eprintln!("  caused by: {}", redact(&s.to_string()));
        src = std::error::Error::source(s);
    }
}
