//! Where and how a template's tests run (#856): the server's trust boundary
//! for test runs it performs, and the limits every run is held to.
//!
//! `faucet serve` installs a [`ServerTestPolicy`] at startup. While one is
//! installed, every document a test run executes is screened exactly like a
//! submitted config (the server's own credentials and, unless allowed,
//! subprocess connectors are refused), and each result records the instance
//! that produced it. The CLI, which already holds direct store access, runs
//! without one.

use std::sync::RwLock;
use std::time::Duration;

use serde_json::Value;

use crate::error::{CliError, CliResult};

/// The trust settings of the `faucet serve` process running tests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerTestPolicy {
    /// This server instance, recorded on every result it produces.
    pub instance: String,
    /// `--allow-subprocess-connectors`.
    pub allow_subprocess: bool,
}

static POLICY: RwLock<Option<ServerTestPolicy>> = RwLock::new(None);

/// Install the policy for this process (called by `faucet serve`).
pub fn install_server_policy(policy: ServerTestPolicy) {
    if let Ok(mut p) = POLICY.write() {
        *p = Some(policy);
    }
}

/// The installed policy, if this process is a server.
pub fn server_policy() -> Option<ServerTestPolicy> {
    POLICY.read().ok().and_then(|p| p.clone())
}

/// The limits a test run is held to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TestLimits {
    /// Wall-clock limit for one case, retries included.
    pub case_timeout: Duration,
    /// Wall-clock limit for a whole bundle run.
    pub run_budget: Duration,
    /// Largest `tests:` block (serialized) a run accepts.
    pub max_bundle_bytes: usize,
}

/// The defaults: 60 s per case, 10 minutes per run, 1 MiB of bundle.
pub const DEFAULT_LIMITS: TestLimits = TestLimits {
    case_timeout: Duration::from_secs(60),
    run_budget: Duration::from_secs(600),
    max_bundle_bytes: 1024 * 1024,
};

static LIMITS: RwLock<TestLimits> = RwLock::new(DEFAULT_LIMITS);

/// The limits in force.
pub fn limits() -> TestLimits {
    LIMITS.read().map(|l| *l).unwrap_or(DEFAULT_LIMITS)
}

/// Replace the limits for this process.
pub fn set_limits(limits: TestLimits) {
    if let Ok(mut l) = LIMITS.write() {
        *l = limits;
    }
}

/// The clock every case runs at unless it pins its own, so a test gives the
/// same answer whenever it runs.
pub const PINNED_CLOCK: &str = "2000-01-01T00:00:00Z";

/// The pinned clock as a timestamp.
pub fn pinned_clock() -> chrono::DateTime<chrono::FixedOffset> {
    chrono::DateTime::parse_from_rfc3339(PINNED_CLOCK).expect("valid constant")
}

/// Screen a document a server-side test run is about to execute. A no-op
/// outside `faucet serve`.
pub fn screen(doc: &Value) -> CliResult<()> {
    match server_policy() {
        Some(policy) => screen_with(&policy, doc),
        None => Ok(()),
    }
}

/// [`screen`] under an explicit policy.
pub fn screen_with(policy: &ServerTestPolicy, doc: &Value) -> CliResult<()> {
    #[cfg(feature = "serve")]
    {
        crate::serve::load::refuse_server_secrets(doc)
            .map_err(|e| CliError::Config(format!("refused by the server: {e}")))?;
        if !policy.allow_subprocess {
            let kinds = crate::serve::load::subprocess_kinds_in(doc);
            if !kinds.is_empty() {
                return Err(CliError::Config(format!(
                    "refused by the server: connector type(s) {} run a program on the server, \
                     and this server was not started with --allow-subprocess-connectors",
                    kinds.into_iter().collect::<Vec<_>>().join(", ")
                )));
            }
        }
    }
    #[cfg(not(feature = "serve"))]
    let _ = (doc, policy);
    Ok(())
}

/// The message for a case that would need a secret param.
pub fn secret_param_refused(name: &str) -> CliError {
    CliError::Config(format!(
        "param `{name}` is secret, and secret params are never supplied to tests — test the \
         template with that param left to its default, or give it a non-secret default"
    ))
}

/// The connectors and transforms compiled into this binary, sorted.
pub fn build_features() -> Vec<String> {
    let mut out: Vec<String> = crate::registry::builtin_source_descriptions()
        .into_iter()
        .map(|(n, _)| format!("source:{n}"))
        .chain(
            crate::registry::builtin_sink_descriptions()
                .into_iter()
                .map(|(n, _)| format!("sink:{n}")),
        )
        .chain(
            crate::transforms::transform_descriptions()
                .into_iter()
                .map(|(n, _)| format!("transform:{n}")),
        )
        .collect();
    out.sort();
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_defaults_and_the_pinned_clock() {
        assert_eq!(DEFAULT_LIMITS.max_bundle_bytes, 1 << 20);
        assert_eq!(pinned_clock().to_rfc3339(), "2000-01-01T00:00:00+00:00");
        let f = build_features();
        assert!(f.iter().any(|x| x.starts_with("source:")), "{f:?}");
        assert!(f.windows(2).all(|w| w[0] <= w[1]));
        assert!(
            secret_param_refused("token")
                .to_string()
                .contains("never supplied to tests")
        );
    }

    #[test]
    fn a_server_refuses_its_own_secrets_and_unallowed_subprocesses() {
        let policy = ServerTestPolicy {
            instance: "i-1".into(),
            allow_subprocess: false,
        };
        let secret = serde_json::json!({"x": "${env:FAUCET_VAULT_KEY}"});
        let e = screen_with(&policy, &secret).unwrap_err().to_string();
        assert!(e.contains("refused by the server"), "{e}");
        let singer = serde_json::json!({"pipeline": {"source": {"type": "singer"}}});
        let e = screen_with(&policy, &singer).unwrap_err().to_string();
        assert!(e.contains("--allow-subprocess-connectors"), "{e}");
        let allowed = ServerTestPolicy {
            allow_subprocess: true,
            ..policy
        };
        screen_with(&allowed, &singer).unwrap();
        screen_with(&allowed, &serde_json::json!({"x": "plain"})).unwrap();
    }

    #[test]
    fn outside_a_server_nothing_is_screened() {
        if server_policy().is_none() {
            screen(&serde_json::json!({"x": "${env:FAUCET_VAULT_KEY}"})).unwrap();
        }
    }
}
