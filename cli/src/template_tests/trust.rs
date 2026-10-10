//! Where and how a template's tests run (#856): the server's trust boundary
//! for test runs it performs, and the limits every run is held to.
//!
//! `faucet serve` scopes a [`ServerTestPolicy`] around every request and
//! background task it runs ([`with_server_policy`]). Inside that scope every
//! document a test run executes is screened for the server's own credentials,
//! and each result records the instance that produced it. The documents are
//! registered template versions, which the server already trusts to run, so
//! subprocess connectors are not refused here. The CLI, which already holds
//! direct store access, runs without a policy.

use std::time::Duration;

use serde_json::Value;

use crate::error::{CliError, CliResult};

/// The trust settings of the `faucet serve` process running tests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerTestPolicy {
    /// This server instance, recorded on every result it produces.
    pub instance: String,
}

tokio::task_local! {
    static POLICY: ServerTestPolicy;
}

/// Run `fut` as work of the server `policy` belongs to.
pub async fn with_server_policy<F: std::future::Future>(
    policy: ServerTestPolicy,
    fut: F,
) -> F::Output {
    POLICY.scope(policy, fut).await
}

/// The policy of the server this task works for, if any.
pub fn server_policy() -> Option<ServerTestPolicy> {
    POLICY.try_with(Clone::clone).ok()
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

/// The limits in force.
pub fn limits() -> TestLimits {
    DEFAULT_LIMITS
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
pub fn screen_with(_policy: &ServerTestPolicy, doc: &Value) -> CliResult<()> {
    #[cfg(feature = "serve")]
    crate::serve::load::refuse_server_secrets(doc)
        .map_err(|e| CliError::Config(format!("refused by the server: {e}")))?;
    #[cfg(not(feature = "serve"))]
    let _ = doc;
    Ok(())
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
    }

    #[cfg(feature = "serve")]
    #[tokio::test]
    async fn a_server_refuses_its_own_secrets_but_runs_its_trusted_templates() {
        let policy = ServerTestPolicy {
            instance: "i-1".into(),
        };
        let secret = serde_json::json!({"x": "${env:FAUCET_VAULT_KEY}"});
        let singer = serde_json::json!({"pipeline": {"source": {"type": "singer"}}});
        with_server_policy(policy.clone(), async {
            assert_eq!(server_policy(), Some(policy.clone()));
            let e = screen(&secret).unwrap_err().to_string();
            assert!(e.contains("refused by the server"), "{e}");
            screen(&singer).unwrap();
        })
        .await;
    }

    #[tokio::test]
    async fn the_policy_is_scoped_to_its_server_task() {
        assert_eq!(server_policy(), None);
        screen(&serde_json::json!({"x": "${env:FAUCET_VAULT_KEY}"})).unwrap();
        let a = ServerTestPolicy {
            instance: "a".into(),
        };
        let b = ServerTestPolicy {
            instance: "b".into(),
        };
        let (ra, rb) = tokio::join!(
            with_server_policy(a.clone(), async { server_policy() }),
            with_server_policy(b.clone(), async { server_policy() }),
        );
        assert_eq!((ra, rb), (Some(a), Some(b)));
        assert_eq!(server_policy(), None);
    }
}
