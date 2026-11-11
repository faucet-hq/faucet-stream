//! Whether the running task may authenticate with the identity of the host
//! faucet runs on: its environment, instance or workload role, and local
//! credential files. Re-exported from [`crate::resolve`].
//!
//! The answer follows the task's scope: [`with_host_credentials`], or the
//! reference policy a host scoped with
//! [`crate::http::with_reference_policy`] (which carries
//! [`crate::resolve::ReferencePolicy::allows_host_credentials`]). A task
//! with no scope is allowed, unless the process called
//! [`deny_unscoped_host_credentials`] — what a host that runs configs it did
//! not write does at startup, so an operation it forgot to scope fails
//! closed.

use crate::error::FaucetError;
use std::sync::OnceLock;

tokio::task_local! {
    static SCOPED: bool;
}

fn unscoped_denied() -> &'static OnceLock<()> {
    static DENIED: OnceLock<()> = OnceLock::new();
    &DENIED
}

/// Refuse the host's identity to every task without a scope, for the rest of
/// the process. The **first** call wins; later calls return `false`.
pub fn deny_unscoped_host_credentials() -> bool {
    unscoped_denied().set(()).is_ok()
}

/// Whether [`deny_unscoped_host_credentials`] was called.
pub fn unscoped_host_credentials_denied() -> bool {
    unscoped_denied().get().is_some()
}

/// Run `fut` with the host's identity allowed or refused, whatever scope
/// encloses it.
pub async fn with_host_credentials<F: std::future::Future>(allowed: bool, fut: F) -> F::Output {
    SCOPED.scope(allowed, fut).await
}

/// The grant scoped over the current task, if any.
pub fn scoped_host_credentials() -> Option<bool> {
    SCOPED.try_with(|allowed| *allowed).ok()
}

/// Whether the current task may authenticate with the host's own identity:
/// its scope's answer, else allowed unless
/// [`deny_unscoped_host_credentials`] was called.
pub fn host_credentials_allowed() -> bool {
    scoped_host_credentials().unwrap_or_else(|| !unscoped_host_credentials_denied())
}

/// Refuse, before a client is built, a `connector` whose `field` would make
/// it authenticate with the host's own identity when the current task may
/// not ([`host_credentials_allowed`]). The error names the field, never a
/// value.
pub fn require_host_credentials(connector: &str, field: &str) -> Result<(), FaucetError> {
    if host_credentials_allowed() {
        return Ok(());
    }
    Err(FaucetError::Config(format!(
        "{connector}: `{field}` authenticates with the server's own identity (its environment, \
         instance or workload role, or local credential files), which this config may not use; \
         name credentials in the config, or start `faucet serve` with \
         `--allow-host-credentials` (never available to a tenant)"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn the_scope_decides_and_the_innermost_scope_wins() {
        assert_eq!(scoped_host_credentials(), None);
        with_host_credentials(false, async {
            assert!(!host_credentials_allowed());
            let err = require_host_credentials("s3", "credentials").unwrap_err();
            let text = err.to_string();
            assert!(text.contains("s3: `credentials`"), "{text}");
            assert!(text.contains("--allow-host-credentials"), "{text}");
            with_host_credentials(true, async {
                assert!(host_credentials_allowed());
                assert!(require_host_credentials("s3", "credentials").is_ok());
            })
            .await;
            assert_eq!(scoped_host_credentials(), Some(false));
        })
        .await;
    }

    #[tokio::test]
    async fn a_reference_policy_scope_carries_its_answer() {
        use crate::resolve::ReferencePolicy;
        use std::sync::Arc;
        let deny = Arc::new(ReferencePolicy::deny_all());
        crate::http::with_reference_policy(deny.clone(), async {
            assert!(!host_credentials_allowed());
            let spawned = tokio::spawn(crate::http::inherit_reference_policy(async {
                host_credentials_allowed()
            }));
            assert!(!spawned.await.unwrap(), "a spawned task keeps the scope");
            with_host_credentials(true, async {
                let spawned = tokio::spawn(crate::http::inherit_reference_policy(async {
                    (
                        host_credentials_allowed(),
                        crate::http::current_reference_policy().is_some(),
                    )
                }));
                assert_eq!(spawned.await.unwrap(), (true, true));
            })
            .await;
        })
        .await;
        let open = Arc::new(ReferencePolicy::deny_all().with_host_credentials(true));
        crate::http::with_reference_policy(open, async {
            assert!(host_credentials_allowed());
        })
        .await;
        let only_grant = tokio::spawn(with_host_credentials(
            false,
            crate::http::inherit_reference_policy(async { host_credentials_allowed() }),
        ));
        assert!(!only_grant.await.unwrap());
    }

    #[test]
    fn denying_unscoped_tasks_is_process_wide_and_first_wins() {
        // The only test in this binary that flips the process-wide switch;
        // every other test here runs inside a scope.
        assert!(!unscoped_host_credentials_denied());
        assert!(host_credentials_allowed());
        assert!(require_host_credentials("kinesis", "credentials").is_ok());
        assert!(deny_unscoped_host_credentials());
        assert!(!deny_unscoped_host_credentials());
        assert!(unscoped_host_credentials_denied());
        assert!(!host_credentials_allowed());
        assert!(require_host_credentials("kinesis", "credentials").is_err());
    }
}
