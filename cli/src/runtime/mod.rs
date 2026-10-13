//! The runtime API (RFC 0013, #887): every operation that loads or executes a
//! pipeline config — `plan`, `submit`, `run`, `cancel`, `replay_dlq`,
//! `verify`, `rollback`, `trigger_template`, plus `doctor`, `backfill` and
//! the status/state config loads — goes through this module, on behalf of a
//! [`Principal`].
//!
//! Each operation applies the same [`Stage`]s in the same order:
//!
//! 1. **load** — parse, merge the default base, bind params, resolve secrets;
//! 2. **trust** — screen the body for its origin, bind the tenant scope and
//!    the reference policy (#881);
//! 3. **policy** — the data-flow policy gate (#702);
//! 4. **budget** — merge the caller's ceilings into the config's (#703);
//! 5. **tenant** — admit the run against its tenant's limits (#709);
//! 6. **governance** — the governed execution options (#875);
//! 7. **execution** — [`run`], the one place a runtime calls the executor.
//!
//! An operation that does not need a stage (a read-only plan writes nothing,
//! so it neither spends a budget nor takes a tenant slot) skips it; it never
//! reorders them. The HTTP handlers, MCP tools, triggers and change-request
//! execution of `faucet serve` are adapters over [`server`]; the CLI commands
//! call [`local`] with [`Principal::local`].

#[cfg(all(
    test,
    feature = "tenants",
    feature = "policy",
    feature = "mcp",
    feature = "triggers",
    feature = "source-csv",
    feature = "sink-jsonl"
))]
mod adapter_matrix;
pub mod local;
mod principal;
#[cfg(feature = "serve")]
pub mod server;

pub use principal::{Origin, Principal};

use crate::error::CliResult;
use crate::executor::{ExecuteOptions, RunSummary};
use crate::expand::ExpandedNode;

/// One stage of an operation, in the order every operation applies them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Stage {
    Load,
    Trust,
    Policy,
    Budget,
    Tenant,
    Governance,
    Execution,
}

impl Stage {
    /// Every stage, in order.
    pub const ORDER: [Stage; 7] = [
        Stage::Load,
        Stage::Trust,
        Stage::Policy,
        Stage::Budget,
        Stage::Tenant,
        Stage::Governance,
        Stage::Execution,
    ];

    /// The stage's name.
    pub fn as_str(self) -> &'static str {
        match self {
            Stage::Load => "load",
            Stage::Trust => "trust",
            Stage::Policy => "policy",
            Stage::Budget => "budget",
            Stage::Tenant => "tenant",
            Stage::Governance => "governance",
            Stage::Execution => "execution",
        }
    }
}

/// Execute expanded nodes for `principal`: the execution stage of every
/// runtime. A body a caller supplied runs under the caller reference policy,
/// so its HTTP clients cannot reach what the policy refuses.
pub async fn run(
    principal: &Principal,
    nodes: Vec<ExpandedNode>,
    opts: ExecuteOptions,
) -> CliResult<RunSummary> {
    under(principal, crate::executor::run_expanded(nodes, opts)).await
}

/// Run `fut` under the principal's runtime reference policy, if any: the
/// whole of an operation whose HTTP clients act for the principal.
pub async fn under<F: std::future::Future>(principal: &Principal, fut: F) -> F::Output {
    match principal.reference_policy() {
        Some(p) => faucet_core::http::with_reference_policy(p, fut).await,
        None => fut.await,
    }
}

/// [`run`] for [`Principal::local`]: the CLI runtimes and the engines
/// (backfill, mirror, DLQ replay) that run inside an operation already
/// scoped to its principal.
pub async fn run_local(nodes: Vec<ExpandedNode>, opts: ExecuteOptions) -> CliResult<RunSummary> {
    run(&Principal::local(), nodes, opts).await
}

/// [`run`] as a boxed future, for a caller the executor itself calls (a
/// post-run verify repair) — the box breaks the recursive future type.
pub fn run_boxed(
    principal: Principal,
    nodes: Vec<ExpandedNode>,
    opts: ExecuteOptions,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = CliResult<RunSummary>> + Send>> {
    Box::pin(async move { run(&principal, nodes, opts).await })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stages_are_ordered_and_named() {
        let mut sorted = Stage::ORDER;
        sorted.sort();
        assert_eq!(sorted, Stage::ORDER);
        let names: Vec<&str> = Stage::ORDER.iter().map(|s| s.as_str()).collect();
        assert_eq!(
            names,
            [
                "load",
                "trust",
                "policy",
                "budget",
                "tenant",
                "governance",
                "execution"
            ]
        );
    }

    #[tokio::test]
    async fn run_executes_nothing_for_no_nodes() {
        let opts = crate::executor::ExecuteOptions::for_tests("rt");
        let summary = run(&Principal::local(), Vec::new(), opts).await.unwrap();
        assert!(summary.invocations.is_empty());
    }
}
