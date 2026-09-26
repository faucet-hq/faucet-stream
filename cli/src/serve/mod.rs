//! `faucet serve` — HTTP control plane (#127). Runs pipeline configs submitted
//! over HTTP, reusing `executor::run_expanded`. Feature-gated on `serve`;
//! structured like `cli/src/schedule/`. See
//! `docs/superpowers/specs/2026-05-30-faucet-serve-design.md`.

pub mod audit;
pub mod auth;
pub mod callback;
pub mod changes;
pub mod cluster;
pub mod config;
pub mod error;
pub mod handlers;
pub mod history;
pub mod idempotency;
pub mod load;
pub mod logs;
#[cfg(feature = "mcp")]
pub mod mcp_route;
pub mod metrics;
pub mod observability;
pub mod preview;
pub mod rbac;
pub mod registry;
pub mod runner;
pub mod server;
pub mod state;
#[cfg(feature = "tenants")]
pub mod tenants;
#[cfg(test)]
pub mod test_support;
#[cfg(feature = "triggers")]
pub mod triggers;
#[cfg(feature = "serve-ui")]
pub mod ui_assets;

pub use config::ServeConfig;

use crate::error::CliResult;

/// MCP endpoint settings for `faucet serve --mcp` (#420). Threaded to
/// [`server::build_router`] so the `/mcp` route mounts only on opt-in. Kept
/// separate from [`ServeConfig`] so the many `ServeConfig` test builders are
/// untouched. `Default` = disabled.
#[derive(Debug, Clone, Default)]
pub struct McpServeSettings {
    /// Mount the `/mcp` route.
    pub enabled: bool,
    /// Expose the mutating MCP tools (still gated by the caller's RBAC scope).
    pub allow_mutations: bool,
}

/// Boot the HTTP control plane and serve until SIGTERM/SIGINT.
pub async fn run_server(config: ServeConfig, mcp: McpServeSettings) -> CliResult<()> {
    server::serve(config, mcp).await
}
