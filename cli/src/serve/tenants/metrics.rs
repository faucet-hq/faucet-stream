//! `faucet_serve_tenant_*`, `faucet_serve_connections` and
//! `faucet_serve_connect_flows_total` (#709). The `tenant` label is bounded by
//! the tenants an operator creates.

use crate::serve::state::ServerState;
use metrics::{counter, describe_counter, describe_gauge, gauge};

pub fn describe() {
    describe_counter!(
        "faucet_serve_tenant_runs_total",
        "Runs started for a tenant that finished, by tenant and outcome"
    );
    describe_counter!(
        "faucet_serve_tenant_limit_rejections_total",
        "Runs refused by a tenant limit, by tenant and limit"
    );
    describe_gauge!(
        "faucet_serve_connections",
        "Tenant connections by status (active / needs_reauth)"
    );
    describe_counter!(
        "faucet_serve_connect_flows_total",
        "Hosted OAuth connect flows, by provider and outcome"
    );
}

pub fn record_run(tenant: &str, outcome: &str) {
    counter!(
        "faucet_serve_tenant_runs_total",
        "tenant" => tenant.to_string(),
        "outcome" => outcome.to_string()
    )
    .increment(1);
}

pub fn record_limit_rejection(tenant: &str, limit: &'static str) {
    counter!(
        "faucet_serve_tenant_limit_rejections_total",
        "tenant" => tenant.to_string(),
        "limit" => limit
    )
    .increment(1);
}

pub fn record_connect_flow(provider: &str, outcome: &'static str) {
    counter!(
        "faucet_serve_connect_flows_total",
        "provider" => provider.to_string(),
        "outcome" => outcome
    )
    .increment(1);
}

/// Count connections by status across every tenant. Best-effort.
pub async fn refresh_connection_gauges(state: &ServerState) {
    let history = state.history();
    let Ok(tenants) = history.tenant_list().await else {
        return;
    };
    let (mut active, mut reauth) = (0u64, 0u64);
    for t in tenants {
        if let Ok(conns) = history.connection_list(&t.id).await {
            for c in conns {
                match c.status {
                    crate::serve::history::tenants::ConnectionStatus::Active => active += 1,
                    crate::serve::history::tenants::ConnectionStatus::NeedsReauth => reauth += 1,
                }
            }
        }
    }
    gauge!("faucet_serve_connections", "status" => "active").set(active as f64);
    gauge!("faucet_serve_connections", "status" => "needs_reauth").set(reauth as f64);
}
