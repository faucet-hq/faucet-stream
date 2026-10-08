//! Generic range partitioning (#479) — split one matrix row into N independent
//! invocations, each scoped to a chunk of a range via `${partition.*}` tokens.
//!
//! This is source-agnostic by construction: substitution walks the string leaves
//! of a connector config, so a REST URL, a SQL `WHERE`, an object prefix and a
//! Mongo filter all work with no connector code. See the [`mod@plan`] module for
//! the mechanism and [`spec`] for why the kinds are a tagged enum.

pub mod plan;
pub mod probe;
pub mod spec;

pub use plan::{PartitionChunk, plan, references_partition, substitute};
pub use probe::{needs_probe, resolve_bounds};

/// Every pass a config needs before `expand` can plan its real rows — probed
/// partition bounds, then discovery fan-out — with the auth catalog in hand.
/// Every command that executes rows (or reads their per-row state) calls this,
/// so they all plan the rows `faucet run` would (#789 CLI-57, CLI-131).
pub async fn resolve_runtime_with(
    cfg: &mut crate::config::PipelineConfig,
    auth: &crate::auth_catalog::AuthCatalog,
) -> crate::error::CliResult<()> {
    resolve_config_bounds(cfg, auth).await?;
    crate::dynamic_fanout::resolve_dynamic_fanout(cfg, auth).await
}

/// [`resolve_runtime_with`] on a copy, building the auth catalog only when a
/// pass has I/O to do.
pub async fn resolve_runtime(
    cfg: &crate::config::PipelineConfig,
) -> crate::error::CliResult<crate::config::PipelineConfig> {
    let mut out = cfg.clone();
    if !has_probes(cfg) && !crate::dynamic_fanout::has_fanout_source(cfg)? {
        return Ok(out);
    }
    let auth = crate::auth_catalog::build_auth_catalog(cfg.auth.as_ref())?;
    resolve_runtime_with(&mut out, &auth).await?;
    Ok(out)
}

/// `cfg` with every probed bound replaced by a one-chunk placeholder, for the
/// offline commands (`validate`, `explain`, template registration …) that plan
/// rows without touching a backend.
pub fn offline(cfg: &crate::config::PipelineConfig) -> crate::config::PipelineConfig {
    let mut out = cfg.clone();
    if let Some(spec) = out.partition.as_mut() {
        *spec = probe::placeholder_bounds(spec);
    }
    for row in &mut out.matrix {
        if let Some(spec) = row.partition.as_mut() {
            *spec = probe::placeholder_bounds(spec);
        }
    }
    out
}

/// Whether any partition in `cfg` discovers a bound at run time.
pub fn has_probes(cfg: &crate::config::PipelineConfig) -> bool {
    cfg.partition.as_ref().is_some_and(needs_probe)
        || cfg
            .matrix
            .iter()
            .any(|r| r.partition.as_ref().is_some_and(needs_probe))
}

/// Resolve every discoverable partition bound in `cfg` in place (#479).
///
/// Runs before `expand`, because planning needs concrete bounds and `expand` is
/// synchronous with no registry access. A config with no probes does no I/O.
pub async fn resolve_config_bounds(
    cfg: &mut crate::config::PipelineConfig,
    auth: &crate::auth_catalog::AuthCatalog,
) -> crate::error::CliResult<()> {
    if let Some(spec) = cfg.partition.clone() {
        cfg.partition = Some(resolve_bounds(&spec, auth).await?);
    }
    for row in &mut cfg.matrix {
        if let Some(spec) = row.partition.clone() {
            row.partition = Some(resolve_bounds(&spec, auth).await?);
        }
    }
    Ok(())
}
pub use spec::{BoundProbe, CountBound, IntBound, PartitionSpec};
