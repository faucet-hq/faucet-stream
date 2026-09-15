//! Run-time discovery-driven matrix fan-out (#647).
//!
//! A generic template can declare a discovery block with `fan_out: true` on its
//! source (a `discovery:` recipe or an `odata:` block) and **no** `matrix:`. At
//! run time — in both `faucet run` and `faucet serve` — [`resolve_dynamic_fanout`]
//! builds that source, calls [`Source::discover`](faucet_core::Source::discover),
//! and generates one matrix row per discovered dataset (each with its config
//! patch + a per-dataset sink `table_id`), *before* [`expand`](crate::expand).
//! So the object list is a trigger-time parameter and every object's fields are
//! resolved live — no pre-generated matrix, new fields picked up automatically.
//!
//! It is a run-time analogue of `faucet discover`: same descriptors → the same
//! rows, materialized in-memory instead of written to a file. A config with no
//! `fan_out` source is left untouched (this is a cheap no-op).

use crate::auth_catalog::AuthCatalog;
use crate::config::{ConnectorSpec, MatrixRow, PipelineConfig};
use crate::error::{CliError, CliResult};
use serde_json::{Value, json};

/// If a source template declares a discovery block with `fan_out: true`, discover
/// its datasets and replace `cfg.matrix` with one row per dataset. No-op otherwise.
pub async fn resolve_dynamic_fanout(cfg: &mut PipelineConfig, auth: &AuthCatalog) -> CliResult<()> {
    let Some((src_ref, spec)) = find_fanout_source(cfg) else {
        return Ok(());
    };
    let source = crate::registry::build_source(&spec.kind, spec.config.clone(), auth, None)
        .await
        .map_err(|e| {
            CliError::Config(format!("dynamic fan-out: building source '{src_ref}': {e}"))
        })?;
    if !source.supports_discover() {
        return Err(CliError::Config(format!(
            "dynamic fan-out: source '{src_ref}' (kind '{}') does not support discovery — \
             remove `fan_out` or point it at a discoverable source",
            spec.kind
        )));
    }
    let descriptors = source
        .discover()
        .await
        .map_err(|e| CliError::Config(format!("dynamic fan-out: discovery failed: {e}")))?;
    if descriptors.is_empty() {
        return Err(CliError::Config(
            "dynamic fan-out: discovery returned no datasets (check the `discovery`/`odata` block)"
                .into(),
        ));
    }
    let sink_ref = fanout_block(&spec)
        .and_then(|b| b.get("sink_ref"))
        .and_then(Value::as_str)
        .map(str::to_string);
    cfg.matrix = descriptors_to_rows(&descriptors, &src_ref, sink_ref.as_deref())?;
    tracing::info!(
        source = %src_ref,
        objects = cfg.matrix.len(),
        "dynamic fan-out: generated matrix from live discovery"
    );
    Ok(())
}

/// The block driving fan-out — a `discovery:` recipe or an `odata:` block —
/// whichever carries `fan_out: true`. Returned so the caller can read `sink_ref`
/// from the same block that opted in.
fn fanout_block(spec: &ConnectorSpec) -> Option<&Value> {
    for key in ["discovery", "odata"] {
        if let Some(block) = spec.config.get(key)
            && block.get("fan_out").and_then(Value::as_bool) == Some(true)
        {
            return Some(block);
        }
    }
    None
}

/// Find a source template (named `sources.*`, or the singular `source`
/// registered as `default`) whose config opts into fan-out via
/// `salesforce.fan_out` or `odata.fan_out`.
fn find_fanout_source(cfg: &PipelineConfig) -> Option<(String, ConnectorSpec)> {
    // Prefer a named template; fall back to the singular default source.
    for (name, spec) in &cfg.pipeline.sources {
        if fanout_block(spec).is_some() {
            return Some((name.clone(), spec.clone()));
        }
    }
    if let Some(spec) = &cfg.pipeline.source
        && fanout_block(spec).is_some()
    {
        return Some(("default".to_string(), spec.clone()));
    }
    None
}

/// Turn discovery descriptors into matrix rows — the structured, in-memory twin
/// of `render_discovered_config`. Each row deep-merges the descriptor's
/// `config_patch` over the source template and its `sink_patch` over `sink_ref`.
fn descriptors_to_rows(
    descriptors: &[faucet_core::DatasetDescriptor],
    src_ref: &str,
    sink_ref: Option<&str>,
) -> CliResult<Vec<MatrixRow>> {
    let ids = crate::commands::discover::unique_row_ids(descriptors);
    let mut rows = Vec::with_capacity(descriptors.len());
    for (d, id) in descriptors.iter().zip(&ids) {
        let mut row = json!({ "id": id, "source": { "config": d.config_patch } });
        if src_ref != "default" {
            row["source"]["ref"] = json!(src_ref);
        }
        if sink_ref.is_some() || d.sink_patch.is_some() {
            let mut sink = json!({});
            if let Some(sr) = sink_ref {
                sink["ref"] = json!(sr);
            }
            if let Some(patch) = &d.sink_patch {
                sink["config"] = patch.clone();
            }
            row["sink"] = sink;
        }
        rows.push(serde_json::from_value::<MatrixRow>(row).map_err(|e| {
            CliError::Config(format!(
                "dynamic fan-out: could not build matrix row '{id}': {e}"
            ))
        })?);
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::DatasetDescriptor;

    #[test]
    fn rows_carry_source_ref_config_and_sink_patch() {
        let ds = vec![
            DatasetDescriptor::new(
                "Account",
                "sobject",
                json!({"async_job": {"submit": {"json": {"query": "SELECT Id FROM Account"}}}}),
            )
            .with_sink_patch(json!({ "table_id": "account" })),
            DatasetDescriptor::new("Churn__c", "sobject", json!({"async_job": {}}))
                .with_sink_patch(json!({ "table_id": "churn_c" })),
        ];
        let rows = descriptors_to_rows(&ds, "default", Some("bigquery")).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].id.as_deref(), Some("Account"));
        // default source template → no explicit ref
        let src = rows[0].source.as_ref().unwrap();
        assert!(src.r#ref.is_none());
        assert_eq!(
            src.config.as_ref().unwrap()["async_job"]["submit"]["json"]["query"],
            "SELECT Id FROM Account"
        );
        let sink = rows[0].sink.as_ref().unwrap();
        assert_eq!(sink.r#ref.as_deref(), Some("bigquery"));
        assert_eq!(sink.config.as_ref().unwrap()["table_id"], "account");
    }

    #[test]
    fn fanout_block_detects_discovery_and_odata() {
        let rec: ConnectorSpec = serde_json::from_value(json!({
            "type": "rest",
            "config": { "discovery": { "fan_out": true, "sink_ref": "bq" } }
        }))
        .unwrap();
        assert_eq!(
            fanout_block(&rec)
                .and_then(|b| b.get("sink_ref"))
                .and_then(Value::as_str),
            Some("bq")
        );

        let od: ConnectorSpec = serde_json::from_value(json!({
            "type": "rest",
            "config": { "odata": { "fan_out": true, "objects": "A,B", "table_prefix": "fno_" } }
        }))
        .unwrap();
        assert!(fanout_block(&od).is_some());

        // fan_out absent / false → not a fan-out source.
        let plain: ConnectorSpec = serde_json::from_value(json!({
            "type": "rest",
            "config": { "odata": { "entity": "Orders" } }
        }))
        .unwrap();
        assert!(fanout_block(&plain).is_none());
    }

    #[test]
    fn named_source_ref_is_set() {
        let ds = vec![DatasetDescriptor::new("Lead", "dataset", json!({}))];
        let rows = descriptors_to_rows(&ds, "api", None).unwrap();
        assert_eq!(
            rows[0].source.as_ref().unwrap().r#ref.as_deref(),
            Some("api")
        );
        assert!(rows[0].sink.is_none()); // no sink_ref, no sink_patch
    }
}
