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
        .and_then(|b| b.get("emit"))
        .and_then(|e| e.get("sink_ref"))
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
/// whichever carries `fan_out: true`. Returned so the caller can read `emit.sink_ref`
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
/// `discovery.fan_out` or `odata.fan_out`.
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

    // ── resolve_dynamic_fanout end-to-end (wiremock) ─────────────────────────

    use wiremock::matchers::{method as m, path as wpath};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn catalog() -> crate::auth_catalog::AuthCatalog {
        crate::auth_catalog::build_auth_catalog(None).unwrap()
    }

    /// A config whose single source template carries `source_config`.
    fn cfg_with_source(source_config: Value, named: bool) -> PipelineConfig {
        let src = json!({ "type": "rest", "config": source_config });
        let pipeline = if named {
            json!({
                "sources": { "api": src },
                "sink": { "type": "stdout", "config": {} }
            })
        } else {
            json!({
                "source": src,
                "sink": { "type": "stdout", "config": {} }
            })
        };
        serde_json::from_value(json!({ "version": 1, "name": "fo", "pipeline": pipeline })).unwrap()
    }

    /// A `discovery:` recipe listing `objects` from `/objects`, emitting a
    /// per-dataset path + sink table_id. Built by serializing a real
    /// `RestStreamConfig` so every required connector field is present.
    fn recipe(base: &str, list_path: &str) -> Value {
        let mut cfg = serde_json::to_value(faucet_source_rest::RestStreamConfig::new(base, "/"))
            .expect("rest config serializes");
        cfg["discovery"] = json!({
            "list": { "get": list_path, "items": "$.items[*]", "name": "$.name" },
            "emit": {
                "config": { "path": "/${name_lower}" },
                "table_id": "t_${name_snake}",
                "sink_ref": "bq"
            },
            "fan_out": true
        });
        cfg
    }

    #[tokio::test]
    async fn no_fanout_source_is_a_no_op() {
        let plain = serde_json::to_value(faucet_source_rest::RestStreamConfig::new(
            "https://api.example.com",
            "/x",
        ))
        .unwrap();
        let mut cfg = cfg_with_source(plain, false);
        cfg.matrix = Vec::new();
        resolve_dynamic_fanout(&mut cfg, &catalog()).await.unwrap();
        assert!(cfg.matrix.is_empty(), "matrix untouched");
    }

    #[tokio::test]
    async fn generates_one_row_per_discovered_dataset() {
        let server = MockServer::start().await;
        Mock::given(m("GET"))
            .and(wpath("/objects"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "items": [{ "name": "Account" }, { "name": "Lead" }]
            })))
            .mount(&server)
            .await;

        // Named template → the generated rows carry an explicit `source.ref`.
        let mut cfg = cfg_with_source(recipe(&server.uri(), "/objects"), true);
        resolve_dynamic_fanout(&mut cfg, &catalog()).await.unwrap();
        assert_eq!(cfg.matrix.len(), 2);
        let row = &cfg.matrix[0];
        assert_eq!(row.id.as_deref(), Some("Account"));
        let src = row.source.as_ref().unwrap();
        assert_eq!(src.r#ref.as_deref(), Some("api"));
        assert_eq!(src.config.as_ref().unwrap()["path"], "/account");
        let sink = row.sink.as_ref().unwrap();
        assert_eq!(sink.r#ref.as_deref(), Some("bq"));
        assert_eq!(sink.config.as_ref().unwrap()["table_id"], "t_account");

        // Singular `pipeline.source` → registered as `default`, so no ref.
        let mut cfg = cfg_with_source(recipe(&server.uri(), "/objects"), false);
        resolve_dynamic_fanout(&mut cfg, &catalog()).await.unwrap();
        assert_eq!(cfg.matrix.len(), 2);
        assert!(cfg.matrix[0].source.as_ref().unwrap().r#ref.is_none());
    }

    #[tokio::test]
    async fn discovery_returning_no_datasets_is_an_error() {
        let server = MockServer::start().await;
        Mock::given(m("GET"))
            .and(wpath("/objects"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "items": [] })))
            .mount(&server)
            .await;
        let mut cfg = cfg_with_source(recipe(&server.uri(), "/objects"), false);
        let err = resolve_dynamic_fanout(&mut cfg, &catalog())
            .await
            .expect_err("an empty fan-out would silently sync nothing");
        assert!(err.to_string().contains("returned no datasets"), "{err}");
    }

    #[tokio::test]
    async fn discovery_failure_is_surfaced_with_context() {
        let server = MockServer::start().await;
        Mock::given(m("GET"))
            .and(wpath("/objects"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;
        let mut cfg = cfg_with_source(recipe(&server.uri(), "/objects"), false);
        let err = resolve_dynamic_fanout(&mut cfg, &catalog())
            .await
            .expect_err("a 500 on the listing must fail the run");
        assert!(err.to_string().contains("discovery failed"), "{err}");
    }

    #[tokio::test]
    async fn a_source_kind_that_cannot_discover_is_rejected() {
        // `csv` has no discovery support; the fan-out opt-in lives in a block
        // it ignores, so the error must name the kind rather than silently
        // running a single un-fanned-out invocation.
        let src = json!({
            "type": "csv",
            "config": { "path": "/tmp/x.csv", "odata": { "fan_out": true } }
        });
        let cfg_json = json!({
            "version": 1,
            "pipeline": { "source": src, "sink": { "type": "stdout", "config": {} } }
        });
        let mut cfg: PipelineConfig = serde_json::from_value(cfg_json).unwrap();
        let err = resolve_dynamic_fanout(&mut cfg, &catalog())
            .await
            .expect_err("csv cannot fan out");
        let msg = err.to_string();
        assert!(msg.contains("dynamic fan-out"), "{msg}");
    }
}
