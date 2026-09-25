//! `faucet schema` — print the JSON Schema for a connector's config.

use crate::cli::{SchemaArgs, SchemaTarget};
use crate::error::{CliError, CliResult};
use crate::registry::{sink_schema, source_schema};
use crate::transforms::transform_schema;

/// Every valid `faucet schema <target>` keyword compiled into this binary, in a
/// stable order. Feature-gated targets appear only when their feature is on, so
/// the listing matches what this build can actually emit. `source`, `sink`, and
/// `transform` additionally take a connector/transform NAME argument.
pub fn schema_targets() -> Vec<&'static str> {
    let mut targets = vec![
        "config",
        "source",
        "sink",
        "transform",
        "dlq",
        "mirror",
        "backfill",
        "partition",
        "execution",
        "resilience",
        "sla",
        "profiling",
        "verify",
        "rollback",
    ];
    #[cfg(feature = "quality")]
    targets.push("quality");
    #[cfg(feature = "contract")]
    targets.push("contract");
    #[cfg(feature = "masking")]
    targets.push("masking");
    targets.push("test");
    targets.push("source-template");
    targets.push("sink-template");
    #[cfg(feature = "templates")]
    targets.push("template-test");
    #[cfg(feature = "templates-sync")]
    targets.push("templates-sync");
    targets.push("secrets");
    #[cfg(feature = "schedule")]
    targets.push("schedule");
    #[cfg(feature = "lineage")]
    targets.push("lineage");
    #[cfg(feature = "triggers")]
    targets.push("triggers");
    #[cfg(feature = "notify")]
    targets.push("notifications");
    #[cfg(feature = "catalog")]
    targets.push("catalog");
    #[cfg(feature = "catalog")]
    targets.push("local-outputs");
    targets.push("params");
    targets
}

/// A config block the web console's submit form can add beside the source,
/// sink, and transforms: its name, a one-line description, and whether it
/// lives under `pipeline:` or at the top level of the config.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct PipelineBlock {
    pub name: &'static str,
    pub description: &'static str,
    pub placement: &'static str,
}

/// Every [`PipelineBlock`] compiled into this binary, in form order.
pub fn pipeline_blocks() -> Vec<PipelineBlock> {
    let block = |name, description, placement| PipelineBlock {
        name,
        description,
        placement,
    };
    let mut blocks = vec![
        block(
            "state",
            "Where bookmarks are kept so the next run resumes where this one stopped",
            "pipeline",
        ),
        block(
            "dlq",
            "A sink for records that fail to write or are quarantined",
            "pipeline",
        ),
        block(
            "delivery",
            "At-least-once, or exactly-once where the source and sink support it",
            "top",
        ),
        block(
            "resilience",
            "Retries, circuit breaker, and poison-record handling for sink writes",
            "top",
        ),
        block(
            "sla",
            "Freshness and volume expectations checked after every run",
            "top",
        ),
        block(
            "profiling",
            "Learn each column's profile per run and flag statistically significant drift — no thresholds",
            "top",
        ),
        block(
            "verify",
            "Compare the destination to the source by content after every run; repair differences",
            "top",
        ),
        block(
            "rollback",
            "Make every run undoable with `faucet rollback` (journal, kept previous table, run-id column)",
            "top",
        ),
    ];
    #[cfg(feature = "quality")]
    blocks.push(block(
        "quality",
        "Per-record and per-batch data-quality checks",
        "pipeline",
    ));
    #[cfg(feature = "contract")]
    blocks.push(block(
        "contract",
        "A versioned promise about the output's fields and types",
        "pipeline",
    ));
    #[cfg(feature = "masking")]
    blocks.push(block(
        "masking",
        "Detect and mask PII before it reaches any sink",
        "pipeline",
    ));
    blocks.push(block(
        "schema",
        "What to do when incoming records drift from the destination schema",
        "pipeline",
    ));
    blocks
}

fn to_schema_value(s: faucet_core::schemars::Schema) -> serde_json::Value {
    serde_json::to_value(s).unwrap_or_else(|_| serde_json::json!({"type": "object"}))
}

/// The `state:` block as a discriminated union over the compiled backends, so
/// a form can offer each backend's own fields (the Rust type keeps `config`
/// untyped because the backends are feature-gated).
fn state_block_schema() -> serde_json::Value {
    use serde_json::json;
    let variant = |kind: &str, config: serde_json::Value| {
        json!({
            "type": "object",
            "properties": { "type": { "const": kind }, "config": config },
            "required": ["type"],
        })
    };
    let mut variants = vec![
        variant(
            "file",
            json!({
                "type": "object",
                "properties": { "path": { "type": "string", "description": "Directory holding one JSON file per bookmark" } },
                "required": ["path"],
            }),
        ),
        variant(
            "memory",
            json!({ "type": "object", "properties": {}, "description": "Kept in memory only; lost when the run ends" }),
        ),
    ];
    #[cfg(feature = "state-redis")]
    variants.push(variant(
        "redis",
        json!({
            "type": "object",
            "properties": {
                "url": { "type": "string", "description": "Redis connection URL" },
                "namespace": { "type": "string", "default": "faucet", "description": "Key prefix" },
            },
            "required": ["url"],
        }),
    ));
    #[cfg(feature = "state-postgres")]
    variants.push(variant(
        "postgres",
        json!({
            "type": "object",
            "properties": {
                "url": { "type": "string", "description": "PostgreSQL connection URL" },
                "table": { "type": "string", "default": "faucet_state", "description": "Bookmark table" },
                "ensure_table": { "type": "boolean", "description": "Create the table if it does not exist" },
                "max_connections": { "type": "integer", "description": "Connection pool size" },
            },
            "required": ["url"],
        }),
    ));
    json!({ "title": "State store", "oneOf": variants })
}

/// The JSON Schema for one [`PipelineBlock`], or `None` for an unknown or
/// uncompiled block.
pub fn block_schema(name: &str) -> Option<serde_json::Value> {
    Some(match name {
        "state" => state_block_schema(),
        "dlq" => to_schema_value(faucet_core::schema_for!(crate::config::DlqSpec)),
        "delivery" => to_schema_value(faucet_core::schema_for!(faucet_core::DeliveryMode)),
        "resilience" => to_schema_value(faucet_core::schema_for!(crate::config::ResilienceSpec)),
        "sla" => to_schema_value(faucet_core::schema_for!(crate::sla::SlaSpec)),
        "profiling" => to_schema_value(faucet_core::schema_for!(faucet_core::ProfilingSpec)),
        "verify" => to_schema_value(faucet_core::schema_for!(crate::verify::VerifySpec)),
        "rollback" => to_schema_value(faucet_core::schema_for!(crate::rollback::RollbackSpec)),
        "schema" => to_schema_value(faucet_core::schema_for!(faucet_core::SchemaDriftSpec)),
        #[cfg(feature = "quality")]
        "quality" => to_schema_value(faucet_core::schema_for!(faucet_core::QualitySpec)),
        #[cfg(feature = "contract")]
        "contract" => to_schema_value(faucet_core::schema_for!(faucet_core::ContractSpec)),
        #[cfg(feature = "masking")]
        "masking" => to_schema_value(faucet_core::schema_for!(faucet_core::MaskingSpec)),
        _ => return None,
    })
}

/// Execute the `schema` subcommand.
pub async fn run(args: SchemaArgs) -> CliResult<()> {
    if args.list {
        println!("Valid `faucet schema <target>` targets:");
        for t in schema_targets() {
            match t {
                "source" | "sink" | "transform" => println!("  {t} <name>"),
                _ => println!("  {t}"),
            }
        }
        return Ok(());
    }
    let target = args.target.ok_or_else(|| {
        CliError::Config(
            "no schema target given — pass one (e.g. `faucet schema source rest`) or \
             `faucet schema --list` to see them all"
                .to_owned(),
        )
    })?;
    let schema = match target {
        SchemaTarget::Config => crate::schema_compose::config_schema(),
        SchemaTarget::Source { name } => source_schema(&name)?,
        SchemaTarget::Sink { name } => sink_schema(&name)?,
        SchemaTarget::Transform { name } => transform_schema(&name)?,
        SchemaTarget::Dlq => block_schema("dlq").expect("dlq is always compiled"),
        SchemaTarget::Verify => block_schema("verify").expect("verify is always compiled"),
        SchemaTarget::Rollback => block_schema("rollback").expect("rollback is always compiled"),
        SchemaTarget::Replication => {
            let s = faucet_core::schema_for!(crate::replication::spec::ReplicationSpec);
            serde_json::to_value(s).unwrap_or_else(|_| serde_json::json!({"type": "object"}))
        }
        SchemaTarget::Backfill => {
            let s = faucet_core::schema_for!(crate::backfill::BackfillSpec);
            serde_json::to_value(s).unwrap_or_else(|_| serde_json::json!({"type": "object"}))
        }
        SchemaTarget::Partition => {
            let s = faucet_core::schema_for!(crate::partition::PartitionSpec);
            serde_json::to_value(s).unwrap_or_else(|_| serde_json::json!({"type": "object"}))
        }
        SchemaTarget::Params => {
            let s = faucet_core::schema_for!(crate::params::ParamSpec);
            serde_json::to_value(s).unwrap_or_else(|_| serde_json::json!({"type": "object"}))
        }
        SchemaTarget::Execution => {
            let s = faucet_core::schema_for!(crate::config::ExecutionSpec);
            serde_json::to_value(s).unwrap_or_else(|_| serde_json::json!({"type": "object"}))
        }
        SchemaTarget::Resilience => {
            let s = faucet_core::schema_for!(crate::config::ResilienceSpec);
            serde_json::to_value(s).unwrap_or_else(|_| serde_json::json!({"type": "object"}))
        }
        SchemaTarget::Sla => {
            let s = faucet_core::schema_for!(crate::sla::SlaSpec);
            serde_json::to_value(s).unwrap_or_else(|_| serde_json::json!({"type": "object"}))
        }
        SchemaTarget::Profiling => {
            let s = faucet_core::schema_for!(faucet_core::ProfilingSpec);
            serde_json::to_value(s).unwrap_or_else(|_| serde_json::json!({"type": "object"}))
        }
        #[cfg(feature = "quality")]
        SchemaTarget::Quality => {
            let quality_schema = faucet_core::schema_for!(faucet_core::QualitySpec);
            serde_json::to_value(quality_schema)
                .unwrap_or_else(|_| serde_json::json!({"type": "object"}))
        }
        #[cfg(feature = "contract")]
        SchemaTarget::Contract => {
            let contract_schema = faucet_core::schema_for!(faucet_core::ContractSpec);
            serde_json::to_value(contract_schema)
                .unwrap_or_else(|_| serde_json::json!({"type": "object"}))
        }
        #[cfg(feature = "masking")]
        SchemaTarget::Masking => {
            let masking_schema = faucet_core::schema_for!(faucet_core::MaskingSpec);
            serde_json::to_value(masking_schema)
                .unwrap_or_else(|_| serde_json::json!({"type": "object"}))
        }
        #[cfg(feature = "schedule")]
        SchemaTarget::Schedule => {
            let s = faucet_core::schema_for!(crate::schedule::spec::ScheduleSpec);
            serde_json::to_value(s).unwrap_or_else(|_| serde_json::json!({"type": "object"}))
        }
        #[cfg(feature = "lineage")]
        SchemaTarget::Lineage => lineage_schema(),
        #[cfg(feature = "triggers")]
        SchemaTarget::Triggers => {
            let s = faucet_core::schema_for!(crate::serve::triggers::spec::TriggersFile);
            serde_json::to_value(s).unwrap_or_else(|_| serde_json::json!({"type": "object"}))
        }
        SchemaTarget::SourceTemplate => {
            serde_json::to_value(faucet_core::schema_for!(crate::hub::spec::SourceTemplate))
                .expect("schema serialization")
        }
        SchemaTarget::SinkTemplate => {
            serde_json::to_value(faucet_core::schema_for!(crate::hub::spec::SinkTemplate))
                .expect("schema serialization")
        }
        SchemaTarget::Deployment => serde_json::to_value(faucet_core::schema_for!(
            crate::hub::spec::DeploymentTemplate
        ))
        .expect("schema serialization"),
        SchemaTarget::Test => {
            let s = faucet_core::schema_for!(crate::pipeline_test::spec::TestSpecFile);
            serde_json::to_value(s).unwrap_or_else(|_| serde_json::json!({"type": "object"}))
        }
        #[cfg(feature = "templates")]
        SchemaTarget::TemplateTest => serde_json::to_value(faucet_core::schema_for!(
            crate::templates::suite::spec::SuiteFile
        ))
        .expect("schema serialization"),
        #[cfg(feature = "templates-sync")]
        SchemaTarget::TemplatesSync => serde_json::to_value(faucet_core::schema_for!(
            crate::templates::sync::spec::SyncFile
        ))
        .expect("schema serialization"),
        #[cfg(feature = "notify")]
        SchemaTarget::Notifications => {
            // The `notifications:` block is a list; emit the per-rule schema.
            let s = faucet_core::schema_for!(crate::notify::NotificationSpec);
            serde_json::to_value(s).unwrap_or_else(|_| serde_json::json!({"type": "object"}))
        }
        #[cfg(feature = "catalog")]
        SchemaTarget::Catalog => {
            let s = faucet_core::schema_for!(crate::catalog::CatalogSpec);
            serde_json::to_value(s).unwrap_or_else(|_| serde_json::json!({"type": "object"}))
        }
        #[cfg(feature = "catalog")]
        SchemaTarget::LocalOutputs => {
            let s = faucet_core::schema_for!(crate::local_outputs::LocalOutputsSpec);
            serde_json::to_value(s).unwrap_or_else(|_| serde_json::json!({"type": "object"}))
        }
        SchemaTarget::Secrets => serde_json::json!({
            "title": "Secrets-manager interpolation grammar",
            "schemes": {
                "vault":    { "syntax": "${vault:<path>[#field]}", "auth": ["VAULT_ADDR", "VAULT_TOKEN", "VAULT_NAMESPACE (optional)"] },
                "aws-sm":   { "syntax": "${aws-sm:<name-or-ARN>[#field]}", "auth": ["aws-config default credential chain"] },
                "gcp-sm":   { "syntax": "${gcp-sm:projects/<p>/secrets/<s>/versions/<v>}", "auth": ["Application Default Credentials"] },
                "azure-kv": { "syntax": "${azure-kv:<vault>/<secret>[/<version>]}", "auth": ["AZURE_* env / managed identity / az login"] }
            },
            "notes": [
                "#field parses the secret as JSON and extracts one key (vault, aws-sm).",
                "Resolved at config load; fetched concurrently and de-duplicated; never persisted.",
                "Build with --features secrets (or per-backend secrets-vault / secrets-aws-sm / ...)."
            ]
        }),
    };
    let body = serde_json::to_string_pretty(&schema).unwrap_or_else(|_| schema.to_string());
    println!("{body}");
    Ok(())
}

/// JSON Schema for the `lineage:` config block (`faucet schema lineage`).
#[cfg(feature = "lineage")]
pub fn lineage_schema() -> serde_json::Value {
    serde_json::to_value(faucet_lineage::schemars_schema())
        .unwrap_or_else(|_| serde_json::json!({"type": "object"}))
}

#[cfg(test)]
mod tests {
    use crate::cli::{SchemaArgs, SchemaTarget};

    #[test]
    fn every_pipeline_block_has_a_schema_and_a_known_placement() {
        let blocks = super::pipeline_blocks();
        assert!(blocks.iter().any(|b| b.name == "dlq"));
        for b in &blocks {
            assert!(matches!(b.placement, "pipeline" | "top"), "{}", b.name);
            assert!(!b.description.is_empty(), "{}", b.name);
            assert!(super::block_schema(b.name).is_some(), "{}", b.name);
        }
        assert!(super::block_schema("nope").is_none());
    }

    #[test]
    fn state_block_offers_each_backend_with_its_own_fields() {
        let v = super::block_schema("state").unwrap();
        let variants = v["oneOf"].as_array().unwrap();
        let kinds: Vec<&str> = variants
            .iter()
            .map(|v| v["properties"]["type"]["const"].as_str().unwrap())
            .collect();
        assert_eq!(&kinds[..2], ["file", "memory"]);
        assert_eq!(
            variants[0]["properties"]["config"]["required"],
            serde_json::json!(["path"])
        );
        let mut offered = kinds.clone();
        offered.sort_unstable();
        let mut compiled = crate::state::available_state_kinds();
        compiled.sort_unstable();
        assert_eq!(
            offered, compiled,
            "the form must offer exactly the compiled backends"
        );
    }

    #[cfg(feature = "lineage")]
    #[test]
    fn schema_lineage_returns_object_schema() {
        let v = super::lineage_schema();
        assert_eq!(v["type"], "object");
        assert!(v["properties"].get("transport").is_some());
        assert!(v["properties"].get("namespace").is_some());
    }

    #[test]
    fn schema_targets_includes_known_targets() {
        let targets = super::schema_targets();
        for known in ["config", "source", "sink", "dlq", "params"] {
            assert!(
                targets.contains(&known),
                "missing target {known}: {targets:?}"
            );
        }
        // No duplicates.
        let mut sorted = targets.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(
            sorted.len(),
            targets.len(),
            "duplicate targets: {targets:?}"
        );
    }

    #[tokio::test]
    async fn schema_list_flag_returns_ok() {
        let r = super::run(SchemaArgs {
            target: None,
            list: true,
        })
        .await;
        assert!(r.is_ok(), "{r:?}");
    }

    #[tokio::test]
    async fn schema_no_target_without_list_errors() {
        let r = super::run(SchemaArgs {
            target: None,
            list: false,
        })
        .await;
        assert!(
            r.is_err(),
            "expected an error when neither target nor --list given"
        );
    }

    #[tokio::test]
    async fn schema_replication_target_ok() {
        // Covers the `SchemaTarget::Replication` arm — it serializes the
        // ReplicationSpec JSON Schema to stdout and returns Ok.
        let r = super::run(SchemaArgs {
            target: Some(SchemaTarget::Replication),
            list: false,
        })
        .await;
        assert!(r.is_ok(), "{r:?}");
    }

    #[tokio::test]
    async fn schema_execution_target_ok() {
        let r = super::run(SchemaArgs {
            target: Some(SchemaTarget::Execution),
            list: false,
        })
        .await;
        assert!(r.is_ok(), "{r:?}");
    }

    #[test]
    fn execution_schema_includes_adaptive_batch_size() {
        let schema = faucet_core::schema_for!(crate::config::ExecutionSpec);
        let value = serde_json::to_value(schema).expect("execution schema serializes");
        assert!(value["properties"].get("adaptive_batch_size").is_some());
    }

    #[tokio::test]
    async fn schema_sla_target_ok() {
        let r = super::run(SchemaArgs {
            target: Some(SchemaTarget::Sla),
            list: false,
        })
        .await;
        assert!(r.is_ok(), "{r:?}");
    }

    #[tokio::test]
    async fn schema_profiling_target_ok() {
        let r = super::run(SchemaArgs {
            target: Some(SchemaTarget::Profiling),
            list: false,
        })
        .await;
        assert!(r.is_ok(), "{r:?}");
        let s = serde_json::to_string(&super::block_schema("profiling").unwrap()).unwrap();
        assert!(s.contains("on_drift") && s.contains("min_history"), "{s}");
        assert!(
            super::pipeline_blocks()
                .iter()
                .any(|b| b.name == "profiling")
        );
    }

    #[test]
    fn sla_schema_exposes_the_three_checks() {
        let schema = faucet_core::schema_for!(crate::sla::SlaSpec);
        let out = serde_json::to_string(&schema).expect("sla schema serializes");
        assert!(out.contains("max_staleness_secs"), "{out}");
        assert!(out.contains("min_rows_per_run"), "{out}");
        assert!(out.contains("volume_anomaly"), "{out}");
    }

    #[tokio::test]
    async fn schema_resilience_target_ok() {
        let r = super::run(SchemaArgs {
            target: Some(SchemaTarget::Resilience),
            list: false,
        })
        .await;
        assert!(r.is_ok(), "{r:?}");
    }

    #[test]
    fn schema_resilience_emits_json_schema() {
        // Mirrors `faucet schema resilience`: the serialized ResilienceSpec
        // schema must expose the retry `max_attempts` knob and the
        // `circuit_breaker` sub-block.
        let schema = faucet_core::schema_for!(crate::config::ResilienceSpec);
        let out = serde_json::to_string(&schema).expect("resilience schema serializes");
        assert!(out.contains("max_attempts"), "{out}");
        assert!(out.contains("circuit_breaker"), "{out}");
    }

    /// `faucet schema template-test` is the only way to discover the suite
    /// file's grammar, so the target must actually resolve to a schema rather
    /// than falling through the dispatch match.
    #[cfg(feature = "templates")]
    #[tokio::test]
    async fn schema_template_test_target_renders() {
        let r = super::run(SchemaArgs {
            target: Some(SchemaTarget::TemplateTest),
            list: false,
        })
        .await;
        assert!(r.is_ok(), "{r:?}");
        // The target must also be discoverable from `--list`.
        assert!(super::schema_targets().contains(&"template-test"));
    }
}
