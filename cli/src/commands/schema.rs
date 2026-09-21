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
        "replication",
        "backfill",
        "partition",
        "execution",
        "resilience",
        "sla",
    ];
    #[cfg(feature = "quality")]
    targets.push("quality");
    #[cfg(feature = "contract")]
    targets.push("contract");
    #[cfg(feature = "masking")]
    targets.push("masking");
    targets.push("test");
    #[cfg(feature = "templates")]
    targets.push("template-test");
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
        SchemaTarget::Dlq => {
            let dlq_schema = faucet_core::schema_for!(crate::config::DlqSpec);
            serde_json::to_value(dlq_schema)
                .unwrap_or_else(|_| serde_json::json!({"type": "object"}))
        }
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
        SchemaTarget::Test => {
            let s = faucet_core::schema_for!(crate::pipeline_test::spec::TestSpecFile);
            serde_json::to_value(s).unwrap_or_else(|_| serde_json::json!({"type": "object"}))
        }
        #[cfg(feature = "templates")]
        SchemaTarget::TemplateTest => serde_json::to_value(faucet_core::schema_for!(
            crate::templates::suite::spec::SuiteFile
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
