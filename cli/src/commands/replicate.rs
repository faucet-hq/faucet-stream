//! `faucet mirror` (alias `faucet replicate`) — load a config with a `mirror:` block, validate it,
//! and run the two-phase snapshot→CDC orchestration.

use crate::cli::ReplicateArgs;
use crate::config::PipelineConfig;
use crate::error::{CliError, CliResult};
use crate::replication::compiled::CompiledReplication;
use crate::replication::{ReplicationOptions, run_replication};

/// Execute the `replicate` subcommand.
pub async fn run(args: ReplicateArgs) -> CliResult<()> {
    let cwd = std::env::current_dir()?;
    let env_path =
        crate::env_loader::resolve_env_file(args.env_file.as_deref(), args.no_env_file, &cwd)?;
    crate::env_loader::load_env_file_if_present(env_path.as_deref())?;
    let path = match args.config {
        Some(p) => p,
        None => crate::env_loader::discover_config_path(&cwd).ok_or(CliError::NoConfigOrFromEnv)?,
    };

    let cfg = PipelineConfig::from_path_async(&path, args.profile.as_deref()).await?;
    let spec = cfg.replication.as_ref().ok_or_else(|| {
        CliError::Config(
            "no `mirror:` block in config (formerly `replication:`) — use `faucet run` for a one-shot \
             run, or add a `mirror:` block (see `faucet schema mirror`)"
                .into(),
        )
    })?;
    // Install observability before compiling the replication spec so the
    // tracing subscriber is live and `CompiledReplication::compile`'s
    // non-upsert-sink warning is actually emitted (it would be lost otherwise).
    crate::obs::install(&cfg)?;

    let compiled = CompiledReplication::compile(spec, &cfg)?;

    let pipeline_name = cfg.name.clone().unwrap_or_else(|| {
        path.file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("pipeline")
            .to_owned()
    });
    let auth = crate::auth_catalog::build_auth_catalog(cfg.auth.as_ref())?;
    let resilience = match &cfg.resilience {
        Some(spec) => Some(spec.to_policy()?),
        None => None,
    };
    #[cfg(feature = "notify")]
    let notifier = crate::notify::Notifier::from_specs(&cfg.notifications)?;
    #[cfg(feature = "catalog")]
    let catalog = match cfg.catalog.as_ref() {
        Some(spec) => Some(crate::catalog::connect_from_spec(spec).await?),
        None => None,
    };
    // Config snapshot for `faucet plan --diff` (#374). Best-effort: skip if the
    // config does not expand cleanly (some replication shapes are orchestration
    // -only). Recorded after the replication run succeeds below.
    #[cfg(feature = "catalog")]
    let snapshot_inputs = catalog.as_ref().and_then(|handle| {
        crate::expand::expand(&cfg)
            .ok()
            .map(|nodes| (handle.clone(), nodes, pipeline_name.clone()))
    });

    run_replication(
        &cfg,
        &compiled,
        ReplicationOptions {
            pipeline_name,
            execution: cfg.execution.clone(),
            auth,
            clock: chrono::Utc::now().fixed_offset(),
            resilience,
            sla: cfg.sla.clone(),
            reconcile: cfg.reconcile.clone(),
            verify: cfg.verify.clone(),
            rollback: cfg.rollback.clone(),
            usage: crate::usage::UsageOptions::from_spec(cfg.usage.as_ref(), path.parent())
                .map_err(CliError::Config)?,
            budget: cfg.budget.clone(),
            #[cfg(feature = "notify")]
            notifier,
            #[cfg(feature = "catalog")]
            catalog,
        },
    )
    .await?;

    // Reached only on success (errors returned via `?` above).
    #[cfg(feature = "catalog")]
    if let Some((handle, nodes, name)) = snapshot_inputs {
        crate::catalog::snapshot::record_if_ok(
            Some(&handle),
            &name,
            crate::catalog::snapshot::on_error_str(&cfg.execution),
            &nodes,
            true,
            chrono::Utc::now(),
        )
        .await;
    }

    // Flush any buffered OTLP telemetry before exiting (no-op without `otel`).
    faucet_core::shutdown_otel();

    println!("replication finished");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// Write `yaml` to a `faucet.yaml` inside a fresh temp dir and return the
    /// path (the dir is leaked so the file outlives the call — fine for a test).
    fn write_config(yaml: &str) -> std::path::PathBuf {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("repl.yaml");
        let mut f = std::fs::File::create(&path).expect("create config");
        f.write_all(yaml.as_bytes()).expect("write config");
        f.flush().expect("flush");
        // Keep the dir alive for the duration of the test process.
        std::mem::forget(dir);
        path
    }

    fn args(path: std::path::PathBuf) -> ReplicateArgs {
        ReplicateArgs {
            config: Some(path),
            env_file: None,
            no_env_file: true,
            profile: None,
        }
    }

    /// A valid pipeline config with NO `replication:` block must error out before
    /// any orchestration runs (no Docker / network reached). Works under default
    /// features (rest source + jsonl sink are always present).
    #[tokio::test]
    async fn errors_when_no_replication_block() {
        let path = write_config(
            r#"
version: 1
name: plain
pipeline:
  source: { type: rest, config: { url: "https://example.com/api" } }
  sink:   { type: jsonl, config: { path: ./out.jsonl } }
"#,
        );
        let err = run(args(path)).await.unwrap_err();
        assert!(
            format!("{err}").contains("replication"),
            "should mention the missing replication block: {err}"
        );
    }

    /// A config WITH a `replication:` block that fails `CompiledReplication::compile`
    /// (here: `state: memory`, which the durable-state rule rejects) must error
    /// before `run_replication` — so no Docker is needed. Gated on the connector
    /// kinds being compiled in so `source_supports_exactly_once` / `source_schema`
    /// resolve (otherwise compile would fail earlier on an unknown-kind error,
    /// which is a different, also-acceptable failure but not the branch under test).
    #[cfg(all(
        feature = "source-postgres-cdc",
        feature = "source-postgres",
        feature = "sink-postgres"
    ))]
    #[tokio::test]
    async fn errors_when_replication_spec_invalid() {
        let path = write_config(
            r#"
version: 1
name: mirror
pipeline:
  source: { type: postgres-cdc, config: { connection_url: "postgres://x", slot_name: s, publication_name: p } }
  sink:   { type: postgres, config: { connection_url: "postgres://y", table_name: t, column_mapping: auto_map, write_mode: upsert, key: [id] } }
  state:  { type: memory, config: {} }
replication:
  mode: snapshot_then_cdc
  snapshot:
    source: { type: postgres, config: { connection_url: "postgres://x", query: "SELECT * FROM t" } }
"#,
        );
        let err = run(args(path)).await.unwrap_err();
        assert!(
            format!("{err}").contains("durable state"),
            "should reject memory state at compile time: {err}"
        );
    }

    /// #670: `faucet mirror` is the command, `faucet replicate` its alias, and
    /// the block parses under both `mirror:` and `replication:`.
    #[test]
    fn mirror_is_the_name_and_replicate_the_alias() {
        use clap::Parser as _;
        for verb in ["mirror", "replicate"] {
            let cli = crate::cli::Cli::try_parse_from(["faucet", verb, "x.yaml"]).unwrap();
            assert!(
                matches!(cli.command, crate::cli::Command::Replicate(_)),
                "{verb}"
            );
        }
        for target in ["mirror", "replication"] {
            let cli = crate::cli::Cli::try_parse_from(["faucet", "schema", target]).unwrap();
            assert!(
                matches!(
                    cli.command,
                    crate::cli::Command::Schema(crate::cli::SchemaArgs {
                        target: Some(crate::cli::SchemaTarget::Replication),
                        ..
                    })
                ),
                "{target}"
            );
        }
        let body = |key: &str| {
            format!(
                "version: 1\nname: m\npipeline:\n  source: {{ type: rest, config: {{ base_url: https://a }} }}\n  sink: {{ type: stdout, config: {{}} }}\n{key}:\n  mode: snapshot_then_cdc\n  snapshot:\n    source: {{ type: rest, config: {{ base_url: https://b }} }}\n"
            )
        };
        for key in ["mirror", "replication"] {
            let cfg = crate::config::parse_with_extension(&body(key), "yaml").unwrap();
            assert!(cfg.replication.is_some(), "{key}");
            let out = serde_json::to_value(&cfg).unwrap();
            assert!(out.get("mirror").is_some() && out.get("replication").is_none());
        }
    }
}
