//! Event-driven pipeline triggers for `faucet serve` (#196).
//!
//! A static `--triggers <file>` defines watchers (object-arrival / webhook /
//! queue-depth) that, on fire, enqueue a run via [`crate::serve::runner::submit`]
//! — reusing the whole queue/executor/idempotency pipeline. Pure decision logic
//! (spec validation, `${trigger.*}` substitution, cursors, edge detection) is
//! separated from the IO shell (watchers, fire path, webhook route).

pub mod compiled;
pub mod context;
pub mod enqueue;
pub mod health;
pub mod metrics;
pub mod spec;
pub mod watcher;
pub mod webhook;

#[cfg(feature = "triggers-object-store")]
pub mod object_arrival;
#[cfg(any(feature = "triggers-redis", feature = "triggers-kafka"))]
pub mod queue_depth;
#[cfg(feature = "schedule")]
pub mod schedule;

use crate::error::{CliError, CliResult};
use crate::serve::state::ServerState;
#[allow(unused_imports)]
use compiled::{CompiledTrigger, CompiledTriggers};
#[cfg(any(
    feature = "triggers-object-store",
    feature = "triggers-redis",
    feature = "triggers-kafka"
))]
use std::sync::Arc;
#[cfg(any(
    feature = "triggers-object-store",
    feature = "triggers-redis",
    feature = "triggers-kafka"
))]
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Load + validate a triggers file. Surfaces a clear `CliError::Serve` on any
/// parse/validation failure (fail-fast at startup).
///
/// Relative `config:` paths in the parsed triggers are resolved relative to the
/// triggers file's parent directory (not the process CWD), matching the
/// behaviour of `!include`/`extends` in pipeline configs.
pub async fn load_triggers(path: &std::path::Path) -> CliResult<CompiledTriggers> {
    let text = tokio::fs::read_to_string(path)
        .await
        .map_err(|e| CliError::Serve(format!("reading triggers file {}: {e}", path.display())))?;
    let is_json = path
        .extension()
        .map(|e| e.eq_ignore_ascii_case("json"))
        .unwrap_or(false);
    let mut file: spec::TriggersFile = if is_json {
        serde_json::from_str(&text)
            .map_err(|e| CliError::Serve(format!("parsing triggers JSON: {e}")))?
    } else {
        serde_yaml::from_str(&text)
            .map_err(|e| CliError::Serve(format!("parsing triggers YAML: {e}")))?
    };

    // Reject unknown fields on a trigger entry. `TriggerSpec` cannot use
    // `deny_unknown_fields` because it carries a `#[serde(flatten)]` kind, so a
    // typo like `debounce_sec` (for `debounce_secs`) would otherwise be silently
    // dropped (#232). Diff the raw document against the typed re-serialization.
    let raw: serde_json::Value = if is_json {
        serde_json::from_str(&text)
            .map_err(|e| CliError::Serve(format!("parsing triggers JSON: {e}")))?
    } else {
        serde_yaml::from_str(&text)
            .map_err(|e| CliError::Serve(format!("parsing triggers YAML: {e}")))?
    };
    if let Some((trigger, field)) = spec::unknown_trigger_fields(&raw, &file).into_iter().next() {
        return Err(CliError::Serve(format!(
            "triggers: unknown field `{field}` in trigger `{trigger}` \
             (check for a typo; run `faucet schema triggers` for the valid fields)"
        )));
    }

    // Resolve relative `config: <path>` entries relative to the triggers file's
    // directory. This makes `config: ../pipelines/load.yaml` work regardless of
    // the process CWD, consistent with `!include`/`extends` path semantics.
    if let Some(base_dir) = path.parent() {
        for trigger in &mut file.triggers {
            if let Some(spec::PipelineRef::Path(ref p)) = trigger.config {
                let p_path = std::path::Path::new(p);
                if p_path.is_relative() {
                    let resolved = base_dir.join(p_path);
                    trigger.config = Some(spec::PipelineRef::Path(
                        resolved.to_string_lossy().into_owned(),
                    ));
                }
            }
        }
    }

    CompiledTriggers::compile(file).map_err(CliError::Serve)
}

/// Spawn supervised watcher tasks for every enabled polling trigger. Webhook
/// triggers need no task (they are served by the route). Returns the join handles
/// (the caller aborts them on shutdown, like the maintenance/lease loops).
pub fn spawn_watchers(
    state: ServerState,
    compiled: &CompiledTriggers,
    #[cfg_attr(
        not(any(
            feature = "triggers-object-store",
            feature = "triggers-redis",
            feature = "triggers-kafka",
            feature = "schedule"
        )),
        allow(unused_variables)
    )]
    shutdown: CancellationToken,
) -> Vec<JoinHandle<()>> {
    #[cfg_attr(
        not(any(
            feature = "triggers-object-store",
            feature = "triggers-redis",
            feature = "triggers-kafka",
            feature = "schedule"
        )),
        allow(unused_mut)
    )]
    let mut handles = Vec::new();
    #[cfg_attr(
        not(any(
            feature = "triggers-object-store",
            feature = "triggers-redis",
            feature = "triggers-kafka",
            feature = "schedule"
        )),
        allow(unused_variables)
    )]
    let health = state.triggers().clone();
    let mut active = 0usize;
    for t in &compiled.triggers {
        if !t.spec.enabled {
            tracing::info!(trigger = t.name(), "trigger disabled; not spawning");
            continue;
        }
        // Pre-emit this trigger's per-trigger series at zero so they exist in
        // `/metrics` from startup (including webhooks, which spawn no task).
        metrics::preinit(t.name(), t.kind_label());
        match &t.spec.kind {
            spec::TriggerKind::Webhook { .. } => {
                active += 1; // served by the route, no task
                tracing::info!(trigger = t.name(), path = ?t.webhook_path, "webhook trigger registered");
            }
            #[cfg(feature = "triggers-object-store")]
            spec::TriggerKind::ObjectArrival {
                store,
                poll_interval_secs,
                mode,
                start_at,
            } => match object_arrival::ObjectArrivalWatcher::build_store(store) {
                Ok((s, bucket, prefix)) => {
                    let w = object_arrival::ObjectArrivalWatcher::new(
                        Arc::new(t.clone()),
                        s,
                        bucket,
                        prefix,
                        *mode,
                        Duration::from_secs(*poll_interval_secs),
                        *start_at,
                        chrono::Utc::now(),
                    );
                    handles.push(tokio::spawn(watcher::run_supervised(
                        w,
                        state.clone(),
                        health.clone(),
                        shutdown.clone(),
                    )));
                    active += 1;
                }
                Err(e) => {
                    tracing::error!(trigger = t.name(), error = %e, "failed to build object store; skipping watcher")
                }
            },
            #[cfg(any(feature = "triggers-redis", feature = "triggers-kafka"))]
            spec::TriggerKind::QueueDepth {
                queue,
                threshold,
                poll_interval_secs,
            } => match queue_depth::build_probe(queue) {
                Ok(probe) => {
                    let w = queue_depth::QueueDepthWatcher::new(
                        Arc::new(t.clone()),
                        probe,
                        *threshold,
                        Duration::from_secs(*poll_interval_secs),
                    );
                    handles.push(tokio::spawn(watcher::run_supervised(
                        w,
                        state.clone(),
                        health.clone(),
                        shutdown.clone(),
                    )));
                    active += 1;
                }
                Err(e) => {
                    tracing::error!(trigger = t.name(), error = %e, "failed to build queue probe; skipping watcher")
                }
            },
            // Backends not compiled in were already rejected by `compile`, but the
            // match must be exhaustive when their features are off.
            #[cfg(not(feature = "triggers-object-store"))]
            spec::TriggerKind::ObjectArrival { .. } => {}
            #[cfg(not(any(feature = "triggers-redis", feature = "triggers-kafka")))]
            spec::TriggerKind::QueueDepth { .. } => {}
            #[cfg(feature = "schedule")]
            spec::TriggerKind::Schedule { cron, timezone } => {
                match compiled::compile_schedule(t.name(), cron, timezone) {
                    Ok(sched) => {
                        let w = schedule::ScheduleWatcher::new(
                            std::sync::Arc::new(t.clone()),
                            sched,
                            chrono::Utc::now(),
                        );
                        handles.push(tokio::spawn(watcher::run_supervised(
                            w,
                            state.clone(),
                            health.clone(),
                            shutdown.clone(),
                        )));
                        active += 1;
                    }
                    Err(e) => {
                        tracing::error!(trigger = t.name(), error = %e, "invalid schedule; skipping watcher")
                    }
                }
            }
            #[cfg(not(feature = "schedule"))]
            spec::TriggerKind::Schedule { .. } => {}
        }
    }
    metrics::active(active);
    handles
}

// Bring the compiled types into the public surface for `server.rs`.
pub use compiled::CompiledTriggers as Compiled;

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[tokio::test]
    async fn load_triggers_resolves_relative_config_path() {
        let dir = tempfile::tempdir().expect("tempdir");
        let base = dir.path();

        // Create a minimal pipeline file that `CompiledTriggers::compile` won't
        // reject for being absent (compile only checks the spec, not the file).
        let pipeline_path = base.join("inner.yaml");
        std::fs::write(
            &pipeline_path,
            "version: 1\npipeline:\n  source:\n    type: rest\n    config: {url: 'http://x'}\n  sink:\n    type: stdout\n    config: {}\n",
        )
        .unwrap();

        // Triggers file lives in the temp dir and references inner.yaml relatively.
        let triggers_path = base.join("triggers.yaml");
        let yaml = "version: 1\ntriggers:\n  - name: t1\n    type: webhook\n    config: ./inner.yaml\n    methods: [POST]\n".to_string();
        {
            let mut f = std::fs::File::create(&triggers_path).unwrap();
            f.write_all(yaml.as_bytes()).unwrap();
        }

        let compiled = load_triggers(&triggers_path)
            .await
            .expect("load_triggers failed");
        assert_eq!(compiled.triggers.len(), 1);

        // The compiled trigger's config path must be absolute (starts with base_dir).
        match &compiled.triggers[0].spec.config {
            Some(crate::serve::triggers::spec::PipelineRef::Path(p)) => {
                let abs = std::path::Path::new(p);
                assert!(abs.is_absolute(), "expected absolute path, got: {p}");
                assert!(
                    abs.starts_with(base),
                    "expected path under temp dir {}, got: {p}",
                    base.display()
                );
            }
            _ => panic!("expected PipelineRef::Path"),
        }
    }

    #[tokio::test]
    async fn load_triggers_rejects_unknown_field() {
        let dir = tempfile::tempdir().expect("tempdir");
        let triggers_path = dir.path().join("triggers.yaml");
        // `debounce_sec` is a typo for `debounce_secs` — a flatten-bearing
        // `TriggerSpec` would silently drop it without the explicit check.
        let yaml = "version: 1\ntriggers:\n  - name: hook\n    type: webhook\n    config: ./inner.yaml\n    methods: [POST]\n    debounce_sec: 5\n";
        std::fs::write(&triggers_path, yaml).unwrap();

        let err = load_triggers(&triggers_path)
            .await
            .expect_err("expected unknown-field rejection");
        let msg = format!("{err}");
        assert!(
            msg.contains("debounce_sec"),
            "error must name the field: {msg}"
        );
        assert!(msg.contains("hook"), "error must name the trigger: {msg}");
    }
}
