//! Validated form of a [`TriggersFile`]. `compile` surfaces every problem at
//! startup (unique names, webhook-path collisions, resolvable pipeline ref,
//! interval/threshold bounds, missing backend feature) so a watcher never fails
//! mid-run from a config mistake. Pure (no IO except reading a path's existence,
//! which is done by the caller; here we only validate shapes).
//!
//! Name charset: trigger names must match `^[A-Za-z0-9_-]+$`; they are embedded
//! verbatim into the webhook route `/v1/triggers/{name}`, so whitespace or slashes
//! would silently break routing.

use super::spec::{PipelineRef, QueueSpec, StoreSpec, TriggerKind, TriggerSpec, TriggersFile};
use std::collections::HashSet;

#[derive(Debug)]
pub struct CompiledTriggers {
    pub triggers: Vec<CompiledTrigger>,
}

#[derive(Debug, Clone)]
pub struct CompiledTrigger {
    pub spec: TriggerSpec,
    /// For webhook triggers: the route path `/v1/triggers/{name}`.
    pub webhook_path: Option<String>,
}

impl CompiledTrigger {
    pub fn name(&self) -> &str {
        &self.spec.name
    }
    pub fn kind_label(&self) -> &'static str {
        match self.spec.kind {
            TriggerKind::ObjectArrival { .. } => "object_arrival",
            TriggerKind::Webhook { .. } => "webhook",
            TriggerKind::QueueDepth { .. } => "queue_depth",
            TriggerKind::Schedule { .. } => "schedule",
        }
    }
}

impl CompiledTriggers {
    /// Validate a parsed file. `err` strings are user-facing.
    pub fn compile(file: TriggersFile) -> Result<Self, String> {
        if file.version != 1 {
            return Err(format!(
                "triggers: unsupported version {} (expected 1)",
                file.version
            ));
        }
        let mut names = HashSet::new();
        let mut compiled = Vec::with_capacity(file.triggers.len());

        for t in file.triggers {
            if t.name.trim().is_empty() {
                return Err("triggers: a trigger has an empty `name`".into());
            }
            if !t
                .name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
            {
                return Err(format!(
                    "triggers: invalid trigger name '{}' (letters, digits, '_' and '-' only)",
                    t.name
                ));
            }
            if !names.insert(t.name.clone()) {
                return Err(format!("triggers: duplicate trigger name '{}'", t.name));
            }
            // Exactly one of a pipeline ref and a template; the ref non-empty.
            match (&t.config, &t.template) {
                (Some(_), Some(_)) => {
                    return Err(format!(
                        "triggers: '{}' sets both `config` and `template`; pick one",
                        t.name
                    ));
                }
                (None, None) => {
                    return Err(format!(
                        "triggers: '{}' needs a `config` or a `template`",
                        t.name
                    ));
                }
                (Some(PipelineRef::Path(p)), None) if p.trim().is_empty() => {
                    return Err(format!("triggers: '{}' has an empty config path", t.name));
                }
                (Some(PipelineRef::Inline(v)), None) if !v.is_object() => {
                    return Err(format!(
                        "triggers: '{}' inline config must be a mapping",
                        t.name
                    ));
                }
                (None, Some(tpl)) => {
                    require_feature(&t.name, "templates")?;
                    if tpl.id.trim().is_empty() {
                        return Err(format!("triggers: '{}' template id is empty", t.name));
                    }
                }
                _ => {}
            }
            if let Some(sel) = &t.tenants {
                require_feature(&t.name, "tenants")?;
                sel.validate()
                    .map_err(|e| format!("triggers: '{}' {e}", t.name))?;
            }

            let webhook_path = match &t.kind {
                TriggerKind::ObjectArrival {
                    poll_interval_secs,
                    store,
                    ..
                } => {
                    if *poll_interval_secs == 0 {
                        return Err(format!(
                            "triggers: '{}' poll_interval_secs must be >= 1",
                            t.name
                        ));
                    }
                    require_feature(&t.name, store_feature(store))?;
                    validate_store(&t.name, store)?;
                    None
                }
                TriggerKind::Webhook { methods, .. } => {
                    if methods.is_empty() {
                        return Err(format!("triggers: '{}' methods must not be empty", t.name));
                    }
                    for m in methods {
                        let mu = m.to_ascii_uppercase();
                        if mu != "POST" && mu != "PUT" {
                            return Err(format!(
                                "triggers: '{}' unsupported webhook method '{}' (POST|PUT)",
                                t.name, m
                            ));
                        }
                    }
                    // Paths are unique because trigger names are already deduplicated above.
                    let path = format!("/v1/triggers/{}", t.name);
                    Some(path)
                }
                TriggerKind::QueueDepth {
                    poll_interval_secs,
                    queue,
                    threshold,
                } => {
                    if *poll_interval_secs == 0 {
                        return Err(format!(
                            "triggers: '{}' poll_interval_secs must be >= 1",
                            t.name
                        ));
                    }
                    if *threshold == 0 {
                        return Err(format!("triggers: '{}' threshold must be >= 1", t.name));
                    }
                    require_feature(&t.name, queue_feature(queue))?;
                    validate_queue(&t.name, queue)?;
                    None
                }
                TriggerKind::Schedule { cron, timezone } => {
                    require_feature(&t.name, "schedule")?;
                    validate_schedule(&t.name, cron, timezone)?;
                    None
                }
            };

            compiled.push(CompiledTrigger {
                spec: t,
                webhook_path,
            });
        }
        Ok(Self { triggers: compiled })
    }

    pub fn webhooks(&self) -> impl Iterator<Item = &CompiledTrigger> {
        self.triggers.iter().filter(|t| t.webhook_path.is_some())
    }
}

fn store_feature(store: &StoreSpec) -> &'static str {
    match store {
        StoreSpec::S3 { .. } | StoreSpec::Gcs { .. } => "triggers-object-store",
    }
}

fn queue_feature(queue: &QueueSpec) -> &'static str {
    match queue {
        QueueSpec::Redis { .. } => "triggers-redis",
        QueueSpec::Kafka { .. } => "triggers-kafka",
    }
}

/// Validates that non-optional string fields in a `StoreSpec` are non-empty.
fn validate_store(name: &str, store: &StoreSpec) -> Result<(), String> {
    match store {
        StoreSpec::S3 { bucket, .. } => {
            if bucket.trim().is_empty() {
                return Err(format!("triggers: '{name}' store bucket must not be empty"));
            }
        }
        StoreSpec::Gcs { bucket, .. } => {
            if bucket.trim().is_empty() {
                return Err(format!("triggers: '{name}' store bucket must not be empty"));
            }
        }
    }
    Ok(())
}

/// Validates that non-optional string fields in a `QueueSpec` are non-empty.
fn validate_queue(name: &str, queue: &QueueSpec) -> Result<(), String> {
    match queue {
        QueueSpec::Redis { url, key, .. } => {
            if url.trim().is_empty() {
                return Err(format!("triggers: '{name}' queue url must not be empty"));
            }
            if key.trim().is_empty() {
                return Err(format!("triggers: '{name}' queue key must not be empty"));
            }
        }
        QueueSpec::Kafka {
            brokers,
            topic,
            group,
        } => {
            if brokers.trim().is_empty() {
                return Err(format!(
                    "triggers: '{name}' queue brokers must not be empty"
                ));
            }
            if topic.trim().is_empty() {
                return Err(format!("triggers: '{name}' queue topic must not be empty"));
            }
            if group.trim().is_empty() {
                return Err(format!("triggers: '{name}' queue group must not be empty"));
            }
        }
    }
    Ok(())
}

/// Returns Ok if the named feature is compiled in; else a clear error naming it.
/// Compile a `schedule` trigger's cron + timezone the way `faucet schedule`
/// does, so both runtimes accept exactly the same expressions.
#[cfg(feature = "schedule")]
pub fn compile_schedule(
    trigger: &str,
    cron: &str,
    timezone: &str,
) -> Result<crate::schedule::compiled::CompiledSchedule, String> {
    let spec: crate::schedule::spec::ScheduleSpec =
        serde_json::from_value(serde_json::json!({ "cron": cron, "timezone": timezone }))
            .map_err(|e| format!("triggers: '{trigger}' {e}"))?;
    crate::schedule::compiled::CompiledSchedule::compile(&spec)
        .map_err(|e| format!("triggers: '{trigger}' {e}"))
}

#[cfg(feature = "schedule")]
fn validate_schedule(trigger: &str, cron: &str, timezone: &str) -> Result<(), String> {
    compile_schedule(trigger, cron, timezone).map(|_| ())
}

#[cfg(not(feature = "schedule"))]
fn validate_schedule(_trigger: &str, _cron: &str, _timezone: &str) -> Result<(), String> {
    Ok(())
}

fn require_feature(trigger: &str, feature: &str) -> Result<(), String> {
    let compiled = match feature {
        "triggers" => cfg!(feature = "triggers"),
        "triggers-object-store" => cfg!(feature = "triggers-object-store"),
        "triggers-redis" => cfg!(feature = "triggers-redis"),
        "triggers-kafka" => cfg!(feature = "triggers-kafka"),
        "schedule" => cfg!(feature = "schedule"),
        "templates" => cfg!(feature = "templates"),
        "tenants" => cfg!(feature = "tenants"),
        _ => true,
    };
    if compiled {
        Ok(())
    } else {
        Err(format!(
            "triggers: '{trigger}' requires a build with the `{feature}` feature"
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file(yaml: &str) -> TriggersFile {
        serde_yaml::from_str(yaml).unwrap()
    }

    #[test]
    fn rejects_duplicate_names() {
        let f = file(
            "version: 1\ntriggers:\n  - { name: a, type: webhook, config: ./x.yaml }\n  - { name: a, type: webhook, config: ./y.yaml }\n",
        );
        let err = CompiledTriggers::compile(f).unwrap_err();
        assert!(err.contains("duplicate trigger name 'a'"), "{err}");
    }

    #[test]
    fn rejects_bad_version() {
        let f = file("version: 2\ntriggers: []\n");
        assert!(
            CompiledTriggers::compile(f)
                .unwrap_err()
                .contains("version")
        );
    }

    #[test]
    fn webhook_gets_path_and_collisions_rejected() {
        // Two webhooks with distinct names → distinct paths, both compile.
        let f = file(
            "version: 1\ntriggers:\n  - { name: a, type: webhook, config: ./x.yaml }\n  - { name: b, type: webhook, config: ./y.yaml }\n",
        );
        let c = CompiledTriggers::compile(f).unwrap();
        assert_eq!(c.webhooks().count(), 2);
        assert_eq!(
            c.triggers[0].webhook_path.as_deref(),
            Some("/v1/triggers/a")
        );
        assert_eq!(
            c.triggers[1].webhook_path.as_deref(),
            Some("/v1/triggers/b")
        );
    }

    #[test]
    fn rejects_zero_threshold() {
        let f = file(
            "version: 1\ntriggers:\n  - name: q\n    type: queue_depth\n    config: ./x.yaml\n    threshold: 0\n    queue: { type: redis, url: \"redis://x\", key: k }\n",
        );
        // Only assert the threshold check when the redis backend is compiled;
        // otherwise the missing-feature error fires first (also acceptable).
        let err = CompiledTriggers::compile(f).unwrap_err();
        assert!(
            err.contains("threshold must be >= 1") || err.contains("triggers-redis"),
            "{err}"
        );
    }

    #[test]
    fn rejects_empty_webhook_methods() {
        let f = file(
            "version: 1\ntriggers:\n  - name: hook\n    type: webhook\n    config: ./x.yaml\n    methods: []\n",
        );
        let err = CompiledTriggers::compile(f).unwrap_err();
        assert!(err.contains("methods must not be empty"), "{err}");
    }

    #[test]
    fn rejects_empty_bucket() {
        let f = file(
            "version: 1\ntriggers:\n  - name: obj\n    type: object_arrival\n    config: ./x.yaml\n    store: { type: s3, bucket: \"\" }\n",
        );
        let err = CompiledTriggers::compile(f).unwrap_err();
        assert!(
            err.contains("store bucket must not be empty") || err.contains("triggers-object-store"),
            "{err}"
        );
    }

    #[test]
    fn rejects_empty_redis_url() {
        let f = file(
            "version: 1\ntriggers:\n  - name: q\n    type: queue_depth\n    config: ./x.yaml\n    queue: { type: redis, url: \"\", key: k }\n",
        );
        let err = CompiledTriggers::compile(f).unwrap_err();
        assert!(
            err.contains("queue url must not be empty") || err.contains("triggers-redis"),
            "{err}"
        );
    }

    #[test]
    fn rejects_name_with_slash() {
        let f = file(
            "version: 1\ntriggers:\n  - { name: \"foo/bar\", type: webhook, config: ./x.yaml }\n",
        );
        let err = CompiledTriggers::compile(f).unwrap_err();
        assert!(err.contains("invalid trigger name 'foo/bar'"), "{err}");
    }
}
