//! User-facing `lineage:` configuration types.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;

/// Top-level `lineage:` block.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct LineageConfig {
    /// Lineage format. Only `openlineage` in v1.
    #[serde(rename = "type", default)]
    pub kind: LineageKind,
    /// OpenLineage namespace for the emitted job/datasets.
    pub namespace: String,
    /// Where events are sent.
    pub transport: Transport,
    /// Job-name template. `${name}` / `${row_id}` / `${now.*}` are resolved
    /// per matrix row at run time.
    #[serde(default = "default_job_name")]
    pub job_name: String,
    /// Optional parent job (orchestrator linkage).
    #[serde(default)]
    pub parent_job: Option<ParentJob>,
    /// Emit column-level lineage facets where the transform chain is mappable.
    #[serde(default)]
    pub include_column_lineage: bool,
    /// Emit dataset schema facets (inferred from a record sample).
    #[serde(default)]
    pub include_schema_facet: bool,
    /// Emit the resolved config body as a SourceCode facet. Off by default —
    /// the resolved config may contain secrets; enabling warns.
    #[serde(default)]
    pub include_source_code_facet: bool,
    /// Which lifecycle events to emit.
    #[serde(default)]
    pub emit_on: EmitOn,
    /// Max records sampled for schema/column facets. Default 100.
    #[serde(default = "default_sample")]
    pub sample_records: usize,
    /// RUNNING heartbeat interval. Default 30s; only used when `emit_on.running`.
    #[serde(
        with = "faucet_core::config::duration_secs",
        default = "default_heartbeat"
    )]
    #[schemars(with = "u64")]
    pub heartbeat_interval: Duration,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum LineageKind {
    #[default]
    Openlineage,
}

/// Lineage event transport.
///
/// Uses the project-wide `{ type, config: { … } }` shape (adjacently tagged),
/// matching connectors and the shared auth catalog. `deny_unknown_fields` is
/// intentionally omitted — serde does not honor it on tagged enums (same as
/// `faucet-core` auth, websocket, grpc).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", content = "config", rename_all = "snake_case")]
pub enum Transport {
    /// POST each event to an OpenLineage HTTP endpoint (e.g. Marquez).
    Http {
        url: String,
        #[serde(
            with = "faucet_core::config::duration_secs",
            default = "default_http_timeout"
        )]
        #[schemars(with = "u64")]
        timeout_secs: Duration,
        #[serde(default)]
        auth: Option<HttpAuth>,
    },
    /// Append each event as one JSON line to a local file.
    File { path: PathBuf },
    /// Produce each event as a JSON message to a Kafka topic.
    #[cfg(feature = "transport-kafka")]
    Kafka { brokers: String, topic: String },
}

/// HTTP transport auth. Same `{ type, config: { … } }` shape as connector auth
/// (e.g. `{ type: bearer, config: { token: … } }`).
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", content = "config", rename_all = "snake_case")]
pub enum HttpAuth {
    Bearer { token: String },
}

impl std::fmt::Debug for HttpAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        faucet_core::util::fmt_redacted(f, "HttpAuth", self, &["token"])
    }
}

/// Parent-job linkage (Airflow, Dagster, …).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ParentJob {
    pub namespace: String,
    pub name: String,
    /// The parent's run id. Without it no parent facet is emitted.
    #[serde(default)]
    pub run_id: Option<String>,
}

/// Per-event emit toggles.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EmitOn {
    #[serde(default = "default_true")]
    pub start: bool,
    #[serde(default)]
    pub running: bool,
    #[serde(default = "default_true")]
    pub complete: bool,
    #[serde(default = "default_true")]
    pub fail: bool,
    #[serde(default = "default_true")]
    pub abort: bool,
}

impl Default for EmitOn {
    fn default() -> Self {
        Self {
            start: true,
            running: false,
            complete: true,
            fail: true,
            abort: true,
        }
    }
}

fn default_true() -> bool {
    true
}
fn default_job_name() -> String {
    "${name}::${row_id}".to_string()
}
fn default_sample() -> usize {
    100
}
fn default_heartbeat() -> Duration {
    Duration::from_secs(30)
}
fn default_http_timeout() -> Duration {
    Duration::from_secs(10)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserializes_http_transport_with_defaults() {
        let json = serde_json::json!({
            "type": "openlineage",
            "namespace": "prod.warehouse",
            "transport": { "type": "http", "config": { "url": "https://marquez/api/v1/lineage" } }
        });
        let cfg: LineageConfig = serde_json::from_value(json).unwrap();
        assert_eq!(cfg.namespace, "prod.warehouse");
        assert_eq!(cfg.job_name, "${name}::${row_id}");
        assert!(cfg.emit_on.start && cfg.emit_on.complete && !cfg.emit_on.running);
        assert_eq!(cfg.sample_records, 100);
        assert!(matches!(cfg.transport, Transport::Http { .. }));
    }

    #[test]
    fn deserializes_file_transport() {
        let json = serde_json::json!({
            "namespace": "n",
            "transport": { "type": "file", "config": { "path": "/tmp/ol.jsonl" } }
        });
        let cfg: LineageConfig = serde_json::from_value(json).unwrap();
        assert!(matches!(cfg.transport, Transport::File { .. }));
    }

    #[test]
    fn deserializes_http_transport_with_bearer_auth() {
        // The nested auth uses the same { type, config } shape as connector auth.
        let json = serde_json::json!({
            "namespace": "n",
            "transport": {
                "type": "http",
                "config": {
                    "url": "https://marquez/api/v1/lineage",
                    "auth": { "type": "bearer", "config": { "token": "secret" } }
                }
            }
        });
        let cfg: LineageConfig = serde_json::from_value(json).unwrap();
        match cfg.transport {
            Transport::Http {
                auth: Some(HttpAuth::Bearer { token }),
                ..
            } => {
                assert_eq!(token, "secret");
            }
            _ => panic!("expected http transport with bearer auth"),
        }
    }

    #[test]
    fn rejects_unknown_field() {
        let json = serde_json::json!({
            "namespace": "n",
            "transport": { "type": "file", "config": { "path": "/tmp/ol.jsonl" } },
            "bogus": true
        });
        assert!(serde_json::from_value::<LineageConfig>(json).is_err());
    }

    #[test]
    fn schema_generates() {
        let _ = schemars::schema_for!(LineageConfig);
    }
}

#[cfg(test)]
mod debug_redaction_tests {
    use super::*;

    #[test]
    fn debug_never_prints_secrets() {
        let v: HttpAuth =
            serde_json::from_str(r#"{"type":"bearer","config":{"token":"S3CRET-1"}}"#).unwrap();
        let dbg = format!("{v:?}");
        assert!(!dbg.contains("S3CRET") && dbg.contains("***"), "{dbg}");
    }
}
