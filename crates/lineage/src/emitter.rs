//! Builds OpenLineage events from a `RunLifecycle` and dispatches them via the
//! configured transport. Emission failures NEVER fail the pipeline run — they
//! are logged and counted, then dropped.

use crate::column::ColumnLineage;
use crate::config::{LineageConfig, Transport};
use crate::event::*;
use crate::lifecycle::{InferredSchema, RunLifecycle};
use crate::transport::{Transport as TransportTrait, file::FileTransport, http::HttpTransport};
use faucet_core::FaucetError;
use metrics::{counter, histogram};
use std::sync::Arc;

pub struct LineageEmitter {
    cfg: LineageConfig,
    transport: Arc<dyn TransportTrait>,
}

impl LineageEmitter {
    pub fn new(cfg: LineageConfig) -> Result<Arc<Self>, FaucetError> {
        if cfg.include_source_code_facet {
            tracing::warn!(
                "lineage.include_source_code_facet is enabled — the resolved config may \
                 contain secrets that will be emitted in the SourceCode facet"
            );
        }
        let transport: Arc<dyn TransportTrait> = match &cfg.transport {
            Transport::Http {
                url,
                timeout_secs,
                auth,
            } => Arc::new(HttpTransport::new(
                url.clone(),
                *timeout_secs,
                auth.clone(),
            )?),
            Transport::File { path } => Arc::new(FileTransport::new(path.clone())),
            #[cfg(feature = "transport-kafka")]
            Transport::Kafka { brokers, topic } => Arc::new(
                crate::transport::kafka::KafkaTransport::new(brokers, topic.clone())?,
            ),
        };
        Ok(Arc::new(Self { cfg, transport }))
    }

    fn enabled(&self, ev: EventType) -> bool {
        let e = &self.cfg.emit_on;
        match ev {
            EventType::Start => e.start,
            EventType::Running => e.running,
            EventType::Complete => e.complete,
            EventType::Abort => e.abort,
            EventType::Fail => e.fail,
        }
    }

    /// Emit one lifecycle event. Never returns an error — failures are logged
    /// and counted via `faucet_lineage_dropped_total`.
    pub async fn emit(&self, ev: EventType, ctx: &RunLifecycle) {
        if !self.enabled(ev) {
            counter!("faucet_lineage_dropped_total", "reason" => "disabled").increment(1);
            return;
        }
        let event = self.build(ev, ctx);
        let body = match serde_json::to_vec(&event) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(error = %e, "lineage event serialization failed; dropping");
                counter!("faucet_lineage_dropped_total", "reason" => "transport_error")
                    .increment(1);
                return;
            }
        };
        let label = event_label(ev);
        let start = std::time::Instant::now();
        let result = self.transport.send(body).await;
        histogram!("faucet_lineage_emit_duration_seconds", "event_type" => label)
            .record(start.elapsed().as_secs_f64());
        match result {
            Ok(()) => {
                counter!("faucet_lineage_events_total", "event_type" => label, "outcome" => "ok")
                    .increment(1);
            }
            Err(e) => {
                tracing::warn!(error = %e, event_type = label, "lineage emission failed; dropping");
                counter!("faucet_lineage_events_total", "event_type" => label, "outcome" => "err")
                    .increment(1);
                counter!("faucet_lineage_dropped_total", "reason" => "transport_error")
                    .increment(1);
            }
        }
    }

    fn build(&self, ev: EventType, ctx: &RunLifecycle) -> RunEvent {
        let terminal = matches!(ev, EventType::Complete | EventType::Abort | EventType::Fail);

        // Run facets.
        let parent = ctx.parent.as_ref().map(|p| ParentRunFacet {
            producer: PRODUCER.into(),
            schema_url: OL_SCHEMA_URL.into(),
            run: ParentRunRef {
                run_id: p.run_id.clone().unwrap_or_else(|| ctx.run_id.clone()),
            },
            job: ParentJobRef {
                namespace: p.namespace.clone(),
                name: p.name.clone(),
            },
        });
        let nominal_time = Some(NominalTimeRunFacet {
            producer: PRODUCER.into(),
            schema_url: OL_SCHEMA_URL.into(),
            nominal_start_time: ctx.started_at.to_rfc3339(),
            nominal_end_time: ctx.finished_at.map(|t| t.to_rfc3339()),
        });

        // Job facets.
        let source_code = ctx.source_code.as_ref().map(|src| SourceCodeJobFacet {
            producer: PRODUCER.into(),
            schema_url: OL_SCHEMA_URL.into(),
            language: "yaml".into(),
            source_code: src.clone(),
        });

        // Input datasets (+ schema on terminal events). Several when a topology
        // sink is fed by a merge or join (#459); `input_schemas` aligns
        // positionally and may be shorter than `inputs`.
        let inputs: Vec<Dataset> = ctx
            .inputs
            .iter()
            .enumerate()
            .map(|(i, r)| {
                let mut ds = Dataset::new(r.namespace.clone(), r.name.clone());
                if terminal
                    && self.cfg.include_schema_facet
                    && let Some(Some(s)) = ctx.input_schemas.get(i)
                {
                    ds.facets.schema = Some(schema_facet(s));
                }
                ds
            })
            .collect();

        // Output dataset (+ schema + column lineage on terminal events).
        let mut output = Dataset::new(ctx.output.namespace.clone(), ctx.output.name.clone());
        if terminal
            && self.cfg.include_schema_facet
            && let Some(s) = &ctx.output_schema
        {
            output.facets.schema = Some(schema_facet(s));
        }
        // Column lineage references a single input's fields, and the derivation
        // models one transform chain — so emit it only when there is exactly one
        // input. A merge/join is opaque to it, and inventing an input to point at
        // would be worse than omitting the facet (the same "never fabricate"
        // rule the opaque-transform list follows).
        if terminal
            && self.cfg.include_column_lineage
            && let Some(cl) = &ctx.column_lineage
            && let [only] = ctx.inputs.as_slice()
        {
            output.facets.column_lineage = Some(column_facet(cl, &only.namespace, &only.name));
        }

        RunEvent {
            event_type: ev,
            event_time: ctx.finished_at.unwrap_or(ctx.started_at).to_rfc3339(),
            run: Run {
                run_id: ctx.run_id.clone(),
                facets: RunFacets {
                    parent,
                    nominal_time,
                },
            },
            job: Job {
                namespace: ctx.job_namespace.clone(),
                name: ctx.job_name.clone(),
                facets: JobFacets { source_code },
            },
            inputs,
            outputs: vec![output],
            producer: PRODUCER.into(),
            schema_url: OL_SCHEMA_URL.into(),
        }
    }
}

fn schema_facet(s: &InferredSchema) -> SchemaDatasetFacet {
    SchemaDatasetFacet::new(
        s.fields
            .iter()
            .map(|(n, t)| SchemaField {
                name: n.clone(),
                type_: t.clone(),
            })
            .collect(),
    )
}

fn column_facet(cl: &ColumnLineage, in_ns: &str, in_name: &str) -> ColumnLineageDatasetFacet {
    let mut fields = std::collections::BTreeMap::new();
    for (out_field, sources) in &cl.edges {
        if sources.is_empty() {
            continue; // literal field: no upstream edge
        }
        fields.insert(
            out_field.clone(),
            ColumnLineageFieldEntry {
                input_fields: sources
                    .iter()
                    .map(|src| ColumnLineageInputField {
                        namespace: in_ns.to_string(),
                        name: in_name.to_string(),
                        field: src.clone(),
                    })
                    .collect(),
            },
        );
    }
    ColumnLineageDatasetFacet {
        producer: PRODUCER.into(),
        schema_url: OL_SCHEMA_URL.into(),
        fields,
    }
}

fn event_label(ev: EventType) -> &'static str {
    match ev {
        EventType::Start => "start",
        EventType::Running => "running",
        EventType::Complete => "complete",
        EventType::Abort => "abort",
        EventType::Fail => "fail",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{EmitOn, LineageConfig, Transport};
    use crate::event::EventType;
    use crate::lifecycle::{DatasetRef, RunLifecycle};
    use chrono::Utc;
    use std::path::PathBuf;

    fn cfg(path: PathBuf) -> LineageConfig {
        LineageConfig {
            kind: Default::default(),
            namespace: "ns".into(),
            transport: Transport::File { path },
            job_name: "j".into(),
            parent_job: None,
            include_column_lineage: false,
            include_schema_facet: false,
            include_source_code_facet: false,
            emit_on: EmitOn::default(),
            sample_records: 100,
            heartbeat_interval: std::time::Duration::from_secs(30),
        }
    }

    fn lifecycle() -> RunLifecycle {
        RunLifecycle {
            job_namespace: "ns".into(),
            job_name: "j".into(),
            run_id: "r1".into(),
            parent: None,
            inputs: vec![DatasetRef {
                namespace: "ns".into(),
                name: "postgres://h/db".into(),
            }],
            output: DatasetRef {
                namespace: "ns".into(),
                name: "bigquery://p.d.t".into(),
            },
            started_at: Utc::now(),
            finished_at: None,
            records: 0,
            error: None,
            input_schemas: Vec::new(),
            output_schema: None,
            column_lineage: None,
            source_code: None,
        }
    }

    #[tokio::test]
    async fn emits_start_to_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ol.jsonl");
        let em = LineageEmitter::new(cfg(path.clone())).unwrap();
        em.emit(EventType::Start, &lifecycle()).await;
        let body = std::fs::read_to_string(&path).unwrap();
        let v: serde_json::Value = serde_json::from_str(body.lines().next().unwrap()).unwrap();
        assert_eq!(v["eventType"], "START");
        assert_eq!(v["inputs"][0]["name"], "postgres://h/db");
    }

    #[tokio::test]
    async fn respects_emit_on_toggles() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ol.jsonl");
        let mut c = cfg(path.clone());
        c.emit_on.running = false;
        let em = LineageEmitter::new(c).unwrap();
        em.emit(EventType::Running, &lifecycle()).await; // disabled → nothing written
        assert!(!path.exists() || std::fs::read_to_string(&path).unwrap().is_empty());
    }

    #[tokio::test]
    async fn transport_error_never_panics() {
        // File transport pointed at an un-creatable path (parent is a file).
        let dir = tempfile::tempdir().unwrap();
        let blocker = dir.path().join("blocker");
        std::fs::write(&blocker, "x").unwrap();
        let path = blocker.join("ol.jsonl"); // parent is a regular file → mkdir fails
        let em = LineageEmitter::new(cfg(path)).unwrap();
        em.emit(EventType::Start, &lifecycle()).await; // must not panic / must return
    }
}
