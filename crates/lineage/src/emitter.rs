//! Builds OpenLineage events from a `RunLifecycle` and dispatches them via the
//! configured transport. Emission failures NEVER fail the pipeline run — they
//! are logged and counted, then dropped.

use crate::column::ColumnLineage;
use crate::config::{LineageConfig, Transport};
use crate::event::*;
use crate::lifecycle::{InferredSchema, RunLifecycle};
use crate::transport::{Transport as TransportTrait, file::FileTransport, http::HttpTransport};
use faucet_core::{FaucetError, redact_uri_credentials};
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
        if let Some(p) = &cfg.parent_job
            && p.run_id.is_none()
        {
            tracing::warn!(
                parent = %p.name,
                "lineage.parent_job has no run_id — OpenLineage's parent facet needs the \
                 parent's run id, so no parent facet is emitted"
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
        let parent = ctx.parent.as_ref().and_then(|p| {
            let run_id = p.run_id.clone()?;
            Some(ParentRunFacet {
                producer: PRODUCER.into(),
                schema_url: OL_SCHEMA_URL.into(),
                run: ParentRunRef { run_id },
                job: ParentJobRef {
                    namespace: p.namespace.clone(),
                    name: p.name.clone(),
                },
            })
        });
        let error_message = ctx
            .error
            .as_deref()
            .filter(|_| matches!(ev, EventType::Fail | EventType::Abort))
            .map(|e| ErrorMessageRunFacet::new(faucet_core::redact::redact(e)));
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
                let mut ds = Dataset::new(r.namespace.clone(), redact_uri_credentials(&r.name));
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
        let mut output = Dataset::new(
            ctx.output.namespace.clone(),
            redact_uri_credentials(&ctx.output.name),
        );
        if terminal
            && self.cfg.include_schema_facet
            && let Some(s) = &ctx.output_schema
        {
            output.facets.schema = Some(schema_facet(s));
        }
        if terminal || matches!(ev, EventType::Running) {
            output.output_facets.output_statistics = Some(OutputStatisticsFacet::new(ctx.records));
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
            output.facets.column_lineage = Some(column_facet(
                cl,
                &only.namespace,
                &redact_uri_credentials(&only.name),
            ));
        }

        RunEvent {
            event_type: ev,
            event_time: ctx.finished_at.unwrap_or(ctx.started_at).to_rfc3339(),
            run: Run {
                run_id: ctx.run_id.clone(),
                facets: RunFacets {
                    parent,
                    nominal_time,
                    error_message,
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

/// A running RUNNING-heartbeat task. Dropping the guard stops the task, so a
/// run whose future is dropped (timeout, cancellation, panic) stops emitting.
#[derive(Debug)]
pub struct HeartbeatGuard(tokio::task::JoinHandle<()>);

impl HeartbeatGuard {
    /// Whether the heartbeat task has stopped.
    pub fn is_finished(&self) -> bool {
        self.0.is_finished()
    }
}

impl Drop for HeartbeatGuard {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl LineageEmitter {
    /// Emit a RUNNING event every `interval` (skipping the first, immediate
    /// tick) until the returned guard is dropped. `records` is read before each
    /// beat to refresh the record count.
    pub fn spawn_heartbeat(
        self: &Arc<Self>,
        interval: std::time::Duration,
        mut ctx: RunLifecycle,
        records: impl Fn() -> Option<u64> + Send + 'static,
    ) -> HeartbeatGuard {
        let em = Arc::clone(self);
        HeartbeatGuard(tokio::spawn(async move {
            let mut tick = tokio::time::interval(interval);
            tick.tick().await;
            loop {
                tick.tick().await;
                if let Some(n) = records() {
                    ctx.records = n;
                }
                em.emit(EventType::Running, &ctx).await;
            }
        }))
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
    async fn dataset_names_never_carry_credentials() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ol.jsonl");
        let mut c = cfg(path.clone());
        c.include_column_lineage = true;
        let em = LineageEmitter::new(c).unwrap();
        let mut ctx = lifecycle();
        ctx.inputs[0].name = "nats://user:s3cret@broker:4222/orders".into();
        ctx.output.name = "https://h/ingest?token=abc".into();
        ctx.column_lineage = crate::column::derive(&["a".to_string()], &[]);
        ctx.finished_at = Some(Utc::now());
        em.emit(EventType::Complete, &ctx).await;
        let body = std::fs::read_to_string(&path).unwrap();
        assert!(!body.contains("s3cret"), "{body}");
        assert!(!body.contains("abc"), "{body}");
        let v: serde_json::Value = serde_json::from_str(body.lines().next().unwrap()).unwrap();
        assert_eq!(v["inputs"][0]["name"], "nats://broker:4222/orders");
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

    async fn emitted(
        c: LineageConfig,
        path: &std::path::Path,
        ev: EventType,
        ctx: &RunLifecycle,
    ) -> serde_json::Value {
        LineageEmitter::new(c).unwrap().emit(ev, ctx).await;
        let body = std::fs::read_to_string(path).unwrap();
        serde_json::from_str(body.lines().last().unwrap()).unwrap()
    }

    #[tokio::test]
    async fn a_parent_without_run_id_emits_no_parent_facet() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ol.jsonl");
        let parent = crate::config::ParentJob {
            namespace: "airflow".into(),
            name: "dag".into(),
            run_id: None,
        };
        let mut c = cfg(path.clone());
        c.parent_job = Some(parent.clone());
        let mut ctx = lifecycle();
        ctx.parent = Some(parent.clone());
        let v = emitted(c.clone(), &path, EventType::Start, &ctx).await;
        assert!(v["run"]["facets"].get("parent").is_none(), "{v}");

        let mut with_id = parent;
        with_id.run_id = Some("p-1".into());
        ctx.parent = Some(with_id);
        let v = emitted(c, &path, EventType::Start, &ctx).await;
        assert_eq!(v["run"]["facets"]["parent"]["run"]["runId"], "p-1");
    }

    #[tokio::test]
    async fn failure_and_volume_reach_the_events() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ol.jsonl");
        let mut ctx = lifecycle();
        ctx.records = 42;
        ctx.error = Some("sink refused the page".into());
        ctx.finished_at = Some(Utc::now());

        let v = emitted(cfg(path.clone()), &path, EventType::Fail, &ctx).await;
        let facet = &v["run"]["facets"]["errorMessage"];
        assert_eq!(facet["message"], "sink refused the page");
        assert_eq!(facet["programmingLanguage"], "rust");
        assert_eq!(
            v["outputs"][0]["outputFacets"]["outputStatistics"]["rowCount"],
            42
        );

        ctx.error = None;
        let v = emitted(cfg(path.clone()), &path, EventType::Complete, &ctx).await;
        assert!(v["run"]["facets"].get("errorMessage").is_none(), "{v}");
        assert_eq!(
            v["outputs"][0]["outputFacets"]["outputStatistics"]["rowCount"],
            42
        );

        let mut running = cfg(path.clone());
        running.emit_on.running = true;
        let v = emitted(running, &path, EventType::Running, &ctx).await;
        assert_eq!(
            v["outputs"][0]["outputFacets"]["outputStatistics"]["rowCount"],
            42
        );
        let v = emitted(cfg(path.clone()), &path, EventType::Start, &ctx).await;
        assert!(v["outputs"][0].get("outputFacets").is_none(), "{v}");
    }

    #[tokio::test]
    async fn dropping_the_heartbeat_guard_stops_the_beats() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ol.jsonl");
        let mut c = cfg(path.clone());
        c.emit_on.running = true;
        let em = LineageEmitter::new(c).unwrap();
        let guard = em.spawn_heartbeat(std::time::Duration::from_millis(10), lifecycle(), || {
            Some(7)
        });
        let lines = |p: &std::path::Path| {
            std::fs::read_to_string(p)
                .map(|b| b.lines().count())
                .unwrap_or(0)
        };
        for _ in 0..200 {
            if lines(&path) >= 2 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let body = std::fs::read_to_string(&path).unwrap();
        let v: serde_json::Value = serde_json::from_str(body.lines().next().unwrap()).unwrap();
        assert_eq!(v["eventType"], "RUNNING");
        assert_eq!(
            v["outputs"][0]["outputFacets"]["outputStatistics"]["rowCount"],
            7
        );

        drop(guard);
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        let after_drop = lines(&path);
        tokio::time::sleep(std::time::Duration::from_millis(80)).await;
        assert_eq!(lines(&path), after_drop, "beats continued after drop");
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
