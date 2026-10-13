//! #872: a connector that panics on the columnar fast path fails only its own
//! matrix row. Under `on_error: continue` the other rows keep running, and the
//! failed row reports the panic as a typed error rather than a dead task.
//!
//! Its own test binary because it installs the process-global plugin registry.
#![cfg(feature = "arrow")]

use arrow::array::RecordBatch;
use faucet_cli::config::PipelineConfig;
use faucet_cli::executor::{ExecuteOptions, run_expanded};
use faucet_cli::expand::expand;
use faucet_cli::registry::PluginRegistry;
use faucet_core::columnar::{ColumnarPage, values_to_record_batch_inferred};
use faucet_core::{FaucetError, Sink, Source, Stream, async_trait};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Emits one two-row Arrow batch.
struct ColumnarSource;

#[async_trait]
impl Source for ColumnarSource {
    async fn fetch_with_context(
        &self,
        _: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        Ok(vec![json!({"id": 1}), json!({"id": 2})])
    }
    fn supports_columnar(&self) -> bool {
        true
    }
    fn stream_batches<'a>(
        &'a self,
        _: &'a HashMap<String, Value>,
        _: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<ColumnarPage, FaucetError>> + Send + 'a>> {
        let page =
            values_to_record_batch_inferred(&[json!({"id": 1}), json!({"id": 2})]).map(|batch| {
                ColumnarPage {
                    batch,
                    bookmark: None,
                }
            });
        Box::pin(futures::stream::iter([page]))
    }
    fn connector_name(&self) -> &'static str {
        "columnar-source"
    }
}

/// Counts columnar rows, or panics inside the Arrow write when told to.
struct ColumnarSink {
    panic: bool,
    rows: Arc<AtomicUsize>,
}

#[async_trait]
impl Sink for ColumnarSink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        Ok(records.len())
    }
    fn supports_columnar(&self) -> bool {
        true
    }
    async fn write_batch_columnar(&self, batch: &RecordBatch) -> Result<usize, FaucetError> {
        if self.panic {
            panic!("columnar kaboom");
        }
        self.rows.fetch_add(batch.num_rows(), Ordering::SeqCst);
        Ok(batch.num_rows())
    }
    fn connector_name(&self) -> &'static str {
        "columnar-sink"
    }
}

const CONFIG: &str = r#"
version: 1
name: fast_path_panic
pipeline:
  source:
    type: columnar-source
  sink:
    type: columnar-sink
    config: { panic: false }
matrix:
  - id: bad
    sink:
      config: { panic: true }
  - id: good
execution:
  max_concurrent: 1
  on_error: continue
"#;

fn opts() -> ExecuteOptions {
    ExecuteOptions {
        legacy_state_writes: false,
        force_lease: false,
        pipeline_name: "fast_path_panic".into(),
        run_id: None,
        execution: None,
        concurrency: None,
        dry_run: false,
        limit: None,
        state_path_override: None,
        state_scope: Default::default(),
        shard: None,
        auth: Default::default(),
        clock: chrono::Utc::now().fixed_offset(),
        cancel: None,
        resilience: None,
        sla: None,
        reconcile: None,
        verify: None,
        rollback: None,
        #[cfg(feature = "lineage")]
        lineage: None,
        #[cfg(feature = "lineage")]
        lineage_cfg: None,
        #[cfg(feature = "notify")]
        notifier: None,
        #[cfg(feature = "catalog")]
        catalog: None,
        usage: Default::default(),
        budget: None,
    }
}

#[tokio::test]
async fn a_columnar_panic_fails_only_its_row_under_on_error_continue() {
    let rows = Arc::new(AtomicUsize::new(0));
    let sink_rows = Arc::clone(&rows);
    PluginRegistry::with_builtins()
        .register_source_with(
            "columnar-source",
            |_| Ok(Box::new(ColumnarSource) as Box<dyn Source>),
            || json!({"type": "object"}),
            "Arrow source for the panic-isolation test",
        )
        .register_sink_with(
            "columnar-sink",
            move |cfg| {
                Ok(Box::new(ColumnarSink {
                    panic: cfg["panic"].as_bool().unwrap_or(false),
                    rows: Arc::clone(&sink_rows),
                }) as Box<dyn Sink>)
            },
            || json!({"type": "object"}),
            "Arrow sink for the panic-isolation test",
        )
        .install()
        .expect("registry installs");

    let cfg = PipelineConfig::from_text(CONFIG, std::path::Path::new("p.yaml")).unwrap();
    let mut o = opts();
    o.execution = cfg.execution.clone();
    let summary = run_expanded(expand(&cfg).unwrap(), o).await.unwrap();

    let by_id = |id: &str| {
        summary
            .invocations
            .iter()
            .find(|i| i.row_id == id)
            .unwrap_or_else(|| panic!("row {id} ran"))
    };
    let bad = by_id("bad");
    let err = bad.error.as_deref().expect("the panicking row fails");
    assert!(err.contains("panic in sink: columnar kaboom"), "{err}");
    assert!(
        !err.contains("task panicked"),
        "the panic must be a typed error, not a dead task: {err}"
    );
    let good = by_id("good");
    assert_eq!(good.error, None);
    assert_eq!(good.records_written, 2);
    assert_eq!(
        rows.load(Ordering::SeqCst),
        2,
        "the good row took the columnar path"
    );
}
