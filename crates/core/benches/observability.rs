//! Observability decorator overhead, in wall-clock time, for local profiling.
//! Compares three configs:
//! - `baseline_no_decorator`: bypass Pipeline::run; drive source/sink directly.
//! - `instrumented_no_recorder`: decorator active but no global recorder.
//! - `instrumented_with_recorder`: decorator + DebuggingRecorder installed.
//!
//! Each sample gets a fresh source from an untimed setup step that hands its
//! records over without copying, so the timing covers the pipeline's own work.
//! CI gates on instruction counts instead (`benches/pipeline_instructions.rs`):
//! wall-clock ratios at the 5% level are noise on a shared runner (#900).

use async_trait::async_trait;
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use faucet_core::{FaucetError, Pipeline, Sink, Source};
use futures::StreamExt;
use metrics_util::debugging::DebuggingRecorder;
use serde_json::{Value, json};
use std::collections::HashMap;
use tokio::runtime::Runtime;

const N: usize = 100_000;

struct MockSource(std::sync::Mutex<Option<Vec<Value>>>);

impl MockSource {
    fn new(records: Vec<Value>) -> Self {
        Self(std::sync::Mutex::new(Some(records)))
    }
}

#[async_trait]
impl Source for MockSource {
    async fn fetch_with_context(
        &self,
        _: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        Ok(self.0.lock().unwrap().take().unwrap_or_default())
    }

    fn connector_name(&self) -> &'static str {
        "mock-source"
    }
}

struct MockSink;

#[async_trait]
impl Sink for MockSink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        Ok(records.len())
    }

    fn connector_name(&self) -> &'static str {
        "mock-sink"
    }
}

fn bench_pipelines(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");
    let records: Vec<Value> = (0..N).map(|i| json!({"i": i})).collect();

    let mut group = c.benchmark_group("observability");
    group.sample_size(30);

    group.bench_function("baseline_no_decorator", |b| {
        b.to_async(&rt).iter_batched(
            || MockSource::new(records.clone()),
            |source| async move {
                let sink = MockSink;
                // Drive source/sink directly, bypassing the decorator path.
                let ctx = HashMap::new();
                let mut pages = source.stream_pages(&ctx, 1000);
                while let Some(page) = pages.next().await {
                    let p = page.expect("page");
                    sink.write_batch(&p.records).await.expect("write");
                }
                sink.flush().await.expect("flush");
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("instrumented_no_recorder", |b| {
        b.to_async(&rt).iter_batched(
            || MockSource::new(records.clone()),
            |source| async move {
                let sink = MockSink;
                Pipeline::new(&source, &sink)
                    .with_name("bench")
                    .with_row("baseline")
                    .run()
                    .await
                    .expect("pipeline");
            },
            BatchSize::LargeInput,
        );
    });

    // Install the global recorder once for the third bench. Failure means
    // another bench/test already installed one; the bench still runs but
    // measures the existing recorder's overhead.
    let _ = metrics::set_global_recorder(DebuggingRecorder::new());

    group.bench_function("instrumented_with_recorder", |b| {
        b.to_async(&rt).iter_batched(
            || MockSource::new(records.clone()),
            |source| async move {
                let sink = MockSink;
                Pipeline::new(&source, &sink)
                    .with_name("bench")
                    .with_row("with-recorder")
                    .run()
                    .await
                    .expect("pipeline");
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

criterion_group!(benches, bench_pipelines);
criterion_main!(benches);
