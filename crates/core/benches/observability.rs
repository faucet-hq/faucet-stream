//! Observability decorator overhead benchmark. Compares three configs:
//! - `baseline_no_decorator`: bypass Pipeline::run; drive source/sink directly.
//! - `instrumented_no_recorder`: decorator active but no global recorder.
//! - `instrumented_with_recorder`: decorator + DebuggingRecorder installed.
//!
//! The CI regression check (`.github/scripts/check_obs_regression.py`) gates
//! on a 5% threshold. The threshold is a regression budget, not a zero-overhead
//! claim: the `metrics` facade has ~3-5% steady-state cost from macro
//! dispatch even with a DebuggingRecorder. The gate catches genuine
//! regressions (lock contention, accidental clones, etc.) without flapping
//! on micro-benchmark noise at `sample_size(10)`.

use async_trait::async_trait;
use criterion::{Criterion, criterion_group, criterion_main};
use faucet_core::{FaucetError, Pipeline, Sink, Source};
use futures::StreamExt;
use metrics_util::debugging::DebuggingRecorder;
use serde_json::{Value, json};
use std::collections::HashMap;
use tokio::runtime::Runtime;

const N: usize = 100_000;

struct MockSource(Vec<Value>);

#[async_trait]
impl Source for MockSource {
    async fn fetch_with_context(
        &self,
        _: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        Ok(self.0.clone())
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
    group.sample_size(10);

    group.bench_function("baseline_no_decorator", |b| {
        b.to_async(&rt).iter(|| async {
            let source = MockSource(records.clone());
            let sink = MockSink;
            // Drive source/sink directly, bypassing the decorator path.
            let ctx = HashMap::new();
            let mut pages = source.stream_pages(&ctx, 1000);
            while let Some(page) = pages.next().await {
                let p = page.expect("page");
                sink.write_batch(&p.records).await.expect("write");
            }
            sink.flush().await.expect("flush");
        });
    });

    group.bench_function("instrumented_no_recorder", |b| {
        b.to_async(&rt).iter(|| async {
            let source = MockSource(records.clone());
            let sink = MockSink;
            Pipeline::new(&source, &sink)
                .with_name("bench")
                .with_row("baseline")
                .run()
                .await
                .expect("pipeline");
        });
    });

    // Install the global recorder once for the third bench. Failure means
    // another bench/test already installed one; the bench still runs but
    // measures the existing recorder's overhead.
    let _ = metrics::set_global_recorder(DebuggingRecorder::new());

    group.bench_function("instrumented_with_recorder", |b| {
        b.to_async(&rt).iter(|| async {
            let source = MockSource(records.clone());
            let sink = MockSink;
            Pipeline::new(&source, &sink)
                .with_name("bench")
                .with_row("with-recorder")
                .run()
                .await
                .expect("pipeline");
        });
    });

    group.finish();
}

criterion_group!(benches, bench_pipelines);
criterion_main!(benches);
