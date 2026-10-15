//! The pipeline's own cost in CPU instructions, counted by Valgrind's callgrind
//! through gungraun. Instruction counts do not move between runs, so CI can
//! hold a PR to a 5% budget against its base commit (#900):
//!
//! ```text
//! cargo bench -p faucet-core --bench pipeline_instructions -- --save-baseline=base   # base commit
//! cargo bench -p faucet-core --bench pipeline_instructions -- --baseline=base --callgrind-limits='ir=5%'
//! ```
//!
//! Each benchmark's input (a current-thread runtime and a source holding
//! pre-built records it hands over without copying) comes from an uncounted
//! setup function. Linux only: Valgrind does not run on macOS.

use async_trait::async_trait;
use faucet_core::{FaucetError, Pipeline, Sink, Source};
use futures::StreamExt;
use gungraun::prelude::*;
use metrics_util::debugging::DebuggingRecorder;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::hint::black_box;
use std::sync::Mutex;
use tokio::runtime::{Builder, Runtime};

const ROWS: usize = 10_000;

struct HandOverSource(Mutex<Option<Vec<Value>>>);

#[async_trait]
impl Source for HandOverSource {
    async fn fetch_with_context(
        &self,
        _: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        Ok(self
            .0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
            .unwrap_or_default())
    }

    fn connector_name(&self) -> &'static str {
        "mock-source"
    }
}

struct CountingSink;

#[async_trait]
impl Sink for CountingSink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        Ok(records.len())
    }

    fn connector_name(&self) -> &'static str {
        "mock-sink"
    }
}

struct Fixture {
    runtime: Runtime,
    source: HandOverSource,
}

fn fixture(rows: usize) -> Fixture {
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let records = (0..rows).map(|i| json!({"i": i, "name": "row"})).collect();
    Fixture {
        runtime,
        source: HandOverSource(Mutex::new(Some(records))),
    }
}

fn fixture_with_recorder(rows: usize) -> Fixture {
    metrics::set_global_recorder(DebuggingRecorder::new()).expect("one recorder per process");
    fixture(rows)
}

#[inline(never)]
fn run_baseline(f: Fixture) -> usize {
    f.runtime.block_on(async {
        let sink = CountingSink;
        let ctx = HashMap::new();
        let mut pages = f.source.stream_pages(&ctx, 1000);
        let mut written = 0;
        while let Some(page) = pages.next().await {
            written += sink
                .write_batch(&page.expect("page").records)
                .await
                .expect("write");
        }
        sink.flush().await.expect("flush");
        assert_eq!(written, ROWS, "every row reaches the sink");
        black_box(written)
    })
}

#[library_benchmark]
#[bench::rows(args = (ROWS), setup = fixture)]
fn baseline(f: Fixture) -> usize {
    run_baseline(f)
}

#[inline(never)]
fn run_pipeline(f: Fixture) -> usize {
    f.runtime.block_on(async {
        let sink = CountingSink;
        let result = Pipeline::new(&f.source, &sink)
            .with_name("bench")
            .with_row("instructions")
            .run()
            .await
            .expect("pipeline");
        assert_eq!(result.records_written, ROWS, "every row reaches the sink");
        black_box(result.records_written)
    })
}

#[library_benchmark]
#[bench::rows(args = (ROWS), setup = fixture)]
fn pipeline(f: Fixture) -> usize {
    run_pipeline(f)
}

#[library_benchmark]
#[bench::rows(args = (ROWS), setup = fixture_with_recorder)]
fn pipeline_with_recorder(f: Fixture) -> usize {
    run_pipeline(f)
}

library_benchmark_group!(
    name = pipeline_overhead,
    benchmarks = [baseline, pipeline, pipeline_with_recorder]
);

main!(library_benchmark_groups = pipeline_overhead);
