//! #872 — a panicking connector fails the run with a typed error on every
//! transfer path, never unwinding through the pipeline.
//!
//! The row path has been panic-isolated since the observability decorators
//! landed; the columnar (Arrow) and native byte-passthrough fast paths were
//! not, so a panicking Arrow writer, Parquet encoder or native loader took the
//! task down instead. Each test drives `Pipeline::run` down one fast path with
//! a connector that panics at one point and asserts the run returns `Err`
//! naming the panic. Before the fix the panic unwound out of `run()` and
//! failed the test itself.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use faucet_core::columnar::ColumnarPage;
use faucet_core::native::{NativeBatch, NativeFormat, NativeLoadCapability, NativeLoadContext};
use faucet_core::write_mode::WriteMode;
use faucet_core::{FaucetError, Pipeline, Sink, Source, Stream, Value, async_trait};

type Boxed<'a, T> = Pin<Box<dyn Stream<Item = Result<T, FaucetError>> + Send + 'a>>;

/// Where the panic fires.
#[derive(Clone, Copy, PartialEq)]
enum At {
    Nowhere,
    SourceOpen,
    SourceItem,
    SinkWrite,
}

fn one_row() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap()
}

/// A stream that yields `item` once, then panics on the next poll.
fn yields_then_panics<'a, T: Send + 'a>(item: T, msg: &'static str) -> Boxed<'a, T> {
    let mut item = Some(item);
    Box::pin(futures::stream::poll_fn(move |_| match item.take() {
        Some(i) => std::task::Poll::Ready(Some(Ok(i))),
        None => panic!("{msg}"),
    }))
}

struct FastSource(At);

#[async_trait]
impl Source for FastSource {
    async fn fetch_with_context(
        &self,
        _: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        Ok(vec![serde_json::json!({"id": 1})])
    }
    fn supports_columnar(&self) -> bool {
        true
    }
    fn stream_batches<'a>(
        &'a self,
        _: &'a HashMap<String, Value>,
        _: usize,
    ) -> Boxed<'a, ColumnarPage> {
        let page = ColumnarPage {
            batch: one_row(),
            bookmark: None,
        };
        match self.0 {
            At::SourceOpen => panic!("columnar source open"),
            At::SourceItem => yields_then_panics(page, "columnar source item"),
            _ => Box::pin(futures::stream::iter([Ok(page)])),
        }
    }
    fn native_output_formats(&self) -> &'static [NativeFormat] {
        &[NativeFormat::NdJson]
    }
    fn stream_native<'a>(
        &'a self,
        _: &'a HashMap<String, Value>,
        _: NativeFormat,
        _: usize,
    ) -> Boxed<'a, NativeBatch> {
        let batch = NativeBatch::bytes(NativeFormat::NdJson, b"{\"id\":1}\n".to_vec())
            .with_records(Some(1));
        match self.0 {
            At::SourceOpen => panic!("native source open"),
            At::SourceItem => yields_then_panics(batch, "native source item"),
            _ => Box::pin(futures::stream::iter([Ok(batch)])),
        }
    }
}

struct ColumnarSink(At);

#[async_trait]
impl Sink for ColumnarSink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        Ok(records.len())
    }
    fn supports_columnar(&self) -> bool {
        true
    }
    async fn write_batch_columnar(&self, batch: &RecordBatch) -> Result<usize, FaucetError> {
        if self.0 == At::SinkWrite {
            panic!("columnar sink write");
        }
        Ok(batch.num_rows())
    }
}

struct NativeSink(At);

#[async_trait]
impl Sink for NativeSink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        Ok(records.len())
    }
    fn native_load_capabilities(&self) -> Vec<NativeLoadCapability> {
        vec![NativeLoadCapability {
            format: NativeFormat::NdJson,
            mechanism: "test-load",
            write_modes: &[WriteMode::Append],
        }]
    }
    async fn load_native(
        &self,
        batch: NativeBatch,
        _: &str,
        _: NativeLoadContext,
    ) -> Result<usize, FaucetError> {
        if self.0 == At::SinkWrite {
            panic!("native sink load");
        }
        Ok(batch.records.unwrap_or(0) as usize)
    }
}

async fn run(source: &dyn Source, sink: &dyn Sink) -> Result<usize, FaucetError> {
    Pipeline::new(source, sink)
        .run()
        .await
        .map(|r| r.records_written)
}

fn assert_panic_error(result: Result<usize, FaucetError>, expected: &str) {
    match result {
        Err(e) => assert!(e.to_string().contains(expected), "{e}"),
        Ok(n) => panic!("expected the run to fail with `{expected}`, wrote {n}"),
    }
}

#[tokio::test]
async fn columnar_path_runs_clean_without_a_panic() {
    let written = run(&FastSource(At::Nowhere), &ColumnarSink(At::Nowhere)).await;
    assert_eq!(written.unwrap(), 1);
}

#[tokio::test]
async fn columnar_sink_panic_fails_the_run() {
    let r = run(&FastSource(At::Nowhere), &ColumnarSink(At::SinkWrite)).await;
    assert_panic_error(r, "panic in sink: columnar sink write");
}

#[tokio::test]
async fn columnar_source_panic_opening_the_stream_fails_the_run() {
    let r = run(&FastSource(At::SourceOpen), &ColumnarSink(At::Nowhere)).await;
    assert_panic_error(r, "panic in source: columnar source open");
}

#[tokio::test]
async fn columnar_source_panic_mid_stream_fails_the_run() {
    let r = run(&FastSource(At::SourceItem), &ColumnarSink(At::Nowhere)).await;
    assert_panic_error(r, "panic in source: columnar source item");
}

#[tokio::test]
async fn native_path_runs_clean_without_a_panic() {
    let written = run(&FastSource(At::Nowhere), &NativeSink(At::Nowhere)).await;
    assert_eq!(written.unwrap(), 1);
}

#[tokio::test]
async fn native_sink_panic_fails_the_run() {
    let r = run(&FastSource(At::Nowhere), &NativeSink(At::SinkWrite)).await;
    assert_panic_error(r, "panic in sink: native sink load");
}

#[tokio::test]
async fn native_source_panic_opening_the_stream_fails_the_run() {
    let r = run(&FastSource(At::SourceOpen), &NativeSink(At::Nowhere)).await;
    assert_panic_error(r, "panic in source: native source open");
}

#[tokio::test]
async fn native_source_panic_mid_stream_fails_the_run() {
    let r = run(&FastSource(At::SourceItem), &NativeSink(At::Nowhere)).await;
    assert_panic_error(r, "panic in source: native source item");
}
