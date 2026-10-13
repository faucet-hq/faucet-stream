//! A non-finite value a source renders as `null` is counted on the run (#885).

use async_trait::async_trait;
use faucet_core::fidelity::{self, NonFiniteCounter};
use faucet_core::{FaucetError, Pipeline, RowAccounts, Sink, Source, StreamPage};
use futures_core::Stream;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

struct FloatSource(Vec<f64>);

#[async_trait]
impl Source for FloatSource {
    async fn fetch_with_context(
        &self,
        _ctx: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        unreachable!("Pipeline::run drives stream_pages")
    }

    fn stream_pages<'a>(
        &'a self,
        _ctx: &'a HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        Box::pin(async_stream::try_stream! {
            let carried = NonFiniteCounter::current();
            let blocking: Vec<Value> = tokio::task::spawn_blocking(move || {
                fidelity::propagate(&carried, || vec![json!({"x": fidelity::float64(f64::NAN)})])
            })
            .await
            .expect("blocking decode");
            let mut records: Vec<Value> = self
                .0
                .iter()
                .map(|v| json!({"x": fidelity::float64(*v)}))
                .collect();
            records.extend(blocking);
            yield StreamPage { records, bookmark: None };
        })
    }
}

#[derive(Default)]
struct Collect(Mutex<Vec<Value>>);

#[async_trait]
impl Sink for Collect {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        self.0.lock().unwrap().extend_from_slice(records);
        Ok(records.len())
    }
}

#[tokio::test]
async fn non_finite_values_land_as_null_and_are_counted_on_the_run() {
    let source = FloatSource(vec![1.5, f64::INFINITY, f64::NEG_INFINITY, -0.0]);
    let sink = Collect::default();
    let accounts = Arc::new(RowAccounts::new());
    Pipeline::new(&source, &sink)
        .with_row_accounts(Arc::clone(&accounts))
        .run()
        .await
        .expect("run");
    let landed = sink.0.lock().unwrap().clone();
    assert_eq!(
        landed,
        vec![
            json!({"x": 1.5}),
            json!({"x": null}),
            json!({"x": null}),
            json!({"x": -0.0}),
            json!({"x": null}),
        ]
    );
    let snap = accounts.snapshot();
    assert_eq!(snap.non_finite, 3);
    assert!(
        snap.summary_line()
            .contains("non-finite values written as null 3"),
        "{}",
        snap.summary_line()
    );
}

#[tokio::test]
async fn each_run_counts_its_own_non_finite_values() {
    let source = FloatSource(vec![2.0]);
    let sink = Collect::default();
    let accounts = Arc::new(RowAccounts::new());
    Pipeline::new(&source, &sink)
        .with_row_accounts(Arc::clone(&accounts))
        .run()
        .await
        .expect("run");
    assert_eq!(
        accounts.snapshot().non_finite,
        1,
        "the blocking decode adds one"
    );
    let plain = Pipeline::new(&source, &sink).run().await;
    assert!(plain.is_ok(), "no row accounts attached is fine too");
}
