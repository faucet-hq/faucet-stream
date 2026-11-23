//! Under `on_drift: evolve` with `on_incompatible: quarantine`, a row whose
//! column changed type incompatibly goes to the DLQ and the rest is written.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use faucet_core::dlq::DlqConfig;
use faucet_core::drift::{SchemaDriftPolicy, SchemaDriftSpec, SchemaEvolution};
use faucet_core::{FaucetError, Pipeline, Sink, Source, async_trait};
use serde_json::{Value, json};

struct Rows(Vec<Value>);

#[async_trait]
impl Source for Rows {
    async fn fetch_with_context(
        &self,
        _ctx: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        Ok(self.0.clone())
    }
}

#[derive(Default)]
struct Evolving(Mutex<Vec<Value>>);

#[async_trait]
impl Sink for Evolving {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        self.0.lock().unwrap().extend_from_slice(records);
        Ok(records.len())
    }
    async fn current_schema(&self) -> Result<Option<Value>, FaucetError> {
        Ok(Some(json!({
            "type": "object",
            "properties": {"id": {"type": "integer"}, "n": {"type": "integer"}}
        })))
    }
    fn supports_schema_evolution(&self) -> bool {
        true
    }
    async fn evolve_schema(&self, _: &SchemaEvolution) -> Result<(), FaucetError> {
        Ok(())
    }
}

#[derive(Default)]
struct Dlq(Mutex<Vec<Value>>);

#[async_trait]
impl Sink for Dlq {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        self.0.lock().unwrap().extend_from_slice(records);
        Ok(records.len())
    }
}

#[tokio::test]
async fn an_incompatible_row_is_quarantined_under_evolve() {
    let source = Rows(vec![
        json!({"id": 1, "n": 2}),
        json!({"id": 2, "n": "not a number"}),
    ]);
    let sink = Evolving::default();
    let dlq = Arc::new(Dlq::default());
    let spec: SchemaDriftSpec =
        serde_json::from_value(json!({"on_drift": "evolve", "on_incompatible": "quarantine"}))
            .unwrap();
    let result = Pipeline::new(&source, &sink)
        .with_schema_drift(SchemaDriftPolicy::compile(&spec))
        .with_dlq(DlqConfig::new(Arc::clone(&dlq) as Arc<dyn Sink>))
        .run()
        .await
        .unwrap();
    assert_eq!(result.records_written, 1);
    assert_eq!(sink.0.lock().unwrap().len(), 1);
    assert_eq!(dlq.0.lock().unwrap().len(), 1);
}
