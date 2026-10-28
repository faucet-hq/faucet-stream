//! Checks 25–26 (#955): a source that declares its column types declares the
//! columns it emits, and a sink that creates its destination from a
//! declaration types the created columns from it.

use faucet_core::{DeclaredColumn, DeclaredColumns, LogicalType, Sink, Source, Value};
use futures::StreamExt;
use std::collections::HashMap;
use std::future::Future;

/// The most rows check 25 reads.
const MAX_ROWS: usize = 1000;

/// Check 25: `source` declares `declared_columns`, its declaration is complete
/// and non-empty, and every record it emits (up to 1000) carries only declared
/// columns, each value a rendering of its declared type.
pub async fn assert_declared_columns_truthful<S: Source + ?Sized>(source: &S) {
    let label = source.connector_name();
    assert!(
        source.capabilities().declared_columns(),
        "[{label}] does not declare the `declared_columns` capability"
    );
    let ctx = HashMap::new();
    let declared = source
        .declared_columns(&ctx)
        .await
        .unwrap_or_else(|e| panic!("[{label}] declared_columns errored: {e}"))
        .unwrap_or_else(|| panic!("[{label}] declares `declared_columns` but declared none"));
    assert!(
        !declared.is_empty() && declared.is_complete(),
        "[{label}] the declaration must be complete and name at least one column"
    );
    let mut pages = source.stream_pages(&ctx, 100);
    let mut seen = 0;
    while let Some(page) = pages.next().await {
        let page = page.unwrap_or_else(|e| panic!("[{label}] read errored: {e}"));
        for record in &page.records {
            let Some(obj) = record.as_object() else {
                panic!("[{label}] emitted a non-object record: {record}");
            };
            for (name, value) in obj {
                let column = declared.get(name).unwrap_or_else(|| {
                    panic!("[{label}] emitted column {name:?}, which it did not declare")
                });
                assert!(
                    column.logical_type.accepts(value),
                    "[{label}] column {name:?} declared {:?} emitted {value}",
                    column.logical_type
                );
            }
            seen += 1;
        }
        if seen >= MAX_ROWS {
            break;
        }
    }
}

/// The declaration check 26 hands the sink.
pub fn declared_corpus() -> DeclaredColumns {
    DeclaredColumns::new([
        DeclaredColumn::new("id", LogicalType::Int64, false),
        DeclaredColumn::new(
            "amount",
            LogicalType::Decimal {
                precision: Some(12),
                scale: Some(2),
            },
            true,
        ),
        DeclaredColumn::new("day", LogicalType::Date, true),
        DeclaredColumn::new("at", LogicalType::Timestamp { tz: true }, true),
        DeclaredColumn::new("note", LogicalType::String, true),
    ])
}

/// Check 26: `sink` declares `declared_types`; handed [`declared_corpus`], a
/// run that writes nothing creates the destination with every declared
/// column, each typed as `expected` says (`(column, type substring)`, matched
/// ignoring case); a page of the types' renderings then writes into it and
/// leaves the column types as they were. `column_types` reads the
/// destination's `(column, type)` pairs (empty when it does not exist). The
/// sink must point at a destination that does not exist yet, with
/// `create_table` on and append mode.
pub async fn assert_declared_types_create<S, F, Fut>(
    sink: &S,
    expected: &[(&str, &str)],
    column_types: F,
) where
    S: Sink + ?Sized,
    F: Fn() -> Fut,
    Fut: Future<Output = Vec<(String, String)>>,
{
    let label = sink.connector_name();
    assert!(
        sink.capabilities().declared_types(),
        "[{label}] does not declare the `declared_types` capability"
    );
    assert!(
        column_types().await.is_empty(),
        "[{label}] check 26 needs a destination that does not exist yet"
    );
    sink.set_declared_columns(declared_corpus());
    sink.complete_run()
        .await
        .unwrap_or_else(|e| panic!("[{label}] complete_run of an empty run errored: {e}"));
    let created = lower(column_types().await);
    for c in declared_corpus().columns() {
        assert!(
            created.iter().any(|(n, _)| n == &c.name),
            "[{label}] an empty run with a complete declaration must create column {:?}: {created:?}",
            c.name
        );
    }
    for (column, ty) in expected {
        let got = created
            .iter()
            .find(|(n, _)| n == &column.to_ascii_lowercase())
            .map(|(_, t)| t.as_str())
            .unwrap_or_default();
        assert!(
            got.contains(&ty.to_ascii_lowercase()),
            "[{label}] declared column {column:?} must be created as {ty}, got {got:?}"
        );
    }
    let row: Value = serde_json::json!({
        "id": 1,
        "amount": "12.50",
        "day": "2024-10-27",
        "at": "2024-02-22T02:00:00+00:00",
        "note": "x",
    });
    let written = sink
        .write_batch(std::slice::from_ref(&row))
        .await
        .unwrap_or_else(|e| {
            panic!("[{label}] writing the renderings into typed columns errored: {e}")
        });
    assert_eq!(written, 1, "[{label}] the typed write must report its row");
    sink.flush()
        .await
        .unwrap_or_else(|e| panic!("[{label}] flush errored: {e}"));
    assert_eq!(
        lower(column_types().await),
        created,
        "[{label}] a write must never retype the created columns"
    );
}

fn lower(cols: Vec<(String, String)>) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = cols
        .into_iter()
        .map(|(n, t)| (n.to_ascii_lowercase(), t.to_ascii_lowercase()))
        .collect();
    out.sort();
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::{FaucetError, StreamPage};
    use serde_json::json;
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};

    struct Declares {
        records: Vec<Value>,
        declared: Option<DeclaredColumns>,
    }

    #[faucet_core::async_trait]
    impl Source for Declares {
        async fn fetch_with_context(
            &self,
            _ctx: &HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Ok(self.records.clone())
        }
        fn capabilities(&self) -> faucet_core::Capabilities {
            faucet_core::Capabilities::of_source(self)
                .to_builder()
                .declared_columns(true)
                .build()
        }
        async fn declared_columns(
            &self,
            _ctx: &HashMap<String, Value>,
        ) -> Result<Option<DeclaredColumns>, FaucetError> {
            Ok(self.declared.clone())
        }
        fn stream_pages<'a>(
            &'a self,
            _ctx: &'a HashMap<String, Value>,
            _batch: usize,
        ) -> Pin<Box<dyn futures::Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>>
        {
            Box::pin(futures::stream::iter([Ok(StreamPage {
                records: self.records.clone(),
                bookmark: None,
            })]))
        }
    }

    fn source(record: Value) -> Declares {
        Declares {
            records: vec![record],
            declared: Some(declared_corpus()),
        }
    }

    #[tokio::test]
    async fn check25_passes_for_a_truthful_declaration() {
        assert_declared_columns_truthful(&source(
            json!({"id": 1, "day": "2024-01-02", "at": null}),
        ))
        .await;
    }

    #[tokio::test]
    #[should_panic(expected = "did not declare")]
    async fn check25_fails_for_an_undeclared_column() {
        assert_declared_columns_truthful(&source(json!({"id": 1, "extra": 2}))).await;
    }

    #[tokio::test]
    #[should_panic(expected = "declared Date emitted")]
    async fn check25_fails_for_a_value_its_type_refuses() {
        assert_declared_columns_truthful(&source(json!({"day": "yesterday"}))).await;
    }

    #[tokio::test]
    #[should_panic(expected = "declared none")]
    async fn check25_fails_without_a_declaration() {
        let mut s = source(json!({}));
        s.declared = None;
        assert_declared_columns_truthful(&s).await;
    }

    #[derive(Clone, Default)]
    struct Typing {
        cols: Arc<Mutex<Vec<(String, String)>>>,
        declared: Arc<Mutex<Option<DeclaredColumns>>>,
        string_only: bool,
        retypes: bool,
    }

    impl Typing {
        fn create(&self) {
            let mut cols = self.cols.lock().unwrap();
            if !cols.is_empty() {
                return;
            }
            let Some(d) = self.declared.lock().unwrap().clone() else {
                return;
            };
            *cols = d
                .columns()
                .iter()
                .map(|c| {
                    let t = match (self.string_only, c.logical_type) {
                        (true, _) | (_, LogicalType::String) => "TEXT",
                        (_, LogicalType::Int64) => "BIGINT",
                        (_, LogicalType::Decimal { .. }) => "NUMERIC(12,2)",
                        (_, LogicalType::Date) => "DATE",
                        _ => "TIMESTAMPTZ",
                    };
                    (c.name.clone(), t.to_string())
                })
                .collect();
        }
    }

    #[faucet_core::async_trait]
    impl Sink for Typing {
        async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
            self.create();
            if self.retypes {
                self.cols.lock().unwrap()[0].1 = "TEXT".into();
            }
            Ok(records.len())
        }
        fn capabilities(&self) -> faucet_core::Capabilities {
            faucet_core::Capabilities::of_sink(self)
                .to_builder()
                .declared_types(true)
                .build()
        }
        fn set_declared_columns(&self, columns: DeclaredColumns) {
            *self.declared.lock().unwrap() = Some(columns);
        }
        async fn complete_run(&self) -> Result<(), FaucetError> {
            self.create();
            Ok(())
        }
    }

    const EXPECTED: &[(&str, &str)] =
        &[("amount", "numeric"), ("day", "date"), ("at", "timestamp")];

    async fn run(sink: Typing) {
        let s = sink.clone();
        assert_declared_types_create(&sink, EXPECTED, || {
            std::future::ready(s.cols.lock().unwrap().clone())
        })
        .await;
    }

    #[tokio::test]
    async fn check26_passes_for_a_typed_create() {
        run(Typing::default()).await;
    }

    #[tokio::test]
    #[should_panic(expected = "must be created as numeric")]
    async fn check26_fails_for_an_untyped_create() {
        run(Typing {
            string_only: true,
            ..Default::default()
        })
        .await;
    }

    #[tokio::test]
    #[should_panic(expected = "never retype")]
    async fn check26_fails_for_a_write_that_retypes() {
        run(Typing {
            retypes: true,
            ..Default::default()
        })
        .await;
    }
}
