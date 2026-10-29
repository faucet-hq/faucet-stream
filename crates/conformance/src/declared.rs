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

/// The instant every `at` value check 26 writes names, in three renderings:
/// `+00:00`, `Z` and a non-UTC offset.
pub const DECLARED_INSTANT: &str = "2024-02-22T02:00:00Z";

/// The rows check 26 writes into the created table.
fn declared_rows() -> Vec<Value> {
    [
        "2024-02-22T02:00:00+00:00",
        "2024-02-22T02:00:00Z",
        "2024-02-22T07:30:00+05:30",
    ]
    .iter()
    .enumerate()
    .map(|(i, at)| {
        serde_json::json!({
            "id": i + 1,
            "amount": "12.50",
            "day": "2024-10-27",
            "at": at,
            "note": "x",
        })
    })
    .collect()
}

/// Check 26: `sink` declares `declared_types`; handed [`declared_corpus`], a
/// run that writes nothing creates the destination with every declared
/// column, each typed as `expected` says (`(column, type substring)`, matched
/// ignoring case); a page of the types' renderings — the instant written as
/// `+00:00`, `Z` and `+05:30` — then writes into it and leaves the column
/// types as they were. `column_types` reads the destination's
/// `(column, type)` pairs (empty when it does not exist). The sink must point
/// at a destination that does not exist yet, with `create_table` on and
/// append mode.
pub async fn assert_declared_types_create<S, F, Fut>(
    sink: &S,
    expected: &[(&str, &str)],
    column_types: F,
) where
    S: Sink + ?Sized,
    F: Fn() -> Fut,
    Fut: Future<Output = Vec<(String, String)>>,
{
    declared_create(sink, expected, &column_types).await;
}

/// Check 26 plus the instants it wrote: `read_at` returns every row's `at` as
/// RFC 3339 (a backend that stores UTC wall-clock time renders it with `Z`),
/// and each must name [`DECLARED_INSTANT`]. Run it with the sink's session in
/// a time zone other than UTC where the backend has one.
pub async fn assert_declared_instants_exact<S, F, Fut, R, RFut>(
    sink: &S,
    expected: &[(&str, &str)],
    column_types: F,
    read_at: R,
) where
    S: Sink + ?Sized,
    F: Fn() -> Fut,
    Fut: Future<Output = Vec<(String, String)>>,
    R: FnOnce() -> RFut,
    RFut: Future<Output = Vec<String>>,
{
    let label = sink.connector_name();
    declared_create(sink, expected, &column_types).await;
    let want = chrono::DateTime::parse_from_rfc3339(DECLARED_INSTANT).expect("instant");
    let read = read_at().await;
    assert_eq!(read.len(), 3, "[{label}] read back {read:?}");
    for at in &read {
        let got = chrono::DateTime::parse_from_rfc3339(at)
            .unwrap_or_else(|e| panic!("[{label}] read back {at:?}, not RFC 3339: {e}"));
        assert_eq!(
            got, want,
            "[{label}] a declared zoned timestamp must keep its instant: wrote {DECLARED_INSTANT}, read {at}"
        );
    }
}

async fn declared_create<S, F, Fut>(sink: &S, expected: &[(&str, &str)], column_types: &F)
where
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
    check_created(label, &created, expected);
    let rows = declared_rows();
    let written = sink.write_batch(&rows).await.unwrap_or_else(|e| {
        panic!("[{label}] writing the renderings into typed columns errored: {e}")
    });
    assert_eq!(
        written,
        rows.len(),
        "[{label}] the typed write must report its rows"
    );
    sink.flush()
        .await
        .unwrap_or_else(|e| panic!("[{label}] flush errored: {e}"));
    assert_eq!(
        lower(column_types().await),
        created,
        "[{label}] a write must never retype the created columns"
    );
}

fn check_created(label: &str, created: &[(String, String)], expected: &[(&str, &str)]) {
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
}

/// Check 27 (#975): a first `write_mode: overwrite` run that writes nothing
/// succeeds and leaves the empty typed table, both when one sink instance
/// drives the whole lifecycle (a library `Pipeline::run`) and when the CLI
/// drives it — begin and commit on lifecycle instances, `complete_run` on the
/// writer. `make(n)` builds a sink in overwrite mode with `create_table` on,
/// aimed at destination `n` (0 and 1, neither existing yet);
/// `column_types(n)` reads destination `n` as in check 26.
#[allow(deprecated)] // drives the 1.x lifecycle hooks until faucet-core 2.0 (RFC 0014)
pub async fn assert_declared_overwrite_empty_run<M, MFut, F, Fut>(
    make: M,
    expected: &[(&str, &str)],
    column_types: F,
) where
    M: Fn(usize) -> MFut,
    MFut: Future<Output = Box<dyn Sink>>,
    F: Fn(usize) -> Fut,
    Fut: Future<Output = Vec<(String, String)>>,
{
    let one = make(0).await;
    let label = one.connector_name();
    assert!(
        one.is_overwrite() && one.capabilities().declared_types(),
        "[{label}] check 27 needs an overwrite sink that declares `declared_types`"
    );
    assert!(
        column_types(0).await.is_empty(),
        "[{label}] destination 0 exists"
    );
    one.set_declared_columns(declared_corpus());
    one.begin_overwrite()
        .await
        .unwrap_or_else(|e| panic!("[{label}] begin_overwrite errored: {e}"));
    one.flush().await.expect("flush");
    one.commit_overwrite()
        .await
        .unwrap_or_else(|e| panic!("[{label}] a zero-row first overwrite failed to commit: {e}"));
    one.complete_run()
        .await
        .unwrap_or_else(|e| panic!("[{label}] complete_run after the commit errored: {e}"));
    check_created(label, &lower(column_types(0).await), expected);

    assert!(
        column_types(1).await.is_empty(),
        "[{label}] destination 1 exists"
    );
    make(1)
        .await
        .begin_overwrite()
        .await
        .unwrap_or_else(|e| panic!("[{label}] begin_overwrite errored: {e}"));
    let writer = make(1).await;
    writer.set_declared_columns(declared_corpus());
    writer.flush().await.expect("flush");
    writer
        .complete_run()
        .await
        .unwrap_or_else(|e| panic!("[{label}] the writer's complete_run errored: {e}"));
    make(1).await.commit_overwrite().await.unwrap_or_else(|e| {
        panic!("[{label}] a zero-row first overwrite failed to commit on another instance: {e}")
    });
    check_created(label, &lower(column_types(1).await), expected);
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

    #[tokio::test]
    async fn check26_instants_compare_by_instant() {
        let sink = Typing::default();
        let s = sink.clone();
        assert_declared_instants_exact(
            &sink,
            EXPECTED,
            || std::future::ready(s.cols.lock().unwrap().clone()),
            || {
                std::future::ready(vec![
                    "2024-02-22T02:00:00Z".to_string(),
                    "2024-02-22T02:00:00+00:00".to_string(),
                    "2024-02-21T21:00:00-05:00".to_string(),
                ])
            },
        )
        .await;
    }

    #[tokio::test]
    #[should_panic(expected = "must keep its instant")]
    async fn check26_fails_for_a_shifted_instant() {
        let sink = Typing::default();
        let s = sink.clone();
        assert_declared_instants_exact(
            &sink,
            EXPECTED,
            || std::future::ready(s.cols.lock().unwrap().clone()),
            || std::future::ready(vec!["2024-02-22T07:30:00Z".to_string(); 3]),
        )
        .await;
    }

    type Tables = Arc<Mutex<HashMap<String, Vec<(String, String)>>>>;

    /// An overwrite sink over shared "tables"; `buggy` creates the target in
    /// `complete_run` the way #975 found it.
    struct Ow {
        dest: usize,
        tables: Tables,
        declared: Mutex<Option<DeclaredColumns>>,
        buggy: bool,
    }

    impl Ow {
        fn names(&self) -> (String, String) {
            (format!("t{}", self.dest), format!("t{}_ovw", self.dest))
        }
        fn create(&self, table: &str) {
            let Some(d) = self.declared.lock().unwrap().clone() else {
                return;
            };
            let cols = d
                .columns()
                .iter()
                .map(|c| (c.name.clone(), format!("{:?}", c.logical_type)))
                .collect();
            self.tables.lock().unwrap().insert(table.to_string(), cols);
        }
        fn exists(&self, table: &str) -> bool {
            self.tables.lock().unwrap().contains_key(table)
        }
    }

    #[faucet_core::async_trait]
    impl Sink for Ow {
        async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
            Ok(records.len())
        }
        fn capabilities(&self) -> faucet_core::Capabilities {
            faucet_core::Capabilities::of_sink(self)
                .to_builder()
                .declared_types(true)
                .build()
        }
        fn is_overwrite(&self) -> bool {
            true
        }
        fn set_declared_columns(&self, columns: DeclaredColumns) {
            *self.declared.lock().unwrap() = Some(columns);
        }
        async fn begin_overwrite(&self) -> Result<(), FaucetError> {
            Ok(())
        }
        async fn commit_overwrite(&self) -> Result<(), FaucetError> {
            let (target, staging) = self.names();
            if self.exists(&target) {
                if !self.exists(&staging) {
                    return Err(FaucetError::Sink("staging missing".into()));
                }
                let cols = self.tables.lock().unwrap().remove(&staging).unwrap();
                self.tables.lock().unwrap().insert(target, cols);
                return Ok(());
            }
            if !self.exists(&staging) {
                self.create(&staging);
            }
            let staged = self.tables.lock().unwrap().remove(&staging);
            if let Some(cols) = staged {
                self.tables.lock().unwrap().insert(target, cols);
            }
            Ok(())
        }
        async fn complete_run(&self) -> Result<(), FaucetError> {
            let (target, staging) = self.names();
            if !self.exists(&target) {
                self.create(if self.buggy { &target } else { &staging });
            }
            Ok(())
        }
    }

    async fn run_overwrite(buggy: bool) {
        let tables = Tables::default();
        let t = tables.clone();
        assert_declared_overwrite_empty_run(
            |dest| {
                let tables = tables.clone();
                async move {
                    Box::new(Ow {
                        dest,
                        tables,
                        declared: Mutex::new(None),
                        buggy,
                    }) as Box<dyn Sink>
                }
            },
            &[("amount", "decimal")],
            |dest| {
                std::future::ready(
                    t.lock()
                        .unwrap()
                        .get(&format!("t{dest}"))
                        .cloned()
                        .unwrap_or_default(),
                )
            },
        )
        .await;
    }

    #[tokio::test]
    async fn check27_passes_when_the_commit_publishes_the_declared_staging() {
        run_overwrite(false).await;
    }

    #[tokio::test]
    #[should_panic(expected = "failed to commit on another instance")]
    async fn check27_fails_when_the_writer_creates_the_target() {
        run_overwrite(true).await;
    }
}
