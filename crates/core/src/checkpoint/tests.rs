use super::*;
use crate::state::MemoryStateStore;
use async_trait::async_trait;
use serde_json::json;
use std::sync::Mutex;
use std::time::Duration;

type Log = Arc<Mutex<Vec<String>>>;

/// A sink that logs each flush and can fail its next `failures` flushes.
struct Flushing {
    name: &'static str,
    log: Log,
    failures: Mutex<u32>,
    error: fn() -> FaucetError,
}

impl Flushing {
    fn new(name: &'static str, log: &Log) -> Self {
        Self {
            name,
            log: Arc::clone(log),
            failures: Mutex::new(0),
            error: || FaucetError::Sink("refused".into()),
        }
    }

    fn failing(self, n: u32, error: fn() -> FaucetError) -> Self {
        *self.failures.lock().unwrap() = n;
        Self { error, ..self }
    }
}

#[async_trait]
impl Sink for Flushing {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        Ok(records.len())
    }
    async fn flush(&self) -> Result<(), FaucetError> {
        self.log
            .lock()
            .unwrap()
            .push(format!("{} flush", self.name));
        let mut left = self.failures.lock().unwrap();
        if *left > 0 {
            *left -= 1;
            return Err((self.error)());
        }
        Ok(())
    }
}

/// A state store that logs each write.
struct Logged {
    log: Log,
    inner: MemoryStateStore,
}

#[async_trait]
impl StateStore for Logged {
    async fn get(&self, key: &str) -> Result<Option<Value>, FaucetError> {
        self.inner.get(key).await
    }
    async fn put(&self, key: &str, value: &Value) -> Result<(), FaucetError> {
        self.log.lock().unwrap().push(format!("put {value}"));
        self.inner.put(key, value).await
    }
    async fn delete(&self, key: &str) -> Result<(), FaucetError> {
        self.inner.delete(key).await
    }
}

fn fixture() -> (Log, Flushing, Arc<dyn Sink>, Logged) {
    let log: Log = Arc::default();
    let sink = Flushing::new("sink", &log);
    let dlq: Arc<dyn Sink> = Arc::new(Flushing::new("dlq", &log));
    let store = Logged {
        log: Arc::clone(&log),
        inner: MemoryStateStore::new(),
    };
    (log, sink, dlq, store)
}

const IDS: (&str, &str, &str) = ("p", "r", "run");

fn events(log: &Log) -> Vec<String> {
    std::mem::take(&mut *log.lock().unwrap())
}

#[tokio::test]
async fn a_page_flushes_the_sink_then_the_dlq_then_writes_state() {
    let (log, sink, dlq, store) = fixture();
    let ck = Checkpointer::new(&sink, Some(&dlq), Some((&store, "k")), Hold::None, IDS);
    ck.page(&json!(1), Stored::Bare).await.unwrap();
    assert_eq!(events(&log), ["sink flush", "dlq flush", "put 1"]);
    ck.page(&json!(2), Stored::ExactlyOnce(4)).await.unwrap();
    assert_eq!(
        events(&log)[2],
        format!("put {}", crate::idempotency::wrap_state(Some(&json!(2)), 4))
    );
}

#[tokio::test]
async fn a_held_page_flushes_only_the_dlq() {
    let (log, sink, dlq, store) = fixture();
    let ck = Checkpointer::new(&sink, Some(&dlq), Some((&store, "k")), Hold::Overwrite, IDS);
    ck.page(&json!(1), Stored::Bare).await.unwrap();
    assert_eq!(events(&log), ["dlq flush"]);
    ck.persist(&json!(1)).await.unwrap();
    assert_eq!(events(&log), ["put 1"]);
}

#[tokio::test]
async fn a_failed_dlq_flush_stops_the_page_before_its_state_write() {
    let (log, sink, _, store) = fixture();
    let dlq: Arc<dyn Sink> =
        Arc::new(Flushing::new("dlq", &log).failing(1, || FaucetError::Sink("down".into())));
    let ck = Checkpointer::new(&sink, Some(&dlq), Some((&store, "k")), Hold::None, IDS);
    let err = ck.page(&json!(1), Stored::Bare).await.unwrap_err();
    assert!(err.to_string().contains("DLQ sink flush failed"), "{err}");
    assert_eq!(events(&log), ["sink flush", "dlq flush"]);
}

#[tokio::test]
async fn finish_flushes_the_dlq_before_the_sink_and_stops_when_it_fails() {
    let (log, sink, dlq, _) = fixture();
    let ck = Checkpointer::new(&sink, Some(&dlq), None, Hold::None, IDS);
    ck.finish(false).await.unwrap();
    assert_eq!(events(&log), ["dlq flush", "sink flush"]);

    let dlq: Arc<dyn Sink> =
        Arc::new(Flushing::new("dlq", &log).failing(1, || FaucetError::Sink("down".into())));
    let ck = Checkpointer::new(&sink, Some(&dlq), None, Hold::None, IDS);
    let err = ck.finish(false).await.unwrap_err();
    assert!(err.to_string().contains("DLQ sink flush failed"), "{err}");
    assert_eq!(events(&log), ["dlq flush"]);
}

#[tokio::test]
async fn a_cancelled_held_run_does_not_flush_the_sink_at_the_end() {
    let (log, sink, dlq, _) = fixture();
    let ck = Checkpointer::new(&sink, Some(&dlq), None, Hold::Overwrite, IDS);
    ck.finish(true).await.unwrap();
    assert_eq!(events(&log), ["dlq flush"]);
    ck.finish(false).await.unwrap();
    assert_eq!(events(&log), ["dlq flush", "sink flush"]);
    let ck = Checkpointer::new(&sink, None, None, Hold::None, IDS);
    ck.finish(true).await.unwrap();
    assert_eq!(events(&log), ["sink flush"]);
}

#[tokio::test]
async fn unwind_swallows_flush_errors_and_returns_the_original() {
    let log: Log = Arc::default();
    let sink = Flushing::new("sink", &log).failing(1, || FaucetError::Sink("x".into()));
    let dlq: Arc<dyn Sink> =
        Arc::new(Flushing::new("dlq", &log).failing(1, || FaucetError::Sink("y".into())));
    let ck = Checkpointer::new(&sink, Some(&dlq), None, Hold::None, IDS);
    let err = ck.unwind(FaucetError::Source("original".into())).await;
    assert_eq!(
        err.to_string(),
        FaucetError::Source("original".into()).to_string()
    );
    assert_eq!(events(&log), ["dlq flush", "sink flush"]);

    let ck = Checkpointer::new(&sink, Some(&dlq), None, Hold::Overwrite, IDS);
    ck.unwind(FaucetError::Source("o".into())).await;
    assert_eq!(events(&log), ["dlq flush"]);

    let ck = Checkpointer::new(&sink, None, None, Hold::None, IDS).without_unwind_sink_flush();
    ck.unwind(FaucetError::Source("o".into())).await;
    assert!(events(&log).is_empty());
}

fn transient() -> FaucetError {
    FaucetError::Timeout {
        operation: "flush".into(),
        after: Duration::from_millis(1),
    }
}

fn quick_retry() -> RetryPolicy {
    RetryPolicy {
        max_attempts: 3,
        base: Duration::from_millis(1),
        max: Duration::from_millis(1),
        jitter: false,
        ..RetryPolicy::default()
    }
}

#[tokio::test]
async fn a_page_retries_a_transient_flush_but_finish_and_unwind_do_not() {
    let log: Log = Arc::default();
    let policy = quick_retry();
    let sink = Flushing::new("sink", &log).failing(1, transient);
    let ck = Checkpointer::new(&sink, None, None, Hold::None, IDS).with_retry(Some(Retry {
        policy: &policy,
        cancel: None,
    }));
    ck.page(&json!(1), Stored::Bare).await.unwrap();
    assert_eq!(events(&log), ["sink flush", "sink flush"]);

    *sink.failures.lock().unwrap() = 1;
    assert!(ck.finish(false).await.is_err());
    assert_eq!(events(&log), ["sink flush"]);

    *sink.failures.lock().unwrap() = 1;
    ck.unwind(FaucetError::Source("o".into())).await;
    assert_eq!(events(&log), ["sink flush"]);
}

/// A state store whose writes take a while.
struct SlowPut(MemoryStateStore);

#[async_trait]
impl StateStore for SlowPut {
    async fn get(&self, key: &str) -> Result<Option<Value>, FaucetError> {
        self.0.get(key).await
    }
    #[allow(clippy::disallowed_methods)] // a slow backend write
    async fn put(&self, key: &str, value: &Value) -> Result<(), FaucetError> {
        tokio::time::sleep(Duration::from_millis(300)).await;
        self.0.put(key, value).await
    }
    async fn delete(&self, key: &str) -> Result<(), FaucetError> {
        self.0.delete(key).await
    }
}

#[test]
fn the_dlq_flush_timer_does_not_count_the_state_write() {
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    let (_, sink, dlq, _) = fixture();
    let store = SlowPut(MemoryStateStore::new());
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    metrics::with_local_recorder(&recorder, || {
        rt.block_on(async {
            let cp = Checkpointer::new(
                &sink,
                Some(&dlq),
                Some((&store, "k")),
                Hold::None,
                ("p", "r", "run"),
            );
            cp.page(&json!(1), Stored::Bare).await.unwrap();
        })
    });
    let seconds: Vec<f64> = snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .filter(|(k, _, _, _)| k.key().name() == "faucet_sink_dlq_flush_duration_seconds")
        .flat_map(|(_, _, _, v)| match v {
            DebugValue::Histogram(h) => h.into_iter().map(|x| x.into_inner()).collect(),
            _ => Vec::new(),
        })
        .collect();
    assert_eq!(seconds.len(), 1, "{seconds:?}");
    assert!(seconds[0] < 0.25, "{seconds:?}");
}

fn accepted(accounts: &crate::accounting::RowAccounts, n: usize) {
    let mut page = crate::accounting::PageAccount::new(n);
    page.accepted = n;
    page.settle(Some(accounts), ("p", "r", "c")).unwrap();
}

#[tokio::test]
async fn rows_are_confirmed_only_once_their_checkpoint_completed() {
    let (_, sink, dlq, store) = fixture();
    let accounts = crate::accounting::RowAccounts::new();
    let ck = Checkpointer::new(&sink, Some(&dlq), Some((&store, "k")), Hold::None, IDS)
        .with_row_accounts(Some(&accounts));
    accepted(&accounts, 3);
    assert_eq!(accounts.snapshot().confirmed, 0);
    ck.page(&json!(1), Stored::Bare).await.unwrap();
    assert_eq!(accounts.snapshot().confirmed, 3);
    accepted(&accounts, 2);
    ck.finish(false).await.unwrap();
    assert_eq!(accounts.snapshot().confirmed, 5);
}

#[tokio::test]
async fn a_failed_flush_or_a_held_run_confirms_nothing() {
    let (log, _, dlq, store) = fixture();
    let accounts = crate::accounting::RowAccounts::new();
    let sink = Flushing::new("sink", &log).failing(2, || FaucetError::Sink("down".into()));
    let ck = Checkpointer::new(&sink, Some(&dlq), Some((&store, "k")), Hold::None, IDS)
        .with_row_accounts(Some(&accounts));
    accepted(&accounts, 4);
    assert!(ck.page(&json!(1), Stored::Bare).await.is_err());
    assert!(ck.finish(false).await.is_err());
    assert_eq!(accounts.snapshot().confirmed, 0);

    let (_, sink, dlq, store) = fixture();
    let held = crate::accounting::RowAccounts::new();
    let ck = Checkpointer::new(&sink, Some(&dlq), Some((&store, "k")), Hold::Overwrite, IDS)
        .with_row_accounts(Some(&held));
    accepted(&held, 4);
    ck.page(&json!(1), Stored::Bare).await.unwrap();
    ck.finish(false).await.unwrap();
    assert_eq!(held.snapshot().confirmed, 0);
}
