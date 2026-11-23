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
