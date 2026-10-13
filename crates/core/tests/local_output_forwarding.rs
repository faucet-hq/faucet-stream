//! Every sink decorator must forward `local_outputs()` (#587).
//!
//! The retention GC can only delete files it was told about, and it learns about
//! them from `Sink::local_outputs()` on the outermost sink in the decorator
//! chain. A decorator that falls back to the trait's empty default therefore does
//! not fail, log, or misbehave in any visible way — it silently hides every file
//! its inner sink created, and those files are never reclaimed. The symptom is a
//! disk filling up weeks later, in a build with one particular feature enabled.
//!
//! That is precisely the bug review caught in `InstrumentedSink`, which forwarded
//! neither `local_outputs()` nor `dataset_uri()` while every sibling decorator
//! did. It was invisible to a grep for existing forwards, and no test covered it
//! because the end-to-end suite only ever exercises the default sink chain —
//! `MetadataSink`, `CleanupTracker`, and `InstrumentedSink` are each activated by
//! a config or feature that the happy path does not use.
//!
//! So this test enumerates the decorators explicitly and asserts the pair of
//! identity/provenance methods survives each one. Adding a decorator to core
//! means adding it here; the cost of forgetting is unreclaimed data.
//!
//! `InstrumentedSink` is covered by a unit test in `observability::decorator`
//! instead, since that module is private — widening the API for a test would be
//! the wrong trade.

use faucet_core::local_outputs::{LocalOutput, LocalOutputLog};
use faucet_core::{FaucetError, Sink};
use serde_json::{Value, json};

const PATH: &str = "/tmp/faucet-forwarding-probe.jsonl";
const URI: &str = "file:///tmp/faucet-forwarding-probe.jsonl";

/// A stand-in for a local-file sink: reports one output and a real dataset URI.
struct FileSink {
    outputs: LocalOutputLog,
}

impl FileSink {
    fn new() -> Self {
        let outputs = LocalOutputLog::new();
        // `false` = faucet created it, i.e. the collectable case. If a decorator
        // drops the forward, this is the entry that goes missing.
        outputs.record_open(PATH, false);
        Self { outputs }
    }
}

#[async_trait::async_trait]
impl Sink for FileSink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        Ok(records.len())
    }
    fn connector_name(&self) -> &'static str {
        "jsonl"
    }
    fn dataset_uri(&self) -> String {
        URI.to_string()
    }
    async fn local_outputs(&self) -> Vec<LocalOutput> {
        self.outputs.snapshot()
    }
}

/// Assert `decorated` still reports the inner sink's file and URI.
async fn assert_forwards(decorated: &dyn Sink, decorator: &str) {
    // A write first, so the assertion holds for a decorator that only populates
    // state once records flow through it.
    decorated.write_batch(&[json!({"id": 1})]).await.unwrap();

    let outputs = decorated.local_outputs().await;
    assert_eq!(
        outputs.len(),
        1,
        "{decorator} dropped local_outputs() — every file its inner sink writes \
         would be invisible to the retention GC and never reclaimed"
    );
    assert_eq!(
        outputs[0].path,
        std::path::PathBuf::from(PATH),
        "{decorator}"
    );
    assert!(
        !outputs[0].pre_existing,
        "{decorator} must forward the classification verbatim: reporting a \
         faucet-created file as pre-existing makes it permanently uncollectable"
    );
    assert_eq!(
        decorated.dataset_uri(),
        URI,
        "{decorator} dropped dataset_uri() — lineage and the catalog would record \
         `jsonl://unknown` for this sink"
    );
}

#[tokio::test]
async fn metadata_sink_forwards() {
    use faucet_core::{CompiledMetadata, Governance, MetadataColumnsSpec, MetadataContext};

    let spec = MetadataColumnsSpec::default();
    let meta = CompiledMetadata::compile(&spec)
        .expect("compile")
        .expect("metadata columns enabled by default when the block is present");
    // Built the way every entry point builds it (#875).
    let sink = Governance::new()
        .with_metadata(
            meta,
            MetadataContext {
                run_id: "run-1".into(),
                source: "csv".into(),
            },
        )
        .sink_builder()
        .build(Box::new(FileSink::new()));
    assert_forwards(&sink, "MetadataSink").await;
}

#[tokio::test]
async fn cleanup_tracker_forwards() {
    use faucet_core::{CleanupPolicy, DEFAULT_MAX_KEYS};
    use std::collections::BTreeMap;

    let inner = FileSink::new();
    let policy = CleanupPolicy::new(
        BTreeMap::from([("contact_id".to_string(), json!(7))]),
        vec!["id".to_string()],
        DEFAULT_MAX_KEYS,
    )
    .expect("valid policy");
    let sink = faucet_core::cleanup::CleanupTracker::new(&inner, &policy);
    assert_forwards(&sink, "CleanupTracker").await;
}

#[tokio::test]
async fn a_decorator_that_drops_the_forward_is_caught() {
    // Guards the guard: proves `assert_forwards` actually fails on a decorator
    // that omits the methods, rather than passing vacuously. Without this, a
    // future refactor could weaken the helper and every test above would still
    // report green.
    struct Forgetful<'a> {
        inner: &'a dyn Sink,
    }

    #[async_trait::async_trait]
    impl Sink for Forgetful<'_> {
        async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
            self.inner.write_batch(records).await
        }
        fn connector_name(&self) -> &'static str {
            self.inner.connector_name()
        }
        // Deliberately no `local_outputs` / `dataset_uri` — the trait defaults
        // silently take over, which is exactly the failure mode.
    }

    let inner = FileSink::new();
    let forgetful = Forgetful { inner: &inner };

    assert!(
        forgetful.local_outputs().await.is_empty(),
        "the default really does swallow the inner sink's outputs"
    );
    let caught = std::panic::AssertUnwindSafe(assert_forwards(&forgetful, "Forgetful"));
    assert!(
        futures::FutureExt::catch_unwind(caught).await.is_err(),
        "assert_forwards must fail for a decorator that drops the forward"
    );
}
