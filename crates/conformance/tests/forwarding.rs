//! The forwarding harnesses and macros (#877): the canonical hook lists match
//! the trait definitions, the macros forward every listed hook, and the
//! harnesses catch a wrapper that does not.

use faucet_conformance::forwarding::{
    ARROW_HOOKS, HookSink, HookSource, SINK_HOOKS, SOURCE_HOOKS, assert_sink_forwards_every_hook,
    assert_source_forwards_every_hook,
};
use faucet_core::{FaucetError, Sink, Source, Value, async_trait, forward_sink, forward_source};
use std::collections::{BTreeSet, HashMap};

const TRAITS: &str = include_str!("../../core/src/traits.rs");

/// The method names declared directly in `pub trait <name>` in `traits.rs`.
fn trait_methods(name: &str) -> BTreeSet<String> {
    let start = TRAITS
        .find(&format!("pub trait {name}: "))
        .unwrap_or_else(|| panic!("trait {name} not found in traits.rs"));
    let body = &TRAITS[start..];
    let end = body.find("\n}\n").expect("trait body closes");
    body[..end]
        .lines()
        .filter_map(|line| {
            let rest = line
                .strip_prefix("    async fn ")
                .or_else(|| line.strip_prefix("    fn "))?;
            let end = rest.find(|c: char| !(c.is_alphanumeric() || c == '_'))?;
            Some(rest[..end].to_string())
        })
        .collect()
}

fn listed(hooks: &[&str]) -> BTreeSet<String> {
    hooks.iter().map(|h| h.to_string()).collect()
}

#[test]
fn source_hooks_match_the_trait() {
    assert_eq!(
        listed(SOURCE_HOOKS),
        trait_methods("Source"),
        "SOURCE_HOOKS must list every Source method (add the new hook to \
         SOURCE_HOOKS, HookSource, assert_source_forwards_every_hook and \
         forward_source!)"
    );
}

#[test]
fn sink_hooks_match_the_trait() {
    assert_eq!(
        listed(SINK_HOOKS),
        trait_methods("Sink"),
        "SINK_HOOKS must list every Sink method (add the new hook to \
         SINK_HOOKS, HookSink, assert_sink_forwards_every_hook and forward_sink!)"
    );
}

#[test]
fn arrow_hooks_are_hooks() {
    for h in ARROW_HOOKS {
        assert!(SOURCE_HOOKS.contains(h) || SINK_HOOKS.contains(h), "{h}");
    }
}

#[test]
fn the_parser_sees_through_attributes_and_generics() {
    let src = trait_methods("Source");
    assert!(src.contains("stream_pages"), "generic method");
    assert!(src.contains("supports_columnar"), "cfg-gated method");
    assert!(src.contains("fetch_with_context"), "required method");
}

struct FullSource {
    inner: HookSource,
}

#[async_trait]
impl Source for FullSource {
    forward_source!(inner);
}

struct FullSink(HookSink);

#[async_trait]
impl Sink for FullSink {
    forward_sink!(0);
}

#[tokio::test]
async fn forward_source_forwards_every_hook() {
    let probe = HookSource::default();
    let wrapped = FullSource {
        inner: probe.clone(),
    };
    assert_source_forwards_every_hook(&wrapped, &probe, &[]).await;
}

#[tokio::test]
async fn forward_sink_forwards_every_hook() {
    let probe = HookSink::default();
    let wrapped = FullSink(probe.clone());
    assert_sink_forwards_every_hook(&wrapped, &probe, &[]).await;
}

#[tokio::test]
async fn forwarded_answers_are_the_inner_connectors() {
    let wrapped = FullSource {
        inner: HookSource::default(),
    };
    assert_eq!(wrapped.connector_name(), "hook");
    assert_eq!(wrapped.state_key().as_deref(), Some("hook"));
    assert!(wrapped.supports_exactly_once());
    assert_eq!(
        wrapped.capture_resume_position().await.unwrap(),
        Some(7.into())
    );
    let sink = FullSink(HookSink::default());
    assert_eq!(
        sink.write_batch(&[Value::Null, Value::Null]).await.unwrap(),
        2
    );
    assert!(sink.supports_idempotent_writes());
}

/// Overrides one hook and takes the trait default for another.
struct Partial {
    inner: Box<dyn Source>,
}

#[async_trait]
impl Source for Partial {
    fn state_key(&self) -> Option<String> {
        Some("mine".into())
    }
    forward_source!(inner; except state_key, supports_exactly_once,);
}

#[tokio::test]
async fn except_leaves_the_listed_hooks_to_the_wrapper() {
    let probe = HookSource::default();
    let wrapped = Partial {
        inner: Box::new(probe.clone()),
    };
    assert_eq!(wrapped.state_key().as_deref(), Some("mine"));
    assert!(!wrapped.supports_exactly_once(), "trait default");
    assert_source_forwards_every_hook(&wrapped, &probe, &["state_key", "supports_exactly_once"])
        .await;
}

/// Forwards nothing: the harness must name every hook.
struct Opaque {
    _inner: HookSink,
}

#[async_trait]
impl Sink for Opaque {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        Ok(records.len())
    }
}

#[tokio::test]
#[should_panic(expected = "Sink hooks not forwarded")]
async fn the_sink_harness_names_unforwarded_hooks() {
    let probe = HookSink::default();
    let wrapped = Opaque {
        _inner: probe.clone(),
    };
    assert_sink_forwards_every_hook(&wrapped, &probe, &[]).await;
}

struct Silent;

#[async_trait]
impl Source for Silent {
    async fn fetch_with_context(
        &self,
        _: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        Ok(vec![])
    }
}

#[tokio::test]
#[should_panic(expected = "Source hooks not forwarded")]
async fn the_source_harness_names_unforwarded_hooks() {
    assert_source_forwards_every_hook(&Silent, &HookSource::default(), &[]).await;
}

#[tokio::test]
#[should_panic(expected = "skip names that are not Sink hooks")]
async fn the_harness_refuses_a_stale_skip_name() {
    let probe = HookSink::default();
    let wrapped = FullSink(probe.clone());
    assert_sink_forwards_every_hook(&wrapped, &probe, &["no_such_hook"]).await;
}

/// Changes `write_batch` only. The reported write must go through it (the
/// trait default), not straight to the inner sink (#879).
struct Doubling(HookSink);

#[async_trait]
impl Sink for Doubling {
    forward_sink!(0; except write_batch);
    async fn write_batch(&self, records: &[Value]) -> Result<usize, faucet_core::FaucetError> {
        Ok(2 * self.0.write_batch(records).await?)
    }
}

#[tokio::test]
async fn a_wrapper_that_changes_write_batch_reports_through_it() {
    let probe = HookSink::default();
    let wrapped = Doubling(probe.clone());
    let report = wrapped.write_batch_reported(&[Value::Null]).await.unwrap();
    assert_eq!(report.accepted, 2, "the wrapper's own write_batch ran");
    assert!(!probe.reached().contains(&"write_batch_reported"));
    assert!(probe.reached().contains(&"write_batch"));
}

/// A governed destination (#875) forwards every hook its layers do not
/// change; with no layer configured it forwards every hook.
#[tokio::test]
async fn a_governed_sink_forwards_every_hook() {
    let probe = HookSink::default();
    let governed = faucet_core::Governance::new()
        .sink_builder()
        .build(Box::new(probe.clone()));
    assert_sink_forwards_every_hook(&governed, &probe, &[]).await;
}
