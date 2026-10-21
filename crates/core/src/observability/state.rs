//! Decorator wrapping a `&dyn StateStore` to emit spans + metrics around
//! every get / put / delete call.

use crate::error::FaucetError;
use crate::observability::decorator::error_kind;
use crate::observability::labels::Labels;
use crate::observability::timer::DurationGuard;
use crate::state::StateStore;
use async_trait::async_trait;
use metrics::{Label, SharedString, counter};
use serde_json::Value;
use std::sync::Arc;
use tracing::{Instrument, info_span};

/// Wraps an `Arc<dyn StateStore>` and emits faucet.state.* spans + metrics
/// around every operation.
pub struct InstrumentedStateStore {
    inner: Arc<dyn StateStore>,
    labels: Labels,
}

impl InstrumentedStateStore {
    pub fn new(inner: Arc<dyn StateStore>, labels: Labels) -> Self {
        Self { inner, labels }
    }

    fn base_labels(&self) -> Vec<Label> {
        vec![
            Label::new(
                "pipeline",
                SharedString::from(self.labels.pipeline.to_string()),
            ),
            Label::new("row", SharedString::from(self.labels.row.to_string())),
        ]
    }

    fn error_labels(&self, op: &'static str, kind: &'static str) -> Vec<Label> {
        let mut l = self.base_labels();
        l.push(Label::new("op", SharedString::const_str(op)));
        l.push(Label::new("kind", SharedString::const_str(kind)));
        l
    }
}

#[async_trait]
impl StateStore for InstrumentedStateStore {
    async fn get(&self, key: &str) -> Result<Option<Value>, FaucetError> {
        let span = info_span!("faucet.state.get",
            pipeline = %self.labels.pipeline,
            row = %self.labels.row,
            run_id = %self.labels.run_id,
            key = key,
        );
        let base = self.base_labels();
        let _timer = DurationGuard::new("faucet_state_get_duration_seconds", base.clone());
        let result = self.inner.get(key).instrument(span).await;
        match result {
            Ok(Some(v)) => {
                let mut l = base;
                l.push(Label::new("outcome", SharedString::const_str("hit")));
                counter!("faucet_state_get_total", l).increment(1);
                Ok(Some(v))
            }
            Ok(None) => {
                let mut l = base;
                l.push(Label::new("outcome", SharedString::const_str("miss")));
                counter!("faucet_state_get_total", l).increment(1);
                Ok(None)
            }
            Err(e) => {
                counter!(
                    "faucet_state_errors_total",
                    self.error_labels("get", error_kind(&e))
                )
                .increment(1);
                Err(e)
            }
        }
    }

    async fn put(&self, key: &str, value: &Value) -> Result<(), FaucetError> {
        let span = info_span!("faucet.state.put",
            pipeline = %self.labels.pipeline,
            row = %self.labels.row,
            run_id = %self.labels.run_id,
            key = key,
        );
        let base = self.base_labels();
        let _timer = DurationGuard::new("faucet_state_put_duration_seconds", base.clone());
        let result = self.inner.put(key, value).instrument(span).await;
        match result {
            Ok(()) => {
                counter!("faucet_state_put_total", base).increment(1);
                Ok(())
            }
            Err(e) => {
                counter!(
                    "faucet_state_errors_total",
                    self.error_labels("put", error_kind(&e))
                )
                .increment(1);
                Err(e)
            }
        }
    }

    async fn delete(&self, key: &str) -> Result<(), FaucetError> {
        let span = info_span!("faucet.state.delete",
            pipeline = %self.labels.pipeline,
            row = %self.labels.row,
            run_id = %self.labels.run_id,
            key = key,
        );
        let base = self.base_labels();
        let _timer = DurationGuard::new("faucet_state_delete_duration_seconds", base.clone());
        let result = self.inner.delete(key).instrument(span).await;
        match result {
            Ok(()) => {
                counter!("faucet_state_delete_total", base).increment(1);
                Ok(())
            }
            Err(e) => {
                counter!(
                    "faucet_state_errors_total",
                    self.error_labels("delete", error_kind(&e))
                )
                .increment(1);
                Err(e)
            }
        }
    }

    async fn check(
        &self,
        ctx: &crate::check::CheckContext,
    ) -> Result<crate::check::CheckReport, FaucetError> {
        self.inner.check(ctx).await
    }

    fn supports_list(&self) -> bool {
        self.inner.supports_list()
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, FaucetError> {
        self.count_error("list", self.inner.list(prefix).await)
    }

    fn supports_compare_and_put(&self) -> bool {
        self.inner.supports_compare_and_put()
    }

    async fn compare_and_put(
        &self,
        key: &str,
        expected: Option<&Value>,
        value: &Value,
    ) -> Result<bool, FaucetError> {
        let base = self.base_labels();
        let _timer = DurationGuard::new("faucet_state_put_duration_seconds", base.clone());
        let written = self.count_error(
            "compare_and_put",
            self.inner.compare_and_put(key, expected, value).await,
        )?;
        if written {
            counter!("faucet_state_put_total", base).increment(1);
        }
        Ok(written)
    }

    fn supports_atomic_batch(&self) -> bool {
        self.inner.supports_atomic_batch()
    }

    async fn put_batch(&self, entries: &[(String, Value)]) -> Result<(), FaucetError> {
        let base = self.base_labels();
        let _timer = DurationGuard::new("faucet_state_put_duration_seconds", base.clone());
        self.count_error("put_batch", self.inner.put_batch(entries).await)?;
        counter!("faucet_state_put_total", base).increment(entries.len() as u64);
        Ok(())
    }
}

impl InstrumentedStateStore {
    fn count_error<T>(
        &self,
        op: &'static str,
        r: Result<T, FaucetError>,
    ) -> Result<T, FaucetError> {
        if let Err(e) = &r {
            counter!("faucet_state_errors_total", self.error_labels(op, error_kind(e)))
                .increment(1);
        }
        r
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observability::decorator::source_tests::{LOCK, labels, snapshotter};
    use crate::state::MemoryStateStore;
    use metrics_util::debugging::DebugValue;
    use serde_json::json;

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn get_records_hit_outcome() {
        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let snap = snapshotter();
        let inner: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
        inner.put("k", &json!("v")).await.unwrap();
        let wrapped = InstrumentedStateStore::new(Arc::clone(&inner), labels());
        let result = wrapped.get("k").await.unwrap();
        assert_eq!(result, Some(json!("v")));
        let snapshot = snap.snapshot();
        let found = snapshot.into_vec().into_iter().any(|(key, _u, _d, v)| {
            key.key().name() == "faucet_state_get_total"
                && key
                    .key()
                    .labels()
                    .any(|l| l.key() == "outcome" && l.value() == "hit")
                && matches!(v, DebugValue::Counter(c) if c >= 1)
        });
        assert!(found, "expected faucet_state_get_total{{outcome=hit}}");
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn get_records_miss_outcome() {
        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let snap = snapshotter();
        let inner: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
        let wrapped = InstrumentedStateStore::new(inner, labels());
        assert!(wrapped.get("absent").await.unwrap().is_none());
        let snapshot = snap.snapshot();
        let found = snapshot.into_vec().into_iter().any(|(key, _u, _d, v)| {
            key.key().name() == "faucet_state_get_total"
                && key
                    .key()
                    .labels()
                    .any(|l| l.key() == "outcome" && l.value() == "miss")
                && matches!(v, DebugValue::Counter(c) if c >= 1)
        });
        assert!(found, "expected faucet_state_get_total{{outcome=miss}}");
    }

    struct Capable(MemoryStateStore);
    #[async_trait]
    impl StateStore for Capable {
        async fn get(&self, key: &str) -> Result<Option<Value>, FaucetError> {
            self.0.get(key).await
        }
        async fn put(&self, key: &str, value: &Value) -> Result<(), FaucetError> {
            self.0.put(key, value).await
        }
        async fn delete(&self, key: &str) -> Result<(), FaucetError> {
            self.0.delete(key).await
        }
        fn supports_list(&self) -> bool {
            true
        }
        async fn list(&self, _prefix: &str) -> Result<Vec<String>, FaucetError> {
            Ok(vec!["from-inner".into()])
        }
        fn supports_compare_and_put(&self) -> bool {
            true
        }
        async fn compare_and_put(
            &self,
            _key: &str,
            _expected: Option<&Value>,
            _value: &Value,
        ) -> Result<bool, FaucetError> {
            Err(FaucetError::State("inner cas".into()))
        }
        fn supports_atomic_batch(&self) -> bool {
            true
        }
        async fn put_batch(&self, _entries: &[(String, Value)]) -> Result<(), FaucetError> {
            Err(FaucetError::State("inner batch".into()))
        }
        async fn check(
            &self,
            _ctx: &crate::check::CheckContext,
        ) -> Result<crate::check::CheckReport, FaucetError> {
            Err(FaucetError::State("inner check".into()))
        }
    }

    #[tokio::test]
    async fn forwards_every_optional_capability_to_the_inner_store() {
        let wrapped = InstrumentedStateStore::new(
            Arc::new(Capable(MemoryStateStore::new())),
            labels(),
        );
        assert!(wrapped.supports_list());
        assert!(wrapped.supports_compare_and_put());
        assert!(wrapped.supports_atomic_batch());
        assert_eq!(wrapped.list("x").await.unwrap(), vec!["from-inner"]);
        let e = wrapped
            .compare_and_put("k", None, &json!(1))
            .await
            .unwrap_err();
        assert!(e.to_string().contains("inner cas"), "{e}");
        let e = wrapped
            .put_batch(&[("k".into(), json!(1))])
            .await
            .unwrap_err();
        assert!(e.to_string().contains("inner batch"), "{e}");
        let e = wrapped
            .check(&crate::check::CheckContext::default())
            .await
            .unwrap_err();
        assert!(e.to_string().contains("inner check"), "{e}");
    }

    #[tokio::test]
    async fn compare_and_put_and_batches_reach_a_real_store() {
        let inner: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
        let wrapped = InstrumentedStateStore::new(Arc::clone(&inner), labels());
        assert!(wrapped.compare_and_put("k", None, &json!(1)).await.unwrap());
        assert!(!wrapped.compare_and_put("k", None, &json!(2)).await.unwrap());
        wrapped
            .put_batch(&[("a".into(), json!(1)), ("b".into(), json!(2))])
            .await
            .unwrap();
        assert_eq!(inner.get("b").await.unwrap(), Some(json!(2)));
    }

    #[test]
    fn implements_every_state_store_method() {
        let traits = include_str!("../state.rs");
        let start = traits.find("pub trait StateStore").expect("trait");
        let body = &traits[start..];
        let body = &body[..body.find("\n}\n").expect("trait end")];
        let mine = include_str!("state.rs");
        let impl_start = mine.find("impl StateStore for InstrumentedStateStore").expect("impl");
        let mine = &mine[impl_start..];
        for line in body.lines() {
            let t = line.trim_start();
            let Some(rest) = t.strip_prefix("async fn ").or_else(|| t.strip_prefix("fn ")) else {
                continue;
            };
            let name: String = rest.chars().take_while(|c| c.is_alphanumeric() || *c == '_').collect();
            assert!(
                mine.contains(&format!("fn {name}(")),
                "InstrumentedStateStore must forward `{name}`"
            );
        }
    }
}
