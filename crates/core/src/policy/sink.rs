//! [`PolicySink`] — the runtime backstop (#702). A sink decorator that
//! classifies every record about to be written — by column name and by value
//! detector — and applies the rules a static pass could not settle (a detector
//! hit on a column nobody declared, a column the static pass never saw). Per
//! the violated rule's `on_runtime`, the page is refused before it reaches the
//! sink (`fail`) or the offending records are quarantined (`quarantine`).
//!
//! Installed outermost by the CLI executor, so it sees post-transform,
//! post-masking records: a properly masked value no longer matches its
//! detector, which is exactly how `mask:` satisfies a rule at run time too.

use super::compile::CompiledPolicy;
use super::evaluate::{ColumnFacts, SinkFacts, Violation, evaluate};
use super::spec::RuntimeAction;
use crate::error::FaucetError;
use crate::traits::{RowOutcome, Sink};
use async_trait::async_trait;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

/// Labels for `pipeline` / `row` on the violation metric.
#[derive(Debug, Clone)]
pub struct PolicyScope {
    pub pipeline: String,
    pub row: String,
}

/// Wraps a sink and enforces the policy on every page it would write.
pub struct PolicySink {
    inner: Box<dyn Sink>,
    policy: Arc<CompiledPolicy>,
    facts: SinkFacts,
    scope: PolicyScope,
}

impl PolicySink {
    pub fn new(
        inner: Box<dyn Sink>,
        policy: Arc<CompiledPolicy>,
        facts: SinkFacts,
        scope: PolicyScope,
    ) -> Self {
        Self {
            inner,
            policy,
            facts,
            scope,
        }
    }

    /// Classify one record's scalar leaves (by name and by value) and evaluate
    /// the rules; returns the violations with the harshest runtime action.
    fn check_record(&self, record: &Value) -> Vec<Violation> {
        let columns = classify_record(&self.policy, record);
        if columns.is_empty() {
            return Vec::new();
        }
        evaluate(&self.policy, &self.facts, &columns)
    }

    /// The runtime action for a set of violations: `fail` wins over
    /// `quarantine` when any violated rule asks for it.
    fn action_for(&self, violations: &[Violation]) -> RuntimeAction {
        let mut action = RuntimeAction::Quarantine;
        for v in violations {
            if let Some(rule) = self.policy.rules().iter().find(|r| r.name == v.rule)
                && rule.on_runtime == RuntimeAction::Fail
            {
                action = RuntimeAction::Fail;
            }
        }
        action
    }

    fn record_metric(&self, v: &Violation, action: RuntimeAction) {
        metrics::counter!(
            "faucet_policy_violations_total",
            "pipeline" => self.scope.pipeline.clone(),
            "row" => self.scope.row.clone(),
            "rule" => v.rule.clone(),
            "phase" => "runtime",
            "action" => action.as_str(),
        )
        .increment(1);
    }

    /// Screen a page: `Ok(offending indexes with their first violation)` when
    /// every violated rule quarantines, `Err` when any asks to fail the run.
    fn screen(&self, records: &[Value]) -> Result<Vec<(usize, Violation)>, FaucetError> {
        let mut offending = Vec::new();
        for (i, r) in records.iter().enumerate() {
            let mut violations = self.check_record(r);
            if violations.is_empty() {
                continue;
            }
            let action = self.action_for(&violations);
            for v in &violations {
                self.record_metric(v, action);
                tracing::warn!(
                    pipeline = %self.scope.pipeline,
                    row = %self.scope.row,
                    rule = %v.rule,
                    column = %v.column,
                    action = action.as_str(),
                    "policy violation at run time: {v}"
                );
            }
            match action {
                RuntimeAction::Fail => {
                    let first = &violations[0];
                    return Err(FaucetError::PolicyViolation {
                        rule: first.rule.clone(),
                        column: first.column.clone(),
                        message: violations
                            .iter()
                            .map(ToString::to_string)
                            .collect::<Vec<_>>()
                            .join("; "),
                    });
                }
                RuntimeAction::Quarantine => offending.push((i, violations.swap_remove(0))),
            }
        }
        Ok(offending)
    }
}

/// Classify a record's scalar leaves into [`ColumnFacts`] (dot-paths), by
/// name and by value. Columns that earn no label are omitted.
pub fn classify_record(policy: &CompiledPolicy, record: &Value) -> Vec<ColumnFacts> {
    let mut out: BTreeMap<String, (BTreeSet<String>, &'static str)> = BTreeMap::new();
    walk(policy, "", record, &mut out);
    out.into_iter()
        .map(|(name, (labels, via))| ColumnFacts {
            name,
            labels,
            masked: None,
            conservative: false,
            via: Some(via.to_string()),
        })
        .collect()
}

fn walk(
    policy: &CompiledPolicy,
    path: &str,
    value: &Value,
    out: &mut BTreeMap<String, (BTreeSet<String>, &'static str)>,
) {
    match value {
        Value::Object(map) => {
            for (k, v) in map {
                let child = if path.is_empty() {
                    k.clone()
                } else {
                    format!("{path}.{k}")
                };
                walk(policy, &child, v, out);
            }
        }
        Value::Array(items) => {
            for (i, v) in items.iter().enumerate() {
                walk(policy, &format!("{path}.{i}"), v, out);
            }
        }
        Value::Null => {}
        scalar => {
            if path.is_empty() {
                return;
            }
            let mut labels = policy.labels_for_name(path);
            let mut via = "name";
            if let Value::String(s) = scalar {
                let by_value = policy.labels_for_value(s);
                if !by_value.is_empty() {
                    via = if labels.is_empty() {
                        "value"
                    } else {
                        "name+value"
                    };
                    labels.extend(by_value);
                }
            }
            if !labels.is_empty() {
                let entry = out
                    .entry(path.to_string())
                    .or_insert_with(|| (BTreeSet::new(), via));
                entry.0.extend(labels);
            }
        }
    }
}

#[async_trait]
impl Sink for PolicySink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        let offending = self.screen(records)?;
        if offending.is_empty() {
            return self.inner.write_batch(records).await;
        }
        // No per-row channel here: a quarantine verdict with no DLQ attached
        // cannot route anywhere, so it is a failure rather than a silent drop.
        let (_, v) = &offending[0];
        Err(FaucetError::PolicyViolation {
            rule: v.rule.clone(),
            column: v.column.clone(),
            message: format!("{v} (quarantine needs a `dlq:` block; none is attached)"),
        })
    }

    async fn write_batch_partial(&self, records: &[Value]) -> Result<Vec<RowOutcome>, FaucetError> {
        let offending = self.screen(records)?;
        if offending.is_empty() {
            return self.inner.write_batch_partial(records).await;
        }
        let quarantined: BTreeMap<usize, Violation> = offending.into_iter().collect();
        let kept: Vec<Value> = records
            .iter()
            .enumerate()
            .filter(|(i, _)| !quarantined.contains_key(i))
            .map(|(_, r)| r.clone())
            .collect();
        let mut inner_outcomes = if kept.is_empty() {
            Vec::new()
        } else {
            self.inner.write_batch_partial(&kept).await?
        }
        .into_iter();
        let outcomes = (0..records.len())
            .map(|i| match quarantined.get(&i) {
                Some(v) => Err(FaucetError::PolicyViolation {
                    rule: v.rule.clone(),
                    column: v.column.clone(),
                    message: v.to_string(),
                }),
                None => inner_outcomes.next().unwrap_or(Ok(())),
            })
            .collect();
        Ok(outcomes)
    }

    async fn write_batch_idempotent(
        &self,
        records: &[Value],
        scope: &str,
        token: &str,
    ) -> Result<usize, FaucetError> {
        let offending = self.screen(records)?;
        if let Some((_, v)) = offending.first() {
            return Err(FaucetError::PolicyViolation {
                rule: v.rule.clone(),
                column: v.column.clone(),
                message: format!("{v} (exactly-once pages cannot be partially quarantined)"),
            });
        }
        self.inner
            .write_batch_idempotent(records, scope, token)
            .await
    }

    async fn flush(&self) -> Result<(), FaucetError> {
        self.inner.flush().await
    }
    fn connector_name(&self) -> &'static str {
        self.inner.connector_name()
    }
    fn dataset_uri(&self) -> String {
        self.inner.dataset_uri()
    }
    async fn local_outputs(&self) -> Vec<crate::local_outputs::LocalOutput> {
        self.inner.local_outputs().await
    }
    fn supports_idempotent_writes(&self) -> bool {
        self.inner.supports_idempotent_writes()
    }
    fn sink_guarantee(&self) -> crate::idempotency::SinkGuarantee {
        self.inner.sink_guarantee()
    }
    fn write_batch_is_replay_safe(&self) -> bool {
        self.inner.write_batch_is_replay_safe()
    }
    fn dedups_by_key(&self) -> bool {
        self.inner.dedups_by_key()
    }
    fn supported_write_modes(&self) -> &'static [crate::write_mode::WriteMode] {
        self.inner.supported_write_modes()
    }
    async fn last_committed_token(&self, scope: &str) -> Result<Option<String>, FaucetError> {
        self.inner.last_committed_token(scope).await
    }
    async fn current_schema(&self) -> Result<Option<Value>, FaucetError> {
        self.inner.current_schema().await
    }
    fn supports_schema_evolution(&self) -> bool {
        self.inner.supports_schema_evolution()
    }
    async fn evolve_schema(
        &self,
        evolution: &crate::drift::SchemaEvolution,
    ) -> Result<(), FaucetError> {
        self.inner.evolve_schema(evolution).await
    }
    fn supports_cleanup(&self) -> bool {
        self.inner.supports_cleanup()
    }
    fn supports_staged_load(&self) -> bool {
        self.inner.supports_staged_load()
    }
    async fn cleanup_scope(
        &self,
        scope: &BTreeMap<String, Value>,
        seen: &crate::cleanup::SeenKeys,
    ) -> Result<u64, FaucetError> {
        self.inner.cleanup_scope(scope, seen).await
    }
    fn is_overwrite(&self) -> bool {
        self.inner.is_overwrite()
    }
    async fn begin_overwrite(&self) -> Result<(), FaucetError> {
        self.inner.begin_overwrite().await
    }
    async fn commit_overwrite(&self) -> Result<(), FaucetError> {
        self.inner.commit_overwrite().await
    }
    async fn abort_overwrite(&self) -> Result<(), FaucetError> {
        self.inner.abort_overwrite().await
    }
    fn supports_rollback(&self) -> bool {
        self.inner.supports_rollback()
    }
    async fn rollback_run(
        &self,
        run_id: &str,
        opts: &crate::rollback::RollbackOptions,
    ) -> Result<crate::rollback::RollbackOutcome, FaucetError> {
        self.inner.rollback_run(run_id, opts).await
    }
    async fn forget_run(&self, run_id: &str) -> Result<(), FaucetError> {
        self.inner.forget_run(run_id).await
    }
    async fn rewind_commit_token(
        &self,
        scope: &str,
        token: Option<&str>,
    ) -> Result<(), FaucetError> {
        self.inner.rewind_commit_token(scope, token).await
    }
    fn readback_source(&self) -> Option<(String, Value)> {
        self.inner.readback_source()
    }
    fn set_roundtrip_recorder(&self, recorder: Arc<crate::observability::RoundtripRecorder>) {
        self.inner.set_roundtrip_recorder(recorder)
    }
    async fn check(
        &self,
        ctx: &crate::check::CheckContext,
    ) -> Result<crate::check::CheckReport, FaucetError> {
        self.inner.check(ctx).await
    }
    // Columnar batches are screened through a `Value` view; the native byte
    // path is deliberately not forwarded (a policy needs parsed records).
    #[cfg(feature = "arrow")]
    fn supports_columnar(&self) -> bool {
        self.inner.supports_columnar()
    }
    #[cfg(feature = "arrow")]
    async fn write_batch_columnar(
        &self,
        batch: &arrow::array::RecordBatch,
    ) -> Result<usize, FaucetError> {
        let values = crate::columnar::record_batch_to_values(batch)?;
        let offending = self.screen(&values)?;
        if let Some((_, v)) = offending.first() {
            return Err(FaucetError::PolicyViolation {
                rule: v.rule.clone(),
                column: v.column.clone(),
                message: format!("{v} (columnar pages cannot be partially quarantined)"),
            });
        }
        self.inner.write_batch_columnar(batch).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::PolicySpec;
    use serde_json::json;
    use std::sync::Mutex;

    struct Capture(Mutex<Vec<Value>>);
    #[async_trait]
    impl Sink for Capture {
        async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
            self.0.lock().unwrap().extend(records.iter().cloned());
            Ok(records.len())
        }
    }

    fn policy(on_runtime: &str) -> Arc<CompiledPolicy> {
        let spec: PolicySpec = serde_json::from_value(json!({
            "classifications": [
                {"label": "pii", "value_detector": "email"},
                {"label": "pii", "fields": ["ssn"]}
            ],
            "rules": [
                {"name": "pii-eu", "when": {"label": "pii"}, "require": {"residency": ["eu"]}, "on_runtime": on_runtime}
            ]
        }))
        .unwrap();
        Arc::new(CompiledPolicy::compile(&spec).unwrap())
    }

    fn sink(policy: Arc<CompiledPolicy>, residency: &str) -> (PolicySink, Arc<Capture>) {
        let cap = Arc::new(Capture(Mutex::new(Vec::new())));
        struct Fwd(Arc<Capture>);
        #[async_trait]
        impl Sink for Fwd {
            async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
                self.0.write_batch(records).await
            }
        }
        let facts = SinkFacts {
            id: "default".into(),
            kind: "jsonl".into(),
            attributes: [("residency".to_string(), residency.to_string())].into(),
        };
        (
            PolicySink::new(
                Box::new(Fwd(Arc::clone(&cap))),
                policy,
                facts,
                PolicyScope {
                    pipeline: "p".into(),
                    row: "r".into(),
                },
            ),
            cap,
        )
    }

    #[test]
    fn classify_walks_nested_paths_by_name_and_value() {
        let p = policy("fail");
        let cols = classify_record(
            &p,
            &json!({"id": 1, "user": {"contact": "a@b.io", "ssn": "x"}, "tags": ["z@y.io"], "n": null}),
        );
        let names: Vec<(&str, &str)> = cols
            .iter()
            .map(|c| (c.name.as_str(), c.via.as_deref().unwrap()))
            .collect();
        assert_eq!(
            names,
            vec![
                ("tags.0", "value"),
                ("user.contact", "value"),
                ("user.ssn", "name")
            ]
        );
        assert!(classify_record(&p, &json!("scalar")).is_empty());
    }

    #[tokio::test]
    async fn fail_refuses_the_page_before_the_sink_sees_it() {
        let (s, cap) = sink(policy("fail"), "us");
        let err = s
            .write_batch(&[json!({"id": 1}), json!({"email": "a@b.io"})])
            .await
            .unwrap_err();
        assert!(
            matches!(&err, FaucetError::PolicyViolation { rule, column, .. } if rule == "pii-eu" && column == "email"),
            "{err}"
        );
        assert!(cap.0.lock().unwrap().is_empty());
        // A compliant sink passes everything through.
        let (s, cap) = sink(policy("fail"), "eu");
        assert_eq!(
            s.write_batch(&[json!({"email": "a@b.io"})]).await.unwrap(),
            1
        );
        assert_eq!(cap.0.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn quarantine_routes_offenders_and_writes_the_rest() {
        let (s, cap) = sink(policy("quarantine"), "us");
        let outcomes = s
            .write_batch_partial(&[
                json!({"id": 1}),
                json!({"email": "a@b.io"}),
                json!({"id": 3}),
            ])
            .await
            .unwrap();
        assert!(outcomes[0].is_ok() && outcomes[2].is_ok());
        assert!(matches!(
            &outcomes[1],
            Err(FaucetError::PolicyViolation { .. })
        ));
        assert_eq!(cap.0.lock().unwrap().len(), 2);
        // Without a per-row channel, quarantine cannot route: the page fails.
        let err = s
            .write_batch(&[json!({"email": "a@b.io"})])
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("quarantine needs a `dlq:`"),
            "{err}"
        );
        let err = s
            .write_batch_idempotent(&[json!({"email": "a@b.io"})], "s", "t")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("exactly-once"), "{err}");
        // A page with only offenders writes nothing and reports every row.
        let outcomes = s
            .write_batch_partial(&[json!({"email": "c@d.io"})])
            .await
            .unwrap();
        assert_eq!(outcomes.len(), 1);
        assert!(outcomes[0].is_err());
        assert_eq!(cap.0.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn capabilities_forward_and_native_is_not_advertised() {
        let (s, _) = sink(policy("fail"), "eu");
        assert!(s.native_load_capabilities().is_empty());
        assert!(!s.supports_idempotent_writes());
        assert!(s.flush().await.is_ok());
        assert!(s.current_schema().await.unwrap().is_none());
        assert!(s.readback_source().is_none());
        assert!(!s.is_overwrite());
        assert!(s.local_outputs().await.is_empty());
    }
}
