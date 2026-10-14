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
use crate::masking::CompiledMasking;
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
    masking: Option<Arc<CompiledMasking>>,
    dlq: Option<SinkFacts>,
    /// The wrapped sink is a DLQ: records are envelopes, the payload is
    /// classified, and any violation fails the write (#870).
    envelopes: bool,
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
            masking: None,
            dlq: None,
            envelopes: false,
        }
    }

    /// Enforce the policy at a DLQ (#870): every envelope's payload is
    /// classified against the DLQ's own `facts`, whichever pass quarantined it
    /// (quality, contract, drift, policy, or a per-row sink failure). There is
    /// no DLQ behind a DLQ, so any violation — `quarantine` rules included —
    /// fails the write, and with it the run.
    pub fn for_dlq(
        inner: Box<dyn Sink>,
        policy: Arc<CompiledPolicy>,
        dlq: SinkFacts,
        scope: PolicyScope,
    ) -> Self {
        Self {
            envelopes: true,
            ..Self::new(inner, policy, dlq, scope)
        }
    }

    /// The DLQ a quarantined record is routed to, evaluated as a destination
    /// of its own: a quarantine whose record the DLQ may not receive fails
    /// the run instead.
    pub fn with_dlq_destination(mut self, dlq: SinkFacts) -> Self {
        self.dlq = Some(dlq);
        self
    }

    /// The masking pass that ran upstream for this sink, so a column it
    /// rewrote by name counts as masked with that action (as in the static
    /// pass) instead of breaching a `mask:` rule.
    pub fn with_masking(mut self, masking: Arc<CompiledMasking>) -> Self {
        self.masking = Some(masking);
        self
    }

    /// Classify one record's scalar leaves (by name and by value) and evaluate
    /// the rules; returns the violations with the harshest runtime action.
    fn check_record(
        &self,
        record: &Value,
        names: &mut NameLabels,
    ) -> (Vec<ColumnFacts>, Vec<Violation>) {
        let columns = classify_cached(&self.policy, record, self.masking.as_deref(), names);
        if columns.is_empty() {
            return (columns, Vec::new());
        }
        let violations = evaluate(&self.policy, &self.facts, &columns);
        (columns, violations)
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

    /// Screen an exactly-once page, which commits whole: any violation fails it.
    fn screen_idempotent(&self, records: &[Value]) -> Result<(), FaucetError> {
        match self.screen(records)?.first() {
            Some((_, v)) => Err(FaucetError::PolicyViolation {
                rule: v.rule.clone(),
                column: v.column.clone(),
                message: format!("{v} (exactly-once pages cannot be partially quarantined)"),
            }),
            None => Ok(()),
        }
    }

    /// Screen a page: `Ok(offending indexes with their first violation)` when
    /// every violated rule quarantines, `Err` when any asks to fail the run.
    fn screen(&self, records: &[Value]) -> Result<Vec<(usize, Violation)>, FaucetError> {
        let mut offending = Vec::new();
        let mut names = NameLabels::new();
        for (i, r) in records.iter().enumerate() {
            let r = if self.envelopes {
                r.get("payload").unwrap_or(r)
            } else {
                r
            };
            let (columns, mut violations) = self.check_record(r, &mut names);
            if violations.is_empty() {
                continue;
            }
            let mut action = if self.envelopes {
                RuntimeAction::Fail
            } else {
                self.action_for(&violations)
            };
            if action == RuntimeAction::Quarantine
                && let Some(dlq) = &self.dlq
            {
                let at_dlq =
                    super::evaluate::dlq_violations(&self.policy, dlq, &columns, &violations);
                if !at_dlq.is_empty() {
                    action = RuntimeAction::Fail;
                    violations = at_dlq;
                }
            }
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
    classify_record_masked(policy, record, None)
}

/// [`classify_record`] for records that went through `masking`: a name label
/// on a container covers every leaf beneath it, and a leaf the masking pass
/// rewrote by name (itself or through an ancestor) reports that action.
pub fn classify_record_masked(
    policy: &CompiledPolicy,
    record: &Value,
    masking: Option<&CompiledMasking>,
) -> Vec<ColumnFacts> {
    classify_cached(policy, record, masking, &mut NameLabels::new())
}

/// Labels each dot-path earns by name, memoized across the records of a page
/// so the classification regexes run once per path, not once per record.
type NameLabels = std::collections::HashMap<String, BTreeSet<String>>;

fn classify_cached(
    policy: &CompiledPolicy,
    record: &Value,
    masking: Option<&CompiledMasking>,
    names: &mut NameLabels,
) -> Vec<ColumnFacts> {
    let mut out: BTreeMap<String, (BTreeSet<String>, &'static str)> = BTreeMap::new();
    walk(policy, "", record, &BTreeSet::new(), &mut out, names);
    out.into_iter()
        .map(|(name, (labels, via))| ColumnFacts {
            masked: masking
                .and_then(|m| m.name_action_for(&name))
                .map(str::to_string),
            name,
            labels,
            conservative: false,
            via: Some(via.to_string()),
        })
        .collect()
}

fn walk(
    policy: &CompiledPolicy,
    path: &str,
    value: &Value,
    inherited: &BTreeSet<String>,
    out: &mut BTreeMap<String, (BTreeSet<String>, &'static str)>,
    names: &mut NameLabels,
) {
    let mut by_name = |path: &str| -> BTreeSet<String> {
        if path.is_empty() {
            return BTreeSet::new();
        }
        names
            .entry(path.to_string())
            .or_insert_with(|| policy.labels_for_name(path))
            .clone()
    };
    match value {
        Value::Object(map) => {
            let mut labels = inherited.clone();
            labels.extend(by_name(path));
            for (k, v) in map {
                let child = if path.is_empty() {
                    k.clone()
                } else {
                    format!("{path}.{k}")
                };
                walk(policy, &child, v, &labels, out, names);
            }
        }
        Value::Array(items) => {
            let mut labels = inherited.clone();
            labels.extend(by_name(path));
            for (i, v) in items.iter().enumerate() {
                walk(policy, &format!("{path}.{i}"), v, &labels, out, names);
            }
        }
        Value::Null => {}
        scalar => {
            if path.is_empty() {
                return;
            }
            let mut labels = by_name(path);
            labels.extend(inherited.iter().cloned());
            let mut via = "name";
            let by_value = policy.labels_for_scalar(scalar);
            if !by_value.is_empty() {
                via = if labels.is_empty() {
                    "value"
                } else {
                    "name+value"
                };
                labels.extend(by_value);
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
    // Classification needs parsed rows, so the native byte path stays off;
    // everything else reaches the wrapped sink.
    crate::forward_sink!(inner; except
        write_batch, write_batch_reported, write_governed, write_batch_partial,
        write_batch_idempotent, write_batch_idempotent_reported,
        write_batch_columnar, native_load_capabilities, load_native);

    async fn write_batch_reported(
        &self,
        records: &[Value],
    ) -> Result<crate::accounting::WriteReport, FaucetError> {
        let offending = self.screen(records)?;
        if let Some((_, v)) = offending.first() {
            return Err(FaucetError::PolicyViolation {
                rule: v.rule.clone(),
                column: v.column.clone(),
                message: format!("{v} (quarantine needs a `dlq:` block; none is attached)"),
            });
        }
        self.inner.write_batch_reported(records).await
    }

    async fn write_governed(
        &self,
        page: &crate::governance::GovernedPage<'_>,
    ) -> Result<crate::accounting::WriteReport, FaucetError> {
        let offending = self.screen(page.records())?;
        if let Some((_, v)) = offending.first() {
            return Err(FaucetError::PolicyViolation {
                rule: v.rule.clone(),
                column: v.column.clone(),
                message: format!("{v} (quarantine needs a `dlq:` block; none is attached)"),
            });
        }
        self.inner.write_governed(page).await
    }

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
        let inner_outcomes = if kept.is_empty() {
            Vec::new()
        } else {
            self.inner.write_batch_partial(&kept).await?
        };
        if inner_outcomes.len() != kept.len() {
            return Err(crate::dlq::outcome_count_mismatch(
                self.inner.connector_name(),
                inner_outcomes.len(),
                kept.len(),
            ));
        }
        let mut inner_outcomes = inner_outcomes.into_iter();
        let outcomes = (0..records.len())
            .map(|i| match quarantined.get(&i) {
                Some(v) => Err(FaucetError::PolicyViolation {
                    rule: v.rule.clone(),
                    column: v.column.clone(),
                    message: v.to_string(),
                }),
                None => inner_outcomes.next().expect("length checked above"),
            })
            .collect();
        Ok(outcomes)
    }

    #[allow(deprecated)] // drives the 1.x lifecycle hook until faucet-core 2.0 (RFC 0014)
    async fn write_batch_idempotent(
        &self,
        records: &[Value],
        scope: &str,
        token: &str,
    ) -> Result<usize, FaucetError> {
        self.screen_idempotent(records)?;
        self.inner
            .write_batch_idempotent(records, scope, token)
            .await
    }

    #[allow(deprecated)] // drives the 1.x lifecycle hook until faucet-core 2.0 (RFC 0014)
    async fn write_batch_idempotent_reported(
        &self,
        records: &[Value],
        scope: &str,
        token: &str,
    ) -> Result<crate::accounting::WriteReport, FaucetError> {
        self.screen_idempotent(records)?;
        self.inner
            .write_batch_idempotent_reported(records, scope, token)
            .await
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

#[allow(deprecated)] // the tests drive the 1.x lifecycle hooks directly (RFC 0014)
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

    struct Fwd(Arc<Capture>);
    #[async_trait]
    impl Sink for Fwd {
        async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
            self.0.write_batch(records).await
        }
    }

    fn sink(policy: Arc<CompiledPolicy>, residency: &str) -> (PolicySink, Arc<Capture>) {
        let cap = Arc::new(Capture(Mutex::new(Vec::new())));
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

    #[tokio::test]
    async fn a_short_inner_outcome_vector_is_an_error() {
        struct Short;
        #[async_trait]
        impl Sink for Short {
            async fn write_batch(&self, r: &[Value]) -> Result<usize, FaucetError> {
                Ok(r.len())
            }
            async fn write_batch_partial(
                &self,
                _r: &[Value],
            ) -> Result<Vec<RowOutcome>, FaucetError> {
                Ok(Vec::new())
            }
        }
        let facts = SinkFacts {
            id: "default".into(),
            kind: "jsonl".into(),
            attributes: [("residency".to_string(), "us".to_string())].into(),
        };
        let s = PolicySink::new(
            Box::new(Short),
            policy("quarantine"),
            facts,
            PolicyScope {
                pipeline: "p".into(),
                row: "r".into(),
            },
        );
        let err = s
            .write_batch_partial(&[json!({"e": "a@b.io"}), json!({"id": 1})])
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("0 per-row outcomes for 1 rows"),
            "{err}"
        );
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

    fn phones_policy() -> Arc<CompiledPolicy> {
        let spec: PolicySpec = serde_json::from_value(json!({
            "classifications": [{"label": "pii", "fields": ["phones", "card"]}],
            "rules": [{"name": "pii-masked", "when": {"label": "pii"}, "mask": ["hash", "partial"], "on_runtime": "fail"}]
        }))
        .unwrap();
        Arc::new(CompiledPolicy::compile(&spec).unwrap())
    }

    fn phones_masking(action: &str) -> Arc<CompiledMasking> {
        let spec: crate::masking::MaskingSpec = serde_json::from_value(json!({
            "key": "k",
            "rules": [{"match": {"field_pattern": "^(phones|card)$"}, "action": {"type": action}}]
        }))
        .unwrap();
        Arc::new(CompiledMasking::compile(&spec).unwrap())
    }

    #[test]
    fn a_container_label_covers_its_leaves_and_masking_is_reported() {
        let p = phones_policy();
        let rec = json!({"phones": ["+14155552671"], "card": {"number": "4111"}, "id": 1});
        let cols = classify_record(&p, &rec);
        let names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["card.number", "phones.0"]);
        assert!(cols.iter().all(|c| c.masked.is_none()));

        let m = phones_masking("hash");
        let cols = classify_record_masked(&p, &rec, Some(&m));
        assert!(cols.iter().all(|c| c.masked.as_deref() == Some("hash")));
    }

    #[tokio::test]
    async fn masked_containers_pass_and_unmasked_ones_are_refused() {
        let p = phones_policy();
        let m = phones_masking("hash");
        let records = crate::masking::apply_masking(
            vec![json!({"phones": ["+14155552671"], "card": {"number": "4111111111111111"}})],
            &m,
        )
        .records;
        let (s, cap) = sink(Arc::clone(&p), "us");
        let s = s.with_masking(Arc::clone(&m));
        assert_eq!(s.write_batch(&records).await.unwrap(), 1);
        assert_eq!(cap.0.lock().unwrap().len(), 1);

        let (s, cap) = sink(Arc::clone(&p), "us");
        let err = s
            .write_batch(&[json!({"phones": ["+14155552671"]})])
            .await
            .unwrap_err();
        assert!(
            matches!(&err, FaucetError::PolicyViolation { column, .. } if column == "phones.0"),
            "{err}"
        );
        assert!(cap.0.lock().unwrap().is_empty());

        let (s, _) = sink(p, "us");
        let s = s.with_masking(phones_masking("tokenize"));
        let err = s
            .write_batch(&[json!({"phones": ["tok"]})])
            .await
            .unwrap_err();
        assert!(err.to_string().contains("only masked with"), "{err}");
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
    async fn quarantine_into_a_non_compliant_dlq_fails_instead() {
        let dlq = |residency: &str| SinkFacts {
            id: "dlq".into(),
            kind: "jsonl".into(),
            attributes: [("residency".to_string(), residency.to_string())].into(),
        };
        let (s, cap) = sink(policy("quarantine"), "us");
        let s = s.with_dlq_destination(dlq("us"));
        let err = s
            .write_batch_partial(&[json!({"id": 1}), json!({"email": "a@b.io"})])
            .await
            .unwrap_err();
        assert!(
            matches!(&err, FaucetError::PolicyViolation { column, .. } if column == "email"),
            "{err}"
        );
        assert!(err.to_string().contains("`dlq`"), "{err}");
        assert!(cap.0.lock().unwrap().is_empty());

        let (s, cap) = sink(policy("quarantine"), "us");
        let s = s.with_dlq_destination(dlq("eu"));
        let outcomes = s
            .write_batch_partial(&[json!({"id": 1}), json!({"email": "a@b.io"})])
            .await
            .unwrap();
        assert!(outcomes[0].is_ok() && outcomes[1].is_err());
        assert_eq!(cap.0.lock().unwrap().len(), 1);
    }

    /// #862 M-020: the DLQ check covers every labelled column of the record,
    /// not only the one that broke the main sink's rule.
    #[tokio::test]
    async fn the_dlq_check_covers_every_column_of_the_quarantined_record() {
        let spec: PolicySpec = serde_json::from_value(json!({
            "classifications": [
                {"label": "email", "fields": ["email"]},
                {"label": "ssn", "fields": ["ssn"]}
            ],
            "rules": [
                {"name": "a", "when": {"label": "email"}, "require": {"residency": ["us"]}, "on_runtime": "quarantine"},
                {"name": "b", "when": {"label": "ssn"}, "require": {"residency": ["eu"]}, "on_runtime": "quarantine"}
            ]
        }))
        .unwrap();
        let (s, cap) = sink(Arc::new(CompiledPolicy::compile(&spec).unwrap()), "eu");
        let s = s.with_dlq_destination(SinkFacts {
            id: "dlq".into(),
            kind: "jsonl".into(),
            attributes: [("residency".to_string(), "us".to_string())].into(),
        });
        let err = s
            .write_batch_partial(&[json!({"email": "x@y.com", "ssn": "123-45-6789"})])
            .await
            .unwrap_err();
        assert!(
            matches!(&err, FaucetError::PolicyViolation { rule, column, .. } if rule == "b" && column == "ssn"),
            "{err}"
        );
        assert!(cap.0.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_card_number_stored_as_a_json_integer_is_classified() {
        let spec: PolicySpec = serde_json::from_value(json!({
            "classifications": [{"label": "pan", "value_detector": "credit_card"}],
            "rules": [{"name": "no-pan", "when": {"label": "pan"}, "deny": true, "on_runtime": "fail"}]
        }))
        .unwrap();
        let (s, cap) = sink(Arc::new(CompiledPolicy::compile(&spec).unwrap()), "us");
        let err = s
            .write_batch(&[json!({"cc": 4111111111111111u64})])
            .await
            .unwrap_err();
        assert!(
            matches!(&err, FaucetError::PolicyViolation { column, .. } if column == "cc"),
            "{err}"
        );
        assert_eq!(
            s.write_batch(&[json!({"n": 42, "ok": true})])
                .await
                .unwrap(),
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

    /// Every capability the decorator does not own is the inner sink's answer
    /// (defaults here, since `Fwd` overrides only `write_batch`) — so a
    /// policy-wrapped upsert / exactly-once / overwrite sink keeps working.
    #[tokio::test]
    async fn every_forwarded_capability_reaches_the_inner_sink() {
        use crate::cleanup::SeenKeys;
        use crate::drift::SchemaEvolution;
        use crate::idempotency::SinkGuarantee;
        use crate::observability::{RoundtripRecorder, RoundtripSide};
        use crate::rollback::{RollbackMode, RollbackOptions};
        use crate::write_mode::WriteMode;
        let (s, cap) = sink(policy("fail"), "eu");
        assert!(!s.connector_name().is_empty());
        assert!(!s.dataset_uri().is_empty());
        assert_eq!(s.sink_guarantee(), SinkGuarantee::AtLeastOnce);
        assert!(!s.write_batch_is_replay_safe());
        assert!(!s.dedups_by_key());
        assert_eq!(s.batch_atomicity(), crate::dlq::BatchAtomicity::BestEffort);
        assert_eq!(s.supported_write_modes(), &[WriteMode::Append]);
        assert!(s.last_committed_token("scope").await.unwrap().is_none());
        assert!(!s.supports_schema_evolution());
        assert!(
            s.evolve_schema(&SchemaEvolution {
                additions: Vec::new(),
                widenings: Vec::new(),
                relax_nullability: Vec::new(),
            })
            .await
            .is_err(),
            "the inner sink cannot evolve, so neither can the wrapper"
        );
        assert!(!s.supports_cleanup());
        assert!(!s.supports_staged_load());
        assert!(
            s.cleanup_scope(&BTreeMap::new(), &SeenKeys::new())
                .await
                .is_err()
        );
        // The overwrite lifecycle is the inner sink's: `Fwd` is append-only,
        // so each step answers exactly as an unwrapped `Fwd` would.
        let plain = Fwd(Arc::new(Capture(Mutex::new(Vec::new()))));
        assert_eq!(
            s.begin_overwrite().await.is_ok(),
            plain.begin_overwrite().await.is_ok()
        );
        assert_eq!(
            s.commit_overwrite().await.is_ok(),
            plain.commit_overwrite().await.is_ok()
        );
        assert_eq!(
            s.abort_overwrite().await.is_ok(),
            plain.abort_overwrite().await.is_ok()
        );
        assert!(s.complete_run().await.is_ok());
        assert!(!s.supports_rollback());
        assert!(
            s.rollback_run(
                "run",
                &RollbackOptions {
                    run_id_column: "_faucet_run_id".into(),
                    mode: RollbackMode::Append,
                    force: false,
                    dry_run: true,
                    later_runs: false,
                },
            )
            .await
            .is_err()
        );
        let _ = s.forget_run("run").await;
        let _ = s.rewind_commit_token("scope", None).await;
        s.set_roundtrip_recorder(Arc::new(RoundtripRecorder::new(
            RoundtripSide::Sink,
            "p",
            "r",
            "jsonl",
        )));
        let _ = s.check(&crate::check::CheckContext::default()).await;
        // The idempotent write path screens too, then forwards.
        assert_eq!(
            s.write_batch_idempotent(&[json!({"email": "a@b.io"})], "s", "t")
                .await
                .unwrap(),
            1
        );
        assert_eq!(cap.0.lock().unwrap().len(), 1);
    }

    #[cfg(feature = "arrow")]
    #[tokio::test]
    async fn columnar_pages_are_screened_whole() {
        use arrow::array::{RecordBatch, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc as A;
        let batch = RecordBatch::try_new(
            A::new(Schema::new(vec![Field::new(
                "email",
                DataType::Utf8,
                false,
            )])),
            vec![A::new(StringArray::from(vec!["a@b.io", "c@d.io"]))],
        )
        .unwrap();
        let (s, cap) = sink(policy("fail"), "eu");
        assert!(!s.supports_columnar());
        assert_eq!(s.write_batch_columnar(&batch).await.unwrap(), 2);
        assert_eq!(cap.0.lock().unwrap().len(), 2);
        // A non-compliant sink refuses the whole batch — a columnar page has
        // no per-row channel to quarantine through.
        let (s, cap) = sink(policy("quarantine"), "us");
        let err = s.write_batch_columnar(&batch).await.unwrap_err();
        assert!(
            err.to_string()
                .contains("columnar pages cannot be partially quarantined"),
            "{err}"
        );
        assert!(cap.0.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn policy_sink_forwards_every_hook_but_native() {
        let probe = crate::forwarding::HookSink::default();
        let s = PolicySink::new(
            Box::new(probe.clone()),
            policy("fail"),
            SinkFacts {
                id: "default".into(),
                kind: "jsonl".into(),
                attributes: Default::default(),
            },
            PolicyScope {
                pipeline: "p".into(),
                row: "r".into(),
            },
        );
        crate::forwarding::assert_sink_forwards_every_hook(
            &s,
            &probe,
            &["native_load_capabilities", "load_native"],
        )
        .await;
    }
}
