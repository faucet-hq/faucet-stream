//! The CLI half of run budgets (#703): the `--max-records` / `--max-bytes` /
//! `--max-duration-secs` / `--allowed-sink` flags, and the merge that turns a
//! config's `budget:` block plus a caller's ceilings into the one
//! [`BudgetSpec`] the executor enforces. Enforcement itself lives in
//! [`faucet_core::budget`].

use faucet_core::BudgetSpec;
use std::collections::HashMap;

/// Budget ceilings given on the command line (`faucet run`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BudgetFlags {
    pub max_records: Option<u64>,
    pub max_bytes: Option<u64>,
    pub max_duration_secs: Option<u64>,
    pub allowed_sinks: Vec<String>,
}

impl BudgetFlags {
    /// The flags as a budget; `None` when no flag was given.
    pub fn into_spec(self) -> Option<BudgetSpec> {
        let spec = BudgetSpec {
            max_records: self.max_records,
            max_bytes: self.max_bytes,
            max_duration_secs: self.max_duration_secs,
            allowed_sinks: self.allowed_sinks,
        };
        (!spec.is_empty()).then_some(spec)
    }
}

/// Sink template name → connector kind for a config, so a budget naming a
/// template and one naming its kind intersect (#789 CLI-44).
pub fn sink_template_kinds(cfg: &crate::config::PipelineConfig) -> HashMap<String, String> {
    let mut kinds: HashMap<String, String> = cfg
        .pipeline
        .sinks
        .iter()
        .map(|(name, spec)| (name.clone(), spec.kind.clone()))
        .collect();
    if let Some(sink) = &cfg.pipeline.sink {
        kinds
            .entry("default".to_string())
            .or_insert_with(|| sink.kind.clone());
    }
    kinds
}

/// [`sink_template_kinds`] read off an untyped config document.
pub fn sink_template_kinds_in_doc(doc: &serde_json::Value) -> HashMap<String, String> {
    let mut kinds = HashMap::new();
    let pipeline = doc.get("pipeline");
    if let Some(sinks) = pipeline
        .and_then(|p| p.get("sinks"))
        .and_then(|s| s.as_object())
    {
        for (name, spec) in sinks {
            if let Some(kind) = spec.get("type").and_then(|t| t.as_str()) {
                kinds.insert(name.clone(), kind.to_string());
            }
        }
    }
    if let Some(kind) = pipeline
        .and_then(|p| p.get("sink"))
        .and_then(|s| s.get("type"))
        .and_then(|t| t.as_str())
    {
        kinds
            .entry("default".to_string())
            .or_insert_with(|| kind.to_string());
    }
    kinds
}

/// The budget a run enforces: the config's block merged with the caller's
/// (the stricter of each ceiling; the intersection of the allowed sinks).
/// Both sides are validated; `None` when neither sets anything.
pub fn effective_budget(
    config: Option<&BudgetSpec>,
    caller: Option<BudgetSpec>,
) -> Result<Option<BudgetSpec>, String> {
    effective_budget_with_kinds(config, caller, &HashMap::new())
}

/// [`effective_budget`] for a config whose sink templates are known, so a
/// template name on one side meets its connector kind on the other.
pub fn effective_budget_with_kinds(
    config: Option<&BudgetSpec>,
    caller: Option<BudgetSpec>,
    template_kinds: &HashMap<String, String>,
) -> Result<Option<BudgetSpec>, String> {
    if let Some(c) = config {
        c.validate()?;
    }
    if let Some(c) = &caller {
        c.validate().map_err(|e| e.replace("budget.", "--"))?;
    }
    let merged = match (config, caller) {
        (Some(a), Some(b)) => a.merge_with(&b, &|name: &str| template_kinds.get(name).cloned()),
        (Some(a), None) => a.clone(),
        (None, Some(b)) => b,
        (None, None) => return Ok(None),
    };
    Ok((!merged.is_empty()).then_some(merged))
}

/// What is left of `spec` once a run has used what `state` recorded over
/// `elapsed` — the budget a follow-up the run starts (a post-run
/// `verify --repair`, #871) is bounded by. `allowed_sinks` carries over. A
/// spent ceiling is left at `0`, so the follow-up's first write is refused
/// (and a spent duration stops it at once) — `0` is not a valid configured
/// ceiling ([`BudgetSpec::validate`]), but the budget sink enforces it.
pub fn remaining_budget(
    spec: &BudgetSpec,
    state: Option<&faucet_core::BudgetState>,
    elapsed: std::time::Duration,
) -> BudgetSpec {
    let left = |max: Option<u64>, used: u64| max.map(|m| m.saturating_sub(used));
    let (records, bytes) = state.map_or((0, 0), |s| (s.records(), s.bytes()));
    BudgetSpec {
        max_records: left(spec.max_records, records),
        max_bytes: left(spec.max_bytes, bytes),
        max_duration_secs: left(spec.max_duration_secs, elapsed.as_secs()),
        allowed_sinks: spec.allowed_sinks.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_caller_list_disjoint_from_the_config_denies_every_sink() {
        let config = BudgetSpec {
            allowed_sinks: vec!["sandbox".into()],
            ..Default::default()
        };
        let caller = BudgetSpec {
            allowed_sinks: vec!["x".into()],
            ..Default::default()
        };
        let m = effective_budget(Some(&config), Some(caller.clone()))
            .unwrap()
            .unwrap();
        assert!(!m.sink_allowed("sandbox", "postgres"));
        assert!(!m.sink_allowed("x", "x"));

        let kinds: HashMap<String, String> =
            [("sandbox".to_string(), "postgres".to_string())].into();
        let by_kind = BudgetSpec {
            allowed_sinks: vec!["postgres".into()],
            ..Default::default()
        };
        let m = effective_budget_with_kinds(Some(&config), Some(by_kind), &kinds)
            .unwrap()
            .unwrap();
        assert!(m.sink_allowed("sandbox", "postgres"));
        assert!(!m.sink_allowed("other", "postgres"));
    }

    #[test]
    fn template_kinds_are_read_from_a_config_and_a_document() {
        let doc = serde_json::json!({"pipeline": {
            "sink": {"type": "jsonl"},
            "sinks": {"sandbox": {"type": "postgres"}}
        }});
        let kinds = sink_template_kinds_in_doc(&doc);
        assert_eq!(kinds["sandbox"], "postgres");
        assert_eq!(kinds["default"], "jsonl");
        let cfg = crate::config::PipelineConfig::from_text(
            "version: 1\npipeline:\n  source: { type: csv, config: { path: a.csv } }\n  sinks:\n    sandbox: { type: jsonl, config: { path: b.jsonl } }\n",
            std::path::Path::new("c.yaml"),
        )
        .unwrap();
        assert_eq!(sink_template_kinds(&cfg)["sandbox"], "jsonl");
    }

    #[test]
    fn flags_become_a_spec_only_when_set() {
        assert!(BudgetFlags::default().into_spec().is_none());
        let spec = BudgetFlags {
            max_records: Some(10),
            allowed_sinks: vec!["warehouse".into()],
            ..Default::default()
        }
        .into_spec()
        .unwrap();
        assert_eq!(spec.max_records, Some(10));
        assert_eq!(spec.allowed_sinks, vec!["warehouse"]);
    }

    #[test]
    fn effective_budget_takes_the_stricter_side_and_validates_both() {
        let config = BudgetSpec {
            max_records: Some(1000),
            max_bytes: Some(1 << 30),
            ..Default::default()
        };
        let caller = BudgetSpec {
            max_records: Some(50),
            max_duration_secs: Some(30),
            ..Default::default()
        };
        let m = effective_budget(Some(&config), Some(caller))
            .unwrap()
            .unwrap();
        assert_eq!(m.max_records, Some(50));
        assert_eq!(m.max_bytes, Some(1 << 30));
        assert_eq!(m.max_duration_secs, Some(30));
        assert_eq!(
            effective_budget(Some(&config), None).unwrap().unwrap(),
            config
        );
        assert!(effective_budget(None, None).unwrap().is_none());
        assert!(
            effective_budget(None, Some(BudgetSpec::default()))
                .unwrap()
                .is_none()
        );
        let err = effective_budget(
            None,
            Some(BudgetSpec {
                max_records: Some(0),
                ..Default::default()
            }),
        )
        .unwrap_err();
        assert!(err.starts_with("--max_records"), "{err}");
        let bad = BudgetSpec {
            max_bytes: Some(0),
            ..Default::default()
        };
        assert!(effective_budget(Some(&bad), None).is_err());
    }

    #[test]
    fn remaining_budget_subtracts_what_the_run_used() {
        let spec = BudgetSpec {
            max_records: Some(10),
            max_bytes: Some(1_000),
            max_duration_secs: Some(60),
            allowed_sinks: vec!["w".into()],
        };
        let none = remaining_budget(&spec, None, std::time::Duration::from_secs(5));
        assert_eq!(none.max_records, Some(10));
        assert_eq!(none.max_duration_secs, Some(55));
        let state = faucet_core::Governance::new()
            .with_budget(spec.clone())
            .budget_state()
            .cloned()
            .unwrap();
        let left = remaining_budget(&spec, Some(&state), std::time::Duration::from_secs(90));
        assert_eq!(left.max_records, Some(10));
        assert_eq!(left.max_bytes, Some(1_000));
        assert_eq!(left.max_duration_secs, Some(0));
        assert_eq!(left.allowed_sinks, vec!["w".to_string()]);
        let open = remaining_budget(&BudgetSpec::default(), None, std::time::Duration::ZERO);
        assert!(open.max_records.is_none() && open.max_duration_secs.is_none());
    }

    struct Accepting;

    #[faucet_core::async_trait]
    impl faucet_core::Sink for Accepting {
        async fn write_batch(
            &self,
            records: &[serde_json::Value],
        ) -> Result<usize, faucet_core::FaucetError> {
            Ok(records.len())
        }
    }

    /// A run that spent its whole record ceiling leaves its follow-up nothing:
    /// the follow-up's first record is refused, not let through.
    #[tokio::test]
    async fn a_spent_ceiling_refuses_the_follow_ups_first_write() {
        use faucet_core::Sink as _;
        let spec = BudgetSpec {
            max_records: Some(100),
            ..Default::default()
        };
        let cancel = tokio_util::sync::CancellationToken::new();
        let (run, state, _timer) =
            faucet_core::BudgetSink::wrap(Box::new(Accepting), spec.clone(), cancel.clone());
        let page: Vec<serde_json::Value> = (0..100).map(|i| serde_json::json!({"i": i})).collect();
        assert_eq!(run.write_batch(&page).await.unwrap(), 100);
        let left = remaining_budget(&spec, Some(&state), std::time::Duration::ZERO);
        assert_eq!(left.max_records, Some(0));
        let (repair, _, _t) = faucet_core::BudgetSink::wrap(Box::new(Accepting), left, cancel);
        let err = repair
            .write_batch(&[serde_json::json!({"i": 100})])
            .await
            .unwrap_err();
        assert!(
            matches!(
                err,
                faucet_core::FaucetError::BudgetExceeded {
                    limit: 0,
                    actual: 1,
                    ..
                }
            ),
            "{err:?}"
        );
    }
}
