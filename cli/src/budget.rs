//! The CLI half of run budgets (#703): the `--max-records` / `--max-bytes` /
//! `--max-duration-secs` / `--allowed-sink` flags, and the merge that turns a
//! config's `budget:` block plus a caller's ceilings into the one
//! [`BudgetSpec`] the executor enforces. Enforcement itself lives in
//! [`faucet_core::budget`].

use faucet_core::BudgetSpec;

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

/// The budget a run enforces: the config's block merged with the caller's
/// (the stricter of each ceiling; the intersection of the allowed sinks).
/// Both sides are validated; `None` when neither sets anything.
pub fn effective_budget(
    config: Option<&BudgetSpec>,
    caller: Option<BudgetSpec>,
) -> Result<Option<BudgetSpec>, String> {
    if let Some(c) = config {
        c.validate()?;
    }
    if let Some(c) = &caller {
        c.validate().map_err(|e| e.replace("budget.", "--"))?;
    }
    let merged = match (config, caller) {
        (Some(a), Some(b)) => a.merge(&b),
        (Some(a), None) => a.clone(),
        (None, Some(b)) => b,
        (None, None) => return Ok(None),
    };
    Ok((!merged.is_empty()).then_some(merged))
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
