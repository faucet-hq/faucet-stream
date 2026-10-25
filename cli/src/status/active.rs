//! A run still in flight, as `faucet status` sees it.

/// A run still in flight, with the row selection it was submitted with.
#[derive(Debug, Clone, PartialEq)]
pub struct ActiveRun {
    pub run_id: String,
    /// `None` = every row.
    pub selection: Option<crate::select::SelectionRequest>,
}

impl ActiveRun {
    /// Whether the run may be running `row`. A selection that filters by tag
    /// or status cannot be decided from the row id, so it counts as included,
    /// and so does one that pulls in ancestors (`include_parents: eligible|all`):
    /// any row not skipped may be one of them.
    pub fn may_include(&self, row: &str) -> bool {
        let Some(sel) = &self.selection else {
            return true;
        };
        let named = |pats: &[String]| {
            pats.iter()
                .any(|p| p == row || crate::select::glob_match(p, row))
        };
        if named(&sel.skip) {
            return false;
        }
        if sel.select.is_empty() && sel.only.is_empty() {
            return true;
        }
        named(&sel.select)
            || named(&sel.only)
            || !sel.tags.is_empty()
            || !sel.status.is_empty()
            || matches!(
                sel.include_parents,
                Some(crate::config::IncludeParents::Eligible | crate::config::IncludeParents::All)
            )
    }
}

impl From<&str> for ActiveRun {
    fn from(run_id: &str) -> Self {
        Self {
            run_id: run_id.to_string(),
            selection: None,
        }
    }
}

impl std::fmt::Display for ActiveRun {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.run_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::IncludeParents;

    fn run(include_parents: Option<IncludeParents>) -> ActiveRun {
        ActiveRun {
            run_id: "r".into(),
            selection: Some(crate::select::SelectionRequest {
                select: vec!["kid".into()],
                skip: vec!["other".into()],
                include_parents,
                ..Default::default()
            }),
        }
    }

    #[test]
    fn a_selection_pulling_in_parents_may_run_any_row_it_does_not_skip() {
        for policy in [IncludeParents::Eligible, IncludeParents::All] {
            let r = run(Some(policy));
            assert!(r.may_include("kid"));
            assert!(r.may_include("parent"), "{policy:?}");
            assert!(!r.may_include("other"), "{policy:?}");
        }
        for r in [run(None), run(Some(IncludeParents::Off))] {
            assert!(r.may_include("kid"));
            assert!(!r.may_include("parent"));
        }
    }
}
