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
    /// or status cannot be decided from the row id, so it counts as included.
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
        named(&sel.select) || named(&sel.only) || !sel.tags.is_empty() || !sel.status.is_empty()
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
