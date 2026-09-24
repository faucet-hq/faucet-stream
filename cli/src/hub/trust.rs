//! Trust signals for choosing between hub templates (#685).
//!
//! When many publishers ship a template for one system, the catalog's CI
//! records facts about each entry in `index.json` under `trust`: GitHub-backed
//! stars (a 👍 on the template's discussion, filtered against young accounts
//! and the namespace's own owners), when it last changed, how long `stable`
//! has held, open issues, how many sinks it composes with, and the publisher's
//! track record. This module reads them and orders variants by them.

use std::cmp::Ordering;

use serde::{Deserialize, Serialize};

/// The `trust` block of one `index.json` entry. Every field is optional: an
/// older catalog, or one whose trust job has not run yet, carries none.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrustSignals {
    /// Qualifying 👍 reactions on the template's discussion.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stars: Option<u64>,
    /// Where to star it: the template's discussion.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub star_url: Option<String>,
    /// Date (`YYYY-MM-DD`) of the newest version.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated: Option<String>,
    /// Date the current `stable` version was published.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stable_since: Option<String>,
    /// Open catalog issues labelled `template:<id>`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub open_issues: Option<u64>,
    /// Sink templates it composes with in full (source templates only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compatible_sinks: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publisher: Option<Publisher>,
}

/// The publishing namespace's track record.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Publisher {
    /// Templates the namespace publishes in this catalog.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub templates: Option<u64>,
    /// Age of the owning GitHub account, in days.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_age_days: Option<u64>,
}

impl TrustSignals {
    /// One line for terminal output: `★ 12 · updated 2026-09-01 · 2 open issues`.
    /// Empty when the catalog records nothing.
    pub fn summary(&self) -> String {
        let mut parts = Vec::new();
        if let Some(n) = self.stars {
            parts.push(format!("★ {n}"));
        }
        if let Some(d) = &self.updated {
            parts.push(format!("updated {d}"));
        }
        match self.open_issues {
            Some(1) => parts.push("1 open issue".into()),
            Some(n) if n > 1 => parts.push(format!("{n} open issues")),
            _ => {}
        }
        parts.join(" · ")
    }
}

/// A template as far as ranking is concerned.
#[derive(Debug, Clone, Copy)]
pub struct Candidate<'a> {
    pub id: &'a str,
    pub official: bool,
    pub trust: Option<&'a TrustSignals>,
}

/// How variants of one system are ordered: official first, then most stars,
/// then most recently updated, then id — a total order, so output is stable.
/// Ranking only orders what the user sees; nothing is ever picked for them.
pub fn rank(a: &Candidate<'_>, b: &Candidate<'_>) -> Ordering {
    let stars = |c: &Candidate<'_>| c.trust.and_then(|t| t.stars).unwrap_or(0);
    let updated = |c: &Candidate<'_>| c.trust.and_then(|t| t.updated.clone());
    b.official
        .cmp(&a.official)
        .then_with(|| stars(b).cmp(&stars(a)))
        .then_with(|| updated(b).cmp(&updated(a)))
        .then_with(|| a.id.cmp(b.id))
}

/// How `faucet hub list` orders templates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SortBy {
    /// Alphabetical by id.
    #[default]
    Name,
    /// Most stars first (then [`rank`]).
    Stars,
    /// Most recently updated first (then id).
    Updated,
}

/// Indices of `cands` in display order.
pub fn order(cands: &[Candidate<'_>], by: SortBy) -> Vec<usize> {
    let mut idx: Vec<usize> = (0..cands.len()).collect();
    let stars = |c: &Candidate<'_>| c.trust.and_then(|t| t.stars).unwrap_or(0);
    let updated = |c: &Candidate<'_>| c.trust.and_then(|t| t.updated.clone());
    idx.sort_by(|&i, &j| {
        let (a, b) = (&cands[i], &cands[j]);
        match by {
            SortBy::Name => a.id.cmp(b.id),
            SortBy::Stars => stars(b).cmp(&stars(a)).then_with(|| rank(a, b)),
            SortBy::Updated => updated(b).cmp(&updated(a)).then_with(|| a.id.cmp(b.id)),
        }
    });
    idx
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(stars: Option<u64>, updated: Option<&str>) -> TrustSignals {
        TrustSignals {
            stars,
            updated: updated.map(str::to_string),
            ..Default::default()
        }
    }

    #[test]
    fn summary_names_what_the_catalog_recorded() {
        assert_eq!(TrustSignals::default().summary(), "");
        let mut s = t(Some(12), Some("2026-09-01"));
        assert_eq!(s.summary(), "★ 12 · updated 2026-09-01");
        s.open_issues = Some(1);
        assert!(s.summary().ends_with("· 1 open issue"));
        s.open_issues = Some(3);
        assert!(s.summary().ends_with("· 3 open issues"));
        s.open_issues = Some(0);
        assert_eq!(s.summary(), "★ 12 · updated 2026-09-01");
    }

    #[test]
    fn rank_orders_official_then_stars_then_freshness_then_id() {
        let (a, b, c, d) = (
            t(Some(3), Some("2026-01-01")),
            t(Some(40), Some("2025-01-01")),
            t(Some(40), Some("2026-06-01")),
            t(None, None),
        );
        let mut v = [
            Candidate {
                id: "z/none",
                official: false,
                trust: Some(&d),
            },
            Candidate {
                id: "a/few",
                official: false,
                trust: Some(&a),
            },
            Candidate {
                id: "b/many-old",
                official: false,
                trust: Some(&b),
            },
            Candidate {
                id: "c/many-new",
                official: false,
                trust: Some(&c),
            },
            Candidate {
                id: "faucet-hq/x",
                official: true,
                trust: None,
            },
            Candidate {
                id: "y/untracked",
                official: false,
                trust: None,
            },
        ];
        v.sort_by(rank);
        let ids: Vec<&str> = v.iter().map(|c| c.id).collect();
        assert_eq!(
            ids,
            [
                "faucet-hq/x",
                "c/many-new",
                "b/many-old",
                "a/few",
                "y/untracked",
                "z/none"
            ]
        );
    }

    #[test]
    fn order_sorts_by_name_stars_or_freshness() {
        let (few, many, fresh) = (
            t(Some(1), Some("2026-01-01")),
            t(Some(9), Some("2025-01-01")),
            t(None, Some("2026-09-01")),
        );
        let c = [
            Candidate {
                id: "b",
                official: false,
                trust: Some(&few),
            },
            Candidate {
                id: "a",
                official: false,
                trust: Some(&many),
            },
            Candidate {
                id: "c",
                official: false,
                trust: Some(&fresh),
            },
            Candidate {
                id: "d",
                official: false,
                trust: None,
            },
        ];
        let ids = |by| {
            order(&c, by)
                .into_iter()
                .map(|i| c[i].id)
                .collect::<Vec<_>>()
        };
        assert_eq!(ids(SortBy::Name), ["a", "b", "c", "d"]);
        assert_eq!(ids(SortBy::Stars), ["a", "b", "c", "d"]);
        assert_eq!(ids(SortBy::Updated), ["c", "b", "a", "d"]);
        assert_eq!(SortBy::default(), SortBy::Name);
    }

    #[test]
    fn trust_round_trips_and_tolerates_missing_fields() {
        let full: TrustSignals = serde_json::from_value(serde_json::json!({
            "stars": 5, "star_url": "https://g/d/1", "updated": "2026-09-01",
            "stable_since": "2026-08-01", "open_issues": 0, "compatible_sinks": 4,
            "publisher": {"templates": 2, "account_age_days": 900},
            "future_field": true
        }))
        .unwrap();
        assert_eq!(full.publisher.as_ref().unwrap().templates, Some(2));
        let back = serde_json::to_value(&full).unwrap();
        assert_eq!(back["stars"], 5);
        assert!(back.get("future_field").is_none());
        let empty: TrustSignals = serde_json::from_str("{}").unwrap();
        assert_eq!(empty, TrustSignals::default());
        assert_eq!(serde_json::to_string(&empty).unwrap(), "{}");
    }
}
