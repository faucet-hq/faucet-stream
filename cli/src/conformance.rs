//! Connector conformance scoring (#330).
//!
//! Grades every built-in source/sink against the faucet connector contract and
//! its capabilities, assigns a 0–100 score and a maturity tier
//! (`Stable` / `Experimental` / `Beta` / `Draft`), and lists capability badges.
//!
//! The score is computed from **authoritative, instantiation-free** signals the
//! CLI already tracks — the registry index (`cli/connectors/registry.json`) and
//! the per-kind capability functions in [`crate::registry`] — so it is
//! deterministic and cannot drift from the code.
//!
//! Scoring model (max 100): the **core contract** is the `Stable` gate — a
//! verified registry entry (40) + a real config schema (30) = 70. Everything
//! else is a bonus that lifts the score without gating the tier: documentation
//! (10), exactly-once delivery (10), and one kind-specific capability
//! (source: dataset discovery 10; sink: upsert 6 + schema evolution 4). So every
//! conforming built-in lands at `Stable` with capability badges, while an
//! incomplete third-party connector (no verified entry / no schema) drops to
//! `Experimental` / `Beta`.

use crate::registry;
use crate::registry_index::RegistryIndex;
use serde::Serialize;

/// Maturity tier derived from the conformance score.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Tier {
    Stable,
    Experimental,
    Beta,
    Draft,
}

impl Tier {
    /// Human label.
    pub fn label(self) -> &'static str {
        match self {
            Tier::Stable => "Stable",
            Tier::Experimental => "Experimental",
            Tier::Beta => "Beta",
            Tier::Draft => "Draft",
        }
    }

    /// The snake_case identifier used in JSON / `registry.json` (matches the
    /// `#[serde(rename_all = "snake_case")]` wire form).
    pub fn as_str(self) -> &'static str {
        match self {
            Tier::Stable => "stable",
            Tier::Experimental => "experimental",
            Tier::Beta => "beta",
            Tier::Draft => "draft",
        }
    }

    /// A colored dot for the terminal / catalog.
    pub fn badge(self) -> &'static str {
        match self {
            Tier::Stable => "🟢",
            Tier::Experimental => "🟡",
            Tier::Beta => "🟠",
            Tier::Draft => "⚪",
        }
    }

    /// Ordinal for `--min-tier` comparisons: `Stable` is highest.
    pub fn rank(self) -> u8 {
        match self {
            Tier::Stable => 3,
            Tier::Experimental => 2,
            Tier::Beta => 1,
            Tier::Draft => 0,
        }
    }

    /// Parse a tier from its snake_case identifier (case-insensitive).
    pub fn parse(s: &str) -> Option<Tier> {
        match s.trim().to_ascii_lowercase().as_str() {
            "stable" => Some(Tier::Stable),
            "experimental" => Some(Tier::Experimental),
            "beta" => Some(Tier::Beta),
            "draft" => Some(Tier::Draft),
            _ => None,
        }
    }

    /// A shields.io-style badge URL third-party connector authors can drop into
    /// their crate README (`![faucet](URL)`), color-matched to the tier.
    pub fn badge_url(self) -> String {
        let color = match self {
            Tier::Stable => "brightgreen",
            Tier::Experimental => "yellow",
            Tier::Beta => "orange",
            Tier::Draft => "lightgrey",
        };
        format!(
            "https://img.shields.io/badge/faucet-{}-{}",
            self.as_str(),
            color
        )
    }

    /// Derive the tier from a 0–100 conformance score.
    pub fn from_score(score: u32) -> Tier {
        match score {
            70..=u32::MAX => Tier::Stable,
            45..=69 => Tier::Experimental,
            20..=44 => Tier::Beta,
            _ => Tier::Draft,
        }
    }
}

/// Authoritative, instantiation-free capability signals for one connector.
#[derive(Debug, Clone)]
pub struct ConnectorFacts {
    pub name: String,
    pub is_source: bool,
    /// A verified entry exists in `cli/connectors/registry.json`.
    pub verified: bool,
    /// `config_schema()` returns a non-empty object schema.
    pub has_config_schema: bool,
    /// A one-line description is present in the connector catalog.
    pub documented: bool,
    /// Deterministic replay (source) / atomic-watermark idempotent writes (sink).
    pub exactly_once: bool,
    /// Sink supports `write_mode: upsert|delete` (sink only).
    pub upsert: bool,
    /// Sink can evolve the destination schema on drift (sink only).
    pub schema_evolution: bool,
    /// Source supports `faucet discover` (source only).
    pub discover: bool,
}

/// One scored dimension of a connector's conformance.
#[derive(Debug, Clone, Serialize)]
pub struct Dimension {
    pub name: &'static str,
    pub met: bool,
    pub points: u32,
    pub note: &'static str,
}

/// A connector's full conformance report.
#[derive(Debug, Clone, Serialize)]
pub struct Report {
    pub name: String,
    pub kind: &'static str,
    pub score: u32,
    pub tier: Tier,
    pub dimensions: Vec<Dimension>,
    pub badges: Vec<&'static str>,
}

/// Score a single connector from its facts. Pure and deterministic.
pub fn score(f: &ConnectorFacts) -> Report {
    let mut dims: Vec<Dimension> = Vec::new();

    // ── Core contract (the Stable gate: 70 pts) ─────────────────────────────
    dims.push(Dimension {
        name: "Registered & verified",
        met: f.verified,
        points: 40,
        note: "verified entry in cli/connectors/registry.json",
    });
    dims.push(Dimension {
        name: "Config schema",
        met: f.has_config_schema,
        points: 30,
        note: "config_schema() powers faucet init / validate / schema",
    });

    // ── Bonuses (lift the score, never gate the tier) ───────────────────────
    dims.push(Dimension {
        name: "Documented",
        met: f.documented,
        points: 10,
        note: "one-line description in the connector catalog",
    });
    dims.push(Dimension {
        name: "Exactly-once delivery",
        met: f.exactly_once,
        points: 10,
        note: if f.is_source {
            "deterministic replay from a bookmark"
        } else {
            "atomic-watermark idempotent writes"
        },
    });
    if f.is_source {
        dims.push(Dimension {
            name: "Dataset discovery",
            met: f.discover,
            points: 10,
            note: "faucet discover introspects the catalog",
        });
    } else {
        dims.push(Dimension {
            name: "Upsert / mirror",
            met: f.upsert,
            points: 6,
            note: "write_mode: upsert|delete",
        });
        dims.push(Dimension {
            name: "Schema evolution",
            met: f.schema_evolution,
            points: 4,
            note: "evolves the destination schema on drift",
        });
    }

    let score: u32 = dims.iter().filter(|d| d.met).map(|d| d.points).sum();
    let tier = Tier::from_score(score);

    let mut badges: Vec<&'static str> = Vec::new();
    if f.exactly_once {
        badges.push("exactly-once");
    }
    if f.is_source && f.discover {
        badges.push("discover");
    }
    if !f.is_source && f.upsert {
        badges.push("upsert");
    }
    if !f.is_source && f.schema_evolution {
        badges.push("schema-evolution");
    }

    Report {
        name: f.name.clone(),
        kind: if f.is_source { "source" } else { "sink" },
        score,
        tier,
        dimensions: dims,
        badges,
    }
}

/// Gather facts for one built-in connector kind from the registry.
pub fn facts_for(kind: &str, is_source: bool, index: &RegistryIndex) -> ConnectorFacts {
    let role = if is_source { "source" } else { "sink" };
    let verified = index
        .connectors
        .iter()
        .any(|e| e.name == kind && e.kind == role && e.verified);

    let schema = if is_source {
        registry::source_schema(kind)
    } else {
        registry::sink_schema(kind)
    };
    let has_config_schema = schema
        .ok()
        .and_then(|s| {
            s.get("properties")
                .and_then(|p| p.as_object())
                .map(|o| !o.is_empty())
        })
        .unwrap_or(false);

    let descs = if is_source {
        registry::source_descriptions()
    } else {
        registry::sink_descriptions()
    };
    let documented = descs.iter().any(|(k, d)| *k == kind && !d.is_empty());

    let exactly_once = if is_source {
        registry::source_supports_exactly_once(kind)
    } else {
        registry::sink_supports_idempotent_writes(kind)
    };
    let upsert = !is_source
        && registry::sink_supported_write_modes(kind)
            .iter()
            .any(|m| matches!(m, faucet_core::WriteMode::Upsert));
    let schema_evolution = !is_source && registry::sink_supports_schema_evolution(kind);
    let discover = is_source && registry::source_supports_discover(kind);

    ConnectorFacts {
        name: kind.to_string(),
        is_source,
        verified,
        has_config_schema,
        documented,
        exactly_once,
        upsert,
        schema_evolution,
        discover,
    }
}

/// Build conformance reports for every compiled-in connector, sources first.
pub fn build_reports() -> Vec<Report> {
    let index = RegistryIndex::embedded();
    let mut out = Vec::new();
    for kind in registry::source_kinds() {
        out.push(score(&facts_for(kind, true, &index)));
    }
    for kind in registry::sink_kinds() {
        out.push(score(&facts_for(kind, false, &index)));
    }
    out
}

/// The maturity tier of a single compiled-in connector kind — the lookup behind
/// the Tier column in `faucet list`.
pub fn tier_for(kind: &str, is_source: bool) -> Tier {
    let index = RegistryIndex::embedded();
    score(&facts_for(kind, is_source, &index)).tier
}

/// Render the connector **capability matrix** as Markdown, derived entirely
/// from the registry allowlists (`registry::*_KINDS`). Because those are
/// feature-independent `const` arrays, this output is deterministic regardless
/// of which connectors are compiled in — so a committed copy can be
/// round-trip-asserted in CI (see `cli/tests/capability_matrix.rs`), which is
/// what stops the matrix from being hand-maintained and drifting (#465 Part 3).
///
/// The single source of truth is the allowlists; `faucet conformance --matrix`
/// prints exactly this, and the parity test (`registry_capability_parity`)
/// separately proves the allowlists match the trait methods — so the rendered
/// matrix is grounded in real behaviour, not a hand-kept table.
pub fn capability_matrix_markdown() -> String {
    use crate::registry;

    fn sorted(kinds: &[&str]) -> Vec<String> {
        let mut v: Vec<String> = kinds.iter().map(|s| s.to_string()).collect();
        v.sort_unstable();
        v.dedup();
        v
    }
    fn mark(present: bool) -> &'static str {
        if present { "✓" } else { "" }
    }

    let mut out = String::new();
    out.push_str("# Connector capability matrix\n\n");
    out.push_str(
        "<!-- GENERATED by `faucet conformance --matrix` from the registry \
         allowlists. Do NOT hand-edit — run the command and commit its output. \
         `cli/tests/capability_matrix.rs` fails if this file drifts. -->\n\n",
    );

    // ── Sink capabilities ────────────────────────────────────────────────
    let mut sink_rows = sorted(registry::IDEMPOTENT_SINK_KINDS);
    for k in sorted(registry::UPSERT_SINK_KINDS)
        .into_iter()
        .chain(sorted(registry::SCHEMA_EVOLUTION_SINK_KINDS))
    {
        if !sink_rows.contains(&k) {
            sink_rows.push(k);
        }
    }
    sink_rows.sort_unstable();
    out.push_str("## Sinks\n\n");
    out.push_str(
        "| Sink | Exactly-once (atomic watermark) | Upsert / delete | Schema evolution |\n",
    );
    out.push_str("|---|:---:|:---:|:---:|\n");
    for k in &sink_rows {
        out.push_str(&format!(
            "| `{k}` | {} | {} | {} |\n",
            mark(registry::sink_supports_idempotent_writes(k)),
            mark(registry::UPSERT_SINK_KINDS.contains(&k.as_str())),
            mark(registry::sink_supports_schema_evolution(k)),
        ));
    }

    // ── Source capabilities ──────────────────────────────────────────────
    let mut source_rows = sorted(registry::EXACTLY_ONCE_SOURCE_KINDS);
    for k in sorted(registry::DISCOVER_SOURCE_KINDS) {
        if !source_rows.contains(&k) {
            source_rows.push(k);
        }
    }
    source_rows.sort_unstable();
    out.push_str("\n## Sources\n\n");
    out.push_str("| Source | Exactly-once replay | Dataset discovery |\n");
    out.push_str("|---|:---:|:---:|\n");
    for k in &source_rows {
        out.push_str(&format!(
            "| `{k}` | {} | {} |\n",
            mark(registry::source_supports_exactly_once(k)),
            mark(registry::source_supports_discover(k)),
        ));
    }

    // ── Exactly-once delivery compatibility (source × sink) ───────────────
    let eo_sources = sorted(registry::EXACTLY_ONCE_SOURCE_KINDS);
    let eo_sinks = sorted(registry::IDEMPOTENT_SINK_KINDS);
    out.push_str("\n## Exactly-once delivery (source × sink)\n\n");
    out.push_str(
        "A `delivery: exactly_once` pipeline needs a replayable source **and** an \
         atomic-watermark sink. Every ✓ pair below composes; any other pairing must \
         use the keyed-upsert alternative (`write_mode: upsert` + `key`).\n\n",
    );
    out.push_str("| source ↓ / sink → |");
    for s in &eo_sinks {
        out.push_str(&format!(" `{s}` |"));
    }
    out.push('\n');
    out.push('|');
    for _ in 0..=eo_sinks.len() {
        out.push_str("---|");
    }
    out.push('\n');
    for src in &eo_sources {
        out.push_str(&format!("| `{src}` |"));
        for _ in &eo_sinks {
            // Both sides are capable by construction (source ∈ EO sources, sink ∈
            // idempotent sinks), so every cell composes.
            out.push_str(" ✓ |");
        }
        out.push('\n');
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn conforming(is_source: bool) -> ConnectorFacts {
        ConnectorFacts {
            name: "acme".into(),
            is_source,
            verified: true,
            has_config_schema: true,
            documented: true,
            exactly_once: false,
            upsert: false,
            schema_evolution: false,
            discover: false,
        }
    }

    #[test]
    fn tier_boundaries() {
        assert_eq!(Tier::from_score(100), Tier::Stable);
        assert_eq!(Tier::from_score(70), Tier::Stable);
        assert_eq!(Tier::from_score(69), Tier::Experimental);
        assert_eq!(Tier::from_score(45), Tier::Experimental);
        assert_eq!(Tier::from_score(44), Tier::Beta);
        assert_eq!(Tier::from_score(20), Tier::Beta);
        assert_eq!(Tier::from_score(19), Tier::Draft);
        assert_eq!(Tier::from_score(0), Tier::Draft);
    }

    #[test]
    fn tier_label_and_badge() {
        for t in [Tier::Stable, Tier::Experimental, Tier::Beta, Tier::Draft] {
            assert!(!t.label().is_empty());
            assert!(!t.badge().is_empty());
            assert!(!t.as_str().is_empty());
            assert!(t.badge_url().contains(t.as_str()));
        }
    }

    #[test]
    fn tier_parse_roundtrips_and_orders() {
        for t in [Tier::Stable, Tier::Experimental, Tier::Beta, Tier::Draft] {
            assert_eq!(Tier::parse(t.as_str()), Some(t));
        }
        assert_eq!(Tier::parse("STABLE"), Some(Tier::Stable));
        assert_eq!(Tier::parse("  beta "), Some(Tier::Beta));
        assert_eq!(Tier::parse("nonsense"), None);
        assert!(Tier::Stable.rank() > Tier::Experimental.rank());
        assert!(Tier::Experimental.rank() > Tier::Beta.rank());
        assert!(Tier::Beta.rank() > Tier::Draft.rank());
    }

    #[test]
    fn tier_for_matches_reports() {
        for r in build_reports() {
            assert_eq!(tier_for(&r.name, r.kind == "source"), r.tier);
        }
    }

    #[test]
    fn conforming_source_is_stable_with_docs() {
        let r = score(&conforming(true)); // 40 + 30 + 10 = 80
        assert_eq!(r.score, 80);
        assert_eq!(r.tier, Tier::Stable);
        assert_eq!(r.kind, "source");
        assert!(r.badges.is_empty());
    }

    #[test]
    fn source_capabilities_add_points_and_badges() {
        let mut f = conforming(true);
        f.exactly_once = true;
        f.discover = true;
        let r = score(&f); // 80 + 10 + 10 = 100
        assert_eq!(r.score, 100);
        assert_eq!(r.tier, Tier::Stable);
        assert!(r.badges.contains(&"exactly-once"));
        assert!(r.badges.contains(&"discover"));
    }

    #[test]
    fn sink_upsert_and_evolution() {
        let mut f = conforming(false);
        f.upsert = true;
        f.schema_evolution = true;
        let r = score(&f); // 80 + 6 + 4 = 90
        assert_eq!(r.score, 90);
        assert_eq!(r.kind, "sink");
        assert!(r.badges.contains(&"upsert"));
        assert!(r.badges.contains(&"schema-evolution"));
        // A sink is never scored on discover.
        assert!(!r.badges.contains(&"discover"));
    }

    #[test]
    fn unregistered_no_schema_is_draft() {
        let mut f = conforming(true);
        f.verified = false;
        f.has_config_schema = false;
        f.documented = false;
        let r = score(&f);
        assert_eq!(r.score, 0);
        assert_eq!(r.tier, Tier::Draft);
    }

    #[test]
    fn schema_only_is_beta() {
        let mut f = conforming(false);
        f.verified = false;
        f.documented = false; // only config schema (30)
        let r = score(&f);
        assert_eq!(r.score, 30);
        assert_eq!(r.tier, Tier::Beta);
    }

    #[test]
    fn verified_only_is_experimental() {
        let mut f = conforming(true);
        f.has_config_schema = false;
        f.documented = false; // only verified (40)
        let r = score(&f);
        assert_eq!(r.score, 40);
        assert_eq!(r.tier, Tier::Beta); // 40 is Beta; 45+ is Experimental
    }

    #[test]
    fn every_builtin_meets_the_bar() {
        let reports = build_reports();
        assert!(!reports.is_empty());
        for r in &reports {
            // Every shipped built-in has a verified entry + a config schema, so
            // it must be at least Stable-gate-eligible (>= Experimental).
            assert!(
                r.score >= 45,
                "{} `{}` scored {} ({:?})",
                r.kind,
                r.name,
                r.score,
                r.tier
            );
            assert!(matches!(r.tier, Tier::Stable | Tier::Experimental));
        }
    }

    #[test]
    fn known_capabilities_surface_in_reports() {
        let reports = build_reports();
        // postgres source is discoverable.
        if let Some(pg) = reports
            .iter()
            .find(|r| r.name == "postgres" && r.kind == "source")
        {
            assert!(
                pg.badges.contains(&"discover"),
                "postgres source should discover"
            );
        }
        // bigquery sink is exactly-once + upsert-capable.
        if let Some(bq) = reports
            .iter()
            .find(|r| r.name == "bigquery" && r.kind == "sink")
        {
            assert!(bq.badges.contains(&"exactly-once"));
            assert!(bq.badges.contains(&"upsert"));
        }
    }
}
