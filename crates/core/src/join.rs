//! Pure hash-join logic for the topology `join` node (issue #72).
//!
//! A [`HashJoin`] buffers one upstream (the **build** side) into an in-memory
//! index keyed by a configurable dotted path, then enriches records streamed
//! from the other upstream (the **probe** side) with projected fields looked
//! up from the matching build record. This is the classic *hash-join* shape:
//! the build side is fully materialized before the probe side starts emitting.
//!
//! This module is **pure data logic** — no I/O, no channels, no `async`. The
//! topology executor ([`crate::topology`]) owns the streaming/channel plumbing
//! and calls [`HashJoin::add_build_page`] / [`HashJoin::probe_page`] as pages
//! arrive. Keeping it pure makes every join semantic unit-testable in
//! isolation (see the extensive tests at the bottom of this file).
//!
//! ## Semantics
//!
//! - **`inner`** drops probe records with no build-side match.
//! - **`left`** passes probe records through for non-matches, filling the
//!   projected fields with [`JoinConfig::on_missing`].
//! - **`on_duplicate`** decides what happens when one probe key matches more
//!   than one build record: [`OnDuplicate::First`] keeps the first, and
//!   [`OnDuplicate::Cartesian`] emits one enriched record per match.
//! - **`on_collision`** decides what happens when a projected `as` name
//!   already exists on the probe record.
//! - **`key_normalize`** controls whether `"42"` (string) and `42` (number)
//!   are treated as the same key ([`KeyNormalize::Stringify`]) or as distinct
//!   keys ([`KeyNormalize::Preserve`], the default — no coercion).

use crate::error::FaucetError;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

/// Default safety cap on build-side records (10M).
pub const DEFAULT_MAX_BUILD_RECORDS: usize = 10_000_000;

/// Join mode — how probe records with no build match are handled.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "lowercase")]
pub enum JoinMode {
    /// Drop probe records that have no matching build record.
    #[default]
    Inner,
    /// Pass probe records through even without a match, filling projected
    /// fields from [`JoinConfig::on_missing`].
    Left,
}

impl std::fmt::Display for JoinMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            JoinMode::Inner => "inner",
            JoinMode::Left => "left",
        })
    }
}

/// What to do when one probe key matches more than one build record.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "lowercase")]
pub enum OnDuplicate {
    /// Keep only the first build-side match (deterministic given build order).
    #[default]
    First,
    /// Emit one enriched record per build-side match (may duplicate the probe
    /// record).
    Cartesian,
}

/// What to do when a projected `as` name collides with an existing field on
/// the probe record.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "lowercase")]
pub enum OnCollision {
    /// Overwrite the probe record's field with the projected value.
    #[default]
    Overwrite,
    /// Leave the probe record's field untouched; skip the projection.
    Skip,
    /// Fail the join node — and with it the run — with a typed error.
    Error,
}

/// How to normalize keys before comparison.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "lowercase")]
pub enum KeyNormalize {
    /// Compare keys as their JSON value (no coercion): `"42"` != `42`.
    #[default]
    Preserve,
    /// Coerce scalar keys to their string form before comparison so `"42"`
    /// and `42` match.
    Stringify,
}

/// A single field projection: copy `from` (a dotted path into the build
/// record) onto the probe record under the name `as_`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct Projection {
    /// Dotted path inside each build (right) record.
    pub from: String,
    /// Output field name written onto the probe (left) record.
    #[serde(rename = "as")]
    pub as_: String,
}

/// Compiled join configuration. Dotted key paths are pre-split at construction.
#[derive(Debug, Clone)]
pub struct JoinConfig {
    /// Inner vs left.
    pub mode: JoinMode,
    /// Dotted path to the build-side (right) key.
    pub build_key: String,
    /// Dotted path to the probe-side (left) key.
    pub probe_key: String,
    /// Fields to copy from the build record onto the probe record.
    pub projections: Vec<Projection>,
    /// Value used to fill projected fields on a `left`-mode non-match.
    pub on_missing: Value,
    /// Multi-match policy.
    pub on_duplicate: OnDuplicate,
    /// Projection-collision policy.
    pub on_collision: OnCollision,
    /// Key-normalization policy.
    pub key_normalize: KeyNormalize,
    /// Safety cap on build-side records.
    pub max_build_records: usize,
}

impl Default for JoinConfig {
    fn default() -> Self {
        Self {
            mode: JoinMode::default(),
            build_key: String::new(),
            probe_key: String::new(),
            projections: Vec::new(),
            on_missing: Value::Null,
            on_duplicate: OnDuplicate::default(),
            on_collision: OnCollision::default(),
            key_normalize: KeyNormalize::default(),
            max_build_records: DEFAULT_MAX_BUILD_RECORDS,
        }
    }
}

/// Running counters for a join, mirroring the `faucet_join_*` metrics.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct JoinStats {
    /// Records ingested by the build side (before null-key skipping).
    pub build_records: u64,
    /// Build records skipped because their key resolved to null/absent.
    pub build_nulls: u64,
    /// Distinct build keys that appeared more than once (duplicate keys).
    pub duplicates: u64,
    /// Records ingested by the probe side.
    pub probe_records: u64,
    /// Probe records that matched at least one build record.
    pub matches: u64,
    /// Probe records with no build-side match.
    pub misses: u64,
    /// Projections skipped because the `from` path was absent on the build
    /// record.
    pub project_misses: u64,
}

/// A hash join: build the index, then probe it.
#[derive(Debug)]
pub struct HashJoin {
    config: JoinConfig,
    /// Build index: canonical key → build records (in build order).
    index: HashMap<String, Vec<Value>>,
    stats: JoinStats,
}

impl HashJoin {
    /// Create an empty join ready to receive build-side pages.
    pub fn new(config: JoinConfig) -> Self {
        Self {
            config,
            index: HashMap::new(),
            stats: JoinStats::default(),
        }
    }

    /// Access the running counters.
    pub fn stats(&self) -> &JoinStats {
        &self.stats
    }

    /// The compiled config.
    pub fn config(&self) -> &JoinConfig {
        &self.config
    }

    /// Ingest a page of build-side records into the index.
    ///
    /// Records whose build key resolves to null/absent are skipped (counted in
    /// [`JoinStats::build_nulls`]). Returns [`FaucetError::Transform`] with a
    /// `JoinBuildOverflow`-style message once the cumulative build count
    /// exceeds [`JoinConfig::max_build_records`].
    pub fn add_build_page(&mut self, records: Vec<Value>) -> Result<(), FaucetError> {
        for rec in records {
            self.stats.build_records += 1;
            if self.stats.build_records as usize > self.config.max_build_records {
                return Err(FaucetError::Transform(format!(
                    "join build side exceeded max_build_records ({}); raise the limit or partition the join",
                    self.config.max_build_records
                )));
            }
            let key = match get_path(&rec, &self.config.build_key) {
                Some(v) if !v.is_null() => v,
                _ => {
                    self.stats.build_nulls += 1;
                    continue;
                }
            };
            let ckey = match canonical_key(key, self.config.key_normalize) {
                Some(k) => k,
                None => {
                    // Non-scalar key under stringify, or otherwise unrepresentable.
                    self.stats.build_nulls += 1;
                    continue;
                }
            };
            let bucket = self.index.entry(ckey).or_default();
            if bucket.len() == 1 {
                self.stats.duplicates += 1;
            }
            bucket.push(rec);
        }
        Ok(())
    }

    /// Number of distinct keys indexed so far.
    pub fn indexed_keys(&self) -> usize {
        self.index.len()
    }

    /// Enrich a page of probe-side records against the built index.
    ///
    /// Must be called only after every build page has been ingested. Returns
    /// the enriched output records (0..N per input record depending on mode
    /// and duplicate policy).
    pub fn probe_page(&mut self, records: Vec<Value>) -> Result<Vec<Value>, FaucetError> {
        let mut out = Vec::with_capacity(records.len());
        for left in records {
            self.stats.probe_records += 1;
            let key = get_path(&left, &self.config.probe_key).filter(|v| !v.is_null());
            let ckey = key.and_then(|k| canonical_key(k, self.config.key_normalize));

            // Split the borrows so the matched build records are read in
            // place while the stats are updated.
            let Self {
                config,
                index,
                stats,
            } = self;
            let matches: Option<&Vec<Value>> = ckey.as_ref().and_then(|k| index.get(k));

            match matches {
                Some(bucket) if !bucket.is_empty() => {
                    stats.matches += 1;
                    let take = match config.on_duplicate {
                        OnDuplicate::First => &bucket[..1],
                        OnDuplicate::Cartesian => &bucket[..],
                    };
                    let (last, rest) = take.split_last().expect("bucket is not empty");
                    for right in rest {
                        let mut enriched = left.clone();
                        apply_projection(config, stats, &mut enriched, Some(right))?;
                        out.push(enriched);
                    }
                    let mut enriched = left;
                    apply_projection(config, stats, &mut enriched, Some(last))?;
                    out.push(enriched);
                }
                _ => {
                    stats.misses += 1;
                    if config.mode == JoinMode::Left {
                        let mut enriched = left;
                        apply_projection(config, stats, &mut enriched, None)?;
                        out.push(enriched);
                    }
                    // inner mode: drop.
                }
            }
        }
        Ok(out)
    }
}

/// Apply the configured projections onto `left`. `right = None` fills every
/// projected field with `on_missing` (the `left`-mode non-match path).
fn apply_projection(
    config: &JoinConfig,
    stats: &mut JoinStats,
    left: &mut Value,
    right: Option<&Value>,
) -> Result<(), FaucetError> {
    for proj in &config.projections {
        let value = match right {
            Some(r) => match get_path(r, &proj.from) {
                Some(v) => v.clone(),
                None => {
                    // Field absent on the matched build record — skip it.
                    stats.project_misses += 1;
                    continue;
                }
            },
            None => config.on_missing.clone(),
        };
        set_field(left, &proj.as_, value, config.on_collision)?;
    }
    Ok(())
}

/// Resolve a dotted path against a JSON value, traversing objects only.
///
/// `"a.b.c"` descends three object levels. A missing key or a non-object
/// intermediate yields `None`. An empty path yields the value itself.
fn get_path<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
    if path.is_empty() {
        return Some(value);
    }
    let mut cur = value;
    for seg in path.split('.') {
        match cur {
            Value::Object(map) => cur = map.get(seg)?,
            _ => return None,
        }
    }
    Some(cur)
}

/// Set `left[name] = value` at the top level, applying the collision policy.
fn set_field(
    left: &mut Value,
    name: &str,
    value: Value,
    on_collision: OnCollision,
) -> Result<(), FaucetError> {
    let obj = match left {
        Value::Object(map) => map,
        _ => {
            return Err(FaucetError::Transform(
                "join can only enrich object-shaped records".into(),
            ));
        }
    };
    if obj.contains_key(name) {
        match on_collision {
            OnCollision::Overwrite => {
                obj.insert(name.to_string(), value);
            }
            OnCollision::Skip => {}
            OnCollision::Error => {
                return Err(FaucetError::Transform(format!(
                    "join projection '{name}' collides with an existing field (on_collision: error)"
                )));
            }
        }
    } else {
        obj.insert(name.to_string(), value);
    }
    Ok(())
}

/// Turn a JSON scalar into a canonical hash-map key string.
///
/// `Preserve` prefixes a type tag so `"42"` and `42` never collide. `Stringify`
/// coerces scalars to their plain string form so they do. Non-scalar keys
/// (objects/arrays) return `None` — they are not valid join keys.
fn canonical_key(value: &Value, mode: KeyNormalize) -> Option<String> {
    match mode {
        KeyNormalize::Preserve => match value {
            Value::String(s) => Some(format!("s:{s}")),
            Value::Number(n) => Some(format!("n:{n}")),
            Value::Bool(b) => Some(format!("b:{b}")),
            Value::Null => None,
            _ => None,
        },
        KeyNormalize::Stringify => match value {
            Value::String(s) => Some(s.clone()),
            Value::Number(n) => Some(n.to_string()),
            Value::Bool(b) => Some(b.to_string()),
            Value::Null => None,
            _ => None,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn cfg() -> JoinConfig {
        JoinConfig {
            build_key: "id".into(),
            probe_key: "customer_id".into(),
            projections: vec![
                Projection {
                    from: "tier".into(),
                    as_: "customer_tier".into(),
                },
                Projection {
                    from: "signup_date".into(),
                    as_: "customer_signup_date".into(),
                },
            ],
            ..Default::default()
        }
    }

    fn build_customers(join: &mut HashJoin) {
        join.add_build_page(vec![
            json!({"id": 1, "tier": "gold", "signup_date": "2020-01-01"}),
            json!({"id": 2, "tier": "silver", "signup_date": "2021-06-15"}),
        ])
        .unwrap();
    }

    // ── get_path ─────────────────────────────────────────────────────────────

    #[test]
    fn get_path_top_level() {
        let v = json!({"a": 1});
        assert_eq!(get_path(&v, "a"), Some(&json!(1)));
    }

    #[test]
    fn get_path_nested() {
        let v = json!({"a": {"b": {"c": 42}}});
        assert_eq!(get_path(&v, "a.b.c"), Some(&json!(42)));
    }

    #[test]
    fn get_path_missing() {
        let v = json!({"a": 1});
        assert_eq!(get_path(&v, "b"), None);
        assert_eq!(get_path(&v, "a.b"), None);
    }

    #[test]
    fn get_path_empty_returns_self() {
        let v = json!({"a": 1});
        assert_eq!(get_path(&v, ""), Some(&v));
    }

    // ── inner join ─────────────────────────────────────────────────────────────

    #[test]
    fn inner_match_enriches() {
        let mut j = HashJoin::new(cfg());
        build_customers(&mut j);
        let out = j
            .probe_page(vec![json!({"order": "A", "customer_id": 1})])
            .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["customer_tier"], json!("gold"));
        assert_eq!(out[0]["customer_signup_date"], json!("2020-01-01"));
        assert_eq!(out[0]["order"], json!("A"));
        assert_eq!(j.stats().matches, 1);
        assert_eq!(j.stats().misses, 0);
    }

    #[test]
    fn inner_no_match_drops() {
        let mut j = HashJoin::new(cfg());
        build_customers(&mut j);
        let out = j
            .probe_page(vec![json!({"order": "A", "customer_id": 999})])
            .unwrap();
        assert!(out.is_empty());
        assert_eq!(j.stats().matches, 0);
        assert_eq!(j.stats().misses, 1);
    }

    // ── left join ─────────────────────────────────────────────────────────────

    #[test]
    fn left_match_enriches() {
        let mut c = cfg();
        c.mode = JoinMode::Left;
        let mut j = HashJoin::new(c);
        build_customers(&mut j);
        let out = j
            .probe_page(vec![json!({"order": "A", "customer_id": 2})])
            .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["customer_tier"], json!("silver"));
    }

    #[test]
    fn left_no_match_passes_through_with_on_missing() {
        let mut c = cfg();
        c.mode = JoinMode::Left;
        c.on_missing = json!("UNKNOWN");
        let mut j = HashJoin::new(c);
        build_customers(&mut j);
        let out = j
            .probe_page(vec![json!({"order": "A", "customer_id": 999})])
            .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["order"], json!("A"));
        assert_eq!(out[0]["customer_tier"], json!("UNKNOWN"));
        assert_eq!(out[0]["customer_signup_date"], json!("UNKNOWN"));
        assert_eq!(j.stats().misses, 1);
    }

    #[test]
    fn left_no_match_default_on_missing_is_null() {
        let mut c = cfg();
        c.mode = JoinMode::Left;
        let mut j = HashJoin::new(c);
        build_customers(&mut j);
        let out = j.probe_page(vec![json!({"customer_id": 999})]).unwrap();
        assert_eq!(out[0]["customer_tier"], Value::Null);
    }

    // ── duplicate build keys ────────────────────────────────────────────────────

    #[test]
    fn duplicate_first_wins() {
        let mut c = cfg();
        c.on_duplicate = OnDuplicate::First;
        let mut j = HashJoin::new(c);
        j.add_build_page(vec![
            json!({"id": 1, "tier": "gold", "signup_date": "d1"}),
            json!({"id": 1, "tier": "platinum", "signup_date": "d2"}),
        ])
        .unwrap();
        let out = j.probe_page(vec![json!({"customer_id": 1})]).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["customer_tier"], json!("gold"));
        assert_eq!(j.stats().duplicates, 1);
    }

    #[test]
    fn duplicates_counts_keys_not_extra_records() {
        let mut j = HashJoin::new(cfg());
        j.add_build_page(vec![
            json!({"id": 1, "tier": "a"}),
            json!({"id": 1, "tier": "b"}),
            json!({"id": 1, "tier": "c"}),
            json!({"id": 2, "tier": "d"}),
            json!({"id": 2, "tier": "e"}),
            json!({"id": 3, "tier": "f"}),
        ])
        .unwrap();
        assert_eq!(j.stats().duplicates, 2);
    }

    #[test]
    fn duplicate_cartesian_emits_all() {
        let mut c = cfg();
        c.on_duplicate = OnDuplicate::Cartesian;
        let mut j = HashJoin::new(c);
        j.add_build_page(vec![
            json!({"id": 1, "tier": "gold", "signup_date": "d1"}),
            json!({"id": 1, "tier": "platinum", "signup_date": "d2"}),
        ])
        .unwrap();
        let out = j.probe_page(vec![json!({"customer_id": 1})]).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0]["customer_tier"], json!("gold"));
        assert_eq!(out[1]["customer_tier"], json!("platinum"));
        assert_eq!(j.stats().matches, 1);
    }

    // ── null keys ───────────────────────────────────────────────────────────────

    #[test]
    fn null_build_key_is_skipped() {
        let mut j = HashJoin::new(cfg());
        j.add_build_page(vec![
            json!({"id": null, "tier": "gold"}),
            json!({"tier": "silver"}), // missing id
            json!({"id": 3, "tier": "bronze", "signup_date": "d"}),
        ])
        .unwrap();
        assert_eq!(j.stats().build_nulls, 2);
        assert_eq!(j.indexed_keys(), 1);
    }

    #[test]
    fn null_probe_key_inner_drops() {
        let mut j = HashJoin::new(cfg());
        build_customers(&mut j);
        let out = j.probe_page(vec![json!({"order": "A"})]).unwrap();
        assert!(out.is_empty());
        assert_eq!(j.stats().misses, 1);
    }

    #[test]
    fn null_probe_key_left_emits_on_missing() {
        let mut c = cfg();
        c.mode = JoinMode::Left;
        let mut j = HashJoin::new(c);
        build_customers(&mut j);
        let out = j.probe_page(vec![json!({"order": "A"})]).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["customer_tier"], Value::Null);
    }

    // ── key normalization ────────────────────────────────────────────────────────

    #[test]
    fn preserve_does_not_coerce_types() {
        let mut c = cfg();
        c.key_normalize = KeyNormalize::Preserve;
        let mut j = HashJoin::new(c);
        j.add_build_page(vec![json!({"id": "42", "tier": "str", "signup_date": "d"})])
            .unwrap();
        // Probe with numeric 42 must NOT match the string "42".
        let out = j.probe_page(vec![json!({"customer_id": 42})]).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn stringify_coerces_types() {
        let mut c = cfg();
        c.key_normalize = KeyNormalize::Stringify;
        let mut j = HashJoin::new(c);
        j.add_build_page(vec![json!({"id": "42", "tier": "str", "signup_date": "d"})])
            .unwrap();
        let out = j.probe_page(vec![json!({"customer_id": 42})]).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["customer_tier"], json!("str"));
    }

    // ── projection edge cases ─────────────────────────────────────────────────────

    #[test]
    fn projection_missing_source_field_is_skipped() {
        let mut j = HashJoin::new(cfg());
        // Build record lacks `signup_date`.
        j.add_build_page(vec![json!({"id": 1, "tier": "gold"})])
            .unwrap();
        let out = j.probe_page(vec![json!({"customer_id": 1})]).unwrap();
        assert_eq!(out[0]["customer_tier"], json!("gold"));
        assert!(out[0].get("customer_signup_date").is_none());
        assert_eq!(j.stats().project_misses, 1);
    }

    #[test]
    fn projection_collision_overwrite() {
        let mut c = cfg();
        c.on_collision = OnCollision::Overwrite;
        let mut j = HashJoin::new(c);
        build_customers(&mut j);
        let out = j
            .probe_page(vec![json!({"customer_id": 1, "customer_tier": "OLD"})])
            .unwrap();
        assert_eq!(out[0]["customer_tier"], json!("gold"));
    }

    #[test]
    fn projection_collision_skip() {
        let mut c = cfg();
        c.on_collision = OnCollision::Skip;
        let mut j = HashJoin::new(c);
        build_customers(&mut j);
        let out = j
            .probe_page(vec![json!({"customer_id": 1, "customer_tier": "OLD"})])
            .unwrap();
        assert_eq!(out[0]["customer_tier"], json!("OLD"));
    }

    #[test]
    fn projection_collision_error() {
        let mut c = cfg();
        c.on_collision = OnCollision::Error;
        let mut j = HashJoin::new(c);
        build_customers(&mut j);
        let err = j
            .probe_page(vec![json!({"customer_id": 1, "customer_tier": "OLD"})])
            .unwrap_err();
        assert!(matches!(err, FaucetError::Transform(_)));
        assert!(err.to_string().contains("collides"));
    }

    #[test]
    fn nested_projection_path() {
        let mut c = cfg();
        c.projections = vec![Projection {
            from: "profile.tier".into(),
            as_: "tier".into(),
        }];
        let mut j = HashJoin::new(c);
        j.add_build_page(vec![json!({"id": 1, "profile": {"tier": "gold"}})])
            .unwrap();
        let out = j.probe_page(vec![json!({"customer_id": 1})]).unwrap();
        assert_eq!(out[0]["tier"], json!("gold"));
    }

    // ── overflow ─────────────────────────────────────────────────────────────────

    #[test]
    fn build_overflow_errors() {
        let mut c = cfg();
        c.max_build_records = 2;
        let mut j = HashJoin::new(c);
        let err = j
            .add_build_page(vec![
                json!({"id": 1, "tier": "a", "signup_date": "d"}),
                json!({"id": 2, "tier": "b", "signup_date": "d"}),
                json!({"id": 3, "tier": "c", "signup_date": "d"}),
            ])
            .unwrap_err();
        assert!(matches!(err, FaucetError::Transform(_)));
        assert!(err.to_string().contains("max_build_records"));
    }

    #[test]
    fn build_at_exactly_limit_ok() {
        let mut c = cfg();
        c.max_build_records = 2;
        let mut j = HashJoin::new(c);
        assert!(
            j.add_build_page(vec![
                json!({"id": 1, "tier": "a", "signup_date": "d"}),
                json!({"id": 2, "tier": "b", "signup_date": "d"}),
            ])
            .is_ok()
        );
    }

    // ── multi-page build ───────────────────────────────────────────────────────────

    #[test]
    fn build_across_multiple_pages() {
        let mut j = HashJoin::new(cfg());
        j.add_build_page(vec![json!({"id": 1, "tier": "gold", "signup_date": "d1"})])
            .unwrap();
        j.add_build_page(vec![
            json!({"id": 2, "tier": "silver", "signup_date": "d2"}),
        ])
        .unwrap();
        assert_eq!(j.indexed_keys(), 2);
        let out = j
            .probe_page(vec![json!({"customer_id": 1}), json!({"customer_id": 2})])
            .unwrap();
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn non_object_record_errors() {
        // A non-object probe record resolves to a null key (miss). Under inner
        // mode it is simply dropped; under left mode we attempt to project onto
        // it, which is where the "object-shaped records only" guard fires.
        let mut c = cfg();
        c.mode = JoinMode::Left;
        let mut j = HashJoin::new(c);
        build_customers(&mut j);
        let err = j.probe_page(vec![json!(42)]).unwrap_err();
        assert!(matches!(err, FaucetError::Transform(_)));
    }

    #[test]
    fn non_object_probe_inner_is_dropped_not_errored() {
        let mut j = HashJoin::new(cfg());
        build_customers(&mut j);
        // inner mode: non-object → null key → miss → dropped, no error.
        let out = j.probe_page(vec![json!(42)]).unwrap();
        assert!(out.is_empty());
        assert_eq!(j.stats().misses, 1);
    }

    #[test]
    fn bool_key_matches() {
        let mut c = cfg();
        c.build_key = "active".into();
        c.probe_key = "is_active".into();
        c.projections = vec![Projection {
            from: "label".into(),
            as_: "label".into(),
        }];
        let mut j = HashJoin::new(c);
        j.add_build_page(vec![json!({"active": true, "label": "yes"})])
            .unwrap();
        let out = j.probe_page(vec![json!({"is_active": true})]).unwrap();
        assert_eq!(out[0]["label"], json!("yes"));
    }

    #[test]
    fn enums_serde_roundtrip() {
        assert_eq!(serde_json::to_value(JoinMode::Left).unwrap(), json!("left"));
        assert_eq!(
            serde_json::from_value::<OnDuplicate>(json!("cartesian")).unwrap(),
            OnDuplicate::Cartesian
        );
        assert_eq!(
            serde_json::from_value::<OnCollision>(json!("skip")).unwrap(),
            OnCollision::Skip
        );
        assert_eq!(
            serde_json::from_value::<KeyNormalize>(json!("stringify")).unwrap(),
            KeyNormalize::Stringify
        );
    }

    #[test]
    fn join_mode_display() {
        assert_eq!(JoinMode::Inner.to_string(), "inner");
        assert_eq!(JoinMode::Left.to_string(), "left");
    }

    #[test]
    fn projection_serde_uses_as_rename() {
        let p: Projection = serde_json::from_value(json!({"from": "a", "as": "b"})).unwrap();
        assert_eq!(p.from, "a");
        assert_eq!(p.as_, "b");
    }
}
