//! Content verification primitives for `faucet verify` (#701).
//!
//! A pipeline can report green while its destination quietly diverges from the
//! source: a missed change event, a hand edit downstream, a retried page that
//! landed twice. Row counts (`reconcile:`) cannot see a row that exists on both
//! sides with different values. This module holds the **pure** building blocks
//! the verifier composes:
//!
//! - [`KeyRange`] — a half-open slice of an integer key space, bisectable, and
//!   convertible into the PK-range [`ShardSpec`] every SQL source already
//!   understands (so a range read needs no new connector code);
//! - [`Normalizer`] + [`row_hash`] — a canonical, type-tolerant fingerprint of
//!   one row's compared columns, so `1` and `1.0`, or two spellings of the same
//!   instant, hash alike;
//! - [`DigestAccumulator`] / [`ContentDigest`] — an order-independent digest of
//!   a set of rows (count + folded row hashes + observed key bounds), computed
//!   client-side while streaming;
//! - [`ServerDigest`] — the same shape computed *inside* the backend (only
//!   comparable when both sides report the same `algorithm`);
//! - [`diff_rows`] — the row-level comparison of two leaf ranges, keyed.
//!
//! Nothing here performs I/O; the CLI (`cli/src/verify/`) drives the sources.

use crate::shard::ShardSpec;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, HashMap};

/// A half-open integer key range `[lo, hi)`; `None` = unbounded on that side.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyRange {
    pub lo: Option<i64>,
    pub hi: Option<i64>,
}

impl KeyRange {
    /// The whole key space.
    pub const ALL: KeyRange = KeyRange { lo: None, hi: None };

    /// Whether `k` falls inside this range.
    pub fn contains(&self, k: i64) -> bool {
        self.lo.is_none_or(|lo| k >= lo) && self.hi.is_none_or(|hi| k < hi)
    }

    /// Number of integer keys the range spans, when both ends are bounded.
    pub fn width(&self) -> Option<u128> {
        match (self.lo, self.hi) {
            (Some(lo), Some(hi)) if hi > lo => Some((hi as i128 - lo as i128) as u128),
            (Some(_), Some(_)) => Some(0),
            _ => None,
        }
    }

    /// The PK-range shard descriptor a SQL source's `apply_shard` narrows to —
    /// the same shape [`plan_pk_shards`](crate::shard::plan_pk_shards) emits, so
    /// the range predicate is built by one shared, tested routine. NULL keys are
    /// owned by the unbounded-above range, so a NULL-key row is read exactly
    /// once across a partition of the key space.
    pub fn to_shard(&self, key: &str) -> ShardSpec {
        ShardSpec::new(
            format!(
                "{}..{}",
                self.lo
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "-inf".into()),
                self.hi
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "+inf".into())
            ),
            serde_json::json!({
                "key": key,
                "lo": self.lo.unwrap_or(0),
                "hi": self.hi.unwrap_or(0),
                "lo_unbounded": self.lo.is_none(),
                "hi_unbounded": self.hi.is_none(),
                "include_null": self.hi.is_none(),
            }),
        )
    }

    /// Split the range in two at its midpoint. An unbounded side is first
    /// clamped to the observed key bounds (`obs`, the union of both sides'
    /// `key_min..=key_max`), because a digest can only tell us where rows *are*.
    /// Returns `None` when the range cannot be split further (one key wide, or
    /// no observed bounds to clamp with) — the caller then treats it as a leaf.
    pub fn bisect(&self, obs: Option<(i64, i64)>) -> Option<(KeyRange, KeyRange)> {
        let lo = self.lo.or(obs.map(|(min, _)| min))?;
        let hi = self.hi.or(obs.map(|(_, max)| max.saturating_add(1)))?;
        if hi - lo < 2 {
            return None;
        }
        let mid = lo + (hi - lo) / 2;
        // Keep the outer edges as they were (possibly unbounded) so the two
        // halves still tile exactly the parent range.
        Some((
            KeyRange {
                lo: self.lo,
                hi: Some(mid),
            },
            KeyRange {
                lo: Some(mid),
                hi: self.hi,
            },
        ))
    }
}

impl std::fmt::Display for KeyRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match (self.lo, self.hi) {
            (None, None) => write!(f, "all"),
            (Some(lo), None) => write!(f, "[{lo}, +inf)"),
            (None, Some(hi)) => write!(f, "(-inf, {hi})"),
            (Some(lo), Some(hi)) => write!(f, "[{lo}, {hi})"),
        }
    }
}

/// Split `[min, max]` into `target` contiguous ranges tiling the whole key
/// space (the first is unbounded below, the last unbounded above), via the same
/// planner PK-range sharding uses.
pub fn plan_ranges(min: i64, max: i64, target: usize) -> Vec<KeyRange> {
    crate::shard::plan_pk_shards("k", min, max, target)
        .iter()
        .filter_map(crate::shard::PkShardBounds::from_spec)
        .map(|b| KeyRange {
            lo: (!b.lo_unbounded).then_some(b.lo),
            hi: (!b.hi_unbounded).then_some(b.hi),
        })
        .collect()
}

/// How values are canonicalised before hashing and comparing, so two backends'
/// spellings of the same value agree.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Normalizer {
    /// Round floats to this absolute tolerance before comparing (`0` = exact).
    #[serde(default)]
    pub float_tolerance: f64,
    /// Parse strings that look like timestamps (RFC 3339, or
    /// `YYYY-MM-DD HH:MM:SS[.fff]`) and compare them as UTC microseconds.
    #[serde(default = "default_true")]
    pub timestamps: bool,
    /// Treat a string holding a number (`"42"`, `"1.5"`) as that number.
    #[serde(default)]
    pub numeric_strings: bool,
}

fn default_true() -> bool {
    true
}

impl Default for Normalizer {
    fn default() -> Self {
        Self {
            float_tolerance: 0.0,
            timestamps: true,
            numeric_strings: false,
        }
    }
}

impl Normalizer {
    /// Fail-fast validation: a finite, non-negative tolerance.
    pub fn validate(&self) -> Result<(), crate::FaucetError> {
        if !self.float_tolerance.is_finite() || self.float_tolerance < 0.0 {
            return Err(crate::FaucetError::Config(format!(
                "verify: float_tolerance must be a finite number >= 0, got {}",
                self.float_tolerance
            )));
        }
        Ok(())
    }

    /// The canonical text of one value. Missing and `null` both canonicalise to
    /// the same token, so a column absent on one side equals `null` on the other.
    pub fn canonical(&self, v: Option<&Value>) -> String {
        match v {
            None | Some(Value::Null) => "\u{0}null".into(),
            Some(Value::Bool(b)) => (if *b { "t" } else { "f" }).into(),
            Some(Value::Number(n)) => self.canonical_number(n.as_f64(), n.as_i64(), n.as_u64()),
            Some(Value::String(s)) => self.canonical_string(s),
            Some(Value::Array(a)) => {
                let parts: Vec<String> = a.iter().map(|x| self.canonical(Some(x))).collect();
                format!("[{}]", parts.join(","))
            }
            Some(Value::Object(o)) => {
                // Sorted keys: object key order is not content.
                let sorted: BTreeMap<&String, &Value> = o.iter().collect();
                let parts: Vec<String> = sorted
                    .into_iter()
                    .map(|(k, x)| format!("{}:{}", k, self.canonical(Some(x))))
                    .collect();
                format!("{{{}}}", parts.join(","))
            }
        }
    }

    fn canonical_number(&self, f: Option<f64>, i: Option<i64>, u: Option<u64>) -> String {
        if let Some(i) = i {
            return format!("n{i}");
        }
        if let Some(u) = u {
            return format!("n{u}");
        }
        let Some(f) = f else {
            return "n?".into();
        };
        // An integral float is the same number as the integer (`1.0` == `1`).
        if f.fract() == 0.0 && f.abs() < 9.0e15 {
            return format!("n{}", f as i64);
        }
        if self.float_tolerance > 0.0 {
            let rounded = (f / self.float_tolerance).round() * self.float_tolerance;
            return format!("f{rounded:e}");
        }
        format!("f{f:e}")
    }

    fn canonical_string(&self, s: &str) -> String {
        if self.timestamps
            && let Some(micros) = parse_timestamp_micros(s)
        {
            return format!("ts{micros}");
        }
        if self.numeric_strings {
            let t = s.trim();
            if let Ok(i) = t.parse::<i64>() {
                return format!("n{i}");
            }
            if let Ok(f) = t.parse::<f64>()
                && f.is_finite()
            {
                return self.canonical_number(Some(f), None, None);
            }
        }
        format!("s{s}")
    }
}

/// UTC microseconds for a timestamp string in RFC 3339 form or the
/// `YYYY-MM-DD HH:MM:SS[.fff][Z|±hh:mm]` form SQL backends emit (a naive
/// timestamp is read as UTC). `None` when the string is not a timestamp.
pub fn parse_timestamp_micros(s: &str) -> Option<i64> {
    let s = s.trim();
    // Cheap pre-check: every timestamp we accept starts with a 4-digit year and
    // a dash, so ordinary strings never pay for the parse attempts.
    let b = s.as_bytes();
    if b.len() < 10 || !b[..4].iter().all(u8::is_ascii_digit) || b[4] != b'-' {
        return None;
    }
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return Some(dt.timestamp_micros());
    }
    let with_space = s.replacen(' ', "T", 1);
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(&with_space) {
        return Some(dt.timestamp_micros());
    }
    if let Ok(naive) = chrono::NaiveDateTime::parse_from_str(&with_space, "%Y-%m-%dT%H:%M:%S%.f") {
        return Some(naive.and_utc().timestamp_micros());
    }
    if let Ok(date) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        return Some(date.and_hms_opt(0, 0, 0)?.and_utc().timestamp_micros());
    }
    None
}

/// FNV-1a over `bytes` with a caller-chosen offset basis, so two independent
/// 64-bit hashes of the same text can be folded into one 128-bit fingerprint.
fn fnv1a_64(bytes: &[u8], basis: u64) -> u64 {
    let mut h = basis;
    for b in bytes {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

const FNV_BASIS_A: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_BASIS_B: u64 = 0x84222325_cbf29ce4;

/// A 128-bit fingerprint of `record`'s compared columns: the key columns plus
/// `columns` (every non-excluded field when `columns` is `None`), each
/// canonicalised through `norm`. Column order does not matter — the fields are
/// hashed in sorted name order.
pub fn row_hash(
    record: &Value,
    key: &[String],
    columns: Option<&[String]>,
    exclude: &[String],
    norm: &Normalizer,
) -> u128 {
    let obj = record.as_object();
    let mut text = String::new();
    let mut names: Vec<&str> = match columns {
        Some(cols) => cols.iter().map(String::as_str).collect(),
        None => obj
            .map(|o| o.keys().map(String::as_str).collect())
            .unwrap_or_default(),
    };
    for k in key {
        if !names.contains(&k.as_str()) {
            names.push(k.as_str());
        }
    }
    names.retain(|n| !is_excluded(n, exclude));
    names.sort_unstable();
    names.dedup();
    for name in names {
        text.push_str(name);
        text.push('=');
        text.push_str(&norm.canonical(obj.and_then(|o| o.get(name))));
        text.push('\u{1f}');
    }
    let a = fnv1a_64(text.as_bytes(), FNV_BASIS_A);
    let b = fnv1a_64(text.as_bytes(), FNV_BASIS_B);
    (u128::from(a) << 64) | u128::from(b)
}

/// Whether a column is excluded: an exact name, or a `prefix*` glob.
pub fn is_excluded(name: &str, exclude: &[String]) -> bool {
    exclude.iter().any(|e| match e.strip_suffix('*') {
        Some(prefix) => name.starts_with(prefix),
        None => e == name,
    })
}

/// The canonical text of a record's key tuple (`k1=…;k2=…`), used to match
/// rows across the two sides.
pub fn key_text(record: &Value, key: &[String], norm: &Normalizer) -> String {
    let obj = record.as_object();
    let mut out = String::new();
    for k in key {
        out.push_str(k);
        out.push('=');
        out.push_str(&norm.canonical(obj.and_then(|o| o.get(k))));
        out.push(';');
    }
    out
}

/// The integer value of a single-column key, when it is one.
pub fn key_int(record: &Value, key: &[String]) -> Option<i64> {
    if key.len() != 1 {
        return None;
    }
    match record.get(&key[0])? {
        Value::Number(n) => n.as_i64(),
        Value::String(s) => s.trim().parse().ok(),
        _ => None,
    }
}

/// An order-independent digest of a set of rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ContentDigest {
    pub rows: u64,
    /// Wrapping sum of the row fingerprints.
    pub sum: u128,
    /// XOR of the row fingerprints (catches a sum collision from swapped rows).
    pub xor: u128,
    /// Smallest / largest integer key seen (single integer keys only).
    pub key_min: Option<i64>,
    pub key_max: Option<i64>,
}

impl ContentDigest {
    /// Whether two digests describe the same row set.
    pub fn same(&self, other: &ContentDigest) -> bool {
        self.rows == other.rows && self.sum == other.sum && self.xor == other.xor
    }

    /// The union of the observed key bounds of two digests.
    pub fn bounds_union(&self, other: &ContentDigest) -> Option<(i64, i64)> {
        let min = match (self.key_min, other.key_min) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }?;
        let max = match (self.key_max, other.key_max) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (a, b) => a.or(b),
        }?;
        Some((min, max))
    }
}

/// Streams rows into a [`ContentDigest`].
#[derive(Debug, Default)]
pub struct DigestAccumulator {
    digest: ContentDigest,
}

impl DigestAccumulator {
    pub fn new() -> Self {
        Self::default()
    }

    /// Fold one row in.
    pub fn add(&mut self, hash: u128, key: Option<i64>) {
        self.digest.rows += 1;
        self.digest.sum = self.digest.sum.wrapping_add(hash);
        self.digest.xor ^= hash;
        if let Some(k) = key {
            self.digest.key_min = Some(self.digest.key_min.map_or(k, |m| m.min(k)));
            self.digest.key_max = Some(self.digest.key_max.map_or(k, |m| m.max(k)));
        }
    }

    pub fn finish(self) -> ContentDigest {
        self.digest
    }
}

/// A digest a backend computed itself, without shipping rows. Two server
/// digests are comparable only when their `algorithm` ids match — each backend
/// hashes its own text rendering of a row, so a Postgres digest says nothing
/// about a SQLite table. When the algorithms differ the verifier streams both
/// sides and uses [`ContentDigest`] instead.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerDigest {
    pub algorithm: String,
    pub rows: u64,
    /// Opaque digest text (a decimal sum, a hex string — whatever the
    /// algorithm defines).
    pub digest: String,
    pub key_min: Option<i64>,
    pub key_max: Option<i64>,
}

impl ServerDigest {
    /// Whether two server digests agree. Different algorithms never agree —
    /// the caller must not treat that as a difference but as "not comparable".
    pub fn comparable(&self, other: &ServerDigest) -> bool {
        self.algorithm == other.algorithm
    }

    pub fn same(&self, other: &ServerDigest) -> bool {
        self.comparable(other) && self.rows == other.rows && self.digest == other.digest
    }

    pub fn bounds_union(&self, other: &ServerDigest) -> Option<(i64, i64)> {
        let min = match (self.key_min, other.key_min) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }?;
        let max = match (self.key_max, other.key_max) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (a, b) => a.or(b),
        }?;
        Some((min, max))
    }
}

/// Why one key differs between the two sides.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DifferenceKind {
    /// The source has the key, the destination does not.
    MissingInDest,
    /// The destination has the key, the source does not.
    ExtraInDest,
    /// Both have the key; these columns differ.
    Changed { columns: Vec<String> },
    /// The key appears more than once on one side, so rows cannot be matched.
    Duplicate { side: Side, count: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    Source,
    Destination,
}

/// One differing key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Difference {
    /// The key columns and their values (from whichever side has the row).
    pub key: Value,
    #[serde(flatten)]
    pub kind: DifferenceKind,
}

impl Difference {
    /// Whether repairing this difference means writing the source row.
    pub fn needs_upsert(&self) -> bool {
        matches!(
            self.kind,
            DifferenceKind::MissingInDest | DifferenceKind::Changed { .. }
        )
    }

    /// Whether repairing this difference means deleting the destination row.
    pub fn needs_delete(&self) -> bool {
        matches!(self.kind, DifferenceKind::ExtraInDest)
    }
}

/// The columns whose canonical values differ between two rows.
fn changed_columns(
    a: &Value,
    b: &Value,
    columns: Option<&[String]>,
    exclude: &[String],
    norm: &Normalizer,
) -> Vec<String> {
    let names: Vec<String> = match columns {
        Some(cols) => cols.to_vec(),
        None => {
            let mut set: Vec<String> = Vec::new();
            for side in [a, b] {
                if let Some(o) = side.as_object() {
                    for k in o.keys() {
                        if !set.contains(k) {
                            set.push(k.clone());
                        }
                    }
                }
            }
            set
        }
    };
    let mut out: Vec<String> = names
        .into_iter()
        .filter(|n| !is_excluded(n, exclude))
        .filter(|n| norm.canonical(a.get(n.as_str())) != norm.canonical(b.get(n.as_str())))
        .collect();
    out.sort();
    out
}

fn key_object(record: &Value, key: &[String]) -> Value {
    let mut m = Map::new();
    for k in key {
        m.insert(k.clone(), record.get(k).cloned().unwrap_or(Value::Null));
    }
    Value::Object(m)
}

/// Compare two leaf ranges row by row. Returns every differing key, ordered by
/// key text so the report is stable. Rows on either side are matched on the
/// canonical key text; a key present more than once on a side is reported as a
/// [`DifferenceKind::Duplicate`] and not matched.
pub fn diff_rows(
    source: &[Value],
    dest: &[Value],
    key: &[String],
    columns: Option<&[String]>,
    exclude: &[String],
    norm: &Normalizer,
) -> Vec<Difference> {
    let mut by_key: HashMap<String, (u64, &Value)> = HashMap::with_capacity(dest.len());
    for d in dest {
        let e = by_key.entry(key_text(d, key, norm)).or_insert((0, d));
        e.0 += 1;
    }
    // A key repeated on the source side cannot be matched row-for-row: it is
    // reported once as a duplicate and takes no part in the comparison.
    let mut source_counts: HashMap<String, u64> = HashMap::with_capacity(source.len());
    for s in source {
        *source_counts.entry(key_text(s, key, norm)).or_insert(0) += 1;
    }
    let mut out: Vec<(String, Difference)> = Vec::new();
    let mut reported: std::collections::HashSet<String> = std::collections::HashSet::new();
    for s in source {
        let kt = key_text(s, key, norm);
        let count = source_counts[&kt];
        if count > 1 {
            if reported.insert(kt.clone()) {
                by_key.remove(&kt);
                out.push((
                    kt,
                    Difference {
                        key: key_object(s, key),
                        kind: DifferenceKind::Duplicate {
                            side: Side::Source,
                            count,
                        },
                    },
                ));
            }
            continue;
        }
        match by_key.remove(&kt) {
            None => out.push((
                kt,
                Difference {
                    key: key_object(s, key),
                    kind: DifferenceKind::MissingInDest,
                },
            )),
            Some((count, _)) if count > 1 => out.push((
                kt,
                Difference {
                    key: key_object(s, key),
                    kind: DifferenceKind::Duplicate {
                        side: Side::Destination,
                        count,
                    },
                },
            )),
            Some((_, d)) => {
                let cols = changed_columns(s, d, columns, exclude, norm);
                if !cols.is_empty() {
                    out.push((
                        kt,
                        Difference {
                            key: key_object(s, key),
                            kind: DifferenceKind::Changed { columns: cols },
                        },
                    ));
                }
            }
        }
    }
    for (kt, (count, d)) in by_key {
        out.push((
            kt,
            Difference {
                key: key_object(d, key),
                kind: if count > 1 {
                    DifferenceKind::Duplicate {
                        side: Side::Destination,
                        count,
                    }
                } else {
                    DifferenceKind::ExtraInDest
                },
            },
        ));
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out.into_iter().map(|(_, d)| d).collect()
}

/// Summary of one verification, serialisable for `--json` and the HTTP API.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VerifyReport {
    /// Ranges compared by digest (both passes).
    pub ranges_compared: u64,
    /// Ranges whose digests disagreed and were bisected/fetched.
    pub ranges_differing: u64,
    /// Rows the source side produced across the leaf fetches.
    pub rows_fetched_source: u64,
    /// Rows the destination side produced across the leaf fetches.
    pub rows_fetched_dest: u64,
    /// Whether digests were computed inside the backends (no rows shipped for
    /// matching ranges) rather than by streaming.
    pub server_digests: bool,
    pub differences: Vec<Difference>,
    /// The scan stopped at `max_rows_scanned`; `differences` is a lower bound.
    pub truncated: bool,
    /// Rows written back by `--repair` (upserts), if repair ran.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repaired_upserts: Option<u64>,
    /// Rows deleted by `--repair --allow-delete`, if repair ran.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repaired_deletes: Option<u64>,
}

impl VerifyReport {
    /// Counts per difference kind, for the human summary.
    pub fn tally(&self) -> (usize, usize, usize, usize) {
        let mut missing = 0;
        let mut extra = 0;
        let mut changed = 0;
        let mut dup = 0;
        for d in &self.differences {
            match d.kind {
                DifferenceKind::MissingInDest => missing += 1,
                DifferenceKind::ExtraInDest => extra += 1,
                DifferenceKind::Changed { .. } => changed += 1,
                DifferenceKind::Duplicate { .. } => dup += 1,
            }
        }
        (missing, extra, changed, dup)
    }

    /// Whether the two sides matched (no differences and nothing skipped).
    pub fn equal(&self) -> bool {
        self.differences.is_empty() && !self.truncated
    }

    /// Whether a repair re-synced every reported difference (and the report is
    /// complete), so the destination now matches.
    pub fn healed(&self) -> bool {
        if self.truncated || self.differences.is_empty() {
            return false;
        }
        let repaired = self.repaired_upserts.unwrap_or(0) + self.repaired_deletes.unwrap_or(0);
        repaired >= self.differences.len() as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn norm() -> Normalizer {
        Normalizer::default()
    }

    #[test]
    fn ranges_tile_the_key_space_and_bisect_to_leaves() {
        let ranges = plan_ranges(1, 100, 4);
        assert_eq!(ranges.len(), 4);
        assert_eq!(ranges[0].lo, None, "first is unbounded below");
        assert_eq!(ranges[3].hi, None, "last is unbounded above");
        for k in [-5, 1, 50, 100, 1_000] {
            assert_eq!(ranges.iter().filter(|r| r.contains(k)).count(), 1, "{k}");
        }
        let (a, b) = KeyRange {
            lo: Some(0),
            hi: Some(10),
        }
        .bisect(None)
        .unwrap();
        assert_eq!(
            (a.lo, a.hi, b.lo, b.hi),
            (Some(0), Some(5), Some(5), Some(10))
        );
        // An unbounded side clamps to the observed bounds.
        let (a, b) = KeyRange::ALL.bisect(Some((10, 19))).unwrap();
        assert_eq!((a.lo, a.hi), (None, Some(15)));
        assert_eq!((b.lo, b.hi), (Some(15), None));
        assert!(KeyRange::ALL.bisect(None).is_none());
        assert!(
            KeyRange {
                lo: Some(3),
                hi: Some(4)
            }
            .bisect(None)
            .is_none(),
            "one key wide is a leaf"
        );
        assert_eq!(
            KeyRange {
                lo: Some(3),
                hi: Some(4)
            }
            .width(),
            Some(1)
        );
        assert_eq!(KeyRange::ALL.width(), None);
        assert_eq!(KeyRange::ALL.to_string(), "all");
        assert_eq!(
            KeyRange {
                lo: Some(1),
                hi: None
            }
            .to_string(),
            "[1, +inf)"
        );
        assert_eq!(
            KeyRange {
                lo: None,
                hi: Some(9)
            }
            .to_string(),
            "(-inf, 9)"
        );
    }

    #[test]
    fn range_shard_is_the_pk_range_shape_sql_sources_parse() {
        let shard = KeyRange {
            lo: Some(5),
            hi: None,
        }
        .to_shard("id");
        let bounds = crate::shard::PkShardBounds::from_spec(&shard).unwrap();
        assert_eq!(
            (bounds.lo, bounds.hi_unbounded, bounds.include_null),
            (5, true, true)
        );
        assert_eq!(shard.id, "5..+inf");
        let sql = bounds.wrap("SELECT * FROM t", |s| format!("\"{s}\""));
        assert!(
            sql.contains("\"id\" >= 5") && sql.contains("IS NULL"),
            "{sql}"
        );
    }

    #[test]
    fn canonical_values_agree_across_spellings() {
        let n = norm();
        assert_eq!(n.canonical(Some(&json!(1))), n.canonical(Some(&json!(1.0))));
        assert_eq!(n.canonical(None), n.canonical(Some(&json!(null))));
        assert_ne!(n.canonical(Some(&json!("1"))), n.canonical(Some(&json!(1))));
        assert_eq!(
            n.canonical(Some(&json!("2026-09-25T10:00:00Z"))),
            n.canonical(Some(&json!("2026-09-25 12:00:00+02:00")))
        );
        assert_eq!(
            n.canonical(Some(&json!("2026-09-25 10:00:00.000"))),
            n.canonical(Some(&json!("2026-09-25T10:00:00Z")))
        );
        assert_eq!(
            n.canonical(Some(&json!("2026-09-25"))),
            n.canonical(Some(&json!("2026-09-25T00:00:00Z")))
        );
        assert_eq!(
            n.canonical(Some(&json!({"b": 1, "a": [true, null]}))),
            n.canonical(Some(&json!({"a": [true, null], "b": 1})))
        );
        // Plain strings never pay for a timestamp parse and stay themselves.
        assert_eq!(n.canonical(Some(&json!("hello"))), "shello");
        assert_eq!(n.canonical(Some(&json!("2026-x"))), "s2026-x");
        assert!(parse_timestamp_micros("not a date").is_none());
    }

    #[test]
    fn float_tolerance_and_numeric_strings_are_opt_in() {
        let exact = norm();
        assert_ne!(
            exact.canonical(Some(&json!(1.00000001))),
            exact.canonical(Some(&json!(1.00000002)))
        );
        let loose = Normalizer {
            float_tolerance: 1e-6,
            ..norm()
        };
        assert_eq!(
            loose.canonical(Some(&json!(1.00000001))),
            loose.canonical(Some(&json!(1.00000002)))
        );
        let strings = Normalizer {
            numeric_strings: true,
            ..norm()
        };
        assert_eq!(
            strings.canonical(Some(&json!("42"))),
            strings.canonical(Some(&json!(42)))
        );
        assert_eq!(
            strings.canonical(Some(&json!("1.5"))),
            strings.canonical(Some(&json!(1.5)))
        );
        assert_eq!(strings.canonical(Some(&json!("abc"))), "sabc");
        assert!(loose.validate().is_ok());
        assert!(
            Normalizer {
                float_tolerance: -1.0,
                ..norm()
            }
            .validate()
            .is_err()
        );
        assert!(
            Normalizer {
                float_tolerance: f64::NAN,
                ..norm()
            }
            .validate()
            .is_err()
        );
    }

    #[test]
    fn row_hash_ignores_field_order_and_excluded_columns() {
        let key = vec!["id".to_string()];
        let a = json!({"id": 1, "name": "x", "_faucet_run_id": "r1"});
        let b = json!({"name": "x", "id": 1, "_faucet_run_id": "r2"});
        let ex = vec!["_faucet_*".to_string()];
        assert_eq!(
            row_hash(&a, &key, None, &ex, &norm()),
            row_hash(&b, &key, None, &ex, &norm())
        );
        assert_ne!(
            row_hash(&a, &key, None, &[], &norm()),
            row_hash(&b, &key, None, &[], &norm()),
            "without the exclusion the run id differs"
        );
        // An explicit column list hashes only those (plus the key).
        let c = json!({"id": 1, "name": "y", "other": 5});
        let cols = vec!["other".to_string()];
        assert_eq!(
            row_hash(&a, &key, Some(&cols), &[], &norm()),
            row_hash(
                &json!({"id": 1, "name": "zzz"}),
                &key,
                Some(&cols),
                &[],
                &norm()
            )
        );
        assert_ne!(
            row_hash(&c, &key, Some(&cols), &[], &norm()),
            row_hash(&a, &key, Some(&cols), &[], &norm())
        );
        assert!(is_excluded("_faucet_loaded_at", &ex));
        assert!(!is_excluded("faucet", &ex));
        assert!(is_excluded("name", &["name".to_string()]));
        assert_eq!(key_int(&a, &key), Some(1));
        assert_eq!(key_int(&json!({"id": "7"}), &key), Some(7));
        assert_eq!(key_int(&a, &["id".into(), "name".into()]), None);
        assert_eq!(key_int(&json!({"id": true}), &key), None);
        assert_eq!(key_text(&a, &key, &norm()), "id=n1;");
    }

    #[test]
    fn digest_is_order_independent_and_tracks_bounds() {
        let key = vec!["id".to_string()];
        let rows = [json!({"id": 3, "v": "a"}), json!({"id": 1, "v": "b"})];
        let mut fwd = DigestAccumulator::new();
        let mut rev = DigestAccumulator::new();
        for r in &rows {
            fwd.add(row_hash(r, &key, None, &[], &norm()), key_int(r, &key));
        }
        for r in rows.iter().rev() {
            rev.add(row_hash(r, &key, None, &[], &norm()), key_int(r, &key));
        }
        let (f, r) = (fwd.finish(), rev.finish());
        assert!(f.same(&r));
        assert_eq!((f.rows, f.key_min, f.key_max), (2, Some(1), Some(3)));
        let mut other = DigestAccumulator::new();
        other.add(
            row_hash(&json!({"id": 3, "v": "a"}), &key, None, &[], &norm()),
            Some(3),
        );
        let o = other.finish();
        assert!(!f.same(&o));
        assert_eq!(f.bounds_union(&o), Some((1, 3)));
        assert_eq!(
            ContentDigest::default().bounds_union(&ContentDigest::default()),
            None
        );
        assert_eq!(ContentDigest::default().bounds_union(&o), Some((3, 3)));
    }

    #[test]
    fn server_digests_compare_only_within_one_algorithm() {
        let pg = ServerDigest {
            algorithm: "postgres:v1".into(),
            rows: 2,
            digest: "12".into(),
            key_min: Some(1),
            key_max: Some(5),
        };
        let same = ServerDigest {
            key_min: Some(2),
            key_max: Some(9),
            ..pg.clone()
        };
        let other_algo = ServerDigest {
            algorithm: "sqlite:v1".into(),
            ..pg.clone()
        };
        assert!(pg.same(&same));
        assert!(!pg.comparable(&other_algo));
        assert!(!pg.same(&other_algo));
        assert!(!pg.same(&ServerDigest {
            digest: "13".into(),
            ..pg.clone()
        }));
        assert_eq!(pg.bounds_union(&same), Some((1, 9)));
        assert_eq!(
            pg.bounds_union(&ServerDigest {
                key_min: None,
                key_max: None,
                ..pg.clone()
            }),
            Some((1, 5))
        );
    }

    #[test]
    fn diff_rows_reports_every_kind_in_key_order() {
        let key = vec!["id".to_string()];
        let source = vec![
            json!({"id": 1, "v": "a"}),
            json!({"id": 2, "v": "b"}),
            json!({"id": 3, "v": "c"}),
            json!({"id": 5, "v": "e"}),
            json!({"id": 5, "v": "e2"}),
        ];
        let dest = vec![
            json!({"id": 1, "v": "a", "_faucet_run_id": "r"}),
            json!({"id": 2, "v": "B"}),
            json!({"id": 4, "v": "d"}),
            json!({"id": 6, "v": "f"}),
            json!({"id": 6, "v": "f"}),
        ];
        let ex = vec!["_faucet_*".to_string()];
        let diffs = diff_rows(&source, &dest, &key, None, &ex, &norm());
        let kinds: Vec<String> = diffs
            .iter()
            .map(|d| format!("{}:{:?}", d.key["id"], d.kind))
            .collect();
        assert_eq!(diffs.len(), 5, "{kinds:?}");
        assert_eq!(
            diffs[0].kind,
            DifferenceKind::Changed {
                columns: vec!["v".into()]
            }
        );
        assert_eq!(diffs[0].key, json!({"id": 2}));
        assert_eq!(diffs[1].kind, DifferenceKind::MissingInDest);
        assert_eq!(diffs[1].key, json!({"id": 3}));
        assert_eq!(diffs[2].kind, DifferenceKind::ExtraInDest);
        assert_eq!(diffs[2].key, json!({"id": 4}));
        assert_eq!(
            diffs[3].kind,
            DifferenceKind::Duplicate {
                side: Side::Source,
                count: 2
            }
        );
        assert_eq!(
            diffs[4].kind,
            DifferenceKind::Duplicate {
                side: Side::Destination,
                count: 2
            }
        );
        assert!(diffs[0].needs_upsert() && diffs[1].needs_upsert());
        assert!(diffs[2].needs_delete() && !diffs[2].needs_upsert());
        assert!(!diffs[3].needs_upsert() && !diffs[3].needs_delete());
        // Identical sides: nothing.
        assert!(diff_rows(&source[..1], &dest[..1], &key, None, &ex, &norm()).is_empty());
        // A key repeated on both sides is one report, on the source side, with
        // the source's count; it is never also "missing" or "extra".
        let three = vec![json!({"id": 9}), json!({"id": 9}), json!({"id": 9})];
        let d = diff_rows(
            &three,
            &[json!({"id": 9}), json!({"id": 9})],
            &key,
            None,
            &[],
            &norm(),
        );
        assert_eq!(d.len(), 1, "a duplicated key is reported once: {d:?}");
        assert_eq!(
            d[0].kind,
            DifferenceKind::Duplicate {
                side: Side::Source,
                count: 3
            }
        );
    }

    #[test]
    fn diff_rows_with_explicit_columns_ignores_the_rest() {
        let key = vec!["id".to_string()];
        let cols = vec!["amount".to_string()];
        let s = vec![json!({"id": 1, "amount": 10, "note": "x"})];
        let d = vec![json!({"id": 1, "amount": 10, "note": "y"})];
        assert!(diff_rows(&s, &d, &key, Some(&cols), &[], &norm()).is_empty());
        let d2 = vec![json!({"id": 1, "amount": 11})];
        let diffs = diff_rows(&s, &d2, &key, Some(&cols), &[], &norm());
        assert_eq!(
            diffs[0].kind,
            DifferenceKind::Changed {
                columns: vec!["amount".into()]
            }
        );
    }

    #[test]
    fn report_healed_only_when_every_difference_was_repaired() {
        let mut r = VerifyReport::default();
        assert!(!r.healed(), "nothing to heal");
        r.differences.push(Difference {
            key: json!({"id": 1}),
            kind: DifferenceKind::MissingInDest,
        });
        r.differences.push(Difference {
            key: json!({"id": 2}),
            kind: DifferenceKind::ExtraInDest,
        });
        assert!(!r.healed(), "no repair ran");
        r.repaired_upserts = Some(1);
        r.repaired_deletes = Some(0);
        assert!(!r.healed(), "the extra row was not deleted");
        r.repaired_deletes = Some(1);
        assert!(r.healed());
        r.truncated = true;
        assert!(!r.healed(), "a truncated report cannot claim to be healed");
    }

    #[test]
    fn report_tallies_and_equality() {
        let mut r = VerifyReport::default();
        assert!(r.equal());
        r.differences.push(Difference {
            key: json!({"id": 1}),
            kind: DifferenceKind::MissingInDest,
        });
        r.differences.push(Difference {
            key: json!({"id": 2}),
            kind: DifferenceKind::ExtraInDest,
        });
        r.differences.push(Difference {
            key: json!({"id": 3}),
            kind: DifferenceKind::Changed { columns: vec![] },
        });
        r.differences.push(Difference {
            key: json!({"id": 4}),
            kind: DifferenceKind::Duplicate {
                side: Side::Source,
                count: 2,
            },
        });
        assert_eq!(r.tally(), (1, 1, 1, 1));
        assert!(!r.equal());
        let truncated = VerifyReport {
            truncated: true,
            ..Default::default()
        };
        assert!(!truncated.equal(), "a truncated scan proves nothing");
        let json = serde_json::to_value(&r).unwrap();
        assert_eq!(json["differences"][0]["kind"], "missing_in_dest");
        assert_eq!(json["differences"][2]["kind"], "changed");
    }
}
