//! Schema-drift detection + policy types (issue #194).
//!
//! Drift is the divergence between an incoming page's inferred top-level shape
//! (via [`crate::schema::infer_schema`]) and the sink's live destination schema
//! (via [`crate::Sink::current_schema`]). The pure [`diff_schema`] classifies
//! each top-level column into one bucket; [`SchemaDriftPolicy`] decides what the
//! pipeline does with the result. Nested objects are treated as a single column.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// One column's drift, expressed in JSON-Schema type-fragment terms
/// (e.g. `{"type":"integer"}` or `{"type":["string","null"]}`).
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnChange {
    /// Top-level column name.
    pub name: String,
    /// Destination type fragment; `None` for an addition (not in destination).
    pub from: Option<Value>,
    /// Inferred type fragment from the incoming page.
    pub to: Value,
}

/// Result of diffing a page's inferred shape against the destination schema.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SchemaDiff {
    /// In the page, not in the destination.
    pub additions: Vec<ColumnChange>,
    /// Existing column whose type widened losslessly (e.g. integer→number,
    /// or gained nullability).
    pub widenings: Vec<ColumnChange>,
    /// Existing column whose type changed in a way that cannot be auto-applied
    /// (narrowing / incompatible type swap).
    pub incompatible: Vec<ColumnChange>,
    /// In the destination and NOT NULL, absent from the page — would fail an
    /// insert unless relaxed to nullable.
    pub droppable_required: Vec<String>,
}

impl SchemaDiff {
    /// `true` when no drift of any kind was detected.
    pub fn is_empty(&self) -> bool {
        self.additions.is_empty()
            && self.widenings.is_empty()
            && self.incompatible.is_empty()
            && self.droppable_required.is_empty()
    }

    /// Column names that drifted, for error messages / metrics.
    pub fn changed_columns(&self) -> Vec<String> {
        self.additions
            .iter()
            .chain(&self.widenings)
            .chain(&self.incompatible)
            .map(|c| c.name.clone())
            .chain(self.droppable_required.iter().cloned())
            .collect()
    }
}

/// The applyable subset of a [`SchemaDiff`] handed to [`crate::Sink::evolve_schema`].
/// Never carries `incompatible` columns — those are routed by the policy.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SchemaEvolution {
    pub additions: Vec<ColumnChange>,
    pub widenings: Vec<ColumnChange>,
    /// Columns to relax from NOT NULL to nullable.
    pub relax_nullability: Vec<String>,
}

impl SchemaEvolution {
    pub fn is_empty(&self) -> bool {
        self.additions.is_empty() && self.widenings.is_empty() && self.relax_nullability.is_empty()
    }
}

/// The set of JSON-Schema primitive type names a fragment carries (excluding `null`),
/// plus whether `null` is present.
fn type_set(fragment: &Value) -> (Vec<String>, bool) {
    let mut names = Vec::new();
    let mut nullable = false;
    match fragment.get("type") {
        Some(Value::String(t)) => {
            if t == "null" {
                nullable = true
            } else {
                names.push(t.clone())
            }
        }
        Some(Value::Array(arr)) => {
            for v in arr {
                if let Some(t) = v.as_str() {
                    if t == "null" {
                        nullable = true
                    } else {
                        names.push(t.to_string())
                    }
                }
            }
        }
        _ => {}
    }
    names.sort();
    (names, nullable)
}

/// Are the page's values already acceptable as-is by the destination?
///
/// True iff the page introduces no nulls a non-nullable destination would
/// reject, AND every non-null base type the page carries is accepted by the
/// destination (an exact base-type match, or `integer` landing in a `number`
/// column). `fits` is never gated by `allow_widening` — a fitting page is not
/// drift at all.
fn fits(dest: &Value, page: &Value) -> bool {
    let (dn, dnull) = type_set(dest);
    let (pn, pnull) = type_set(page);
    // The page must not introduce nulls a non-nullable destination rejects.
    if pnull && !dnull {
        return false;
    }
    let also = also_accepts(dest);
    // Every page base type must be accepted by the destination.
    pn.iter().all(|t| {
        dn.contains(t)
            || also.contains(&t.as_str())
            || (t == "integer" && dn.iter().any(|d| d == "number"))
    })
}

/// Whether one record's own `value` for a column fits the destination column
/// type `dest` — the per-row test behind a column a page-level diff reported
/// as widened or incompatible, so a row whose value already fits is kept.
pub(crate) fn value_fits(dest: &Value, name: &str, value: &Value) -> bool {
    let destination = serde_json::json!({"type": "object", "properties": {name: dest}});
    let mut row = serde_json::Map::new();
    row.insert(name.to_string(), value.clone());
    let page = infer_page_schema(&destination, &[Value::Object(row)]);
    page.get("properties")
        .and_then(|p| p.get(name))
        .is_some_and(|fragment| fits(dest, fragment))
}

/// Key a sink may add to a column fragment in [`crate::Sink::current_schema`]
/// listing extra JSON types the column stores faithfully without changing its
/// declared type — e.g. `{"type":"integer","x-faucet-also-accepts":["boolean"]}`
/// for a column the sink itself creates for booleans. Such page types fit the
/// column; they never take part in widening.
pub const DRIFT_ALSO_ACCEPTS: &str = "x-faucet-also-accepts";

fn also_accepts(dest: &Value) -> Vec<&str> {
    dest.get(DRIFT_ALSO_ACCEPTS)
        .and_then(Value::as_array)
        .map(|a| a.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default()
}

/// `page` without the base types `dest` lists under [`DRIFT_ALSO_ACCEPTS`]
/// (and does not declare), so they stay out of widening.
fn without_also_accepted(dest: &Value, page: &Value) -> Value {
    let also = also_accepts(dest);
    if also.is_empty() {
        return page.clone();
    }
    let (dn, _) = type_set(dest);
    let (pn, pnull) = type_set(page);
    let mut kept: Vec<Value> = pn
        .into_iter()
        .filter(|t| dn.contains(t) || !also.contains(&t.as_str()))
        .map(Value::String)
        .collect();
    if pnull {
        kept.push(Value::String("null".into()));
    }
    let mut out = page.clone();
    if let Some(obj) = out.as_object_mut() {
        let ty = match kept.len() {
            1 => kept.remove(0),
            _ => Value::Array(kept),
        };
        obj.insert("type".into(), ty);
    }
    out
}

/// Can the destination column losslessly evolve to accept the page?
///
/// The merged non-null base family must collapse to a single base type: take
/// `dest ∪ page`, drop `integer` if `number` is present (int→number collapse),
/// and require exactly one element. Nullability relaxation is always evolvable,
/// so only the base-family check gates this.
fn evolvable(dest: &Value, page: &Value) -> bool {
    let (dn, _) = type_set(dest);
    let (pn, _) = type_set(page);
    let mut merged: Vec<String> = dn.into_iter().chain(pn).collect();
    merged.sort();
    merged.dedup();
    if merged.iter().any(|t| t == "number") {
        merged.retain(|t| t != "integer");
    }
    merged.len() == 1
}

/// True when the non-null base type family changes from `from` to `to`
/// (after the integer→number collapse) — i.e. the column needs an `ALTER TYPE`.
///
/// Mirrors the `evolvable` base-family collapse: take `from ∪ to`, drop
/// `integer` when `number` is present, and treat the change as a base-type
/// widening only when the resulting family is no longer the destination's
/// original family. Nullability changes alone never count here.
pub fn base_widened(from: &Value, to: &Value) -> bool {
    let (dn, _) = type_set(from);
    let (pn, _) = type_set(to);
    // Collapse the destination's own family the same way (so a nullable-only
    // change yields an identical family and returns false).
    let collapse = |names: Vec<String>| -> Vec<String> {
        let mut m: Vec<String> = names;
        m.sort();
        m.dedup();
        if m.iter().any(|t| t == "number") {
            m.retain(|t| t != "integer");
        }
        m
    };
    let dest_family = collapse(dn.clone());
    let merged = collapse(dn.into_iter().chain(pn).collect());
    merged != dest_family
}

/// True when `to` permits null but `from` does not — the column needs its
/// `NOT NULL` constraint relaxed.
pub fn adds_null(from: &Value, to: &Value) -> bool {
    let (_, fnull) = type_set(from);
    let (_, tnull) = type_set(to);
    tnull && !fnull
}

/// Is `s` the exact text of a JSON number — the rendering
/// [`crate::fidelity`] gives an exact decimal or an integer past `i64`?
fn is_numeric_text(s: &str) -> bool {
    let b = s.as_bytes();
    let mut i = 0;
    if b.first() == Some(&b'-') {
        i += 1;
    }
    let digits = |i: &mut usize| {
        let start = *i;
        while *i < b.len() && b[*i].is_ascii_digit() {
            *i += 1;
        }
        *i > start
    };
    if !digits(&mut i) {
        return false;
    }
    if i < b.len() && b[i] == b'.' {
        i += 1;
        if !digits(&mut i) {
            return false;
        }
    }
    if i < b.len() && (b[i] == b'e' || b[i] == b'E') {
        i += 1;
        if i < b.len() && (b[i] == b'+' || b[i] == b'-') {
            i += 1;
        }
        if !digits(&mut i) {
            return false;
        }
    }
    i == b.len()
}

/// Infer a page's shape for [`diff_schema`] against `destination`.
///
/// Like [`crate::schema::infer_schema`], except that a top-level column the
/// destination stores as `number` whose string values on this page are all
/// exact numeric text (how [`crate::fidelity`] renders exact decimals and
/// big integers) is typed `number`: those values land in the column exactly,
/// so they are not drift. A string that is not numeric text keeps the column
/// `string`.
pub fn infer_page_schema(destination: &Value, records: &[Value]) -> Value {
    let mut inferred = crate::schema::infer_schema(records);
    let Some(dest_props) = destination.get("properties").and_then(Value::as_object) else {
        return inferred;
    };
    let Some(props) = inferred
        .get_mut("properties")
        .and_then(Value::as_object_mut)
    else {
        return inferred;
    };
    for (name, fragment) in props.iter_mut() {
        let Some(dest) = dest_props.get(name) else {
            continue;
        };
        let (dn, _) = type_set(dest);
        let (pn, pnull) = type_set(fragment);
        if !dn.iter().any(|t| t == "number") || !pn.iter().any(|t| t == "string") {
            continue;
        }
        let all_numeric = records
            .iter()
            .filter_map(|r| r.get(name))
            .all(|v| v.as_str().is_none_or(is_numeric_text));
        if !all_numeric {
            continue;
        }
        let mut names: Vec<String> = pn
            .into_iter()
            .map(|t| if t == "string" { "number".into() } else { t })
            .collect();
        if names.iter().any(|t| t == "number") {
            names.retain(|t| t != "integer");
        }
        names.sort();
        names.dedup();
        let mut ty: Vec<Value> = names.into_iter().map(Value::String).collect();
        if pnull {
            ty.push(Value::String("null".into()));
        }
        let ty = if ty.len() == 1 {
            ty.remove(0)
        } else {
            Value::Array(ty)
        };
        if let Some(obj) = fragment.as_object_mut() {
            obj.insert("type".into(), ty);
        }
    }
    inferred
}

/// Diff a page's inferred shape against the destination schema (top-level columns).
///
/// `destination` and `page` are both `infer_schema`-shaped object schemas
/// (`{"type":"object","properties":{...}}`). `allow_widening` gates whether a
/// lossless widening lands in `widenings` (true) or `incompatible` (false).
pub fn diff_schema(destination: &Value, page: &Value, allow_widening: bool) -> SchemaDiff {
    let empty = serde_json::Map::new();
    let dest_props = destination
        .get("properties")
        .and_then(|p| p.as_object())
        .unwrap_or(&empty);
    let page_props = page
        .get("properties")
        .and_then(|p| p.as_object())
        .unwrap_or(&empty);

    let mut diff = SchemaDiff::default();

    for (name, page_ty) in page_props {
        match dest_props.get(name) {
            None => diff.additions.push(ColumnChange {
                name: name.clone(),
                from: None,
                to: page_ty.clone(),
            }),
            Some(dest_ty) => {
                if fits(dest_ty, page_ty) {
                    continue; // page values already acceptable — no drift
                }
                let page_ty = &without_also_accepted(dest_ty, page_ty);
                let change = ColumnChange {
                    name: name.clone(),
                    from: Some(dest_ty.clone()),
                    to: page_ty.clone(),
                };
                if allow_widening && evolvable(dest_ty, page_ty) {
                    diff.widenings.push(change);
                } else {
                    diff.incompatible.push(change);
                }
            }
        }
    }

    // Destination columns absent from the page: drift only if NOT NULL.
    for (name, dest_ty) in dest_props {
        if !page_props.contains_key(name) {
            let (_, nullable) = type_set(dest_ty);
            if !nullable {
                diff.droppable_required.push(name.clone());
            }
        }
    }
    diff.additions.sort_by(|a, b| a.name.cmp(&b.name));
    diff.widenings.sort_by(|a, b| a.name.cmp(&b.name));
    diff.incompatible.sort_by(|a, b| a.name.cmp(&b.name));
    diff.droppable_required.sort();
    diff
}

/// What to do when drift is detected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "snake_case")]
pub enum OnDrift {
    /// Detect + emit a metric and a one-shot log; write the page unchanged.
    #[default]
    Warn,
    /// Apply additive/widening DDL to the destination, then write.
    Evolve,
    /// Drop unknown (non-destination) fields from every record; write the rest.
    Ignore,
    /// Route the records that exhibit the drift to the DLQ; write the rest.
    Quarantine,
    /// Raise `FaucetError::SchemaDrift` and abort.
    Fail,
}

/// `evolve`-only: what to do with a narrowing/incompatible change that can't be
/// auto-applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "snake_case")]
pub enum OnIncompatible {
    /// Abort the run (default).
    #[default]
    Fail,
    /// Route the offending records to the DLQ.
    Quarantine,
}

fn default_true() -> bool {
    true
}

/// User-facing `schema:` config block (pipeline level).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SchemaDriftSpec {
    /// Policy applied when drift is detected.
    #[serde(default)]
    pub on_drift: OnDrift,
    /// Whether a lossless widening counts as evolvable (vs incompatible).
    /// Only consulted by `evolve`. Default: true.
    #[serde(default = "default_true")]
    pub allow_type_widening: bool,
    /// `evolve` only: action for an incompatible residue. Default: fail.
    #[serde(default)]
    pub on_incompatible: OnIncompatible,
    /// `evolve` only: whether the **absence** of a destination `NOT NULL`
    /// column from a page may drop that column's `NOT NULL` constraint.
    ///
    /// Default `false`: a column merely omitted from one batch is *not*
    /// evidence the column is genuinely optional (a transient/partial page
    /// omits it just as readily as a real schema change), so auto-relaxing
    /// would silently and irreversibly weaken the destination's integrity
    /// (issue #194 / F28). When `false`, an omitted required column is left
    /// untouched — a page that genuinely lacks a required value then fails
    /// loudly at write time rather than degrading the schema. Set `true` only
    /// when you deliberately want missing-column omission to relax the
    /// constraint. Nullability relaxation driven by an *observed* null value
    /// in a present column (a widening that adds `null`) is unaffected by this
    /// flag — that is evidence-based, not omission-based.
    #[serde(default)]
    pub relax_nullability_on_missing: bool,
}

/// Compiled, ready-to-run drift policy. Cheap to clone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchemaDriftPolicy {
    pub on_drift: OnDrift,
    pub allow_widening: bool,
    pub on_incompatible: OnIncompatible,
    /// See [`SchemaDriftSpec::relax_nullability_on_missing`]. Default `false`.
    pub relax_nullability_on_missing: bool,
}

impl SchemaDriftPolicy {
    /// Compile a spec into a runnable policy. Infallible — there is nothing to
    /// validate that serde hasn't already (the DLQ requirement is enforced by
    /// the pipeline at run start and by the CLI at config-load).
    pub fn compile(spec: &SchemaDriftSpec) -> Self {
        Self {
            on_drift: spec.on_drift,
            allow_widening: spec.allow_type_widening,
            on_incompatible: spec.on_incompatible,
            relax_nullability_on_missing: spec.relax_nullability_on_missing,
        }
    }

    /// `true` when this policy can route records to a DLQ (so one must exist).
    pub fn requires_dlq(&self) -> bool {
        self.on_drift == OnDrift::Quarantine
            || (self.on_drift == OnDrift::Evolve
                && self.on_incompatible == OnIncompatible::Quarantine)
    }
}

/// Backend-neutral base column type inferred from a JSON-Schema fragment.
/// Each SQL sink maps these to its concrete keyword.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlBaseType {
    Integer,
    Double,
    Boolean,
    Text,
    /// Nested object / array → stored as JSON/JSONB/NVARCHAR(MAX) text.
    Json,
}

/// Map a top-level JSON-Schema type fragment to a base SQL type, or `None` for
/// a pure-null fragment (caller falls back to TEXT/NVARCHAR for an added column
/// whose only observed value was null).
pub fn json_schema_base_type(fragment: &Value) -> Option<SqlBaseType> {
    let (names, _nullable) = type_set(fragment);
    // Prefer the widest informative type among the union.
    if names.iter().any(|t| t == "object" || t == "array") {
        return Some(SqlBaseType::Json);
    }
    if names.iter().any(|t| t == "string") {
        return Some(SqlBaseType::Text);
    }
    if names.iter().any(|t| t == "number") {
        return Some(SqlBaseType::Double);
    }
    if names.iter().any(|t| t == "integer") {
        return Some(SqlBaseType::Integer);
    }
    if names.iter().any(|t| t == "boolean") {
        return Some(SqlBaseType::Boolean);
    }
    None
}

#[cfg(test)]
mod tests {
    #[test]
    fn numeric_text_matches_exact_number_renderings_only() {
        for ok in [
            "0",
            "-12",
            "12.50",
            "1e10",
            "-1.5E-3",
            "123456789012345678901234567890",
        ] {
            assert!(super::is_numeric_text(ok), "{ok}");
        }
        for bad in [
            "", "-", "1.", ".5", "1e", "abc", "NaN", "1,5", " 1", "+1", "1e+",
        ] {
            assert!(!super::is_numeric_text(bad), "{bad}");
        }
    }

    #[test]
    fn exact_decimal_strings_fit_a_numeric_column() {
        let dest = json!({"type": "object", "properties": {
            "amount": {"type": "number"},
            "n": {"type": ["number", "null"]},
            "label": {"type": "string"},
            "count": {"type": "integer"},
        }});
        let page = vec![
            json!({"amount": "12.50", "n": null, "label": "12", "count": "7"}),
            json!({"amount": 3, "n": "99999999999999999999", "label": "x", "count": 1}),
        ];
        let inferred = super::infer_page_schema(&dest, &page);
        assert_eq!(inferred["properties"]["amount"]["type"], json!("number"));
        assert_eq!(
            inferred["properties"]["n"]["type"],
            json!(["number", "null"])
        );
        assert_eq!(inferred["properties"]["label"]["type"], json!("string"));
        let diff = diff_schema(&dest, &inferred, true);
        assert!(
            diff.incompatible
                .iter()
                .all(|c| c.name != "amount" && c.name != "n"),
            "{diff:?}"
        );
        // An integer column is not a decimal target: text stays drift.
        assert!(
            diff.incompatible.iter().any(|c| c.name == "count"),
            "{diff:?}"
        );
    }

    #[test]
    fn non_numeric_text_into_a_numeric_column_is_still_drift() {
        let dest = json!({"type": "object", "properties": {"amount": {"type": "number"}}});
        let page = vec![json!({"amount": "12.50"}), json!({"amount": "n/a"})];
        let inferred = super::infer_page_schema(&dest, &page);
        let diff = diff_schema(&dest, &inferred, true);
        assert_eq!(diff.incompatible.len(), 1, "{diff:?}");
        // No destination properties: plain inference.
        assert_eq!(
            super::infer_page_schema(&json!({}), &page),
            crate::schema::infer_schema(&page)
        );
    }

    use super::*;

    #[test]
    fn value_fits_tests_one_value_against_the_destination_column() {
        let int = json!({"type": "integer"});
        assert!(value_fits(&int, "n", &json!(7)));
        assert!(!value_fits(&int, "n", &json!(1.5)));
        assert!(!value_fits(&int, "n", &json!("x")));
        assert!(
            !value_fits(&int, "n", &Value::Null),
            "a NOT NULL column refuses null"
        );
        let nullable = json!({"type": ["number", "null"]});
        assert!(value_fits(&nullable, "n", &json!(1.5)));
        assert!(value_fits(&nullable, "n", &json!(7)));
        assert!(value_fits(&nullable, "n", &Value::Null));
        // Exact numeric text lands in a number column (fidelity rendering).
        assert!(value_fits(&nullable, "n", &json!("12.50")));
    }
    use serde_json::json;

    fn schema(props: Value) -> Value {
        json!({ "type": "object", "properties": props })
    }

    #[test]
    fn also_accepts_lets_extra_types_fit_without_widening() {
        let dest = schema(json!({
            "flag": {"type": ["integer", "null"], DRIFT_ALSO_ACCEPTS: ["boolean"]},
            "doc": {"type": ["string", "null"], DRIFT_ALSO_ACCEPTS: ["object", "array"]},
        }));
        let page = schema(json!({
            "flag": {"type": "boolean"},
            "doc": {"type": ["object", "array"]},
        }));
        assert!(diff_schema(&dest, &page, true).is_empty());
        let page = schema(json!({"flag": {"type": "number"}}));
        let d = diff_schema(&dest, &page, true);
        assert_eq!(d.widenings.len(), 1);
        let page = schema(json!({"flag": {"type": ["boolean", "number"]}}));
        let d = diff_schema(&dest, &page, true);
        assert_eq!(d.widenings.len(), 1, "{d:?}");
        assert_eq!(d.widenings[0].to, json!({"type": "number"}));
        assert!(base_widened(
            d.widenings[0].from.as_ref().unwrap(),
            &d.widenings[0].to
        ));
        let strict = schema(json!({
            "flag": {"type": "integer", DRIFT_ALSO_ACCEPTS: ["boolean"]},
        }));
        let page = schema(json!({"flag": {"type": ["boolean", "null"]}}));
        let d = diff_schema(&strict, &page, true);
        assert_eq!(d.widenings[0].to, json!({"type": "null"}));
        assert!(adds_null(
            d.widenings[0].from.as_ref().unwrap(),
            &d.widenings[0].to
        ));
        let plain = schema(json!({"flag": {"type": ["integer", "null"]}}));
        let page = schema(json!({"flag": {"type": "boolean"}}));
        assert_eq!(diff_schema(&plain, &page, true).incompatible.len(), 1);
    }

    #[test]
    fn sql_type_mapping() {
        use super::SqlBaseType::*;
        assert_eq!(
            json_schema_base_type(&json!({"type":"integer"})),
            Some(Integer)
        );
        assert_eq!(
            json_schema_base_type(&json!({"type":"number"})),
            Some(Double)
        );
        assert_eq!(
            json_schema_base_type(&json!({"type":"boolean"})),
            Some(Boolean)
        );
        assert_eq!(json_schema_base_type(&json!({"type":"string"})), Some(Text));
        assert_eq!(
            json_schema_base_type(&json!({"type":["string","null"]})),
            Some(Text)
        );
        assert_eq!(json_schema_base_type(&json!({"type":"object"})), Some(Json));
        assert_eq!(json_schema_base_type(&json!({"type":"array"})), Some(Json));
        assert_eq!(json_schema_base_type(&json!({"type":"null"})), None);
    }

    #[test]
    fn no_drift_when_shapes_match() {
        let dest = schema(json!({ "id": {"type": "integer"}, "name": {"type": "string"} }));
        let page = schema(json!({ "id": {"type": "integer"}, "name": {"type": "string"} }));
        let d = diff_schema(&dest, &page, true);
        assert!(d.is_empty(), "got {d:?}");
    }

    #[test]
    fn detects_addition() {
        let dest = schema(json!({ "id": {"type": "integer"} }));
        let page = schema(json!({ "id": {"type": "integer"}, "email": {"type": "string"} }));
        let d = diff_schema(&dest, &page, true);
        assert_eq!(d.additions.len(), 1);
        assert_eq!(d.additions[0].name, "email");
        assert!(d.additions[0].from.is_none());
        assert_eq!(d.additions[0].to, json!({"type": "string"}));
        assert!(d.widenings.is_empty() && d.incompatible.is_empty());
    }

    #[test]
    fn integer_to_number_is_widening_when_allowed() {
        let dest = schema(json!({ "score": {"type": "integer"} }));
        let page = schema(json!({ "score": {"type": "number"} }));
        let d = diff_schema(&dest, &page, true);
        assert_eq!(d.widenings.len(), 1, "got {d:?}");
        assert_eq!(d.widenings[0].name, "score");
        assert!(d.incompatible.is_empty());
    }

    #[test]
    fn integer_to_number_is_incompatible_when_widening_disallowed() {
        let dest = schema(json!({ "score": {"type": "integer"} }));
        let page = schema(json!({ "score": {"type": "number"} }));
        let d = diff_schema(&dest, &page, false);
        assert_eq!(d.incompatible.len(), 1, "got {d:?}");
        assert!(d.widenings.is_empty());
    }

    #[test]
    fn gaining_nullability_is_widening() {
        let dest = schema(json!({ "name": {"type": "string"} }));
        let page = schema(json!({ "name": {"type": ["string", "null"]} }));
        let d = diff_schema(&dest, &page, true);
        assert_eq!(d.widenings.len(), 1, "got {d:?}");
    }

    #[test]
    fn string_to_integer_is_incompatible() {
        let dest = schema(json!({ "id": {"type": "string"} }));
        let page = schema(json!({ "id": {"type": "integer"} }));
        let d = diff_schema(&dest, &page, true);
        assert_eq!(d.incompatible.len(), 1, "got {d:?}");
        assert!(d.widenings.is_empty());
    }

    #[test]
    fn required_destination_column_absent_from_page_is_droppable_required() {
        // Destination has a non-nullable `created_at` the page never provides.
        let dest = schema(json!({
            "id": {"type": "integer"},
            "created_at": {"type": "string"}
        }));
        let page = schema(json!({ "id": {"type": "integer"} }));
        let d = diff_schema(&dest, &page, true);
        assert_eq!(
            d.droppable_required,
            vec!["created_at".to_string()],
            "got {d:?}"
        );
    }

    #[test]
    fn nullable_destination_column_absent_from_page_is_not_drift() {
        // A column the destination already allows to be null is fine to omit.
        let dest = schema(json!({
            "id": {"type": "integer"},
            "note": {"type": ["string", "null"]}
        }));
        let page = schema(json!({ "id": {"type": "integer"} }));
        let d = diff_schema(&dest, &page, true);
        assert!(d.is_empty(), "got {d:?}");
    }

    #[test]
    fn nested_object_treated_as_single_column() {
        // A change *inside* a nested object is invisible — top-level only.
        let dest =
            schema(json!({ "meta": {"type": "object", "properties": {"a": {"type": "integer"}}} }));
        let page = schema(
            json!({ "meta": {"type": "object", "properties": {"a": {"type": "integer"}, "b": {"type": "string"}}} }),
        );
        let d = diff_schema(&dest, &page, true);
        assert!(
            d.is_empty(),
            "nested changes must not surface as drift; got {d:?}"
        );
    }

    /// Which bucket a single-column diff landed in.
    #[derive(Debug, PartialEq)]
    enum Bucket {
        None,
        Widening,
        Incompatible,
    }

    /// Diff a single column `col` (D vs P) and report which bucket it landed in.
    fn classify_one(dest_ty: Value, page_ty: Value, allow_widening: bool) -> Bucket {
        let dest = schema(json!({ "col": dest_ty }));
        let page = schema(json!({ "col": page_ty }));
        let d = diff_schema(&dest, &page, allow_widening);
        assert!(d.additions.is_empty(), "unexpected addition: {d:?}");
        assert!(
            d.droppable_required.is_empty(),
            "unexpected droppable: {d:?}"
        );
        match (d.widenings.len(), d.incompatible.len()) {
            (0, 0) => Bucket::None,
            (1, 0) => Bucket::Widening,
            (0, 1) => Bucket::Incompatible,
            _ => panic!("ambiguous classification: {d:?}"),
        }
    }

    #[test]
    fn truth_table_allow_widening() {
        use Bucket::*;
        // (dest, page, expected bucket) — allow_widening = true.
        let cases: &[(Value, Value, Bucket)] = &[
            (json!({"type": "integer"}), json!({"type": "integer"}), None),
            (json!({"type": "string"}), json!({"type": "string"}), None),
            (
                json!({"type": ["string", "null"]}),
                json!({"type": ["string", "null"]}),
                None,
            ),
            // Regression guard: non-null page fits a nullable dest.
            (
                json!({"type": ["string", "null"]}),
                json!({"type": "string"}),
                None,
            ),
            // Regression guard: integer fits a number column.
            (json!({"type": "number"}), json!({"type": "integer"}), None),
            // integer → number widening.
            (
                json!({"type": "integer"}),
                json!({"type": "number"}),
                Widening,
            ),
            // string → nullable string (relax null).
            (
                json!({"type": "string"}),
                json!({"type": ["string", "null"]}),
                Widening,
            ),
            // integer → nullable number (int→number + null relax).
            (
                json!({"type": "integer"}),
                json!({"type": ["number", "null"]}),
                Widening,
            ),
            // nullable integer dest, number page → int→number, dest already nullable.
            (
                json!({"type": ["integer", "null"]}),
                json!({"type": "number"}),
                Widening,
            ),
            // Genuine incompatibilities.
            (
                json!({"type": "string"}),
                json!({"type": "integer"}),
                Incompatible,
            ),
            (
                json!({"type": "integer"}),
                json!({"type": "string"}),
                Incompatible,
            ),
            (
                json!({"type": "boolean"}),
                json!({"type": "number"}),
                Incompatible,
            ),
        ];
        for (dest, page, want) in cases {
            let got = classify_one(dest.clone(), page.clone(), true);
            assert_eq!(
                &got, want,
                "allow_widening=true: D={dest} P={page} expected {want:?} got {got:?}"
            );
        }
    }

    #[test]
    fn truth_table_widening_disallowed() {
        use Bucket::*;
        // (dest, page, expected bucket) — allow_widening = false.
        let cases: &[(Value, Value, Bucket)] = &[
            // int→number is no longer a widening; with widening off it is incompatible.
            (
                json!({"type": "integer"}),
                json!({"type": "number"}),
                Incompatible,
            ),
            // `fits` still applies regardless of allow_widening.
            (
                json!({"type": ["string", "null"]}),
                json!({"type": "string"}),
                None,
            ),
            // null relaxation is a widening, not a fit → incompatible when disallowed.
            (
                json!({"type": "string"}),
                json!({"type": ["string", "null"]}),
                Incompatible,
            ),
        ];
        for (dest, page, want) in cases {
            let got = classify_one(dest.clone(), page.clone(), false);
            assert_eq!(
                &got, want,
                "allow_widening=false: D={dest} P={page} expected {want:?} got {got:?}"
            );
        }
    }

    #[test]
    fn base_widened_detects_base_type_change() {
        // integer → number is a base-type widening (needs ALTER TYPE).
        assert!(base_widened(
            &json!({"type": "integer"}),
            &json!({"type": "number"})
        ));
        // nullability-only relaxation is NOT a base-type widening.
        assert!(!base_widened(
            &json!({"type": "string"}),
            &json!({"type": ["string", "null"]})
        ));
        // identical base type, no change.
        assert!(!base_widened(
            &json!({"type": "integer"}),
            &json!({"type": "integer"})
        ));
        // integer dest, nullable number page → base family changed.
        assert!(base_widened(
            &json!({"type": "integer"}),
            &json!({"type": ["number", "null"]})
        ));
        // number dest, integer page → integer collapses into number, no change.
        assert!(!base_widened(
            &json!({"type": "number"}),
            &json!({"type": "integer"})
        ));
    }

    #[test]
    fn adds_null_detects_nullability_relaxation() {
        assert!(adds_null(
            &json!({"type": "string"}),
            &json!({"type": ["string", "null"]})
        ));
        // already nullable destination → not adding null.
        assert!(!adds_null(
            &json!({"type": ["string", "null"]}),
            &json!({"type": "string"})
        ));
        assert!(!adds_null(
            &json!({"type": "string"}),
            &json!({"type": "string"})
        ));
        // page nullable, dest not → adds null even with a base change.
        assert!(adds_null(
            &json!({"type": "integer"}),
            &json!({"type": ["number", "null"]})
        ));
    }

    #[test]
    fn spec_defaults() {
        let spec: SchemaDriftSpec = serde_json::from_str("{}").unwrap();
        assert_eq!(spec.on_drift, OnDrift::Warn);
        assert!(spec.allow_type_widening);
        assert_eq!(spec.on_incompatible, OnIncompatible::Fail);
    }

    #[test]
    fn on_drift_serializes_snake_case() {
        assert_eq!(
            serde_json::to_string(&OnDrift::Evolve).unwrap(),
            "\"evolve\""
        );
        assert_eq!(
            serde_json::to_string(&OnDrift::Quarantine).unwrap(),
            "\"quarantine\""
        );
    }

    #[test]
    fn policy_compile_carries_flags() {
        let spec: SchemaDriftSpec =
            serde_json::from_str(r#"{"on_drift":"evolve","allow_type_widening":false}"#).unwrap();
        let policy = SchemaDriftPolicy::compile(&spec);
        assert_eq!(policy.on_drift, OnDrift::Evolve);
        assert!(!policy.allow_widening);
        assert_eq!(policy.on_incompatible, OnIncompatible::Fail);
    }

    #[test]
    fn policy_requires_dlq_only_for_quarantine_paths() {
        let q: SchemaDriftSpec = serde_json::from_str(r#"{"on_drift":"quarantine"}"#).unwrap();
        assert!(SchemaDriftPolicy::compile(&q).requires_dlq());
        let evo_q: SchemaDriftSpec =
            serde_json::from_str(r#"{"on_drift":"evolve","on_incompatible":"quarantine"}"#)
                .unwrap();
        assert!(SchemaDriftPolicy::compile(&evo_q).requires_dlq());
        let warn: SchemaDriftSpec = serde_json::from_str(r#"{"on_drift":"warn"}"#).unwrap();
        assert!(!SchemaDriftPolicy::compile(&warn).requires_dlq());
    }
}
