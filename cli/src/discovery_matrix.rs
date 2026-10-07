//! Pure planning logic for the discovery-driven request matrix (#501).
//!
//! A `discover:` row enumerates a value-set at runtime (build source → drain →
//! project `select` → dedup); a `for_each: [dims]` row fans out over the
//! **cartesian product** of those value-sets, one invocation per tuple.
//!
//! Everything here is pure (no I/O): projection, dedup, the cartesian product,
//! the per-tuple interpolation context, and the tuple state-key suffix — so the
//! fan-out semantics are unit-testable without a network. The only I/O (running
//! the discovery source) lives in the executor and feeds [`Dim::values`].

use serde_json::{Map, Value};
use std::collections::HashMap;

/// Hard ceiling on the number of invocations a single `for_each` row may expand
/// to. A cartesian product multiplies quickly; above this the run fails rather
/// than silently spawning an unbounded fleet.
pub const MAX_MATRIX_PRODUCT: usize = 10_000;

/// One resolved discovery dimension: the discovery row id, the alias its
/// projected value is exposed under, and the deduped value-set.
#[derive(Debug, Clone, PartialEq)]
pub struct Dim {
    /// Discovery row id (the `${<id>.<alias>}` reference target).
    pub id: String,
    /// Alias the projected value is exposed under.
    pub alias: String,
    /// Projected, deduped values (first-seen order).
    pub values: Vec<Value>,
}

/// A **collected (list-valued) dimension** (#531): a chained `discover:` row
/// (`for_each: [...]` + `collect: true`) that publishes the *whole* deduped
/// value-set as one list per upstream tuple, keyed by that tuple. A consuming
/// row injects the list into one request (`${<id>.<alias>}` → comma-joined),
/// rather than fanning out one invocation per element.
#[derive(Debug, Clone, PartialEq)]
pub struct CollectedDim {
    /// Discovery row id (the `${<id>.<alias>}` reference target).
    pub id: String,
    /// Alias the collected list is exposed under.
    pub alias: String,
    /// The upstream discovery dimension ids this row fanned out over, in order;
    /// used to compute the tuple key both when storing and when injecting.
    pub dims: Vec<String>,
    /// The collected list per upstream tuple, keyed by [`collected_tuple_key`].
    pub by_tuple: HashMap<String, Vec<Value>>,
}

/// A canonical, symmetric key for one upstream tuple over `dims`, derived from a
/// per-tuple interpolation context (`{dim_id: {alias: value}}`). Both the
/// storing side (the chained discovery, over its own `for_each` dims) and the
/// reading side (a consuming `for_each` row, whose dims are a superset) compute
/// the same key for the same tuple, so a collected list looks up correctly. Pure.
pub fn collected_tuple_key(dims: &[String], ctx: &HashMap<String, Value>) -> String {
    dims.iter()
        .map(|d| {
            let v = ctx.get(d).cloned().unwrap_or(Value::Null);
            format!("{d}={}", serde_json::to_string(&v).unwrap_or_default())
        })
        .collect::<Vec<_>>()
        .join("&")
}

/// Enrich a product tuple `ctx` in place with every collected dimension whose
/// list is available for this tuple, so `${<id>.<alias>}` resolves to the list.
/// A collected dim with no entry for the tuple injects an empty list (the
/// consuming request then renders an empty param — the "type has no properties"
/// fallback). Pure.
pub fn inject_collected(ctx: &mut HashMap<String, Value>, collected: &[CollectedDim]) {
    for cd in collected {
        let key = collected_tuple_key(&cd.dims, ctx);
        let list = cd.by_tuple.get(&key).cloned().unwrap_or_default();
        let mut obj = Map::new();
        obj.insert(cd.alias.clone(), Value::Array(list));
        ctx.insert(cd.id.clone(), Value::Object(obj));
    }
}

/// Project `select` (a dot-path, optionally `$`-prefixed) from each record and
/// dedup the results in first-seen order. `null` / missing projections are
/// skipped. `$` (or `""`) selects the whole record. Pure.
pub fn project_dedup(records: &[Value], select: &str) -> Vec<Value> {
    let mut seen: Vec<Value> = Vec::new();
    for rec in records {
        let Some(v) = project_value(rec, select) else {
            continue;
        };
        if v.is_null() {
            continue;
        }
        if !seen.contains(&v) {
            seen.push(v);
        }
    }
    seen
}

/// Resolve a dot-path projection against one record. Supports a leading `$.` or
/// bare `$` (whole record), then `a.b.c` segment walking into objects. Returns
/// `None` if any segment is missing. Pure.
fn project_value(record: &Value, select: &str) -> Option<Value> {
    let path = select
        .strip_prefix("$.")
        .or_else(|| select.strip_prefix('$'))
        .unwrap_or(select);
    if path.is_empty() {
        return Some(record.clone());
    }
    let mut cur = record;
    for seg in path.split('.') {
        cur = cur.get(seg)?;
    }
    Some(cur.clone())
}

/// The number of invocations `dims` expands to (the product of the value-set
/// sizes). Returns `0` if any dimension is empty. Saturating, so it never
/// overflows; compare against [`MAX_MATRIX_PRODUCT`]. Pure.
pub fn product_size(dims: &[Dim]) -> usize {
    if dims.is_empty() {
        return 0;
    }
    dims.iter()
        .map(|d| d.values.len())
        .try_fold(1usize, |acc, n| acc.checked_mul(n))
        .unwrap_or(usize::MAX)
}

/// Build the cartesian product of the dimensions as per-tuple interpolation
/// contexts. Each context maps `dim_id -> { alias: value }`, so a token
/// `${dim_id.alias}` resolves to that tuple's value via the same
/// `interpolate_record` path the parent/child matrix uses. An empty dimension
/// yields no tuples. Pure.
pub fn cartesian(dims: &[Dim]) -> Vec<HashMap<String, Value>> {
    if dims.is_empty() || dims.iter().any(|d| d.values.is_empty()) {
        return Vec::new();
    }
    let mut out: Vec<HashMap<String, Value>> = vec![HashMap::new()];
    for dim in dims {
        let mut next = Vec::with_capacity(out.len() * dim.values.len());
        for base in &out {
            for v in &dim.values {
                let mut ctx = base.clone();
                let mut obj = Map::new();
                obj.insert(dim.alias.clone(), v.clone());
                ctx.insert(dim.id.clone(), Value::Object(obj));
                next.push(ctx);
            }
        }
        out = next;
    }
    out
}

/// Longest raw value kept verbatim in a state-key segment.
const STATE_KEY_COMPONENT_MAX: usize = 64;

/// Render one fan-out value as a state-key segment (#789 CLI-52/CLI-53). A
/// value made only of state-key characters is kept as is, so existing keys do
/// not move; anything else (an email, a space, non-ASCII, `::`, an over-long
/// value) becomes a readable prefix plus a stable 64-bit hash of the raw value.
/// `allow_separators` admits `:` and `/` (a child's parent key, which stands
/// alone after `::`); a product tuple keeps them for its own separators.
pub fn state_key_component(raw: &str, allow_separators: bool) -> String {
    let plain = |c: char| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.');
    let legal = |c: char| plain(c) || (allow_separators && matches!(c, ':' | '/'));
    if !raw.is_empty()
        && raw.len() <= STATE_KEY_COMPONENT_MAX
        && raw.chars().all(legal)
        && !raw.contains("::")
        && !raw.starts_with(':')
        && !raw.ends_with(':')
    {
        return raw.to_owned();
    }
    let readable: String = raw.chars().filter(|c| plain(*c)).take(32).collect();
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for b in raw.as_bytes() {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{readable}.h{hash:016x}")
}

/// Stable, collision-resistant state-key suffix for one product tuple, in
/// declared dimension order: `alias:value/alias:value…`, each part a legal
/// state-key segment ([`state_key_component`]). `dims` supplies the order +
/// aliases; `ctx` is one entry from [`cartesian`]. Pure.
pub fn tuple_state_key_suffix(dims: &[Dim], ctx: &HashMap<String, Value>) -> String {
    dims.iter()
        .map(|d| {
            let v = ctx
                .get(&d.id)
                .and_then(|o| o.get(&d.alias))
                .map(value_brief)
                .unwrap_or_else(|| "(missing)".to_string());
            format!(
                "{}:{}",
                state_key_component(&d.alias, false),
                state_key_component(&v, false)
            )
        })
        .collect::<Vec<_>>()
        .join("/")
}

/// Brief scalar rendering of a JSON value for a state-key segment.
fn value_brief(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => "null".to_string(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn project_dedup_dotpath_and_dollar() {
        let recs = vec![
            json!({"id": 1, "name": "a"}),
            json!({"id": 2, "name": "b"}),
            json!({"id": 1, "name": "c"}), // dup id
        ];
        assert_eq!(project_dedup(&recs, "$.id"), vec![json!(1), json!(2)]);
        assert_eq!(project_dedup(&recs, "id"), vec![json!(1), json!(2)]);
        // `$` selects whole records (all distinct here).
        assert_eq!(project_dedup(&recs, "$").len(), 3);
    }

    #[test]
    fn project_dedup_skips_null_and_missing() {
        let recs = vec![
            json!({"id": 1}),
            json!({"id": null}),
            json!({"other": 9}), // missing `id`
            json!({"id": 2}),
        ];
        assert_eq!(project_dedup(&recs, "$.id"), vec![json!(1), json!(2)]);
    }

    #[test]
    fn project_nested_path() {
        let recs = vec![
            json!({"meta": {"code": "X"}}),
            json!({"meta": {"code": "Y"}}),
        ];
        assert_eq!(
            project_dedup(&recs, "$.meta.code"),
            vec![json!("X"), json!("Y")]
        );
    }

    fn dim(id: &str, alias: &str, vals: Vec<Value>) -> Dim {
        Dim {
            id: id.into(),
            alias: alias.into(),
            values: vals,
        }
    }

    #[test]
    fn cartesian_two_dims_is_product() {
        let dims = vec![
            dim("subs", "subsidiary_id", vec![json!(1), json!(2)]),
            dim(
                "fields",
                "field_id",
                vec![json!("a"), json!("b"), json!("c")],
            ),
        ];
        let ctxs = cartesian(&dims);
        assert_eq!(ctxs.len(), 6);
        // First tuple pairs the first value of each dim.
        assert_eq!(ctxs[0]["subs"]["subsidiary_id"], json!(1));
        assert_eq!(ctxs[0]["fields"]["field_id"], json!("a"));
        // Product covers every combination.
        let pairs: Vec<(Value, Value)> = ctxs
            .iter()
            .map(|c| {
                (
                    c["subs"]["subsidiary_id"].clone(),
                    c["fields"]["field_id"].clone(),
                )
            })
            .collect();
        assert!(pairs.contains(&(json!(2), json!("c"))));
    }

    #[test]
    fn cartesian_single_dim() {
        let dims = vec![dim("d", "v", vec![json!(1), json!(2)])];
        let ctxs = cartesian(&dims);
        assert_eq!(ctxs.len(), 2);
        assert_eq!(ctxs[1]["d"]["v"], json!(2));
    }

    #[test]
    fn cartesian_empty_dim_yields_nothing() {
        let dims = vec![dim("a", "x", vec![json!(1)]), dim("b", "y", vec![])];
        assert!(cartesian(&dims).is_empty());
        assert!(cartesian(&[]).is_empty());
    }

    #[test]
    fn product_size_math() {
        assert_eq!(product_size(&[]), 0);
        assert_eq!(
            product_size(&[
                dim("a", "x", vec![json!(1), json!(2)]),
                dim("b", "y", vec![json!(1)])
            ]),
            2
        );
        assert_eq!(
            product_size(&[dim("a", "x", vec![]), dim("b", "y", vec![json!(1)])]),
            0
        );
    }

    #[test]
    fn collected_tuple_key_symmetric_between_store_and_read() {
        // The chained discovery stores over its own dims; the consuming row reads
        // over its (superset) dims. For the shared upstream tuple the key matches.
        let dims = vec!["types".to_string()];
        let store_ctx: HashMap<String, Value> =
            [("types".to_string(), json!({"name": "deal"}))].into();
        // Consuming row's ctx also has an extra axis, but the key uses only `dims`.
        let read_ctx: HashMap<String, Value> = [
            ("types".to_string(), json!({"name": "deal"})),
            ("other".to_string(), json!({"x": 1})),
        ]
        .into();
        assert_eq!(
            collected_tuple_key(&dims, &store_ctx),
            collected_tuple_key(&dims, &read_ctx)
        );
    }

    #[test]
    fn inject_collected_puts_the_list_into_ctx() {
        let cd = CollectedDim {
            id: "props".into(),
            alias: "name".into(),
            dims: vec!["types".into()],
            by_tuple: [(
                collected_tuple_key(
                    &["types".into()],
                    &[("types".to_string(), json!({"name": "deal"}))].into(),
                ),
                vec![json!("amount"), json!("stage")],
            )]
            .into(),
        };
        let mut ctx: HashMap<String, Value> =
            [("types".to_string(), json!({"name": "deal"}))].into();
        inject_collected(&mut ctx, std::slice::from_ref(&cd));
        assert_eq!(ctx["props"]["name"], json!(["amount", "stage"]));

        // A tuple with no collected entry injects an empty list.
        let mut miss: HashMap<String, Value> =
            [("types".to_string(), json!({"name": "ticket"}))].into();
        inject_collected(&mut miss, &[cd]);
        assert_eq!(miss["props"]["name"], json!([]));
    }

    #[test]
    fn tuple_state_key_is_stable_and_ordered() {
        let dims = vec![
            dim("subs", "subsidiary_id", vec![json!(1)]),
            dim("fields", "field_id", vec![json!("a")]),
        ];
        let ctxs = cartesian(&dims);
        let suffix = tuple_state_key_suffix(&dims, &ctxs[0]);
        assert_eq!(suffix, "subsidiary_id:1/field_id:a");
        faucet_core::state::validate_state_key(&format!("p::row::{suffix}")).unwrap();
    }

    #[test]
    fn tuple_state_key_encodes_values_outside_the_key_charset() {
        let dims = vec![dim(
            "users",
            "email",
            vec![
                json!("a@x.com"),
                json!("b@x.com"),
                json!("a b"),
                json!("x::y"),
            ],
        )];
        let ctxs = cartesian(&dims);
        let keys: Vec<String> = ctxs
            .iter()
            .map(|c| format!("p::row::{}", tuple_state_key_suffix(&dims, c)))
            .collect();
        for k in &keys {
            faucet_core::state::validate_state_key(k).unwrap();
        }
        let distinct: std::collections::HashSet<_> = keys.iter().collect();
        assert_eq!(distinct.len(), keys.len(), "{keys:?}");
        assert!(keys[0].starts_with("p::row::email:ax.com.h"), "{}", keys[0]);
        assert_eq!(
            keys,
            ctxs.iter()
                .map(|c| format!("p::row::{}", tuple_state_key_suffix(&dims, c)))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn state_key_component_keeps_legal_values_and_hashes_the_rest() {
        assert_eq!(state_key_component("cust-42_a.b", false), "cust-42_a.b");
        assert_eq!(state_key_component("a/b:c", true), "a/b:c");
        assert_ne!(state_key_component("a/b:c", false), "a/b:c");
        for raw in ["", "x::y", ":x", "y:", "émoji", &"z".repeat(65)] {
            let c = state_key_component(raw, true);
            assert!(c.contains(".h"), "{raw:?} → {c}");
            faucet_core::state::validate_state_key(&format!("p::r::{c}")).unwrap();
        }
        assert_ne!(
            state_key_component("a b", true),
            state_key_component("a  b", true)
        );
    }
}
