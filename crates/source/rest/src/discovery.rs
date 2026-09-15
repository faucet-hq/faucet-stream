//! Generic, config-driven dataset discovery for the REST source (#647).
//!
//! **Vendor-neutral.** A `discovery:` block declares *which API calls to make and
//! how to extract datasets from their JSON responses*, reusing the source's
//! `auth` / `base_url` / `headers`. The engine understands only: request →
//! JSONPath-extract an array → (optional) per-item request → extract fields +
//! types → interpolate templates → emit a [`DatasetDescriptor`]. It has **no**
//! knowledge of any specific API — the request paths, query strings, and raw
//! type names are all plain values the user writes in config.
//!
//! The I/O (running the requests through the source's authenticated client) lives
//! in `stream.rs`; everything here is pure and unit-tested.

use faucet_core::FaucetError;
use faucet_core::discover::{DatasetDescriptor, columns_to_schema, nullable_type};
use faucet_core::util::{extract_records, snake_case};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::HashMap;

/// A generic discovery recipe on a REST source.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DiscoverySpec {
    /// Step 1 — enumerate datasets from a listing endpoint. Optional: omit it and
    /// supply [`objects`](Self::objects) directly (e.g. from a run param).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub list: Option<DiscoveryList>,
    /// Datasets supplied directly — a YAML list **or** a comma-separated string
    /// (`"A,B,C"`), so a single string run-param can drive it
    /// (`objects: "${param.objects}"`). Used instead of `list` when non-empty.
    #[serde(
        default,
        deserialize_with = "crate::config::de_objects",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub objects: Vec<String>,
    /// Step 2 — per-dataset field discovery (builds a typed schema + a field list
    /// to template into `emit`). Optional: omit when the dataset needs no field
    /// enumeration (e.g. an API that returns every column by default).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub describe: Option<DiscoveryDescribe>,
    /// Step 3 — what each discovered dataset emits (the source `config_patch`
    /// that selects it, and an optional sink `table_id`). Templated.
    pub emit: DiscoveryEmit,
    /// Fan out **at run time**: `faucet run` / `serve` turn the discovered
    /// datasets into one matrix row each, before `expand`. Default `false`
    /// (the block then only powers `faucet discover`).
    #[serde(default)]
    pub fan_out: bool,
    /// Sink template each fanned-out dataset routes to (an entry under
    /// `pipeline.sinks`). Unset ⇒ the default (singular) sink, with `table_id`
    /// merged in.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sink_ref: Option<String>,
}

/// Step 1 — the listing request + how to read dataset names out of its response.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DiscoveryList {
    /// Request path (relative to the source `base_url`); `GET`.
    pub get: String,
    /// JSONPath selecting the array of listing items (e.g. `$.objects[*]`).
    pub items: String,
    /// JSONPath, relative to each item, yielding its name (e.g. `$.name`).
    pub name: String,
    /// Optional keep-if predicate over each item (e.g. `queryable == true`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keep_if: Option<DiscoveryPredicate>,
    /// Drop any dataset whose name ends with one of these suffixes (e.g.
    /// `[ChangeEvent, Feed, Share, History]`). Vendor-neutral name filtering.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub exclude_name_suffixes: Vec<String>,
}

/// A keep-if predicate: keep a listing item when the value at `path` equals
/// `equals`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DiscoveryPredicate {
    /// JSONPath relative to the item.
    pub path: String,
    /// Keep the item iff the extracted value equals this.
    pub equals: Value,
}

/// Step 2 — the per-dataset describe request + how to read fields + types.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DiscoveryDescribe {
    /// Request path template (relative to `base_url`), with `${name}` for the
    /// dataset (e.g. `/catalog/objects/${name}/fields`); `GET`.
    pub get: String,
    /// JSONPath selecting the array of field objects (e.g. `$.fields[*]`).
    pub fields: String,
    /// JSONPath, relative to each field, yielding its name.
    pub field_name: String,
    /// JSONPath, relative to each field, yielding its raw type string. Absent ⇒
    /// no typed schema is built (fields are still listed for templating).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field_type: Option<String>,
    /// JSONPath, relative to each field, yielding a truthy "is nullable" flag.
    /// Absent ⇒ every column is nullable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field_nullable: Option<String>,
    /// Raw-type → JSON-Schema-type map (`integer` / `number` / `boolean` /
    /// `string` / `object` / `array`). Key `"*"` is the fallback; absent ⇒
    /// `string`.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub type_map: HashMap<String, String>,
    /// Drop fields whose raw type is one of these (e.g. compound / blob types
    /// not selectable in a query).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub skip_types: Vec<String>,
}

/// Step 3 — per-dataset output, templated with `${name}`, `${name_snake}`, and
/// `${field_names}` (the comma-joined selectable field list).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DiscoveryEmit {
    /// A JSON object deep-merged into the dataset's source config (e.g. the
    /// `async_job` query, or an `odata.entity`). Every string leaf is templated.
    pub config: Value,
    /// Optional sink `table_id` template (e.g. `${name_snake}`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table_id: Option<String>,
}

impl DiscoverySpec {
    /// Fail-fast validation (paths non-empty, an `emit.config` object, at least
    /// one of `list` / `objects`).
    pub fn validate(&self) -> Result<(), FaucetError> {
        if self.list.is_none() && self.objects.is_empty() {
            return Err(FaucetError::Config(
                "rest discovery: set `list:` (to enumerate datasets) or `objects:` \
                 (an explicit list), or both"
                    .into(),
            ));
        }
        if let Some(l) = &self.list {
            for (f, v) in [("get", &l.get), ("items", &l.items), ("name", &l.name)] {
                if v.trim().is_empty() {
                    return Err(FaucetError::Config(format!(
                        "rest discovery: `list.{f}` must not be empty"
                    )));
                }
            }
        }
        if let Some(d) = &self.describe {
            for (f, v) in [
                ("get", &d.get),
                ("fields", &d.fields),
                ("field_name", &d.field_name),
            ] {
                if v.trim().is_empty() {
                    return Err(FaucetError::Config(format!(
                        "rest discovery: `describe.{f}` must not be empty"
                    )));
                }
            }
        }
        if !self.emit.config.is_object() {
            return Err(FaucetError::Config(
                "rest discovery: `emit.config` must be a JSON object".into(),
            ));
        }
        Ok(())
    }
}

/// Extract a single scalar at `path` relative to `value`, as a `Value`.
fn extract_one(value: &Value, path: &str) -> Option<Value> {
    extract_records(value, Some(path))
        .ok()
        .and_then(|v| v.into_iter().next())
}

/// Extract a single scalar at `path` as a string (JSON strings unquoted; other
/// scalars stringified).
fn extract_str(value: &Value, path: &str) -> Option<String> {
    extract_one(value, path).map(|v| match v {
        Value::String(s) => s,
        other => other.to_string(),
    })
}

/// Truthiness for a nullable flag: `true` / non-zero / non-empty-string.
fn is_truthy(v: &Value) -> bool {
    match v {
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().map(|f| f != 0.0).unwrap_or(false),
        Value::String(s) => !s.is_empty() && !s.eq_ignore_ascii_case("false"),
        Value::Null => false,
        _ => true,
    }
}

/// Map a raw type string to a JSON-Schema type via `type_map` (`"*"` fallback,
/// then `string`).
fn map_type(raw: Option<&str>, type_map: &HashMap<String, String>) -> String {
    raw.and_then(|r| type_map.get(r))
        .or_else(|| type_map.get("*"))
        .cloned()
        .unwrap_or_else(|| "string".to_string())
}

/// Filter + name a listing response's items into dataset names (keep_if, then
/// name-suffix exclusion), sorted for determinism.
pub fn dataset_names(list_response: &Value, list: &DiscoveryList) -> Vec<String> {
    let items = extract_records(list_response, Some(&list.items)).unwrap_or_default();
    let mut names: Vec<String> = items
        .iter()
        .filter(|item| match &list.keep_if {
            Some(p) => extract_one(item, &p.path).as_ref() == Some(&p.equals),
            None => true,
        })
        .filter_map(|item| extract_str(item, &list.name))
        .filter(|name| {
            !list
                .exclude_name_suffixes
                .iter()
                .any(|suf| name.ends_with(suf.as_str()))
        })
        .collect();
    names.sort();
    names.dedup();
    names
}

/// Render `${name}`, `${name_lower}`, `${name_snake}`, and `${field_names}` in a
/// template string. `${name_lower}` is a plain lowercase (underscores preserved),
/// distinct from `${name_snake}` (word-split snake_case).
fn render_template(tmpl: &str, name: &str, field_names: &[String]) -> String {
    tmpl.replace("${name_lower}", &name.to_lowercase())
        .replace("${name_snake}", &snake_case(name))
        .replace("${name}", name)
        .replace("${field_names}", &field_names.join(", "))
}

/// Recursively template every string leaf of a JSON value.
fn render_value(v: &Value, name: &str, field_names: &[String]) -> Value {
    match v {
        Value::String(s) => Value::String(render_template(s, name, field_names)),
        Value::Array(a) => Value::Array(
            a.iter()
                .map(|x| render_value(x, name, field_names))
                .collect(),
        ),
        Value::Object(m) => Value::Object(
            m.iter()
                .map(|(k, val)| (k.clone(), render_value(val, name, field_names)))
                .collect(),
        ),
        other => other.clone(),
    }
}

/// Build one descriptor for a dataset. `describe_response` is the per-dataset
/// describe body (when a `describe:` step ran); `None` when there is no describe
/// step. Returns `None` when a describe step ran but yielded no usable fields
/// (nothing to query) — mirroring "skip empty object".
pub fn build_descriptor(
    name: &str,
    describe_response: Option<&Value>,
    spec: &DiscoverySpec,
) -> Option<DatasetDescriptor> {
    let mut field_names: Vec<String> = Vec::new();
    let mut cols: Vec<(String, Value)> = Vec::new();

    if let (Some(desc), Some(resp)) = (&spec.describe, describe_response) {
        let fields = extract_records(resp, Some(&desc.fields)).unwrap_or_default();
        for f in &fields {
            let Some(fname) = extract_str(f, &desc.field_name) else {
                continue;
            };
            let raw_type = desc.field_type.as_ref().and_then(|p| extract_str(f, p));
            if let Some(rt) = &raw_type
                && desc.skip_types.iter().any(|s| s == rt)
            {
                continue;
            }
            field_names.push(fname.clone());
            if desc.field_type.is_some() {
                let json_ty = map_type(raw_type.as_deref(), &desc.type_map);
                let frag = json!({ "type": json_ty });
                let nullable = desc
                    .field_nullable
                    .as_ref()
                    .and_then(|p| extract_one(f, p))
                    .map(|v| is_truthy(&v))
                    .unwrap_or(true);
                let frag = if nullable { nullable_type(frag) } else { frag };
                cols.push((fname, frag));
            }
        }
        // A describe step that found nothing selectable ⇒ skip this dataset.
        if field_names.is_empty() {
            return None;
        }
    }

    let config_patch = render_value(&spec.emit.config, name, &field_names);
    let mut d = DatasetDescriptor::new(name.to_string(), "dataset", config_patch);
    if !cols.is_empty() {
        d = d.with_schema(columns_to_schema(cols));
    }
    if let Some(tid) = &spec.emit.table_id {
        d = d.with_sink_patch(json!({ "table_id": render_template(tid, name, &field_names) }));
    }
    Some(d)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn recipe() -> DiscoverySpec {
        // A describe-style discovery recipe (list endpoint → per-dataset describe
        // → a templated query + typed schema), expressed purely in config. The
        // endpoints/types are arbitrary strings the engine treats as opaque.
        serde_json::from_value(json!({
            "list": {
                "get": "/catalog/objects",
                "items": "$.objects[*]",
                "name": "$.name",
                "keep_if": { "path": "$.queryable", "equals": true },
                "exclude_name_suffixes": ["ChangeEvent", "Feed", "Share", "History", "Tag"]
            },
            "describe": {
                "get": "/catalog/objects/${name}/fields",
                "fields": "$.fields[*]",
                "field_name": "$.name",
                "field_type": "$.type",
                "field_nullable": "$.nillable",
                "type_map": { "int": "integer", "double": "number", "currency": "number", "boolean": "boolean", "*": "string" },
                "skip_types": ["address", "location", "base64"]
            },
            "emit": {
                "config": { "query": { "operation": "queryAll", "sql": "SELECT ${field_names} FROM ${name}" } },
                "table_id": "${name_snake}"
            },
            "fan_out": true
        }))
        .unwrap()
    }

    #[test]
    fn dataset_names_filters_by_predicate_and_suffix_and_sorts() {
        let spec = recipe();
        let list = spec.list.as_ref().unwrap();
        let resp = json!({ "objects": [
            { "name": "Opportunity", "queryable": true },
            { "name": "Account", "queryable": true },
            { "name": "AccountFeed", "queryable": true },     // excluded suffix
            { "name": "Secret", "queryable": false },          // predicate drops
            { "name": "AccountHistory", "queryable": true },   // excluded suffix
        ]});
        assert_eq!(dataset_names(&resp, list), vec!["Account", "Opportunity"]);
    }

    #[test]
    fn build_descriptor_builds_query_schema_and_table_id() {
        let spec = recipe();
        let describe = json!({ "fields": [
            { "name": "Id", "type": "id", "nillable": false },
            { "name": "Name", "type": "string", "nillable": true },
            { "name": "AnnualRevenue", "type": "currency", "nillable": true },
            { "name": "IsDeleted", "type": "boolean", "nillable": false },
            { "name": "BillingAddress", "type": "address", "nillable": true }, // skipped
        ]});
        let d = build_descriptor("Account", Some(&describe), &spec).unwrap();
        assert_eq!(d.name, "Account");
        // Query built from the surviving fields, in order (${field_names} join).
        let q = d.config_patch["query"]["sql"].as_str().unwrap();
        assert_eq!(q, "SELECT Id, Name, AnnualRevenue, IsDeleted FROM Account");
        assert_eq!(d.config_patch["query"]["operation"], "queryAll");
        // Typed schema + nullability.
        let s = d.schema.as_ref().unwrap();
        assert_eq!(s["properties"]["Id"]["type"], "string"); // non-null → bare
        assert_eq!(s["properties"]["Name"]["type"][1], "null"); // nillable → [T,null]
        assert_eq!(s["properties"]["AnnualRevenue"]["type"][0], "number");
        assert_eq!(s["properties"]["IsDeleted"]["type"], "boolean");
        assert!(s["properties"].get("BillingAddress").is_none()); // compound dropped
        // Sink routing.
        assert_eq!(d.sink_patch, Some(json!({ "table_id": "account" })));
    }

    #[test]
    fn build_descriptor_skips_dataset_with_no_selectable_fields() {
        let spec = recipe();
        let describe = json!({ "fields": [ { "name": "Loc", "type": "location" } ] });
        assert!(build_descriptor("Weird", Some(&describe), &spec).is_none());
    }

    #[test]
    fn acronym_table_id_uses_shared_snake() {
        // ${name_snake} must match the keys_case transform (isocode, not i_s_o_code).
        let spec = recipe();
        let describe = json!({ "fields": [ { "name": "Id", "type": "id", "nillable": false } ] });
        let d = build_descriptor("ISOCode", Some(&describe), &spec).unwrap();
        assert_eq!(d.sink_patch, Some(json!({ "table_id": "isocode" })));
    }

    #[test]
    fn validate_requires_list_or_objects_and_object_emit() {
        let mut spec = recipe();
        spec.validate().unwrap();
        spec.list = None;
        spec.objects.clear();
        assert!(spec.validate().is_err());
    }
}
