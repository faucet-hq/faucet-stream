//! Compose the top-level JSON Schema for a whole `faucet.yaml` config (#213).
//!
//! `faucet schema source <name>` already emits per-connector schemas, but there
//! was no single schema for the *entire* config document. This module builds
//! one by taking the derived [`PipelineConfig`](crate::config::PipelineConfig)
//! schema (which covers `version` / `name` / `pipeline` / `matrix` /
//! `execution` / `auth` / `vars` / `params` / `schedule` / `lineage` / `quality` / `dlq` /
//! … — every block whose type derives `JsonSchema`) and layering per-connector
//! discrimination on top: the `source` / `sink` positions become a `oneOf` over
//! the compiled-in connector kinds, each branch pinning `type: <kind>` and
//! embedding that connector's own config schema.
//!
//! Editors (the VS Code YAML extension, JetBrains) consume the emitted schema
//! for autocomplete, inline docs, and as-you-type validation via a
//! `# yaml-language-server: $schema=…` header.
//!
//! **Interpolation tolerance.** faucet configs pervasively use `${env:…}` /
//! `${vars:…}` / `${now.*}` placeholders — a string standing in for a typed
//! value. So every embedded connector-config subtree is *relaxed*: `required`
//! is dropped, `additionalProperties` is opened, and every declared scalar type
//! also accepts a `string`. This keeps property/description autocomplete while
//! never rejecting a valid-but-interpolated config. The strict top-level grammar
//! (unknown-key rejection, `version`, block shapes) is preserved.

use crate::registry::{sink_kinds, sink_schema, source_kinds, source_schema};
use serde_json::{Map, Value, json};

/// Build the composed top-level config schema.
pub fn config_schema() -> Value {
    let mut root = serde_json::to_value(faucet_core::schema_for!(crate::config::PipelineConfig))
        .unwrap_or_else(|_| json!({"type": "object"}));

    let root_obj = match root.as_object_mut() {
        Some(o) => o,
        None => return root,
    };
    root_obj.insert(
        "title".into(),
        json!("faucet pipeline configuration (faucet.yaml / faucet.json)"),
    );

    // Accumulate namespaced connector-config `$defs` here, then merge once.
    let mut extra_defs: Map<String, Value> = Map::new();
    let source_union = connector_union(
        "source",
        &source_kinds(),
        source_schema,
        true,
        &mut extra_defs,
    );
    let sink_union = connector_union("sink", &sink_kinds(), sink_schema, false, &mut extra_defs);

    let defs_key = if root_obj.contains_key("definitions") && !root_obj.contains_key("$defs") {
        "definitions"
    } else {
        "$defs"
    };
    let defs = root_obj
        .entry(defs_key.to_string())
        .or_insert_with(|| json!({}))
        .as_object_mut()
        .expect("$defs is an object");
    for (k, v) in extra_defs {
        defs.insert(k, v);
    }
    defs.insert("SourceConnector".into(), source_union);
    defs.insert("SinkConnector".into(), sink_union);

    // Point the source/sink positions of PipelineSpec at the discriminated
    // unions. When the derived schema uses a different `$defs` key we mirror it.
    let ref_prefix = format!("#/{defs_key}/");
    if let Some(pipeline_spec) = defs.get_mut("PipelineSpec").and_then(Value::as_object_mut)
        && let Some(props) = pipeline_spec
            .get_mut("properties")
            .and_then(Value::as_object_mut)
    {
        retarget_property(
            props,
            "source",
            &format!("{ref_prefix}SourceConnector"),
            false,
        );
        retarget_property(props, "sink", &format!("{ref_prefix}SinkConnector"), false);
        retarget_property(
            props,
            "sources",
            &format!("{ref_prefix}SourceConnector"),
            true,
        );
        retarget_property(props, "sinks", &format!("{ref_prefix}SinkConnector"), true);
    }

    root
}

/// Replace a `PipelineSpec` property that references `ConnectorSpec` with one
/// that references the discriminated union. `map_valued` handles the
/// `sources` / `sinks` maps (`additionalProperties`), otherwise the singular
/// nullable `source` / `sink`.
fn retarget_property(
    props: &mut Map<String, Value>,
    key: &str,
    target_ref: &str,
    map_valued: bool,
) {
    if !props.contains_key(key) {
        return;
    }
    let new = if map_valued {
        json!({
            "type": ["object", "null"],
            "additionalProperties": { "$ref": target_ref },
        })
    } else {
        json!({ "anyOf": [ { "$ref": target_ref }, { "type": "null" } ] })
    };
    props.insert(key.to_string(), new);
}

/// Build a `oneOf` connector schema discriminated by `type`, embedding each
/// kind's (relaxed) config schema. `source_side` connectors additionally allow
/// `transforms` / `inherit_transforms` at the connector level.
fn connector_union(
    ns: &str,
    kinds: &[&str],
    schema_fn: fn(&str) -> crate::error::CliResult<Value>,
    source_side: bool,
    extra_defs: &mut Map<String, Value>,
) -> Value {
    let mut variants = Vec::new();
    for &kind in kinds {
        // The connector's typed config schema powers editor autocomplete, but
        // it is *advisory*, not hard-rejecting: faucet threads the raw `config`
        // Value straight through to the connector's own `Deserialize`, and a
        // connector's serde shape can differ from its schemars projection (e.g.
        // an enum accepting both a bare string and a `{type: …}` object). So we
        // wrap it as `anyOf: [<typed>, true]` — editors still surface the typed
        // fields, but a config the connector accepts is never flagged invalid.
        let typed = match schema_fn(kind) {
            Ok(s) => embed_config(s, &format!("{ns}_{kind}"), extra_defs),
            Err(_) => json!(true),
        };
        let config = json!({ "anyOf": [typed, true] });
        let mut props = Map::new();
        props.insert("type".into(), json!({ "const": kind }));
        props.insert("config".into(), config);
        if source_side {
            props.insert("transforms".into(), json!({ "type": ["array", "null"] }));
            props.insert("inherit_transforms".into(), json!({ "type": "boolean" }));
        }
        // Destination attributes a data-flow policy reasons about (#702);
        // accepted on either side by `ConnectorSpec`, meaningful on sinks.
        props.insert(
            "attributes".into(),
            json!({ "type": "object", "additionalProperties": { "type": "string" } }),
        );
        variants.push(json!({
            "type": "object",
            "title": kind,
            "properties": props,
            "required": ["type"],
            "additionalProperties": false,
        }));
    }
    json!({ "oneOf": variants })
}

/// Take a connector's standalone config schema, lift its internal `$defs` into
/// the shared map under a namespace (rewriting refs so they don't collide),
/// strip the schema metadata, and relax it for interpolation tolerance.
fn embed_config(mut schema: Value, ns: &str, extra_defs: &mut Map<String, Value>) -> Value {
    // Pull out and namespace the connector's own definitions.
    if let Some(obj) = schema.as_object_mut() {
        for key in ["$defs", "definitions"] {
            if let Some(Value::Object(inner)) = obj.remove(key) {
                for (name, mut def) in inner {
                    let ns_name = format!("{ns}__{name}");
                    rewrite_refs(&mut def, ns);
                    relax_for_interpolation(&mut def);
                    extra_defs.insert(ns_name, def);
                }
            }
        }
        obj.remove("$schema");
        obj.remove("$id");
    }
    rewrite_refs(&mut schema, ns);
    relax_for_interpolation(&mut schema);
    schema
}

/// Rewrite every `#/$defs/X` / `#/definitions/X` ref to `#/$defs/{ns}__X` so a
/// connector's definitions can live alongside every other connector's in one
/// shared `$defs` map without name collisions.
fn rewrite_refs(value: &mut Value, ns: &str) {
    match value {
        Value::Object(map) => {
            if let Some(Value::String(r)) = map.get_mut("$ref") {
                for prefix in ["#/$defs/", "#/definitions/"] {
                    if let Some(name) = r.strip_prefix(prefix) {
                        *r = format!("#/$defs/{ns}__{name}");
                        break;
                    }
                }
            }
            for v in map.values_mut() {
                rewrite_refs(v, ns);
            }
        }
        Value::Array(arr) => {
            for v in arr {
                rewrite_refs(v, ns);
            }
        }
        _ => {}
    }
}

/// Relax a connector-config schema so interpolated configs still validate:
/// drop `required`, open `additionalProperties`, and let every declared scalar
/// type also be a `string` (a `${…}` placeholder). Recurses through
/// `properties`, `items`, `additionalProperties`, and `oneOf`/`anyOf`/`allOf`.
fn relax_for_interpolation(value: &mut Value) {
    let Value::Object(map) = value else {
        if let Value::Array(arr) = value {
            for v in arr {
                relax_for_interpolation(v);
            }
        }
        return;
    };

    map.remove("required");

    // A `$ref` node's target is relaxed where it is defined; don't touch it.
    if !map.contains_key("$ref") {
        // Allow a string wherever a typed scalar is expected.
        if let Some(t) = map.get_mut("type") {
            *t = allow_string(std::mem::take(t));
        }
        // Open objects so extra (interpolated / future) keys don't fail.
        if map
            .get("additionalProperties")
            .map(|v| v == &json!(false))
            .unwrap_or(false)
        {
            map.insert("additionalProperties".into(), json!(true));
        }
    }

    for (k, v) in map.iter_mut() {
        // `enum`/`const` values are data, not schemas — leave them intact.
        if k == "enum" || k == "const" {
            continue;
        }
        relax_for_interpolation(v);
    }
}

/// Broaden an interpolatable scalar `type` to also accept a `string` (a
/// `${…}` placeholder). Only `integer` / `number` / `boolean` are broadened:
/// an `${env:…}` substitution replaces a scalar, never an object/array, and
/// broadening an object/array type would let a bare-string `oneOf` unit variant
/// (e.g. `column_mapping: auto_map`) also match its object sibling — an
/// `oneOf` ambiguity that fails validation.
fn allow_string(t: Value) -> Value {
    match &t {
        Value::String(s) if matches!(s.as_str(), "integer" | "number" | "boolean") => {
            json!([s, "string"])
        }
        // Arrays like `["integer","null"]` → add "string" only when a numeric /
        // boolean type is present (never for object/array/string unions).
        Value::Array(arr)
            if arr
                .iter()
                .any(|v| matches!(v.as_str(), Some("integer" | "number" | "boolean")))
                && !arr.iter().any(|v| v == &json!("string")) =>
        {
            let mut arr = arr.clone();
            arr.push(json!("string"));
            Value::Array(arr)
        }
        _ => t,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn composed_schema_has_top_level_grammar() {
        let s = config_schema();
        assert_eq!(s["type"], "object");
        let props = &s["properties"];
        for key in [
            "version",
            "name",
            "pipeline",
            "matrix",
            "execution",
            "auth",
            "vars",
            "params",
        ] {
            assert!(
                props.get(key).is_some(),
                "missing top-level property `{key}`"
            );
        }
    }

    #[cfg(all(feature = "source-csv", feature = "sink-jsonl"))]
    #[test]
    fn source_and_sink_unions_discriminate_by_kind() {
        let s = config_schema();
        let defs = s.get("$defs").or_else(|| s.get("definitions")).unwrap();
        let source_union = &defs["SourceConnector"]["oneOf"];
        let has_csv = source_union
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v["properties"]["type"]["const"] == json!("csv"));
        assert!(has_csv, "source union should have a csv branch");

        let sink_union = &defs["SinkConnector"]["oneOf"];
        let has_jsonl = sink_union
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v["properties"]["type"]["const"] == json!("jsonl"));
        assert!(has_jsonl, "sink union should have a jsonl branch");
    }

    #[test]
    fn allow_string_broadens_only_interpolatable_scalars() {
        assert_eq!(allow_string(json!("integer")), json!(["integer", "string"]));
        assert_eq!(allow_string(json!("number")), json!(["number", "string"]));
        assert_eq!(allow_string(json!("boolean")), json!(["boolean", "string"]));
        // Left alone: strings, objects, arrays, and null (broadening these
        // would create `oneOf` ambiguity).
        assert_eq!(allow_string(json!("string")), json!("string"));
        assert_eq!(allow_string(json!("object")), json!("object"));
        assert_eq!(allow_string(json!("array")), json!("array"));
        assert_eq!(
            allow_string(json!(["integer", "null"])),
            json!(["integer", "null", "string"])
        );
        // An object/string union (an enum unit variant + its object sibling)
        // must NOT gain another string.
        assert_eq!(
            allow_string(json!(["object", "null"])),
            json!(["object", "null"])
        );
    }

    #[test]
    fn relax_drops_required_and_opens_objects() {
        let mut v = json!({
            "type": "object",
            "required": ["a"],
            "additionalProperties": false,
            "properties": { "a": { "type": "integer" } }
        });
        relax_for_interpolation(&mut v);
        assert!(v.get("required").is_none());
        assert_eq!(v["additionalProperties"], json!(true));
        assert_eq!(v["properties"]["a"]["type"], json!(["integer", "string"]));
    }

    #[test]
    fn rewrite_refs_namespaces_defs() {
        let mut v = json!({ "$ref": "#/$defs/Auth" });
        rewrite_refs(&mut v, "source_rest");
        assert_eq!(v["$ref"], json!("#/$defs/source_rest__Auth"));
    }
}
