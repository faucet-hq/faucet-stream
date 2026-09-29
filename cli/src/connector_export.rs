//! Connector Hub export (#572): one machine-readable document describing every
//! connector in the registry index — identity, install coordinates, derived
//! capabilities, conformance score and tier, config schema, and a `faucet
//! init`-style YAML snippet — for offline generators such as the website hub.
//!
//! Every capability is derived from the same registry functions the capability
//! matrix and conformance scoring use, or from the connector's config schema;
//! nothing is copied by hand.

use crate::conformance::{Report, Tier, facts_for, score};
use crate::init_template::schema_to_yaml_template;
use crate::registry;
use crate::registry_index::{ConnectorEntry, RegistryIndex};
use serde::Serialize;
use serde_json::Value;

/// Wire identifier of the export document.
pub const EXPORT_FORMAT: &str = "faucet-connector-export";
/// Shape version; bumped only on a breaking change to the document.
pub const EXPORT_VERSION: u32 = 1;

/// The closed set of Connector Hub categories, in display order.
pub const CATEGORIES: &[(&str, &str)] = &[
    ("databases", "Databases"),
    ("cdc", "Change data capture"),
    ("warehouses", "Warehouses"),
    ("streaming", "Streaming & queues"),
    ("files", "Files & object stores"),
    ("apis", "APIs & web"),
    ("bridges", "Bridges & output"),
];

/// The whole export document.
#[derive(Debug, Clone, Serialize)]
pub struct ConnectorExport {
    pub format: &'static str,
    pub version: u32,
    pub faucet_version: &'static str,
    pub categories: Vec<Category>,
    pub tiers: Vec<TierInfo>,
    pub connectors: Vec<ExportedConnector>,
}

/// One category id and its label.
#[derive(Debug, Clone, Serialize)]
pub struct Category {
    pub id: &'static str,
    pub label: &'static str,
}

/// One maturity tier: id, label, minimum score and badge URL.
#[derive(Debug, Clone, Serialize)]
pub struct TierInfo {
    pub id: &'static str,
    pub label: &'static str,
    pub min_score: u32,
    pub badge_url: String,
}

/// One connector.
#[derive(Debug, Clone, Serialize)]
pub struct ExportedConnector {
    /// `<kind>-<name>`, unique across the document.
    pub id: String,
    pub name: String,
    pub kind: String,
    pub title: String,
    pub category: Option<String>,
    pub description: String,
    pub verified: bool,
    #[serde(rename = "crate")]
    pub krate: String,
    pub feature: String,
    pub keywords: Vec<String>,
    /// Workspace path of the crate (`crates/<kind>/<name>`) for verified built-ins.
    pub repository_path: Option<String>,
    pub docs_url: String,
    pub crates_io_url: String,
    /// Whether this binary compiled the connector in (schema and init snippet
    /// are only available when it did).
    pub compiled: bool,
    pub conformance: Report,
    pub capabilities: Capabilities,
    pub config_fields: Vec<ConfigField>,
    pub config_schema: Option<Value>,
    pub init_snippet: Option<String>,
}

/// Capabilities derived from the registry allowlists and the config schema.
#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub struct Capabilities {
    /// Source: `deterministic` / `non_deterministic` replay. Sink:
    /// `atomic_watermark` / `keyed_upsert` / `at_least_once`.
    pub delivery: String,
    pub exactly_once: bool,
    /// Config exposes a `compression` setting.
    pub compression: bool,
    // Source-only.
    pub discover: bool,
    pub incremental: bool,
    pub reports_lag: bool,
    // Sink-only.
    pub write_modes: Vec<String>,
    pub upsert: bool,
    pub overwrite: bool,
    pub cleanup: bool,
    pub schema_evolution: bool,
    pub staged_load: bool,
    pub rollback: bool,
}

/// One top-level config field.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ConfigField {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: String,
    pub required: bool,
    pub default: Option<Value>,
    pub description: String,
}

fn schema_has(schema: Option<&Value>, prop: &str) -> bool {
    schema
        .and_then(|s| s.get("properties"))
        .and_then(Value::as_object)
        .is_some_and(|p| p.contains_key(prop))
}

fn wire<T: Serialize>(v: T) -> String {
    serde_json::to_value(v)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_default()
}

/// Derive a connector's capabilities. Pure over the registry allowlists and
/// the (optional) config schema.
pub fn capabilities(kind: &str, is_source: bool, schema: Option<&Value>) -> Capabilities {
    let compression = schema_has(schema, "compression");
    if is_source {
        return Capabilities {
            delivery: wire(registry::source_replay_guarantee(kind)),
            exactly_once: registry::source_supports_exactly_once(kind),
            compression,
            discover: registry::source_supports_discover(kind),
            incremental: schema_has(schema, "replication_method"),
            reports_lag: registry::source_reports_lag(kind),
            ..Capabilities::default()
        };
    }
    let modes = registry::sink_supported_write_modes(kind);
    Capabilities {
        delivery: wire(registry::sink_guarantee(kind)),
        exactly_once: registry::sink_supports_idempotent_writes(kind),
        compression,
        write_modes: modes.iter().map(|m| m.as_str().to_string()).collect(),
        upsert: modes
            .iter()
            .any(|m| matches!(m, faucet_core::WriteMode::Upsert)),
        overwrite: registry::sink_supports_overwrite(kind),
        cleanup: registry::sink_supports_cleanup(kind),
        schema_evolution: registry::sink_supports_schema_evolution(kind),
        staged_load: registry::sink_supports_staged_load(kind),
        rollback: crate::rollback::ROLLBACK_SINK_KINDS.contains(&kind),
        ..Capabilities::default()
    }
}

fn resolve_ref<'a>(schema: &'a Value, node: &'a Value) -> &'a Value {
    node.get("$ref")
        .and_then(Value::as_str)
        .and_then(|r| r.strip_prefix("#/$defs/"))
        .and_then(|name| schema.get("$defs").and_then(|d| d.get(name)))
        .unwrap_or(node)
}

fn type_label(schema: &Value, node: &Value) -> String {
    let node = resolve_ref(schema, node);
    if let Some(t) = node.get("type") {
        return match t {
            Value::String(s) => s.clone(),
            Value::Array(a) => a
                .iter()
                .filter_map(Value::as_str)
                .filter(|s| *s != "null")
                .collect::<Vec<_>>()
                .join(" | "),
            _ => "any".into(),
        };
    }
    for key in ["anyOf", "oneOf"] {
        if let Some(Value::Array(alts)) = node.get(key) {
            let mut labels: Vec<String> = alts
                .iter()
                .filter(|a| a.get("type").and_then(Value::as_str) != Some("null"))
                .map(|a| {
                    if a.get("properties").is_some() || a.get("const").is_some() {
                        "object".into()
                    } else {
                        type_label(schema, a)
                    }
                })
                .collect();
            labels.dedup();
            if !labels.is_empty() {
                return labels.join(" | ");
            }
        }
    }
    if node.get("enum").is_some() {
        return "string".into();
    }
    if node.get("properties").is_some() {
        return "object".into();
    }
    "any".into()
}

/// The top-level fields of a config schema, required ones first, each group
/// in schema order.
pub fn config_fields(schema: &Value) -> Vec<ConfigField> {
    let required: Vec<&str> = schema
        .get("required")
        .and_then(Value::as_array)
        .map(|a| a.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();
    let Some(props) = schema.get("properties").and_then(Value::as_object) else {
        return Vec::new();
    };
    let mut fields: Vec<ConfigField> = props
        .iter()
        .map(|(name, node)| ConfigField {
            name: name.clone(),
            ty: type_label(schema, node),
            required: required.contains(&name.as_str()),
            default: node.get("default").cloned(),
            description: node
                .get("description")
                .or_else(|| resolve_ref(schema, node).get("description"))
                .and_then(Value::as_str)
                .unwrap_or_default()
                .trim()
                .to_string(),
        })
        .collect();
    fields.sort_by_key(|f| !f.required);
    fields
}

/// A `faucet init`-style YAML block for one connector.
pub fn init_snippet(kind: &str, is_source: bool, schema: &Value) -> String {
    let role = if is_source { "source" } else { "sink" };
    format!(
        "{role}:\n  type: {kind}\n  config:\n{}",
        schema_to_yaml_template(schema, 4)
    )
}

fn repository_path(e: &ConnectorEntry) -> Option<String> {
    (e.verified && e.crate_name() == format!("faucet-{}-{}", e.kind, e.name))
        .then(|| format!("crates/{}/{}", e.kind, e.name))
}

/// Export one registry entry.
pub fn export_entry(e: &ConnectorEntry, index: &RegistryIndex) -> ExportedConnector {
    let is_source = e.kind == "source";
    let compiled = if is_source {
        registry::source_exists(&e.name)
    } else {
        registry::sink_exists(&e.name)
    };
    let schema = compiled
        .then(|| {
            if is_source {
                registry::source_schema(&e.name)
            } else {
                registry::sink_schema(&e.name)
            }
            .ok()
        })
        .flatten();
    let krate = e.crate_name();
    ExportedConnector {
        id: format!("{}-{}", e.kind, e.name),
        name: e.name.clone(),
        kind: e.kind.clone(),
        title: e.title.clone().unwrap_or_else(|| e.name.clone()),
        category: e.category.clone(),
        description: e.description.clone(),
        verified: e.verified,
        docs_url: format!("https://docs.rs/{krate}"),
        crates_io_url: format!("https://crates.io/crates/{krate}"),
        repository_path: repository_path(e),
        feature: e.feature_flag(),
        keywords: e.keywords.clone(),
        krate,
        compiled,
        conformance: score(&facts_for(&e.name, is_source, index)),
        capabilities: capabilities(&e.name, is_source, schema.as_ref()),
        config_fields: schema.as_ref().map(config_fields).unwrap_or_default(),
        init_snippet: schema.as_ref().map(|s| init_snippet(&e.name, is_source, s)),
        config_schema: schema,
    }
}

/// Build the export over `index`, sorted by kind then name.
pub fn build_export(index: &RegistryIndex) -> ConnectorExport {
    let mut connectors: Vec<ExportedConnector> = index
        .connectors
        .iter()
        .filter(|e| e.kind == "source" || e.kind == "sink")
        .map(|e| export_entry(e, index))
        .collect();
    connectors.sort_by(|a, b| (&a.kind, &a.name).cmp(&(&b.kind, &b.name)));
    let tiers = [
        (Tier::Stable, 70),
        (Tier::Experimental, 45),
        (Tier::Beta, 20),
        (Tier::Draft, 0),
    ]
    .into_iter()
    .map(|(t, min_score)| TierInfo {
        id: t.as_str(),
        label: t.label(),
        min_score,
        badge_url: t.badge_url(),
    })
    .collect();
    ConnectorExport {
        format: EXPORT_FORMAT,
        version: EXPORT_VERSION,
        faucet_version: env!("CARGO_PKG_VERSION"),
        categories: CATEGORIES
            .iter()
            .map(|(id, label)| Category { id, label })
            .collect(),
        tiers,
        connectors,
    }
}

/// Connectors in the export this binary did not compile in (`--require-all`).
pub fn missing(export: &ConnectorExport) -> Vec<String> {
    export
        .connectors
        .iter()
        .filter(|c| !c.compiled)
        .map(|c| c.id.clone())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn every_registry_entry_is_exported_once() {
        let index = RegistryIndex::embedded();
        let export = build_export(&index);
        assert_eq!(export.format, EXPORT_FORMAT);
        assert_eq!(export.version, EXPORT_VERSION);
        assert_eq!(export.connectors.len(), index.connectors.len());
        let mut ids: Vec<&str> = export.connectors.iter().map(|c| c.id.as_str()).collect();
        ids.dedup();
        assert_eq!(ids.len(), index.connectors.len());
    }

    #[test]
    fn verified_entries_have_known_category_title_and_path() {
        let ids: Vec<&str> = CATEGORIES.iter().map(|(id, _)| *id).collect();
        for c in build_export(&RegistryIndex::embedded()).connectors {
            let cat = c.category.as_deref().unwrap_or_default();
            assert!(ids.contains(&cat), "{} has category {cat:?}", c.id);
            assert_ne!(c.title, "", "{}", c.id);
            let path = c.repository_path.clone().expect("verified path");
            let manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("..")
                .join(&path)
                .join("Cargo.toml");
            let text = std::fs::read_to_string(&manifest).expect("crate manifest");
            assert!(text.contains(&format!("name = \"{}\"", c.krate)), "{path}");
        }
    }

    #[test]
    fn compiled_connectors_carry_schema_fields_and_snippet() {
        let export = build_export(&RegistryIndex::embedded());
        let pg = export
            .connectors
            .iter()
            .find(|c| c.id == "sink-postgres")
            .expect("postgres sink");
        assert!(pg.compiled);
        assert!(pg.config_schema.is_some());
        assert!(!pg.config_fields.is_empty());
        let snip = pg.init_snippet.as_deref().unwrap();
        assert!(snip.starts_with("sink:\n  type: postgres\n  config:\n"));
        assert!(pg.capabilities.upsert && pg.capabilities.exactly_once);
        assert_eq!(pg.capabilities.delivery, "atomic_watermark");
        assert!(pg.capabilities.write_modes.contains(&"upsert".to_string()));
        assert_eq!(pg.conformance.tier, Tier::Stable);
        assert_eq!(export.tiers[0].id, "stable");
    }

    #[test]
    fn uncompiled_entry_is_metadata_only() {
        let index: RegistryIndex = serde_json::from_value(json!({
            "connectors": [{"name": "acme", "kind": "source", "verified": false}]
        }))
        .unwrap();
        let export = build_export(&index);
        let c = &export.connectors[0];
        assert!(!c.compiled);
        assert!(c.config_schema.is_none() && c.init_snippet.is_none());
        assert!(c.repository_path.is_none());
        assert_eq!(c.krate, "faucet-source-acme");
        assert_eq!(c.title, "acme");
        assert_eq!(missing(&export), vec!["source-acme".to_string()]);
    }

    #[test]
    fn source_capabilities_come_from_registry_and_schema() {
        let schema = json!({"properties": {"compression": {}, "replication_method": {}}});
        let c = capabilities("postgres-cdc", true, Some(&schema));
        assert!(c.exactly_once && c.compression && c.incremental && c.reports_lag);
        assert_eq!(c.delivery, "deterministic");
        let plain = capabilities("postgres", true, None);
        assert!(plain.discover && !plain.exactly_once && !plain.compression);
        assert_eq!(plain.delivery, "non_deterministic");
        assert!(plain.write_modes.is_empty());
    }

    #[test]
    fn config_fields_resolve_types_required_and_defaults() {
        let schema = json!({
            "required": ["url"],
            "properties": {
                "opt": {"type": ["integer", "null"], "default": 5, "description": " n "},
                "url": {"type": "string"},
                "auth": {"$ref": "#/$defs/Auth"},
                "mode": {"enum": ["a", "b"]},
                "nested": {"properties": {}},
                "alt": {"anyOf": [{"type": "string"}, {"type": "null"}]},
                "tagged": {"oneOf": [{"const": "x"}, {"properties": {}}]},
                "odd": {"type": 3},
                "none": {}
            },
            "$defs": {"Auth": {"type": "object", "description": "creds"}}
        });
        let fields = config_fields(&schema);
        assert_eq!(fields[0].name, "url");
        assert!(fields[0].required);
        let get = |n: &str| fields.iter().find(|f| f.name == n).unwrap().clone();
        assert_eq!(get("opt").ty, "integer");
        assert_eq!(get("opt").default, Some(json!(5)));
        assert_eq!(get("opt").description, "n");
        assert_eq!(get("auth").ty, "object");
        assert_eq!(get("auth").description, "creds");
        assert_eq!(get("mode").ty, "string");
        assert_eq!(get("nested").ty, "object");
        assert_eq!(get("alt").ty, "string");
        assert_eq!(get("tagged").ty, "object");
        assert_eq!(get("odd").ty, "any");
        assert_eq!(get("none").ty, "any");
        assert!(config_fields(&json!({})).is_empty());
    }
}
