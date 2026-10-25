//! `${tenant.*}` tokens (#709): a template triggered for a tenant can route
//! each tenant to its own destination — `${tenant.id}`, `${tenant.name}` and
//! `${tenant.labels.<key>}`. `faucet serve` binds them over the parsed
//! document before the config is loaded; every other runtime refuses a
//! leftover token rather than handing the literal text to a connector.

use crate::error::{CliError, CliResult};
use serde_json::Value;
use std::collections::BTreeMap;

/// The reserved interpolation id.
pub const TENANT_ID: &str = "tenant";

const PREFIX: &str = "${tenant.";

/// What a `${tenant.*}` token can read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantValues {
    pub id: String,
    pub name: Option<String>,
    pub labels: BTreeMap<String, String>,
}

impl TenantValues {
    fn lookup(&self, path: &str) -> Result<String, String> {
        match path {
            "id" => Ok(self.id.clone()),
            "name" => Ok(self.name.clone().unwrap_or_else(|| self.id.clone())),
            _ => match path.strip_prefix("labels.") {
                Some(key) if !key.is_empty() => self.labels.get(key).cloned().ok_or_else(|| {
                    format!(
                        "tenant '{}' has no label '{key}' (referenced as `${{tenant.{path}}}`)",
                        self.id
                    )
                }),
                _ => Err(format!(
                    "unknown tenant token `${{tenant.{path}}}` — use `${{tenant.id}}`, \
                     `${{tenant.name}}` or `${{tenant.labels.<key>}}`"
                )),
            },
        }
    }
}

/// Where a token sits: SQL text and table-like names take only a tenant value
/// that is a plain identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Field {
    Plain,
    Sql,
    Identifier,
}

/// Substitute every `${tenant.*}` token in `s`. A substituted value is
/// escaped (`${` → `$${`) so no later load pass reads it as a directive.
fn bind_str(s: &str, tenant: &TenantValues, field: Field) -> Result<String, String> {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(start) = crate::interpolate::find_unescaped(rest, PREFIX) {
        out.push_str(&rest[..start]);
        let after = &rest[start + PREFIX.len()..];
        let Some(end) = after.find('}') else {
            return Err(format!("unterminated `${{tenant.` token in '{s}'"));
        };
        let path = &after[..end];
        let value = tenant.lookup(path)?;
        if field != Field::Plain && !crate::sql_bind::is_safe_identifier(&value) {
            let place = if field == Field::Sql {
                "a SQL query"
            } else {
                "a table, collection or index name"
            };
            return Err(format!(
                "`${{tenant.{path}}}` in {place} resolves to a value of tenant '{}' that is not \
                 a plain identifier (letters, digits, `_`, `.`, `-`, `$`); only such values are \
                 placed in SQL text or names",
                tenant.id
            ));
        }
        out.push_str(&value.replace("${", "$${"));
        rest = &after[end + 1..];
    }
    out.push_str(rest);
    Ok(out)
}

/// Bind every `${tenant.*}` token in a parsed document. With `tenant: None`,
/// any token is an error naming why: the run was not started for a tenant.
pub fn bind_document(doc: &mut Value, tenant: Option<&TenantValues>) -> Result<(), String> {
    let kinds = TemplateKinds::of(doc);
    bind_in(doc, tenant, Field::Plain, &kinds, None)
}

/// Whether an object is a source or a sink connector spec.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Role {
    Source,
    Sink,
}

/// The connector kind of each named source / sink template, so a matrix row
/// or topology node that picks one by `ref` (and names no `type`) has its
/// fields classified by the template's kind.
#[derive(Debug, Default)]
struct TemplateKinds {
    sources: BTreeMap<String, String>,
    sinks: BTreeMap<String, String>,
}

impl TemplateKinds {
    fn of(doc: &Value) -> Self {
        let pipeline = doc.get("pipeline");
        let named = |key: &str| -> BTreeMap<String, String> {
            pipeline
                .and_then(|p| p.get(key))
                .and_then(Value::as_object)
                .into_iter()
                .flatten()
                .filter_map(|(name, spec)| {
                    let kind = spec.get("type").and_then(Value::as_str)?;
                    Some((name.clone(), kind.to_string()))
                })
                .collect()
        };
        let mut kinds = Self {
            sources: named("sources"),
            sinks: named("sinks"),
        };
        for (key, map) in [("source", &mut kinds.sources), ("sink", &mut kinds.sinks)] {
            if let Some(kind) = pipeline
                .and_then(|p| p.get(key))
                .and_then(|s| s.get("type"))
                .and_then(Value::as_str)
            {
                map.entry("default".to_string())
                    .or_insert_with(|| kind.to_string());
            }
        }
        kinds
    }

    /// The kind a `type`-less spec in `role` resolves to through its `ref`
    /// (else the `default` template).
    fn resolve(&self, role: Role, spec: &serde_json::Map<String, Value>) -> Option<String> {
        let name = spec.get("ref").and_then(Value::as_str).unwrap_or("default");
        match role {
            Role::Source => self.sources.get(name),
            Role::Sink => self.sinks.get(name),
        }
        .cloned()
    }
}

fn bind_in(
    doc: &mut Value,
    tenant: Option<&TenantValues>,
    field: Field,
    kinds: &TemplateKinds,
    role: Option<Role>,
) -> Result<(), String> {
    match doc {
        Value::String(s) if crate::interpolate::contains_unescaped(s, PREFIX) => match tenant {
            Some(t) => {
                *s = bind_str(s, t, field)?;
                Ok(())
            }
            None => Err(unbound_message(s)),
        },
        Value::Array(a) => a
            .iter_mut()
            .try_for_each(|v| bind_in(v, tenant, field, kinds, None)),
        Value::Object(m) => {
            // A topology node names its role in `kind`.
            let role = role.or(match m.get("kind").and_then(Value::as_str) {
                Some("source") => Some(Role::Source),
                Some("sink") => Some(Role::Sink),
                _ => None,
            });
            let kind = m
                .get("type")
                .and_then(Value::as_str)
                .map(str::to_string)
                .or_else(|| role.and_then(|r| kinds.resolve(r, m)));
            m.iter_mut().try_for_each(|(k, v)| match (k.as_str(), v) {
                ("config", Value::Object(cfg)) => cfg.iter_mut().try_for_each(|(k, v)| {
                    bind_in(v, tenant, config_field(kind.as_deref(), k), kinds, None)
                }),
                ("source", v) => bind_in(v, tenant, Field::Plain, kinds, Some(Role::Source)),
                ("sink", v) => bind_in(v, tenant, Field::Plain, kinds, Some(Role::Sink)),
                (_, v) => bind_in(v, tenant, Field::Plain, kinds, None),
            })
        }
        _ => Ok(()),
    }
}

/// The kind of a connector config field; a spec whose kind is unknown (no
/// `type`, and no template it resolves to) counts every `query` / `sql` field
/// as SQL.
fn config_field(kind: Option<&str>, key: &str) -> Field {
    if crate::sql_bind::IDENTIFIER_FIELDS.contains(&key) {
        return Field::Identifier;
    }
    let sql = match kind {
        Some(kind) => crate::sql_bind::query_field(kind) == Some(key),
        None => matches!(key, "query" | "sql"),
    };
    if sql { Field::Sql } else { Field::Plain }
}

fn unbound_message(s: &str) -> String {
    format!(
        "'{s}' references a `${{tenant.*}}` token, which is bound only in a run started \
         for a tenant (`POST /v1/tenants/{{tenant}}/runs` or \
         `/v1/tenants/{{tenant}}/templates/{{id}}/runs`)"
    )
}

/// Refuse a leftover `${tenant.*}` token in a connector config (every runtime
/// but a tenant run reaches the executor with them unbound).
pub fn reject_unbound(value: &Value, owner: &str) -> CliResult<()> {
    match value {
        Value::String(s) if crate::interpolate::contains_unescaped(s, PREFIX) => Err(
            CliError::Config(format!("the {owner} config: {}", unbound_message(s))),
        ),
        Value::Array(a) => a.iter().try_for_each(|v| reject_unbound(v, owner)),
        Value::Object(m) => m.values().try_for_each(|v| reject_unbound(v, owner)),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn acme() -> TenantValues {
        TenantValues {
            id: "acme".into(),
            name: Some("Acme Corp".into()),
            labels: BTreeMap::from([("region".into(), "eu".into())]),
        }
    }

    #[test]
    fn values_stay_literal_and_sql_takes_only_identifiers() {
        let t = TenantValues {
            id: "acme".into(),
            name: Some("O'Reilly".into()),
            labels: BTreeMap::from([
                ("schema".into(), "public; DROP SCHEMA billing".into()),
                ("ref".into(), "${vault:secret/x}".into()),
                ("ok".into(), "acme_eu".into()),
            ]),
        };
        let mut doc = json!({"note": "${tenant.labels.ref}"});
        bind_document(&mut doc, Some(&t)).unwrap();
        assert_eq!(doc["note"], "$${vault:secret/x}");

        let mut doc = json!({"source": {"type": "postgres", "config": {
            "query": "SELECT * FROM ${tenant.labels.ok}.orders"}}});
        bind_document(&mut doc, Some(&t)).unwrap();
        assert_eq!(
            doc["source"]["config"]["query"],
            "SELECT * FROM acme_eu.orders"
        );

        for (doc, place) in [
            (
                json!({"source": {"type": "postgres", "config": {
                    "query": "SELECT * FROM ${tenant.labels.schema}.orders"}}}),
                "a SQL query",
            ),
            (
                json!({"matrix": [{"source": {"config": {"query": "x = '${tenant.name}'"}}}]}),
                "a SQL query",
            ),
            (
                json!({"sink": {"type": "postgres", "config": {"table_name": "${tenant.name}"}}}),
                "a table",
            ),
        ] {
            let mut doc = doc;
            let err = bind_document(&mut doc, Some(&t)).unwrap_err();
            assert!(
                err.contains(place) && err.contains("plain identifier"),
                "{err}"
            );
        }

        let mut rest = json!({"source": {"type": "rest", "config": {"query": "${tenant.name}"}}});
        bind_document(&mut rest, Some(&t)).unwrap();
        assert_eq!(rest["source"]["config"]["query"], "O'Reilly");
    }

    #[test]
    fn a_row_picking_a_template_by_ref_is_classified_by_its_kind() {
        let doc = |row_query: &str| {
            json!({
                "pipeline": {
                    "sources": {
                        "gql": {"type": "graphql", "config": {}},
                        "db": {"type": "postgres", "config": {}},
                    },
                    "sink": {"type": "postgres", "config": {}},
                },
                "matrix": [
                    {"source": {"ref": "gql", "config": {"query": row_query}}},
                ],
                "nodes_check": {"kind": "source", "ref": "gql", "config": {"query": row_query}},
            })
        };
        // GraphQL `query` text is not SQL: a non-identifier name binds.
        let mut ok = doc("{ orgs(name: \"${tenant.name}\") { id } }");
        bind_document(&mut ok, Some(&acme())).unwrap();
        assert_eq!(
            ok["matrix"][0]["source"]["config"]["query"],
            "{ orgs(name: \"Acme Corp\") { id } }"
        );
        assert_eq!(
            ok["nodes_check"]["config"]["query"],
            "{ orgs(name: \"Acme Corp\") { id } }"
        );
        // A row on the SQL template still takes only identifiers.
        let mut sql = json!({
            "pipeline": {"sources": {"db": {"type": "postgres", "config": {}}}},
            "matrix": [{"source": {"ref": "db", "config": {"query": "x = '${tenant.name}'"}}}],
        });
        let err = bind_document(&mut sql, Some(&acme())).unwrap_err();
        assert!(err.contains("a SQL query"), "{err}");
        // No `ref`: the `default` template (the singular `pipeline.source`).
        let mut dflt = json!({
            "pipeline": {"source": {"type": "graphql", "config": {}}},
            "matrix": [{"source": {"config": {"query": "${tenant.name}"}}}],
        });
        bind_document(&mut dflt, Some(&acme())).unwrap();
        assert_eq!(dflt["matrix"][0]["source"]["config"]["query"], "Acme Corp");
    }

    #[test]
    fn binds_ids_names_and_labels_anywhere() {
        let mut doc = json!({
            "sink": {"config": {"table": "raw_${tenant.id}", "tags": ["${tenant.labels.region}"]}},
            "name": "${tenant.name}",
            "n": 3
        });
        bind_document(&mut doc, Some(&acme())).unwrap();
        assert_eq!(doc["sink"]["config"]["table"], "raw_acme");
        assert_eq!(doc["sink"]["config"]["tags"][0], "eu");
        assert_eq!(doc["name"], "Acme Corp");
        assert_eq!(doc["n"], 3);
    }

    #[test]
    fn name_falls_back_to_id_and_repeats_bind() {
        let t = TenantValues {
            name: None,
            ..acme()
        };
        let mut doc = json!("${tenant.name}/${tenant.id}");
        bind_document(&mut doc, Some(&t)).unwrap();
        assert_eq!(doc, "acme/acme");
    }

    #[test]
    fn errors_name_the_problem() {
        let t = acme();
        let err = bind_document(&mut json!("${tenant.labels.tier}"), Some(&t)).unwrap_err();
        assert!(err.contains("no label 'tier'"), "{err}");
        let err = bind_document(&mut json!("${tenant.secret}"), Some(&t)).unwrap_err();
        assert!(err.contains("unknown tenant token"), "{err}");
        let err = bind_document(&mut json!("${tenant.labels.}"), Some(&t)).unwrap_err();
        assert!(err.contains("unknown tenant token"), "{err}");
        let err = bind_document(&mut json!("x ${tenant.id"), Some(&t)).unwrap_err();
        assert!(err.contains("unterminated"), "{err}");
        let err = bind_document(&mut json!({"a": ["${tenant.id}"]}), None).unwrap_err();
        assert!(err.contains("started for a tenant"), "{err}");
    }

    #[test]
    fn reject_unbound_walks_the_config() {
        assert!(reject_unbound(&json!({"path": "out.jsonl"}), "sink").is_ok());
        let err = reject_unbound(&json!({"a": [{"b": "${tenant.id}"}]}), "sink").unwrap_err();
        assert!(err.to_string().contains("the sink config"), "{err}");
    }
}
