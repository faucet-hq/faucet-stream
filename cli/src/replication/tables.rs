//! Pure planning for a multi-table mirror (#731): which discovered tables are
//! mirrored, what each one's destination / key / snapshot selection is, how
//! the CDC source is scoped to the set, and how a change record is routed back
//! to its table. No I/O — every function here is unit-tested directly.

use crate::replication::spec::{TableOverride, TablesSpec, WithoutPrimaryKey};
use faucet_core::DatasetDescriptor;
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, BTreeSet};

/// CDC source kinds that can drive a multi-table mirror (each implements
/// `Source::record_table`). `dynamodb` qualifies in `mode: streams`.
pub const MULTI_TABLE_CDC_KINDS: &[&str] = &[
    "postgres-cdc",
    "mysql-cdc",
    "mongodb-cdc",
    "mssql-cdc",
    "oracle-cdc",
    "dynamodb",
];

/// Everything needed to mirror one table.
#[derive(Debug, Clone, PartialEq)]
pub struct TablePlan {
    /// The discovered table name (`public.orders`).
    pub name: String,
    /// Node id / state-key segment for the table (`{mirror}::{id}`).
    pub id: String,
    /// Upsert key (empty for an append-only table).
    pub key: Vec<String>,
    /// Effective destination write mode.
    pub write_mode: String,
    /// Full sink config for this table.
    pub sink_config: Value,
    /// Full snapshot-source config for this table.
    pub snapshot_config: Value,
    /// Per-table schema-drift policy override.
    pub schema_drift: Option<faucet_core::SchemaDriftSpec>,
    /// Catalog row estimate, for snapshot progress.
    pub estimated_rows: Option<u64>,
}

/// The outcome of resolving one discovered table against the config.
#[derive(Debug, Clone, PartialEq)]
pub enum Resolution {
    /// Mirror it with this plan.
    Mirror(Box<TablePlan>),
    /// Never mirror it; the reason is reported in status.
    Refused(String),
}

/// Whether `name` passes the include / exclude globs.
pub fn selected(spec: &TablesSpec, name: &str) -> bool {
    spec.include
        .iter()
        .any(|g| crate::select::glob_match(g, name))
        && !spec
            .exclude
            .iter()
            .any(|g| crate::select::glob_match(g, name))
}

/// A state-key-safe node id for a table name: characters a state key cannot
/// hold become `_`, and a leading `__` is prefixed so a table never takes a
/// marker's key.
pub fn node_id(table: &str) -> String {
    let id: String = table
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.') {
                c
            } else {
                '_'
            }
        })
        .collect();
    match id.strip_prefix('.') {
        Some(rest) => format!("_{rest}"),
        // `__name__` segments are the mirror's own markers
        // (`{name}::__replication__`), so a table may never take one.
        None if id.starts_with("__") => format!("t{id}"),
        None => id,
    }
}

/// `(schema, table_name)` of a discovered name: split at the last `.`.
pub fn split_name(table: &str) -> (&str, &str) {
    table.rsplit_once('.').unwrap_or(("", table))
}

/// Replace `{table}`, `{table_name}` and `{schema}` in every string of `value`.
pub fn render_placeholders(value: &Value, table: &str) -> Value {
    let (schema, name) = split_name(table);
    match value {
        Value::String(s) => Value::String(
            s.replace("{table_name}", name)
                .replace("{schema}", schema)
                .replace("{table}", table),
        ),
        Value::Array(items) => Value::Array(
            items
                .iter()
                .map(|v| render_placeholders(v, table))
                .collect(),
        ),
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(k, v)| (k.clone(), render_placeholders(v, table)))
                .collect(),
        ),
        other => other.clone(),
    }
}

/// The default destination patch for a sink kind, or `None` when the kind has
/// no single table field (file sinks) and `tables.destination` is required.
pub fn default_destination(sink_kind: &str) -> Option<Value> {
    let field = match sink_kind {
        "postgres" | "mysql" | "sqlite" | "duckdb" | "redshift" | "spanner" | "dynamodb" => {
            "table_name"
        }
        "mssql" | "snowflake" | "clickhouse" | "oracle" | "databricks" => "table",
        "bigquery" => "table_id",
        "mongodb" => "collection",
        "elasticsearch" => "index",
        _ => return None,
    };
    Some(json!({ field: "{table_name}" }))
}

/// Resolve one discovered table: its destination, key, write mode and
/// snapshot selection. `sink_template` / `snapshot_template` are the
/// connector configs from `pipeline.sink` / `mirror.snapshot.source`;
/// `upsert_capable` says whether the sink supports keyed writes.
pub fn resolve(
    spec: &TablesSpec,
    sink_kind: &str,
    sink_template: &Value,
    snapshot_template: &Value,
    upsert_capable: bool,
    over: Option<&TableOverride>,
    descriptor: &DatasetDescriptor,
) -> Resolution {
    let table = descriptor.name.as_str();
    let destination = match spec
        .destination
        .clone()
        .or_else(|| default_destination(sink_kind))
    {
        Some(d) => d,
        None => {
            return Resolution::Refused(format!(
                "sink '{sink_kind}' has no default destination field — set \
                 mirror.tables.destination (e.g. {{ path: \"out/{{table}}.jsonl\" }})"
            ));
        }
    };
    let mut sink = sink_template.clone();
    if !sink.is_object() {
        sink = Value::Object(Map::new());
    }
    crate::merge::merge_value(&mut sink, render_placeholders(&destination, table));
    if let Some(extra) = over.and_then(|o| o.sink.as_ref()) {
        crate::merge::merge_value(&mut sink, render_placeholders(extra, table));
    }

    let template_mode = sink_template
        .get("write_mode")
        .and_then(Value::as_str)
        .map(str::to_string);
    let mut write_mode = over
        .and_then(|o| o.write_mode.clone())
        .or(template_mode)
        .unwrap_or_else(|| if upsert_capable { "upsert" } else { "append" }.to_string());
    let keyed = matches!(write_mode.as_str(), "upsert" | "delete");
    let mut key: Vec<String> = Vec::new();
    if keyed {
        key = over
            .and_then(|o| o.key.clone())
            .or_else(|| descriptor.primary_key.clone())
            .unwrap_or_default();
        if key.is_empty() {
            match spec.without_primary_key {
                WithoutPrimaryKey::Refuse => {
                    return Resolution::Refused(format!(
                        "table '{table}' has no primary key — set mirror.per_table.\"{table}\".key \
                         or mirror.tables.without_primary_key: append"
                    ));
                }
                WithoutPrimaryKey::Append => write_mode = "append".to_string(),
            }
        }
    }
    if let Some(obj) = sink.as_object_mut() {
        obj.insert("write_mode".into(), Value::String(write_mode.clone()));
        if key.is_empty() {
            if write_mode == "append" {
                obj.remove("key");
            }
        } else {
            obj.insert("key".into(), json!(key));
        }
    }

    let mut snapshot = snapshot_template.clone();
    if !snapshot.is_object() {
        snapshot = Value::Object(Map::new());
    }
    crate::merge::merge_value(&mut snapshot, descriptor.config_patch.clone());
    if let Some(extra) = over.and_then(|o| o.snapshot.as_ref()) {
        crate::merge::merge_value(&mut snapshot, render_placeholders(extra, table));
    }

    Resolution::Mirror(Box::new(TablePlan {
        name: table.to_string(),
        id: node_id(table),
        key,
        write_mode,
        sink_config: sink,
        snapshot_config: snapshot,
        schema_drift: over.and_then(|o| o.schema_drift.clone()),
        estimated_rows: descriptor.estimated_rows,
    }))
}

/// The sink config one mirrored table would get, for offline validation of a
/// multi-table mirror (`faucet validate` has no catalog to discover): the
/// destination rendered for a placeholder table keyed on `id`.
pub fn sample_sink_config(
    spec: &TablesSpec,
    sink_kind: &str,
    sink_template: &Value,
    upsert_capable: bool,
) -> Option<Value> {
    let sample = DatasetDescriptor::new("sample_schema.sample_table", "table", json!({}))
        .with_primary_key(vec!["id".to_string()]);
    match resolve(
        spec,
        sink_kind,
        sink_template,
        &json!({}),
        upsert_capable,
        None,
        &sample,
    ) {
        Resolution::Mirror(plan) => Some(plan.sink_config),
        Resolution::Refused(_) => None,
    }
}

/// Two tables whose plans would write the same destination, or share a node
/// id, cannot both be mirrored. Returns `(kept, refused-with-reason)`.
pub fn refuse_collisions(
    mut plans: Vec<TablePlan>,
    incumbents: &BTreeSet<String>,
) -> (Vec<TablePlan>, Vec<(String, String)>) {
    plans.sort_by_key(|p| !incumbents.contains(&p.name));
    let mut by_dest: BTreeMap<String, String> = BTreeMap::new();
    let mut ids: BTreeMap<String, String> = BTreeMap::new();
    let mut kept = Vec::new();
    let mut refused = Vec::new();
    for plan in plans {
        let mut dest = plan.sink_config.clone();
        if let Some(obj) = dest.as_object_mut() {
            obj.remove("key");
            obj.remove("write_mode");
        }
        let dest = dest.to_string();
        if let Some(other) = by_dest.get(&dest) {
            refused.push((
                plan.name.clone(),
                format!(
                    "writes the same destination as '{other}' — set mirror.tables.destination \
                     (e.g. {{ table_name: \"{{schema}}_{{table_name}}\" }})"
                ),
            ));
        } else if let Some(other) = ids.get(&plan.id) {
            refused.push((
                plan.name.clone(),
                format!("its state key collides with '{other}'"),
            ));
        } else {
            by_dest.insert(dest, plan.name.clone());
            ids.insert(plan.id.clone(), plan.name.clone());
            kept.push(plan);
        }
    }
    (kept, refused)
}

/// Scope a CDC source config to the tables it streams for the mirror. Kinds
/// whose stream is not narrowed by config (postgres-cdc publications, a
/// mongodb-cdc database scope, mysql-cdc's own filters) pass through.
pub fn cdc_config_for(kind: &str, base: &Value, tables: &[String]) -> Value {
    let mut cfg = base.clone();
    let Some(obj) = cfg.as_object_mut() else {
        return cfg;
    };
    match kind {
        "oracle-cdc" => {
            obj.insert("tables".into(), json!(tables));
        }
        "mssql-cdc"
            if obj
                .get("capture_instances")
                .and_then(Value::as_array)
                .is_none_or(|a| a.is_empty()) =>
        {
            let instances: Vec<String> = tables
                .iter()
                .map(|t| {
                    let (schema, name) = split_name(t);
                    if schema.is_empty() {
                        format!("dbo_{name}")
                    } else {
                        format!("{schema}_{name}")
                    }
                })
                .collect();
            obj.insert("capture_instances".into(), json!(instances));
        }
        "dynamodb" => {
            if let Some(first) = tables.first() {
                obj.insert("table_name".into(), json!(first));
            }
        }
        _ => {}
    }
    cfg
}

/// Group tables onto change streams: every kind shares one stream per
/// connection except DynamoDB, where each table has its own stream.
pub fn stream_groups(kind: &str, tables: &[String]) -> Vec<Vec<String>> {
    if tables.is_empty() {
        return Vec::new();
    }
    if kind == "dynamodb" {
        tables.iter().map(|t| vec![t.clone()]).collect()
    } else {
        vec![tables.to_vec()]
    }
}

/// The database a MySQL snapshot connection points at: discovery reports bare
/// table names there, while the binlog reports `database.table`.
pub fn db_qualifier(snapshot_kind: &str, snapshot_config: &Value) -> Option<String> {
    if snapshot_kind != "mysql" {
        return None;
    }
    let url = snapshot_config.get("connection_url")?.as_str()?;
    let after_host = url.split_once("://").map_or(url, |(_, r)| r);
    let path = after_host.split_once('/')?.1;
    let db = path.split(['?', '#']).next()?;
    (!db.is_empty()).then(|| db.to_string())
}

/// Where a change record for `name` goes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Route {
    /// A table streaming in this cycle.
    Active(String),
    /// A known table not streaming now (pending, snapshotting, paused,
    /// dropped, refused) — dropped here; its snapshot or re-snapshot covers it.
    Held,
    /// A matching table the mirror has not seen — triggers discovery.
    New(String),
    /// Not a mirrored table.
    Ignored,
}

/// Routes change records to their table.
#[derive(Debug, Clone, Default)]
pub struct Router {
    /// Tables streaming in this cycle.
    pub active: BTreeSet<String>,
    /// Every table the mirror knows (any phase).
    pub known: BTreeSet<String>,
    /// MySQL database qualifier (see [`db_qualifier`]).
    pub qualifier: Option<String>,
    /// Include / exclude globs for spotting new tables; `None` = ignore them.
    pub follow: Option<TablesSpec>,
}

impl Router {
    fn canonical(&self, name: &str) -> Option<String> {
        if self.known.contains(name) {
            return Some(name.to_string());
        }
        match (&self.qualifier, name.split_once('.')) {
            (Some(q), Some((schema, table))) if schema == q && self.known.contains(table) => {
                Some(table.to_string())
            }
            _ => None,
        }
    }

    /// Route a record whose source-reported table is `name`.
    pub fn route(&self, name: &str) -> Route {
        match self.canonical(name) {
            Some(t) if self.active.contains(&t) => Route::Active(t),
            Some(_) => Route::Held,
            None => {
                let bare = match (&self.qualifier, name.split_once('.')) {
                    (Some(q), Some((schema, table))) if schema == q => table,
                    (Some(_), Some(_)) => return Route::Ignored,
                    _ => name,
                };
                match &self.follow {
                    Some(spec) if selected(spec, bare) => Route::New(bare.to_string()),
                    _ => Route::Ignored,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication::spec::NewTables;

    fn spec() -> TablesSpec {
        serde_yaml::from_str("include: [\"public.*\"]\nexclude: [\"public.audit_*\"]").unwrap()
    }

    fn desc(name: &str, pk: &[&str]) -> DatasetDescriptor {
        DatasetDescriptor::new(
            name,
            "table",
            json!({"query": format!("SELECT * FROM {name}")}),
        )
        .with_primary_key(pk.iter().map(|s| s.to_string()).collect())
        .with_estimated_rows(7)
    }

    #[test]
    fn globs_include_and_exclude() {
        let s = spec();
        assert!(selected(&s, "public.orders"));
        assert!(!selected(&s, "public.audit_log"));
        assert!(!selected(&s, "sales.leads"));
        let all: TablesSpec = serde_yaml::from_str("{}").unwrap();
        assert!(selected(&all, "anything"));
        assert_eq!(all.new_tables, NewTables::Follow);
    }

    #[test]
    fn node_ids_are_state_key_safe() {
        assert_eq!(node_id("public.orders"), "public.orders");
        assert_eq!(node_id("my table$x"), "my_table_x");
        assert_eq!(node_id(".hidden"), "_hidden");
        // A table named like the mirror marker never shares its key (#789 CLI-89).
        assert_eq!(node_id("__replication__"), "t__replication__");
        assert_ne!(
            crate::executor::build_state_key("m", &node_id("__replication__"), None),
            crate::replication::state::marker_key("m")
        );
        faucet_core::state::validate_state_key(&format!("m::{}", node_id("a b.\"c\""))).unwrap();
    }

    #[test]
    fn placeholders_render_in_nested_values() {
        let v = json!({"table_name": "{schema}_{table_name}", "tags": ["{table}"], "n": 1});
        assert_eq!(
            render_placeholders(&v, "public.orders"),
            json!({"table_name": "public_orders", "tags": ["public.orders"], "n": 1})
        );
        assert_eq!(render_placeholders(&json!("{schema}"), "orders"), json!(""));
    }

    #[test]
    fn default_destinations_cover_keyed_sinks() {
        assert_eq!(
            default_destination("postgres"),
            Some(json!({"table_name": "{table_name}"}))
        );
        assert_eq!(
            default_destination("mssql"),
            Some(json!({"table": "{table_name}"}))
        );
        assert_eq!(
            default_destination("bigquery"),
            Some(json!({"table_id": "{table_name}"}))
        );
        assert_eq!(
            default_destination("mongodb"),
            Some(json!({"collection": "{table_name}"}))
        );
        assert_eq!(
            default_destination("elasticsearch"),
            Some(json!({"index": "{table_name}"}))
        );
        assert_eq!(default_destination("jsonl"), None);
    }

    #[test]
    fn resolves_keyed_table_from_primary_key() {
        let sink = json!({"connection_url": "postgres://d", "column_mapping": "auto_map"});
        let snap = json!({"connection_url": "postgres://s", "query": "SELECT 1"});
        let Resolution::Mirror(p) = resolve(
            &spec(),
            "postgres",
            &sink,
            &snap,
            true,
            None,
            &desc("public.orders", &["id"]),
        ) else {
            panic!("expected a plan")
        };
        assert_eq!(p.key, vec!["id"]);
        assert_eq!(p.write_mode, "upsert");
        assert_eq!(p.sink_config["table_name"], "orders");
        assert_eq!(p.sink_config["key"], json!(["id"]));
        assert_eq!(p.snapshot_config["query"], "SELECT * FROM public.orders");
        assert_eq!(p.snapshot_config["connection_url"], "postgres://s");
        assert_eq!(p.estimated_rows, Some(7));
    }

    #[test]
    fn a_missing_sink_template_starts_from_the_destination() {
        let Resolution::Mirror(p) = resolve(
            &spec(),
            "postgres",
            &Value::Null,
            &json!({}),
            true,
            None,
            &desc("public.orders", &["id"]),
        ) else {
            panic!("expected a plan")
        };
        assert_eq!(p.sink_config["table_name"], "orders");
        assert_eq!(p.sink_config["key"], json!(["id"]));
    }

    #[test]
    fn keyless_tables_are_refused_or_appended() {
        let sink = json!({});
        let d = desc("public.log", &[]);
        match resolve(&spec(), "postgres", &sink, &json!({}), true, None, &d) {
            Resolution::Refused(r) => assert!(r.contains("no primary key"), "{r}"),
            other => panic!("{other:?}"),
        }
        let mut s = spec();
        s.without_primary_key = WithoutPrimaryKey::Append;
        let Resolution::Mirror(p) = resolve(
            &s,
            "postgres",
            &json!({"key": ["x"]}),
            &json!({}),
            true,
            None,
            &d,
        ) else {
            panic!()
        };
        assert_eq!(p.write_mode, "append");
        assert!(p.sink_config.get("key").is_none());
        match resolve(&spec(), "jsonl", &sink, &json!({}), false, None, &d) {
            Resolution::Refused(r) => assert!(r.contains("destination"), "{r}"),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn overrides_win_over_discovery_and_template() {
        let over = TableOverride {
            key: Some(vec!["order_id".into()]),
            write_mode: Some("upsert".into()),
            sink: Some(json!({"table_name": "orders_mirror"})),
            snapshot: Some(json!({"query": "SELECT * FROM {table} WHERE live"})),
            schema_drift: Some(serde_yaml::from_str("on_drift: evolve").unwrap()),
        };
        let mut s = spec();
        s.destination = Some(json!({"table_name": "{schema}_{table_name}"}));
        let Resolution::Mirror(p) = resolve(
            &s,
            "postgres",
            &json!({"write_mode": "append"}),
            &json!({}),
            true,
            Some(&over),
            &desc("public.orders", &["id"]),
        ) else {
            panic!()
        };
        assert_eq!(p.key, vec!["order_id"]);
        assert_eq!(p.sink_config["table_name"], "orders_mirror");
        assert_eq!(
            p.snapshot_config["query"],
            "SELECT * FROM public.orders WHERE live"
        );
        assert!(p.schema_drift.is_some());
        let Resolution::Mirror(p) = resolve(
            &s,
            "postgres",
            &json!({"write_mode": "append"}),
            &json!(null),
            true,
            None,
            &desc("public.orders", &["id"]),
        ) else {
            panic!()
        };
        assert_eq!(p.write_mode, "append", "template write_mode kept");
        assert_eq!(p.sink_config["table_name"], "public_orders");
        assert!(p.snapshot_config.is_object());
    }

    #[test]
    fn sample_sink_config_renders_a_placeholder_table() {
        let cfg =
            sample_sink_config(&spec(), "postgres", &json!({"connection_url": "x"}), true).unwrap();
        assert_eq!(cfg["table_name"], "sample_table");
        assert_eq!(cfg["key"], json!(["id"]));
        assert!(sample_sink_config(&spec(), "jsonl", &json!({}), false).is_none());
    }

    #[test]
    fn colliding_destinations_and_ids_are_refused() {
        let plan = |name: &str, table: &str, id: &str| TablePlan {
            name: name.into(),
            id: id.into(),
            key: vec![],
            write_mode: "append".into(),
            sink_config: json!({"table_name": table, "key": ["k"]}),
            snapshot_config: json!({}),
            schema_drift: None,
            estimated_rows: None,
        };
        let (kept, refused) = refuse_collisions(
            vec![
                plan("a.orders", "orders", "a.orders"),
                plan("b.orders", "orders", "b.orders"),
                plan("c x", "cx", "c_x"),
                plan("c$x", "cy", "c_x"),
            ],
            &BTreeSet::new(),
        );
        assert_eq!(
            kept.iter().map(|p| p.name.as_str()).collect::<Vec<_>>(),
            ["a.orders", "c x"]
        );
        assert_eq!(refused.len(), 2);
        assert!(refused[0].1.contains("same destination"));
        assert!(refused[1].1.contains("collides"));
    }

    #[test]
    fn an_incumbent_keeps_its_destination_when_a_new_table_sorts_first() {
        let plan = |name: &str| TablePlan {
            name: name.into(),
            id: name.into(),
            key: vec!["id".into()],
            write_mode: "upsert".into(),
            sink_config: json!({"table_name": "users"}),
            snapshot_config: json!({}),
            schema_drift: None,
            estimated_rows: None,
        };
        let incumbents = BTreeSet::from(["public.users".to_string()]);
        let (kept, refused) =
            refuse_collisions(vec![plan("auth.users"), plan("public.users")], &incumbents);
        assert_eq!(
            kept.iter().map(|p| p.name.as_str()).collect::<Vec<_>>(),
            ["public.users"]
        );
        assert_eq!(refused.len(), 1);
        assert_eq!(refused[0].0, "auth.users");
        assert!(refused[0].1.contains("'public.users'"));
    }

    #[test]
    fn cdc_configs_are_scoped_per_kind() {
        let tables = vec!["APP.ORDERS".to_string(), "APP.ITEMS".to_string()];
        assert_eq!(
            cdc_config_for("oracle-cdc", &json!({"tables": ["X"]}), &tables)["tables"],
            json!(["APP.ORDERS", "APP.ITEMS"])
        );
        assert_eq!(
            cdc_config_for(
                "mssql-cdc",
                &json!({}),
                &["dbo.Orders".into(), "Items".into()]
            )["capture_instances"],
            json!(["dbo_Orders", "dbo_Items"])
        );
        let named = json!({"capture_instances": ["custom_ci"]});
        assert_eq!(
            cdc_config_for("mssql-cdc", &named, &tables),
            named,
            "explicit instances are kept"
        );
        assert_eq!(
            cdc_config_for("dynamodb", &json!({"mode": "streams"}), &["t1".into()])["table_name"],
            "t1"
        );
        let pg = json!({"slot_name": "s"});
        assert_eq!(cdc_config_for("postgres-cdc", &pg, &tables), pg);
        assert_eq!(
            cdc_config_for("oracle-cdc", &json!(null), &tables),
            json!(null)
        );
    }

    #[test]
    fn streams_group_by_connection() {
        let t = vec!["a".to_string(), "b".to_string()];
        assert_eq!(stream_groups("postgres-cdc", &t), vec![t.clone()]);
        assert_eq!(
            stream_groups("dynamodb", &t),
            vec![vec!["a".to_string()], vec!["b".to_string()]]
        );
        assert!(stream_groups("mysql-cdc", &[]).is_empty());
    }

    #[test]
    fn mysql_qualifier_comes_from_the_snapshot_url() {
        let c = |u: &str| json!({"connection_url": u});
        assert_eq!(
            db_qualifier("mysql", &c("mysql://u:p@h:3306/shop?ssl=1")),
            Some("shop".into())
        );
        assert_eq!(db_qualifier("mysql", &c("mysql://h:3306/")), None);
        assert_eq!(db_qualifier("mysql", &c("mysql://h:3306")), None);
        assert_eq!(db_qualifier("postgres", &c("postgres://h/db")), None);
        assert_eq!(db_qualifier("mysql", &json!({})), None);
    }

    #[test]
    fn router_routes_active_held_new_and_ignored() {
        let mut r = Router {
            active: ["public.orders".to_string()].into(),
            known: ["public.orders".to_string(), "public.items".to_string()].into(),
            qualifier: None,
            follow: Some(spec()),
        };
        assert_eq!(
            r.route("public.orders"),
            Route::Active("public.orders".into())
        );
        assert_eq!(r.route("public.items"), Route::Held);
        assert_eq!(r.route("public.new_t"), Route::New("public.new_t".into()));
        assert_eq!(r.route("public.audit_x"), Route::Ignored);
        assert_eq!(r.route("sales.x"), Route::Ignored);
        r.follow = None;
        assert_eq!(r.route("public.new_t"), Route::Ignored);

        let q = Router {
            active: ["orders".to_string()].into(),
            known: ["orders".to_string()].into(),
            qualifier: Some("shop".into()),
            follow: Some(serde_yaml::from_str("{}").unwrap()),
        };
        assert_eq!(q.route("shop.orders"), Route::Active("orders".into()));
        assert_eq!(q.route("shop.fresh"), Route::New("fresh".into()));
        assert_eq!(q.route("other.orders"), Route::Ignored);
        assert_eq!(q.route("bare"), Route::New("bare".into()));
    }
}
