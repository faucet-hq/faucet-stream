//! Pure query planning: bind resolution, incremental filtering, discovery row
//! grouping and state-key derivation. No driver calls.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};

use faucet_common_oracle::{TypeFamily, quote_ident_oracle};
use faucet_core::replication::{filter_incremental, max_replication_value, max_value};
use faucet_core::{DatasetDescriptor, FaucetError};
use serde_json::{Value, json};

use crate::config::{OracleReplication, OracleSourceConfig, has_bookmark_placeholder};

/// Incremental-replication context resolved for one run.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct IncrementalCtx {
    pub column: String,
    pub start: Value,
}

/// A planned query: the SQL, the positional values for `:1..:n`, and the
/// bookmark value for `:bookmark` (when incremental).
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PlannedQuery {
    pub sql: String,
    pub params: Vec<Value>,
    pub bookmark: Option<Value>,
    pub incremental: Option<IncrementalCtx>,
}

/// Resolve parent-context placeholders and the incremental cursor.
pub(crate) fn plan_query(
    config: &OracleSourceConfig,
    context: &HashMap<String, Value>,
    start_bookmark: Option<&Value>,
) -> PlannedQuery {
    let (sql, ctx_values) = faucet_core::util::substitute_context_bind_params(
        &config.query,
        context,
        config.params.len() + 1,
        |i| format!(":{i}"),
    );
    let mut params = config.params.clone();
    params.extend(ctx_values);

    let incremental = match &config.replication {
        OracleReplication::Full => None,
        OracleReplication::Incremental {
            column,
            initial_value,
        } => Some(IncrementalCtx {
            column: column.clone(),
            start: start_bookmark
                .cloned()
                .unwrap_or_else(|| initial_value.clone()),
        }),
    };
    let bookmark = match &incremental {
        Some(ctx) if has_bookmark_placeholder(&sql) => Some(ctx.start.clone()),
        _ => None,
    };
    PlannedQuery {
        sql,
        params,
        bookmark,
        incremental,
    }
}

/// Map the statement's bind names (as the driver reports them, upper-cased) to
/// values: `:N` → `params[N-1]`, `:BOOKMARK` → the cursor.
pub(crate) fn resolve_binds(
    names: &[String],
    params: &[Value],
    bookmark: Option<&Value>,
) -> Result<Vec<(String, Value)>, FaucetError> {
    names
        .iter()
        .map(|name| {
            if let Ok(n) = name.parse::<usize>() {
                return params
                    .get(n.wrapping_sub(1))
                    .cloned()
                    .map(|v| (name.clone(), v))
                    .ok_or_else(|| {
                        FaucetError::Config(format!(
                            "oracle query binds :{name} but only {} `params` value(s) are set",
                            params.len()
                        ))
                    });
            }
            if name.eq_ignore_ascii_case("BOOKMARK") {
                return bookmark.cloned().map(|v| (name.clone(), v)).ok_or_else(|| {
                    FaucetError::Config(
                        "oracle query uses :bookmark but replication is not incremental".into(),
                    )
                });
            }
            Err(FaucetError::Config(format!(
                "oracle query binds unknown placeholder :{name} (use :1..:n for `params` \
                 and :bookmark for the incremental cursor)"
            )))
        })
        .collect()
}

/// An owned bind value with a JSON-faithful Oracle type.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum OwnedBind {
    Null,
    Int(i64),
    Float(f64),
    Text(String),
}

impl OwnedBind {
    pub fn from_value(v: &Value) -> Self {
        match v {
            Value::Null => OwnedBind::Null,
            Value::Bool(b) => OwnedBind::Int(i64::from(*b)),
            Value::Number(n) => match n.as_i64() {
                Some(i) => OwnedBind::Int(i),
                None if n.is_u64() => OwnedBind::Text(n.to_string()),
                None => OwnedBind::Float(n.as_f64().unwrap_or_default()),
            },
            Value::String(s) => OwnedBind::Text(s.clone()),
            other => OwnedBind::Text(other.to_string()),
        }
    }
}

/// Filter a page for incremental replication and advance `running_max`.
pub(crate) fn apply_incremental(
    page: Vec<Value>,
    incr: Option<&IncrementalCtx>,
    running_max: &mut Option<Value>,
) -> Vec<Value> {
    let Some(ctx) = incr else {
        return page;
    };
    let kept = filter_incremental(page, &ctx.column, &ctx.start);
    if let Some(m) = max_replication_value(&kept, &ctx.column) {
        let m = m.clone();
        *running_max = Some(match running_max.take() {
            Some(prev) => max_value(prev, m),
            None => m,
        });
    }
    kept
}

/// Derive a stable state key from the database scope and a query fingerprint.
pub(crate) fn default_state_key(config: &OracleSourceConfig) -> String {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    config.query.hash(&mut hasher);
    format!(
        "oracle:{}:{:016x}",
        config.connection.scope_label(),
        hasher.finish()
    )
}

/// One `ALL_TAB_COLUMNS` row joined with `ALL_TABLES.NUM_ROWS`.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CatalogRow {
    pub owner: String,
    pub table: String,
    pub column: String,
    pub data_type: String,
    pub scale: Option<i64>,
    pub nullable: bool,
    pub num_rows: Option<i64>,
}

/// Group catalog rows (ordered by owner, table, column id) into one descriptor
/// per table. Native `JSON` columns are serialized in the generated query and
/// listed under `json_columns`, since the driver cannot fetch them directly.
pub(crate) fn descriptors_from_catalog(
    rows: Vec<CatalogRow>,
) -> Result<Vec<DatasetDescriptor>, FaucetError> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < rows.len() {
        let (owner, table) = (rows[i].owner.clone(), rows[i].table.clone());
        let mut j = i;
        while j < rows.len() && rows[j].owner == owner && rows[j].table == table {
            j += 1;
        }
        out.push(table_descriptor(&rows[i..j])?);
        i = j;
    }
    Ok(out)
}

fn table_descriptor(cols: &[CatalogRow]) -> Result<DatasetDescriptor, FaucetError> {
    let first = &cols[0];
    let from = format!(
        "{}.{}",
        quote_ident_oracle(&first.owner)?,
        quote_ident_oracle(&first.table)?
    );
    let mut select = Vec::with_capacity(cols.len());
    let mut json_columns = Vec::new();
    let mut schema_cols = Vec::with_capacity(cols.len());
    for c in cols {
        let family = TypeFamily::from_data_type(&c.data_type, c.scale);
        let q = quote_ident_oracle(&c.column)?;
        if family == TypeFamily::Json {
            select.push(format!("JSON_SERIALIZE({q} RETURNING CLOB) AS {q}"));
            json_columns.push(c.column.clone());
        } else {
            select.push(q);
        }
        let mut fragment = family.json_schema();
        if c.nullable {
            fragment = faucet_core::nullable_type(fragment);
        }
        schema_cols.push((c.column.clone(), fragment));
    }
    let query = if json_columns.is_empty() {
        format!("SELECT * FROM {from}")
    } else {
        format!("SELECT {} FROM {from}", select.join(", "))
    };
    let patch = if json_columns.is_empty() {
        json!({ "query": query })
    } else {
        json!({ "query": query, "json_columns": json_columns })
    };
    let mut d = DatasetDescriptor::new(format!("{}.{}", first.owner, first.table), "table", patch)
        .with_schema(faucet_core::columns_to_schema(schema_cols));
    if let Some(n) = first.num_rows.filter(|n| *n >= 0) {
        d = d.with_estimated_rows(n as u64);
    }
    Ok(d)
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_common_oracle::OracleConnectionConfig;

    fn cfg(query: &str) -> OracleSourceConfig {
        OracleSourceConfig::new(
            OracleConnectionConfig::new("db", 1521, "FREEPDB1", "u", "p"),
            query,
        )
    }

    #[test]
    fn plan_full_query_keeps_params() {
        let mut c = cfg("SELECT * FROM T WHERE A = :1");
        c.params = vec![json!(5)];
        let p = plan_query(&c, &HashMap::new(), None);
        assert_eq!(p.sql, "SELECT * FROM T WHERE A = :1");
        assert_eq!(p.params, vec![json!(5)]);
        assert_eq!(p.bookmark, None);
        assert_eq!(p.incremental, None);
    }

    #[test]
    fn plan_substitutes_context_after_params() {
        let mut c = cfg("SELECT * FROM T WHERE A = :1 AND P = {parent.id}");
        c.params = vec![json!("x")];
        let ctx = HashMap::from([("parent.id".to_string(), json!(9))]);
        let p = plan_query(&c, &ctx, None);
        assert_eq!(p.sql, "SELECT * FROM T WHERE A = :1 AND P = :2");
        assert_eq!(p.params, vec![json!("x"), json!(9)]);
    }

    #[test]
    fn plan_incremental_uses_bookmark_or_initial() {
        let mut c = cfg("SELECT * FROM T WHERE U > :bookmark");
        c.replication = OracleReplication::Incremental {
            column: "U".into(),
            initial_value: json!(0),
        };
        let p = plan_query(&c, &HashMap::new(), None);
        assert_eq!(p.bookmark, Some(json!(0)));
        let stored = json!(42);
        let p = plan_query(&c, &HashMap::new(), Some(&stored));
        assert_eq!(p.bookmark, Some(json!(42)));
        assert_eq!(p.incremental.unwrap().start, json!(42));

        c.query = "SELECT * FROM T".into();
        let p = plan_query(&c, &HashMap::new(), None);
        assert_eq!(p.bookmark, None);
        assert!(p.incremental.is_some());
    }

    #[test]
    fn resolve_binds_by_name() {
        let names = vec!["1".to_string(), "BOOKMARK".to_string(), "2".to_string()];
        let out = resolve_binds(&names, &[json!("a"), json!(2)], Some(&json!(7))).unwrap();
        assert_eq!(
            out,
            vec![
                ("1".to_string(), json!("a")),
                ("BOOKMARK".to_string(), json!(7)),
                ("2".to_string(), json!(2)),
            ]
        );
        assert!(resolve_binds(&["3".into()], &[json!(1)], None).is_err());
        assert!(resolve_binds(&["0".into()], &[json!(1)], None).is_err());
        assert!(resolve_binds(&["BOOKMARK".into()], &[], None).is_err());
        let err = resolve_binds(&["FOO".into()], &[], None).unwrap_err();
        assert!(err.to_string().contains(":FOO"));
    }

    #[test]
    fn owned_bind_types() {
        assert_eq!(OwnedBind::from_value(&Value::Null), OwnedBind::Null);
        assert_eq!(OwnedBind::from_value(&json!(true)), OwnedBind::Int(1));
        assert_eq!(OwnedBind::from_value(&json!(-3)), OwnedBind::Int(-3));
        assert_eq!(
            OwnedBind::from_value(&json!(u64::MAX)),
            OwnedBind::Text(u64::MAX.to_string())
        );
        assert_eq!(OwnedBind::from_value(&json!(1.5)), OwnedBind::Float(1.5));
        assert_eq!(
            OwnedBind::from_value(&json!("s")),
            OwnedBind::Text("s".into())
        );
        assert_eq!(
            OwnedBind::from_value(&json!({"a": 1})),
            OwnedBind::Text("{\"a\":1}".into())
        );
    }

    #[test]
    fn incremental_filter_tracks_max() {
        let ctx = IncrementalCtx {
            column: "C".into(),
            start: json!(10),
        };
        let mut running = None;
        let kept = apply_incremental(
            vec![json!({"C": 5}), json!({"C": 15}), json!({"C": 12})],
            Some(&ctx),
            &mut running,
        );
        assert_eq!(kept.len(), 2);
        assert_eq!(running, Some(json!(15)));
        let kept = apply_incremental(vec![json!({"C": 20})], Some(&ctx), &mut running);
        assert_eq!(kept.len(), 1);
        assert_eq!(running, Some(json!(20)));
        let mut none = None;
        assert_eq!(apply_incremental(vec![json!({})], None, &mut none).len(), 1);
        assert_eq!(none, None);
    }

    #[test]
    fn state_key_is_stable_and_valid() {
        let c = cfg("SELECT * FROM T");
        let k = default_state_key(&c);
        assert_eq!(k, default_state_key(&c));
        assert!(k.starts_with("oracle:FREEPDB1:"), "{k}");
        faucet_core::state::validate_state_key(&k).unwrap();
    }

    fn row(owner: &str, table: &str, col: &str, ty: &str, scale: Option<i64>) -> CatalogRow {
        CatalogRow {
            owner: owner.into(),
            table: table.into(),
            column: col.into(),
            data_type: ty.into(),
            scale,
            nullable: col != "ID",
            num_rows: Some(12),
        }
    }

    #[test]
    fn descriptors_group_and_type() {
        let rows = vec![
            row("APP", "ORDERS", "ID", "NUMBER", Some(0)),
            row("APP", "ORDERS", "TOTAL", "NUMBER", Some(2)),
            row("APP", "ORDERS", "DOC", "JSON", None),
            row("HR", "EMP", "ID", "NUMBER", Some(0)),
        ];
        let ds = descriptors_from_catalog(rows).unwrap();
        assert_eq!(ds.len(), 2);
        assert_eq!(ds[0].name, "APP.ORDERS");
        assert_eq!(ds[0].estimated_rows, Some(12));
        assert_eq!(
            ds[0].config_patch["query"],
            "SELECT \"ID\", \"TOTAL\", JSON_SERIALIZE(\"DOC\" RETURNING CLOB) AS \"DOC\" \
             FROM \"APP\".\"ORDERS\""
        );
        assert_eq!(ds[0].config_patch["json_columns"], json!(["DOC"]));
        let schema = ds[0].schema.as_ref().unwrap();
        assert_eq!(schema["properties"]["ID"]["type"], "integer");
        assert_eq!(
            schema["properties"]["TOTAL"]["type"],
            json!(["number", "null"])
        );
        assert_eq!(
            ds[1].config_patch,
            json!({"query": "SELECT * FROM \"HR\".\"EMP\""})
        );
    }

    #[test]
    fn descriptors_handle_missing_stats_and_bad_names() {
        let mut r = row("APP", "T", "ID", "NUMBER", Some(0));
        r.num_rows = None;
        let ds = descriptors_from_catalog(vec![r]).unwrap();
        assert_eq!(ds[0].estimated_rows, None);
        assert!(descriptors_from_catalog(vec![row("A\"", "T", "ID", "NUMBER", None)]).is_err());
        assert!(descriptors_from_catalog(Vec::new()).unwrap().is_empty());
    }
}
