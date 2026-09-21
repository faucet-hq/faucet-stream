//! Schema-driven SQL generation for the BigQuery exactly-once write path (#215).
//!
//! All functions here are **pure** (no I/O): given the target table's schema as
//! [`FieldSpec`]s, they generate the SQL for the atomic
//! `INSERT … SELECT FROM UNNEST(JSON_QUERY_ARRAY(@payload))` + watermark `MERGE`
//! transaction that makes a page's rows and its commit token land atomically.
//! See `docs/superpowers/specs/2026-06-10-bigquery-exactly-once-design.md`.

use faucet_core::idempotency::{
    COMMIT_TOKEN_SCOPE_COL, COMMIT_TOKEN_TABLE, COMMIT_TOKEN_TOKEN_COL,
};
use gcp_bigquery_client::model::field_type::FieldType;
use gcp_bigquery_client::model::table_field_schema::TableFieldSchema;

/// A normalized BigQuery column spec — the sink's own mirror of the client's
/// `TableFieldSchema`, so the SQL generator is independent of the client model
/// and trivially constructible in unit tests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldSpec {
    /// Column / field name (a valid BigQuery identifier, since it came from
    /// BigQuery's own schema).
    pub name: String,
    /// Normalized type.
    pub ty: BqType,
    /// `true` for a `REPEATED` (array) column.
    pub repeated: bool,
    /// Sub-fields for `STRUCT`/`RECORD` columns (empty otherwise).
    pub fields: Vec<FieldSpec>,
}

/// Normalized BigQuery type, collapsing the client's alias variants
/// (`INT64`=`INTEGER`, `FLOAT64`=`FLOAT`, `BOOL`=`BOOLEAN`, `STRUCT`=`RECORD`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BqType {
    String,
    Bytes,
    Int64,
    Float64,
    Numeric,
    BigNumeric,
    Bool,
    Timestamp,
    Date,
    Time,
    Datetime,
    Interval,
    Geography,
    Json,
    Struct,
}

impl BqType {
    /// Map the client's `FieldType` discriminant to a normalized [`BqType`].
    pub fn from_field_type(ft: &FieldType) -> Self {
        match ft {
            FieldType::String => BqType::String,
            FieldType::Bytes => BqType::Bytes,
            FieldType::Integer | FieldType::Int64 => BqType::Int64,
            FieldType::Float | FieldType::Float64 => BqType::Float64,
            FieldType::Numeric => BqType::Numeric,
            FieldType::Bignumeric => BqType::BigNumeric,
            FieldType::Boolean | FieldType::Bool => BqType::Bool,
            FieldType::Timestamp => BqType::Timestamp,
            FieldType::Date => BqType::Date,
            FieldType::Time => BqType::Time,
            FieldType::Datetime => BqType::Datetime,
            FieldType::Interval => BqType::Interval,
            FieldType::Geography => BqType::Geography,
            FieldType::Json => BqType::Json,
            FieldType::Record | FieldType::Struct => BqType::Struct,
        }
    }

    /// The BigQuery SQL type keyword used in a `CAST(... AS <kw>)` / array
    /// element type.
    fn sql_keyword(&self) -> &'static str {
        match self {
            BqType::String => "STRING",
            BqType::Bytes => "BYTES",
            BqType::Int64 => "INT64",
            BqType::Float64 => "FLOAT64",
            BqType::Numeric => "NUMERIC",
            BqType::BigNumeric => "BIGNUMERIC",
            BqType::Bool => "BOOL",
            BqType::Timestamp => "TIMESTAMP",
            BqType::Date => "DATE",
            BqType::Time => "TIME",
            BqType::Datetime => "DATETIME",
            BqType::Interval => "INTERVAL",
            BqType::Geography => "GEOGRAPHY",
            BqType::Json => "JSON",
            BqType::Struct => "STRUCT",
        }
    }
}

impl FieldSpec {
    /// Convert a client `TableFieldSchema` (possibly nested) into a [`FieldSpec`].
    pub fn from_table_field(f: &TableFieldSchema) -> Self {
        let repeated = f.mode.as_deref() == Some("REPEATED");
        let ty = BqType::from_field_type(&f.r#type);
        let fields = f
            .fields
            .as_ref()
            .map(|sub| sub.iter().map(FieldSpec::from_table_field).collect())
            .unwrap_or_default();
        FieldSpec {
            name: f.name.clone(),
            ty,
            repeated,
            fields,
        }
    }
}

/// SQL string literal for a path/value: single-quoted with `\` and `'` escaped
/// (BigQuery accepts backslash escapes in quoted strings).
pub(crate) fn sql_str(s: &str) -> String {
    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "\\'"))
}

/// Backtick-quoted identifier, with backslash and backtick **escaped** — the
/// representation BigQuery's quoted-identifier grammar defines.
///
/// This used to *strip* backticks "defensively, since the schema came from
/// BigQuery". It does not: the same function quotes **user-supplied**
/// identifiers — `write.key` columns in the `MERGE` predicates
/// ([`crate::merge`]) and cleanup scope columns — so a key column named
/// ``a`b`` was silently rewritten to `ab`, and the MERGE then matched a
/// *different, real* column. Silently targeting the wrong column is the worst
/// available outcome; escaping instead yields either the correct identifier or
/// a loud BigQuery error (#654 M15).
pub(crate) fn quote_ident(name: &str) -> String {
    format!("`{}`", name.replace('\\', "\\\\").replace('`', "\\`"))
}

/// A single JSONPath member segment. Safe BigQuery identifiers use the `.name`
/// form; anything else is bracket-quoted (`['weird.name']`).
pub(crate) fn json_path_segment(name: &str) -> String {
    let safe = match name.chars().next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {
            name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        }
        _ => false,
    };
    if safe {
        format!(".{name}")
    } else {
        let esc = name.replace('\\', "\\\\").replace('\'', "\\'");
        format!("['{esc}']")
    }
}

/// Wrap a STRING-typed SQL expression `var` into the column's target type.
/// `Struct` is handled by [`column_expr`], never here.
pub(crate) fn wrap_scalar(ty: &BqType, var: &str) -> String {
    match ty {
        BqType::String => var.to_string(),
        BqType::Bytes => format!("FROM_BASE64({var})"),
        BqType::Geography => format!("ST_GEOGFROMTEXT({var})"),
        BqType::Json => format!("PARSE_JSON({var})"),
        BqType::Struct => unreachable!("struct is handled by column_expr"),
        other => format!("CAST({var} AS {})", other.sql_keyword()),
    }
}

/// Build the `field AS name, …` list for a STRUCT, recursing into each child.
fn struct_field_list(
    fields: &[FieldSpec],
    json_var: &str,
    base_path: &str,
    depth: usize,
) -> String {
    fields
        .iter()
        .map(|f| {
            let child_path = format!("{base_path}{}", json_path_segment(&f.name));
            let expr = column_expr(f, json_var, &child_path, depth);
            format!("{expr} AS {}", quote_ident(&f.name))
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// Generate the SQL extraction expression for one column.
///
/// `json_var` is always the UNNEST element alias or the top-level row variable;
/// `path` is always a JSONPath relative to that variable's JSON root (e.g. `"$.field"`
/// at top level, `"$.child"` inside a nested struct). `depth` keeps nested `UNNEST`
/// aliases unique (`e{depth}` for struct elements, `x{depth}` for scalars).
pub(crate) fn column_expr(field: &FieldSpec, json_var: &str, path: &str, depth: usize) -> String {
    if field.repeated {
        if field.ty == BqType::Struct {
            let elem = format!("e{depth}");
            let fields = struct_field_list(&field.fields, &elem, "$", depth + 1);
            format!(
                "ARRAY(SELECT AS STRUCT {fields} FROM UNNEST(JSON_QUERY_ARRAY({json_var}, {p})) AS {elem})",
                p = sql_str(path)
            )
        } else {
            let x = format!("x{depth}");
            let src = if field.ty == BqType::Json {
                format!("JSON_QUERY_ARRAY({json_var}, {p})", p = sql_str(path))
            } else {
                format!("JSON_VALUE_ARRAY({json_var}, {p})", p = sql_str(path))
            };
            let elem = wrap_scalar(&field.ty, &x);
            format!("ARRAY(SELECT {elem} FROM UNNEST({src}) AS {x})")
        }
    } else if field.ty == BqType::Struct {
        let fields = struct_field_list(&field.fields, json_var, path, depth + 1);
        format!("STRUCT({fields})")
    } else {
        let raw = if field.ty == BqType::Json {
            format!("JSON_QUERY({json_var}, {p})", p = sql_str(path))
        } else {
            format!("JSON_VALUE({json_var}, {p})", p = sql_str(path))
        };
        wrap_scalar(&field.ty, &raw)
    }
}

/// Backtick-quoted fully-qualified table reference (`` `project.dataset.table` ``).
///
/// Inputs are not backtick-stripped (unlike [`quote_ident`]): BigQuery
/// project/dataset/table names cannot contain a backtick per BQ naming rules,
/// and these come from admin-controlled config, never from row data.
pub(crate) fn table_ref(project: &str, dataset: &str, table: &str) -> String {
    format!("`{project}.{dataset}.{table}`")
}

/// Reference to the shared commit-token watermark table in the target dataset.
fn commit_table_ref(project: &str, dataset: &str) -> String {
    format!("`{project}.{dataset}.{COMMIT_TOKEN_TABLE}`")
}

/// `CREATE TABLE IF NOT EXISTS` for the watermark table. Run as its own query
/// job (DDL is kept out of the data transaction, mirroring the SQL sinks).
pub fn build_create_commit_table(project: &str, dataset: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {t} ({scope} STRING NOT NULL, {token} STRING NOT NULL, updated_at TIMESTAMP)",
        t = commit_table_ref(project, dataset),
        scope = COMMIT_TOKEN_SCOPE_COL,
        token = COMMIT_TOKEN_TOKEN_COL,
    )
}

/// Parameterized read of the last committed token for `@scope`.
///
/// `LIMIT 1` has no `ORDER BY`: the `MERGE` below maintains exactly one
/// watermark row per scope, so there is never more than one row to choose from.
pub fn build_select_token(project: &str, dataset: &str) -> String {
    format!(
        "SELECT {token} FROM {t} WHERE {scope} = @scope LIMIT 1",
        token = COMMIT_TOKEN_TOKEN_COL,
        t = commit_table_ref(project, dataset),
        scope = COMMIT_TOKEN_SCOPE_COL,
    )
}

/// Parameterized upsert of the watermark row (one row per `scope`).
pub fn build_merge_token(project: &str, dataset: &str) -> String {
    format!(
        "MERGE {t} T USING (SELECT @scope AS {scope}, @token AS {token}) S ON T.{scope} = S.{scope} \
WHEN MATCHED THEN UPDATE SET {token} = S.{token}, updated_at = CURRENT_TIMESTAMP() \
WHEN NOT MATCHED THEN INSERT ({scope}, {token}, updated_at) VALUES (S.{scope}, S.{token}, CURRENT_TIMESTAMP())",
        t = commit_table_ref(project, dataset),
        scope = COMMIT_TOKEN_SCOPE_COL,
        token = COMMIT_TOKEN_TOKEN_COL,
    )
}

/// The atomic overwrite swap (#492): replace `target_ref`'s contents with
/// `temp_ref`'s in one multi-statement transaction. `TRUNCATE`+`INSERT … SELECT`
/// (rather than `CREATE OR REPLACE … AS SELECT`) preserves the target table's
/// own partitioning, clustering, and description. Both refs must be
/// backtick-quoted `` `p.d.t` `` already.
pub fn build_overwrite_commit_sql(target_ref: &str, temp_ref: &str) -> String {
    format!(
        "BEGIN TRANSACTION;\n\
         TRUNCATE TABLE {target_ref};\n\
         INSERT INTO {target_ref} SELECT * FROM {temp_ref};\n\
         COMMIT TRANSACTION;"
    )
}

/// Scoped/windowed overwrite commit (#518): delete only the rows matching
/// `where_clause`, then insert the staged rows, in one transaction. Preserves
/// out-of-scope rows and the target's partitioning/clustering.
pub fn build_scoped_overwrite_commit_sql(
    target_ref: &str,
    temp_ref: &str,
    where_clause: &str,
) -> String {
    format!(
        "BEGIN TRANSACTION;\n\
         DELETE FROM {target_ref} WHERE {where_clause};\n\
         INSERT INTO {target_ref} SELECT * FROM {temp_ref};\n\
         COMMIT TRANSACTION;"
    )
}

/// The typed `INSERT … SELECT FROM UNNEST(JSON_QUERY_ARRAY(@payload))` statement.
pub(crate) fn build_insert_select(
    columns: &[FieldSpec],
    project: &str,
    dataset: &str,
    table: &str,
) -> String {
    let col_list = columns
        .iter()
        .map(|f| quote_ident(&f.name))
        .collect::<Vec<_>>()
        .join(", ");
    let exprs = columns
        .iter()
        .map(|f| {
            let path = format!("${}", json_path_segment(&f.name));
            column_expr(f, "r", &path, 0)
        })
        .collect::<Vec<_>>()
        .join(",\n    ");
    format!(
        "INSERT INTO {t} ({col_list})\nSELECT\n    {exprs}\nFROM UNNEST(JSON_QUERY_ARRAY(@payload)) AS r",
        t = table_ref(project, dataset, table),
    )
}

/// The full atomic transaction: typed INSERT of the page + watermark MERGE.
pub fn build_transaction_sql(
    columns: &[FieldSpec],
    project: &str,
    dataset: &str,
    table: &str,
) -> String {
    format!(
        "BEGIN TRANSACTION;\n{insert};\n{merge};\nCOMMIT TRANSACTION;",
        insert = build_insert_select(columns, project, dataset, table),
        merge = build_merge_token(project, dataset),
    )
}

/// Deterministic, sanitized `requestId` for transport-retry dedup. Correctness
/// does not depend on it (the transaction + core skip logic are authoritative);
/// it just suppresses duplicate jobs from a retried HTTP request within
/// BigQuery's stateless-query window.
///
/// The scope hash uses FNV-1a rather than `std::hash::DefaultHasher` so the id
/// is byte-stable across Rust toolchains (`DefaultHasher`'s algorithm is
/// explicitly allowed to change between releases). The resulting id is bounded
/// at ~112 chars — well under BigQuery's 1024-char `requestId` limit.
pub fn build_request_id(scope: &str, token: &str) -> String {
    // FNV-1a 64-bit over the scope bytes.
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in scope.as_bytes() {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    let safe_scope: String = scope
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .take(64)
        .collect();
    format!("faucet_eo_{safe_scope}_{h:016x}_{token}")
}

// ---------------------------------------------------------------------------
// Schema introspection + evolution (issue #194)
// ---------------------------------------------------------------------------

use serde_json::{Map, Value, json};

/// JSON-Schema base type keyword for a [`BqType`], used by
/// [`fieldspecs_to_json_schema`] when reporting the sink's live schema for
/// drift detection. Types without a JSON-native scalar (`BYTES`, `TIMESTAMP`,
/// `DATE`, …) map to `"string"` — the shape they take in a JSON page.
fn bq_to_json_base(ty: &BqType) -> &'static str {
    match ty {
        BqType::Int64 => "integer",
        BqType::Float64 | BqType::Numeric | BqType::BigNumeric => "number",
        BqType::Bool => "boolean",
        BqType::Struct => "object",
        _ => "string",
    }
}

/// Convert the target table's [`FieldSpec`]s into an `infer_schema`-shaped JSON
/// Schema object (`{"type":"object","properties":{<col>: <fragment>, …}}`) so
/// the drift policy can diff a page against the live destination.
///
/// Every column is reported as nullable (`{"type":[base,"null"]}`): a
/// schema-only `tables.get` carries `mode` per field, but treating columns as
/// nullable is the safe default for drift — it never spuriously flags a page
/// that omits an optional column. A `REPEATED` field is reported as an
/// `array`.
pub fn fieldspecs_to_json_schema(fields: &[FieldSpec]) -> Value {
    let mut props = Map::new();
    for f in fields {
        let fragment = if f.repeated {
            json!({ "type": ["array", "null"] })
        } else {
            json!({ "type": [bq_to_json_base(&f.ty), "null"] })
        };
        props.insert(f.name.clone(), fragment);
    }
    json!({ "type": "object", "properties": Value::Object(props) })
}

/// Map a [`faucet_core::SqlBaseType`] (the type inferred for a drifted column)
/// to the BigQuery type keyword used in `ADD COLUMN` / `SET DATA TYPE` DDL.
///
/// Integers map to `INT64` and floats to `FLOAT64`; the drift engine only ever
/// widens integer→number, and BigQuery permits `INT64 → FLOAT64`.
pub fn base_to_bq(t: faucet_core::SqlBaseType) -> &'static str {
    use faucet_core::SqlBaseType::*;
    match t {
        Integer => "INT64",
        Double => "FLOAT64",
        Boolean => "BOOL",
        Text => "STRING",
        Json => "JSON",
    }
}

/// `CREATE SCHEMA IF NOT EXISTS `<project>`.`<dataset>`` — best-effort dataset
/// creation so `create_table` can target a dataset that does not exist yet.
///
/// Both identifiers are **escaped, never stripped** (#654 M15). Stripping a
/// backtick rewrites the id into a *different but perfectly valid* one, so the
/// DDL silently creates (or targets) the wrong dataset; escaping yields either
/// the intended identifier or a loud BigQuery parse error.
pub fn build_ensure_dataset_ddl(project: &str, dataset: &str) -> String {
    format!(
        "CREATE SCHEMA IF NOT EXISTS {}.{}",
        quote_ident(project),
        quote_ident(dataset)
    )
}

/// `ALTER TABLE <ref> ADD COLUMN IF NOT EXISTS `<col>` <bq_type>` — idempotent
/// column addition. `table_ref` is already backtick-quoted (`` `p.d.t` ``).
pub fn build_add_column_ddl(table_ref: &str, col: &str, bq_type: &str) -> String {
    format!(
        "ALTER TABLE {table_ref} ADD COLUMN IF NOT EXISTS {} {bq_type}",
        quote_ident(col)
    )
}

/// `ALTER TABLE <ref> ALTER COLUMN `<col>` SET DATA TYPE <bq_type>` — widen an
/// existing column's type. Naturally idempotent (re-running the same widening
/// is a no-op). BigQuery permits a lossless relaxation here (INT64→FLOAT64,
/// NUMERIC→BIGNUMERIC/FLOAT64).
pub fn build_alter_type_ddl(table_ref: &str, col: &str, bq_type: &str) -> String {
    format!(
        "ALTER TABLE {table_ref} ALTER COLUMN {} SET DATA TYPE {bq_type}",
        quote_ident(col)
    )
}

/// `ALTER TABLE <ref> ALTER COLUMN `<col>` DROP NOT NULL` — relax a REQUIRED
/// column to NULLABLE. Naturally idempotent.
pub fn build_drop_not_null_ddl(table_ref: &str, col: &str) -> String {
    format!(
        "ALTER TABLE {table_ref} ALTER COLUMN {} DROP NOT NULL",
        quote_ident(col)
    )
}

/// Map a single `infer_schema` field spec (a JSON-Schema fragment) to the
/// BigQuery column-type keyword used in a `CREATE TABLE` column list. Scalars
/// map precisely; objects and arrays become `JSON`; a null-only or untyped
/// field falls back to `STRING` (the shape it takes in a JSON page).
pub fn bq_column_type(field_spec: &serde_json::Value) -> &'static str {
    let token = match field_spec.get("type") {
        Some(serde_json::Value::String(s)) => Some(s.as_str()),
        Some(serde_json::Value::Array(a)) => {
            a.iter().filter_map(|v| v.as_str()).find(|s| *s != "null")
        }
        _ => None,
    };
    match token {
        Some("integer") => "INT64",
        Some("number") => "FLOAT64",
        Some("boolean") => "BOOL",
        Some("string") => match field_spec.get("format").and_then(|f| f.as_str()) {
            // RFC3339 timestamp / date strings (e.g. OData `Edm.DateTimeOffset` /
            // `Edm.Date`) get their real column type; BigQuery coerces the string
            // value into it on load.
            Some("date-time") => "TIMESTAMP",
            Some("date") => "DATE",
            _ => "STRING",
        },
        Some("object") | Some("array") => "JSON",
        _ => "STRING",
    }
}

/// Build a `CREATE OR REPLACE TABLE` DDL for `project.dataset.table` from a
/// schema inferred over `sample` records. Columns are emitted in a stable
/// (sorted) order so the DDL is deterministic. Returns `None` when no columns
/// can be inferred (an empty page, or records with no object fields) — the
/// caller must not attempt to create a zero-column table.
///
/// `CREATE OR REPLACE` (not `IF NOT EXISTS`) so it also fixes a **schemaless**
/// table (e.g. one created by a bare `bq mk` with no schema): `IF NOT EXISTS`
/// would no-op and leave it unusable. The caller only reaches this when the
/// target has no usable schema (missing or schemaless), so nothing typed is
/// dropped.
pub fn build_create_table_ddl(
    project: &str,
    dataset: &str,
    table: &str,
    sample: &[serde_json::Value],
) -> Option<String> {
    let schema = faucet_core::schema::infer_schema(sample);
    build_create_table_ddl_from_schema(project, dataset, table, &schema)
}

/// Like [`build_create_table_ddl`], but from an already-computed `infer_schema`-shaped
/// JSON Schema (`{"type":"object","properties":{…}}`) — used when the column types
/// are known authoritatively (e.g. from OData `$metadata`) rather than inferred
/// from a data sample. Returns `None` when no columns can be derived.
pub fn build_create_table_ddl_from_schema(
    project: &str,
    dataset: &str,
    table: &str,
    schema: &serde_json::Value,
) -> Option<String> {
    let props = schema.get("properties")?.as_object()?;
    if props.is_empty() {
        return None;
    }
    let mut names: Vec<&String> = props.keys().collect();
    names.sort();
    let cols: Vec<String> = names
        .iter()
        .map(|n| format!("{} {}", quote_ident(n), bq_column_type(&props[*n])))
        .collect();
    Some(format!(
        "CREATE OR REPLACE TABLE {} ({})",
        table_ref(project, dataset, table),
        cols.join(", ")
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scalar(name: &str, ty: BqType) -> FieldSpec {
        FieldSpec {
            name: name.into(),
            ty,
            repeated: false,
            fields: vec![],
        }
    }

    #[test]
    fn bq_column_type_maps_each_json_type() {
        use serde_json::json;
        assert_eq!(bq_column_type(&json!({"type": "integer"})), "INT64");
        assert_eq!(bq_column_type(&json!({"type": "number"})), "FLOAT64");
        assert_eq!(bq_column_type(&json!({"type": "boolean"})), "BOOL");
        assert_eq!(bq_column_type(&json!({"type": "string"})), "STRING");
        assert_eq!(bq_column_type(&json!({"type": "object"})), "JSON");
        assert_eq!(bq_column_type(&json!({"type": "array"})), "JSON");
    }

    #[test]
    fn bq_column_type_nullable_picks_non_null_and_unknown_falls_back_to_string() {
        use serde_json::json;
        // A nullable field is rendered by infer_schema as ["string","null"].
        assert_eq!(
            bq_column_type(&json!({"type": ["integer", "null"]})),
            "INT64"
        );
        assert_eq!(
            bq_column_type(&json!({"type": ["null", "string"]})),
            "STRING"
        );
        // Null-only or untyped → STRING.
        assert_eq!(bq_column_type(&json!({"type": "null"})), "STRING");
        assert_eq!(bq_column_type(&json!({})), "STRING");
    }

    #[test]
    fn build_create_table_ddl_from_sample() {
        use serde_json::json;
        let sample = vec![
            json!({"id": 1, "name": "a", "active": true, "score": 1.5, "meta": {"k": "v"}}),
            json!({"id": 2, "name": "b", "active": false, "score": 2.0, "tags": ["x"]}),
        ];
        let ddl = build_create_table_ddl("p", "d", "t", &sample).expect("ddl");
        assert!(
            ddl.starts_with("CREATE OR REPLACE TABLE `p.d.t` ("),
            "ddl: {ddl}"
        );
        // Columns are sorted; scalars typed precisely, object/array → JSON.
        assert!(ddl.contains("`active` BOOL"), "ddl: {ddl}");
        assert!(ddl.contains("`id` INT64"), "ddl: {ddl}");
        assert!(ddl.contains("`meta` JSON"), "ddl: {ddl}");
        assert!(ddl.contains("`name` STRING"), "ddl: {ddl}");
        assert!(ddl.contains("`score` FLOAT64"), "ddl: {ddl}");
        // `tags` is absent from the first record → nullable array → JSON.
        assert!(ddl.contains("`tags` JSON"), "ddl: {ddl}");
    }

    #[test]
    fn build_create_table_ddl_none_for_empty_or_columnless() {
        use serde_json::json;
        assert!(build_create_table_ddl("p", "d", "t", &[]).is_none());
        // Non-object records yield no properties.
        assert!(build_create_table_ddl("p", "d", "t", &[json!(1), json!("x")]).is_none());
    }

    #[test]
    fn field_type_aliases_collapse() {
        assert_eq!(BqType::from_field_type(&FieldType::Integer), BqType::Int64);
        assert_eq!(BqType::from_field_type(&FieldType::Int64), BqType::Int64);
        assert_eq!(BqType::from_field_type(&FieldType::Float), BqType::Float64);
        assert_eq!(BqType::from_field_type(&FieldType::Boolean), BqType::Bool);
        assert_eq!(BqType::from_field_type(&FieldType::Record), BqType::Struct);
        assert_eq!(BqType::from_field_type(&FieldType::Struct), BqType::Struct);
    }

    #[test]
    fn from_table_field_reads_mode_and_nested_fields() {
        let tf = TableFieldSchema {
            name: "addr".into(),
            r#type: FieldType::Record,
            mode: Some("REPEATED".into()),
            fields: Some(vec![TableFieldSchema::string("city")]),
            categories: None,
            description: None,
            policy_tags: None,
        };
        let fs = FieldSpec::from_table_field(&tf);
        assert_eq!(
            fs,
            FieldSpec {
                name: "addr".into(),
                ty: BqType::Struct,
                repeated: true,
                fields: vec![scalar("city", BqType::String)],
            }
        );
    }

    fn repeated(name: &str, ty: BqType) -> FieldSpec {
        FieldSpec {
            name: name.into(),
            ty,
            repeated: true,
            fields: vec![],
        }
    }
    fn record(name: &str, repeated: bool, fields: Vec<FieldSpec>) -> FieldSpec {
        FieldSpec {
            name: name.into(),
            ty: BqType::Struct,
            repeated,
            fields,
        }
    }

    #[test]
    fn scalar_exprs_per_type() {
        assert_eq!(
            column_expr(&scalar("s", BqType::String), "r", "$.s", 0),
            "JSON_VALUE(r, '$.s')"
        );
        assert_eq!(
            column_expr(&scalar("n", BqType::Int64), "r", "$.n", 0),
            "CAST(JSON_VALUE(r, '$.n') AS INT64)"
        );
        assert_eq!(
            column_expr(&scalar("f", BqType::Float64), "r", "$.f", 0),
            "CAST(JSON_VALUE(r, '$.f') AS FLOAT64)"
        );
        assert_eq!(
            column_expr(&scalar("b", BqType::Bool), "r", "$.b", 0),
            "CAST(JSON_VALUE(r, '$.b') AS BOOL)"
        );
        assert_eq!(
            column_expr(&scalar("ts", BqType::Timestamp), "r", "$.ts", 0),
            "CAST(JSON_VALUE(r, '$.ts') AS TIMESTAMP)"
        );
        assert_eq!(
            column_expr(&scalar("by", BqType::Bytes), "r", "$.by", 0),
            "FROM_BASE64(JSON_VALUE(r, '$.by'))"
        );
        assert_eq!(
            column_expr(&scalar("g", BqType::Geography), "r", "$.g", 0),
            "ST_GEOGFROMTEXT(JSON_VALUE(r, '$.g'))"
        );
        assert_eq!(
            column_expr(&scalar("j", BqType::Json), "r", "$.j", 0),
            "PARSE_JSON(JSON_QUERY(r, '$.j'))"
        );
    }

    #[test]
    fn repeated_scalar_exprs() {
        assert_eq!(
            column_expr(&repeated("xs", BqType::String), "r", "$.xs", 0),
            "ARRAY(SELECT x0 FROM UNNEST(JSON_VALUE_ARRAY(r, '$.xs')) AS x0)"
        );
        assert_eq!(
            column_expr(&repeated("ns", BqType::Int64), "r", "$.ns", 0),
            "ARRAY(SELECT CAST(x0 AS INT64) FROM UNNEST(JSON_VALUE_ARRAY(r, '$.ns')) AS x0)"
        );
        assert_eq!(
            column_expr(&repeated("js", BqType::Json), "r", "$.js", 0),
            "ARRAY(SELECT PARSE_JSON(x0) FROM UNNEST(JSON_QUERY_ARRAY(r, '$.js')) AS x0)"
        );
    }

    #[test]
    fn nested_struct_expr() {
        let f = record(
            "addr",
            false,
            vec![scalar("city", BqType::String), scalar("zip", BqType::Int64)],
        );
        assert_eq!(
            column_expr(&f, "r", "$.addr", 0),
            "STRUCT(JSON_VALUE(r, '$.addr.city') AS `city`, CAST(JSON_VALUE(r, '$.addr.zip') AS INT64) AS `zip`)"
        );
    }

    #[test]
    fn repeated_record_expr_uses_unnest_element() {
        let f = record(
            "items",
            true,
            vec![scalar("sku", BqType::String), scalar("qty", BqType::Int64)],
        );
        assert_eq!(
            column_expr(&f, "r", "$.items", 0),
            "ARRAY(SELECT AS STRUCT JSON_VALUE(e0, '$.sku') AS `sku`, CAST(JSON_VALUE(e0, '$.qty') AS INT64) AS `qty` FROM UNNEST(JSON_QUERY_ARRAY(r, '$.items')) AS e0)"
        );
    }

    #[test]
    fn nested_repeated_record_aliases_are_unique() {
        // ARRAY<STRUCT<tags ARRAY<STRING>>> nested inside ARRAY<STRUCT<...>>
        let inner = repeated("tags", BqType::String);
        let f = record("groups", true, vec![inner]);
        let sql = column_expr(&f, "r", "$.groups", 0);
        // Outer element alias e0; the inner repeated scalar uses x1 (depth+1) —
        // distinct from any outer alias.
        assert_eq!(
            sql,
            "ARRAY(SELECT AS STRUCT ARRAY(SELECT x1 FROM UNNEST(JSON_VALUE_ARRAY(e0, '$.tags')) AS x1) AS `tags` FROM UNNEST(JSON_QUERY_ARRAY(r, '$.groups')) AS e0)"
        );
    }

    #[test]
    fn unsafe_member_name_uses_bracket_path() {
        // A name with a dot would be ambiguous in `$.a.b`; bracket-quote it.
        assert_eq!(json_path_segment("a.b"), "['a.b']");
        assert_eq!(json_path_segment("ok_name"), ".ok_name");
        assert_eq!(json_path_segment("_lead"), "._lead");
        assert_eq!(json_path_segment("1bad"), "['1bad']");
    }

    #[test]
    fn create_commit_table_sql() {
        assert_eq!(
            build_create_commit_table("p", "d"),
            "CREATE TABLE IF NOT EXISTS `p.d._faucet_commit_token` (scope STRING NOT NULL, token STRING NOT NULL, updated_at TIMESTAMP)"
        );
    }

    #[test]
    fn overwrite_commit_sql_swaps_in_a_transaction() {
        let target = table_ref("p", "d", "t");
        let temp = table_ref("p", "d", "t__faucet_ovw");
        let sql = build_overwrite_commit_sql(&target, &temp);
        assert!(sql.starts_with("BEGIN TRANSACTION;"));
        assert!(sql.contains("TRUNCATE TABLE `p.d.t`;"));
        assert!(sql.contains("INSERT INTO `p.d.t` SELECT * FROM `p.d.t__faucet_ovw`;"));
        assert!(sql.trim_end().ends_with("COMMIT TRANSACTION;"));
    }

    #[test]
    fn scoped_overwrite_commit_sql_deletes_in_scope_then_inserts() {
        let target = table_ref("p", "d", "t");
        let temp = table_ref("p", "d", "t__faucet_ovw");
        let sql = build_scoped_overwrite_commit_sql(
            &target,
            &temp,
            "`posting_date` >= '2024-06-01' AND `posting_date` < '2024-07-01'",
        );
        assert!(sql.starts_with("BEGIN TRANSACTION;"));
        assert!(sql.contains(
            "DELETE FROM `p.d.t` WHERE `posting_date` >= '2024-06-01' AND `posting_date` < '2024-07-01';"
        ));
        assert!(sql.contains("INSERT INTO `p.d.t` SELECT * FROM `p.d.t__faucet_ovw`;"));
        assert!(!sql.contains("TRUNCATE"));
        assert!(sql.trim_end().ends_with("COMMIT TRANSACTION;"));
    }

    #[test]
    fn select_token_sql() {
        assert_eq!(
            build_select_token("p", "d"),
            "SELECT token FROM `p.d._faucet_commit_token` WHERE scope = @scope LIMIT 1"
        );
    }

    #[test]
    fn merge_token_sql() {
        assert_eq!(
            build_merge_token("p", "d"),
            "MERGE `p.d._faucet_commit_token` T USING (SELECT @scope AS scope, @token AS token) S ON T.scope = S.scope WHEN MATCHED THEN UPDATE SET token = S.token, updated_at = CURRENT_TIMESTAMP() WHEN NOT MATCHED THEN INSERT (scope, token, updated_at) VALUES (S.scope, S.token, CURRENT_TIMESTAMP())"
        );
    }

    #[test]
    fn transaction_sql_wraps_insert_and_merge() {
        let cols = vec![scalar("id", BqType::Int64), scalar("name", BqType::String)];
        let sql = build_transaction_sql(&cols, "p", "d", "t");
        assert!(sql.starts_with("BEGIN TRANSACTION;\n"), "got: {sql}");
        assert!(
            sql.contains("INSERT INTO `p.d.t` (`id`, `name`)"),
            "got: {sql}"
        );
        assert!(
            sql.contains("FROM UNNEST(JSON_QUERY_ARRAY(@payload)) AS r"),
            "got: {sql}"
        );
        assert!(
            sql.contains("MERGE `p.d._faucet_commit_token` T"),
            "got: {sql}"
        );
        assert!(
            sql.trim_end().ends_with("COMMIT TRANSACTION;"),
            "got: {sql}"
        );
        let i = sql.find("INSERT INTO").unwrap();
        let m = sql.find("MERGE").unwrap();
        let c = sql.find("COMMIT TRANSACTION").unwrap();
        assert!(i < m && m < c, "statement order wrong: {sql}");
    }

    #[test]
    fn request_id_is_deterministic_and_sanitized() {
        let a = build_request_id("pipe::row1", "00000000000000000007");
        let b = build_request_id("pipe::row1", "00000000000000000007");
        assert_eq!(a, b, "must be deterministic across calls/processes");
        assert!(
            a.chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-'),
            "request_id must be sanitized: {a}"
        );
        assert!(a.ends_with("_00000000000000000007"), "got: {a}");
        assert_ne!(a, build_request_id("pipe::row2", "00000000000000000007"));
    }

    #[test]
    fn sql_str_escapes_backslash_then_quote() {
        // The replace order is load-bearing: backslash first, then single-quote.
        assert_eq!(sql_str("a'b"), "'a\\'b'");
        assert_eq!(sql_str("a\\b"), "'a\\\\b'");
        assert_eq!(sql_str("$.ok"), "'$.ok'");
    }

    // --- schema introspection + evolution (issue #194) ---

    #[test]
    fn fieldspec_to_json_schema_maps_types_and_nullability() {
        let fields = vec![
            scalar("id", BqType::Int64),
            scalar("score", BqType::Float64),
            scalar("amount", BqType::Numeric),
            scalar("flag", BqType::Bool),
            scalar("name", BqType::String),
            scalar("ts", BqType::Timestamp),
            repeated("tags", BqType::String),
            record("addr", false, vec![scalar("city", BqType::String)]),
        ];
        let js = fieldspecs_to_json_schema(&fields);
        assert_eq!(js["type"], "object");
        let p = &js["properties"];
        // Numeric collapses to JSON `number`; non-JSON-native types → `string`.
        assert_eq!(p["id"]["type"], json!(["integer", "null"]));
        assert_eq!(p["score"]["type"], json!(["number", "null"]));
        assert_eq!(p["amount"]["type"], json!(["number", "null"]));
        assert_eq!(p["flag"]["type"], json!(["boolean", "null"]));
        assert_eq!(p["name"]["type"], json!(["string", "null"]));
        assert_eq!(p["ts"]["type"], json!(["string", "null"]));
        // A repeated field is an array regardless of its element type.
        assert_eq!(p["tags"]["type"], json!(["array", "null"]));
        // A non-repeated struct is an object.
        assert_eq!(p["addr"]["type"], json!(["object", "null"]));
    }

    #[test]
    fn fieldspec_to_json_schema_empty_fields() {
        let js = fieldspecs_to_json_schema(&[]);
        assert_eq!(js, json!({ "type": "object", "properties": {} }));
    }

    #[test]
    fn json_schema_to_bq_type_per_base() {
        use faucet_core::SqlBaseType::*;
        assert_eq!(base_to_bq(Integer), "INT64");
        assert_eq!(base_to_bq(Double), "FLOAT64");
        assert_eq!(base_to_bq(Boolean), "BOOL");
        assert_eq!(base_to_bq(Text), "STRING");
        assert_eq!(base_to_bq(Json), "JSON");
    }

    #[test]
    fn add_column_ddl_is_idempotent_and_quoted() {
        assert_eq!(
            build_add_column_ddl("`p.d.t`", "email", "STRING"),
            "ALTER TABLE `p.d.t` ADD COLUMN IF NOT EXISTS `email` STRING"
        );
    }

    #[test]
    fn alter_type_ddl_sets_data_type() {
        assert_eq!(
            build_alter_type_ddl("`p.d.t`", "score", "FLOAT64"),
            "ALTER TABLE `p.d.t` ALTER COLUMN `score` SET DATA TYPE FLOAT64"
        );
    }

    #[test]
    fn drop_not_null_ddl() {
        assert_eq!(
            build_drop_not_null_ddl("`p.d.t`", "created_at"),
            "ALTER TABLE `p.d.t` ALTER COLUMN `created_at` DROP NOT NULL"
        );
    }

    #[test]
    fn quote_ident_escapes_rather_than_silently_rewriting() {
        use super::quote_ident;
        // Ordinary identifiers are untouched apart from the quotes.
        assert_eq!(quote_ident("order_id"), "`order_id`");
        assert_eq!(quote_ident("weird name"), "`weird name`");
        // A backtick in a USER-supplied key column must not be dropped: the old
        // behaviour turned `a`b` into `ab`, so the MERGE predicate matched a
        // different, real column — silently writing to the wrong place.
        assert_eq!(quote_ident("a`b"), "`a\\`b`");
        // Backslashes escape first, so the escape itself cannot be forged.
        assert_eq!(quote_ident("a\\b"), "`a\\\\b`");
        assert_eq!(quote_ident("a\\`b"), "`a\\\\\\`b`");
    }

    /// The dataset DDL was the last site still *stripping* backticks (#654
    /// M15). Stripping is the dangerous half: `` my`ds `` became `myds`, a
    /// different but perfectly valid dataset, so the CREATE silently landed
    /// somewhere else. Escaping yields the intended name or a parse error.
    #[test]
    fn ensure_dataset_ddl_escapes_both_identifiers() {
        assert_eq!(
            build_ensure_dataset_ddl("proj", "ds"),
            "CREATE SCHEMA IF NOT EXISTS `proj`.`ds`"
        );
        assert_eq!(
            build_ensure_dataset_ddl("pr`oj", "d`s"),
            "CREATE SCHEMA IF NOT EXISTS `pr\\`oj`.`d\\`s`"
        );
        // The separating dot stays outside the quotes, so a dotted *value*
        // cannot smuggle in an extra qualifier.
        assert_eq!(
            build_ensure_dataset_ddl("a.b", "c"),
            "CREATE SCHEMA IF NOT EXISTS `a.b`.`c`"
        );
    }
}
