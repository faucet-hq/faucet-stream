//! Pure SQL generation for the Databricks sink — no I/O.
//!
//! Every value crosses the wire as a **string cell** (or SQL `NULL`) and is
//! cast to the destination column's declared type on the server, whether the
//! rows arrive inline (`FROM VALUES`) or from a staged all-string Parquet file
//! (`read_files` / `COPY INTO`). One rule for both paths means the insert path
//! and the staged path can never disagree about how a value lands.

use std::ops::Range;

use faucet_core::drift::{SchemaEvolution, SqlBaseType, adds_null, base_widened};
use faucet_core::idempotency::{
    COMMIT_TOKEN_SCOPE_COL, COMMIT_TOKEN_TABLE, COMMIT_TOKEN_TOKEN_COL,
};
use faucet_core::{FaucetError, PlannedColumn, json_schema_base_type};
use serde_json::{Map, Value, json};

/// Exactly-once append: the pipeline scope a row was written under.
pub const EO_SCOPE_COL: &str = "_faucet_scope";
/// Exactly-once append: the page sequence a row was written under.
pub const EO_SEQ_COL: &str = "_faucet_seq";
/// Merge source column carrying `'u'` (upsert) or `'d'` (delete).
pub const OP_COL: &str = "__faucet_op";

const UPDATED_AT_COL: &str = "updated_at";

/// A column of the destination table, as `information_schema.columns` reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableColumn {
    /// Column name.
    pub name: String,
    /// `full_data_type`, e.g. `bigint`, `decimal(10,2)`, `struct<a:int>`.
    pub full_type: String,
    /// Whether the column accepts NULL.
    pub nullable: bool,
}

impl TableColumn {
    pub fn new(name: impl Into<String>, full_type: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            full_type: full_type.into(),
            nullable: true,
        }
    }

    fn is_eo(&self) -> bool {
        self.name.eq_ignore_ascii_case(EO_SCOPE_COL) || self.name.eq_ignore_ascii_case(EO_SEQ_COL)
    }
}

/// `catalog.schema.table`, quoted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableRef {
    pub catalog: Option<String>,
    pub schema: String,
    pub table: String,
}

impl TableRef {
    pub fn new(catalog: Option<&str>, schema: &str, table: &str) -> Self {
        Self {
            catalog: catalog.map(str::to_owned),
            schema: schema.to_owned(),
            table: table.to_owned(),
        }
    }

    /// Another table in the same schema.
    pub fn sibling(&self, table: &str) -> Self {
        Self {
            table: table.to_owned(),
            ..self.clone()
        }
    }

    /// The quoted, qualified name.
    pub fn sql(&self) -> String {
        match &self.catalog {
            Some(c) => format!(
                "{}.{}.{}",
                quote_ident(c),
                quote_ident(&self.schema),
                quote_ident(&self.table)
            ),
            None => format!("{}.{}", quote_ident(&self.schema), quote_ident(&self.table)),
        }
    }

    /// Unquoted dotted name, for messages and dataset URIs.
    pub fn display(&self) -> String {
        match &self.catalog {
            Some(c) => format!("{c}.{}.{}", self.schema, self.table),
            None => format!("{}.{}", self.schema, self.table),
        }
    }
}

/// Backtick-quote an identifier (a backtick inside is doubled).
pub fn quote_ident(s: &str) -> String {
    format!("`{}`", s.replace('`', "``"))
}

/// A Databricks SQL string literal. Databricks processes backslash escapes
/// inside literals (and treats `''` as two adjacent literals, not an escaped
/// quote), so both `\` and `'` are backslash-escaped.
pub fn string_literal(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for ch in s.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '\'' => out.push_str("\\'"),
            c => out.push(c),
        }
    }
    out.push('\'');
    out
}

/// The string cell a JSON value travels as (`None` = SQL NULL). Nested values
/// travel as JSON text.
pub fn cell_text(v: &Value) -> Option<String> {
    match v {
        Value::Null => None,
        Value::String(s) => Some(s.clone()),
        Value::Bool(b) => Some(b.to_string()),
        Value::Number(n) => Some(n.to_string()),
        other => Some(other.to_string()),
    }
}

fn cell_literal(c: &Option<String>) -> String {
    match c {
        None => "NULL".to_owned(),
        Some(s) => string_literal(s),
    }
}

/// Convert a string expression to the column's declared type.
pub fn cast_expr(expr: &str, full_type: &str) -> String {
    let t = full_type.trim().to_ascii_lowercase();
    if t == "string" {
        expr.to_owned()
    } else if t.starts_with("struct") || t.starts_with("array") || t.starts_with("map") {
        format!("from_json({expr}, {})", string_literal(full_type.trim()))
    } else if t == "variant" {
        format!("parse_json({expr})")
    } else {
        format!("CAST({expr} AS {})", full_type.trim())
    }
}

/// Databricks type keyword for a planned / evolved column.
pub fn base_type_sql(t: SqlBaseType) -> &'static str {
    match t {
        SqlBaseType::Integer => "BIGINT",
        SqlBaseType::Double => "DOUBLE",
        SqlBaseType::Boolean => "BOOLEAN",
        SqlBaseType::Text | SqlBaseType::Json => "STRING",
    }
}

/// `CREATE TABLE IF NOT EXISTS … USING DELTA` from planned columns, plus the
/// exactly-once bookkeeping columns when `eo`.
pub fn create_table_sql(table: &TableRef, cols: &[PlannedColumn], eo: bool) -> String {
    let mut defs: Vec<String> = cols
        .iter()
        .map(|c| format!("{} {}", quote_ident(&c.name), base_type_sql(c.base_type)))
        .collect();
    if eo {
        defs.push(format!("{} STRING", quote_ident(EO_SCOPE_COL)));
        defs.push(format!("{} BIGINT", quote_ident(EO_SEQ_COL)));
    }
    format!(
        "CREATE TABLE IF NOT EXISTS {} ({}) USING DELTA",
        table.sql(),
        defs.join(", ")
    )
}

/// The table columns a freshly created table has (what [`create_table_sql`] made).
pub fn planned_table_columns(cols: &[PlannedColumn], eo: bool) -> Vec<TableColumn> {
    let mut out: Vec<TableColumn> = cols
        .iter()
        .map(|c| TableColumn::new(&c.name, base_type_sql(c.base_type).to_ascii_lowercase()))
        .collect();
    if eo {
        out.extend(eo_columns());
    }
    out
}

fn eo_columns() -> [TableColumn; 2] {
    [
        TableColumn::new(EO_SCOPE_COL, "string"),
        TableColumn::new(EO_SEQ_COL, "bigint"),
    ]
}

/// The exactly-once bookkeeping columns missing from `cols`.
pub fn missing_eo_columns(cols: &[TableColumn]) -> Vec<TableColumn> {
    eo_columns()
        .into_iter()
        .filter(|e| !cols.iter().any(|c| c.name.eq_ignore_ascii_case(&e.name)))
        .collect()
}

/// `ALTER TABLE … ADD COLUMNS (…)`.
pub fn add_columns_sql(table: &TableRef, cols: &[TableColumn]) -> String {
    format!(
        "ALTER TABLE {} ADD COLUMNS ({})",
        table.sql(),
        cols.iter()
            .map(|c| format!(
                "{} {}",
                quote_ident(&c.name),
                c.full_type.to_ascii_uppercase()
            ))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

/// Column listing for a table (binds `:faucet_schema` and `:faucet_table`).
pub fn describe_sql(catalog: Option<&str>) -> String {
    let src = match catalog {
        Some(c) => format!("{}.information_schema.columns", quote_ident(c)),
        None => "information_schema.columns".to_owned(),
    };
    format!(
        "SELECT column_name, full_data_type, is_nullable FROM {src} \
         WHERE lower(table_schema) = lower(:faucet_schema) AND lower(table_name) = lower(:faucet_table) \
         ORDER BY ordinal_position"
    )
}

/// Decode [`describe_sql`] rows.
pub fn columns_from_rows(rows: Vec<Vec<Option<String>>>) -> Vec<TableColumn> {
    rows.into_iter()
        .filter_map(|r| {
            let mut it = r.into_iter();
            let name = it.next().flatten()?;
            let full_type = it.next().flatten().unwrap_or_else(|| "string".into());
            let nullable = !matches!(it.next().flatten().as_deref(), Some("NO"));
            Some(TableColumn {
                name,
                full_type,
                nullable,
            })
        })
        .collect()
}

/// A JSON-Schema fragment for a column (the drift policy's view).
pub fn json_fragment(col: &TableColumn) -> Value {
    let t = col.full_type.trim().to_ascii_lowercase();
    let base = match t.as_str() {
        "tinyint" | "smallint" | "int" | "integer" | "bigint" | "long" | "byte" | "short" => {
            "integer"
        }
        "float" | "double" | "real" => "number",
        "boolean" => "boolean",
        _ if t.starts_with("decimal") => "number",
        _ if t.starts_with("struct") || t.starts_with("map") || t == "variant" => "object",
        _ if t.starts_with("array") => "array",
        _ => "string",
    };
    if col.nullable {
        json!({ "type": [base, "null"] })
    } else {
        json!({ "type": base })
    }
}

/// The `infer_schema`-shaped destination schema; the exactly-once bookkeeping
/// columns are hidden.
pub fn schema_from_columns(cols: &[TableColumn]) -> Value {
    let mut props = Map::new();
    for c in cols.iter().filter(|c| !c.is_eo()) {
        props.insert(c.name.clone(), json_fragment(c));
    }
    json!({ "type": "object", "properties": props })
}

/// A page projected onto the destination's columns: the table columns the
/// page carries (table order) and one string cell per column per row.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Matrix {
    pub columns: Vec<TableColumn>,
    pub rows: Vec<Vec<Option<String>>>,
}

impl Matrix {
    /// Estimated SQL-literal bytes of each row.
    pub fn row_sizes(&self) -> Vec<usize> {
        self.rows
            .iter()
            .map(|r| {
                r.iter()
                    .map(|c| c.as_ref().map_or(4, |s| s.len() + 4))
                    .sum::<usize>()
                    + 4
            })
            .collect()
    }

    /// Estimated size of the whole page.
    pub fn estimated_bytes(&self) -> usize {
        self.row_sizes().iter().sum()
    }

    /// Append a trailing column (e.g. the merge op) with a cell per row.
    pub fn push_column(&mut self, col: TableColumn, cells: Vec<Option<String>>) {
        self.columns.push(col);
        for (row, cell) in self.rows.iter_mut().zip(cells) {
            row.push(cell);
        }
    }
}

/// Project `records` onto `table` (matched case-insensitively). Keys the table
/// does not have are reported, never dropped.
pub fn build_matrix(records: &[Value], table: &[TableColumn]) -> Result<Matrix, FaucetError> {
    let mut present = vec![false; table.len()];
    let mut unknown: Vec<String> = Vec::new();
    for r in records {
        let obj = r.as_object().ok_or_else(|| {
            FaucetError::Sink("databricks sink: records must be JSON objects".into())
        })?;
        for k in obj.keys() {
            match table.iter().position(|c| c.name.eq_ignore_ascii_case(k)) {
                Some(i) => present[i] = true,
                None => {
                    if !unknown.contains(k) {
                        unknown.push(k.clone());
                    }
                }
            }
        }
    }
    if !unknown.is_empty() {
        unknown.sort();
        return Err(FaucetError::Sink(format!(
            "databricks sink: column(s) {} are not in the destination table; add them \
             (e.g. `schema: {{ on_drift: evolve }}`) or drop them (`on_drift: ignore`)",
            unknown.join(", ")
        )));
    }
    let columns: Vec<TableColumn> = table
        .iter()
        .zip(&present)
        .filter(|(_, p)| **p)
        .map(|(c, _)| c.clone())
        .collect();
    let rows = records
        .iter()
        .map(|r| {
            let obj = r.as_object().expect("checked above");
            columns
                .iter()
                .map(|c| {
                    obj.iter()
                        .find(|(k, _)| k.eq_ignore_ascii_case(&c.name))
                        .and_then(|(_, v)| cell_text(v))
                })
                .collect()
        })
        .collect();
    Ok(Matrix { columns, rows })
}

/// Where a statement reads the page's string cells from.
#[derive(Debug, Clone, Copy)]
pub enum Relation<'a> {
    /// Inline `VALUES` rows (columns `v.c0 … v.cN`).
    Values(&'a [Vec<Option<String>>]),
    /// A staged all-string Parquet file (columns named like the table's).
    File(&'a str),
}

impl Relation<'_> {
    fn col(&self, i: usize, name: &str) -> String {
        match self {
            Relation::Values(_) => format!("v.c{i}"),
            Relation::File(_) => quote_ident(name),
        }
    }

    fn from(&self, width: usize) -> String {
        match self {
            Relation::Values(rows) => {
                let body = rows
                    .iter()
                    .map(|r| {
                        format!(
                            "({})",
                            r.iter().map(cell_literal).collect::<Vec<_>>().join(", ")
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                let names = (0..width)
                    .map(|i| format!("c{i}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("VALUES {body} AS v({names})")
            }
            Relation::File(uri) => {
                format!("read_files({}, format => 'parquet')", string_literal(uri))
            }
        }
    }
}

fn projection(cols: &[TableColumn], rel: Relation<'_>) -> Vec<String> {
    cols.iter()
        .enumerate()
        .map(|(i, c)| {
            format!(
                "{} AS {}",
                cast_expr(&rel.col(i, &c.name), &c.full_type),
                quote_ident(&c.name)
            )
        })
        .collect()
}

fn col_list(cols: &[TableColumn]) -> String {
    cols.iter()
        .map(|c| quote_ident(&c.name))
        .collect::<Vec<_>>()
        .join(", ")
}

/// `INSERT INTO t (cols) SELECT <casts> FROM <rel>`; `eo` appends the scope /
/// sequence literals.
pub fn insert_sql(
    table: &TableRef,
    cols: &[TableColumn],
    rel: Relation<'_>,
    eo: Option<(&str, u64)>,
) -> String {
    let mut names = col_list(cols);
    let mut proj = projection(cols, rel);
    if let Some((scope, seq)) = eo {
        names.push_str(&format!(
            ", {}, {}",
            quote_ident(EO_SCOPE_COL),
            quote_ident(EO_SEQ_COL)
        ));
        proj.push(string_literal(scope));
        proj.push(seq.to_string());
    }
    format!(
        "INSERT INTO {} ({names}) SELECT {} FROM {}",
        table.sql(),
        proj.join(", "),
        rel.from(cols.len())
    )
}

fn eo_predicate(scope: &str, seq: u64) -> String {
    format!(
        "{} = {} AND {} >= {seq}",
        quote_ident(EO_SCOPE_COL),
        string_literal(scope),
        quote_ident(EO_SEQ_COL)
    )
}

/// The exactly-once append write: one atomic Delta commit that removes
/// whatever an earlier attempt of this page (or any later one) left under
/// `scope` and inserts the page. `table_cols` is the whole table in order,
/// because `REPLACE WHERE` takes no column list.
pub fn replace_where_sql(
    table: &TableRef,
    table_cols: &[TableColumn],
    page_cols: &[TableColumn],
    rel: Relation<'_>,
    scope: &str,
    seq: u64,
) -> String {
    let proj: Vec<String> = table_cols
        .iter()
        .map(|tc| {
            if tc.name.eq_ignore_ascii_case(EO_SCOPE_COL) {
                format!("{} AS {}", string_literal(scope), quote_ident(&tc.name))
            } else if tc.name.eq_ignore_ascii_case(EO_SEQ_COL) {
                format!("CAST({seq} AS BIGINT) AS {}", quote_ident(&tc.name))
            } else if let Some(i) = page_cols
                .iter()
                .position(|p| p.name.eq_ignore_ascii_case(&tc.name))
            {
                format!(
                    "{} AS {}",
                    cast_expr(&rel.col(i, &page_cols[i].name), &tc.full_type),
                    quote_ident(&tc.name)
                )
            } else {
                format!(
                    "CAST(NULL AS {}) AS {}",
                    tc.full_type,
                    quote_ident(&tc.name)
                )
            }
        })
        .collect();
    format!(
        "INSERT INTO {} REPLACE WHERE {} SELECT {} FROM {}",
        table.sql(),
        eo_predicate(scope, seq),
        proj.join(", "),
        rel.from(page_cols.len())
    )
}

/// Exactly-once empty page: drop anything an earlier attempt left.
pub fn delete_eo_sql(table: &TableRef, scope: &str, seq: u64) -> String {
    format!(
        "DELETE FROM {} WHERE {}",
        table.sql(),
        eo_predicate(scope, seq)
    )
}

/// One `MERGE` applying upserts and deletes. `cols` are the merge columns
/// (key included); the relation's last column is [`OP_COL`].
pub fn merge_sql(
    table: &TableRef,
    cols: &[TableColumn],
    key: &[String],
    rel: Relation<'_>,
) -> String {
    let mut proj = projection(cols, rel);
    proj.push(format!(
        "{} AS {}",
        rel.col(cols.len(), OP_COL),
        quote_ident(OP_COL)
    ));
    let is_key = |name: &str| key.iter().any(|k| k.eq_ignore_ascii_case(name));
    let on = key
        .iter()
        .map(|k| format!("t.{0} = s.{0}", quote_ident(k)))
        .collect::<Vec<_>>()
        .join(" AND ");
    let set: Vec<String> = cols
        .iter()
        .filter(|c| !is_key(&c.name))
        .map(|c| format!("t.{0} = s.{0}", quote_ident(&c.name)))
        .collect();
    let op = quote_ident(OP_COL);
    let update = if set.is_empty() {
        String::new()
    } else {
        format!(" WHEN MATCHED THEN UPDATE SET {}", set.join(", "))
    };
    format!(
        "MERGE INTO {t} AS t USING (SELECT {proj} FROM {from}) AS s ON {on} \
         WHEN MATCHED AND s.{op} = 'd' THEN DELETE{update} \
         WHEN NOT MATCHED AND s.{op} = 'u' THEN INSERT ({names}) VALUES ({vals})",
        t = table.sql(),
        proj = proj.join(", "),
        from = rel.from(cols.len() + 1),
        names = col_list(cols),
        vals = cols
            .iter()
            .map(|c| format!("s.{}", quote_ident(&c.name)))
            .collect::<Vec<_>>()
            .join(", "),
    )
}

/// `COPY INTO` one staged file. `COPY INTO` remembers every file it loaded
/// into a table and skips it on a repeat, so re-running this after an
/// ambiguous failure cannot load the file twice.
pub fn copy_into_sql(
    table: &TableRef,
    cols: &[TableColumn],
    dir_uri: &str,
    file_name: &str,
    copy_options: Option<&str>,
) -> String {
    let proj = projection(cols, Relation::File(""));
    let opts = match copy_options.map(str::trim).filter(|s| !s.is_empty()) {
        Some(o) => format!(" COPY_OPTIONS ({o})"),
        None => String::new(),
    };
    format!(
        "COPY INTO {} FROM (SELECT {} FROM {}) FILEFORMAT = PARQUET FILES = ({}){opts}",
        table.sql(),
        proj.join(", "),
        string_literal(dir_uri),
        string_literal(file_name)
    )
}

pub fn drop_table_sql(table: &TableRef) -> String {
    format!("DROP TABLE IF EXISTS {}", table.sql())
}

pub fn create_like_sql(staging: &TableRef, target: &TableRef) -> String {
    format!("CREATE TABLE {} LIKE {}", staging.sql(), target.sql())
}

/// The overwrite swap: one Delta commit replaces the target's contents.
pub fn insert_overwrite_sql(target: &TableRef, staging: &TableRef) -> String {
    format!(
        "INSERT OVERWRITE TABLE {} SELECT * FROM {}",
        target.sql(),
        staging.sql()
    )
}

pub fn rename_sql(from: &TableRef, to: &TableRef) -> String {
    format!("ALTER TABLE {} RENAME TO {}", from.sql(), to.sql())
}

/// The watermark table, a sibling of the target.
pub fn token_table(target: &TableRef) -> TableRef {
    target.sibling(COMMIT_TOKEN_TABLE)
}

pub fn token_table_ddl(target: &TableRef) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {} ({} STRING NOT NULL, {} STRING NOT NULL, {} TIMESTAMP) USING DELTA",
        token_table(target).sql(),
        quote_ident(COMMIT_TOKEN_SCOPE_COL),
        quote_ident(COMMIT_TOKEN_TOKEN_COL),
        quote_ident(UPDATED_AT_COL),
    )
}

/// Binds `:scope`.
pub fn token_select_sql(target: &TableRef) -> String {
    format!(
        "SELECT {} FROM {} WHERE {} = :scope LIMIT 1",
        quote_ident(COMMIT_TOKEN_TOKEN_COL),
        token_table(target).sql(),
        quote_ident(COMMIT_TOKEN_SCOPE_COL)
    )
}

/// Upsert the one watermark row per scope. Binds `:scope` and `:token`.
pub fn token_merge_sql(target: &TableRef) -> String {
    let scope = quote_ident(COMMIT_TOKEN_SCOPE_COL);
    let token = quote_ident(COMMIT_TOKEN_TOKEN_COL);
    let at = quote_ident(UPDATED_AT_COL);
    format!(
        "MERGE INTO {t} AS t USING (SELECT :scope AS {scope}, :token AS {token}) AS s \
         ON t.{scope} = s.{scope} \
         WHEN MATCHED THEN UPDATE SET t.{token} = s.{token}, t.{at} = current_timestamp() \
         WHEN NOT MATCHED THEN INSERT ({scope}, {token}, {at}) VALUES (s.{scope}, s.{token}, current_timestamp())",
        t = token_table(target).sql(),
    )
}

/// Delete the watermark row for a scope. Binds `:scope`.
pub fn token_delete_sql(target: &TableRef) -> String {
    format!(
        "DELETE FROM {} WHERE {} = :scope",
        token_table(target).sql(),
        quote_ident(COMMIT_TOKEN_SCOPE_COL)
    )
}

/// Split rows into statement-sized ranges: at most `batch` rows (`0` = no row
/// limit) and at most `max_bytes` of literals (`overhead` = fixed statement
/// text). A single oversized row still gets its own range.
pub fn chunk_ranges(
    row_sizes: &[usize],
    batch: usize,
    max_bytes: usize,
    overhead: usize,
) -> Vec<Range<usize>> {
    let mut out = Vec::new();
    let mut start = 0;
    let mut bytes = overhead;
    for (i, sz) in row_sizes.iter().enumerate() {
        let rows = i - start;
        let over_rows = batch != 0 && rows >= batch;
        let over_bytes = rows > 0 && bytes + sz > max_bytes;
        if over_rows || over_bytes {
            out.push(start..i);
            start = i;
            bytes = overhead;
        }
        bytes += sz;
    }
    if start < row_sizes.len() {
        out.push(start..row_sizes.len());
    }
    out
}

fn is_narrow_numeric(t: &str) -> bool {
    matches!(
        t,
        "tinyint" | "smallint" | "int" | "integer" | "byte" | "short" | "float" | "real"
    )
}

/// The DDL applying an evolution against the table's current columns.
/// Idempotent by construction: columns that already exist are not re-added.
pub fn plan_evolution(
    table: &TableRef,
    evo: &SchemaEvolution,
    current: &[TableColumn],
) -> Result<Vec<String>, FaucetError> {
    let find = |name: &str| current.iter().find(|c| c.name.eq_ignore_ascii_case(name));
    let mut out = Vec::new();

    let adds: Vec<TableColumn> = evo
        .additions
        .iter()
        .filter(|a| find(&a.name).is_none())
        .map(|a| {
            let t = json_schema_base_type(&a.to).unwrap_or(SqlBaseType::Text);
            TableColumn::new(&a.name, base_type_sql(t).to_ascii_lowercase())
        })
        .collect();
    if !adds.is_empty() {
        out.push(add_columns_sql(table, &adds));
    }

    let mut relax: Vec<String> = Vec::new();
    let mut widen_enabled = false;
    for w in &evo.widenings {
        let Some(col) = find(&w.name) else { continue };
        let current_type = col.full_type.trim().to_ascii_lowercase();
        if let Some(from) = &w.from {
            if base_widened(from, &w.to) {
                let target = json_schema_base_type(&w.to);
                let already = match target {
                    Some(SqlBaseType::Double) => {
                        matches!(current_type.as_str(), "double")
                            || current_type.starts_with("decimal")
                    }
                    Some(SqlBaseType::Text | SqlBaseType::Json) => current_type == "string",
                    _ => false,
                };
                if !already {
                    if target == Some(SqlBaseType::Double) && is_narrow_numeric(&current_type) {
                        if !widen_enabled {
                            out.push(format!(
                                "ALTER TABLE {} SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')",
                                table.sql()
                            ));
                            widen_enabled = true;
                        }
                        out.push(format!(
                            "ALTER TABLE {} ALTER COLUMN {} TYPE DOUBLE",
                            table.sql(),
                            quote_ident(&col.name)
                        ));
                    } else {
                        return Err(FaucetError::Sink(format!(
                            "databricks sink: Delta cannot widen column `{}` from {} to {} in place \
                             (supported: tinyint/smallint/int/float → double); cast the column \
                             upstream or migrate the table",
                            col.name,
                            col.full_type,
                            target.map(base_type_sql).unwrap_or("STRING")
                        )));
                    }
                }
            }
            if adds_null(from, &w.to) && !col.nullable {
                relax.push(col.name.clone());
            }
        }
    }
    for name in &evo.relax_nullability {
        if let Some(col) = find(name)
            && !col.nullable
        {
            relax.push(col.name.clone());
        }
    }
    relax.sort();
    relax.dedup();
    for name in relax {
        out.push(format!(
            "ALTER TABLE {} ALTER COLUMN {} DROP NOT NULL",
            table.sql(),
            quote_ident(&name)
        ));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::drift::ColumnChange;

    fn t() -> TableRef {
        TableRef::new(Some("main"), "sales", "orders")
    }

    fn cols() -> Vec<TableColumn> {
        vec![
            TableColumn::new("id", "bigint"),
            TableColumn::new("name", "string"),
            TableColumn::new("tags", "array<string>"),
        ]
    }

    #[test]
    fn identifiers_and_literals_escape() {
        assert_eq!(quote_ident("a`b"), "`a``b`");
        assert_eq!(string_literal(r"it's a \ path"), r"'it\'s a \\ path'");
        assert_eq!(t().sql(), "`main`.`sales`.`orders`");
        assert_eq!(t().display(), "main.sales.orders");
        let nocat = TableRef::new(None, "s", "x");
        assert_eq!(nocat.sql(), "`s`.`x`");
        assert_eq!(nocat.display(), "s.x");
        assert_eq!(t().sibling("y").table, "y");
    }

    #[test]
    fn cells_render_as_text() {
        assert_eq!(cell_text(&Value::Null), None);
        assert_eq!(cell_text(&json!("x")).as_deref(), Some("x"));
        assert_eq!(cell_text(&json!(true)).as_deref(), Some("true"));
        assert_eq!(cell_text(&json!(1.5)).as_deref(), Some("1.5"));
        assert_eq!(cell_text(&json!({"a": 1})).as_deref(), Some(r#"{"a":1}"#));
    }

    #[test]
    fn casts_follow_declared_type() {
        assert_eq!(cast_expr("x", "string"), "x");
        assert_eq!(cast_expr("x", "bigint"), "CAST(x AS bigint)");
        assert_eq!(cast_expr("x", "decimal(10,2)"), "CAST(x AS decimal(10,2))");
        assert_eq!(
            cast_expr("x", "struct<a:int>"),
            "from_json(x, 'struct<a:int>')"
        );
        assert_eq!(
            cast_expr("x", "ARRAY<STRING>"),
            "from_json(x, 'ARRAY<STRING>')"
        );
        assert_eq!(
            cast_expr("x", "map<string,int>"),
            "from_json(x, 'map<string,int>')"
        );
        assert_eq!(cast_expr("x", "variant"), "parse_json(x)");
    }

    #[test]
    fn create_table_with_and_without_eo_columns() {
        let planned =
            faucet_core::plan_columns(&[json!({"id": 1, "n": "a", "ok": true, "f": 1.5, "o": {}})])
                .unwrap();
        let sql = create_table_sql(&t(), &planned, false);
        assert_eq!(
            sql,
            "CREATE TABLE IF NOT EXISTS `main`.`sales`.`orders` (`f` DOUBLE, `id` BIGINT, `n` STRING, `o` STRING, `ok` BOOLEAN) USING DELTA"
        );
        let eo = create_table_sql(&t(), &planned, true);
        assert!(
            eo.ends_with("`ok` BOOLEAN, `_faucet_scope` STRING, `_faucet_seq` BIGINT) USING DELTA")
        );
        let tc = planned_table_columns(&planned, true);
        assert_eq!(tc.len(), 7);
        assert_eq!(tc[0], TableColumn::new("f", "double"));
        assert!(missing_eo_columns(&tc).is_empty());
        assert_eq!(missing_eo_columns(&cols()).len(), 2);
    }

    #[test]
    fn describe_and_decode() {
        assert!(describe_sql(Some("main")).contains("FROM `main`.information_schema.columns"));
        assert!(describe_sql(None).contains("FROM information_schema.columns"));
        let got = columns_from_rows(vec![
            vec![Some("id".into()), Some("bigint".into()), Some("NO".into())],
            vec![Some("n".into()), None, Some("YES".into())],
            vec![None],
        ]);
        assert_eq!(
            got,
            vec![
                TableColumn {
                    name: "id".into(),
                    full_type: "bigint".into(),
                    nullable: false
                },
                TableColumn::new("n", "string"),
            ]
        );
    }

    #[test]
    fn schema_view_hides_eo_columns_and_maps_types() {
        let mut c = vec![
            TableColumn {
                name: "id".into(),
                full_type: "INT".into(),
                nullable: false,
            },
            TableColumn::new("amt", "decimal(10,2)"),
            TableColumn::new("f", "float"),
            TableColumn::new("ok", "boolean"),
            TableColumn::new("s", "struct<a:int>"),
            TableColumn::new("v", "variant"),
            TableColumn::new("arr", "array<int>"),
            TableColumn::new("ts", "timestamp"),
        ];
        c.extend(missing_eo_columns(&[]));
        let s = schema_from_columns(&c);
        let p = &s["properties"];
        assert_eq!(p["id"], json!({"type": "integer"}));
        assert_eq!(p["amt"], json!({"type": ["number", "null"]}));
        assert_eq!(p["f"]["type"][0], json!("number"));
        assert_eq!(p["ok"]["type"][0], json!("boolean"));
        assert_eq!(p["s"]["type"][0], json!("object"));
        assert_eq!(p["v"]["type"][0], json!("object"));
        assert_eq!(p["arr"]["type"][0], json!("array"));
        assert_eq!(p["ts"]["type"][0], json!("string"));
        assert!(p.get("_faucet_seq").is_none() && p.get("_faucet_scope").is_none());
    }

    #[test]
    fn matrix_projects_onto_table_order_case_insensitively() {
        let recs = vec![
            json!({"NAME": "a", "id": 1}),
            json!({"id": 2, "tags": ["x"]}),
        ];
        let m = build_matrix(&recs, &cols()).unwrap();
        assert_eq!(
            m.columns
                .iter()
                .map(|c| c.name.as_str())
                .collect::<Vec<_>>(),
            vec!["id", "name", "tags"]
        );
        assert_eq!(
            m.rows,
            vec![
                vec![Some("1".into()), Some("a".into()), None],
                vec![Some("2".into()), None, Some(r#"["x"]"#.into())],
            ]
        );
        assert_eq!(m.row_sizes(), vec![5 + 5 + 4 + 4, 5 + 4 + 9 + 4]);
        assert_eq!(m.estimated_bytes(), 18 + 22);
    }

    #[test]
    fn matrix_reports_unknown_columns_and_non_objects() {
        let err = build_matrix(
            &[json!({"id": 1, "zz": 2, "aa": 3}), json!({"zz": 1})],
            &cols(),
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("aa, zz"), "{err}");
        assert!(build_matrix(&[json!(1)], &cols()).is_err());
    }

    #[test]
    fn insert_from_values_and_file() {
        let c = &cols()[..2];
        let rows = vec![vec![Some("1".into()), Some("o'k".into())], vec![None, None]];
        let sql = insert_sql(&t(), c, Relation::Values(&rows), None);
        assert_eq!(
            sql,
            "INSERT INTO `main`.`sales`.`orders` (`id`, `name`) SELECT CAST(v.c0 AS bigint) AS `id`, v.c1 AS `name` \
             FROM VALUES ('1', 'o\\'k'), (NULL, NULL) AS v(c0, c1)"
        );
        let eo = insert_sql(
            &t(),
            c,
            Relation::File("/Volumes/a/b/c/f.parquet"),
            Some(("p::r", 7)),
        );
        assert_eq!(
            eo,
            "INSERT INTO `main`.`sales`.`orders` (`id`, `name`, `_faucet_scope`, `_faucet_seq`) \
             SELECT CAST(`id` AS bigint) AS `id`, `name` AS `name`, 'p::r', 7 \
             FROM read_files('/Volumes/a/b/c/f.parquet', format => 'parquet')"
        );
    }

    #[test]
    fn replace_where_projects_every_table_column() {
        let mut table = cols();
        table.extend(missing_eo_columns(&[]));
        let page = vec![TableColumn::new("name", "string")];
        let rows = vec![vec![Some("a".into())]];
        let sql = replace_where_sql(&t(), &table, &page, Relation::Values(&rows), "s", 3);
        assert_eq!(
            sql,
            "INSERT INTO `main`.`sales`.`orders` REPLACE WHERE `_faucet_scope` = 's' AND `_faucet_seq` >= 3 \
             SELECT CAST(NULL AS bigint) AS `id`, v.c0 AS `name`, CAST(NULL AS array<string>) AS `tags`, \
             's' AS `_faucet_scope`, CAST(3 AS BIGINT) AS `_faucet_seq` FROM VALUES ('a') AS v(c0)"
        );
        assert_eq!(
            delete_eo_sql(&t(), "s", 3),
            "DELETE FROM `main`.`sales`.`orders` WHERE `_faucet_scope` = 's' AND `_faucet_seq` >= 3"
        );
    }

    #[test]
    fn merge_applies_updates_deletes_inserts() {
        let c = &cols()[..2];
        let rows = vec![
            vec![Some("1".into()), Some("a".into()), Some("u".into())],
            vec![Some("2".into()), None, Some("d".into())],
        ];
        let sql = merge_sql(&t(), c, &["id".into()], Relation::Values(&rows));
        assert_eq!(
            sql,
            "MERGE INTO `main`.`sales`.`orders` AS t USING (SELECT CAST(v.c0 AS bigint) AS `id`, v.c1 AS `name`, \
             v.c2 AS `__faucet_op` FROM VALUES ('1', 'a', 'u'), ('2', NULL, 'd') AS v(c0, c1, c2)) AS s \
             ON t.`id` = s.`id` WHEN MATCHED AND s.`__faucet_op` = 'd' THEN DELETE \
             WHEN MATCHED THEN UPDATE SET t.`name` = s.`name` \
             WHEN NOT MATCHED AND s.`__faucet_op` = 'u' THEN INSERT (`id`, `name`) VALUES (s.`id`, s.`name`)"
        );
        let key_only = merge_sql(
            &t(),
            &cols()[..1],
            &["ID".into()],
            Relation::File("s3://b/k"),
        );
        assert!(!key_only.contains("UPDATE SET"));
        assert!(key_only.contains(
            "`__faucet_op` AS `__faucet_op` FROM read_files('s3://b/k', format => 'parquet')"
        ));
    }

    #[test]
    fn copy_into_with_and_without_options() {
        let sql = copy_into_sql(&t(), &cols()[..1], "s3://b/p/", "f.parquet", None);
        assert_eq!(
            sql,
            "COPY INTO `main`.`sales`.`orders` FROM (SELECT CAST(`id` AS bigint) AS `id` FROM 's3://b/p/') \
             FILEFORMAT = PARQUET FILES = ('f.parquet')"
        );
        let with = copy_into_sql(
            &t(),
            &cols()[..1],
            "d",
            "f",
            Some(" 'mergeSchema' = 'false' "),
        );
        assert!(with.ends_with("COPY_OPTIONS ('mergeSchema' = 'false')"));
        assert!(!copy_into_sql(&t(), &cols()[..1], "d", "f", Some("  ")).contains("COPY_OPTIONS"));
    }

    #[test]
    fn overwrite_and_token_statements() {
        let s = t().sibling("orders__faucet_ovw");
        assert_eq!(
            drop_table_sql(&s),
            "DROP TABLE IF EXISTS `main`.`sales`.`orders__faucet_ovw`"
        );
        assert_eq!(
            create_like_sql(&s, &t()),
            "CREATE TABLE `main`.`sales`.`orders__faucet_ovw` LIKE `main`.`sales`.`orders`"
        );
        assert_eq!(
            insert_overwrite_sql(&t(), &s),
            "INSERT OVERWRITE TABLE `main`.`sales`.`orders` SELECT * FROM `main`.`sales`.`orders__faucet_ovw`"
        );
        assert_eq!(
            rename_sql(&s, &t()),
            "ALTER TABLE `main`.`sales`.`orders__faucet_ovw` RENAME TO `main`.`sales`.`orders`"
        );
        assert!(token_table_ddl(&t()).starts_with(
            "CREATE TABLE IF NOT EXISTS `main`.`sales`.`_faucet_commit_token` (`scope` STRING NOT NULL"
        ));
        assert_eq!(
            token_select_sql(&t()),
            "SELECT `token` FROM `main`.`sales`.`_faucet_commit_token` WHERE `scope` = :scope LIMIT 1"
        );
        assert!(
            token_merge_sql(&t()).contains("USING (SELECT :scope AS `scope`, :token AS `token`)")
        );
        assert_eq!(
            token_delete_sql(&t()),
            "DELETE FROM `main`.`sales`.`_faucet_commit_token` WHERE `scope` = :scope"
        );
    }

    #[test]
    fn chunking_respects_rows_and_bytes() {
        assert_eq!(chunk_ranges(&[10; 5], 2, 1000, 0), vec![0..2, 2..4, 4..5]);
        assert_eq!(chunk_ranges(&[10; 5], 0, 25, 0), vec![0..2, 2..4, 4..5]);
        assert_eq!(chunk_ranges(&[100, 1], 0, 50, 0), vec![0..1, 1..2]);
        assert_eq!(chunk_ranges(&[5; 3], 0, 30, 10), vec![0..3]);
        assert!(chunk_ranges(&[], 10, 10, 0).is_empty());
    }

    fn change(name: &str, from: Option<Value>, to: Value) -> ColumnChange {
        ColumnChange {
            name: name.into(),
            from,
            to,
        }
    }

    #[test]
    fn evolution_adds_missing_columns_only() {
        let evo = SchemaEvolution {
            additions: vec![
                change("email", None, json!({"type": "string"})),
                change("id", None, json!({"type": "integer"})),
                change("blank", None, json!({"type": "null"})),
            ],
            widenings: vec![],
            relax_nullability: vec![],
        };
        let sql = plan_evolution(&t(), &evo, &cols()).unwrap();
        assert_eq!(
            sql,
            vec![
                "ALTER TABLE `main`.`sales`.`orders` ADD COLUMNS (`email` STRING, `blank` STRING)"
            ]
        );
        assert!(
            plan_evolution(&t(), &SchemaEvolution::default(), &cols())
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn evolution_widens_narrow_ints_and_relaxes_nullability() {
        let current = vec![
            TableColumn {
                name: "qty".into(),
                full_type: "int".into(),
                nullable: false,
            },
            TableColumn::new("price", "float"),
            TableColumn::new("amt", "double"),
            TableColumn {
                name: "note".into(),
                full_type: "string".into(),
                nullable: false,
            },
        ];
        let evo = SchemaEvolution {
            additions: vec![],
            widenings: vec![
                change(
                    "qty",
                    Some(json!({"type": "integer"})),
                    json!({"type": ["number", "null"]}),
                ),
                change(
                    "price",
                    Some(json!({"type": "integer"})),
                    json!({"type": "number"}),
                ),
                change(
                    "amt",
                    Some(json!({"type": "integer"})),
                    json!({"type": "number"}),
                ),
                change(
                    "gone",
                    Some(json!({"type": "integer"})),
                    json!({"type": "number"}),
                ),
                change(
                    "note",
                    Some(json!({"type": "string"})),
                    json!({"type": ["string", "null"]}),
                ),
            ],
            relax_nullability: vec!["note".into(), "qty".into(), "amt".into(), "nope".into()],
        };
        let sql = plan_evolution(&t(), &evo, &current).unwrap();
        assert_eq!(
            sql,
            vec![
                "ALTER TABLE `main`.`sales`.`orders` SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')",
                "ALTER TABLE `main`.`sales`.`orders` ALTER COLUMN `qty` TYPE DOUBLE",
                "ALTER TABLE `main`.`sales`.`orders` ALTER COLUMN `price` TYPE DOUBLE",
                "ALTER TABLE `main`.`sales`.`orders` ALTER COLUMN `note` DROP NOT NULL",
                "ALTER TABLE `main`.`sales`.`orders` ALTER COLUMN `qty` DROP NOT NULL",
            ]
        );
    }

    #[test]
    fn evolution_refuses_unsupported_widening() {
        let evo = SchemaEvolution {
            additions: vec![],
            widenings: vec![change(
                "id",
                Some(json!({"type": "integer"})),
                json!({"type": "number"}),
            )],
            relax_nullability: vec![],
        };
        let err = plan_evolution(&t(), &evo, &cols()).unwrap_err().to_string();
        assert!(err.contains("cannot widen column `id`"), "{err}");
        let to_text = SchemaEvolution {
            additions: vec![],
            widenings: vec![change(
                "name",
                Some(json!({"type": "integer"})),
                json!({"type": "string"}),
            )],
            relax_nullability: vec![],
        };
        assert!(plan_evolution(&t(), &to_text, &cols()).unwrap().is_empty());
    }
}
