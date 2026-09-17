//! Connection setup, reference-relation loading, and query validation.

use crate::config::{RelationSource, RelationSpec, SqlTransformConfig};
use crate::http::fetch_http_rows;
use crate::shovel::{records_to_record_batch, values_to_record_batch};
use duckdb::Connection;
use duckdb::vtab::arrow::{ArrowVTab, arrow_recordbatch_to_query_params};
use faucet_core::FaucetError;

const RESERVED: &str = "batch";

fn cfg_err(msg: impl Into<String>) -> FaucetError {
    FaucetError::Config(format!("sql transform: {}", msg.into()))
}

/// A relation we may need to reload on mtime change.
pub(crate) struct Reloadable {
    pub name: String,
    pub path: String,
    pub has_header: bool,
    pub is_csv: bool,
    pub last_mtime: Option<std::time::SystemTime>,
}

/// Open an in-memory connection, apply pragmas, register the Arrow VTab, and
/// load every reference relation. Returns the connection + reload tracking.
pub(crate) fn build_connection(
    cfg: &SqlTransformConfig,
) -> Result<(Connection, Vec<Reloadable>), FaucetError> {
    let conn = Connection::open_in_memory().map_err(|e| cfg_err(format!("open duckdb: {e}")))?;
    if let Some(ref m) = cfg.memory_limit {
        conn.execute_batch(&format!("SET memory_limit='{}';", sql_escape(m)))
            .map_err(|e| cfg_err(format!("memory_limit: {e}")))?;
    }
    if let Some(t) = cfg.threads {
        conn.execute_batch(&format!("SET threads={t};"))
            .map_err(|e| cfg_err(format!("threads: {e}")))?;
    }
    conn.register_table_function::<ArrowVTab>("arrow")
        .map_err(|e| cfg_err(format!("register arrow vtab: {e}")))?;

    let mut reloadables = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for rel in &cfg.relations {
        validate_ident(&rel.name)?;
        if rel.name.eq_ignore_ascii_case(RESERVED) {
            return Err(cfg_err("relation name 'batch' is reserved for the page"));
        }
        if !seen.insert(rel.name.clone()) {
            return Err(cfg_err(format!("duplicate relation name '{}'", rel.name)));
        }
        load_relation(&conn, rel)?;
        if rel.reload_on_change
            && let Some(r) = reloadable_for(rel)?
        {
            reloadables.push(r);
        }
    }
    Ok((conn, reloadables))
}

fn validate_ident(name: &str) -> Result<(), FaucetError> {
    let ok = !name.is_empty()
        && name
            .chars()
            .next()
            .map(|c| c.is_ascii_alphabetic() || c == '_')
            .unwrap_or(false)
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
    if ok {
        Ok(())
    } else {
        Err(cfg_err(format!("invalid relation name '{name}'")))
    }
}

fn load_relation(conn: &Connection, rel: &RelationSpec) -> Result<(), FaucetError> {
    match &rel.source {
        RelationSource::Csv { path, has_header } => {
            check_exists(path)?;
            conn.execute_batch(&format!(
                "CREATE OR REPLACE TABLE \"{}\" AS SELECT * FROM read_csv_auto('{}', header={});",
                rel.name,
                sql_escape(path),
                has_header
            ))
            .map_err(|e| cfg_err(format!("load csv relation '{}': {e}", rel.name)))
        }
        RelationSource::Jsonl { path } => {
            check_exists(path)?;
            conn.execute_batch(&format!(
                "CREATE OR REPLACE TABLE \"{}\" AS SELECT * FROM read_json_auto('{}', format='newline_delimited');",
                rel.name,
                sql_escape(path)
            ))
            .map_err(|e| cfg_err(format!("load jsonl relation '{}': {e}", rel.name)))
        }
        RelationSource::Values { columns, rows } => {
            if columns.is_empty() {
                return Err(cfg_err(format!(
                    "values relation '{}' has no columns",
                    rel.name
                )));
            }
            let batch = values_to_record_batch(columns, rows)?;
            let params = arrow_recordbatch_to_query_params(batch);
            conn.execute(
                &format!(
                    "CREATE OR REPLACE TABLE \"{}\" AS SELECT * FROM arrow(?, ?)",
                    rel.name
                ),
                params,
            )
            .map_err(|e| cfg_err(format!("load values relation '{}': {e}", rel.name)))?;
            Ok(())
        }
        RelationSource::Http {
            url,
            method,
            headers,
            records_path,
        } => {
            // Fetch exactly once, here at compile time. The resulting DuckDB
            // table persists for the whole run (Http is never a Reloadable), so
            // no page re-hits the endpoint.
            let rows = fetch_http_rows(&rel.name, url, *method, headers, records_path.as_deref())?;
            if rows.is_empty() {
                return Err(cfg_err(format!(
                    "http relation '{}' fetched zero rows; a reference relation must have \
                     at least one row so its columns can be inferred",
                    rel.name
                )));
            }
            let batch = records_to_record_batch(&rows)
                .map_err(|e| cfg_err(format!("http relation '{}': {e}", rel.name)))?;
            let params = arrow_recordbatch_to_query_params(batch);
            conn.execute(
                &format!(
                    "CREATE OR REPLACE TABLE \"{}\" AS SELECT * FROM arrow(?, ?)",
                    rel.name
                ),
                params,
            )
            .map_err(|e| cfg_err(format!("load http relation '{}': {e}", rel.name)))?;
            Ok(())
        }
    }
}

fn reloadable_for(rel: &RelationSpec) -> Result<Option<Reloadable>, FaucetError> {
    let (path, has_header, is_csv) = match &rel.source {
        RelationSource::Csv { path, has_header } => (path.clone(), *has_header, true),
        RelationSource::Jsonl { path } => (path.clone(), false, false),
        // Inline and HTTP relations are loaded once for the whole run; there is
        // no file to re-stat, so they are never reloadable.
        RelationSource::Values { .. } | RelationSource::Http { .. } => return Ok(None),
    };
    let last_mtime = std::fs::metadata(&path).and_then(|m| m.modified()).ok();
    Ok(Some(Reloadable {
        name: rel.name.clone(),
        path,
        has_header,
        is_csv,
        last_mtime,
    }))
}

fn check_exists(path: &str) -> Result<(), FaucetError> {
    if std::path::Path::new(path).exists() {
        Ok(())
    } else {
        Err(cfg_err(format!(
            "reference relation file does not exist: {path}"
        )))
    }
}

/// Escape a string literal for safe interpolation inside a single-quoted SQL
/// string (e.g. a file path passed to `read_csv_auto('…')`).
pub(crate) fn sql_escape(s: &str) -> String {
    s.replace('\'', "''")
}

/// Whether a bind failure is about **column resolution** — the one class that
/// is genuinely unknowable at config-load time.
///
/// `batch`'s column schema comes from the first page at run time, so a query
/// selecting `batch` columns cannot be fully bound yet. Everything else —
/// syntax, a missing table, an unknown function — is a real defect and must
/// fail here rather than on page 1 of a production run.
///
/// This is the only residual text match in validation, scoped to one DuckDB
/// error family. It replaced a rule that fail-**opened**: it tolerated any
/// error whose text merely contained "batch", so a query joining a genuinely
/// missing `daily_batches` table was accepted (#654 H7).
fn is_column_resolution_error(msg: &str) -> bool {
    let m = msg.to_ascii_lowercase();
    // Both DuckDB wordings occur: unqualified ("Referenced column \"x\" not
    // found") and qualified/aliased ("Table \"b\" does not have a column
    // named \"x\"").
    m.contains("referenced column")
        || m.contains("does not have a column named")
        || (m.contains("binder error") && m.contains("group by"))
}

/// Validate the query at config-load time, binding it against a
/// **placeholder** `batch` relation so table resolution actually happens.
///
/// Registering the placeholder is what lets this fail closed: previously no
/// `batch` existed during validation, so every "relation does not exist"
/// error had to be tolerated. Now only column resolution against the unknown
/// page schema is excused — see [`is_column_resolution_error`].
pub(crate) fn validate_query(conn: &Connection, query: &str) -> Result<(), FaucetError> {
    // Zero-row placeholder so `FROM batch` resolves. The real run replaces it
    // (`CREATE OR REPLACE TEMP TABLE batch AS SELECT * FROM arrow(...)`)
    // before the first page is transformed, so it cannot reach any output.
    conn.execute_batch(&format!(
        "CREATE OR REPLACE TEMP TABLE \"{RESERVED}\" AS \
         SELECT NULL AS __faucet_placeholder WHERE 1=0;"
    ))
    .map_err(|e| {
        FaucetError::Config(format!(
            "sql transform: could not register the `{RESERVED}` validation placeholder: {e}"
        ))
    })?;

    match conn.prepare(query) {
        Ok(_) => Ok(()),
        Err(e) => {
            let msg = e.to_string();
            if is_column_resolution_error(&msg) {
                Ok(())
            } else {
                Err(FaucetError::Transform(format!(
                    "sql transform: invalid query: {msg}"
                )))
            }
        }
    }
}
