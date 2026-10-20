//! Config types for the SQL transform. No I/O or DuckDB here.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

/// Configuration for the `sql` transform.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SqlTransformConfig {
    /// The SQL statement. The page's records are the relation `batch`. Must
    /// produce a result set; each result row becomes one output record.
    pub query: String,
    /// Reference relations loaded once at compile time and joinable by name.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub relations: Vec<RelationSpec>,
    /// Optional DuckDB `memory_limit` pragma (e.g. "1GB"). Default: DuckDB's own.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_limit: Option<String>,
    /// Optional DuckDB `threads` pragma. Default: DuckDB's own. Set to 1–2 for
    /// high-fan-out matrices to avoid CPU over-subscription across rows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub threads: Option<usize>,
}

/// A reference relation registered before the first page.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct RelationSpec {
    /// Relation name as referenced in the query. Must be a safe SQL identifier
    /// and must not be `batch` (reserved for the page).
    pub name: String,
    /// Where the relation's data comes from.
    pub source: RelationSource,
    /// Re-stat the file's mtime before each page; rebuild + atomic swap if it
    /// changed. Default false. Ignored for `values` and `http` (both loaded
    /// once for the whole run).
    #[serde(default)]
    pub reload_on_change: bool,
}

/// HTTP method used to fetch an `http` reference relation. Only the two verbs a
/// small read-only list endpoint needs are supported; `GET` is the default.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "UPPERCASE")]
pub enum HttpMethod {
    /// `GET` — the default.
    #[default]
    Get,
    /// `POST` — for POST-search list endpoints (no request body is sent).
    Post,
}

// serde `default = "..."` needs a function, not a literal.
fn default_true() -> bool {
    true
}

/// The data source for a reference relation.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RelationSource {
    /// Delimited file loaded via DuckDB `read_csv_auto`.
    Csv {
        /// Filesystem path to the CSV file (absolute, or relative to the working directory).
        #[schemars(extend("x-faucet-resource" = "path"))]
        path: String,
        /// Whether the first row is a header row. Default: `true`.
        #[serde(default = "default_true")]
        has_header: bool,
    },
    /// Newline-delimited JSON loaded via DuckDB `read_json_auto`.
    Jsonl {
        /// Filesystem path to the JSONL file (absolute, or relative to the working directory).
        #[schemars(extend("x-faucet-resource" = "path"))]
        path: String,
    },
    /// Inline rows materialized into a table.
    Values {
        /// Column names, in declaration order.
        columns: Vec<String>,
        /// Rows of cell values; each inner row must have the same length as `columns`.
        rows: Vec<Vec<Value>>,
    },
    /// Rows fetched from a small REST endpoint **once** at compile/first-use and
    /// cached for the whole run (never re-fetched per page). The response is
    /// materialized into a DuckDB table joinable by the relation's `name`.
    Http {
        /// Endpoint URL to fetch the rows from.
        #[schemars(extend("x-faucet-resource" = "url"))]
        url: String,
        /// HTTP method. Default: `GET`.
        #[serde(default)]
        method: HttpMethod,
        /// Static request headers sent with the fetch (e.g. a bearer token
        /// injected via `${...}` at the CLI layer). Optional.
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        headers: BTreeMap<String, String>,
        /// JSONPath selecting the row array in the response body (e.g.
        /// `$.items[*]`). If omitted, the whole body is used and must be a JSON
        /// array. Every selected element must be a JSON object (one table row).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        records_path: Option<String>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_round_trips_and_schema_builds() {
        let cfg: SqlTransformConfig = serde_json::from_value(serde_json::json!({
            "query": "SELECT * FROM batch",
            "relations": [
                {"name": "countries",
                 "source": {"type": "csv", "path": "c.csv", "has_header": true}}
            ]
        }))
        .unwrap();
        assert_eq!(cfg.relations.len(), 1);
        assert!(matches!(
            cfg.relations[0].source,
            RelationSource::Csv { .. }
        ));
        // schema_for! must succeed (used by `faucet schema transform sql`).
        let schema = schemars::schema_for!(SqlTransformConfig);
        let json = serde_json::to_value(&schema).unwrap();
        assert!(
            json.get("properties")
                .and_then(|p| p.get("query"))
                .is_some()
        );
    }
}
