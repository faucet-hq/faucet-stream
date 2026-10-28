//! Statements through the Redshift Data API (`credentials: { type:
//! redshift_data_api }`).
//!
//! A statement is submitted with `ExecuteStatement`, polled with
//! `DescribeStatement` until it is `FINISHED`, `FAILED` or `ABORTED`, and its
//! rows are read page by page with `GetStatementResult`. Every request is one
//! call through the caller's [`CallContext`]: submitting a write is a single
//! attempt (`call_once`), polling waits through [`CallContext::wait`], so the
//! run's deadline and cancel end a long statement (which is then cancelled
//! on the service, best effort).
//!
//! Data API parameters are named strings and cannot be NULL or empty, so
//! [`named_parameters`] turns the `$n` placeholders the connectors write into
//! `:pN` parameters and writes a NULL or empty value into the SQL itself.

use std::time::Duration;

use aws_sdk_redshiftdata::types::{Field, SqlParameter, StatusString};
use faucet_core::FaucetError;
use faucet_core::io::CallContext;
use serde_json::{Map, Value};
use tokio::sync::OnceCell;

use crate::config::{RedshiftAwsCredentials, RedshiftConnection, RedshiftCredentials};

/// The first wait between two `DescribeStatement` polls; it doubles up to
/// [`POLL_MAX`].
pub const POLL_START: Duration = Duration::from_millis(50);

/// The longest wait between two polls.
pub const POLL_MAX: Duration = Duration::from_secs(1);

/// How long the best-effort `CancelStatement` of an abandoned statement may
/// take.
const CANCEL_TIMEOUT: Duration = Duration::from_secs(30);

/// Whether a statement changes data: a write is submitted in one attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Effect {
    /// A query; resubmitting it is harmless.
    Read,
    /// A write; never resubmitted.
    Write,
}

/// SQL with its named parameters.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Statement {
    /// The SQL text, with `:pN` parameters.
    pub sql: String,
    /// `(name, value)` pairs, each referenced by the SQL.
    pub parameters: Vec<(String, String)>,
}

impl Statement {
    /// SQL with no parameters.
    pub fn new(sql: impl Into<String>) -> Self {
        Self {
            sql: sql.into(),
            parameters: Vec::new(),
        }
    }
}

/// A finished statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Executed {
    /// The statement id.
    pub id: String,
    /// Whether it produced a result set to read.
    pub has_result_set: bool,
    /// Rows it returned or changed, as the service reports.
    pub rows: i64,
}

/// One result column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataColumn {
    /// Column name.
    pub name: String,
    /// The type name the service reports (`int8`, `numeric`, `timestamptz`).
    pub type_name: String,
    /// `Some(false)` when the column is `NOT NULL`, `None` when unknown.
    pub nullable: Option<bool>,
}

/// One page of `GetStatementResult`.
#[derive(Debug, Clone)]
pub struct ResultPage {
    /// The result's columns.
    pub columns: Vec<DataColumn>,
    /// The rows, one field per column.
    pub rows: Vec<Vec<Field>>,
    /// The token of the next page, `None` on the last one.
    pub next_token: Option<String>,
}

#[derive(Debug, Clone)]
enum Target {
    Cluster {
        id: String,
        db_user: Option<String>,
        secret_arn: Option<String>,
    },
    Workgroup {
        name: String,
        secret_arn: Option<String>,
    },
}

/// A Redshift Data API connection.
pub struct DataApiClient {
    target: Target,
    database: String,
    region: Option<String>,
    aws_credentials: RedshiftAwsCredentials,
    endpoint_url: Option<String>,
    error: fn(String) -> FaucetError,
    client: OnceCell<aws_sdk_redshiftdata::Client>,
}

impl std::fmt::Debug for DataApiClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataApiClient")
            .field("target", &self.target)
            .field("database", &self.database)
            .finish_non_exhaustive()
    }
}

impl DataApiClient {
    /// The client for a `redshift_data_api` connection, validated (no I/O);
    /// `None` for any other credentials method. `error` wraps a failure in
    /// the caller's error kind (`FaucetError::Source` or `Sink`).
    pub fn from_connection(
        conn: &RedshiftConnection,
        error: fn(String) -> FaucetError,
    ) -> Result<Option<Self>, FaucetError> {
        let RedshiftCredentials::RedshiftDataApi {
            region,
            cluster_identifier,
            workgroup_name,
            secret_arn,
            db_user,
            aws_credentials,
            endpoint_url,
        } = &conn.credentials
        else {
            return Ok(None);
        };
        let present = |v: &Option<String>| {
            v.as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
        };
        if conn.database.trim().is_empty() {
            return Err(FaucetError::Config(
                "redshift data api: `database` must not be empty".into(),
            ));
        }
        let target = match (present(cluster_identifier), present(workgroup_name)) {
            (Some(id), None) => {
                let (db_user, secret_arn) = (present(db_user), present(secret_arn));
                if db_user.is_some() == secret_arn.is_some() {
                    return Err(FaucetError::Config(
                        "redshift data api: a cluster needs exactly one of `db_user` \
                         (temporary credentials) or `secret_arn`"
                            .into(),
                    ));
                }
                Target::Cluster {
                    id,
                    db_user,
                    secret_arn,
                }
            }
            (None, Some(name)) => {
                if present(db_user).is_some() {
                    return Err(FaucetError::Config(
                        "redshift data api: `db_user` applies to a provisioned cluster; a \
                         serverless workgroup authenticates as the IAM identity or `secret_arn`"
                            .into(),
                    ));
                }
                Target::Workgroup {
                    name,
                    secret_arn: present(secret_arn),
                }
            }
            _ => {
                return Err(FaucetError::Config(
                    "redshift data api: set exactly one of `cluster_identifier` or \
                     `workgroup_name`"
                        .into(),
                ));
            }
        };
        Ok(Some(Self {
            target,
            database: conn.database.clone(),
            region: region.clone(),
            aws_credentials: aws_credentials.clone(),
            endpoint_url: endpoint_url.clone(),
            error,
            client: OnceCell::new(),
        }))
    }

    async fn client(&self) -> &aws_sdk_redshiftdata::Client {
        self.client
            .get_or_init(|| async {
                let sdk =
                    crate::aws::sdk_config(self.region.as_deref(), &self.aws_credentials).await;
                let mut b = aws_sdk_redshiftdata::config::Builder::from(&sdk);
                if let Some(e) = &self.endpoint_url {
                    b = b.endpoint_url(e);
                }
                aws_sdk_redshiftdata::Client::from_conf(b.build())
            })
            .await
    }

    fn fail(&self, what: &str, e: impl std::error::Error) -> FaucetError {
        (self.error)(format!(
            "redshift data api: {what} failed: {}",
            crate::aws::describe(&e)
        ))
    }

    /// Submit `statement` and wait until it finishes. A failed or aborted
    /// statement is an error carrying the service's message.
    pub async fn execute(
        &self,
        calls: &CallContext,
        effect: Effect,
        statement: &Statement,
    ) -> Result<Executed, FaucetError> {
        let client = self.client().await;
        let mut parameters = Vec::with_capacity(statement.parameters.len());
        for (name, value) in &statement.parameters {
            parameters.push(
                SqlParameter::builder()
                    .name(name)
                    .value(value)
                    .build()
                    .map_err(|e| self.fail("building a parameter", e))?,
            );
        }
        let submit = || async {
            let mut req = client
                .execute_statement()
                .sql(&statement.sql)
                .database(&self.database)
                .set_parameters((!parameters.is_empty()).then(|| parameters.clone()));
            req = match &self.target {
                Target::Cluster {
                    id,
                    db_user,
                    secret_arn,
                } => req
                    .cluster_identifier(id)
                    .set_db_user(db_user.clone())
                    .set_secret_arn(secret_arn.clone()),
                Target::Workgroup { name, secret_arn } => {
                    req.workgroup_name(name).set_secret_arn(secret_arn.clone())
                }
            };
            let out = req
                .send()
                .await
                .map_err(|e| self.fail("ExecuteStatement", e))?;
            out.id().map(str::to_string).ok_or_else(|| {
                (self.error)("redshift data api: ExecuteStatement returned no id".into())
            })
        };
        let id = match effect {
            Effect::Write => calls.call_once("submit", submit()).await?,
            Effect::Read => calls.call("submit", submit).await?,
        };
        match Box::pin(self.wait(calls, client, &id)).await {
            Err(e @ (FaucetError::Timeout { .. } | FaucetError::Cancelled { .. })) => {
                cancel_detached(client.clone(), id);
                Err(e)
            }
            other => other,
        }
    }

    async fn wait(
        &self,
        calls: &CallContext,
        client: &aws_sdk_redshiftdata::Client,
        id: &str,
    ) -> Result<Executed, FaucetError> {
        let mut delay = POLL_START;
        loop {
            let d = calls
                .call("poll", || async {
                    client
                        .describe_statement()
                        .id(id)
                        .send()
                        .await
                        .map_err(|e| self.fail("DescribeStatement", e))
                })
                .await?;
            match d.status() {
                Some(StatusString::Finished) => {
                    return Ok(Executed {
                        id: id.to_string(),
                        has_result_set: d.has_result_set().unwrap_or(false),
                        rows: d.result_rows(),
                    });
                }
                Some(StatusString::Failed) => {
                    return Err((self.error)(format!(
                        "redshift data api: statement failed: {}",
                        d.error().unwrap_or("the service gave no reason")
                    )));
                }
                Some(StatusString::Aborted) => {
                    return Err((self.error)(
                        "redshift data api: statement was aborted".into(),
                    ));
                }
                _ => {}
            }
            calls.wait(delay).await?;
            delay = (delay * 2).min(POLL_MAX);
        }
    }

    /// One page of a finished statement's result (the call `fetch`).
    pub async fn result_page(
        &self,
        calls: &CallContext,
        id: &str,
        next_token: Option<&str>,
    ) -> Result<ResultPage, FaucetError> {
        let client = self.client().await;
        let out = calls
            .call("fetch", || async {
                client
                    .get_statement_result()
                    .id(id)
                    .set_next_token(next_token.map(str::to_string))
                    .send()
                    .await
                    .map_err(|e| self.fail("GetStatementResult", e))
            })
            .await?;
        Ok(ResultPage {
            columns: out
                .column_metadata()
                .iter()
                .map(|c| DataColumn {
                    name: c.name().or(c.label()).unwrap_or_default().to_string(),
                    type_name: c.type_name().unwrap_or_default().to_string(),
                    nullable: match c.nullable() {
                        0 => Some(false),
                        1 => Some(true),
                        _ => None,
                    },
                })
                .collect(),
            rows: out.records().to_vec(),
            next_token: out
                .next_token()
                .filter(|t| !t.is_empty())
                .map(str::to_string),
        })
    }

    /// Run a query and read every row as a JSON object (small catalog
    /// queries; a large result streams through [`result_page`](Self::result_page)).
    pub async fn query(
        &self,
        calls: &CallContext,
        statement: &Statement,
    ) -> Result<(Vec<DataColumn>, Vec<Map<String, Value>>), FaucetError> {
        let done = Box::pin(self.execute(calls, Effect::Read, statement)).await?;
        let mut columns = Vec::new();
        let mut rows = Vec::new();
        if !done.has_result_set {
            return Ok((columns, rows));
        }
        let mut token = None;
        loop {
            let page = Box::pin(self.result_page(calls, &done.id, token.as_deref())).await?;
            if columns.is_empty() {
                columns = page.columns;
            }
            for fields in &page.rows {
                rows.push(row_to_json(&columns, fields).map_err(|e| (self.error)(e.to_string()))?);
            }
            match page.next_token {
                Some(t) => token = Some(t),
                None => return Ok((columns, rows)),
            }
        }
    }
}

/// Cancel an abandoned statement on a spawned task, best effort.
fn cancel_detached(client: aws_sdk_redshiftdata::Client, id: String) {
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        return;
    };
    handle.spawn(async move {
        let ctx = CallContext::new().with_timeout(CANCEL_TIMEOUT);
        let cancelled = ctx
            .call_once("cancel", async {
                client
                    .cancel_statement()
                    .id(&id)
                    .send()
                    .await
                    .map_err(|e| FaucetError::Source(crate::aws::describe(&e)))
            })
            .await;
        if let Err(e) = cancelled {
            tracing::warn!(statement = %id, error = %e, "redshift data api: cancelling an abandoned statement failed");
        }
    });
}

/// Turn the `$n` placeholders of `sql` into Data API parameters bound to
/// `binds[n - 1]`. A NULL becomes `NULL` and an empty string `''` in the SQL
/// (a parameter value can be neither); a number, boolean or nested value is
/// passed as its text, which the server converts to the type it is used as.
/// Placeholders inside quoted strings, quoted identifiers and comments are
/// left alone, as is a `$n` with no bind.
pub fn named_parameters(sql: &str, binds: &[Value]) -> Statement {
    let mut out = String::with_capacity(sql.len());
    let mut used = vec![false; binds.len()];
    let bytes = sql.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        let skip_to = match c {
            b'\'' | b'"' => closing_quote(bytes, i),
            b'-' if bytes.get(i + 1) == Some(&b'-') => bytes[i..]
                .iter()
                .position(|b| *b == b'\n')
                .map_or(bytes.len(), |p| i + p),
            b'/' if bytes.get(i + 1) == Some(&b'*') => sql[i + 2..]
                .find("*/")
                .map_or(bytes.len(), |p| i + 2 + p + 2),
            _ => i,
        };
        if skip_to > i {
            out.push_str(&sql[i..skip_to]);
            i = skip_to;
            continue;
        }
        let after_ident = i > 0 && (bytes[i - 1].is_ascii_alphanumeric() || bytes[i - 1] == b'_');
        if c == b'$' && !after_ident {
            let digits = bytes[i + 1..]
                .iter()
                .take_while(|b| b.is_ascii_digit())
                .count();
            if let Ok(n) = sql[i + 1..i + 1 + digits].parse::<usize>()
                && (1..=binds.len()).contains(&n)
            {
                match &binds[n - 1] {
                    Value::Null => out.push_str("NULL"),
                    Value::String(s) if s.is_empty() => out.push_str("''"),
                    _ => {
                        out.push_str(&format!(":p{n}"));
                        used[n - 1] = true;
                    }
                }
                i += 1 + digits;
                continue;
            }
        }
        let ch = sql[i..].chars().next().unwrap_or_default();
        out.push(ch);
        i += ch.len_utf8();
    }
    let parameters = binds
        .iter()
        .enumerate()
        .filter(|(n, _)| used[*n])
        .map(|(n, v)| (format!("p{}", n + 1), parameter_text(v)))
        .collect();
    Statement {
        sql: out,
        parameters,
    }
}

/// The index just past the quote that closes the one at `start` (a doubled
/// quote is an escaped one).
fn closing_quote(bytes: &[u8], start: usize) -> usize {
    let q = bytes[start];
    let mut i = start + 1;
    while i < bytes.len() {
        if bytes[i] == q {
            if bytes.get(i + 1) == Some(&q) {
                i += 2;
                continue;
            }
            return i + 1;
        }
        i += 1;
    }
    bytes.len()
}

/// A bind value as a Data API parameter's text.
pub fn parameter_text(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// One result row as a JSON object keyed by column name.
pub fn row_to_json(
    columns: &[DataColumn],
    fields: &[Field],
) -> Result<Map<String, Value>, FaucetError> {
    if columns.len() != fields.len() {
        return Err(FaucetError::Source(format!(
            "redshift data api: a row has {} field(s) for {} column(s)",
            fields.len(),
            columns.len()
        )));
    }
    columns
        .iter()
        .zip(fields)
        .map(|(c, f)| Ok((c.name.clone(), field_to_json(c, f)?)))
        .collect()
}

/// One field as JSON through `faucet_core::fidelity`: integers as numbers,
/// floats as their shortest form, exact numerics as strings at their scale,
/// instants as RFC 3339 with an offset, local date-times without one, binary
/// as base64. A text value that does not parse as its column's temporal type
/// (`infinity`) is kept as text.
pub fn field_to_json(column: &DataColumn, field: &Field) -> Result<Value, FaucetError> {
    let ty = column.type_name.to_ascii_lowercase();
    Ok(match field {
        Field::IsNull(true) => Value::Null,
        Field::IsNull(false) => {
            return Err(unreadable(column, "an isNull field that is false"));
        }
        Field::LongValue(v) => Value::from(*v),
        Field::DoubleValue(v) => floating(&ty, *v),
        Field::BooleanValue(v) => Value::Bool(*v),
        Field::BlobValue(b) => faucet_core::fidelity::binary(b.as_ref()),
        Field::StringValue(s) => text_value(column, &ty, s)?,
        _ => return Err(unreadable(column, "a field of an unknown kind")),
    })
}

fn unreadable(column: &DataColumn, what: &str) -> FaucetError {
    FaucetError::Source(format!(
        "redshift data api: column `{}` ({}) holds {what}",
        column.name, column.type_name
    ))
}

/// A double field: a `float4` keeps its single-precision shortest form.
fn floating(ty: &str, v: f64) -> Value {
    match ty {
        "float4" | "real" => faucet_core::fidelity::float32(v as f32),
        _ => faucet_core::fidelity::float64(v),
    }
}

fn text_value(column: &DataColumn, ty: &str, s: &str) -> Result<Value, FaucetError> {
    use faucet_core::fidelity::{self, TimePrecision};
    let bad = |e: &dyn std::fmt::Display| {
        FaucetError::Source(format!(
            "redshift data api: column `{}` ({}): {e}",
            column.name, column.type_name
        ))
    };
    Ok(match ty {
        "numeric" | "decimal" => fidelity::decimal_text(s).map_err(|e| bad(&e))?,
        "int2" | "int4" | "int8" | "smallint" | "integer" | "bigint" => {
            fidelity::integer_text(s).map_err(|e| bad(&e))?
        }
        "float4" | "float8" | "real" | "double precision" => {
            float_text(ty, s).map_err(|e| bad(&e))?
        }
        "bool" | "boolean" => match s {
            "t" | "true" | "TRUE" => Value::Bool(true),
            "f" | "false" | "FALSE" => Value::Bool(false),
            _ => return Err(bad(&format!("{s:?} is not a boolean"))),
        },
        "timestamptz" | "timestamp with time zone" => {
            chrono::DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f%#z")
                .map(|t| fidelity::timestamp(&t, TimePrecision::Micros))
                .unwrap_or_else(|_| Value::String(s.to_string()))
        }
        "timestamp" | "timestamp without time zone" => {
            chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
                .map(|t| fidelity::timestamp_naive(&t, TimePrecision::Micros))
                .unwrap_or_else(|_| Value::String(s.to_string()))
        }
        "time" | "time without time zone" => chrono::NaiveTime::parse_from_str(s, "%H:%M:%S%.f")
            .map(|t| fidelity::time(&t, TimePrecision::Micros))
            .unwrap_or_else(|_| Value::String(s.to_string())),
        "varbyte" | "varbinary" | "binary varying" | "bytea" => match hex_bytes(s) {
            Some(b) => fidelity::binary(&b),
            None => return Err(bad(&"the value is not hex")),
        },
        "json" | "jsonb" => {
            fidelity::parse_json(s.as_bytes()).unwrap_or_else(|_| Value::String(s.to_string()))
        }
        _ => Value::String(s.to_string()),
    })
}

/// A floating-point column sent as text (`NaN`, `Infinity` included).
fn float_text(ty: &str, s: &str) -> Result<Value, String> {
    let v: f64 = match s.trim() {
        "NaN" => f64::NAN,
        "Infinity" => f64::INFINITY,
        "-Infinity" => f64::NEG_INFINITY,
        t => t.parse().map_err(|_| format!("{s:?} is not a number"))?,
    };
    Ok(floating(ty, v))
}

/// Hex text (`0aff`, or `\x0aff` as PostgreSQL writes `bytea`) as bytes.
fn hex_bytes(s: &str) -> Option<Vec<u8>> {
    let s = s.strip_prefix("\\x").unwrap_or(s);
    if s.len() % 2 != 0 {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_redshiftdata::primitives::Blob;
    use serde_json::json;

    fn col(name: &str, ty: &str) -> DataColumn {
        DataColumn {
            name: name.into(),
            type_name: ty.into(),
            nullable: None,
        }
    }

    #[test]
    fn placeholders_become_named_parameters_with_nulls_and_empties_inline() {
        let st = named_parameters(
            "SELECT * FROM t WHERE a = $1 AND b = CAST($2 AS TIMESTAMP) AND c = $3 AND d = $4 \
             AND e = $1 AND f = $10 AND g = '$2' AND \"$3\" = 1 -- $4\n AND h = $5 /* $5 */ AND x$1 = 2",
            &[
                json!(7),
                json!("2026-01-01"),
                json!(null),
                json!(""),
                json!({"k": [1]}),
            ],
        );
        assert_eq!(
            st.sql,
            "SELECT * FROM t WHERE a = :p1 AND b = CAST(:p2 AS TIMESTAMP) AND c = NULL AND d = '' \
             AND e = :p1 AND f = $10 AND g = '$2' AND \"$3\" = 1 -- $4\n AND h = :p5 /* $5 */ AND x$1 = 2"
        );
        assert_eq!(
            st.parameters,
            vec![
                ("p1".to_string(), "7".to_string()),
                ("p2".to_string(), "2026-01-01".to_string()),
                ("p5".to_string(), r#"{"k":[1]}"#.to_string()),
            ]
        );
        let st = named_parameters("SELECT 'it''s $1', $1, 'unterminated $1", &[json!(true)]);
        assert_eq!(st.sql, "SELECT 'it''s $1', :p1, 'unterminated $1");
        assert_eq!(st.parameters, vec![("p1".into(), "true".into())]);
        assert_eq!(named_parameters("SELECT $1", &[]).sql, "SELECT $1");
        assert_eq!(Statement::new("SELECT 1").parameters, vec![]);
    }

    #[test]
    fn typed_fields_render_through_fidelity() {
        let cases: Vec<(DataColumn, Field, Value)> = vec![
            (col("n", "int8"), Field::IsNull(true), Value::Null),
            (
                col("i", "int8"),
                Field::LongValue(i64::MIN),
                json!(i64::MIN),
            ),
            (col("f", "float8"), Field::DoubleValue(0.1), json!(0.1)),
            (
                col("f4", "float4"),
                Field::DoubleValue(0.1f32 as f64),
                json!(0.1),
            ),
            (col("b", "bool"), Field::BooleanValue(true), json!(true)),
            (
                col("bin", "varbyte"),
                Field::BlobValue(Blob::new(vec![0, 1, 2, 253, 254, 255])),
                json!("AAEC/f7/"),
            ),
            (
                col("d", "numeric"),
                Field::StringValue("12345678901234567890.123456789".into()),
                json!("12345678901234567890.123456789"),
            ),
            (
                col("s", "numeric"),
                Field::StringValue("1.50".into()),
                json!("1.50"),
            ),
            (
                col("big", "int8"),
                Field::StringValue("123".into()),
                json!(123),
            ),
            (
                col("ft", "float8"),
                Field::StringValue("NaN".into()),
                Value::Null,
            ),
            (
                col("ft", "float8"),
                Field::StringValue("1.5".into()),
                json!(1.5),
            ),
            (
                col("bt", "bool"),
                Field::StringValue("f".into()),
                json!(false),
            ),
            (
                col("tz", "timestamptz"),
                Field::StringValue("2026-09-18 12:34:56.123456+05:30".into()),
                json!("2026-09-18T12:34:56.123456+05:30"),
            ),
            (
                col("tz", "timestamptz"),
                Field::StringValue("2026-09-18 12:34:56+00".into()),
                json!("2026-09-18T12:34:56.000000Z"),
            ),
            (
                col("ts", "timestamp"),
                Field::StringValue("1969-07-20 20:17:40".into()),
                json!("1969-07-20T20:17:40.000000"),
            ),
            (
                col("ts", "timestamp"),
                Field::StringValue("infinity".into()),
                json!("infinity"),
            ),
            (
                col("tm", "time"),
                Field::StringValue("01:02:03.5".into()),
                json!("01:02:03.500000"),
            ),
            (
                col("bx", "bytea"),
                Field::StringValue("\\x00ff".into()),
                json!("AP8="),
            ),
            (
                col("js", "json"),
                Field::StringValue(r#"{"a":123456789012345678901234567890}"#.into()),
                json!({"a": "123456789012345678901234567890"}),
            ),
            (
                col("v", "varchar"),
                Field::StringValue("it's".into()),
                json!("it's"),
            ),
            (
                col("dt", "date"),
                Field::StringValue("2026-02-28".into()),
                json!("2026-02-28"),
            ),
        ];
        for (c, f, want) in cases {
            assert_eq!(field_to_json(&c, &f).unwrap(), want, "{c:?} {f:?}");
        }
        for (c, f, needle) in [
            (col("d", "numeric"), Field::StringValue("abc".into()), "`d`"),
            (
                col("b", "bool"),
                Field::StringValue("yes".into()),
                "boolean",
            ),
            (
                col("f", "float8"),
                Field::StringValue("x".into()),
                "not a number",
            ),
            (col("v", "varbyte"), Field::StringValue("abc".into()), "hex"),
            (col("n", "int4"), Field::IsNull(false), "isNull"),
        ] {
            let e = field_to_json(&c, &f).unwrap_err().to_string();
            assert!(e.contains(needle), "{e}");
        }
    }

    #[test]
    fn rows_map_by_column_and_refuse_a_width_mismatch() {
        let cols = [col("id", "int4"), col("name", "varchar")];
        let row = row_to_json(
            &cols,
            &[Field::LongValue(1), Field::StringValue("a".into())],
        )
        .unwrap();
        assert_eq!(Value::Object(row), json!({"id": 1, "name": "a"}));
        assert!(row_to_json(&cols, &[Field::LongValue(1)]).is_err());
    }

    #[test]
    fn data_api_targets_are_validated() {
        let conn = |cfg: Value| -> RedshiftConnection {
            serde_json::from_value(json!({
                "database": "dev",
                "credentials": {"type": "redshift_data_api", "config": cfg}
            }))
            .unwrap()
        };
        let ok = DataApiClient::from_connection(
            &conn(json!({"cluster_identifier": "c", "db_user": "u"})),
            FaucetError::Source,
        )
        .unwrap()
        .unwrap();
        assert!(format!("{ok:?}").contains("Cluster"));
        assert!(
            DataApiClient::from_connection(
                &conn(json!({"workgroup_name": "w"})),
                FaucetError::Source
            )
            .unwrap()
            .is_some()
        );
        assert!(
            DataApiClient::from_connection(
                &RedshiftConnection::new("h", "d", "u", "p"),
                FaucetError::Source
            )
            .unwrap()
            .is_none()
        );
        let mut no_db = conn(json!({"workgroup_name": "w"}));
        no_db.database = String::new();
        for (c, needle) in [
            (no_db, "database"),
            (
                conn(json!({"cluster_identifier": "c"})),
                "exactly one of `db_user`",
            ),
            (
                conn(json!({"cluster_identifier": "c", "db_user": "u", "secret_arn": "a"})),
                "exactly one of `db_user`",
            ),
            (
                conn(json!({"workgroup_name": "w", "db_user": "u"})),
                "provisioned",
            ),
            (conn(json!({})), "exactly one of `cluster_identifier`"),
        ] {
            let e = DataApiClient::from_connection(&c, FaucetError::Source)
                .unwrap_err()
                .to_string();
            assert!(e.contains(needle), "{e}");
        }
    }

    #[test]
    fn hex_decoding() {
        assert_eq!(hex_bytes("0aff"), Some(vec![10, 255]));
        assert_eq!(hex_bytes("\\x"), Some(vec![]));
        assert_eq!(hex_bytes("0"), None);
        assert_eq!(hex_bytes("zz"), None);
    }
}
