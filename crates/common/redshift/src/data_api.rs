//! Statements through the Redshift Data API (`credentials: { type:
//! redshift_data_api }`).
//!
//! A statement is submitted with `ExecuteStatement`, polled with
//! `DescribeStatement` until it is `FINISHED`, `FAILED` or `ABORTED`, and its
//! rows are read page by page with `GetStatementResult`. Every request is one
//! call through the caller's [`CallContext`]: submitting a write is a single
//! attempt (`call_once`), polling waits through [`CallContext::wait`], so the
//! run's deadline and cancel end a long statement (which is then cancelled
//! on the service, best effort). A failed poll is retried
//! ([`POLL_FAILURE_RETRIES`]); when polling gives up, the statement is
//! cancelled too, and for a write the error says its outcome is unknown.
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

/// How many consecutive failed `DescribeStatement` calls are retried before
/// polling gives up on a submitted statement.
pub const POLL_FAILURE_RETRIES: u32 = 4;

/// The first wait after a failed poll; it doubles on each further failure.
pub const POLL_FAILURE_WAIT: Duration = Duration::from_millis(250);

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
    /// statement is an error carrying the service's message. When waiting
    /// stops before the statement finishes, it is cancelled (best effort); a
    /// write whose polling failed is an error naming the statement and saying
    /// its outcome is unknown.
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
            Ok(done) => Ok(done),
            Err(Waited::Finished(e)) => Err(e),
            // The statement may still run: ask the service to cancel it.
            Err(Waited::Abandoned(e)) => {
                cancel_detached(client.clone(), id.clone());
                match e {
                    FaucetError::Timeout { .. } | FaucetError::Cancelled { .. } => Err(e),
                    e if effect == Effect::Write => Err((self.error)(format!(
                        "redshift data api: statement {id} was submitted but polling it failed \
                         ({e}); a cancel was requested, but whether it committed is unknown, so \
                         check the destination before re-running"
                    ))),
                    e => Err(e),
                }
            }
        }
    }

    async fn wait(
        &self,
        calls: &CallContext,
        client: &aws_sdk_redshiftdata::Client,
        id: &str,
    ) -> Result<Executed, Waited> {
        let mut delay = POLL_START;
        let mut failures = 0;
        loop {
            let polled = calls
                .call("poll", || async {
                    client
                        .describe_statement()
                        .id(id)
                        .send()
                        .await
                        .map_err(|e| self.fail("DescribeStatement", e))
                })
                .await;
            let d = match polled {
                Ok(d) => d,
                Err(e @ (FaucetError::Timeout { .. } | FaucetError::Cancelled { .. })) => {
                    return Err(Waited::Abandoned(e));
                }
                // Polling is read-only: keep trying before giving up on a
                // statement that may still finish.
                Err(e) if failures < POLL_FAILURE_RETRIES => {
                    tracing::warn!(statement = %id, error = %e, "redshift data api: polling a statement failed; retrying");
                    calls
                        .wait(POLL_FAILURE_WAIT * 2u32.pow(failures))
                        .await
                        .map_err(Waited::Abandoned)?;
                    failures += 1;
                    continue;
                }
                Err(e) => return Err(Waited::Abandoned(e)),
            };
            failures = 0;
            match d.status() {
                Some(StatusString::Finished) => {
                    return Ok(Executed {
                        id: id.to_string(),
                        has_result_set: d.has_result_set().unwrap_or(false),
                        rows: d.result_rows(),
                    });
                }
                Some(StatusString::Failed) => {
                    return Err(Waited::Finished((self.error)(format!(
                        "redshift data api: statement failed: {}",
                        d.error().unwrap_or("the service gave no reason")
                    ))));
                }
                Some(StatusString::Aborted) => {
                    return Err(Waited::Finished((self.error)(
                        "redshift data api: statement was aborted".into(),
                    )));
                }
                _ => {}
            }
            calls.wait(delay).await.map_err(Waited::Abandoned)?;
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

/// How waiting for a submitted statement ended, when it did not finish.
enum Waited {
    /// The service reported it failed or was aborted: nothing is left running.
    Finished(FaucetError),
    /// Waiting stopped (deadline, cancel, polling failed) while the statement
    /// may still be running.
    Abandoned(FaucetError),
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
    if !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok())
        .collect()
}

#[cfg(test)]
mod tests;
