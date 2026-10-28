//! Test doubles shared by the Redshift crates' integration tests (included
//! with `#[path]` by the source and sink tests).
//!
//! - [`DataApiFake`]: a stateful contract fake of the Redshift Data API
//!   (`redshift-data`, JSON 1.1 over `X-Amz-Target: RedshiftData.*`). Each
//!   `ExecuteStatement` runs against a real PostgreSQL container, so the SQL a
//!   connector sends is checked for real; statements move SUBMITTED → STARTED
//!   → FINISHED (or FAILED with the server's error), and results come back
//!   paged as typed fields, the way the service renders them: integers as
//!   `longValue`, floats as `doubleValue`, booleans as `booleanValue`, NULL as
//!   `isNull`, `bytea` as `blobValue`, everything else as its text in
//!   `stringValue`. Parameters (`:pN`, string values) are written into the SQL
//!   as quoted literals, which Redshift's implicit conversion matches.
//! - [`credentials_response`] / [`serverless_response`]: the AWS answers of
//!   `GetClusterCredentials` (Query protocol, XML) and serverless
//!   `GetCredentials` (JSON 1.1).
#![allow(dead_code, clippy::disallowed_methods)]

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, mpsc};

use base64::Engine as _;
use serde_json::{Value, json};
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use wiremock::{Request, Respond, ResponseTemplate};

pub const REGION: &str = "us-east-1";
pub const ACCESS_KEY: &str = "AKIDFAUCETTEST";

/// A PostgreSQL container (password auth: user/password `postgres`).
pub async fn start_postgres() -> (ContainerAsync<Postgres>, u16) {
    let container: ContainerAsync<Postgres> =
        faucet_conformance::containers::start(|| Postgres::default().with_tag("16-alpine")).await;
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    (container, port)
}

pub fn admin_url(port: u16) -> String {
    format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres")
}

/// A result set: `(name, type name)` columns and rows of Data API fields.
type ResultSet = (Vec<(String, String)>, Vec<Vec<Value>>);

/// Runs SQL on its own thread and runtime, so a synchronous wiremock
/// responder can wait for it.
struct PgExec {
    tx: Mutex<mpsc::Sender<(String, mpsc::Sender<Result<ResultSet, String>>)>>,
}

impl PgExec {
    fn new(url: String) -> Self {
        let (tx, rx) = mpsc::channel::<(String, mpsc::Sender<Result<ResultSet, String>>)>();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let pool = rt.block_on(sqlx::PgPool::connect(&url)).expect("fake pool");
            while let Ok((sql, reply)) = rx.recv() {
                let _ = reply.send(rt.block_on(run(&pool, &sql)));
            }
        });
        Self { tx: Mutex::new(tx) }
    }

    fn run(&self, sql: &str) -> Result<ResultSet, String> {
        let (reply, rx) = mpsc::channel();
        self.tx
            .lock()
            .unwrap()
            .send((sql.to_string(), reply))
            .unwrap();
        rx.recv().unwrap()
    }
}

async fn run(pool: &sqlx::PgPool, sql: &str) -> Result<ResultSet, String> {
    use sqlx::{Column, Executor, Row, TypeInfo, ValueRef};
    let rows = sqlx::raw_sql(sql)
        .fetch_all(pool)
        .await
        .map_err(|e| e.to_string())?;
    let columns: Vec<(String, String)> = match rows.first() {
        Some(r) => r
            .columns()
            .iter()
            .map(|c| (c.name().to_string(), c.type_info().name().to_lowercase()))
            .collect(),
        None => match pool.describe(sql).await {
            Ok(d) => d
                .columns()
                .iter()
                .map(|c| (c.name().to_string(), c.type_info().name().to_lowercase()))
                .collect(),
            Err(_) => Vec::new(),
        },
    };
    let mut out = Vec::with_capacity(rows.len());
    for row in &rows {
        let mut fields = Vec::with_capacity(columns.len());
        for (i, (_, ty)) in columns.iter().enumerate() {
            let raw = row.try_get_raw(i).map_err(|e| e.to_string())?;
            if raw.is_null() {
                fields.push(json!({"isNull": true}));
                continue;
            }
            let text = raw.as_str().map_err(|e| e.to_string())?.to_string();
            fields.push(field(ty, &text));
        }
        out.push(fields);
    }
    Ok((columns, out))
}

/// One cell as the Data API renders it.
fn field(ty: &str, text: &str) -> Value {
    match ty {
        "int2" | "int4" | "int8" => json!({"longValue": text.parse::<i64>().unwrap()}),
        "float4" | "float8" => match text.parse::<f64>() {
            Ok(v) if v.is_finite() => json!({"doubleValue": v}),
            _ => json!({"stringValue": text}),
        },
        "bool" => json!({"booleanValue": text == "t"}),
        "bytea" => {
            let hex = text.strip_prefix("\\x").unwrap_or(text);
            let bytes: Vec<u8> = (0..hex.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
                .collect();
            json!({"blobValue": base64::engine::general_purpose::STANDARD.encode(bytes)})
        }
        _ => json!({"stringValue": text}),
    }
}

/// `:pN` parameters written into `sql` as quoted literals.
pub fn bind(sql: &str, params: &[(String, String)]) -> String {
    let by_name: HashMap<&str, &str> = params
        .iter()
        .map(|(n, v)| (n.as_str(), v.as_str()))
        .collect();
    let mut out = String::new();
    let mut chars = sql.char_indices().peekable();
    let mut quote: Option<char> = None;
    while let Some((i, c)) = chars.next() {
        if let Some(q) = quote {
            out.push(c);
            if c == q {
                quote = None;
            }
            continue;
        }
        if c == '\'' || c == '"' {
            quote = Some(c);
            out.push(c);
            continue;
        }
        if c == ':' && sql[i + 1..].starts_with('p') && !sql[..i].ends_with(':') {
            let digits: String = sql[i + 2..]
                .chars()
                .take_while(|d| d.is_ascii_digit())
                .collect();
            if let Some(v) = by_name.get(format!("p{digits}").as_str())
                && !digits.is_empty()
            {
                out.push_str(&format!("'{}'", v.replace('\'', "''")));
                for _ in 0..=digits.len() {
                    chars.next();
                }
                continue;
            }
        }
        out.push(c);
    }
    out
}

/// One statement the fake accepted.
#[derive(Clone, Debug)]
pub struct Submitted {
    /// The request body of `ExecuteStatement`.
    pub body: Value,
    /// The SQL with its parameters written in.
    pub bound_sql: String,
}

struct Statement {
    statuses: VecDeque<&'static str>,
    error: Option<String>,
    result: Option<ResultSet>,
}

#[derive(Default)]
struct State {
    next: u64,
    statements: HashMap<String, Statement>,
    submitted: Vec<Submitted>,
    targets: Vec<String>,
    authorizations: Vec<String>,
    cancelled: Vec<String>,
}

/// The Redshift Data API contract fake.
#[derive(Clone)]
pub struct DataApiFake {
    state: Arc<Mutex<State>>,
    pg: Arc<PgExec>,
    page_size: usize,
    stall: bool,
}

impl DataApiFake {
    /// A fake running statements on the PostgreSQL behind `url`, serving
    /// `page_size` rows per `GetStatementResult` page.
    pub fn new(url: String, page_size: usize) -> Self {
        Self {
            state: Arc::default(),
            pg: Arc::new(PgExec::new(url)),
            page_size,
            stall: false,
        }
    }

    /// Statements never leave STARTED (deadline and cancel tests).
    pub fn stalled(mut self) -> Self {
        self.stall = true;
        self
    }

    /// Mount on `server` (every `POST /`).
    pub async fn mount(&self, server: &wiremock::MockServer) {
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .respond_with(self.clone())
            .mount(server)
            .await;
    }

    pub fn submitted(&self) -> Vec<Submitted> {
        self.state.lock().unwrap().submitted.clone()
    }

    /// The `X-Amz-Target` of every request, in order.
    pub fn targets(&self) -> Vec<String> {
        self.state.lock().unwrap().targets.clone()
    }

    pub fn authorizations(&self) -> Vec<String> {
        self.state.lock().unwrap().authorizations.clone()
    }

    pub fn cancelled(&self) -> Vec<String> {
        self.state.lock().unwrap().cancelled.clone()
    }
}

fn amz_json(body: Value) -> ResponseTemplate {
    ResponseTemplate::new(200)
        .insert_header("content-type", "application/x-amz-json-1.1")
        .set_body_json(body)
}

fn amz_error(kind: &str, message: &str) -> ResponseTemplate {
    ResponseTemplate::new(400)
        .insert_header("content-type", "application/x-amz-json-1.1")
        .set_body_json(json!({"__type": kind, "message": message}))
}

impl Respond for DataApiFake {
    fn respond(&self, req: &Request) -> ResponseTemplate {
        let target = req
            .headers
            .get("x-amz-target")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_string();
        let body: Value = serde_json::from_slice(&req.body).unwrap_or(Value::Null);
        let mut st = self.state.lock().unwrap();
        st.targets.push(target.clone());
        if let Some(a) = req
            .headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
        {
            st.authorizations.push(a.to_string());
        }
        match target.as_str() {
            "RedshiftData.ExecuteStatement" => {
                let Some(sql) = body["Sql"].as_str() else {
                    return amz_error("ValidationException", "Sql is required");
                };
                if body["Database"].as_str().is_none_or(str::is_empty) {
                    return amz_error("ValidationException", "Database is required");
                }
                let params: Vec<(String, String)> = body["Parameters"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .map(|p| {
                        (
                            p["name"].as_str().unwrap().to_string(),
                            p["value"].as_str().unwrap().to_string(),
                        )
                    })
                    .collect();
                if params.iter().any(|(_, v)| v.is_empty()) {
                    return amz_error("ValidationException", "parameter value must not be empty");
                }
                let bound_sql = bind(sql, &params);
                st.next += 1;
                let id = format!("stmt-{}", st.next);
                st.submitted.push(Submitted {
                    body: body.clone(),
                    bound_sql: bound_sql.clone(),
                });
                drop(st);
                let outcome = self.pg.run(&bound_sql);
                let mut st = self.state.lock().unwrap();
                let statement = match outcome {
                    Ok(result) => Statement {
                        statuses: if self.stall {
                            VecDeque::from(["SUBMITTED", "STARTED"])
                        } else {
                            VecDeque::from(["SUBMITTED", "STARTED", "FINISHED"])
                        },
                        error: None,
                        result: Some(result),
                    },
                    Err(e) => Statement {
                        statuses: VecDeque::from(["SUBMITTED", "FAILED"]),
                        error: Some(e),
                        result: None,
                    },
                };
                st.statements.insert(id.clone(), statement);
                amz_json(json!({"Id": id, "Database": body["Database"]}))
            }
            "RedshiftData.DescribeStatement" => {
                let id = body["Id"].as_str().unwrap_or_default().to_string();
                let Some(s) = st.statements.get_mut(&id) else {
                    return amz_error("ResourceNotFoundException", "no such statement");
                };
                let status = if s.statuses.len() > 1 {
                    s.statuses.pop_front().unwrap()
                } else {
                    s.statuses[0]
                };
                let rows = s.result.as_ref().map_or(0, |(_, r)| r.len());
                let mut out = json!({
                    "Id": id,
                    "Status": status,
                    "HasResultSet": s.result.as_ref().is_some_and(|(c, _)| !c.is_empty()),
                    "ResultRows": rows,
                });
                if status == "FAILED" {
                    out["Error"] = json!(s.error.clone().unwrap_or_default());
                }
                amz_json(out)
            }
            "RedshiftData.GetStatementResult" => {
                let id = body["Id"].as_str().unwrap_or_default();
                let Some((columns, rows)) = st.statements.get(id).and_then(|s| s.result.clone())
                else {
                    return amz_error("ResourceNotFoundException", "no result");
                };
                let start: usize = body["NextToken"]
                    .as_str()
                    .map_or(0, |t| t.trim_start_matches("page-").parse().unwrap());
                let end = (start + self.page_size).min(rows.len());
                let mut out = json!({
                    "Records": rows[start..end],
                    "ColumnMetadata": columns
                        .iter()
                        .map(|(n, t)| json!({"name": n, "label": n, "typeName": t, "nullable": 2}))
                        .collect::<Vec<_>>(),
                    "TotalNumRows": rows.len(),
                });
                if end < rows.len() {
                    out["NextToken"] = json!(format!("page-{end}"));
                }
                amz_json(out)
            }
            "RedshiftData.CancelStatement" => {
                st.cancelled
                    .push(body["Id"].as_str().unwrap_or_default().to_string());
                amz_json(json!({"Status": true}))
            }
            other => amz_error("UnknownOperationException", other),
        }
    }
}

/// The `GetClusterCredentials` answer (Query protocol): `user` and `password`,
/// expiring `lifetime` from now.
pub fn credentials_response(
    user: &str,
    password: &str,
    lifetime: std::time::Duration,
) -> ResponseTemplate {
    let expires = chrono::Utc::now() + chrono::Duration::from_std(lifetime).unwrap();
    ResponseTemplate::new(200)
        .insert_header("content-type", "text/xml")
        .set_body_string(format!(
            "<GetClusterCredentialsResponse xmlns=\"http://redshift.amazonaws.com/doc/2012-12-01/\">\
             <GetClusterCredentialsResult><DbUser>{user}</DbUser><DbPassword>{password}</DbPassword>\
             <Expiration>{}</Expiration></GetClusterCredentialsResult>\
             <ResponseMetadata><RequestId>req-1</RequestId></ResponseMetadata>\
             </GetClusterCredentialsResponse>",
            expires.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
        ))
}

/// The serverless `GetCredentials` answer (JSON 1.1).
pub fn serverless_response(
    user: &str,
    password: &str,
    lifetime: std::time::Duration,
) -> ResponseTemplate {
    let expires = chrono::Utc::now() + chrono::Duration::from_std(lifetime).unwrap();
    amz_json(json!({
        "dbUser": user,
        "dbPassword": password,
        "expiration": expires.timestamp(),
        "nextRefreshTime": expires.timestamp(),
    }))
}

/// The form fields of a Query-protocol request.
pub fn form(req: &Request) -> HashMap<String, String> {
    url::form_urlencoded::parse(&req.body)
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect()
}

/// Static access-key credentials for the fakes, in config form.
pub fn access_key() -> Value {
    json!({"type": "access_key", "config": {"access_key_id": ACCESS_KEY, "secret_access_key": "secret"}})
}
