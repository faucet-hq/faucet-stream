//! `write_strategy: copy` end to end (#967): the sink stages each chunk to S3
//! (MinIO) and issues Redshift's `COPY … FROM 's3://…' IAM_ROLE …`.
//!
//! Redshift has no local image, so the sink talks to Postgres through a
//! wire-protocol proxy that plays the part of Redshift's `COPY`: it records the
//! statement, reads the staged objects (through the manifest when there is
//! one) from MinIO, and runs the load they describe — `FORMAT AS JSON 'auto
//! ignorecase'` maps keys to columns ignoring case, `FORMAT AS CSV NULL AS …`
//! reads RFC-4180 cells with the unquoted marker as NULL. Every other message
//! passes through untouched.

#[path = "../../../common/redshift/tests/support/mod.rs"]
mod support;

use std::sync::{Arc, Mutex};

use aws_sdk_s3::Client as S3Client;
use faucet_common_redshift::{RedshiftConnection, RedshiftCredentials};
use faucet_conformance::stores;
use faucet_core::{FaucetError, Sink};
use faucet_sink_redshift::config::RedshiftCopySpec;
use faucet_sink_redshift::{
    RedshiftCopyFormat, RedshiftSink, RedshiftSinkConfig, RedshiftWriteStrategy,
};
use serde_json::{Map, Value, json};
use sqlx::Row;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const BUCKET: &str = "redshift-stage";
const ROLE: &str = "arn:aws:iam::123456789012:role/redshift-load";

fn s3_client(endpoint: &str) -> S3Client {
    let creds = aws_sdk_s3::config::Credentials::new(
        stores::MINIO_ACCESS_KEY,
        stores::MINIO_SECRET_KEY,
        None,
        None,
        "test",
    );
    let conf = aws_sdk_s3::config::Builder::new()
        .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
        .region(aws_sdk_s3::config::Region::new(stores::MINIO_REGION))
        .endpoint_url(endpoint)
        .credentials_provider(creds)
        .force_path_style(true)
        .build();
    S3Client::from_conf(conf)
}

async fn get(s3: &S3Client, uri: &str) -> Vec<u8> {
    let rest = uri.strip_prefix("s3://").expect("s3 uri");
    let (bucket, key) = rest.split_once('/').expect("bucket/key");
    let obj = s3
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .unwrap_or_else(|e| panic!("get {uri}: {e:?}"));
    obj.body.collect().await.unwrap().into_bytes().to_vec()
}

/// What the proxy saw: each `COPY` statement and each manifest it read.
#[derive(Default)]
struct Seen {
    copies: Vec<String>,
    manifests: Vec<Value>,
}

/// The single-quoted literal that starts at `s` (`'…'` with `''` escapes).
fn literal(s: &str) -> String {
    let mut out = String::new();
    let mut chars = s.strip_prefix('\'').expect("literal").chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\'' {
            if chars.peek() == Some(&'\'') {
                chars.next();
                out.push('\'');
            } else {
                break;
            }
        } else {
            out.push(c);
        }
    }
    out
}

fn quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// One CSV line as `(cell, was_quoted)` pairs.
fn csv_cells(line: &str) -> Vec<(String, bool)> {
    let mut cells = Vec::new();
    let mut chars = line.chars().peekable();
    loop {
        let mut cell = String::new();
        let quoted = chars.peek() == Some(&'"');
        if quoted {
            chars.next();
            while let Some(c) = chars.next() {
                if c == '"' {
                    if chars.peek() == Some(&'"') {
                        chars.next();
                        cell.push('"');
                    } else {
                        break;
                    }
                } else {
                    cell.push(c);
                }
            }
        }
        while let Some(&c) = chars.peek() {
            if c == ',' {
                break;
            }
            cell.push(c);
            chars.next();
        }
        cells.push((cell, quoted));
        if chars.next().is_none() {
            return cells;
        }
    }
}

/// Split CSV text into records, keeping newlines inside quoted cells.
fn csv_records(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut in_quotes = false;
    for c in text.chars() {
        if c == '"' {
            in_quotes = !in_quotes;
        }
        if c == '\n' && !in_quotes {
            out.push(std::mem::take(&mut cur));
        } else {
            cur.push(c);
        }
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

/// Rewrite a Redshift `COPY` into the Postgres statement that loads the same
/// rows, reading the staged objects from S3.
async fn emulate_copy(sql: &str, s3: &S3Client, seen: &Mutex<Seen>) -> String {
    seen.lock().unwrap().copies.push(sql.to_string());
    let body = sql.strip_prefix("COPY ").expect("COPY");
    let from = body.find(" FROM '").expect("FROM");
    let (head, rest) = body.split_at(from);
    let (table, columns) = match head.find(" (") {
        Some(i) => (&head[..i], Some(head[i + 2..head.len() - 1].to_string())),
        None => (head, None),
    };
    let source = literal(&rest[" FROM ".len()..]);
    let uris: Vec<String> = if rest.trim_end().ends_with(" MANIFEST") {
        let manifest: Value = serde_json::from_slice(&get(s3, &source).await).unwrap();
        seen.lock().unwrap().manifests.push(manifest.clone());
        manifest["entries"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| e["url"].as_str().unwrap().to_string())
            .collect()
    } else {
        vec![source]
    };
    let mut bodies = Vec::new();
    for uri in &uris {
        bodies.push(String::from_utf8(get(s3, uri).await).unwrap());
    }
    if rest.contains("FORMAT AS JSON 'auto ignorecase'") {
        let rows: Vec<Value> = bodies
            .iter()
            .flat_map(|b| b.lines().filter(|l| !l.is_empty()).map(str::to_string))
            .map(|l| {
                let obj: Map<String, Value> = serde_json::from_str(&l).unwrap();
                Value::Object(
                    obj.into_iter()
                        .map(|(k, v)| (k.to_lowercase(), v))
                        .collect(),
                )
            })
            .collect();
        format!(
            "INSERT INTO {table} SELECT * FROM json_populate_recordset(NULL::{table}, {})",
            quote(&Value::Array(rows).to_string())
        )
    } else {
        let null_at = rest.find("NULL AS ").expect("NULL AS");
        let null = literal(&rest[null_at + "NULL AS ".len()..]);
        let rows: Vec<String> = bodies
            .iter()
            .flat_map(|b| csv_records(b))
            .map(|line| {
                let cells: Vec<String> = csv_cells(&line)
                    .into_iter()
                    .map(|(cell, quoted)| {
                        if !quoted && cell == null {
                            "NULL".to_string()
                        } else {
                            quote(&cell)
                        }
                    })
                    .collect();
                format!("({})", cells.join(", "))
            })
            .collect();
        format!(
            "INSERT INTO {table} ({}) VALUES {}",
            columns.expect("CSV COPY names its columns"),
            rows.join(", ")
        )
    }
}

fn cstr(buf: &[u8]) -> (&str, usize) {
    let end = buf.iter().position(|b| *b == 0).expect("cstring");
    (std::str::from_utf8(&buf[..end]).unwrap(), end + 1)
}

fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![tag];
    out.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
    out.extend_from_slice(body);
    out
}

/// Forward one client connection to Postgres, emulating any `COPY … FROM 's3://`.
async fn proxy_conn(client: TcpStream, upstream: u16, s3: S3Client, seen: Arc<Mutex<Seen>>) {
    let server = TcpStream::connect(("127.0.0.1", upstream)).await.unwrap();
    let (mut cr, mut cw) = client.into_split();
    let (mut sr, mut sw) = server.into_split();
    tokio::spawn(async move {
        let _ = tokio::io::copy(&mut sr, &mut cw).await;
    });
    let mut len = [0u8; 4];
    loop {
        if cr.read_exact(&mut len).await.is_err() {
            return;
        }
        let mut startup = vec![0u8; u32::from_be_bytes(len) as usize - 4];
        cr.read_exact(&mut startup).await.unwrap();
        sw.write_all(&len).await.unwrap();
        sw.write_all(&startup).await.unwrap();
        // An SSLRequest (refused by this plaintext server) precedes the real startup.
        if startup[..4] != 80877103u32.to_be_bytes() {
            break;
        }
    }
    loop {
        let mut tag = [0u8; 1];
        if cr.read_exact(&mut tag).await.is_err() {
            return;
        }
        cr.read_exact(&mut len).await.unwrap();
        let mut body = vec![0u8; u32::from_be_bytes(len) as usize - 4];
        cr.read_exact(&mut body).await.unwrap();
        let out = match tag[0] {
            b'P' => {
                let (name, a) = cstr(&body);
                let (query, b) = cstr(&body[a..]);
                if query.starts_with("COPY ") && query.contains(" FROM 's3://") {
                    let sql = emulate_copy(query, &s3, &seen).await;
                    let mut nb = Vec::new();
                    nb.extend_from_slice(name.as_bytes());
                    nb.push(0);
                    nb.extend_from_slice(sql.as_bytes());
                    nb.push(0);
                    nb.extend_from_slice(&body[a + b..]);
                    frame(b'P', &nb)
                } else {
                    frame(b'P', &body)
                }
            }
            b'Q' => {
                let (query, _) = cstr(&body);
                if query.starts_with("COPY ") && query.contains(" FROM 's3://") {
                    let mut nb = emulate_copy(query, &s3, &seen).await.into_bytes();
                    nb.push(0);
                    frame(b'Q', &nb)
                } else {
                    frame(b'Q', &body)
                }
            }
            t => frame(t, &body),
        };
        if sw.write_all(&out).await.is_err() {
            return;
        }
    }
}

async fn start_proxy(upstream: u16, s3: S3Client, seen: Arc<Mutex<Seen>>) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        while let Ok((conn, _)) = listener.accept().await {
            tokio::spawn(proxy_conn(conn, upstream, s3.clone(), seen.clone()));
        }
    });
    port
}

struct Env {
    _pg: testcontainers::ContainerAsync<Postgres>,
    _minio: stores::Store,
    pool: sqlx::PgPool,
    s3: S3Client,
    minio: String,
    proxy: u16,
    pg_port: u16,
    seen: Arc<Mutex<Seen>>,
}

async fn env() -> Env {
    // The sink's S3 client takes the default credential chain.
    unsafe {
        std::env::set_var("AWS_ACCESS_KEY_ID", stores::MINIO_ACCESS_KEY);
        std::env::set_var("AWS_SECRET_ACCESS_KEY", stores::MINIO_SECRET_KEY);
        std::env::set_var("AWS_EC2_METADATA_DISABLED", "true");
    }
    let pg =
        faucet_conformance::containers::start(|| Postgres::default().with_tag("16-alpine")).await;
    let pg_port = pg.get_host_port_ipv4(5432).await.unwrap();
    let minio = stores::minio().await;
    let s3 = s3_client(&minio.endpoint);
    s3.create_bucket().bucket(BUCKET).send().await.unwrap();
    let pool = sqlx::PgPool::connect(&format!(
        "postgres://postgres:postgres@127.0.0.1:{pg_port}/postgres"
    ))
    .await
    .unwrap();
    let seen = Arc::new(Mutex::new(Seen::default()));
    let proxy = start_proxy(pg_port, s3.clone(), seen.clone()).await;
    Env {
        _pg: pg,
        pool,
        s3,
        minio: minio.endpoint.clone(),
        _minio: minio,
        proxy,
        pg_port,
        seen,
    }
}

fn config(
    env: &Env,
    table: &str,
    format: RedshiftCopyFormat,
    batch_size: usize,
) -> RedshiftSinkConfig {
    let mut conn = RedshiftConnection::new("127.0.0.1", "postgres", "postgres", "postgres");
    conn.port = env.proxy;
    conn.tls = false;
    RedshiftSinkConfig {
        connection: conn,
        table_name: table.into(),
        create_table: false,
        commit_rows: None,
        commit_bytes: None,
        schema: None,
        write_strategy: RedshiftWriteStrategy::Copy,
        copy: Some(RedshiftCopySpec {
            format,
            staging_bucket: Some(BUCKET.into()),
            staging_prefix: "loads/".into(),
            iam_role: Some(ROLE.into()),
            region: Some(stores::MINIO_REGION.into()),
            endpoint_url: Some(env.minio.replace("127.0.0.1", "localhost")),
        }),
        copy_format: RedshiftCopyFormat::Jsonl,
        staging_bucket: None,
        staging_prefix: String::new(),
        iam_role: None,
        region: None,
        endpoint_url: None,
        batch_size,
        max_connections: 2,
    }
}

async fn staged_keys(env: &Env) -> Vec<String> {
    let listed = env
        .s3
        .list_objects_v2()
        .bucket(BUCKET)
        .send()
        .await
        .unwrap();
    listed
        .contents()
        .iter()
        .filter_map(|o| o.key().map(str::to_string))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn jsonl_copy_stages_chunks_through_a_manifest_and_loads_every_row() {
    let env = env().await;
    sqlx::query("CREATE TABLE events (id BIGINT, userid TEXT, amount DOUBLE PRECISION)")
        .execute(&env.pool)
        .await
        .unwrap();
    let sink = RedshiftSink::new(config(&env, "events", RedshiftCopyFormat::Jsonl, 2))
        .await
        .unwrap();
    let records: Vec<Value> = (1..=5)
        .map(|i| json!({"id": i, "userId": format!("u{i}"), "amount": i as f64 + 0.5}))
        .collect();
    assert_eq!(sink.write_batch(&records).await.unwrap(), 5);
    sink.flush().await.unwrap();

    let rows = sqlx::query("SELECT id, userid, amount FROM events ORDER BY id")
        .fetch_all(&env.pool)
        .await
        .unwrap();
    let got: Vec<(i64, String, f64)> = rows
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect();
    let want: Vec<(i64, String, f64)> = (1..=5)
        .map(|i| (i, format!("u{i}"), i as f64 + 0.5))
        .collect();
    assert_eq!(got, want, "ignorecase maps userId to userid");

    {
        let seen = env.seen.lock().unwrap();
        assert_eq!(
            seen.copies.len(),
            1,
            "one COPY per group: {:?}",
            seen.copies
        );
        let sql = &seen.copies[0];
        assert!(
            sql.starts_with("COPY \"events\" FROM 's3://redshift-stage/loads/"),
            "{sql}"
        );
        assert!(sql.contains(&format!("IAM_ROLE '{ROLE}'")), "{sql}");
        assert!(sql.contains("FORMAT AS JSON 'auto ignorecase'"), "{sql}");
        assert!(sql.contains("TIMEFORMAT 'auto' DATEFORMAT 'auto'"), "{sql}");
        assert!(sql.ends_with(" REGION 'us-east-1' MANIFEST"), "{sql}");
        let entries = seen.manifests[0]["entries"].as_array().unwrap();
        assert_eq!(entries.len(), 3, "5 rows at batch_size 2 stage 3 objects");
        assert!(entries.iter().all(|e| e["mandatory"] == true));
        assert!(entries.iter().all(|e| {
            e["url"]
                .as_str()
                .unwrap()
                .starts_with("s3://redshift-stage/loads/")
                && e["url"].as_str().unwrap().ends_with(".jsonl")
        }));
    }
    assert_eq!(
        staged_keys(&env).await,
        Vec::<String>::new(),
        "staged objects are removed"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn csv_copy_round_trips_nulls_quotes_and_the_null_marker() {
    let env = env().await;
    sqlx::query("CREATE TABLE notes (id BIGINT, note TEXT, extra TEXT)")
        .execute(&env.pool)
        .await
        .unwrap();
    let sink = RedshiftSink::new(config(&env, "notes", RedshiftCopyFormat::Csv, 0))
        .await
        .unwrap();
    let records = vec![
        json!({"id": 1, "note": "a, \"quoted\"\nline", "extra": null}),
        json!({"id": 2, "note": "", "extra": "x"}),
        json!({"id": 3, "note": "__FAUCET_NULL__"}),
    ];
    assert_eq!(sink.write_batch(&records).await.unwrap(), 3);
    sink.flush().await.unwrap();

    let rows = sqlx::query("SELECT id, note, extra FROM notes ORDER BY id")
        .fetch_all(&env.pool)
        .await
        .unwrap();
    let got: Vec<(i64, Option<String>, Option<String>)> = rows
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect();
    assert_eq!(
        got,
        vec![
            (1, Some("a, \"quoted\"\nline".into()), None),
            (2, Some(String::new()), Some("x".into())),
            (3, Some("__FAUCET_NULL__".into()), None),
        ]
    );
    let seen = env.seen.lock().unwrap();
    let sql = &seen.copies[0];
    assert!(
        sql.starts_with(
            "COPY \"notes\" (\"id\", \"note\", \"extra\") FROM 's3://redshift-stage/loads/"
        ),
        "{sql}"
    );
    assert!(
        sql.contains(".csv' IAM_ROLE"),
        "one object, no manifest: {sql}"
    );
    assert!(
        sql.contains("FORMAT AS CSV NULL AS '__FAUCET_NULL__'"),
        "{sql}"
    );
    assert!(!sql.contains("MANIFEST"), "{sql}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_copy_surfaces_the_error_and_removes_its_staged_objects() {
    let env = env().await;
    sqlx::query("CREATE TABLE typed (id BIGINT)")
        .execute(&env.pool)
        .await
        .unwrap();
    let sink = RedshiftSink::new(config(&env, "typed", RedshiftCopyFormat::Jsonl, 0))
        .await
        .unwrap();
    sink.write_batch(&[json!({"id": "not a number"})])
        .await
        .unwrap();
    let err = sink.flush().await.unwrap_err().to_string();
    assert!(err.contains("redshift: COPY failed"), "{err}");
    assert!(err.contains("bigint"), "{err}");
    assert_eq!(env.seen.lock().unwrap().copies.len(), 1);
    assert_eq!(staged_keys(&env).await, Vec::<String>::new());
}

/// An `iam` or `redshift_data_api` config missing its target is refused at
/// build time, before any connection, AWS or S3 call.
#[tokio::test]
async fn incomplete_iam_and_data_api_credentials_are_refused_before_any_io() {
    for (creds, want) in [
        (
            json!({"type": "iam", "config": {"region": "us-east-1", "db_user": "admin"}}),
            "set exactly one of `cluster_identifier` (provisioned) or `workgroup_name`",
        ),
        (
            json!({"type": "redshift_data_api", "config": {"region": "us-east-1", "cluster_identifier": "c"}}),
            "a cluster needs exactly one of `db_user`",
        ),
    ] {
        let mut conn = RedshiftConnection::new("127.0.0.1", "dev", "admin", "unused");
        conn.port = 1;
        conn.credentials = serde_json::from_value::<RedshiftCredentials>(creds).unwrap();
        let cfg = RedshiftSinkConfig {
            connection: conn,
            table_name: "t".into(),
            create_table: false,
            commit_rows: None,
            commit_bytes: None,
            schema: None,
            write_strategy: RedshiftWriteStrategy::Insert,
            copy: None,
            copy_format: RedshiftCopyFormat::Jsonl,
            staging_bucket: None,
            staging_prefix: String::new(),
            iam_role: None,
            region: None,
            endpoint_url: None,
            batch_size: 0,
            max_connections: 1,
        };
        match RedshiftSink::new(cfg).await {
            Err(FaucetError::Config(msg)) => assert!(msg.contains(want), "{msg}"),
            Err(other) => panic!("unexpected error {other:?}"),
            Ok(_) => panic!("{want}: must be refused"),
        }
    }
}

/// `copy` through the Data API: the chunk is staged to S3, the `COPY` runs as
/// a Data API statement, and the staged object is removed whether the load
/// finished or failed.
#[tokio::test(flavor = "multi_thread")]
async fn copy_through_the_data_api_runs_copy_as_a_statement_and_cleans_up() {
    let env = env().await;
    sqlx::query("CREATE TABLE events (id BIGINT, name TEXT)")
        .execute(&env.pool)
        .await
        .unwrap();
    for (outcome, want) in [
        (Ok(()), None),
        (
            Err("S3ServiceException: Access Denied".to_string()),
            Some("Access Denied"),
        ),
    ] {
        let server = wiremock::MockServer::start().await;
        let fake = support::DataApiFake::new(support::admin_url(env.pg_port), 100).copying(outcome);
        fake.mount(&server).await;
        let mut cfg = config(&env, "events", RedshiftCopyFormat::Jsonl, 0);
        cfg.connection.credentials = serde_json::from_value(json!({
            "type": "redshift_data_api",
            "config": {"workgroup_name": "wg", "region": stores::MINIO_REGION,
                       "aws_credentials": support::access_key(), "endpoint_url": server.uri()}
        }))
        .unwrap();
        let sink = RedshiftSink::new(cfg).await.unwrap();
        sink.write_batch(&[json!({"id": 1, "name": "a"}), json!({"id": 2})])
            .await
            .unwrap();
        let flushed = sink.flush().await;
        let copy = fake
            .submitted()
            .into_iter()
            .map(|s| s.body)
            .find(|b| b["Sql"].as_str().is_some_and(|s| s.starts_with("COPY ")))
            .expect("a COPY statement");
        let sql = copy["Sql"].as_str().unwrap();
        assert!(
            sql.starts_with(&format!("COPY \"events\" FROM 's3://{BUCKET}/loads/")),
            "{sql}"
        );
        assert!(
            sql.contains(&format!(
                "IAM_ROLE '{ROLE}' FORMAT AS JSON 'auto ignorecase'"
            )),
            "{sql}"
        );
        assert_eq!(copy["WorkgroupName"], "wg");
        match want {
            None => flushed.unwrap(),
            Some(needle) => {
                let err = flushed.unwrap_err().to_string();
                assert!(err.contains("redshift: COPY failed"), "{err}");
                assert!(err.contains(needle), "{err}");
            }
        }
        assert_eq!(staged_keys(&env).await, Vec::<String>::new());
        assert!(
            env.seen.lock().unwrap().copies.is_empty(),
            "nothing went over the wire"
        );
    }
}
