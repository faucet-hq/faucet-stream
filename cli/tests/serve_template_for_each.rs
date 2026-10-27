//! `for_each:` streams over a list param (#952), end to end over HTTP: a
//! source template repeating a SQLite table read per caller-chosen table
//! registers and passes its tests, a trigger with three tables runs three
//! rows — each into its own destination, with its own row in the run record
//! — a failing table is reported on its own row without hiding the others,
//! and duplicate, invalid and over-limit lists are refused before anything
//! starts. The rows API lists the expanded rows and selects by rendered name.
#![cfg(all(feature = "templates", feature = "source-sqlite"))]
#![allow(clippy::disallowed_methods)] // tests drive time and raw clients directly

use serde_json::{Value, json};

#[path = "support/template_server.rs"]
mod template_server;
use template_server::Server;

async fn seed(db: &std::path::Path) {
    use sqlx::Connection;
    let url = format!("sqlite://{}?mode=rwc", db.display());
    let mut c = sqlx::SqliteConnection::connect(&url).await.unwrap();
    for (table, rows) in [("orders", 3), ("customers", 2), ("items", 1)] {
        sqlx::query(&format!(
            "CREATE TABLE {table} (id INTEGER PRIMARY KEY, v TEXT)"
        ))
        .execute(&mut c)
        .await
        .unwrap();
        for i in 0..rows {
            sqlx::query(&format!("INSERT INTO {table} (id, v) VALUES ({i}, 'x')"))
                .execute(&mut c)
                .await
                .unwrap();
        }
    }
}

fn source(db: &std::path::Path) -> String {
    format!(
        r#"kind: source-template
name: tables
description: Every chosen table of one SQLite database
params:
  db: {{ type: string, default: "{}" }}
  prefix: {{ type: string, default: main }}
  tables: {{ type: list, required: true, items: {{ type: string }}, max_items: 3 }}
source:
  type: sqlite
  config:
    database_url: "sqlite:${{param.db}}"
    query: "SELECT 1"
streams:
  - for_each: ${{param.tables}}
    as: t
    name: "${{param.prefix}}_${{t}}"
    source: {{ config: {{ query: 'SELECT * FROM "${{t}}"' }} }}
tests:
  sink: files
  suite:
    auto: {{ defaults_baseline: true }}
"#,
        db.display()
    )
}

fn sink(out: &std::path::Path) -> String {
    format!(
        "kind: sink-template\nname: files\nparams:\n  out_dir: {{ type: string, default: {} }}\nsink:\n  type: jsonl\n  config: {{}}\nper_stream:\n  path: \"${{param.out_dir}}/${{stream}}.jsonl\"\n",
        out.display()
    )
}

async fn trigger(s: &Server, tables: Value) -> (u16, Value) {
    s.call(
        "POST",
        "/v1/templates/tables/runs",
        Some(json!({ "sink": "files", "sink_version": "newest", "params": { "tables": tables } })),
    )
    .await
}

fn rows(rec: &Value) -> Vec<(String, String)> {
    let mut v: Vec<(String, String)> = rec["invocations"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| {
            (
                i["row_id"].as_str().unwrap().to_string(),
                i["status"].as_str().unwrap().to_string(),
            )
        })
        .collect();
    v.sort();
    v
}

#[tokio::test(flavor = "multi_thread")]
async fn a_list_param_runs_one_row_per_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("app.db");
    seed(&db).await;
    let out = dir.path().join("out");
    let s = Server::start().await;

    let (code, r) = s.register(&sink(&out)).await;
    assert_eq!(code, 201, "{r}");
    let (code, r) = s.register(&source(&db)).await;
    assert_eq!(code, 201, "{r}");
    assert_eq!(
        r["tests"]["passed"], true,
        "registers and passes its tests: {r}"
    );
    let (code, r) = s
        .call(
            "POST",
            "/v1/templates/tables/launch",
            Some(json!({ "version": 1 })),
        )
        .await;
    assert_eq!(code, 200, "{r}");

    // Three tables → three rows, each into its own destination.
    let (code, r) = trigger(&s, json!(["orders", "customers", "items"])).await;
    assert_eq!(code, 202, "{r}");
    let streams: Vec<&str> = r["streams"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["stream"].as_str().unwrap())
        .collect();
    assert_eq!(streams, vec!["main_orders", "main_customers", "main_items"]);
    let rec = s.wait_run(r["run_id"].as_str().unwrap()).await;
    assert_eq!(rec["status"], "completed", "{rec}");
    assert_eq!(
        rows(&rec),
        vec![
            ("main_customers".into(), "succeeded".into()),
            ("main_items".into(), "succeeded".into()),
            ("main_orders".into(), "succeeded".into()),
        ]
    );
    for (name, n) in [("main_orders", 3), ("main_customers", 2), ("main_items", 1)] {
        let text = std::fs::read_to_string(out.join(format!("{name}.jsonl"))).unwrap();
        assert_eq!(text.lines().count(), n, "{name}: {text}");
    }

    // A failing table is reported on its own row; the other still runs.
    let (code, r) = trigger(&s, json!("orders,missing")).await;
    assert_eq!(code, 202, "comma text for a list of strings: {r}");
    let rec = s.wait_run(r["run_id"].as_str().unwrap()).await;
    assert_eq!(
        rows(&rec),
        vec![
            ("main_missing".into(), "failed".into()),
            ("main_orders".into(), "succeeded".into()),
        ],
        "{rec}"
    );
    let failed = rec["invocations"]
        .as_array()
        .unwrap()
        .iter()
        .find(|i| i["row_id"] == "main_missing")
        .unwrap();
    assert!(
        failed["error"].as_str().unwrap().contains("missing"),
        "{failed}"
    );

    // Refused before anything starts.
    for (tables, want) in [
        (json!(["orders", "orders"]), "already has"),
        (json!(["Bad-Name"]), "is not a stream name"),
        (json!(["a", "b", "c", "d"]), "more than `max_items: 3`"),
        (json!([]), "at least 1 item"),
        (json!([["nested"]]), "cannot be a list"),
    ] {
        let (code, r) = trigger(&s, tables.clone()).await;
        assert_eq!(code, 422, "{tables}: {r}");
        assert!(r.to_string().contains(want), "{tables} → {r}");
    }

    // The rows API lists the expanded rows and selects by rendered name.
    let params = urlencode(r#"{"tables":["orders","items"]}"#);
    let (code, r) = s
        .call(
            "GET",
            &format!("/v1/templates/tables/rows?sink=files&sink_version=newest&state=false&select=main_items&params={params}"),
            None,
        )
        .await;
    assert_eq!(code, 200, "{r}");
    let ids: Vec<&str> = r["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|x| x["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, vec!["main_orders", "main_items"], "{r}");
    assert_eq!(r["run_set"], json!(["main_items"]), "{r}");
    let (_, bare) = s
        .call("GET", "/v1/templates/tables/rows?state=false", None)
        .await;
    assert!(
        bare["notes"].to_string().contains("placeholder items"),
        "{bare}"
    );

    // A selected subset of the expanded rows runs alone.
    let (code, r) = s
        .call(
            "POST",
            "/v1/templates/tables/runs",
            Some(json!({
                "sink": "files",
                "sink_version": "newest",
                "params": { "tables": ["orders", "items"] },
                "selection": { "select": ["main_items"] },
            })),
        )
        .await;
    assert_eq!(code, 202, "{r}");
    let rec = s.wait_run(r["run_id"].as_str().unwrap()).await;
    assert_eq!(
        rows(&rec),
        vec![("main_items".into(), "succeeded".into())],
        "{rec}"
    );
}

fn urlencode(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}
