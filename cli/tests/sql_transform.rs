#![cfg(all(
    feature = "transform-sql",
    feature = "source-csv",
    feature = "sink-jsonl"
))]

use faucet_cli::config::TransformSpec;
use faucet_cli::transforms::compile_transforms;

/// End-to-end pipeline: CSV orders + CSV countries reference, GROUP BY + JOIN
/// via embedded DuckDB, output to JSONL. Verifies records are aggregated and
/// joined correctly.
#[tokio::test]
async fn csv_groupby_join_to_jsonl() {
    let dir = tempfile::tempdir().unwrap();
    let orders = dir.path().join("orders.csv");
    let countries = dir.path().join("countries.csv");
    let out = dir.path().join("out.jsonl");

    std::fs::write(
        &orders,
        "order_id,country_code,amount\n1,US,10\n2,US,5\n3,IN,7\n",
    )
    .unwrap();
    std::fs::write(&countries, "code,country\nUS,United States\nIN,India\n").unwrap();

    let yaml = format!(
        r#"version: 1
name: t
pipeline:
  source:
    type: csv
    config:
      path: "{orders}"
      has_headers: true
      batch_size: 0
  transforms:
    - type: sql
      config:
        query: |
          SELECT c.country, COUNT(*) AS n, SUM(CAST(o.amount AS DOUBLE)) AS total
          FROM batch o
          JOIN countries c ON o.country_code = c.code
          GROUP BY c.country
          ORDER BY c.country
        relations:
          - name: countries
            source:
              type: csv
              path: "{countries}"
              has_header: true
  sink:
    type: jsonl
    config:
      path: "{out}"
"#,
        orders = orders.display(),
        countries = countries.display(),
        out = out.display(),
    );

    faucet_cli::run_from_yaml_str(&yaml)
        .await
        .expect("pipeline run");

    let body = std::fs::read_to_string(&out).unwrap();
    let rows: Vec<serde_json::Value> = body
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();

    // India: 1 order, total 7; United States: 2 orders, total 15.
    let india = rows
        .iter()
        .find(|r| r["country"] == "India")
        .expect("India row");
    assert_eq!(india["n"], 1);

    let us = rows
        .iter()
        .find(|r| r["country"] == "United States")
        .expect("United States row");
    assert_eq!(us["n"], 2);
    assert_eq!(us["total"], 15.0);
}

/// `faucet validate` path: `compile_transforms` surfaces a query error for
/// syntactically invalid SQL at config-load time.
#[test]
fn validate_reports_bad_sql() {
    let specs = vec![TransformSpec {
        kind: "sql".into(),
        config: serde_json::json!({"query": "SELEKT oops"}),
    }];
    let err = compile_transforms(&specs).unwrap_err();
    let msg = format!("{err}").to_lowercase();
    assert!(
        msg.contains("sel") || msg.contains("syntax") || msg.contains("invalid"),
        "expected error message to mention the bad SQL token, got: {err}"
    );
}
