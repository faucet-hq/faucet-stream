//! GraphQL incremental replication through the real CLI run path (#751): the
//! same config runs twice against a wiremock GraphQL endpoint with a `file`
//! state store. The second run binds the stored bookmark into the filter
//! variable, reads only the new row, and advances the bookmark.
#![cfg(all(feature = "source-graphql", feature = "sink-jsonl"))]

use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

#[derive(Clone, Default)]
struct Api {
    rows: Arc<Mutex<Vec<(u64, String)>>>,
    filters: Arc<Mutex<Vec<Value>>>,
}

impl wiremock::Respond for Api {
    fn respond(&self, req: &Request) -> ResponseTemplate {
        let body: Value = serde_json::from_slice(&req.body).unwrap();
        let vars = &body["variables"];
        let filter = vars["query"].clone();
        self.filters.lock().unwrap().push(filter.clone());
        let since = filter
            .as_str()
            .and_then(|q| q.strip_prefix("updated_at:>"))
            .unwrap_or("")
            .to_string();
        let after: usize = vars["after"].as_str().map_or(0, |c| c.parse().unwrap());
        let first = vars["first"].as_u64().unwrap() as usize;
        let rows: Vec<(u64, String)> = self
            .rows
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, u)| u.as_str() > since.as_str())
            .cloned()
            .collect();
        let page: Vec<Value> = rows
            .iter()
            .skip(after)
            .take(first)
            .map(|(id, u)| json!({ "node": { "id": id, "updatedAt": u } }))
            .collect();
        let end = after + page.len();
        ResponseTemplate::new(200).set_body_json(json!({
            "data": { "orders": {
                "edges": page,
                "pageInfo": { "hasNextPage": end < rows.len(), "endCursor": end.to_string() }
            } }
        }))
    }
}

fn read_ids(path: &std::path::Path) -> Vec<u64> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .map(|l| {
            serde_json::from_str::<Value>(l).unwrap()["id"]
                .as_u64()
                .unwrap()
        })
        .collect()
}

/// The source bookmarks in the state dir; run-health records sit beside them.
fn stored_bookmarks(dir: &std::path::Path) -> Vec<Value> {
    let mut out = Vec::new();
    for e in std::fs::read_dir(dir).unwrap() {
        let text = std::fs::read_to_string(e.unwrap().path()).unwrap();
        let v: Value = serde_json::from_str(&text).unwrap();
        if v.get("owner") == Some(&json!("graphql")) {
            out.push(v["data"].clone());
        }
    }
    out
}

#[tokio::test]
async fn second_cli_run_reads_only_new_rows_and_advances_the_bookmark() {
    let server = MockServer::start().await;
    let api = Api::default();
    *api.rows.lock().unwrap() = (1..=3)
        .map(|i| (i, format!("2026-01-0{i}T00:00:00Z")))
        .collect();
    Mock::given(method("POST"))
        .respond_with(api.clone())
        .mount(&server)
        .await;

    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("state");
    let out = dir.path().join("orders.jsonl");
    let yaml = format!(
        r#"
version: 1
name: gql_orders
pipeline:
  source:
    type: graphql
    config:
      endpoint: "{endpoint}"
      query: "query($after: String, $first: Int, $query: String) {{ orders(first: $first, after: $after, query: $query) {{ edges {{ node {{ id updatedAt }} }} pageInfo {{ hasNextPage endCursor }} }} }}"
      variables: {{ query: "updated_at:>2000-01-01T00:00:00Z" }}
      auth: {{ type: none }}
      records_path: "$.data.orders.edges[*].node"
      pagination:
        has_next_page_path: "$.data.orders.pageInfo.hasNextPage"
        cursor_path: "$.data.orders.pageInfo.endCursor"
        cursor_variable: after
        page_size_variable: first
      batch_size: 2
      replication_method: {{ type: Incremental }}
      replication_key: updatedAt
      replication_bind: {{ variable: query, template: "updated_at:>${{bookmark}}", format: iso8601 }}
  sink:
    type: jsonl
    config: {{ path: "{out}" }}
  state:
    type: file
    config: {{ path: "{state}" }}
"#,
        endpoint = server.uri(),
        out = out.display(),
        state = state.display(),
    );

    let run = faucet_cli::run_from_yaml_str(&yaml)
        .await
        .expect("first run");
    assert!(
        !run.had_failures(),
        "first run failed: {:?}",
        run.invocations
    );
    assert_eq!(read_ids(&out), vec![1, 2, 3]);
    assert_eq!(
        *api.filters.lock().unwrap(),
        vec![json!("updated_at:>2000-01-01T00:00:00Z"); 2]
    );
    let first = stored_bookmarks(&state);
    assert_eq!(first.len(), 1, "{first:?}");
    assert_eq!(first[0], json!("2026-01-03T00:00:00Z"));

    api.rows
        .lock()
        .unwrap()
        .push((4, "2026-01-04T00:00:00Z".into()));
    api.filters.lock().unwrap().clear();
    let run = faucet_cli::run_from_yaml_str(&yaml)
        .await
        .expect("second run");
    assert!(
        !run.had_failures(),
        "second run failed: {:?}",
        run.invocations
    );
    assert_eq!(read_ids(&out), vec![4], "only the new row is written");
    assert_eq!(
        *api.filters.lock().unwrap(),
        vec![json!("updated_at:>2026-01-03T00:00:00Z")]
    );
    let second = stored_bookmarks(&state);
    assert_eq!(second.len(), 1, "{second:?}");
    assert_eq!(second[0], json!("2026-01-04T00:00:00Z"));
}
