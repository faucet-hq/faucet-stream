//! A wiremock GraphQL server shared by the incremental-replication tests: it
//! honours an `updated_at:>…` filter variable and Relay cursor paging.
#![allow(dead_code)]

use faucet_source_graphql::GraphqlStreamConfig;
use faucet_source_graphql::config::{GraphqlPagination, GraphqlReplicationBind};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use wiremock::{MockServer, Request, ResponseTemplate};

pub const QUERY: &str = "query($after: String, $first: Int, $filter: OrderFilter) { \
    orders(first: $first, after: $after, filter: $filter) { \
    edges { node { id updatedAt } } pageInfo { hasNextPage endCursor } } }";

#[derive(Clone, Default)]
pub struct Api {
    pub rows: Arc<Mutex<Vec<(u64, String)>>>,
    pub filters: Arc<Mutex<Vec<Value>>>,
    /// Ignore the filter and return every row (a server that drops the bind).
    pub ignore_filter: bool,
}

impl wiremock::Respond for Api {
    fn respond(&self, req: &Request) -> ResponseTemplate {
        let body: Value = serde_json::from_slice(&req.body).unwrap();
        let vars = &body["variables"];
        let filter = vars
            .pointer("/filter/query")
            .cloned()
            .unwrap_or(Value::Null);
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
            .filter(|(_, u)| self.ignore_filter || u.as_str() > since.as_str())
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

pub fn config(server: &MockServer) -> GraphqlStreamConfig {
    let mut c = GraphqlStreamConfig::new(server.uri(), QUERY)
        .records_path("$.data.orders.edges[*].node")
        .variables(json!({ "filter": { "query": "updated_at:>2000-01-01T00:00:00Z" } }))
        .pagination(GraphqlPagination {
            has_next_page_path: "$.data.orders.pageInfo.hasNextPage".into(),
            cursor_path: "$.data.orders.pageInfo.endCursor".into(),
            cursor_variable: "after".into(),
            page_size_variable: "first".into(),
        })
        .with_batch_size(2);
    c.replication_method = faucet_core::ReplicationMethod::Incremental;
    c.replication_key = Some("updatedAt".into());
    c.replication_bind = Some(GraphqlReplicationBind {
        variable: "/filter/query".into(),
        template: "updated_at:>${bookmark}".into(),
        format: faucet_core::BindFormat::Iso8601,
        value_type: faucet_core::BindValueType::String,
    });
    c
}

pub fn rows(n: u64) -> Vec<(u64, String)> {
    (1..=n)
        .map(|i| (i, format!("2026-01-0{i}T00:00:00Z")))
        .collect()
}
