//! Row routing for multi-stream results (#768).
//!
//! A Shopify bulk-operation result is one JSONL file mixing parents
//! (`{"id": "gid://shopify/Order/1", …}`) and children
//! (`{"id": "gid://shopify/LineItem/7", "__parentId": "gid://shopify/Order/1"}`).
//! `records_route` stamps each row with the stream it belongs to, optionally
//! copies `__parentId` into a join column, and drops (or refuses) rows of a
//! type no route names.
//!
//! ```yaml
//! records_route:
//!   by: id_type
//!   routes:
//!     Order:    { stream: orders }
//!     LineItem: { stream: order_line_items, parent_key_as: order_id }
//! ```

use faucet_core::FaucetError;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashSet};

/// Field a Shopify bulk child row carries its parent's GID in.
pub const PARENT_ID_FIELD: &str = "__parentId";

/// Route-key prefix for a row with no GID of its own, keyed by its parent's type.
pub const CHILD_OF_PREFIX: &str = "child_of:";

fn default_stream_field() -> String {
    "_stream".to_string()
}

/// What a route key is read from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RouteBy {
    /// The object type in a GID (`gid://<app>/<Type>/<id>`) in the row's `id`.
    /// A row without one is keyed `child_of:<ParentType>` from `__parentId`.
    #[default]
    IdType,
    /// The value of a top-level [`RecordsRoute::field`] (e.g. `__typename`).
    Field,
}

/// Where one route's rows go.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RouteTarget {
    /// Stream name stamped into [`RecordsRoute::stream_field`].
    pub stream: String,
    /// Copy `__parentId` into this column so the child stream joins back.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_key_as: Option<String>,
}

/// The `records_route:` block.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RecordsRoute {
    /// `id_type` (default) or `field`.
    #[serde(default)]
    pub by: RouteBy,
    /// `by: field` only: the top-level discriminator field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    /// Route key → target. Keys are object types (`Order`), `child_of:<Type>`
    /// for id-less children, or discriminator values under `by: field`.
    pub routes: BTreeMap<String, RouteTarget>,
    /// Column each routed row is stamped with its stream name (default `_stream`).
    #[serde(default = "default_stream_field")]
    pub stream_field: String,
    /// Emit only these streams; rows of the other configured routes are
    /// skipped deliberately (not counted as unknown). Empty = every route.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub only: Vec<String>,
    /// Fail the run on a row no route names, instead of dropping it with a
    /// one-shot warning per type. Default `false`.
    #[serde(default)]
    pub strict: bool,
}

impl RecordsRoute {
    /// Validate at config-load time.
    pub fn validate(&self) -> Result<(), FaucetError> {
        let err = |m: String| Err(FaucetError::Config(format!("rest: records_route: {m}")));
        match (self.by, self.field.as_deref().map(str::trim)) {
            (RouteBy::Field, None | Some("")) => {
                return err("`by: field` needs `field` (the discriminator column)".into());
            }
            (RouteBy::IdType, Some(_)) => {
                return err("`field` applies only to `by: field`".into());
            }
            _ => {}
        }
        if self.routes.is_empty() {
            return err("`routes` must name at least one route".into());
        }
        if self.stream_field.trim().is_empty() {
            return err("`stream_field` must not be empty".into());
        }
        let mut streams = HashSet::new();
        for (key, target) in &self.routes {
            if key.trim().is_empty() {
                return err("route keys must not be empty".into());
            }
            if target.stream.trim().is_empty() {
                return err(format!("route '{key}' has an empty `stream`"));
            }
            if target
                .parent_key_as
                .as_deref()
                .is_some_and(|c| c.trim().is_empty())
            {
                return err(format!("route '{key}' has an empty `parent_key_as`"));
            }
            streams.insert(target.stream.as_str());
        }
        for s in &self.only {
            if !streams.contains(s.as_str()) {
                return err(format!("`only` names '{s}', which no route emits"));
            }
        }
        Ok(())
    }
}

/// The object type in a GID: `gid://shopify/Order/123` → `Order`.
/// Query strings (`gid://shopify/Video/1?x=y`) are ignored.
pub fn gid_type(gid: &str) -> Option<&str> {
    let rest = gid.strip_prefix("gid://")?;
    let mut parts = rest.split('/');
    let _app = parts.next().filter(|a| !a.is_empty())?;
    let ty = parts.next().filter(|t| !t.is_empty())?;
    parts.next().filter(|id| !id.is_empty())?;
    Some(ty)
}

/// The route key of one row, or `None` when nothing identifies it.
pub fn route_key(route: &RecordsRoute, rec: &Value) -> Option<String> {
    let obj = rec.as_object()?;
    match route.by {
        RouteBy::Field => match obj.get(route.field.as_deref()?)? {
            Value::String(s) => Some(s.clone()),
            Value::Null => None,
            other => Some(other.to_string()),
        },
        RouteBy::IdType => {
            if let Some(t) = obj.get("id").and_then(Value::as_str).and_then(gid_type) {
                return Some(t.to_string());
            }
            obj.get(PARENT_ID_FIELD)
                .and_then(Value::as_str)
                .and_then(gid_type)
                .map(|t| format!("{CHILD_OF_PREFIX}{t}"))
        }
    }
}

/// Per-run routing state: counts and one-shot warnings for unknown types.
#[derive(Debug)]
pub struct Router<'a> {
    route: &'a RecordsRoute,
    warned: HashSet<String>,
    dropped: BTreeMap<String, u64>,
}

impl<'a> Router<'a> {
    /// A fresh router for one run.
    pub fn new(route: &'a RecordsRoute) -> Self {
        Self {
            route,
            warned: HashSet::new(),
            dropped: BTreeMap::new(),
        }
    }

    /// Route one page: stamp the stream, copy the parent key, drop unknowns
    /// (or fail under `strict`).
    pub fn route_page(&mut self, records: Vec<Value>) -> Result<Vec<Value>, FaucetError> {
        let mut out = Vec::with_capacity(records.len());
        for mut rec in records {
            let key = route_key(self.route, &rec);
            let target = key.as_deref().and_then(|k| self.route.routes.get(k));
            let Some(target) = target else {
                let label = key.unwrap_or_else(|| "(unidentified)".to_string());
                if self.route.strict {
                    return Err(FaucetError::Source(format!(
                        "rest: records_route: row of type '{label}' matches no route \
                         (strict: true)"
                    )));
                }
                if self.warned.insert(label.clone()) {
                    tracing::warn!(
                        route_key = %label,
                        "rest: records_route: dropping rows of a type no route names"
                    );
                }
                *self.dropped.entry(label).or_default() += 1;
                continue;
            };
            if !self.route.only.is_empty() && !self.route.only.contains(&target.stream) {
                continue;
            }
            if let Value::Object(map) = &mut rec {
                if let Some(col) = &target.parent_key_as {
                    let parent = map.get(PARENT_ID_FIELD).cloned().unwrap_or(Value::Null);
                    map.insert(col.clone(), parent);
                }
                map.insert(
                    self.route.stream_field.clone(),
                    Value::String(target.stream.clone()),
                );
            }
            out.push(rec);
        }
        Ok(out)
    }

    /// Rows dropped per unknown route key so far.
    pub fn dropped(&self) -> &BTreeMap<String, u64> {
        &self.dropped
    }

    /// Log the run's drop totals once, when anything was dropped.
    pub fn finish(&self) {
        if !self.dropped.is_empty() {
            let total: u64 = self.dropped.values().sum();
            tracing::warn!(
                dropped = total,
                by_type = ?self.dropped,
                "rest: records_route: rows dropped because no route names their type"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn shopify() -> RecordsRoute {
        serde_json::from_value(json!({
            "routes": {
                "Order": { "stream": "orders" },
                "LineItem": { "stream": "order_line_items", "parent_key_as": "order_id" },
                "child_of:Order": { "stream": "order_notes", "parent_key_as": "order_id" }
            }
        }))
        .unwrap()
    }

    #[test]
    fn gid_type_parses_shopify_ids() {
        assert_eq!(gid_type("gid://shopify/Order/1"), Some("Order"));
        assert_eq!(gid_type("gid://shopify/Video/1?x=y"), Some("Video"));
        assert_eq!(gid_type("gid://shopify/Order"), None);
        assert_eq!(gid_type("gid://shopify//1"), None);
        assert_eq!(gid_type("gid:///Order/1"), None);
        assert_eq!(gid_type("gid://shopify/Order/"), None);
        assert_eq!(gid_type("Order/1"), None);
    }

    #[test]
    fn route_key_prefers_own_id_then_parent() {
        let r = shopify();
        assert_eq!(
            route_key(&r, &json!({"id": "gid://shopify/Order/1"})).as_deref(),
            Some("Order")
        );
        assert_eq!(
            route_key(
                &r,
                &json!({"id": "gid://shopify/LineItem/2", "__parentId": "gid://shopify/Order/1"})
            )
            .as_deref(),
            Some("LineItem")
        );
        assert_eq!(
            route_key(
                &r,
                &json!({"note": "x", "__parentId": "gid://shopify/Order/1"})
            )
            .as_deref(),
            Some("child_of:Order")
        );
        assert_eq!(route_key(&r, &json!({"id": 5})), None);
        assert_eq!(route_key(&r, &json!("scalar")), None);
    }

    #[test]
    fn route_key_by_field() {
        let r: RecordsRoute = serde_json::from_value(json!({
            "by": "field", "field": "__typename",
            "routes": { "Order": { "stream": "orders" } }
        }))
        .unwrap();
        assert_eq!(
            route_key(&r, &json!({"__typename": "Order"})).as_deref(),
            Some("Order")
        );
        assert_eq!(
            route_key(&r, &json!({"__typename": 3})).as_deref(),
            Some("3")
        );
        assert_eq!(route_key(&r, &json!({"__typename": null})), None);
        assert_eq!(route_key(&r, &json!({})), None);
    }

    #[test]
    fn router_stamps_streams_and_parent_keys() {
        let r = shopify();
        let mut router = Router::new(&r);
        let out = router
            .route_page(vec![
                json!({"id": "gid://shopify/Order/1"}),
                json!({"id": "gid://shopify/LineItem/2", "__parentId": "gid://shopify/Order/1"}),
                json!({"note": "n", "__parentId": "gid://shopify/Order/1"}),
            ])
            .unwrap();
        assert_eq!(out.len(), 3);
        assert_eq!(out[0]["_stream"], "orders");
        assert!(out[0].get("order_id").is_none());
        assert_eq!(out[1]["_stream"], "order_line_items");
        assert_eq!(out[1]["order_id"], "gid://shopify/Order/1");
        assert_eq!(out[2]["_stream"], "order_notes");
        assert!(router.dropped().is_empty());
        router.finish();
    }

    #[test]
    fn router_counts_and_drops_unknown_types() {
        let r = shopify();
        let mut router = Router::new(&r);
        let out = router
            .route_page(vec![
                json!({"id": "gid://shopify/Refund/1"}),
                json!({"id": "gid://shopify/Refund/2"}),
                json!({"x": 1}),
                json!({"id": "gid://shopify/Order/3"}),
            ])
            .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(router.dropped().get("Refund"), Some(&2));
        assert_eq!(router.dropped().get("(unidentified)"), Some(&1));
        router.finish();
    }

    #[test]
    fn strict_router_fails_on_unknown_type() {
        let mut r = shopify();
        r.strict = true;
        let err = Router::new(&r)
            .route_page(vec![json!({"id": "gid://shopify/Refund/1"})])
            .unwrap_err();
        assert!(err.to_string().contains("'Refund'"), "{err}");
    }

    #[test]
    fn only_skips_other_routes_without_counting_them() {
        let mut r = shopify();
        r.only = vec!["order_line_items".into()];
        let mut router = Router::new(&r);
        let out = router
            .route_page(vec![
                json!({"id": "gid://shopify/Order/1"}),
                json!({"id": "gid://shopify/LineItem/2", "__parentId": "gid://shopify/Order/1"}),
            ])
            .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["_stream"], "order_line_items");
        assert!(router.dropped().is_empty());
    }

    #[test]
    fn missing_parent_copies_null() {
        let r = shopify();
        let out = Router::new(&r)
            .route_page(vec![json!({"id": "gid://shopify/LineItem/2"})])
            .unwrap();
        assert_eq!(out[0]["order_id"], Value::Null);
    }

    #[test]
    fn validate_rejects_bad_shapes() {
        assert!(shopify().validate().is_ok());
        let bad = |v: Value| {
            serde_json::from_value::<RecordsRoute>(v)
                .unwrap()
                .validate()
        };
        let r = json!({ "stream": "s" });
        assert!(
            bad(json!({"by": "field", "routes": {"A": r}}))
                .unwrap_err()
                .to_string()
                .contains("needs `field`")
        );
        assert!(bad(json!({"by": "field", "field": " ", "routes": {"A": r}})).is_err());
        assert!(
            bad(json!({"field": "t", "routes": {"A": r}}))
                .unwrap_err()
                .to_string()
                .contains("only to `by: field`")
        );
        assert!(bad(json!({"routes": {}})).is_err());
        assert!(bad(json!({"stream_field": "", "routes": {"A": r}})).is_err());
        assert!(bad(json!({"routes": {" ": r}})).is_err());
        assert!(bad(json!({"routes": {"A": {"stream": " "}}})).is_err());
        assert!(bad(json!({"routes": {"A": {"stream": "s", "parent_key_as": ""}}})).is_err());
        assert!(
            bad(json!({"only": ["nope"], "routes": {"A": r}}))
                .unwrap_err()
                .to_string()
                .contains("'nope'")
        );
        assert!(bad(json!({"only": ["s"], "routes": {"A": r}})).is_ok());
    }
}
