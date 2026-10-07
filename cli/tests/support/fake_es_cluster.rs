//! A small stateful Elasticsearch stand-in for the overwrite lifecycle: indices
//! with their documents and mappings, aliases, `_bulk` (honouring
//! `require_alias`), `_aliases`, `_refresh`, create and delete.

use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use wiremock::{Request, Respond, ResponseTemplate};

#[derive(Default)]
pub struct State {
    pub indices: BTreeMap<String, (Vec<Value>, Value)>,
    pub aliases: BTreeMap<String, BTreeSet<String>>,
}

#[derive(Clone, Default)]
pub struct FakeCluster(pub Arc<Mutex<State>>);

impl FakeCluster {
    pub fn add_index(&self, index: &str, docs: Vec<Value>, alias: Option<&str>) {
        let mut st = self.0.lock().unwrap();
        st.indices.insert(index.to_string(), (docs, json!({})));
        if let Some(a) = alias {
            st.aliases
                .entry(a.to_string())
                .or_default()
                .insert(index.to_string());
        }
    }

    pub fn alias_docs(&self, alias: &str) -> Vec<Value> {
        let st = self.0.lock().unwrap();
        st.aliases
            .get(alias)
            .into_iter()
            .flatten()
            .flat_map(|i| st.indices[i].0.clone())
            .collect()
    }

    pub fn index_names(&self) -> Vec<String> {
        self.0.lock().unwrap().indices.keys().cloned().collect()
    }

    pub fn alias_names(&self) -> Vec<String> {
        self.0.lock().unwrap().aliases.keys().cloned().collect()
    }
}

fn not_found() -> ResponseTemplate {
    ResponseTemplate::new(404).set_body_json(json!({"error": "not found"}))
}

impl Respond for FakeCluster {
    fn respond(&self, req: &Request) -> ResponseTemplate {
        let mut st = self.0.lock().unwrap();
        let path = req.url.path().trim_start_matches('/').to_string();
        let parts: Vec<&str> = path.split('/').collect();
        let method = req.method.as_str();
        match (method, parts.as_slice()) {
            ("GET", ["_alias", name]) => match st.aliases.get(*name) {
                Some(targets) if !targets.is_empty() => {
                    let mut body = serde_json::Map::new();
                    for t in targets {
                        body.insert(t.clone(), json!({"aliases": {*name: {}}}));
                    }
                    ResponseTemplate::new(200).set_body_json(Value::Object(body))
                }
                _ => not_found(),
            },
            ("HEAD", [name]) => {
                if st.indices.contains_key(*name) {
                    ResponseTemplate::new(200)
                } else {
                    ResponseTemplate::new(404)
                }
            }
            ("GET", [index, "_mapping"]) => match st.indices.get(*index) {
                Some((_, mappings)) => ResponseTemplate::new(200)
                    .set_body_json(json!({ *index: { "mappings": mappings } })),
                None => not_found(),
            },
            ("PUT", [index]) => {
                let body: Value = serde_json::from_slice(&req.body).unwrap_or(json!({}));
                st.indices.insert(
                    index.to_string(),
                    (
                        Vec::new(),
                        body.get("mappings").cloned().unwrap_or(json!({})),
                    ),
                );
                for alias in body["aliases"]
                    .as_object()
                    .into_iter()
                    .flat_map(|m| m.keys())
                {
                    st.aliases
                        .entry(alias.clone())
                        .or_default()
                        .insert(index.to_string());
                }
                ResponseTemplate::new(200).set_body_json(json!({"acknowledged": true}))
            }
            ("DELETE", [index]) => {
                if st.indices.remove(*index).is_none() {
                    return not_found();
                }
                for targets in st.aliases.values_mut() {
                    targets.remove(*index);
                }
                st.aliases.retain(|_, t| !t.is_empty());
                ResponseTemplate::new(200).set_body_json(json!({"acknowledged": true}))
            }
            ("POST", [_, "_refresh"]) => ResponseTemplate::new(200).set_body_json(json!({})),
            ("POST", ["_aliases"]) => {
                let body: Value = serde_json::from_slice(&req.body).unwrap();
                for action in body["actions"].as_array().unwrap() {
                    if let Some(add) = action.get("add") {
                        st.aliases
                            .entry(add["alias"].as_str().unwrap().to_string())
                            .or_default()
                            .insert(add["index"].as_str().unwrap().to_string());
                    }
                    if let Some(rm) = action.get("remove")
                        && let Some(t) = st.aliases.get_mut(rm["alias"].as_str().unwrap())
                    {
                        t.remove(rm["index"].as_str().unwrap());
                    }
                }
                st.aliases.retain(|_, t| !t.is_empty());
                ResponseTemplate::new(200).set_body_json(json!({"acknowledged": true}))
            }
            ("POST", ["_bulk"]) => {
                let require_alias = req
                    .url
                    .query_pairs()
                    .any(|(k, v)| k == "require_alias" && v == "true");
                let text = String::from_utf8_lossy(&req.body).to_string();
                let mut lines = text.lines().filter(|l| !l.is_empty());
                let mut items = Vec::new();
                let mut errors = false;
                while let Some(action) = lines.next() {
                    let action: Value = serde_json::from_str(action).unwrap();
                    let (kind, meta) = action.as_object().unwrap().iter().next().unwrap();
                    let doc: Value = if kind == "delete" {
                        Value::Null
                    } else {
                        serde_json::from_str(lines.next().unwrap()).unwrap()
                    };
                    let target = meta["_index"].as_str().unwrap().to_string();
                    let resolved = match st.aliases.get(&target) {
                        Some(t) if t.len() == 1 => Some(t.iter().next().unwrap().clone()),
                        Some(_) => None,
                        None if st.indices.contains_key(&target) && !require_alias => {
                            Some(target.clone())
                        }
                        None if !require_alias => {
                            st.indices.insert(target.clone(), (Vec::new(), json!({})));
                            Some(target.clone())
                        }
                        None => None,
                    };
                    match resolved {
                        Some(index) => {
                            st.indices.get_mut(&index).unwrap().0.push(doc);
                            items.push(json!({ kind: { "_index": index, "status": 201 } }));
                        }
                        None => {
                            errors = true;
                            items.push(json!({ kind: {
                                "_index": target,
                                "status": 404,
                                "error": { "type": "index_not_found_exception",
                                           "reason": "require_alias: no such alias" }
                            }}));
                        }
                    }
                }
                ResponseTemplate::new(200).set_body_json(json!({"errors": errors, "items": items}))
            }
            _ => not_found(),
        }
    }
}
