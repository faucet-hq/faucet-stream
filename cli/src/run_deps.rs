//! Completion dependencies a run request adds between its rows / streams
//! (#960): `"depends_on": {"orders": ["customers"]}` makes `orders` start
//! only after `customers` completes, as if the config or template had
//! declared the edge.
//!
//! The edges are folded into the config itself — a matrix row's
//! `depends_on:` list, or a source-template stream's — before the run is
//! loaded, so expansion, row selection (`include_parents`), the executor's
//! readiness / skip cascade, the stored body a cluster peer re-runs and the
//! idempotency fingerprint all treat them exactly like declared edges. A
//! request only adds edges; it never removes one.

use crate::error::{CliError, CliResult};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashMap};

/// Run label carrying a run's request-supplied dependencies, in their
/// [canonical](RunDependencies::canonical) form.
pub const LABEL_DEPENDS_ON: &str = "depends_on";

/// The wire form: row / stream id → the ids it waits for.
#[derive(
    Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
#[serde(transparent)]
pub struct RunDependencies(pub BTreeMap<String, Vec<String>>);

impl RunDependencies {
    /// Every `(row, prerequisite)` edge, in key order then list order.
    pub fn edges(&self) -> impl Iterator<Item = (&str, &str)> {
        self.0
            .iter()
            .flat_map(|(row, deps)| deps.iter().map(move |d| (row.as_str(), d.as_str())))
    }

    /// Whether the request adds no edge.
    pub fn is_empty(&self) -> bool {
        self.edges().next().is_none()
    }

    /// A stable one-line form, sorted and deduplicated:
    /// `orders:customers;payments:orders,refunds`. The run label and the
    /// change-request fingerprint use it.
    pub fn canonical(&self) -> String {
        let mut parts = Vec::new();
        for (row, deps) in &self.0 {
            let set: BTreeSet<&str> = deps.iter().map(String::as_str).collect();
            if !set.is_empty() {
                parts.push(format!(
                    "{row}:{}",
                    set.into_iter().collect::<Vec<_>>().join(",")
                ));
            }
        }
        parts.join(";")
    }

    /// Union the edges into a config document's matrix rows. Returns whether
    /// any row gained an edge. A topology (`pipeline.nodes`) or a config
    /// without `matrix:` has no rows to order, so a non-empty request is
    /// refused there.
    pub fn apply_to_document(&self, doc: &mut Value) -> CliResult<bool> {
        if self.is_empty() {
            return Ok(false);
        }
        let topology = match doc.pointer("/pipeline/nodes") {
            Some(Value::Object(n)) => !n.is_empty(),
            Some(Value::Array(n)) => !n.is_empty(),
            _ => false,
        };
        if topology {
            return Err(CliError::Config(
                "run request `depends_on`: this pipeline is a topology (`pipeline.nodes`) — it \
                 has no matrix rows to order; express the order with its `edges:`"
                    .into(),
            ));
        }
        let rows: &mut [Value] = match doc.get_mut("matrix").and_then(Value::as_array_mut) {
            Some(rows) => rows,
            None => &mut [],
        };
        let mut nodes = Vec::with_capacity(rows.len());
        for (i, row) in rows.iter().enumerate() {
            let id = row
                .get("id")
                .and_then(Value::as_str)
                .map(str::to_string)
                .unwrap_or_else(|| format!("row-{i}"));
            let parent = row
                .get("parent")
                .and_then(Value::as_str)
                .map(str::to_string);
            let deps = string_list(row.get("depends_on"));
            nodes.push(Node { id, parent, deps });
        }
        let merged = self.merge(&nodes)?;
        let mut changed = false;
        for (row, (node, deps)) in rows.iter_mut().zip(nodes.iter().zip(merged)) {
            if deps.len() == node.deps.len() {
                continue;
            }
            if let Some(obj) = row.as_object_mut() {
                obj.insert("depends_on".into(), Value::from(deps));
                changed = true;
            }
        }
        Ok(changed)
    }

    /// Union the edges into a source template's streams (before its
    /// selection narrows them and it is composed).
    pub fn apply_to_streams(&self, src: &mut crate::hub::SourceTemplate) -> CliResult<()> {
        if self.is_empty() {
            return Ok(());
        }
        let nodes: Vec<Node> = src
            .streams
            .iter()
            .map(|s| Node {
                id: s.name.clone(),
                parent: s.parent.clone(),
                deps: s.depends_on.clone(),
            })
            .collect();
        let merged = self.merge(&nodes)?;
        for (s, deps) in src.streams.iter_mut().zip(merged) {
            s.depends_on = deps;
        }
        Ok(())
    }

    /// Union the edges into a config text (`json` = JSON, else YAML). `None`
    /// when no row gained an edge (the text stays as written); otherwise the
    /// document re-rendered as JSON.
    pub fn apply_to_text(&self, text: &str, json: bool) -> CliResult<Option<String>> {
        if self.is_empty() {
            return Ok(None);
        }
        let mut doc: Value = if json {
            serde_json::from_str(text)
                .map_err(|e| CliError::Config(format!("invalid JSON: {e}")))?
        } else {
            crate::yaml::from_str(text)
                .map_err(|e| CliError::Config(format!("invalid YAML: {e}")))?
        };
        if !self.apply_to_document(&mut doc)? {
            return Ok(None);
        }
        serde_json::to_string(&doc)
            .map(Some)
            .map_err(|e| CliError::Internal(e.to_string()))
    }

    /// Each node's dependency list with the request's edges appended (in
    /// order, without duplicates), after checking every name and that the
    /// combined `parent:` + `depends_on:` graph stays acyclic.
    fn merge(&self, nodes: &[Node]) -> CliResult<Vec<Vec<String>>> {
        let index: HashMap<&str, usize> = nodes
            .iter()
            .enumerate()
            .map(|(i, n)| (n.id.as_str(), i))
            .collect();
        let available = || nodes.iter().map(|n| n.id.clone()).collect::<Vec<_>>();
        let mut merged: Vec<Vec<String>> = nodes.iter().map(|n| n.deps.clone()).collect();
        let mut requested: BTreeSet<(usize, usize)> = BTreeSet::new();
        for (row, dep) in self.edges() {
            let edge = format!("{row} → {dep}");
            let unknown = |name: &str| CliError::UnknownRunDependency {
                edge: edge.clone(),
                name: name.to_string(),
                available: available(),
            };
            let r = *index.get(row).ok_or_else(|| unknown(row))?;
            let d = *index.get(dep).ok_or_else(|| unknown(dep))?;
            requested.insert((r, d));
            if !merged[r].iter().any(|x| x == dep) {
                merged[r].push(dep.to_string());
            }
        }
        // Kahn's algorithm over row → prerequisite edges: whatever cannot be
        // ordered sits on (or behind) a cycle.
        let mut prereqs: Vec<BTreeSet<usize>> = vec![BTreeSet::new(); nodes.len()];
        for (i, n) in nodes.iter().enumerate() {
            let named = n.parent.iter().chain(merged[i].iter());
            for p in named {
                // An unknown name in the config's own edges is the config's
                // error; expansion reports it.
                if let Some(&j) = index.get(p.as_str()) {
                    prereqs[i].insert(j);
                }
            }
        }
        let mut waiting: Vec<usize> = prereqs.iter().map(BTreeSet::len).collect();
        let mut dependents: Vec<Vec<usize>> = vec![Vec::new(); nodes.len()];
        for (i, ps) in prereqs.iter().enumerate() {
            for &p in ps {
                dependents[p].push(i);
            }
        }
        let mut ready: Vec<usize> = (0..nodes.len()).filter(|&i| waiting[i] == 0).collect();
        let mut done = vec![false; nodes.len()];
        while let Some(i) = ready.pop() {
            done[i] = true;
            for &d in &dependents[i] {
                waiting[d] -= 1;
                if waiting[d] == 0 {
                    ready.push(d);
                }
            }
        }
        if done.iter().all(|d| *d) {
            return Ok(merged);
        }
        // What is left waits on a cycle; peel off the rows merely behind one
        // (no remaining row waits for them) so only the cycle is named.
        let mut on_cycle: Vec<bool> = done.iter().map(|d| !d).collect();
        loop {
            let leaf = (0..nodes.len())
                .find(|&i| on_cycle[i] && !dependents[i].iter().any(|&d| on_cycle[d]));
            match leaf {
                Some(i) => on_cycle[i] = false,
                None => break,
            }
        }
        let stuck: Vec<usize> = (0..nodes.len()).filter(|&i| on_cycle[i]).collect();
        let edges: Vec<String> = requested
            .iter()
            .filter(|(r, d)| on_cycle[*r] && on_cycle[*d])
            .map(|(r, d)| format!("{} → {}", nodes[*r].id, nodes[*d].id))
            .collect();
        let rows: Vec<String> = stuck.iter().map(|&i| nodes[i].id.clone()).collect();
        if edges.is_empty() {
            // The config's own edges already form the cycle.
            return Err(CliError::DependencyCycle { ids: rows });
        }
        Err(CliError::RunDependencyCycle { edges, rows })
    }
}

/// Whether `e` is a refused request `depends_on` — a caller error.
pub fn is_run_dependency_error(e: &CliError) -> bool {
    matches!(
        e,
        CliError::UnknownRunDependency { .. } | CliError::RunDependencyCycle { .. }
    ) || matches!(e, CliError::Config(m) if m.starts_with("run request `depends_on`"))
}

struct Node {
    id: String,
    parent: Option<String>,
    deps: Vec<String>,
}

fn string_list(v: Option<&Value>) -> Vec<String> {
    v.and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn deps(v: Value) -> RunDependencies {
        serde_json::from_value(v).unwrap()
    }

    fn doc() -> Value {
        json!({
            "version": 1,
            "pipeline": {},
            "matrix": [
                {"id": "customers"},
                {"id": "orders", "depends_on": ["customers"]},
                {"id": "payments"},
                {"id": "items", "parent": "orders"},
            ]
        })
    }

    #[test]
    fn edges_are_unioned_and_config_edges_kept() {
        let mut d = doc();
        let changed = deps(json!({"payments": ["orders", "customers"], "orders": ["customers"]}))
            .apply_to_document(&mut d)
            .unwrap();
        assert!(changed);
        assert_eq!(d["matrix"][1]["depends_on"], json!(["customers"]));
        assert_eq!(d["matrix"][2]["depends_on"], json!(["orders", "customers"]));
        assert!(d["matrix"][0].get("depends_on").is_none());
    }

    #[test]
    fn already_declared_edges_change_nothing() {
        let mut d = doc();
        let before = d.clone();
        assert!(
            !deps(json!({"orders": ["customers"]}))
                .apply_to_document(&mut d)
                .unwrap()
        );
        assert_eq!(d, before);
        assert!(
            !RunDependencies::default()
                .apply_to_document(&mut d)
                .unwrap()
        );
        assert!(
            !deps(json!({"orders": []}))
                .apply_to_document(&mut d)
                .unwrap()
        );
    }

    #[test]
    fn an_unknown_row_or_dependency_is_named_with_the_edge() {
        for (req, name) in [
            (json!({"nope": ["customers"]}), "nope"),
            (json!({"orders": ["ghost"]}), "ghost"),
        ] {
            let e = deps(req).apply_to_document(&mut doc()).unwrap_err();
            match &e {
                CliError::UnknownRunDependency {
                    name: n, available, ..
                } => {
                    assert_eq!(n, name);
                    assert!(available.contains(&"payments".to_string()));
                }
                other => panic!("{other:?}"),
            }
            assert!(e.to_string().contains(name), "{e}");
            assert!(is_run_dependency_error(&e));
        }
    }

    #[test]
    fn a_cycle_names_the_request_edges_in_it() {
        // customers → orders (request) + orders → customers (config).
        let e = deps(json!({"customers": ["orders"], "payments": ["customers"]}))
            .apply_to_document(&mut doc())
            .unwrap_err();
        match &e {
            CliError::RunDependencyCycle { edges, rows } => {
                assert_eq!(edges, &vec!["customers → orders".to_string()]);
                assert!(rows.contains(&"customers".to_string()));
                assert!(rows.contains(&"orders".to_string()));
            }
            other => panic!("{other:?}"),
        }
        assert!(is_run_dependency_error(&e));
        // Through a `parent:` edge, and a self edge.
        assert!(matches!(
            deps(json!({"orders": ["items"]})).apply_to_document(&mut doc()),
            Err(CliError::RunDependencyCycle { .. })
        ));
        let e = deps(json!({"payments": ["payments"]}))
            .apply_to_document(&mut doc())
            .unwrap_err();
        assert!(e.to_string().contains("payments → payments"), "{e}");
    }

    #[test]
    fn a_config_cycle_is_reported_as_the_configs() {
        let mut d = json!({"matrix": [
            {"id": "a", "depends_on": ["b"]},
            {"id": "b", "depends_on": ["a"]},
            {"id": "c"},
        ]});
        assert!(matches!(
            deps(json!({"c": ["a"]})).apply_to_document(&mut d),
            Err(CliError::DependencyCycle { .. })
        ));
    }

    #[test]
    fn rows_without_an_id_use_their_synthetic_id() {
        let mut d = json!({"matrix": [{}, {}]});
        assert!(
            deps(json!({"row-1": ["row-0"]}))
                .apply_to_document(&mut d)
                .unwrap()
        );
        assert_eq!(d["matrix"][1]["depends_on"], json!(["row-0"]));
    }

    #[test]
    fn no_rows_or_a_topology_refuses() {
        let e = deps(json!({"a": ["b"]}))
            .apply_to_document(&mut json!({"pipeline": {}}))
            .unwrap_err();
        assert!(e.to_string().contains("no matrix rows"), "{e}");
        let e = deps(json!({"a": ["b"]}))
            .apply_to_document(&mut json!({"pipeline": {"nodes": [{"id": "a"}]}}))
            .unwrap_err();
        assert!(e.to_string().contains("topology"), "{e}");
        assert!(is_run_dependency_error(&e));
    }

    #[test]
    fn a_topology_config_is_told_to_use_its_edges() {
        let mut d = json!({
            "pipeline": {
                "nodes": {
                    "src": {"type": "source"},
                    "out": {"type": "sink"}
                },
                "edges": [{"from": "src", "to": "out"}]
            }
        });
        let e = deps(json!({"out": ["src"]}))
            .apply_to_document(&mut d)
            .unwrap_err();
        let msg = e.to_string();
        assert!(msg.contains("topology"), "{msg}");
        assert!(msg.contains("`edges:`"), "{msg}");
        assert!(is_run_dependency_error(&e));
    }

    #[test]
    fn text_is_rewritten_only_when_an_edge_is_added() {
        let yaml = "version: 1\nmatrix:\n  - id: a\n  - id: b\n";
        let d = deps(json!({"b": ["a"]}));
        let out = d.apply_to_text(yaml, false).unwrap().unwrap();
        let v: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["matrix"][1]["depends_on"], json!(["a"]));
        assert!(d.apply_to_text(&out, true).unwrap().is_none());
        assert!(d.apply_to_text("{", true).is_err());
        assert!(d.apply_to_text(": : :", false).is_err());
        assert!(
            RunDependencies::default()
                .apply_to_text("{", true)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn stream_edges_join_the_templates_own() {
        let mut src: crate::hub::SourceTemplate = crate::yaml::from_str(
            "kind: source-template\nname: shop\nsource: { type: csv, config: { path: /x } }\n\
             streams:\n  - name: customers\n  - name: orders\n  - name: refunds\n    \
             depends_on: [orders]\n",
        )
        .unwrap();
        deps(json!({"refunds": ["customers"], "orders": ["customers"]}))
            .apply_to_streams(&mut src)
            .unwrap();
        assert_eq!(src.streams[1].depends_on, vec!["customers"]);
        assert_eq!(src.streams[2].depends_on, vec!["orders", "customers"]);
        RunDependencies::default()
            .apply_to_streams(&mut src)
            .unwrap();
        assert!(matches!(
            deps(json!({"orders": ["refunds"]})).apply_to_streams(&mut src),
            Err(CliError::RunDependencyCycle { .. })
        ));
        assert!(matches!(
            deps(json!({"orders": ["ghost"]})).apply_to_streams(&mut src),
            Err(CliError::UnknownRunDependency { .. })
        ));
    }

    #[test]
    fn canonical_is_sorted_and_deduplicated() {
        let d = deps(
            json!({"payments": ["refunds", "orders", "orders"], "orders": ["customers"], "x": []}),
        );
        assert_eq!(d.canonical(), "orders:customers;payments:orders,refunds");
        assert!(RunDependencies::default().is_empty());
        assert!(deps(json!({"x": []})).is_empty());
    }
}
