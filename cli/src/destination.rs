//! Which rows write the same physical destination (#789).
//!
//! A `write_mode: overwrite` destination is replaced by one atomic swap, so
//! every row that writes it has to take part in the same swap; a run that
//! includes only some of them replaces the table with a subset. Content
//! verification has the mirror-image problem: a row whose destination other
//! rows also write cannot be compared against that destination on its own.

use std::collections::{HashMap, HashSet};

use faucet_core::Value;

use crate::error::{CliError, CliResult};
use crate::expand::{ExpandedNode, NodeRole};
use crate::interpolate::{Directive, iter_directives};

/// Sink config keys that tune *how* a sink writes, not *where*. Two rows whose
/// configs differ only in these write the same destination.
const NON_IDENTITY_KEYS: &[&str] = &[
    "batch_size",
    "max_connections",
    "concurrency",
    "write_mode",
    "key",
    "delete_marker",
    "create_table",
    "column_mapping",
    "on_unknown_field",
    "timeout_secs",
    "batch_atomicity",
    "rollback",
    "scope",
    "_overwrite_staging",
    "_activate_version",
];

/// Whether a node's sink is configured for `write_mode: overwrite`.
pub fn is_overwrite(node: &ExpandedNode) -> bool {
    node.sink.config.get("write_mode").and_then(Value::as_str) == Some("overwrite")
}

/// Whether an overwrite replaces only a scoped window (#518) rather than the
/// whole destination.
pub fn is_scoped(node: &ExpandedNode) -> bool {
    node.sink.config.get("scope").is_some_and(|s| !s.is_null())
}

/// A whole-destination overwrite.
fn is_full_overwrite(node: &ExpandedNode) -> bool {
    is_overwrite(node) && !is_scoped(node)
}

fn has_runtime_token(v: &Value) -> bool {
    match v {
        Value::String(s) => iter_directives(s)
            .any(|(_, d)| matches!(d, Directive::Deferred { id, .. } if id != "now")),
        Value::Array(a) => a.iter().any(has_runtime_token),
        Value::Object(m) => m.values().any(has_runtime_token),
        _ => false,
    }
}

/// A stable identity for the destination a node's sink writes, when it can be
/// known before the run: `None` when the sink config carries a per-invocation
/// token (`${parent.*}`, `${dim.*}`), whose destinations differ at run time.
pub fn destination_key(node: &ExpandedNode) -> Option<String> {
    if has_runtime_token(&node.sink.config) {
        return None;
    }
    let mut cfg = node.sink.config.clone();
    if let Value::Object(map) = &mut cfg {
        for k in NON_IDENTITY_KEYS {
            map.remove(*k);
        }
    }
    Some(format!(
        "{}\u{0}{}",
        node.sink.kind,
        serde_json::to_string(&cfg).unwrap_or_default()
    ))
}

/// A short, credential-free description of a node's destination for messages.
pub fn describe(node: &ExpandedNode) -> String {
    for field in [
        "table",
        "table_name",
        "table_id",
        "collection",
        "index",
        "path",
    ] {
        if let Some(v) = node.sink.config.get(field).and_then(Value::as_str) {
            return format!("{} {v}", node.sink.kind);
        }
    }
    node.sink.kind.clone()
}

/// Rows other than `node` in `all` that write the same destination.
pub fn peers<'a>(node: &ExpandedNode, all: &'a [ExpandedNode]) -> Vec<&'a ExpandedNode> {
    let Some(key) = destination_key(node) else {
        return Vec::new();
    };
    all.iter()
        .filter(|n| n.id != node.id && destination_key(n).as_deref() == Some(key.as_str()))
        .collect()
}

/// Refuse a run whose selection (`--select`/`--only`/`--skip`/`--tag`, a
/// parked `status:`, a serve/MCP/template selection) leaves out a row that
/// shares a full-overwrite destination with a selected row (#789 CLI-04).
pub fn check_overwrite_selection(all: &[ExpandedNode], selected: &[ExpandedNode]) -> CliResult<()> {
    let chosen: HashSet<&str> = selected.iter().map(|n| n.id.as_str()).collect();
    for node in selected.iter().filter(|n| is_full_overwrite(n)) {
        let left_out: Vec<&str> = peers(node, all)
            .into_iter()
            .filter(|p| !chosen.contains(p.id.as_str()))
            .map(|p| p.id.as_str())
            .collect();
        if !left_out.is_empty() {
            return Err(CliError::Config(format!(
                "row '{}' replaces {} with `write_mode: overwrite`, but row(s) {} also write it \
                 and are not part of this run — the swap would delete their rows. Run them \
                 together (widen the selection or `--status`), or give the rows separate \
                 destinations",
                node.id,
                describe(node),
                quoted(&left_out)
            )));
        }
    }
    Ok(())
}

fn quoted(ids: &[&str]) -> String {
    ids.iter()
        .map(|i| format!("'{i}'"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Execution level of every node: roots and rows with no in-set prerequisite
/// are 0; a row runs one level after its parent and every `depends_on` row.
fn levels(nodes: &[ExpandedNode]) -> HashMap<&str, usize> {
    let by_id: HashMap<&str, &ExpandedNode> = nodes.iter().map(|n| (n.id.as_str(), n)).collect();
    fn level<'a>(
        id: &'a str,
        by_id: &HashMap<&'a str, &'a ExpandedNode>,
        memo: &mut HashMap<&'a str, usize>,
        visiting: &mut HashSet<&'a str>,
    ) -> usize {
        if let Some(l) = memo.get(id) {
            return *l;
        }
        let Some(node) = by_id.get(id) else {
            return 0;
        };
        if !visiting.insert(id) {
            return 0;
        }
        let mut prereqs: Vec<&'a str> = node.depends_on.iter().map(String::as_str).collect();
        if let NodeRole::Child { parent_id, .. } = &node.role {
            prereqs.push(parent_id.as_str());
        }
        let l = prereqs
            .into_iter()
            .filter(|p| by_id.contains_key(p))
            .map(|p| level(p, by_id, memo, visiting) + 1)
            .max()
            .unwrap_or(0);
        visiting.remove(id);
        memo.insert(id, l);
        l
    }
    let mut memo = HashMap::new();
    let mut visiting = HashSet::new();
    for n in nodes {
        level(n.id.as_str(), &by_id, &mut memo, &mut visiting);
    }
    memo
}

/// Refuse rows that share a full-overwrite destination but run in different
/// execution levels (#789 CLI-04): each level commits its own swap, so the
/// later level would replace the earlier one's rows.
pub fn check_overwrite_levels(nodes: &[ExpandedNode]) -> CliResult<()> {
    let level_of = levels(nodes);
    for node in nodes.iter().filter(|n| is_full_overwrite(n)) {
        for peer in peers(node, nodes) {
            if level_of.get(peer.id.as_str()) != level_of.get(node.id.as_str()) {
                return Err(CliError::Config(format!(
                    "rows '{}' and '{}' both replace {} with `write_mode: overwrite`, but one \
                     runs after the other (`parent:` / `depends_on`), so the second swap would \
                     delete the first row's data — give them separate destinations or make \
                     them run at the same level",
                    node.id,
                    peer.id,
                    describe(node)
                )));
            }
        }
    }
    Ok(())
}

/// Refuse post-run content verification where it cannot be meaningful
/// (#789 CLI-08 / CLI-09): an overwrite row (verify would read the old table,
/// before the swap) and a row whose destination other rows also write (a
/// partition chunk, or several rows on one table), whose source covers only
/// part of what the destination holds.
pub fn check_verify_scope(node: &ExpandedNode, all: &[ExpandedNode]) -> CliResult<()> {
    if is_overwrite(node) {
        return Err(CliError::Config(format!(
            "verify: row '{}' writes with `write_mode: overwrite`, which swaps the new data in \
             after the run, so a post-run check would read the replaced table — set \
             `verify.after_run: false` and run `faucet verify` once the run has finished",
            node.id
        )));
    }
    check_owns_destination(node, all)
}

/// Refuse to verify a row whose destination other rows also write: its source
/// covers only part of what the destination holds (#789 CLI-08).
pub fn check_owns_destination(node: &ExpandedNode, all: &[ExpandedNode]) -> CliResult<()> {
    let shared: Vec<&str> = peers(node, all).iter().map(|p| p.id.as_str()).collect();
    if !shared.is_empty() {
        return Err(CliError::Config(format!(
            "verify: row '{}' shares {} with row(s) {}, so its source covers only part of the \
             destination — every other row's data would count as a difference (and \
             `allow_delete` would remove it). Verify rows that own their destination",
            node.id,
            describe(node),
            quoted(&shared)
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::json;

    fn spec(kind: &str, config: Value) -> crate::config::ConnectorSpec {
        crate::config::ConnectorSpec {
            kind: kind.into(),
            config,
            transforms: None,
            inherit_transforms: true,
            status: None,
            tags: Vec::new(),
            complete_for: None,
            attributes: Default::default(),
        }
    }

    fn node(id: &str, role: NodeRole, deps: &[&str], sink: Value) -> ExpandedNode {
        ExpandedNode {
            id: id.into(),
            row_index: 0,
            weight: None,
            role,
            source: spec("csv", json!({})),
            sink: spec("sqlite", sink),
            transforms: Vec::new(),
            state: None,
            dlq: None,
            sla: None,
            profiling: None,
            #[cfg(feature = "policy")]
            policy: None,
            delivery: faucet_core::DeliveryMode::AtLeastOnce,
            delivery_guarantee: faucet_core::DeliveryGuarantee::AtLeastOnce,
            #[cfg(feature = "quality")]
            quality: None,
            #[cfg(feature = "contract")]
            contract: None,
            #[cfg(feature = "masking")]
            masking: None,
            sink_ref: "default".into(),
            schema: None,
            depends_on: deps.iter().map(|d| d.to_string()).collect(),
            status: crate::config::SourceStatus::Active,
            tags: Vec::new(),
            cleanup_scope: None,
            metadata_columns: None,
            #[cfg(feature = "catalog")]
            local_outputs: None,
            deferred_refs: Vec::new(),
            source_override: None,
        }
    }

    fn ow(table: &str) -> Value {
        json!({"database_url": "sqlite:x.db", "table_name": table, "write_mode": "overwrite"})
    }

    #[test]
    fn destination_key_ignores_write_knobs_and_skips_runtime_tokens() {
        let a = node("a", NodeRole::Root, &[], ow("t"));
        let mut b_cfg = ow("t");
        b_cfg["batch_size"] = json!(5);
        let b = node("b", NodeRole::Root, &[], b_cfg);
        assert_eq!(destination_key(&a), destination_key(&b));
        let c = node("c", NodeRole::Root, &[], ow("t_${p.id}"));
        assert!(destination_key(&c).is_none());
        let d = node("d", NodeRole::Root, &[], ow("t_${now.date}"));
        assert!(destination_key(&d).is_some());
        assert_eq!(describe(&a), "sqlite t");
        let mut bare = a.clone();
        bare.sink.config = json!({});
        assert_eq!(describe(&bare), "sqlite");
    }

    #[test]
    fn selection_that_drops_an_overwrite_peer_is_refused() {
        let us = node("us", NodeRole::Root, &[], ow("customers"));
        let eu = node("eu", NodeRole::Root, &[], ow("customers"));
        let other = node("other", NodeRole::Root, &[], ow("orders"));
        let all = vec![us.clone(), eu.clone(), other.clone()];
        let e = check_overwrite_selection(&all, std::slice::from_ref(&eu))
            .unwrap_err()
            .to_string();
        assert!(e.contains("'us'") && e.contains("customers"), "{e}");
        check_overwrite_selection(&all, &[us, eu]).unwrap();
        check_overwrite_selection(&all, &[other]).unwrap();

        let mut scoped_cfg = ow("customers");
        scoped_cfg["scope"] = json!({"column": "d"});
        let scoped = node("scoped", NodeRole::Root, &[], scoped_cfg);
        let peer = node("peer", NodeRole::Root, &[], ow("customers"));
        check_overwrite_selection(&[scoped.clone(), peer], &[scoped]).unwrap();
    }

    #[test]
    fn overwrite_peers_in_different_levels_are_refused() {
        let first = node("first", NodeRole::Root, &[], ow("t"));
        let second = node("second", NodeRole::Root, &["first"], ow("t"));
        let e = check_overwrite_levels(&[first.clone(), second])
            .unwrap_err()
            .to_string();
        assert!(e.contains("'first'") && e.contains("'second'"), "{e}");

        let sibling = node("sibling", NodeRole::Root, &[], ow("t"));
        check_overwrite_levels(&[first.clone(), sibling]).unwrap();

        let child = node(
            "child",
            NodeRole::Child {
                parent_id: "first".into(),
                parent_key: "id".into(),
            },
            &["missing"],
            ow("other"),
        );
        check_overwrite_levels(&[first, child]).unwrap();
    }

    #[test]
    fn levels_survive_a_dependency_cycle() {
        let a = node("a", NodeRole::Root, &["b"], ow("t"));
        let b = node("b", NodeRole::Root, &["a"], ow("u"));
        let nodes = [a, b];
        assert_eq!(levels(&nodes).len(), 2);
    }

    #[test]
    fn verify_scope_refuses_overwrite_and_shared_destinations() {
        let o = node("o", NodeRole::Root, &[], ow("t"));
        assert!(
            check_verify_scope(&o, std::slice::from_ref(&o))
                .unwrap_err()
                .to_string()
                .contains("after_run")
        );
        let a = node("a", NodeRole::Root, &[], json!({"table_name": "t"}));
        let b = node("b", NodeRole::Root, &[], json!({"table_name": "t"}));
        let e = check_verify_scope(&a, &[a.clone(), b.clone()])
            .unwrap_err()
            .to_string();
        assert!(e.contains("'b'"), "{e}");
        let c = node("c", NodeRole::Root, &[], json!({"table_name": "c"}));
        check_verify_scope(&c, &[a, b, c.clone()]).unwrap();
    }
}
