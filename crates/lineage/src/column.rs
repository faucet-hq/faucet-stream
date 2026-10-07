//! Deterministic column-level lineage from a declarative transform chain.
//!
//! v1 supports the field-preserving / explicit-mapping transforms exactly.
//! Any structure-changing or key-rewriting transform (`flatten`, `explode`,
//! `keys_case`, `rename_keys`) and any `Custom`/unknown transform is
//! [`ColumnOp::Opaque`]; if the chain contains one, [`derive()`] returns `None`
//! and **no** column-lineage facet is emitted (never fabricated).

use indexmap::IndexMap;
use std::collections::BTreeSet;

/// A lineage-relevant view of one transform stage. The CLI maps its resolved
/// transform specs onto these.
#[derive(Debug, Clone)]
pub enum ColumnOp {
    /// Keys unchanged (cast / redact / value_case / spell_symbols / filter).
    Identity,
    /// Explicit 1:1 key renames (old → new).
    Rename(Vec<(String, String)>),
    /// Keep only these top-level keys.
    Select(Vec<String>),
    /// Remove these top-level keys.
    Drop(Vec<String>),
    /// Add these new keys as literals (no upstream edge).
    Set(Vec<String>),
    /// Structure-changing / key-rewriting / unknown — lineage not derivable.
    Opaque,
}

/// Output field → ordered list of originating input field names. A field with
/// an empty source list is a literal (`set`).
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnLineage {
    pub edges: IndexMap<String, Vec<String>>,
}

/// Fold the transform chain over the input field set. Returns `None` if any op
/// is [`ColumnOp::Opaque`].
pub fn derive(input_fields: &[String], ops: &[ColumnOp]) -> Option<ColumnLineage> {
    if ops.iter().any(|o| matches!(o, ColumnOp::Opaque)) {
        return None;
    }
    // Working map: current field name → set of source fields.
    let mut cur: IndexMap<String, BTreeSet<String>> = IndexMap::new();
    for f in input_fields {
        cur.insert(f.clone(), BTreeSet::from([f.clone()]));
    }
    for op in ops {
        match op {
            ColumnOp::Identity => {}
            ColumnOp::Rename(pairs) => {
                // Mirrors the runtime: every pair reads the pre-rename record.
                let staged: Vec<(String, BTreeSet<String>)> = pairs
                    .iter()
                    .filter(|(from, to)| from != to)
                    .filter_map(|(from, to)| cur.get(from).map(|s| (to.clone(), s.clone())))
                    .collect();
                for (from, to) in pairs {
                    if from != to {
                        cur.shift_remove(from);
                    }
                }
                for (to, sources) in staged {
                    cur.insert(to, sources);
                }
            }
            ColumnOp::Select(keep) => {
                let keep: BTreeSet<&String> = keep.iter().collect();
                cur.retain(|k, _| keep.contains(k));
            }
            ColumnOp::Drop(remove) => {
                let remove: BTreeSet<&String> = remove.iter().collect();
                cur.retain(|k, _| !remove.contains(k));
            }
            ColumnOp::Set(added) => {
                for f in added {
                    cur.insert(f.clone(), BTreeSet::new());
                }
            }
            ColumnOp::Opaque => unreachable!("guarded above"),
        }
    }
    let edges = cur
        .into_iter()
        .map(|(k, v)| (k, v.into_iter().collect()))
        .collect();
    Some(ColumnLineage { edges })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn inputs() -> Vec<String> {
        vec!["id".into(), "name".into(), "email".into()]
    }

    #[test]
    fn identity_chain_maps_each_field_to_itself() {
        let cl = derive(&inputs(), &[ColumnOp::Identity]).unwrap();
        assert_eq!(cl.edges.get("id").unwrap(), &vec!["id".to_string()]);
        assert_eq!(cl.edges.len(), 3);
    }

    #[test]
    fn rename_field_rekeys_and_preserves_source() {
        let ops = [ColumnOp::Rename(vec![("email".into(), "contact".into())])];
        let cl = derive(&inputs(), &ops).unwrap();
        assert_eq!(cl.edges.get("contact").unwrap(), &vec!["email".to_string()]);
        assert!(!cl.edges.contains_key("email"));
    }

    #[test]
    fn interacting_renames_read_the_pre_rename_record() {
        let ab = || vec!["a".to_string(), "b".to_string()];
        let swap = [ColumnOp::Rename(vec![
            ("a".into(), "b".into()),
            ("b".into(), "a".into()),
        ])];
        let cl = derive(&ab(), &swap).unwrap();
        assert_eq!(cl.edges.get("a").unwrap(), &vec!["b".to_string()]);
        assert_eq!(cl.edges.get("b").unwrap(), &vec!["a".to_string()]);
        assert_eq!(cl.edges.len(), 2);

        let chain = [ColumnOp::Rename(vec![
            ("a".into(), "b".into()),
            ("b".into(), "c".into()),
        ])];
        let cl = derive(&ab(), &chain).unwrap();
        assert_eq!(cl.edges.get("b").unwrap(), &vec!["a".to_string()]);
        assert_eq!(cl.edges.get("c").unwrap(), &vec!["b".to_string()]);
        assert!(!cl.edges.contains_key("a"));

        let noop = [ColumnOp::Rename(vec![("a".into(), "a".into())])];
        assert_eq!(derive(&ab(), &noop).unwrap().edges.len(), 2);
    }

    #[test]
    fn select_retains_only_listed() {
        let cl = derive(&inputs(), &[ColumnOp::Select(vec!["id".into()])]).unwrap();
        assert_eq!(
            cl.edges.keys().cloned().collect::<Vec<_>>(),
            vec!["id".to_string()]
        );
    }

    #[test]
    fn drop_removes_listed() {
        let cl = derive(&inputs(), &[ColumnOp::Drop(vec!["email".into()])]).unwrap();
        assert!(!cl.edges.contains_key("email"));
        assert_eq!(cl.edges.len(), 2);
    }

    #[test]
    fn set_adds_field_with_no_upstream() {
        let cl = derive(&inputs(), &[ColumnOp::Set(vec!["created".into()])]).unwrap();
        assert!(cl.edges.get("created").unwrap().is_empty());
    }

    #[test]
    fn opaque_op_yields_no_lineage() {
        assert!(derive(&inputs(), &[ColumnOp::Opaque]).is_none());
        assert!(derive(&inputs(), &[ColumnOp::Identity, ColumnOp::Opaque]).is_none());
    }
}
