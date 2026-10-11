//! In-process cache of `Relation` messages so subsequent `Insert`/`Update`/
//! `Delete` events can look up column names + type OIDs by relation OID.

use super::messages::Relation;
use faucet_core::FaucetError;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Default)]
pub struct RelationRegistry {
    by_oid: HashMap<u32, Relation>,
    roots: HashMap<u32, String>,
    resolved: HashSet<u32>,
    unresolved: Vec<u32>,
}

impl RelationRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, rel: Relation) {
        // A re-sent Relation with a different column set means the table's
        // schema changed mid-stream (ALTER TABLE). Subsequent tuples decode
        // against the *new* descriptor, but a same-arity rename/type change
        // can silently bind values to the wrong column names — surface it so
        // an operator can correlate any downstream surprise (#78/#46).
        if let Some(prev) = self.by_oid.get(&rel.oid) {
            let prev_cols: Vec<(&str, u32)> = prev
                .columns
                .iter()
                .map(|c| (c.name.as_str(), c.type_oid))
                .collect();
            let new_cols: Vec<(&str, u32)> = rel
                .columns
                .iter()
                .map(|c| (c.name.as_str(), c.type_oid))
                .collect();
            if prev_cols != new_cols {
                tracing::warn!(
                    relation = %rel.name,
                    oid = rel.oid,
                    "postgres-cdc: relation column set changed mid-stream (schema change); \
                     subsequent rows decode against the new descriptor"
                );
            }
        }
        if !self.resolved.contains(&rel.oid) && !self.unresolved.contains(&rel.oid) {
            self.unresolved.push(rel.oid);
        }
        self.by_oid.insert(rel.oid, rel);
    }

    /// Relation oids seen since the last call whose partition root is unknown.
    pub fn take_unresolved(&mut self) -> Vec<u32> {
        std::mem::take(&mut self.unresolved)
    }

    /// Record the partition roots (`schema.table`) of `oids`; an oid absent
    /// from `roots` is not a partition.
    pub fn set_roots(&mut self, oids: &[u32], roots: HashMap<u32, String>) {
        self.resolved.extend(oids.iter().copied());
        self.roots.extend(roots);
    }

    /// The `schema.table` of the partitioned table `oid` is a leaf of.
    pub fn root(&self, oid: u32) -> Option<&str> {
        self.roots.get(&oid).map(String::as_str)
    }

    pub fn get(&self, oid: u32) -> Result<&Relation, FaucetError> {
        self.by_oid.get(&oid).ok_or_else(|| {
            FaucetError::Source(format!(
                "pgoutput: change event for unknown relation oid {oid} \
                 (Relation message must precede first change)"
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pgoutput::messages::ReplicaIdentity;

    fn rel(oid: u32, name: &str) -> Relation {
        Relation {
            oid,
            namespace: "public".into(),
            name: name.into(),
            replica_identity: ReplicaIdentity::Default,
            columns: vec![],
        }
    }

    #[test]
    fn new_relations_are_queued_once_for_partition_resolution() {
        let mut r = RelationRegistry::new();
        r.insert(rel(1, "parted_eu"));
        r.insert(rel(1, "parted_eu"));
        r.insert(rel(2, "users"));
        let pending = r.take_unresolved();
        assert_eq!(pending, vec![1, 2]);
        r.set_roots(&pending, HashMap::from([(1, "public.parted".to_string())]));
        assert_eq!(r.root(1), Some("public.parted"));
        assert_eq!(r.root(2), None);
        r.insert(rel(1, "parted_eu"));
        assert!(r.take_unresolved().is_empty());
    }

    #[test]
    fn insert_then_get() {
        let mut r = RelationRegistry::new();
        r.insert(rel(16384, "users"));
        assert_eq!(r.get(16384).unwrap().name, "users");
    }

    #[test]
    fn second_insert_replaces() {
        let mut r = RelationRegistry::new();
        r.insert(rel(16384, "users_v1"));
        r.insert(rel(16384, "users_v2"));
        assert_eq!(r.get(16384).unwrap().name, "users_v2");
    }

    #[test]
    fn missing_oid_errors() {
        let r = RelationRegistry::new();
        let err = r.get(99999).unwrap_err();
        assert!(format!("{err}").contains("99999"));
    }

    #[test]
    fn reinsert_with_changed_columns_warns_and_replaces() {
        use crate::pgoutput::messages::ColumnDesc;
        let col = |name: &str, oid: u32| ColumnDesc {
            flags: 0,
            name: name.into(),
            type_oid: oid,
            type_modifier: -1,
        };
        let mut rel_v1 = rel(16384, "users");
        rel_v1.columns = vec![col("id", 23)];
        let mut rel_v2 = rel(16384, "users");
        // A different column set (added column) exercises the schema-change
        // warning branch; subsequent lookups bind against the new descriptor.
        rel_v2.columns = vec![col("id", 23), col("email", 25)];

        let mut r = RelationRegistry::new();
        r.insert(rel_v1);
        r.insert(rel_v2);

        let got = r.get(16384).unwrap();
        assert_eq!(got.columns.len(), 2);
        assert_eq!(got.columns[1].name, "email");
        assert_eq!(got.columns[1].type_oid, 25);
    }
}
