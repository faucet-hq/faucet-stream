//! Classify a state key under a pipeline's namespace (`{name}::…`) into what
//! it holds: a row's bookmark, a child/shard sub-bookmark, or one of the
//! reserved per-row / per-pipeline markers.

use serde::Serialize;

/// `{base}::__status__` — the last run outcomes of one invocation.
pub const STATUS_SUFFIX: &str = "__status__";
/// `{base}::__lease__` — held while a run of the invocation is in flight.
pub const LEASE_SUFFIX: &str = "__lease__";

/// What one key holds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "id")]
pub enum KeyKind {
    /// A resume position (bare bookmark or exactly-once envelope).
    Bookmark,
    /// `::__sla__` — SLA history.
    Sla,
    /// `::__profiling__` — column-profile baseline.
    Profiling,
    /// `::__rollback__` — retained undoable runs.
    RollbackIndex,
    /// `::__rollback__::{run}` — one undoable run's marker.
    RollbackRun(String),
    /// `::__status__` — last run outcomes.
    Status,
    /// `::__lease__` — an in-flight run.
    Lease,
    /// `{name}::__replication__` — the snapshot→CDC phase marker.
    Replication,
    /// `{name}::__backfill__::{hash}` — a backfill's progress marker.
    BackfillMarker(String),
    /// `{name}::backfill::{unit}` — one backfill unit's bookmark.
    BackfillUnit(String),
}

impl KeyKind {
    /// Whether this is a marker (anything but a resume position).
    pub fn is_marker(&self) -> bool {
        !matches!(self, Self::Bookmark | Self::BackfillUnit(_))
    }

    /// Short label for tables.
    pub fn label(&self) -> &'static str {
        match self {
            Self::Bookmark => "bookmark",
            Self::Sla => "sla",
            Self::Profiling => "profiling",
            Self::RollbackIndex => "rollback-index",
            Self::RollbackRun(_) => "rollback-run",
            Self::Status => "status",
            Self::Lease => "lease",
            Self::Replication => "replication",
            Self::BackfillMarker(_) => "backfill-marker",
            Self::BackfillUnit(_) => "backfill-unit",
        }
    }
}

/// A key under a pipeline's namespace, decoded.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ClassifiedKey {
    pub key: String,
    /// The matrix row / topology sink node the key belongs to (`None` for the
    /// pipeline-level markers).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub row: Option<String>,
    /// The part between the row and the marker: a child's parent-key value, a
    /// product tuple, or a shard id.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sub: Option<String>,
    #[serde(flatten)]
    pub kind: KeyKind,
}

impl ClassifiedKey {
    /// The invocation key a marker hangs off (`{name}::{row}[::{sub}]`).
    pub fn base(&self, pipeline: &str) -> Option<String> {
        let row = self.row.as_ref()?;
        Some(match &self.sub {
            Some(sub) => format!("{pipeline}::{row}::{sub}"),
            None => format!("{pipeline}::{row}"),
        })
    }
}

/// Decode `key` relative to `pipeline`; `None` when it is outside the namespace.
pub fn classify(pipeline: &str, key: &str) -> Option<ClassifiedKey> {
    let rest = key.strip_prefix(pipeline)?.strip_prefix("::")?;
    if rest.is_empty() {
        return None;
    }
    let segs: Vec<&str> = rest.split("::").collect();
    let join = |s: &[&str]| s.join("::");
    let pipeline_level = |kind| ClassifiedKey {
        key: key.to_string(),
        row: None,
        sub: None,
        kind,
    };
    match segs[0] {
        "__replication__" => return Some(pipeline_level(KeyKind::Replication)),
        "__backfill__" if segs.len() > 1 => {
            return Some(pipeline_level(KeyKind::BackfillMarker(join(&segs[1..]))));
        }
        "backfill" if segs.len() > 1 => {
            // A unit's own run markers hang off `{name}::backfill::{unit}`.
            let (unit_end, kind) = match segs.last() {
                Some(&STATUS_SUFFIX) if segs.len() > 2 => (segs.len() - 1, Some(KeyKind::Status)),
                Some(&LEASE_SUFFIX) if segs.len() > 2 => (segs.len() - 1, Some(KeyKind::Lease)),
                _ => (segs.len(), None),
            };
            let unit = join(&segs[1..unit_end]);
            return Some(match kind {
                Some(kind) => ClassifiedKey {
                    key: key.to_string(),
                    row: None,
                    sub: Some(unit),
                    kind,
                },
                None => pipeline_level(KeyKind::BackfillUnit(unit)),
            });
        }
        _ => {}
    }
    let row = segs[0].to_string();
    let reserved = segs.iter().enumerate().skip(1).find_map(|(i, s)| {
        let kind = match *s {
            "__sla__" => KeyKind::Sla,
            "__profiling__" => KeyKind::Profiling,
            STATUS_SUFFIX => KeyKind::Status,
            LEASE_SUFFIX => KeyKind::Lease,
            "__rollback__" if i + 1 < segs.len() => KeyKind::RollbackRun(join(&segs[i + 1..])),
            "__rollback__" => KeyKind::RollbackIndex,
            _ => return None,
        };
        Some((i, kind))
    });
    let (end, kind) = reserved.unwrap_or((segs.len(), KeyKind::Bookmark));
    let sub = (end > 1).then(|| join(&segs[1..end]));
    Some(ClassifiedKey {
        key: key.to_string(),
        row: Some(row),
        sub,
        kind,
    })
}

/// `{base}::__status__`.
pub fn status_key(base: &str) -> String {
    format!("{base}::{STATUS_SUFFIX}")
}

/// `{base}::__lease__`.
pub fn lease_key(base: &str) -> String {
    format!("{base}::{LEASE_SUFFIX}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn c(key: &str) -> ClassifiedKey {
        classify("orders", key).unwrap_or_else(|| panic!("{key} should classify"))
    }

    #[test]
    fn classifies_every_shape() {
        let k = c("orders::customers");
        assert_eq!(
            (k.row.as_deref(), k.sub.as_deref()),
            (Some("customers"), None)
        );
        assert_eq!(k.kind, KeyKind::Bookmark);
        assert_eq!(k.base("orders").unwrap(), "orders::customers");

        let k = c("orders::lines::42");
        assert_eq!(k.sub.as_deref(), Some("42"));
        assert_eq!(k.kind, KeyKind::Bookmark);
        assert_eq!(k.base("orders").unwrap(), "orders::lines::42");

        assert_eq!(c("orders::a::__sla__").kind, KeyKind::Sla);
        assert_eq!(c("orders::a::__profiling__").kind, KeyKind::Profiling);
        assert_eq!(c("orders::a::__status__").kind, KeyKind::Status);
        assert_eq!(c("orders::a::__lease__").kind, KeyKind::Lease);
        assert_eq!(c("orders::a::__rollback__").kind, KeyKind::RollbackIndex);
        assert_eq!(
            c("orders::a::__rollback__::r1").kind,
            KeyKind::RollbackRun("r1".into())
        );
        let k = c("orders::a::7::__status__");
        assert_eq!((k.sub.as_deref(), k.kind), (Some("7"), KeyKind::Status));
        assert_eq!(c("orders::__replication__").kind, KeyKind::Replication);
        assert_eq!(c("orders::__replication__").base("orders"), None);
        assert_eq!(
            c("orders::__backfill__::abc").kind,
            KeyKind::BackfillMarker("abc".into())
        );
        assert_eq!(
            c("orders::backfill::2026-09-01").kind,
            KeyKind::BackfillUnit("2026-09-01".into())
        );
        let k = c("orders::backfill::u1::__status__");
        assert_eq!(
            (k.row, k.sub.as_deref(), k.kind),
            (None, Some("u1"), KeyKind::Status)
        );
        let k = c("orders::backfill::u1::__lease__");
        assert_eq!(k.kind, KeyKind::Lease);
        // A row literally named `backfill` with no unit is still a row.
        assert_eq!(c("orders::backfill").kind, KeyKind::Bookmark);
    }

    #[test]
    fn outside_the_namespace_is_none() {
        assert!(classify("orders", "other::a").is_none());
        assert!(classify("orders", "ordersx::a").is_none());
        assert!(classify("orders", "orders::").is_none());
        assert!(classify("orders", "orders").is_none());
    }

    #[test]
    fn labels_and_marker_flags() {
        let all = [
            KeyKind::Bookmark,
            KeyKind::Sla,
            KeyKind::Profiling,
            KeyKind::RollbackIndex,
            KeyKind::RollbackRun("r".into()),
            KeyKind::Status,
            KeyKind::Lease,
            KeyKind::Replication,
            KeyKind::BackfillMarker("h".into()),
            KeyKind::BackfillUnit("u".into()),
        ];
        let labels: Vec<_> = all.iter().map(KeyKind::label).collect();
        assert_eq!(labels.len(), 10);
        assert!(!KeyKind::Bookmark.is_marker());
        assert!(!KeyKind::BackfillUnit("u".into()).is_marker());
        assert!(KeyKind::Sla.is_marker());
        assert_eq!(status_key("p::r"), "p::r::__status__");
        assert_eq!(lease_key("p::r"), "p::r::__lease__");
    }
}
