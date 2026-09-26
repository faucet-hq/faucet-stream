//! Serde config types for the top-level `catalog:` block (#279).

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

fn default_sample_records() -> usize {
    100
}

/// The top-level `catalog:` block: opts a `faucet run` / `schedule` /
/// `replicate` pipeline into recording the Data Movement Catalog after every
/// successful root invocation. `faucet serve` records into its `--history`
/// backend automatically — this block is for the non-serve runtimes (and for
/// `faucet catalog`, which reads the same store).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CatalogSpec {
    /// Where the catalog is stored: `sqlite:<path>` (e.g.
    /// `sqlite:./faucet-catalog.db`), a `postgres://…` URL, or `memory`
    /// (process-lifetime only — useful for tests). SQL backends require the
    /// matching `serve-history-sqlite` / `serve-history-postgres` build
    /// feature. Point `faucet serve --history` at the same URL to browse the
    /// accumulated catalog in the control plane + web console.
    pub url: String,

    /// How many records to sample per run for schema inference (per side).
    /// The sample bounds memory; the schema timeline only ever stores the
    /// inferred schema, never the records.
    #[serde(default = "default_sample_records")]
    pub sample_records: usize,

    /// Dataset annotations (#707): owners and declared external consumers,
    /// merged into the catalog after every run that touches the dataset.
    /// Pipelines that read a dataset are consumers automatically (lineage);
    /// list here what the catalog cannot see — dashboards, models, exports —
    /// so `faucet plan --impact` can name who a change affects.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub datasets: Vec<DatasetAnnotationSpec>,
}

/// One dataset's owners + consumers (#707).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DatasetAnnotationSpec {
    /// The dataset's canonical URI exactly as `faucet catalog datasets` prints
    /// it (credential-redacted, `${now.*}` segments as tokens), or its 16-hex
    /// id. Matched against the run's source and sink datasets.
    pub dataset: String,
    /// Owners — a team, an email, a pager rotation.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub owners: Vec<String>,
    /// External consumers.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub consumers: Vec<ConsumerSpec>,
}

/// One declared consumer (#707).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ConsumerSpec {
    /// Unique per dataset.
    pub name: String,
    /// `dashboard`, `model`, `export`, `report`, … (free-form).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// Who to tell — an email, a channel, a URL.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contact: Option<String>,
    /// The columns it reads. Empty = every column.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<String>,
}

impl DatasetAnnotationSpec {
    /// Whether this annotation targets the dataset with `id` / `uri`.
    pub fn matches(&self, id: &str, uri: &str) -> bool {
        self.dataset == id || self.dataset == uri
    }

    /// The annotation to merge, attributed to `config`.
    pub fn to_annotation(
        &self,
        now: chrono::DateTime<chrono::Utc>,
    ) -> crate::serve::history::catalog::CatalogAnnotation {
        crate::serve::history::catalog::CatalogAnnotation {
            owners: (!self.owners.is_empty()).then(|| self.owners.clone()),
            consumers: self
                .consumers
                .iter()
                .map(|c| crate::serve::history::catalog::CatalogConsumer {
                    name: c.name.clone(),
                    kind: c.kind.clone(),
                    contact: c.contact.clone(),
                    columns: c.columns.clone(),
                    registered_by: "config".to_string(),
                    registered_at: now,
                })
                .collect(),
            replace_consumers: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_minimal_block_with_defaults() {
        let spec: CatalogSpec = serde_yaml::from_str("url: sqlite:./cat.db").unwrap();
        assert_eq!(spec.url, "sqlite:./cat.db");
        assert_eq!(spec.sample_records, 100);
        assert!(spec.datasets.is_empty());
    }

    #[test]
    fn rejects_unknown_fields() {
        let err = serde_yaml::from_str::<CatalogSpec>("url: memory\nnope: 1").unwrap_err();
        assert!(err.to_string().contains("nope"));
    }

    #[test]
    fn schema_generates() {
        let schema = schemars::schema_for!(CatalogSpec);
        let v = serde_json::to_value(&schema).unwrap();
        assert!(v["properties"]["url"].is_object());
    }
}
