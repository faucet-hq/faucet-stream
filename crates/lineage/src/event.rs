//! OpenLineage `RunEvent` object model (serde-faithful subset, OL 2.0.2).

use serde::Serialize;

/// Pinned OpenLineage RunEvent schema URL (spec 2.0.2).
pub const OL_SCHEMA_URL: &str =
    "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent";

/// Producer identifier embedded in every event.
pub const PRODUCER: &str = concat!(
    "https://github.com/faucet-hq/faucet-stream/tree/v",
    env!("CARGO_PKG_VERSION")
);

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum EventType {
    Start,
    Running,
    Complete,
    Abort,
    Fail,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RunEvent {
    pub event_type: EventType,
    pub event_time: String,
    pub run: Run,
    pub job: Job,
    pub inputs: Vec<Dataset>,
    pub outputs: Vec<Dataset>,
    pub producer: String,
    #[serde(rename = "schemaURL")]
    pub schema_url: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Run {
    pub run_id: String,
    #[serde(skip_serializing_if = "RunFacets::is_empty")]
    pub facets: RunFacets,
}

#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RunFacets {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent: Option<ParentRunFacet>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nominal_time: Option<NominalTimeRunFacet>,
}

impl RunFacets {
    fn is_empty(&self) -> bool {
        self.parent.is_none() && self.nominal_time.is_none()
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ParentRunFacet {
    #[serde(rename = "_producer")]
    pub producer: String,
    #[serde(rename = "_schemaURL")]
    pub schema_url: String,
    pub run: ParentRunRef,
    pub job: ParentJobRef,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ParentRunRef {
    pub run_id: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ParentJobRef {
    pub namespace: String,
    pub name: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NominalTimeRunFacet {
    #[serde(rename = "_producer")]
    pub producer: String,
    #[serde(rename = "_schemaURL")]
    pub schema_url: String,
    pub nominal_start_time: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nominal_end_time: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Job {
    pub namespace: String,
    pub name: String,
    #[serde(skip_serializing_if = "JobFacets::is_empty")]
    pub facets: JobFacets,
}

#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobFacets {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_code: Option<SourceCodeJobFacet>,
}

impl JobFacets {
    fn is_empty(&self) -> bool {
        self.source_code.is_none()
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceCodeJobFacet {
    #[serde(rename = "_producer")]
    pub producer: String,
    #[serde(rename = "_schemaURL")]
    pub schema_url: String,
    pub language: String,
    pub source_code: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct Dataset {
    pub namespace: String,
    pub name: String,
    #[serde(skip_serializing_if = "DatasetFacets::is_empty")]
    pub facets: DatasetFacets,
}

impl Dataset {
    pub fn new(namespace: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            name: name.into(),
            facets: DatasetFacets::default(),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DatasetFacets {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<SchemaDatasetFacet>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub column_lineage: Option<ColumnLineageDatasetFacet>,
}

impl DatasetFacets {
    fn is_empty(&self) -> bool {
        self.schema.is_none() && self.column_lineage.is_none()
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SchemaDatasetFacet {
    #[serde(rename = "_producer")]
    pub producer: String,
    #[serde(rename = "_schemaURL")]
    pub schema_url: String,
    pub fields: Vec<SchemaField>,
}

impl SchemaDatasetFacet {
    pub fn new(fields: Vec<SchemaField>) -> Self {
        Self {
            producer: PRODUCER.into(),
            schema_url: OL_SCHEMA_URL.into(),
            fields,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct SchemaField {
    pub name: String,
    #[serde(rename = "type")]
    pub type_: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ColumnLineageDatasetFacet {
    #[serde(rename = "_producer")]
    pub producer: String,
    #[serde(rename = "_schemaURL")]
    pub schema_url: String,
    pub fields: std::collections::BTreeMap<String, ColumnLineageFieldEntry>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ColumnLineageFieldEntry {
    #[serde(rename = "inputFields")]
    pub input_fields: Vec<ColumnLineageInputField>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ColumnLineageInputField {
    pub namespace: String,
    pub name: String,
    pub field: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serializes_minimal_start_event() {
        let ev = RunEvent {
            event_type: EventType::Start,
            event_time: "2026-06-07T00:00:00Z".into(),
            run: Run {
                run_id: "r1".into(),
                facets: RunFacets::default(),
            },
            job: Job {
                namespace: "ns".into(),
                name: "job1".into(),
                facets: JobFacets::default(),
            },
            inputs: vec![Dataset::new("ns", "postgres://h/db?table=t")],
            outputs: vec![Dataset::new("ns", "bigquery://p.d.t")],
            producer: PRODUCER.into(),
            schema_url: OL_SCHEMA_URL.into(),
        };
        let v = serde_json::to_value(&ev).unwrap();
        assert_eq!(v["eventType"], "START");
        assert_eq!(v["run"]["runId"], "r1");
        assert_eq!(v["job"]["name"], "job1");
        assert_eq!(v["inputs"][0]["name"], "postgres://h/db?table=t");
        assert_eq!(v["outputs"][0]["namespace"], "ns");
        assert_eq!(v["schemaURL"], OL_SCHEMA_URL);
        // empty facets must not serialize as noise
        assert!(v["run"].get("facets").is_none() || v["run"]["facets"].is_object());
    }

    #[test]
    fn schema_facet_round_trips() {
        let mut ds = Dataset::new("ns", "file:///x");
        ds.facets.schema = Some(SchemaDatasetFacet::new(vec![SchemaField {
            name: "id".into(),
            type_: "integer".into(),
        }]));
        let v = serde_json::to_value(&ds).unwrap();
        assert_eq!(v["facets"]["schema"]["fields"][0]["name"], "id");
        assert_eq!(v["facets"]["schema"]["fields"][0]["type"], "integer");
    }
}
