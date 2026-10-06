//! A registered template's rows (#741) — the registry adapter over
//! [`crate::hub::rows`]: resolve the versions, dispatch on the kind, read the
//! run history the status view needs.

use super::OverlayChoice;
use super::store::{TemplateStore, parse_body, resolve_overlay};
use crate::error::{CliError, CliResult};
use crate::hub::rows::{ListOptions, RowsReport, list_pipeline, list_source, pipeline_name};
use crate::hub::spec::TemplateKind;
use crate::select::SelectionRequest;

/// What to list.
#[derive(Debug, Default)]
pub struct RowsQuery<'a> {
    pub id: &'a str,
    pub version: u32,
    /// For a source template: the sink template (id, concrete version).
    pub sink: Option<(&'a str, u32)>,
    /// A deployment overlay applied over the pairing.
    pub overlay: Option<OverlayChoice>,
    pub selection: Option<&'a SelectionRequest>,
    pub state: bool,
}

async fn record(
    store: &TemplateStore,
    id: &str,
    version: u32,
) -> CliResult<crate::serve::history::templates::TemplateRecord> {
    store
        .template_get(id, Some(version))
        .await
        .map_err(|e| crate::templates::store::registry_err("template registry read", e))?
        .ok_or_else(|| CliError::UnknownPipelineTemplate {
            id: id.to_string(),
            version: Some(version),
        })
}

async fn history(
    store: &TemplateStore,
    name: &str,
) -> (Vec<crate::status::HistoryRun>, Vec<String>) {
    crate::status::history::read(store.as_ref(), name)
        .await
        .unwrap_or_default()
}

/// List a registered template's rows.
pub async fn list_rows(store: &TemplateStore, q: RowsQuery<'_>) -> CliResult<RowsReport> {
    let rec = record(store, q.id, q.version).await?;
    let doc = parse_body(&rec.body, rec.format)?;
    let mut report = match rec.kind {
        TemplateKind::Pipeline => {
            if let Some((sink, _)) = q.sink {
                return Err(CliError::Config(format!(
                    "'{}' is a complete pipeline template — it takes no sink (got `{sink}`)",
                    q.id
                )));
            }
            let name = pipeline_name(&doc, q.id);
            let history = if q.state {
                history(store, &name).await
            } else {
                Default::default()
            };
            list_pipeline(
                &doc,
                q.id,
                ListOptions {
                    selection: q.selection,
                    state: q.state,
                    history,
                },
            )
            .await?
        }
        TemplateKind::SourceTemplate => {
            let src: crate::hub::SourceTemplate = serde_json::from_value(doc).map_err(|e| {
                CliError::Internal(format!("stored source-template '{}': {e}", q.id))
            })?;
            let sink = match q.sink {
                Some((id, v)) => {
                    let s = record(store, id, v).await?;
                    if s.kind != TemplateKind::SinkTemplate {
                        return Err(CliError::Config(format!(
                            "'{id}' is a {} — `sink` must name a sink-template",
                            s.kind
                        )));
                    }
                    let t: crate::hub::SinkTemplate =
                        serde_json::from_value(parse_body(&s.body, s.format)?).map_err(|e| {
                            CliError::Internal(format!("stored sink-template '{id}': {e}"))
                        })?;
                    Some((t, s.version))
                }
                None => None,
            };
            let overlay = match &q.overlay {
                Some(choice) => Some(resolve_overlay(store, choice).await?.0),
                None => None,
            };
            let history = if q.state {
                history(store, &src.id()).await
            } else {
                Default::default()
            };
            let mut report = list_source(
                &src,
                sink.as_ref().map(|(t, _)| t),
                overlay.as_ref(),
                ListOptions {
                    selection: q.selection,
                    state: q.state,
                    history,
                },
            )
            .await?;
            report.sink_version = sink.map(|(_, v)| v);
            report
        }
        TemplateKind::SinkTemplate | TemplateKind::Deployment => {
            return Err(CliError::Config(format!(
                "'{}' is a {} — it has no rows of its own; list a source template's rows with \
                 `sink` set to it",
                q.id, rec.kind
            )));
        }
    };
    report.template = rec.id;
    report.version = Some(rec.version);
    Ok(report)
}
