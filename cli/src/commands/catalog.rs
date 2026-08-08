//! `faucet catalog` — browse the Data Movement Catalog (#279) accumulated by
//! a config's `catalog:` store: datasets, schema timelines, volume/freshness,
//! and the lineage graph. Read-only; the same store `faucet run` / `schedule`
//! / `replicate` write into (point `faucet serve --history` at the same URL to
//! browse it in the control plane / web console instead).

use crate::catalog::CatalogHandle;
use crate::cli::{
    CatalogArgs, CatalogCommand, CatalogConfigArgs, CatalogDatasetsArgs, CatalogLineageArgs,
    CatalogShowArgs,
};
use crate::config::PipelineConfig;
use crate::error::{CliError, CliResult};
use crate::serve::history::catalog::{
    CatalogDataset, CatalogDatasetDetail, CatalogLineageEdge, CatalogListFilter,
};

/// Pretty-print any serializable value (JSON output mode).
fn to_pretty<T: serde::Serialize>(value: &T) -> CliResult<String> {
    serde_json::to_string_pretty(value)
        .map_err(|e| CliError::Internal(format!("rendering catalog JSON: {e}")))
}

/// Execute the `catalog` subcommand.
pub async fn run(args: CatalogArgs) -> CliResult<()> {
    match args.command {
        CatalogCommand::Datasets(a) => datasets(a).await,
        CatalogCommand::Show(a) => show(a).await,
        CatalogCommand::Lineage(a) => lineage(a).await,
    }
}

/// Load the config named by the shared flags and connect its `catalog:` store.
async fn connect(common: &CatalogConfigArgs) -> CliResult<CatalogHandle> {
    let cwd = std::env::current_dir()?;
    let env_path =
        crate::env_loader::resolve_env_file(common.env_file.as_deref(), common.no_env_file, &cwd)?;
    crate::env_loader::load_env_file_if_present(env_path.as_deref())?;
    let path = match &common.config {
        Some(p) => p.clone(),
        None => crate::env_loader::discover_config_path(&cwd).ok_or(CliError::NoConfigOrFromEnv)?,
    };
    let cfg = PipelineConfig::from_path_async(&path, common.profile.as_deref()).await?;
    let spec = cfg.catalog.as_ref().ok_or_else(|| {
        CliError::Config(
            "no `catalog:` block in this config — add one naming the store (e.g. \
             `catalog: { url: sqlite:./faucet-catalog.db }`), or run \
             `faucet schema catalog` for the block's JSON Schema"
                .to_string(),
        )
    })?;
    crate::catalog::connect_from_spec(spec).await
}

async fn datasets(args: CatalogDatasetsArgs) -> CliResult<()> {
    let handle = connect(&args.common).await?;
    let page = handle
        .store
        .catalog_list_datasets(&CatalogListFilter {
            kind: args.kind,
            q: args.q,
            limit: args.limit.max(1),
            cursor: None,
        })
        .await
        .map_err(|e| CliError::Internal(format!("catalog read: {e}")))?;
    if args.common.json {
        println!("{}", to_pretty(&page)?);
        return Ok(());
    }
    if page.datasets.is_empty() {
        println!("catalog is empty — run a pipeline with this `catalog:` store first");
        return Ok(());
    }
    println!(
        "{:<16}  {:<12}  {:<12}  {:>5}  {:>12}  {:<20}  URI",
        "ID", "KIND", "ROLES", "RUNS", "ROWS (LAST)", "LAST SUCCESS"
    );
    for d in &page.datasets {
        println!(
            "{:<16}  {:<12}  {:<12}  {:>5}  {:>12}  {:<20}  {}",
            d.id,
            d.kind,
            d.roles.join(","),
            d.runs,
            d.last_records,
            d.last_success.format("%Y-%m-%dT%H:%M:%SZ"),
            d.uri
        );
    }
    if page.next_cursor.is_some() {
        println!("… more — raise --limit to see the rest");
    }
    Ok(())
}

/// Resolve `id` against the store, accepting a unique prefix of a dataset id.
async fn resolve_dataset(
    handle: &CatalogHandle,
    id: &str,
) -> CliResult<Option<CatalogDatasetDetail>> {
    if let Some(detail) = handle
        .store
        .catalog_get_dataset(id)
        .await
        .map_err(|e| CliError::Internal(format!("catalog read: {e}")))?
    {
        return Ok(Some(detail));
    }
    // Prefix match over the (bounded) dataset list.
    let page = handle
        .store
        .catalog_list_datasets(&CatalogListFilter {
            limit: 1000,
            ..Default::default()
        })
        .await
        .map_err(|e| CliError::Internal(format!("catalog read: {e}")))?;
    let matches: Vec<&CatalogDataset> = page
        .datasets
        .iter()
        .filter(|d| d.id.starts_with(id))
        .collect();
    match matches.as_slice() {
        [one] => {
            let full = one.id.clone();
            handle
                .store
                .catalog_get_dataset(&full)
                .await
                .map_err(|e| CliError::Internal(format!("catalog read: {e}")))
        }
        [] => Ok(None),
        many => Err(CliError::Config(format!(
            "dataset id prefix '{id}' is ambiguous ({} matches) — use the full id",
            many.len()
        ))),
    }
}

async fn show(args: CatalogShowArgs) -> CliResult<()> {
    let handle = connect(&args.common).await?;
    let detail = resolve_dataset(&handle, &args.id).await?.ok_or_else(|| {
        CliError::Config(format!(
            "no catalogued dataset with id '{}' — list ids with `faucet catalog datasets`",
            args.id
        ))
    })?;
    if args.common.json {
        println!("{}", to_pretty(&detail)?);
        return Ok(());
    }
    let d = &detail.dataset;
    println!("dataset  {}", d.uri);
    println!("id       {}", d.id);
    println!("kind     {}   roles {}", d.kind, d.roles.join(","));
    println!("pipeline {}   last run {}", d.pipeline, d.last_run_id);
    println!(
        "runs     {}   rows {} (last) / {} (total)",
        d.runs, d.last_records, d.total_records
    );
    println!(
        "seen     {} → {}   last success {}",
        d.first_seen.format("%Y-%m-%dT%H:%M:%SZ"),
        d.last_seen.format("%Y-%m-%dT%H:%M:%SZ"),
        d.last_success.format("%Y-%m-%dT%H:%M:%SZ"),
    );

    println!(
        "\nschema timeline ({} versions):",
        detail.schema_timeline.len()
    );
    for v in &detail.schema_timeline {
        let cols = v.schema["properties"]
            .as_object()
            .map(|p| p.len())
            .unwrap_or(0);
        print!(
            "  v{}  {}  {} column(s)  run {}",
            v.version,
            v.recorded_at.format("%Y-%m-%dT%H:%M:%SZ"),
            cols,
            v.run_id
        );
        if let Some(diff) = &v.diff {
            let names = |key: &str| -> Vec<String> {
                diff[key]
                    .as_array()
                    .map(|a| {
                        a.iter()
                            .filter_map(|c| {
                                c.get("column")
                                    .or(Some(c))
                                    .and_then(|v| v.as_str())
                                    .map(String::from)
                            })
                            .collect()
                    })
                    .unwrap_or_default()
            };
            let mut parts = Vec::new();
            for (label, key) in [
                ("+", "added"),
                ("~", "widened"),
                ("!", "changed"),
                ("-", "removed"),
            ] {
                let n = names(key);
                if !n.is_empty() {
                    parts.push(format!("{label}{}", n.join(&format!(" {label}"))));
                }
            }
            if !parts.is_empty() {
                print!("  [{}]", parts.join("  "));
            }
        }
        println!();
    }

    println!("\nrecent volume (newest first):");
    for p in detail.stats.iter().take(10) {
        println!(
            "  {}  {:>10} row(s)  run {}",
            p.recorded_at.format("%Y-%m-%dT%H:%M:%SZ"),
            p.records,
            p.run_id
        );
    }

    println!("\nupstream:");
    if detail.upstream.is_empty() {
        println!("  (none)");
    }
    for e in &detail.upstream {
        println!(
            "  {}  ({} run(s), pipeline {})",
            e.src_uri, e.runs, e.pipeline
        );
    }
    println!("downstream:");
    if detail.downstream.is_empty() {
        println!("  (none)");
    }
    for e in &detail.downstream {
        println!(
            "  {}  ({} run(s), pipeline {})",
            e.dst_uri, e.runs, e.pipeline
        );
    }
    Ok(())
}

async fn lineage(args: CatalogLineageArgs) -> CliResult<()> {
    let handle = connect(&args.common).await?;
    let edges = handle
        .store
        .catalog_lineage(args.root.as_deref(), args.depth.max(1))
        .await
        .map_err(|e| CliError::Internal(format!("catalog read: {e}")))?;
    if args.common.json {
        println!("{}", to_pretty(&serde_json::json!({ "edges": edges }))?);
        return Ok(());
    }
    if edges.is_empty() {
        println!(
            "no lineage edges recorded{}",
            match &args.root {
                Some(r) => format!(" around '{r}'"),
                None => String::new(),
            }
        );
        return Ok(());
    }
    print!("{}", render_edges(&edges));
    Ok(())
}

/// Human rendering of the edge list: `src → dst` grouped lines.
fn render_edges(edges: &[CatalogLineageEdge]) -> String {
    let mut out = String::new();
    for e in edges {
        out.push_str(&format!(
            "{}  →  {}\n    pipeline {} (row {}), {} run(s), {} row(s) last, last seen {}{}\n",
            e.src_uri,
            e.dst_uri,
            e.pipeline,
            e.row,
            e.runs,
            e.last_records,
            e.last_seen.format("%Y-%m-%dT%H:%M:%SZ"),
            if e.column_lineage.is_some() {
                ", column lineage recorded"
            } else {
                ""
            }
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serve::history::catalog::{
        CatalogUpdate, DatasetObservation, DatasetRole, apply_edge,
    };

    #[test]
    fn render_edges_lists_each_edge_with_context() {
        let update = CatalogUpdate {
            run_id: "r9".into(),
            pipeline: "p".into(),
            row: "default".into(),
            recorded_at: chrono::Utc::now(),
            sources: vec![DatasetObservation {
                uri: "csv://./in.csv".into(),
                kind: "csv".into(),
                role: DatasetRole::Source,
                schema: None,
                records: 4,
            }],
            sink: DatasetObservation {
                uri: "jsonl://./out.jsonl".into(),
                kind: "jsonl".into(),
                role: DatasetRole::Sink,
                schema: None,
                records: 4,
            },
            column_lineage: Some(serde_json::json!({"fields": {}})),
        };
        let edge = apply_edge(None, &update, &update.sources[0]);
        let text = render_edges(&[edge]);
        assert!(
            text.contains("csv://./in.csv  →  jsonl://./out.jsonl"),
            "{text}"
        );
        assert!(
            text.contains("pipeline p (row default), 1 run(s)"),
            "{text}"
        );
        assert!(text.contains("column lineage recorded"), "{text}");
    }
}
