//! `faucet init` — scaffold a starter `pipeline.yaml` from each connector's
//! JSON Schema. Defaults to a `rest` → `file` pipeline.

use std::collections::HashMap;

use crate::cli::InitArgs;
use crate::error::{CliError, CliResult};
#[cfg(feature = "cli-interactive")]
use crate::init_template::discover_tagged_enum_fields;
use crate::init_template::schema_to_yaml_template_with_choices;
use crate::registry;

const DEFAULT_SOURCE: &str = "rest";
const DEFAULT_SINK: &str = "file";
const DEFAULT_NAME: &str = "my-pipeline";
const CONFIG_INDENT: usize = 8;

/// Execute the `init` subcommand.
pub async fn run(args: InitArgs) -> CliResult<()> {
    if args.output.exists() && !args.force {
        return Err(CliError::ScaffoldExists {
            path: args.output.clone(),
        });
    }

    if args.discover {
        return run_singer_discover(&args).await;
    }

    let (source_kind, sink_kind) = resolve_kinds(&args)?;
    let name = args.name.as_deref().unwrap_or(DEFAULT_NAME);
    let template = &args.template;

    let source_schema = registry::source_schema(&source_kind)?;
    let sink_schema = registry::sink_schema(&sink_kind)?;
    let (source_choices, sink_choices) = if args.interactive {
        interactive_variant_choices(&source_schema, &sink_schema)?
    } else {
        (HashMap::new(), HashMap::new())
    };

    let body = render_pipeline(
        name,
        template,
        &source_kind,
        &source_schema,
        &source_choices,
        &sink_kind,
        &sink_schema,
        &sink_choices,
    );
    std::fs::write(&args.output, body)?;
    println!("wrote {}", args.output.display());
    Ok(())
}

/// Where `--discover` writes the Singer catalog: `catalog.json` next to the
/// output config.
#[cfg_attr(not(feature = "source-singer"), allow(dead_code))]
fn catalog_path_for(output: &std::path::Path) -> std::path::PathBuf {
    match output.parent() {
        Some(dir) if !dir.as_os_str().is_empty() => dir.join("catalog.json"),
        _ => std::path::PathBuf::from("catalog.json"),
    }
}

/// `faucet init --source singer --discover --executable <tap>`: run the tap's
/// discovery, write the catalog next to the output, and scaffold a config that
/// references it and lists the discovered streams.
#[cfg(feature = "source-singer")]
async fn run_singer_discover(args: &InitArgs) -> CliResult<()> {
    let source_kind = args.source.as_deref().unwrap_or("singer");
    if source_kind != "singer" {
        return Err(CliError::Config(format!(
            "`--discover` is only supported for `--source singer` (got `{source_kind}`)"
        )));
    }
    let executable = args.executable.as_deref().ok_or_else(|| {
        CliError::Config("`--discover` requires `--executable <tap>`".to_string())
    })?;

    // A hand-curated catalog is never replaced without --force.
    let catalog_path = catalog_path_for(&args.output);
    if catalog_path.exists() && !args.force {
        return Err(CliError::ScaffoldExists { path: catalog_path });
    }

    let cfg = faucet_source_singer::SingerSourceConfig::new(executable, "");
    let raw_catalog = faucet_source_singer::discover(&cfg).await?; // FaucetError -> CliError
    let streams = faucet_source_singer::catalog_stream_ids(&raw_catalog);

    // When a target stream is given, mark it (and any inferable parent streams)
    // `selected` — most DB / SDK taps sync nothing from an unselected catalog.
    let target = args.stream.as_deref().unwrap_or("");
    let (catalog, selected, warnings) = if target.is_empty() {
        (raw_catalog, Vec::new(), Vec::new())
    } else {
        let sel = faucet_source_singer::select_streams(&raw_catalog, target);
        (sel.catalog, sel.selected, sel.warnings)
    };

    let catalog_json = serde_json::to_string_pretty(&catalog)
        .map_err(|e| CliError::Config(format!("failed to serialize catalog: {e}")))?;
    std::fs::write(&catalog_path, catalog_json)?;

    // Inline the catalog as compact JSON (YAML is a JSON superset, so a
    // single-line flow mapping parses correctly — unlike `${file:…}`, which
    // would insert the file's contents as a *string*, not an object).
    let catalog_inline = serde_json::to_string(&catalog)
        .map_err(|e| CliError::Config(format!("failed to serialize catalog: {e}")))?;
    let name = args.name.as_deref().unwrap_or(DEFAULT_NAME);
    let body = render_singer_config(name, executable, &catalog_inline, &streams, target);
    std::fs::write(&args.output, body)?;

    println!(
        "discovered {} stream(s): {}",
        streams.len(),
        if streams.is_empty() {
            "(none)".to_string()
        } else {
            streams.join(", ")
        }
    );
    if !selected.is_empty() {
        println!(
            "selected {} stream(s): {}",
            selected.len(),
            selected.join(", ")
        );
    }
    for w in &warnings {
        eprintln!("warning: {w}");
    }
    println!(
        "wrote {} and {}",
        catalog_path.display(),
        args.output.display()
    );
    Ok(())
}

#[cfg(not(feature = "source-singer"))]
async fn run_singer_discover(_args: &InitArgs) -> CliResult<()> {
    Err(CliError::Config(
        "`--discover` requires the `source-singer` build feature".to_string(),
    ))
}

/// Render a Singer scaffold that inlines the discovered catalog and lists the
/// streams. `stream:` is left empty on purpose (the user must pick one; leaving
/// it empty is flagged by `faucet doctor`). The same catalog is also written to
/// `catalog.json` for reference / use as the tap's `--catalog`.
#[cfg(feature = "source-singer")]
fn render_singer_config(
    name: &str,
    executable: &str,
    catalog_inline: &str,
    streams: &[String],
    stream: &str,
) -> String {
    let discovered = if streams.is_empty() {
        "(none discovered)".to_string()
    } else {
        streams.join(", ")
    };
    format!(
        "version: 1\n\
         name: {name}\n\
         pipeline:\n\
         \x20 source:\n\
         \x20   type: singer\n\
         \x20   config:\n\
         \x20     executable: {executable}\n\
         \x20     # Discovered catalog, inlined as compact JSON (also saved to catalog.json).\n\
         \x20     # With --stream, the target stream (and any parents) are marked selected.\n\
         \x20     catalog: {catalog_inline}\n\
         \x20     # stream is REQUIRED. Discovered streams: {discovered}\n\
         \x20     # Set it to one of the above; leaving it empty fails `faucet doctor`.\n\
         \x20     stream: \"{stream}\"\n\
         \x20     # The tap's own config (secret-resolved by faucet). Fill in as the tap needs:\n\
         \x20     tap_config: {{}}\n\
         \x20 sink:\n\
         \x20   type: file\n\
         \x20   config:\n\
         \x20     path: ./out/records.jsonl\n"
    )
}

fn resolve_kinds(args: &InitArgs) -> CliResult<(String, String)> {
    let source = args.source.clone();
    let sink = args.sink.clone();

    let (source, sink) = if args.interactive {
        interactive_prompt(source, sink)?
    } else {
        (
            source.unwrap_or_else(|| DEFAULT_SOURCE.to_string()),
            sink.unwrap_or_else(|| DEFAULT_SINK.to_string()),
        )
    };

    if !registry::source_exists(&source) {
        return Err(unknown_kind_err("source", &source));
    }
    if !registry::sink_exists(&sink) {
        return Err(unknown_kind_err("sink", &sink));
    }
    Ok((source, sink))
}

#[cfg(feature = "cli-interactive")]
fn interactive_prompt(source: Option<String>, sink: Option<String>) -> CliResult<(String, String)> {
    use std::io::IsTerminal;
    if !std::io::stdin().is_terminal() {
        return fallback_kinds(source, sink, "stdin is not a TTY");
    }
    let sources = registry::source_kinds();
    let sinks = registry::sink_kinds();
    let s = if let Some(s) = source {
        s
    } else {
        prompt_select("source", &sources)?
    };
    let k = if let Some(k) = sink {
        k
    } else {
        prompt_select("sink", &sinks)?
    };
    Ok((s, k))
}

#[cfg(not(feature = "cli-interactive"))]
fn interactive_prompt(source: Option<String>, sink: Option<String>) -> CliResult<(String, String)> {
    fallback_kinds(
        source,
        sink,
        "the `cli-interactive` build feature is not enabled",
    )
}

fn fallback_kinds(
    source: Option<String>,
    sink: Option<String>,
    reason: &str,
) -> CliResult<(String, String)> {
    match (source, sink) {
        (Some(s), Some(k)) => Ok((s, k)),
        (s, k) => {
            tracing::warn!(
                "ignoring --interactive: {reason}; falling back to --source/--sink (or defaults)"
            );
            Ok((
                s.unwrap_or_else(|| DEFAULT_SOURCE.to_string()),
                k.unwrap_or_else(|| DEFAULT_SINK.to_string()),
            ))
        }
    }
}

#[cfg(feature = "cli-interactive")]
fn prompt_select(kind: &str, options: &[&'static str]) -> CliResult<String> {
    let choice = inquire::Select::new(&format!("Pick a {kind} connector"), options.to_vec())
        .prompt()
        .map_err(|e| CliError::Io(std::io::Error::other(e.to_string())))?;
    Ok(choice.to_string())
}

#[cfg(feature = "cli-interactive")]
fn interactive_variant_choices(
    source_schema: &serde_json::Value,
    sink_schema: &serde_json::Value,
) -> CliResult<(HashMap<String, String>, HashMap<String, String>)> {
    use std::io::IsTerminal;
    if !std::io::stdin().is_terminal() {
        return Ok((HashMap::new(), HashMap::new()));
    }
    let source = prompt_variants_for("source", source_schema)?;
    let sink = prompt_variants_for("sink", sink_schema)?;
    Ok((source, sink))
}

#[cfg(not(feature = "cli-interactive"))]
fn interactive_variant_choices(
    _source_schema: &serde_json::Value,
    _sink_schema: &serde_json::Value,
) -> CliResult<(HashMap<String, String>, HashMap<String, String>)> {
    tracing::warn!("ignoring --interactive: the `cli-interactive` build feature is not enabled");
    Ok((HashMap::new(), HashMap::new()))
}

#[cfg(feature = "cli-interactive")]
fn prompt_variants_for(
    side: &str,
    schema: &serde_json::Value,
) -> CliResult<HashMap<String, String>> {
    let fields = discover_tagged_enum_fields(schema);
    let mut choices = HashMap::new();
    for field in fields {
        let label = format!("Pick a variant for {side}.{}", field.path);
        let opts: Vec<String> = field.variants.clone();
        let chosen = inquire::Select::new(&label, opts)
            .prompt()
            .map_err(|e| CliError::Io(std::io::Error::other(e.to_string())))?;
        choices.insert(field.path, chosen);
    }
    Ok(choices)
}

fn unknown_kind_err(kind: &'static str, name: &str) -> CliError {
    let available = if kind == "source" {
        registry::source_kinds()
    } else {
        registry::sink_kinds()
    };
    CliError::UnknownConnector {
        kind,
        name: name.to_owned(),
        available: if available.is_empty() {
            "(none — rebuild faucet-cli with the relevant feature enabled)".to_owned()
        } else {
            available.join(", ")
        },
    }
}

#[allow(clippy::too_many_arguments)]
fn render_pipeline(
    name: &str,
    template: &str,
    source_kind: &str,
    source_schema: &serde_json::Value,
    source_choices: &HashMap<String, String>,
    sink_kind: &str,
    sink_schema: &serde_json::Value,
    sink_choices: &HashMap<String, String>,
) -> String {
    let source_yaml =
        schema_to_yaml_template_with_choices(source_schema, CONFIG_INDENT, source_choices);
    let sink_yaml = schema_to_yaml_template_with_choices(sink_schema, CONFIG_INDENT, sink_choices);

    let mut body = String::new();
    body.push_str("version: 1\n");
    body.push_str(&format!("name: {name}\n\n"));
    body.push_str("# Optional shared constants. Reference these anywhere via ${vars.KEY}.\n");
    body.push_str("# vars:\n");
    body.push_str("#   api_base: https://api.example.com\n\n");
    body.push_str("# Named source/sink templates. Matrix rows pick from these via ref:.\n");
    body.push_str("# A matrix row that omits ref: inherits the `default` template,\n");
    body.push_str("# which keeps backwards-compat with the legacy singular shape.\n");
    body.push_str("pipeline:\n");
    body.push_str("  sources:\n");
    body.push_str(&format!("    {template}:\n"));
    body.push_str(&format!("      type: {source_kind}\n"));
    body.push_str("      config:\n");
    body.push_str(&source_yaml);
    body.push('\n');
    body.push_str("  # transforms:\n");
    body.push_str("  #   - { type: keys_case, config: { mode: snake } }\n\n");
    body.push_str("  sinks:\n");
    body.push_str(&format!("    {template}:\n"));
    body.push_str(&format!("      type: {sink_kind}\n"));
    body.push_str("      config:\n");
    body.push_str(&sink_yaml);
    body.push('\n');
    body.push_str("  # Optional state store (required by CDC sources and resumable runs).\n");
    body.push_str("  # state:\n");
    body.push_str("  #   type: file\n");
    body.push_str("  #   config: { path: ./.faucet-state }\n\n");
    body.push_str("  # Optional Dead Letter Queue.\n");
    body.push_str("  # dlq:\n");
    body.push_str("  #   sink:\n");
    body.push_str("  #     type: file\n");
    body.push_str("  #     config: { path: ./dlq.jsonl }\n");
    body.push_str("  #   on_batch_error: propagate   # or dlq_all\n\n");
    body.push_str("# Optional matrix block. Each row picks a template via ref:\n");
    body.push_str("# (omit ref: to inherit the `default` template above) and may\n");
    body.push_str("# override `type:` / `config:` per row.\n");
    if template != "default" {
        // No `default` template exists, so a row must name this one.
        body.push_str("matrix:\n");
        body.push_str(&format!("  - id: {template}\n"));
        body.push_str(&format!("    source: {{ ref: {template} }}\n"));
        body.push_str(&format!("    sink: {{ ref: {template} }}\n"));
    }
    body.push_str("# matrix:\n");
    body.push_str("#   - id: users\n");
    body.push_str(&format!(
        "#     source: {{ ref: {template}, config: {{ path: /v1/users }} }}\n"
    ));
    body
}

#[cfg(test)]
mod tests {
    use super::{DEFAULT_SINK, DEFAULT_SOURCE, resolve_kinds};
    use crate::cli::InitArgs;
    use crate::error::CliError;
    use std::path::PathBuf;

    fn args(source: Option<&str>, sink: Option<&str>) -> InitArgs {
        InitArgs {
            name: None,
            source: source.map(String::from),
            sink: sink.map(String::from),
            output: PathBuf::from("pipeline.yaml"),
            force: false,
            interactive: false,
            template: "default".into(),
            discover: false,
            executable: None,
            stream: None,
        }
    }

    /// A named template scaffolds a config that validates: the matrix row
    /// that picks it is emitted, since no `default` template exists
    /// (#789 CLI-159).
    #[tokio::test]
    async fn a_named_template_scaffold_expands() {
        let dir = tempfile::tempdir().unwrap();
        let mut a = args(Some("csv"), Some("jsonl"));
        a.output = dir.path().join("p.yaml");
        a.template = "warehouse".into();
        super::run(a).await.unwrap();
        let cfg = crate::config::PipelineConfig::from_path_tolerating_secrets(
            dir.path().join("p.yaml"),
            None,
        )
        .unwrap();
        let nodes = crate::expand::expand(&cfg).expect("a fresh scaffold expands");
        assert_eq!(nodes[0].id, "warehouse");
    }

    /// `--discover` never overwrites an existing catalog without --force
    /// (#789 CLI-158).
    #[cfg(feature = "source-singer")]
    #[tokio::test]
    async fn discover_refuses_to_overwrite_a_catalog() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("catalog.json"), "{}").unwrap();
        let mut a = args(Some("singer"), None);
        a.output = dir.path().join("p.yaml");
        a.discover = true;
        a.executable = Some("/nonexistent/tap".into());
        let err = super::run(a).await.unwrap_err();
        assert!(matches!(err, CliError::ScaffoldExists { .. }), "{err}");
        assert_eq!(
            std::fs::read_to_string(dir.path().join("catalog.json")).unwrap(),
            "{}"
        );
    }

    #[test]
    fn resolve_kinds_passes_valid_kinds_through() {
        let (src, sink) = resolve_kinds(&args(Some("rest"), Some("jsonl"))).unwrap();
        assert_eq!((src.as_str(), sink.as_str()), ("rest", "jsonl"));
    }

    #[test]
    fn resolve_kinds_falls_back_to_defaults() {
        let (src, sink) = resolve_kinds(&args(None, None)).unwrap();
        assert_eq!(
            (src.as_str(), sink.as_str()),
            (DEFAULT_SOURCE, DEFAULT_SINK)
        );
    }

    #[test]
    fn resolve_kinds_rejects_unknown_source() {
        let err = resolve_kinds(&args(Some("does-not-exist"), Some("jsonl"))).unwrap_err();
        assert!(
            matches!(err, CliError::UnknownConnector { kind: "source", ref name, .. } if name == "does-not-exist"),
            "got: {err:?}"
        );
    }

    #[test]
    fn resolve_kinds_rejects_unknown_sink() {
        let err = resolve_kinds(&args(Some("rest"), Some("does-not-exist"))).unwrap_err();
        assert!(
            matches!(err, CliError::UnknownConnector { kind: "sink", ref name, .. } if name == "does-not-exist"),
            "got: {err:?}"
        );
    }
}
