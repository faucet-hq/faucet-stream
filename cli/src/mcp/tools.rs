//! MCP tool definitions + in-process dispatch (issue #420).
//!
//! Every tool is a thin shape-adapter over an existing faucet capability —
//! `registry` (list / schema), `init_template` (scaffold), `expand` /
//! `topology` (validate), `preview`, and `run_from_yaml_str` (the one gated
//! mutating tool). No pipeline logic is re-implemented here.

use super::McpContext;
use crate::config::PipelineConfig;
use crate::mcp::protocol::{ToolDef, tool_error, tool_text};
use serde_json::{Value, json};

/// Hard cap on `preview` rows regardless of the requested limit — an MCP
/// client must never trigger a full extract of a large source.
const PREVIEW_MAX: usize = 100;

/// Build the advertised tool list for `tools/list`, honoring the mutation gate.
pub fn tool_defs(ctx: &McpContext) -> Vec<ToolDef> {
    let mut defs = vec![
        ToolDef {
            name: "list_connectors",
            description: "List all compiled-in sources, sinks, transforms, and state stores, each with a one-line description and (for connectors) a conformance tier.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "kind": { "type": "string", "enum": ["source", "sink", "transform", "state", "all"], "description": "Filter to one category (default: all)." }
                }
            }),
        },
        ToolDef {
            name: "get_connector_schema",
            description: "Return the JSON Schema for a connector or transform's config block.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "kind": { "type": "string", "enum": ["source", "sink", "transform"] },
                    "name": { "type": "string", "description": "Connector/transform name, e.g. 'rest' or 'keys_case'." }
                },
                "required": ["kind", "name"]
            }),
        },
        ToolDef {
            name: "scaffold_config",
            description: "Generate a commented YAML pipeline skeleton for a source→sink pair (read-only: returns text, writes nothing).",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "source": { "type": "string", "description": "Source connector kind." },
                    "sink": { "type": "string", "description": "Sink connector kind." },
                    "name": { "type": "string", "description": "Optional pipeline name." }
                },
                "required": ["source", "sink"]
            }),
        },
    ];
    // `validate_config` and `preview` act on a caller-supplied config: they
    // resolve `${env:}`/`${file:}`/`${secret:}` server-side and (for `preview`)
    // build the described connector and return its records. That is a strictly
    // higher capability than schema introspection, so it is separately gated
    // (#456 C4) and not advertised when the caller may not use it.
    if ctx.allow_config_execution {
        defs.push(ToolDef {
            name: "validate_config",
            description: "Fully validate a pipeline YAML/JSON config (structure, templates, matrix/topology graph). Returns a per-node report or the validation error.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "config": { "type": "string", "description": "The pipeline config document (YAML or JSON)." }
                },
                "required": ["config"]
            }),
        });
        defs.push(ToolDef {
            name: "preview",
            description: "Fetch a bounded sample of records from a config's first source (source side only; downstream sinks are not run). Capped at 100 rows.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "config": { "type": "string", "description": "The pipeline config document (YAML or JSON)." },
                    "limit": { "type": "integer", "description": "Max rows to return (1–100, default 10)." }
                },
                "required": ["config"]
            }),
        });
    }
    if ctx.allow_mutations {
        defs.push(ToolDef {
            name: "run_pipeline",
            description: "Run a pipeline from an inline config. MUTATING — gated behind --allow-mutations. Pass dry_run:true to validate+preview only.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "config": { "type": "string" },
                    "dry_run": { "type": "boolean", "description": "If true, validate + preview only; do not write to any sink." }
                },
                "required": ["config"]
            }),
        });
    }
    #[cfg(feature = "templates")]
    if ctx.templates.is_some() {
        defs.push(ToolDef {
            name: "list_templates",
            description: "List registered pipeline templates (newest version of each, plus its release status) with the typed params each one takes.",
            input_schema: json!({ "type": "object", "properties": {} }),
        });
        defs.push(ToolDef {
            name: "get_template",
            description: "Show one registered pipeline template: its declared params, stored config body, and available versions.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "id": { "type": "string", "description": "Template id." },
                    "version": { "description": "Version: a number, or a named channel. Derived: \"stable\" (the launched version — the default), \"previous\", \"newest\". Assignable: \"dev\", \"test\", \"staging\", \"pre-prod\", \"canary\", \"prod\". Note \"latest\" is deliberately not a channel — use \"stable\" for the current release or \"newest\" for the highest version number.", "oneOf": [{ "type": "integer" }, { "type": "string" }] }
                },
                "required": ["id"]
            }),
        });
        if ctx.allow_mutations && ctx.allow_template_admin {
            defs.push(ToolDef {
                name: "register_template",
                description: "Register a template document as a new version: `kind: source-template` (a system + its streams), `kind: sink-template` (a destination), or `kind: pipeline` (a complete config). Params are declared with `params:`. MUTATING — gated behind --allow-mutations.",
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "config": { "type": "string", "description": "The pipeline config document (YAML or JSON), stored verbatim." },
                        "id": { "type": "string", "description": "Template id. Derived from the config's `name:` when omitted." },
                        "description": { "type": "string" },
                        "tags": { "type": "array", "items": { "type": "string" }, "description": "Named environment channels to point at the new version (dev/test/staging/pre-prod/canary/prod). Derived channels (stable/previous/newest) are rejected." },
                        "launch": { "type": "boolean", "description": "Make the new version live immediately. Off by default: a register is inert, so a new build never moves existing callers until it is launched." }
                    },
                    "required": ["config"]
                }),
            });
            defs.push(ToolDef {
                name: "launch_template",
                description: "Make a template version live — what unpinned runs will use. MUTATING. This is the only action that moves existing callers; registering a build does not.",
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "id": { "type": "string" },
                        "version": { "description": "Version to launch: a number, or a channel whose current target to copy. Defaults to \"newest\".", "oneOf": [{ "type": "integer" }, { "type": "string" }] }
                    },
                    "required": ["id"]
                }),
            });
            defs.push(ToolDef {
                name: "rollback_template",
                description: "Re-launch a template's previously launched version. MUTATING.",
                input_schema: json!({
                    "type": "object",
                    "properties": { "id": { "type": "string" } },
                    "required": ["id"]
                }),
            });
            defs.push(ToolDef {
                name: "deprecate_template",
                description: "Retire a template (or revive it with undo:true). MUTATING. A deprecated template keeps serving existing callers but every trigger warns.",
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "id": { "type": "string" },
                        "reason": { "type": "string" },
                        "undo": { "type": "boolean" }
                    },
                    "required": ["id"]
                }),
            });
        }
        if ctx.allow_mutations {
            defs.push(ToolDef {
                name: "run_template",
                description: "Run a registered template with the given params: a source-template composed with `sink` (a registered sink-template), or a complete `pipeline` template. MUTATING — gated behind --allow-mutations. Pass dry_run:true to materialize + validate only.",
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "id": { "type": "string" },
                        "version": { "description": "Version: a number, or a named channel. Derived: \"stable\" (the launched version — the default), \"previous\", \"newest\". Assignable: \"dev\", \"test\", \"staging\", \"pre-prod\", \"canary\", \"prod\". Note \"latest\" is deliberately not a channel — use \"stable\" for the current release or \"newest\" for the highest version number.", "oneOf": [{ "type": "integer" }, { "type": "string" }] },
                        "params": { "type": "object", "description": "Values for the template's declared params." },
                        "sink": { "type": "string", "description": "For a source-template: the registered sink-template to compose in (required for a source template; a `pipeline` template takes none)." },
                        "sink_version": { "description": "Version of the sink template: a number or a channel. Default \"stable\".", "oneOf": [{ "type": "integer" }, { "type": "string" }] },
                        "overlay": { "description": "Deployment overlay for a composed run: a registered `kind: deployment` id, or an inline mapping of operational blocks (state, dlq, notifications, sla, resilience, execution, delivery, schedule, streams).", "oneOf": [{ "type": "string" }, { "type": "object" }] },
                        "overlay_version": { "description": "Version of a registered overlay: a number or a channel. Default \"stable\".", "oneOf": [{ "type": "integer" }, { "type": "string" }] },
                        "env": { "type": "object", "description": "Per-run overrides for ${env:VAR} resolution." },
                        "dry_run": { "type": "boolean", "description": "If true, materialize + validate only; do not write to any sink." }
                    },
                    "required": ["id"]
                }),
            });
        }
    }
    if ctx.changes.is_some() {
        defs.push(ToolDef {
            name: "propose_run",
            description: "Propose a pipeline run as a change request (#703) instead of running it: the server plans it (resolved rows, delivery guarantee, policy verdict, impact) and stores it pending; an approver runs it. Use this when the server requires approval, or whenever a human should review first. Returns the pending request (id, plan summary, approvals needed).",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "config": { "type": "string", "description": "The pipeline config (YAML or JSON)." },
                    "config_format": { "type": "string", "enum": ["yaml", "json"], "description": "Default yaml." },
                    "name": { "type": "string", "description": "Run name." },
                    "reason": { "type": "string", "description": "Why — shown to the approvers." },
                    "budget": { "type": "object", "description": "Ceilings the approved run must honour: max_records, max_bytes, max_duration_secs, allowed_sinks.", "properties": {
                        "max_records": { "type": "integer" }, "max_bytes": { "type": "integer" },
                        "max_duration_secs": { "type": "integer" }, "allowed_sinks": { "type": "array", "items": { "type": "string" } } } },
                    "labels": { "type": "object", "additionalProperties": { "type": "string" } },
                    "timeout_secs": { "type": "integer" },
                    "clock": { "type": "string", "description": "RFC 3339 / YYYY-MM-DD run clock for ${now.*}." }
                },
                "required": ["config", "reason"]
            }),
        });
        defs.push(ToolDef {
            name: "propose_template",
            description: "Propose a template change as a change request (#703): `action: register` files a new template version (the document is validated and planned, nothing is stored until approved); `action: launch` proposes making a version live. An approver executes it. Returns the pending request.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "action": { "type": "string", "enum": ["register", "launch"], "description": "Default register." },
                    "config": { "type": "string", "description": "register: the template document (YAML or JSON)." },
                    "id": { "type": "string", "description": "register: explicit template id (derived from name: when omitted). launch: the template id (required)." },
                    "description": { "type": "string", "description": "register: template description." },
                    "launch": { "type": "boolean", "description": "register: also launch the new version once registered." },
                    "version": { "description": "launch: the version to make live (number or channel; default newest).", "oneOf": [{ "type": "integer" }, { "type": "string" }] },
                    "reason": { "type": "string", "description": "Why — shown to the approvers." }
                },
                "required": ["reason"]
            }),
        });
    }
    defs
}

/// Dispatch a `tools/call`. Returns the MCP `tools/call` result envelope
/// (`content` + `isError`). A tool-level failure is `tool_error(..)`, not a
/// JSON-RPC protocol error.
pub async fn call_tool(ctx: &McpContext, name: &str, args: &Value) -> Value {
    let result: Result<String, String> = match name {
        "list_connectors" => list_connectors(args),
        "get_connector_schema" => get_connector_schema(args),
        "scaffold_config" => scaffold_config(args),
        // Gated at call time as well as in the advertised list: an agent can
        // always name a tool it was never offered.
        "validate_config" => {
            if !ctx.allow_config_execution {
                Err(CONFIG_EXEC_GATE.to_string())
            } else {
                validate_config(ctx, args).await
            }
        }
        "preview" => {
            if !ctx.allow_config_execution {
                Err(CONFIG_EXEC_GATE.to_string())
            } else {
                preview(ctx, args).await
            }
        }
        "run_pipeline" => {
            if !ctx.allow_mutations {
                Err("run_pipeline is disabled; start the MCP server with --allow-mutations to enable mutating tools".to_string())
            } else if ctx.changes.as_ref().is_some_and(|c| {
                c.state
                    .requires_approval(crate::serve::changes::ChangeKind::Run)
            }) {
                Err("this server requires an approved change request for runs (--require-approval run); use propose_run instead".to_string())
            } else {
                run_pipeline(ctx, args).await
            }
        }
        "propose_run" => match &ctx.changes {
            Some(p) => propose_run(p, args).await,
            None => Err(
                "propose_run is only available on a server transport (faucet serve --mcp)"
                    .to_string(),
            ),
        },
        "propose_template" => match &ctx.changes {
            Some(p) => propose_template(p, args).await,
            None => Err(
                "propose_template is only available on a server transport (faucet serve --mcp)"
                    .to_string(),
            ),
        },
        #[cfg(feature = "templates")]
        "list_templates" => list_templates(ctx).await,
        #[cfg(feature = "templates")]
        "get_template" => get_template(ctx, args).await,
        #[cfg(feature = "templates")]
        "register_template" => {
            if !ctx.allow_mutations {
                Err(MUTATION_GATE.to_string())
            } else if !ctx.allow_template_admin {
                Err(TEMPLATE_ADMIN_GATE.to_string())
            } else {
                register_template(ctx, args).await
            }
        }
        #[cfg(feature = "templates")]
        "launch_template" => {
            if !ctx.allow_mutations {
                Err(MUTATION_GATE.to_string())
            } else if !ctx.allow_template_admin {
                Err(TEMPLATE_ADMIN_GATE.to_string())
            } else {
                launch_template(ctx, args).await
            }
        }
        #[cfg(feature = "templates")]
        "rollback_template" => {
            if !ctx.allow_mutations {
                Err(MUTATION_GATE.to_string())
            } else if !ctx.allow_template_admin {
                Err(TEMPLATE_ADMIN_GATE.to_string())
            } else {
                rollback_template(ctx, args).await
            }
        }
        #[cfg(feature = "templates")]
        "deprecate_template" => {
            if !ctx.allow_mutations {
                Err(MUTATION_GATE.to_string())
            } else if !ctx.allow_template_admin {
                Err(TEMPLATE_ADMIN_GATE.to_string())
            } else {
                deprecate_template(ctx, args).await
            }
        }
        #[cfg(feature = "templates")]
        "run_template" => {
            if !ctx.allow_mutations {
                Err(MUTATION_GATE.to_string())
            } else {
                run_template(ctx, args).await
            }
        }
        other => Err(format!("unknown tool '{other}'")),
    };
    match result {
        Ok(text) => tool_text(text),
        // Redact any resolved secret material that reached an error string.
        Err(msg) => tool_error(crate::secrets::registry::redact(&msg)),
    }
}

/// The MCP-facing rendering of a change request: everything an agent needs
/// to tell the human what to approve, without the plan rows' bulk.
fn render_change(change: &crate::serve::changes::ChangeRequest) -> Result<String, String> {
    let plan = change.plan.as_ref().map(|p| &p.summary);
    let value = json!({
        "change_id": change.id,
        "kind": change.kind,
        "status": change.status,
        "requester": change.requester,
        "reason": change.reason,
        "required_approvals": change.required_approvals,
        "approvals": change.approvals.len(),
        "expires_at": change.expires_at,
        "plan": plan,
        "budget": change.budget,
        "next": format!(
            "an approver runs it with POST /v1/changes/{}/approve (or the console's Changes page)",
            change.id
        ),
    });
    serde_json::to_string_pretty(&value).map_err(|e| e.to_string())
}

async fn propose_run(p: &crate::mcp::ChangeProposer, args: &Value) -> Result<String, String> {
    let config = str_arg(args, "config")?;
    let reason = str_arg(args, "reason")?;
    let mut payload = json!({
        "config": config,
        "config_format": args.get("config_format").cloned().unwrap_or(json!("yaml")),
    });
    for key in ["name", "labels", "timeout_secs", "clock"] {
        if let Some(v) = args.get(key).filter(|v| !v.is_null()) {
            payload[key] = v.clone();
        }
    }
    let budget = match args.get("budget").filter(|v| !v.is_null()) {
        Some(v) => Some(serde_json::from_value(v.clone()).map_err(|e| format!("budget: {e}"))?),
        None => None,
    };
    let change = crate::serve::changes::create(
        &p.state,
        &p.actor,
        crate::serve::changes::NewChange {
            kind: crate::serve::changes::ChangeKind::Run,
            payload,
            reason: Some(reason.to_string()),
            budget,
        },
    )
    .await
    .map_err(|e| e.to_string())?;
    render_change(&change)
}

async fn propose_template(p: &crate::mcp::ChangeProposer, args: &Value) -> Result<String, String> {
    let reason = str_arg(args, "reason")?;
    let action = args
        .get("action")
        .and_then(Value::as_str)
        .unwrap_or("register");
    let (kind, payload) = match action {
        "register" => {
            let config = str_arg(args, "config")?;
            let mut payload = json!({ "config": config });
            for key in ["id", "description", "launch"] {
                if let Some(v) = args.get(key).filter(|v| !v.is_null()) {
                    payload[key] = v.clone();
                }
            }
            (crate::serve::changes::ChangeKind::TemplateRegister, payload)
        }
        "launch" => {
            let id = str_arg(args, "id")?;
            let mut payload = json!({ "id": id });
            if let Some(v) = args.get("version").filter(|v| !v.is_null()) {
                payload["version"] = v.clone();
            }
            (crate::serve::changes::ChangeKind::TemplateLaunch, payload)
        }
        other => {
            return Err(format!(
                "unknown action `{other}` (expected register or launch)"
            ));
        }
    };
    let change = crate::serve::changes::create(
        &p.state,
        &p.actor,
        crate::serve::changes::NewChange {
            kind,
            payload,
            reason: Some(reason.to_string()),
            budget: None,
        },
    )
    .await
    .map_err(|e| e.to_string())?;
    render_change(&change)
}

fn str_arg<'a>(args: &'a Value, key: &str) -> Result<&'a str, String> {
    args.get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("missing required string argument '{key}'"))
}

fn tier_of(kind: &str, is_source: bool) -> &'static str {
    crate::conformance::tier_for(kind, is_source).as_str()
}

fn list_connectors(args: &Value) -> Result<String, String> {
    let filter = args.get("kind").and_then(Value::as_str).unwrap_or("all");
    let want = |c: &str| filter == "all" || filter == c;

    let mut out = json!({});
    let obj = out.as_object_mut().unwrap();
    if want("source") {
        let sources: Vec<Value> = crate::registry::source_descriptions()
            .into_iter()
            .map(|(name, desc)| json!({ "name": name, "description": desc, "tier": tier_of(name, true) }))
            .collect();
        obj.insert("sources".into(), json!(sources));
    }
    if want("sink") {
        let sinks: Vec<Value> = crate::registry::sink_descriptions()
            .into_iter()
            .map(|(name, desc)| json!({ "name": name, "description": desc, "tier": tier_of(name, false) }))
            .collect();
        obj.insert("sinks".into(), json!(sinks));
    }
    if want("transform") {
        let transforms: Vec<Value> = crate::transforms::transform_descriptions()
            .into_iter()
            .map(|(name, desc)| json!({ "name": name, "description": desc }))
            .collect();
        obj.insert("transforms".into(), json!(transforms));
    }
    if want("state") {
        obj.insert(
            "state_stores".into(),
            json!(crate::state::available_state_kinds()),
        );
    }
    Ok(pretty(&out))
}

fn get_connector_schema(args: &Value) -> Result<String, String> {
    let kind = str_arg(args, "kind")?;
    let name = str_arg(args, "name")?;
    let schema = match kind {
        "source" => crate::registry::source_schema(name),
        "sink" => crate::registry::sink_schema(name),
        "transform" => crate::transforms::transform_schema(name),
        other => return Err(format!("kind must be source|sink|transform, got '{other}'")),
    }
    .map_err(|e| e.to_string())?;
    Ok(pretty(&schema))
}

fn scaffold_config(args: &Value) -> Result<String, String> {
    let source = str_arg(args, "source")?;
    let sink = str_arg(args, "sink")?;
    let name = args
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or("pipeline");

    let src_schema = crate::registry::source_schema(source).map_err(|e| e.to_string())?;
    let sink_schema = crate::registry::sink_schema(sink).map_err(|e| e.to_string())?;
    let src_yaml = crate::init_template::schema_to_yaml_template(&src_schema, 6);
    let sink_yaml = crate::init_template::schema_to_yaml_template(&sink_schema, 6);

    Ok(format!(
        "version: 1\nname: {name}\npipeline:\n  source:\n    type: {source}\n    config:\n{src_yaml}\n  sink:\n    type: {sink}\n    config:\n{sink_yaml}"
    ))
}

/// Parse an inline config document, mirroring the file-load pipeline: resolve
/// `${env:}` / `${file:}` / `${secret:}` per scalar, bind `${param.*}`, then take
/// the typed path. YAML is a JSON superset, so one parser handles both wire
/// formats.
///
/// `mode` decides what an unsupplied `required` param means: `Placeholder` for
/// the read-only introspection tools (a parameterized config still validates),
/// `Strict` where the config is about to actually run.
fn parse_config_with(text: &str, mode: crate::params::BindMode) -> Result<PipelineConfig, String> {
    let mut doc: Value = serde_yaml::from_str(text).map_err(|e| e.to_string())?;
    crate::interpolate::interpolate_value(&mut doc).map_err(|e| e.to_string())?;
    crate::params::bind_document(&mut doc, &Default::default(), mode).map_err(|e| e.to_string())?;
    PipelineConfig::from_value(doc).map_err(|e| e.to_string())
}

/// Read-only introspection: a config whose required params arrive later still
/// validates, against type-shaped placeholders.
fn parse_config(text: &str) -> Result<PipelineConfig, String> {
    parse_config_with(text, crate::params::BindMode::Placeholder)
}

async fn validate_config(ctx: &McpContext, args: &Value) -> Result<String, String> {
    let text = str_arg(args, "config")?;
    let cfg = parse_config(text)?;

    if crate::topology::is_topology(&cfg) {
        let topo = crate::topology::build_topology(&cfg, &ctx.auth)
            .await
            .map_err(|e| e.to_string())?;
        return Ok(pretty(&json!({
            "valid": true,
            "mode": "topology",
            "nodes": topo.nodes().iter().map(|n| json!({"id": n.id, "kind": n.kind.kind_str()})).collect::<Vec<_>>(),
            "edges": topo.edges().len(),
        })));
    }

    let nodes = crate::expand::expand(&cfg).map_err(|e| e.to_string())?;
    let rows: Vec<Value> = nodes
        .iter()
        .map(|n| {
            json!({
                "id": n.id,
                "source": n.source.kind,
                "sink": n.sink.kind,
                "transforms": n.transforms.len(),
            })
        })
        .collect();
    Ok(pretty(&json!({
        "valid": true,
        "mode": "matrix",
        "name": cfg.name,
        "rows": rows,
    })))
}

async fn preview(ctx: &McpContext, args: &Value) -> Result<String, String> {
    use faucet_core::stage::{apply_stages, compile_stage};

    let text = str_arg(args, "config")?;
    let limit = args
        .get("limit")
        .and_then(Value::as_u64)
        .map(|n| (n as usize).clamp(1, PREVIEW_MAX))
        .unwrap_or(10);

    let cfg = parse_config(text)?;
    if crate::topology::is_topology(&cfg) {
        return crate::topology::preview_to_string(&cfg, &ctx.auth, limit)
            .await
            .map_err(|e| e.to_string());
    }

    let nodes = crate::expand::expand(&cfg).map_err(|e| e.to_string())?;
    let first_root = nodes
        .iter()
        .find(|n| matches!(n.role, crate::expand::NodeRole::Root))
        .ok_or_else(|| "no root row to preview".to_string())?;

    let source = crate::registry::build_source(
        &first_root.source.kind,
        first_root.source.config.clone(),
        &ctx.auth,
        None,
    )
    .await
    .map_err(|e| e.to_string())?;
    let stages =
        crate::transforms::compile_transforms(&first_root.transforms).map_err(|e| e.to_string())?;
    let records = source.fetch_all().await.map_err(|e| e.to_string())?;
    let records: Vec<Value> = if stages.is_empty() {
        records
    } else {
        let compiled = stages
            .iter()
            .map(compile_stage)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| e.to_string())?;
        let mut out = Vec::with_capacity(records.len());
        for r in records {
            out.extend(apply_stages(r, &compiled).map_err(|e| e.to_string())?);
        }
        out
    };
    let limited: Vec<Value> = records.into_iter().take(limit).collect();
    Ok(pretty(
        &json!({ "row": first_root.id, "count": limited.len(), "records": limited }),
    ))
}

async fn run_pipeline(ctx: &McpContext, args: &Value) -> Result<String, String> {
    let text = str_arg(args, "config")?;
    let dry_run = args
        .get("dry_run")
        .and_then(Value::as_bool)
        .unwrap_or(false);

    if dry_run {
        let mut report = validate_config(ctx, args).await?;
        report.push_str("\n\n-- preview --\n");
        report.push_str(
            &preview(ctx, args)
                .await
                .unwrap_or_else(|e| format!("preview skipped: {e}")),
        );
        return Ok(report);
    }

    let summary = crate::run_from_yaml_str(text)
        .await
        .map_err(|e| e.to_string())?;
    let failed = summary.failure_count();
    let total: usize = summary.invocations.iter().map(|i| i.records_written).sum();
    let doc = json!({
        "invocations": summary.invocations.len(),
        "ok": summary.invocations.len() - failed,
        "failed": failed,
        "records_written": total,
    });
    if failed > 0 {
        return Err(format!(
            "pipeline had {failed} failed invocation(s): {}",
            pretty(&doc)
        ));
    }
    Ok(pretty(&doc))
}

fn pretty(v: &Value) -> String {
    serde_json::to_string_pretty(v).unwrap_or_else(|_| v.to_string())
}

// ── Pipeline template tools (#444) ──────────────────────────────────────────

/// Shared refusal text for a mutating template tool on a read-only server.
#[cfg(feature = "templates")]
const MUTATION_GATE: &str =
    "this tool is disabled; start the MCP server with --allow-mutations to enable mutating tools";

/// Refusal text for the template-lifecycle tools when the caller is not an admin.
const TEMPLATE_ADMIN_GATE: &str = "managing templates (register, launch, roll back, deprecate) \
     is admin-only; an operator can run registered templates with run_template";

/// Refusal text for the config-executing tools when the caller lacks the scope.
/// Phrased for the HTTP transport, which is the only place the gate closes.
const CONFIG_EXEC_GATE: &str = "this tool acts on a config you supply — it resolves \
     ${env:}/${file:}/${secret:} on the server and builds the connectors you name — so it \
     requires the same scope as POST /v1/doctor (role `operator` or `admin`), not a read-only \
     token";

/// The wired template registry, or a tool error explaining how to wire one.
#[cfg(feature = "templates")]
fn template_store(ctx: &McpContext) -> Result<&crate::templates::TemplateStore, String> {
    ctx.templates.as_ref().ok_or_else(|| {
        "no pipeline-template registry is configured — start `faucet mcp --template-store \
         <url>`, or use the /mcp route of a `faucet serve --mcp` whose --history backend holds \
         the registry"
            .to_string()
    })
}

#[cfg(feature = "templates")]
async fn list_templates(ctx: &McpContext) -> Result<String, String> {
    let store = template_store(ctx)?;
    let templates = crate::templates::list_with_state(store)
        .await
        .map_err(|e| e.to_string())?;
    Ok(pretty(&json!({
        "count": templates.len(),
        "templates": templates,
    })))
}

/// Read the optional `version` argument: a number, or one of the closed set of
/// named channels. Absent = `stable` (the *launched* version), so an agent that
/// never mentions versions rides releases rather than picking up every new
/// registration.
#[cfg(feature = "templates")]
fn version_arg(args: &Value) -> Result<crate::serve::history::templates::VersionSelector, String> {
    use crate::serve::history::templates::VersionSelector;
    match args.get("version") {
        None | Some(Value::Null) => Ok(VersionSelector::default()),
        Some(v) => serde_json::from_value::<VersionSelector>(v.clone()).map_err(|e| e.to_string()),
    }
}

/// Resolve the `version` argument against the registry (every channel, derived or
/// assigned, needs a lookup — nothing falls back to "the newest build").
#[cfg(feature = "templates")]
async fn resolved_version_arg(
    store: &crate::templates::TemplateStore,
    id: &str,
    args: &Value,
) -> Result<u32, String> {
    crate::templates::resolve_version(store, id, version_arg(args)?)
        .await
        .map_err(|e| e.to_string())
}

#[cfg(feature = "templates")]
async fn get_template(ctx: &McpContext, args: &Value) -> Result<String, String> {
    let store = template_store(ctx)?;
    let id = str_arg(args, "id")?;
    let version = resolved_version_arg(store, id, args).await?;
    let record = store
        .template_get(id, Some(version))
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("no pipeline template '{id}'"))?;
    let launches = store
        .template_launches(id)
        .await
        .map_err(|e| e.to_string())?;
    let state = crate::templates::template_state(store, id)
        .await
        .map_err(|e| e.to_string())?;
    Ok(pretty(&json!({
        "template": record,
        "state": state,
        "is_stable": state.stable == Some(record.version),
        "launches": launches,
    })))
}

/// Read the optional `tags` array, validating each against the closed channel set.
#[cfg(feature = "templates")]
fn tags_arg(args: &Value) -> Result<Vec<crate::serve::history::templates::VersionChannel>, String> {
    use crate::serve::history::templates::VersionChannel;
    let Some(list) = args.get("tags").and_then(Value::as_array) else {
        return Ok(Vec::new());
    };
    list.iter()
        .map(|v| {
            v.as_str()
                .ok_or_else(|| "each `tags` entry must be a channel name".to_string())
                .and_then(|s| VersionChannel::parse(s).map_err(|e| e.to_string()))
        })
        .collect()
}

#[cfg(feature = "templates")]
async fn register_template(ctx: &McpContext, args: &Value) -> Result<String, String> {
    use crate::templates::RegisterRequest;
    let store = template_store(ctx)?;
    let config = str_arg(args, "config")?;
    let record = crate::templates::register(
        store,
        RegisterRequest {
            id: args.get("id").and_then(Value::as_str).map(str::to_string),
            body: config.to_string(),
            // MCP always hands over an inline document; YAML parses JSON too.
            format: crate::serve::load::ConfigFormat::Yaml,
            description: args
                .get("description")
                .and_then(Value::as_str)
                .map(str::to_string),
            tags: tags_arg(args)?,
            launch: args.get("launch").and_then(Value::as_bool).unwrap_or(false),
            created_by: Some("mcp".to_string()),
        },
    )
    .await
    .map_err(|e| e.to_string())?;
    Ok(pretty(&json!({
        "registered": record.summary(),
    })))
}

#[cfg(feature = "templates")]
async fn launch_template(ctx: &McpContext, args: &Value) -> Result<String, String> {
    use crate::serve::history::templates::VersionSelector;
    let store = template_store(ctx)?;
    let id = str_arg(args, "id")?;
    let target = match args.get("version") {
        None | Some(Value::Null) => VersionSelector::newest(),
        Some(_) => version_arg(args)?,
    };
    let outcome = crate::templates::launch(store, id, target, Some("mcp"))
        .await
        .map_err(|e| e.to_string())?;
    Ok(pretty(&json!({
        "id": id,
        "version": outcome.version,
        "replaced": outcome.replaced,
        "already_launched": outcome.already_launched,
        "first_launch": outcome.first_launch,
    })))
}

#[cfg(feature = "templates")]
async fn rollback_template(ctx: &McpContext, args: &Value) -> Result<String, String> {
    let store = template_store(ctx)?;
    let id = str_arg(args, "id")?;
    let outcome = crate::templates::rollback(store, id, Some("mcp"))
        .await
        .map_err(|e| e.to_string())?;
    Ok(pretty(&json!({
        "id": id,
        "version": outcome.version,
        "replaced": outcome.replaced,
    })))
}

#[cfg(feature = "templates")]
async fn deprecate_template(ctx: &McpContext, args: &Value) -> Result<String, String> {
    let store = template_store(ctx)?;
    let id = str_arg(args, "id")?;
    let undo = args.get("undo").and_then(Value::as_bool).unwrap_or(false);
    let reason = args
        .get("reason")
        .and_then(Value::as_str)
        .map(str::to_string);
    let status = crate::templates::set_deprecated(store, id, reason, Some("mcp"), !undo)
        .await
        .map_err(|e| e.to_string())?;
    Ok(pretty(&json!({ "id": id, "status": status.as_str() })))
}

#[cfg(feature = "templates")]
async fn run_template(ctx: &McpContext, args: &Value) -> Result<String, String> {
    let store = template_store(ctx)?;
    let id = str_arg(args, "id")?;
    let version = resolved_version_arg(store, id, args).await?;
    let deprecated = crate::templates::deprecation_warning(
        &crate::templates::template_state(store, id)
            .await
            .map_err(|e| e.to_string())?,
        version,
    );
    let dry_run = args
        .get("dry_run")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let supplied: crate::params::SuppliedParams = args
        .get("params")
        .and_then(Value::as_object)
        .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
        .unwrap_or_default();
    let env: std::collections::BTreeMap<String, String> = args
        .get("env")
        .and_then(Value::as_object)
        .map(|m| {
            m.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        })
        .unwrap_or_default();

    let sink = crate::templates::SinkChoice {
        id: args.get("sink").and_then(Value::as_str).map(str::to_string),
        version: match args.get("sink_version") {
            None | Some(Value::Null) => Default::default(),
            Some(v) => serde_json::from_value(v.clone()).map_err(|e| e.to_string())?,
        },
        overlay: match args.get("overlay") {
            None | Some(Value::Null) => None,
            Some(Value::String(id)) => Some(crate::templates::OverlayChoice::Registered {
                id: id.clone(),
                version: match args.get("overlay_version") {
                    None | Some(Value::Null) => Default::default(),
                    Some(v) => serde_json::from_value(v.clone()).map_err(|e| e.to_string())?,
                },
            }),
            Some(v @ Value::Object(_)) => Some(crate::templates::OverlayChoice::Inline(v.clone())),
            Some(_) => return Err("`overlay` must be a deployment id or a mapping".into()),
        },
    };
    let materialized = crate::templates::materialize_for_run(
        store,
        id,
        version,
        &sink,
        &supplied,
        &env,
        // The MCP tool runs the pipeline in this process; nothing is persisted.
        crate::templates::Materialize::Local,
    )
    .await
    .map_err(|e| e.to_string())?;

    if dry_run {
        // Validate the materialized config without touching a sink, and never
        // echo the body — a secret param value would be in it.
        let cfg = parse_config_with(&materialized.body, crate::params::BindMode::Strict)?;
        let rows = crate::expand::expand(&cfg)
            .map_err(|e| e.to_string())?
            .len();
        return Ok(pretty(&json!({
            "template_id": materialized.template_id,
            "template_version": materialized.version,
            "sink_template": materialized.sink_id,
            "sink_template_version": materialized.sink_version,
            "streams": materialized.streams,
            "overlay": materialized.overlay_id,
            "overlay_version": materialized.overlay_version,
            "overlay_contributes": materialized.overlay_contributes,
            "warnings": materialized.warnings,
            "params": materialized.params_redacted,
            "rows": rows,
            "dry_run": true,
            "deprecated": deprecated,
        })));
    }

    // The materialized body is JSON, which `run_from_yaml_str` parses (YAML is a
    // JSON superset) and takes through the ordinary run path.
    let summary = crate::run_from_yaml_str(&materialized.body)
        .await
        .map_err(|e| e.to_string())?;
    let failed = summary.failure_count();
    let total: usize = summary.invocations.iter().map(|i| i.records_written).sum();
    let doc = json!({
        "template_id": materialized.template_id,
        "template_version": materialized.version,
        "sink_template": materialized.sink_id,
        "sink_template_version": materialized.sink_version,
        "overlay": materialized.overlay_id,
        "overlay_version": materialized.overlay_version,
        "params": materialized.params_redacted,
        "invocations": summary.invocations.len(),
        "ok": summary.invocations.len() - failed,
        "failed": failed,
        "records_written": total,
        "deprecated": deprecated,
    });
    if failed > 0 {
        return Err(format!(
            "template run had {failed} failed invocation(s): {}",
            pretty(&doc)
        ));
    }
    Ok(pretty(&doc))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mcp::McpContext;

    fn ctx(allow: bool) -> McpContext {
        McpContext::new(
            crate::auth_catalog::build_auth_catalog(None).unwrap(),
            allow,
        )
    }

    #[tokio::test]
    async fn propose_tools_file_change_requests_on_a_server() {
        let dir = tempfile::tempdir().unwrap();
        let csv = dir.path().join("in.csv");
        std::fs::write(&csv, "id\n1\n").unwrap();
        let config = format!(
            "version: 1\nname: mcp-chg\npipeline:\n  source:\n    type: csv\n    config:\n      path: {}\n  sink:\n    type: jsonl\n    config:\n      path: {}\n",
            csv.display(),
            dir.path().join("out.jsonl").display()
        );
        for tool in ["propose_run", "propose_template"] {
            let out = call_tool(&ctx(true), tool, &json!({})).await;
            assert_eq!(out["isError"], true);
            assert!(
                out["content"][0]["text"]
                    .as_str()
                    .unwrap()
                    .contains("server transport")
            );
        }
        let proposer = crate::mcp::ChangeProposer {
            state: crate::serve::test_support::test_state(),
            actor: crate::serve::rbac::AuthContext::system("agent"),
        };
        let c = ctx(true).with_changes(proposer);
        assert!(format!("{:?}", c.changes.as_ref().unwrap()).contains("system:agent"));

        let out = call_tool(
            &c,
            "propose_run",
            &json!({"config": config, "reason": "nightly", "name": "n", "labels": null,
                    "budget": {"max_records": 10}}),
        )
        .await;
        assert_eq!(out["isError"], false, "{out}");
        assert!(
            out["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains("pending")
        );
        let bad = call_tool(
            &c,
            "propose_run",
            &json!({"config": config, "reason": "r", "budget": {"max_records": "x"}}),
        )
        .await;
        assert!(
            bad["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains("budget")
        );
        let bad = call_tool(&c, "propose_run", &json!({"config": "{", "reason": "r"})).await;
        assert_eq!(bad["isError"], true);

        let bad = call_tool(
            &c,
            "propose_template",
            &json!({"reason": "r", "action": "burn"}),
        )
        .await;
        assert!(
            bad["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains("unknown action")
        );
        let reg = call_tool(
            &c,
            "propose_template",
            &json!({"reason": "r", "config": config, "id": "mcp-chg", "launch": null}),
        )
        .await;
        #[cfg(feature = "templates")]
        assert_eq!(reg["isError"], false, "{reg}");
        #[cfg(not(feature = "templates"))]
        assert_eq!(reg["isError"], true);
        let launch = call_tool(
            &c,
            "propose_template",
            &json!({"reason": "r", "action": "launch", "id": "ghost", "version": 1}),
        )
        .await;
        assert!(launch["content"][0]["text"].is_string());
        let missing = call_tool(
            &c,
            "propose_template",
            &json!({"reason": "r", "action": "launch"}),
        )
        .await;
        assert_eq!(missing["isError"], true);
    }

    #[test]
    fn tool_defs_gate_mutations() {
        let ro = tool_defs(&ctx(false));
        assert!(ro.iter().all(|t| t.name != "run_pipeline"));
        let rw = tool_defs(&ctx(true));
        assert!(rw.iter().any(|t| t.name == "run_pipeline"));
    }

    #[tokio::test]
    async fn list_connectors_includes_sources_and_tier() {
        let out = call_tool(&ctx(false), "list_connectors", &json!({})).await;
        assert_eq!(out["isError"], false);
        let text = out["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("\"sources\""));
        assert!(text.contains("\"tier\""));
    }

    #[tokio::test]
    async fn list_connectors_filter_kind() {
        let out = call_tool(
            &ctx(false),
            "list_connectors",
            &json!({"kind": "transform"}),
        )
        .await;
        let text = out["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("\"transforms\""));
        assert!(!text.contains("\"sources\""));
    }

    #[tokio::test]
    async fn get_connector_schema_unknown_is_tool_error() {
        let out = call_tool(
            &ctx(false),
            "get_connector_schema",
            &json!({"kind":"source","name":"nope"}),
        )
        .await;
        assert_eq!(out["isError"], true);
    }

    #[tokio::test]
    async fn unknown_tool_errors() {
        let out = call_tool(&ctx(false), "does_not_exist", &json!({})).await;
        assert_eq!(out["isError"], true);
    }

    #[tokio::test]
    async fn run_pipeline_blocked_without_mutations() {
        let out = call_tool(&ctx(false), "run_pipeline", &json!({"config":"version: 1"})).await;
        assert_eq!(out["isError"], true);
        assert!(
            out["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains("--allow-mutations")
        );
    }

    // ── handler coverage: scaffold / validate / preview / run ────────────────

    fn csv_config(dir: &std::path::Path) -> String {
        let csv = dir.join("in.csv");
        std::fs::write(&csv, "id,name\n1,alice\n2,bob\n").unwrap();
        let out = dir.join("out.jsonl");
        format!(
            "version: 1\nname: t\npipeline:\n  source:\n    type: csv\n    config:\n      path: {}\n  sink:\n    type: jsonl\n    config:\n      path: {}\n",
            csv.display(),
            out.display()
        )
    }

    fn topology_config(dir: &std::path::Path) -> String {
        let csv = dir.join("in.csv");
        std::fs::write(&csv, "id,name\n1,alice\n").unwrap();
        let out = dir.join("out.jsonl");
        format!(
            "version: 1\nname: t\npipeline:\n  sources:\n    s: {{ type: csv, config: {{ path: {} }} }}\n  sinks:\n    o: {{ type: jsonl, config: {{ path: {} }} }}\n  nodes:\n    src: {{ kind: source, ref: s }}\n    w: {{ kind: sink, ref: o }}\n  edges:\n    - {{ from: src, to: w }}\n",
            csv.display(),
            out.display()
        )
    }

    #[tokio::test]
    async fn scaffold_config_emits_yaml() {
        let out = call_tool(
            &ctx(false),
            "scaffold_config",
            &json!({"source":"csv","sink":"jsonl","name":"demo"}),
        )
        .await;
        assert_eq!(out["isError"], false);
        let text = out["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("name: demo"));
        assert!(text.contains("type: csv"));
        assert!(text.contains("type: jsonl"));
    }

    #[tokio::test]
    async fn scaffold_config_missing_arg_errors() {
        let out = call_tool(&ctx(false), "scaffold_config", &json!({"source":"csv"})).await;
        assert_eq!(out["isError"], true);
        assert!(out["content"][0]["text"].as_str().unwrap().contains("sink"));
    }

    #[tokio::test]
    async fn validate_config_matrix_ok() {
        let dir = tempfile::tempdir().unwrap();
        let out = call_tool(
            &ctx(false),
            "validate_config",
            &json!({ "config": csv_config(dir.path()) }),
        )
        .await;
        assert_eq!(out["isError"], false);
        let text = out["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("\"mode\": \"matrix\""));
        assert!(text.contains("\"valid\": true"));
    }

    #[tokio::test]
    async fn validate_config_topology_ok() {
        let dir = tempfile::tempdir().unwrap();
        let out = call_tool(
            &ctx(false),
            "validate_config",
            &json!({ "config": topology_config(dir.path()) }),
        )
        .await;
        assert_eq!(out["isError"], false);
        assert!(
            out["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains("\"mode\": \"topology\"")
        );
    }

    #[tokio::test]
    async fn validate_config_bad_yaml_errors() {
        let out = call_tool(
            &ctx(false),
            "validate_config",
            &json!({ "config": "this: is: not: valid: yaml:" }),
        )
        .await;
        assert_eq!(out["isError"], true);
    }

    #[tokio::test]
    async fn preview_matrix_returns_records() {
        let dir = tempfile::tempdir().unwrap();
        let out = call_tool(
            &ctx(false),
            "preview",
            &json!({ "config": csv_config(dir.path()), "limit": 1 }),
        )
        .await;
        assert_eq!(out["isError"], false);
        let text = out["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("\"count\": 1"));
        assert!(text.contains("alice"));
    }

    #[tokio::test]
    async fn preview_topology_returns_sources() {
        let dir = tempfile::tempdir().unwrap();
        let out = call_tool(
            &ctx(false),
            "preview",
            &json!({ "config": topology_config(dir.path()) }),
        )
        .await;
        assert_eq!(out["isError"], false);
        assert!(
            out["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains("\"sources\"")
        );
    }

    #[tokio::test]
    async fn run_pipeline_dry_run_validates_and_previews() {
        let dir = tempfile::tempdir().unwrap();
        let out = call_tool(
            &ctx(true),
            "run_pipeline",
            &json!({ "config": csv_config(dir.path()), "dry_run": true }),
        )
        .await;
        assert_eq!(out["isError"], false);
        let text = out["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("-- preview --"));
    }

    #[tokio::test]
    async fn run_pipeline_real_writes_sink() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = csv_config(dir.path());
        let out = call_tool(&ctx(true), "run_pipeline", &json!({ "config": cfg })).await;
        assert_eq!(out["isError"], false, "{}", out["content"][0]["text"]);
        assert!(
            out["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains("\"records_written\": 2")
        );
        assert_eq!(
            std::fs::read_to_string(dir.path().join("out.jsonl"))
                .unwrap()
                .lines()
                .count(),
            2
        );
    }

    #[tokio::test]
    async fn get_connector_schema_transform_ok() {
        let out = call_tool(
            &ctx(false),
            "get_connector_schema",
            &json!({"kind":"transform","name":"keys_case"}),
        )
        .await;
        assert_eq!(out["isError"], false);
    }

    #[tokio::test]
    async fn get_connector_schema_bad_kind_errors() {
        let out = call_tool(
            &ctx(false),
            "get_connector_schema",
            &json!({"kind":"weird","name":"x"}),
        )
        .await;
        assert_eq!(out["isError"], true);
    }

    #[tokio::test]
    async fn validate_config_accepts_a_parameterized_config() {
        // Read-only introspection binds required params to placeholders, so a
        // template-shaped config still validates (#444).
        let dir = tempfile::tempdir().unwrap();
        let cfg = format!(
            "version: 1\nname: t\nparams:\n  tag: {{ required: true }}\npipeline:\n  source:\n    type: csv\n    config:\n      path: {}\n  sink:\n    type: jsonl\n    config:\n      path: {}\n",
            dir.path().join("in-${param.tag}.csv").display(),
            dir.path().join("out.jsonl").display()
        );
        let out = call_tool(&ctx(false), "validate_config", &json!({ "config": cfg })).await;
        assert_eq!(out["isError"], false, "{}", out["content"][0]["text"]);
    }

    // ── Pipeline template tools (#444) ──────────────────────────────────────

    #[cfg(feature = "templates")]
    mod templates {
        use super::*;
        use std::sync::Arc;
        use std::time::Duration;

        fn tpl_ctx(allow: bool) -> McpContext {
            let store = Arc::new(crate::serve::history::memory::MemoryHistory::new(
                Duration::from_secs(60),
            )) as crate::templates::TemplateStore;
            McpContext::new(
                crate::auth_catalog::build_auth_catalog(None).unwrap(),
                allow,
            )
            .with_templates(store)
        }

        fn body(dir: &std::path::Path) -> String {
            let csv = dir.join("in.csv");
            std::fs::write(&csv, "id,name\n1,alice\n2,bob\n").unwrap();
            format!(
                "version: 1\nname: mcp-tpl\nparams:\n  tag: {{ required: true }}\npipeline:\n  source:\n    type: csv\n    config:\n      path: {}\n  sink:\n    type: jsonl\n    config:\n      path: {}\n",
                csv.display(),
                dir.join("out-${param.tag}.jsonl").display()
            )
        }

        fn hub_pair(dir: &std::path::Path) -> (String, String) {
            std::fs::write(dir.join("orders.csv"), "id,total\n1,10\n").unwrap();
            let source = format!(
                "kind: source-template\nname: acme-exports\ndescription: Acme exports\nparams:\n  data_dir: {{ type: string, default: {} }}\nsource:\n  type: csv\n  config:\n    path: \"${{param.data_dir}}/orders.csv\"\nstreams:\n  - {{ name: orders, primary_keys: [id], write: [overwrite, upsert] }}\n",
                dir.display()
            );
            let sink = format!(
                "kind: sink-template\nname: local-jsonl\ndescription: Local files\nparams:\n  out_dir: {{ type: string, default: {} }}\nsink:\n  type: jsonl\n  config: {{ append: false }}\nper_stream:\n  path: \"${{param.out_dir}}/${{source}}/${{stream}}.jsonl\"\nwrite_mode_aliases: {{ overwrite: append }}\n",
                dir.display()
            );
            (source, sink)
        }

        #[tokio::test]
        async fn run_template_composes_a_source_with_a_sink() {
            let dir = tempfile::tempdir().unwrap();
            let ctx = tpl_ctx(true);
            let (source, sink) = hub_pair(dir.path());
            for config in [source, sink] {
                let out = call_tool(
                    &ctx,
                    "register_template",
                    &json!({"config": config, "launch": true}),
                )
                .await;
                assert_eq!(out["isError"], false, "{out}");
            }
            // The list carries each template's kind.
            let out = call_tool(&ctx, "list_templates", &json!({})).await;
            let text = out["content"][0]["text"].as_str().unwrap();
            assert!(
                text.contains("source-template") && text.contains("sink-template"),
                "{text}"
            );

            // A source template needs a sink …
            let out = call_tool(
                &ctx,
                "run_template",
                &json!({"id": "acme-exports", "dry_run": true}),
            )
            .await;
            assert_eq!(out["isError"], true);
            assert!(out["content"][0]["text"].as_str().unwrap().contains("sink"));

            // … and composes with one; the dry run reports both halves and the plan.
            let out = call_tool(
                &ctx,
                "run_template",
                &json!({"id": "acme-exports", "sink": "local-jsonl", "sink_version": "stable", "dry_run": true}),
            )
            .await;
            assert_eq!(out["isError"], false, "{out}");
            let doc: Value =
                serde_json::from_str(out["content"][0]["text"].as_str().unwrap()).unwrap();
            assert_eq!(doc["sink_template"], json!("local-jsonl"));
            assert_eq!(doc["sink_template_version"], json!(1));
            assert_eq!(doc["streams"][0]["stream"], json!("orders"));
            assert_eq!(doc["rows"], json!(1));

            // A real run writes the stream file under <out_dir>/<source>/.
            let out = call_tool(
                &ctx,
                "run_template",
                &json!({"id": "acme-exports", "sink": "local-jsonl"}),
            )
            .await;
            assert_eq!(out["isError"], false, "{out}");
            let written =
                std::fs::read_to_string(dir.path().join("acme-exports/orders.jsonl")).unwrap();
            assert_eq!(written.lines().count(), 1);

            // A malformed sink version is a tool error, not a panic.
            let out = call_tool(
                &ctx,
                "run_template",
                &json!({"id": "acme-exports", "sink": "local-jsonl", "sink_version": "latest", "dry_run": true}),
            )
            .await;
            assert_eq!(out["isError"], true);

            // #679: an inline overlay is applied and reported in the dry run; a
            // registered one resolves by id; a malformed one is a tool error.
            let out = call_tool(
                &ctx,
                "run_template",
                &json!({"id": "acme-exports", "sink": "local-jsonl", "dry_run": true,
                        "overlay": {"execution": {"max_concurrent": 1}}}),
            )
            .await;
            assert_eq!(out["isError"], false, "{out}");
            let doc: Value =
                serde_json::from_str(out["content"][0]["text"].as_str().unwrap()).unwrap();
            assert_eq!(doc["overlay"], json!("inline"));
            assert_eq!(doc["overlay_contributes"], json!(["execution"]));
            let out = call_tool(
                &ctx,
                "run_template",
                &json!({"id": "acme-exports", "sink": "local-jsonl", "dry_run": true,
                        "overlay": "nope", "overlay_version": "stable"}),
            )
            .await;
            assert_eq!(out["isError"], true, "an unknown registered overlay");
            let out = call_tool(
                &ctx,
                "run_template",
                &json!({"id": "acme-exports", "sink": "local-jsonl", "dry_run": true, "overlay": 3}),
            )
            .await;
            assert_eq!(out["isError"], true);
            assert!(
                out["content"][0]["text"]
                    .as_str()
                    .unwrap()
                    .contains("deployment id or a mapping")
            );
        }

        #[tokio::test]
        async fn tools_are_hidden_without_a_store() {
            let names: Vec<&str> = tool_defs(&ctx(true)).iter().map(|t| t.name).collect();
            for t in ["list_templates", "register_template", "launch_template"] {
                assert!(!names.contains(&t), "{t} must be hidden: {names:?}");
            }
            // Calling one anyway is a clear tool error, not a panic.
            let out = call_tool(&ctx(true), "list_templates", &json!({})).await;
            assert_eq!(out["isError"], true);
            assert!(
                out["content"][0]["text"]
                    .as_str()
                    .unwrap()
                    .contains("--template-store")
            );
        }

        #[tokio::test]
        async fn read_tools_are_ungated_and_write_tools_are_gated() {
            let ro: Vec<&str> = tool_defs(&tpl_ctx(false)).iter().map(|t| t.name).collect();
            for t in ["list_templates", "get_template"] {
                assert!(ro.contains(&t), "{t} should be read-only: {ro:?}");
            }
            let mutating = [
                "register_template",
                "run_template",
                "launch_template",
                "rollback_template",
                "deprecate_template",
            ];
            for t in mutating {
                assert!(!ro.contains(&t), "{t} must be gated: {ro:?}");
            }
            let rw: Vec<&str> = tool_defs(&tpl_ctx(true)).iter().map(|t| t.name).collect();
            for t in mutating {
                assert!(rw.contains(&t), "{t} should appear with mutations: {rw:?}");
                let out = call_tool(&tpl_ctx(false), t, &json!({"id":"x","config":"y"})).await;
                assert_eq!(out["isError"], true, "{t} must be gated");
                assert!(
                    out["content"][0]["text"]
                        .as_str()
                        .unwrap()
                        .contains("--allow-mutations")
                );
            }
        }

        #[tokio::test]
        async fn an_operator_runs_templates_but_the_lifecycle_tools_are_admin_only() {
            let operator = tpl_ctx(true).with_template_admin(false);
            let names: Vec<&str> = tool_defs(&operator).iter().map(|t| t.name).collect();
            assert!(names.contains(&"run_template"), "{names:?}");
            for t in [
                "register_template",
                "launch_template",
                "rollback_template",
                "deprecate_template",
            ] {
                assert!(
                    !names.contains(&t),
                    "{t} must be hidden from an operator: {names:?}"
                );
                let out = call_tool(&operator, t, &json!({"id":"x","config":"y"})).await;
                assert_eq!(out["isError"], true, "{t}");
                assert!(
                    out["content"][0]["text"]
                        .as_str()
                        .unwrap()
                        .contains("admin-only"),
                    "{t}"
                );
            }
        }

        #[tokio::test]
        async fn register_list_get_and_run_round_trip() {
            let dir = tempfile::tempdir().unwrap();
            let ctx = tpl_ctx(true);

            // `launch: true` registers and goes live in one step.
            let out = call_tool(
                &ctx,
                "register_template",
                &json!({ "config": body(dir.path()), "launch": true }),
            )
            .await;
            assert_eq!(out["isError"], false, "{}", out["content"][0]["text"]);
            assert!(
                out["content"][0]["text"]
                    .as_str()
                    .unwrap()
                    .contains("mcp-tpl")
            );

            let out = call_tool(&ctx, "list_templates", &json!({})).await;
            let text = out["content"][0]["text"].as_str().unwrap();
            assert!(text.contains("\"count\": 1"), "{text}");
            assert!(text.contains("\"launched\""), "status is surfaced: {text}");

            let out = call_tool(&ctx, "get_template", &json!({"id":"mcp-tpl"})).await;
            assert_eq!(out["isError"], false);
            let text = out["content"][0]["text"].as_str().unwrap();
            assert!(text.contains("\"launches\""), "{text}");
            assert!(text.contains("${param.tag}"), "body is verbatim: {text}");

            let out = call_tool(&ctx, "get_template", &json!({"id":"nope"})).await;
            assert_eq!(out["isError"], true);

            // A missing required param is a tool error naming it.
            let out = call_tool(&ctx, "run_template", &json!({"id":"mcp-tpl"})).await;
            assert_eq!(out["isError"], true);
            assert!(out["content"][0]["text"].as_str().unwrap().contains("tag"));

            // dry_run materializes + validates without writing.
            let out = call_tool(
                &ctx,
                "run_template",
                &json!({"id":"mcp-tpl","params":{"tag":"dry"},"dry_run":true}),
            )
            .await;
            assert_eq!(out["isError"], false, "{}", out["content"][0]["text"]);
            assert!(
                out["content"][0]["text"]
                    .as_str()
                    .unwrap()
                    .contains("\"dry_run\": true")
            );
            assert!(!dir.path().join("out-dry.jsonl").exists());

            // The real run writes through the ordinary pipeline path.
            let out = call_tool(
                &ctx,
                "run_template",
                &json!({"id":"mcp-tpl","params":{"tag":"real"}}),
            )
            .await;
            assert_eq!(out["isError"], false, "{}", out["content"][0]["text"]);
            assert!(
                out["content"][0]["text"]
                    .as_str()
                    .unwrap()
                    .contains("\"records_written\": 2")
            );
            assert_eq!(
                std::fs::read_to_string(dir.path().join("out-real.jsonl"))
                    .unwrap()
                    .lines()
                    .count(),
                2
            );
        }

        #[tokio::test]
        async fn a_draft_template_is_not_runnable_unpinned() {
            let dir = tempfile::tempdir().unwrap();
            let ctx = tpl_ctx(true);
            // No `launch` — the work-in-progress state.
            call_tool(
                &ctx,
                "register_template",
                &json!({ "config": body(dir.path()) }),
            )
            .await;

            let out = call_tool(
                &ctx,
                "run_template",
                &json!({"id":"mcp-tpl","params":{"tag":"x"},"dry_run":true}),
            )
            .await;
            assert_eq!(out["isError"], true);
            let text = out["content"][0]["text"].as_str().unwrap();
            assert!(text.contains("no launched version"), "{text}");

            // An explicit build still runs, so a draft is testable.
            let out = call_tool(
                &ctx,
                "run_template",
                &json!({"id":"mcp-tpl","params":{"tag":"x"},"version":"newest","dry_run":true}),
            )
            .await;
            assert_eq!(out["isError"], false, "{}", out["content"][0]["text"]);
        }

        #[tokio::test]
        async fn launch_rollback_and_deprecate_tools() {
            let dir = tempfile::tempdir().unwrap();
            let ctx = tpl_ctx(true);
            call_tool(
                &ctx,
                "register_template",
                &json!({ "config": body(dir.path()), "launch": true }),
            )
            .await; // v1 live
            call_tool(
                &ctx,
                "register_template",
                &json!({ "config": body(dir.path()) }),
            )
            .await; // v2 build

            // Launch defaults to `newest`.
            let out = call_tool(&ctx, "launch_template", &json!({"id":"mcp-tpl"})).await;
            assert_eq!(out["isError"], false, "{}", out["content"][0]["text"]);
            let text = out["content"][0]["text"].as_str().unwrap();
            assert!(text.contains("\"version\": 2"), "{text}");
            assert!(text.contains("\"replaced\": 1"), "{text}");

            // Rollback returns to v1.
            let out = call_tool(&ctx, "rollback_template", &json!({"id":"mcp-tpl"})).await;
            assert_eq!(out["isError"], false, "{}", out["content"][0]["text"]);
            assert!(
                out["content"][0]["text"]
                    .as_str()
                    .unwrap()
                    .contains("\"version\": 1")
            );

            // Deprecate, then revive.
            let out = call_tool(
                &ctx,
                "deprecate_template",
                &json!({"id":"mcp-tpl","reason":"superseded"}),
            )
            .await;
            assert!(
                out["content"][0]["text"]
                    .as_str()
                    .unwrap()
                    .contains("deprecated")
            );
            // Launching into a retired template is refused.
            let out = call_tool(&ctx, "launch_template", &json!({"id":"mcp-tpl"})).await;
            assert_eq!(out["isError"], true);
            let out = call_tool(
                &ctx,
                "deprecate_template",
                &json!({"id":"mcp-tpl","undo":true}),
            )
            .await;
            assert!(
                out["content"][0]["text"]
                    .as_str()
                    .unwrap()
                    .contains("launched")
            );
        }

        #[tokio::test]
        async fn register_with_tags_and_run_by_channel() {
            let dir = tempfile::tempdir().unwrap();
            let ctx = tpl_ctx(true);

            // v1 live; v2 tagged `dev` but not launched.
            call_tool(
                &ctx,
                "register_template",
                &json!({ "config": body(dir.path()), "launch": true }),
            )
            .await;
            let out = call_tool(
                &ctx,
                "register_template",
                &json!({ "config": body(dir.path()), "tags": ["dev"] }),
            )
            .await;
            assert_eq!(out["isError"], false, "{}", out["content"][0]["text"]);

            let out = call_tool(&ctx, "get_template", &json!({"id":"mcp-tpl"})).await;
            let text = out["content"][0]["text"].as_str().unwrap();
            assert!(text.contains("\"stable\": 1"), "{text}");
            assert!(text.contains("\"newest\": 2"), "{text}");
            assert!(text.contains("\"dev\": 2"), "{text}");

            // Each selector resolves to its own version.
            for (version, want) in [
                (json!("stable"), 1),
                (json!("dev"), 2),
                (json!("newest"), 2),
                (json!(1), 1),
            ] {
                let out = call_tool(
                    &ctx,
                    "run_template",
                    &json!({"id":"mcp-tpl","params":{"tag":"c"},"version":version,"dry_run":true}),
                )
                .await;
                assert_eq!(out["isError"], false, "{}", out["content"][0]["text"]);
                let text = out["content"][0]["text"].as_str().unwrap();
                assert!(
                    text.contains(&format!("\"template_version\": {want}")),
                    "{version} should resolve to v{want}: {text}"
                );
            }

            // A channel outside the closed set is a tool error, on both paths.
            for args in [
                json!({ "config": body(dir.path()), "tags": ["prd"] }),
                json!({ "config": body(dir.path()), "tags": ["stable"] }),
            ] {
                let out = call_tool(&ctx, "register_template", &args).await;
                assert_eq!(out["isError"], true, "{args}");
            }
            for bad in ["nope", "latest", "canary"] {
                let out = call_tool(
                    &ctx,
                    "run_template",
                    &json!({"id":"mcp-tpl","params":{"tag":"c"},"version":bad}),
                )
                .await;
                assert_eq!(out["isError"], true, "version={bad} must be refused");
            }
        }

        #[tokio::test]
        async fn register_rejects_an_invalid_config() {
            let out = call_tool(
                &tpl_ctx(true),
                "register_template",
                &json!({ "config": "version: 1\nname: x\nbogus: 1\npipeline: {}\n" }),
            )
            .await;
            assert_eq!(out["isError"], true);
        }

        #[tokio::test]
        async fn env_overrides_flow_through_run_template() {
            let dir = tempfile::tempdir().unwrap();
            let ctx = tpl_ctx(true);
            let csv = dir.path().join("in.csv");
            std::fs::write(&csv, "id\n1\n").unwrap();
            let cfg = format!(
                "version: 1\nname: mcp-env\npipeline:\n  source:\n    type: csv\n    config:\n      path: {}\n  sink:\n    type: jsonl\n    config:\n      path: {}/out-${{env:MCP_TPL_SUFFIX}}.jsonl\n",
                csv.display(),
                dir.path().display()
            );
            call_tool(
                &ctx,
                "register_template",
                &json!({ "config": cfg, "launch": true }),
            )
            .await;
            let out = call_tool(
                &ctx,
                "run_template",
                &json!({"id":"mcp-env","env":{"MCP_TPL_SUFFIX":"eu"}}),
            )
            .await;
            assert_eq!(out["isError"], false, "{}", out["content"][0]["text"]);
            assert!(dir.path().join("out-eu.jsonl").exists());
        }
    }
}
