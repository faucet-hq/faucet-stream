//! MCP (Model Context Protocol) server surface for faucet (issue #420).
//!
//! A transport-agnostic JSON-RPC 2.0 dispatcher ([`handle_message`]) plus the
//! tool implementations ([`tools`]). Two front-ends drive it:
//!
//! - **stdio** — the `faucet mcp` subcommand ([`crate::commands::mcp`]), for
//!   local agents (Claude Desktop / Code).
//! - **Streamable HTTP** — the `/mcp` route mounted by `faucet serve --mcp`,
//!   which inherits serve's bearer-auth + RBAC + audit.
//!
//! The dispatcher re-exposes existing faucet capabilities (list / schema /
//! scaffold / validate / preview, and a gated `run_pipeline`) in the shape an
//! LLM agent speaks; it re-implements no pipeline logic. Read-only tools are
//! always available; the mutating `run_pipeline` tool appears only when the
//! context is constructed with `allow_mutations = true` (the `--allow-mutations`
//! flag, ANDed with the caller's RBAC scope on the HTTP transport).

pub mod protocol;
pub mod tools;

use crate::auth_catalog::AuthCatalog;
use protocol::*;
use serde_json::{Value, json};

/// Everything a tool handler needs, independent of transport.
pub struct McpContext {
    /// Shared auth providers (for `preview`/`run_pipeline` connector builds).
    pub auth: AuthCatalog,
    /// Whether mutating tools (`run_pipeline`, `register_template`,
    /// `run_template`) are exposed and callable.
    pub allow_mutations: bool,
    /// Whether tools that **act on a caller-supplied config** — `validate_config`
    /// and `preview` — are exposed and callable.
    ///
    /// These are read-only in the sense that they write nothing, but they are not
    /// harmless: `preview` constructs the connector the caller describes and
    /// returns its records, and both resolve `${env:}` / `${file:}` /
    /// `${secret:}` against the *server's* environment and filesystem. On the
    /// HTTP transport that is server-side file read and outbound network reach,
    /// so it requires the same scope as `POST /v1/doctor` (operator+) rather than
    /// the schema-read scope the route's baseline uses (#456 C4). The stdio
    /// transport runs as the local user and sets this `true`.
    pub allow_config_execution: bool,
    /// Whether the template-lifecycle tools (`register_template`,
    /// `launch_template`, `rollback_template`, `deprecate_template`) are exposed
    /// and callable, on top of `allow_mutations`. The HTTP transport passes the
    /// caller's `TemplateAdmin` decision (admin-only, #698); `run_template` needs
    /// only `allow_mutations`. The stdio transport runs as the local user and
    /// sets this `true`.
    pub allow_template_admin: bool,
    /// The pipeline template registry (#444), when one is wired: `faucet serve
    /// --mcp` passes its own `--history` backend; `faucet mcp` needs
    /// `--template-store`. Absent = the template tools are not advertised at all,
    /// so an agent never sees a tool it cannot use.
    #[cfg(feature = "templates")]
    pub templates: Option<crate::templates::TemplateStore>,
    /// The change-request proposer (#703), when the transport can reach a
    /// server: `faucet serve --mcp` wires its own state and the caller's
    /// identity, so `propose_run` / `propose_template` create pending requests
    /// as that principal. Absent (the stdio transport) = not advertised.
    pub changes: Option<ChangeProposer>,
}

/// What the MCP `propose_*` tools need to file a change request (#703).
#[derive(Clone)]
pub struct ChangeProposer {
    pub state: crate::serve::state::ServerState,
    pub actor: crate::serve::rbac::AuthContext,
}

impl std::fmt::Debug for ChangeProposer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChangeProposer")
            .field("actor", &self.actor.principal)
            .finish()
    }
}

impl McpContext {
    /// A context for a **local** caller (the `faucet mcp` stdio transport), which
    /// already runs with the user's own privileges: config-executing tools are
    /// enabled, mutations follow `allow_mutations`.
    pub fn new(auth: AuthCatalog, allow_mutations: bool) -> Self {
        Self {
            auth,
            allow_mutations,
            allow_config_execution: true,
            allow_template_admin: true,
            #[cfg(feature = "templates")]
            templates: None,
            changes: None,
        }
    }

    /// Attach a change-request proposer (#703), enabling `propose_run` /
    /// `propose_template`. The HTTP transport passes it only when the caller
    /// holds `ChangeRequest`.
    pub fn with_changes(mut self, proposer: ChangeProposer) -> Self {
        self.changes = Some(proposer);
        self
    }

    /// Set whether `validate_config` / `preview` are available. The HTTP
    /// transport passes the caller's RBAC decision here.
    pub fn with_config_execution(mut self, allow: bool) -> Self {
        self.allow_config_execution = allow;
        self
    }

    /// Set whether the template-lifecycle tools are available. The HTTP
    /// transport passes the caller's RBAC decision here.
    pub fn with_template_admin(mut self, allow: bool) -> Self {
        self.allow_template_admin = allow;
        self
    }

    /// Attach a template registry, enabling the template tools.
    #[cfg(feature = "templates")]
    pub fn with_templates(mut self, store: crate::templates::TemplateStore) -> Self {
        self.templates = Some(store);
        self
    }
}

/// Install a tracing subscriber that writes to **stderr** — stdout is reserved
/// for the JSON-RPC message stream in `faucet mcp` (stdio) mode.
pub fn install_stderr_tracing(level: &str, format: crate::cli::LogFormat) {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_new(level).unwrap_or_else(|_| EnvFilter::new("info"));
    // Always stderr, whatever the format: stdout carries the MCP JSON-RPC
    // stream, and two JSON streams on one pipe would corrupt the protocol.
    let builder = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr);
    match format {
        crate::cli::LogFormat::Text => {
            let _ = builder.try_init();
        }
        crate::cli::LogFormat::Json => {
            let _ = builder
                .json()
                .flatten_event(true)
                .with_current_span(true)
                .with_span_list(false)
                .try_init();
        }
    }
}

/// Server identity reported in `initialize`.
fn server_info() -> Value {
    json!({ "name": "faucet", "version": env!("CARGO_PKG_VERSION") })
}

/// Handle one JSON-RPC message. Returns `Some(response_json)` for a request,
/// or `None` for a notification (which gets no response). A malformed message
/// yields a JSON-RPC parse/invalid-request error envelope.
pub async fn handle_message(ctx: &McpContext, raw: &str) -> Option<String> {
    let req: JsonRpcRequest = match serde_json::from_str(raw) {
        Ok(r) => r,
        Err(e) => {
            return Some(render(error_no_id(
                PARSE_ERROR,
                format!("parse error: {e}"),
            )));
        }
    };

    // Notifications (no id) get no response, whatever the method.
    if req.is_notification() {
        return None;
    }
    let id = req.id.clone().unwrap_or(Value::Null);

    let resp = match req.method.as_str() {
        "initialize" => success(
            id,
            json!({
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": { "tools": {}, "resources": {} },
                "serverInfo": server_info(),
            }),
        ),
        "ping" => success(id, json!({})),
        "tools/list" => {
            let defs = tools::tool_defs(ctx);
            success(id, json!({ "tools": defs }))
        }
        "tools/call" => match req.params.get("name").and_then(Value::as_str) {
            Some(name) => {
                let empty = json!({});
                let args = req.params.get("arguments").unwrap_or(&empty);
                let result = tools::call_tool(ctx, name, args).await;
                success(id, result)
            }
            None => error(id, INVALID_PARAMS, "tools/call requires a 'name' parameter"),
        },
        "resources/list" => success(id, json!({ "resources": [] })),
        "resources/read" => error(
            id,
            INVALID_PARAMS,
            "no readable resources are exposed in this version",
        ),
        other => error(id, METHOD_NOT_FOUND, format!("unknown method '{other}'")),
    };
    Some(render(resp))
}

/// Drive an MCP session over any byte streams: read newline-delimited JSON-RPC
/// from `reader`, write each response (one JSON object per line) to `writer`.
/// Blank lines are skipped; notifications produce no output. Returns when the
/// reader hits EOF. The `faucet mcp` stdio command wraps stdin/stdout with
/// this; tests drive it with in-memory buffers.
pub async fn serve_stdio<R, W>(ctx: &McpContext, reader: R, writer: &mut W) -> std::io::Result<()>
where
    R: tokio::io::AsyncBufRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
    let mut lines = reader.lines();
    while let Some(line) = lines.next_line().await? {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if let Some(response) = handle_message(ctx, trimmed).await {
            writer.write_all(response.as_bytes()).await?;
            writer.write_all(b"\n").await?;
            writer.flush().await?;
        }
    }
    Ok(())
}

fn render(v: Value) -> String {
    serde_json::to_string(&v).unwrap_or_else(|_| {
        r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32603,"message":"failed to serialize response"}}"#.to_string()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx() -> McpContext {
        McpContext::new(
            crate::auth_catalog::build_auth_catalog(None).unwrap(),
            false,
        )
    }

    async fn call(raw: &str) -> Value {
        let s = handle_message(&ctx(), raw).await.expect("response");
        serde_json::from_str(&s).unwrap()
    }

    #[tokio::test]
    async fn initialize_reports_protocol_and_server() {
        let v = call(r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}"#).await;
        assert_eq!(v["result"]["protocolVersion"], PROTOCOL_VERSION);
        assert_eq!(v["result"]["serverInfo"]["name"], "faucet");
        assert!(v["result"]["capabilities"]["tools"].is_object());
    }

    #[tokio::test]
    async fn ping_ok() {
        let v = call(r#"{"jsonrpc":"2.0","id":2,"method":"ping"}"#).await;
        assert!(v["result"].is_object());
    }

    #[tokio::test]
    async fn notification_gets_no_response() {
        let out = handle_message(
            &ctx(),
            r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#,
        )
        .await;
        assert!(out.is_none());
    }

    #[tokio::test]
    async fn tools_list_has_readonly_and_hides_mutations() {
        let v = call(r#"{"jsonrpc":"2.0","id":3,"method":"tools/list"}"#).await;
        let names: Vec<String> = v["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["name"].as_str().unwrap().to_string())
            .collect();
        assert!(names.contains(&"list_connectors".to_string()));
        assert!(names.contains(&"validate_config".to_string()));
        assert!(!names.contains(&"run_pipeline".to_string()));
    }

    /// #456 C4: a caller without the config-execution scope must neither see nor
    /// be able to invoke the tools that build connectors from a config they
    /// supply — that is server-side file read and outbound network reach.
    #[tokio::test]
    async fn config_executing_tools_are_hidden_and_refused_without_the_scope() {
        let ctx = McpContext::new(
            crate::auth_catalog::build_auth_catalog(None).unwrap(),
            false,
        )
        .with_config_execution(false);

        let listed = handle_message(&ctx, r#"{"jsonrpc":"2.0","id":1,"method":"tools/list"}"#)
            .await
            .unwrap();
        assert!(
            !listed.contains("validate_config") && !listed.contains("\"preview\""),
            "must not be advertised: {listed}"
        );
        // Still present: pure introspection needs no elevated scope.
        assert!(listed.contains("list_connectors"), "{listed}");

        // Naming an unadvertised tool must be refused, not executed.
        for tool in ["validate_config", "preview"] {
            let out = tools::call_tool(
                &ctx,
                tool,
                &json!({ "config": "version: 1\npipeline: {}\n" }),
            )
            .await;
            assert_eq!(out["isError"], true, "{tool} must be refused");
            let text = out["content"][0]["text"].as_str().unwrap();
            assert!(text.contains("operator"), "{tool}: {text}");
        }
    }

    #[tokio::test]
    async fn tools_list_shows_mutations_when_allowed() {
        let ctx = McpContext::new(crate::auth_catalog::build_auth_catalog(None).unwrap(), true);
        let s = handle_message(&ctx, r#"{"jsonrpc":"2.0","id":3,"method":"tools/list"}"#)
            .await
            .unwrap();
        assert!(s.contains("run_pipeline"));
    }

    #[tokio::test]
    async fn tools_call_dispatches() {
        let v = call(
            r#"{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"list_connectors","arguments":{"kind":"state"}}}"#,
        )
        .await;
        assert_eq!(v["result"]["isError"], false);
        assert!(
            v["result"]["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains("state_stores")
        );
    }

    #[tokio::test]
    async fn tools_call_without_name_is_invalid_params() {
        let v = call(r#"{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{}}"#).await;
        assert_eq!(v["error"]["code"], INVALID_PARAMS);
    }

    #[tokio::test]
    async fn unknown_method_is_method_not_found() {
        let v = call(r#"{"jsonrpc":"2.0","id":6,"method":"frobnicate"}"#).await;
        assert_eq!(v["error"]["code"], METHOD_NOT_FOUND);
    }

    #[tokio::test]
    async fn malformed_json_is_parse_error() {
        let s = handle_message(&ctx(), "not json").await.unwrap();
        let v: Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["error"]["code"], PARSE_ERROR);
    }

    #[tokio::test]
    async fn resources_list_is_empty() {
        let v = call(r#"{"jsonrpc":"2.0","id":7,"method":"resources/list"}"#).await;
        assert_eq!(v["result"]["resources"].as_array().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn resources_read_is_invalid_params() {
        let v = call(r#"{"jsonrpc":"2.0","id":8,"method":"resources/read","params":{"uri":"x"}}"#)
            .await;
        assert_eq!(v["error"]["code"], INVALID_PARAMS);
    }

    #[test]
    fn install_stderr_tracing_is_idempotent() {
        // Just exercises the installer (try_init, so a second global subscriber
        // is a no-op rather than a panic).
        install_stderr_tracing("info", crate::cli::LogFormat::Text);
        install_stderr_tracing("debug", crate::cli::LogFormat::Json);
    }

    #[tokio::test]
    async fn serve_stdio_processes_lines_and_skips_blanks_and_notifications() {
        use std::io::Cursor;
        // A request, a blank line (skipped), a notification (no output), then a
        // second request. Expect exactly two response lines.
        let input = "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"ping\"}\n\
                     \n\
                     {\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}\n\
                     {\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/list\"}\n";
        let mut output: Vec<u8> = Vec::new();
        let reader = tokio::io::BufReader::new(Cursor::new(input.as_bytes().to_vec()));
        serve_stdio(&ctx(), reader, &mut output).await.unwrap();
        let text = String::from_utf8(output).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(
            lines.len(),
            2,
            "one response per request, none for blank/notification"
        );
        let first: Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(first["id"], 1);
        let second: Value = serde_json::from_str(lines[1]).unwrap();
        assert!(second["result"]["tools"].is_array());
    }
}
