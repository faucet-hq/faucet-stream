//! `faucet mcp` — an MCP server over stdio (issue #420).
//!
//! Reads newline-delimited JSON-RPC 2.0 messages from stdin and writes each
//! response to stdout (one JSON object per line), dispatching through the
//! shared [`crate::mcp`] handler. There is no bearer / RBAC layer, so the
//! default server treats every config a client sends as untrusted: the
//! config-reading tools (`validate_config`, `preview`) screen it like a body
//! submitted to `faucet serve` — no subprocess connectors, no `${env:}` /
//! `${file:}` / secret-manager directives, no local paths. `--allow-mutations`
//! declares the client trusted: configs run with the local user's access and
//! `run_pipeline` is exposed.
//!
//! Logs go to **stderr** (installed in `run_main`) so the stdout JSON-RPC
//! stream stays clean.

use crate::cli::McpArgs;
use crate::error::CliResult;
use crate::mcp::{McpContext, serve_stdio};
use tokio::io::BufReader;

pub async fn run(args: McpArgs) -> CliResult<()> {
    let cwd = std::env::current_dir()?;
    let env_path =
        crate::env_loader::resolve_env_file(args.env_file.as_deref(), args.no_env_file, &cwd)?;
    crate::env_loader::load_env_file_if_present(env_path.as_deref())?;

    // stdio mode has no shared `auth:` catalog; inline connector auth still
    // works for configs passed to preview/validate/run_pipeline.
    let auth = crate::auth_catalog::build_auth_catalog(None)?;
    #[cfg_attr(not(feature = "templates"), allow(unused_mut))]
    let mut ctx = stdio_context(auth, args.allow_mutations);

    // Pipeline-template registry (#444): opt-in over stdio, since there is no
    // `--history` backend to inherit. Without it the template tools are not
    // advertised at all.
    #[cfg(feature = "templates")]
    if let Some(url) = args.template_store.as_deref() {
        ctx = ctx.with_templates(crate::templates::resolve_store_url(url).await?);
        tracing::info!(store = %url, "pipeline-template registry attached");
    }

    tracing::info!(
        allow_mutations = args.allow_mutations,
        "faucet mcp stdio server started"
    );

    let mut stdout = tokio::io::stdout();
    serve_stdio(&ctx, BufReader::new(tokio::io::stdin()), &mut stdout).await?;

    tracing::info!("faucet mcp stdio server stopped (stdin closed)");
    Ok(())
}

/// The stdio context: without `--allow-mutations` a client's configs are
/// screened as caller-submitted bodies, since `preview` builds and reads the
/// source it is handed.
fn stdio_context(auth: crate::auth_catalog::AuthCatalog, allow_mutations: bool) -> McpContext {
    let ctx = McpContext::new(auth, allow_mutations);
    if allow_mutations {
        ctx
    } else {
        ctx.with_submitted_origin(crate::serve::load::BodyOrigin::Caller {
            allow_subprocess: false,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn catalog() -> crate::auth_catalog::AuthCatalog {
        crate::auth_catalog::build_auth_catalog(None).unwrap()
    }

    #[tokio::test]
    async fn the_read_only_server_screens_configs_it_previews() {
        let ctx = stdio_context(catalog(), false);
        let singer = "version: 1\npipeline:\n  source: { type: singer, config: { executable: /bin/sh, stream: x } }\n  sink: { type: stdout, config: {} }\n";
        let env = "version: 1\npipeline:\n  source: { type: csv, config: { path: \"${env:HOME}/x.csv\" } }\n  sink: { type: stdout, config: {} }\n";
        let file = "version: 1\npipeline:\n  source: { type: csv, config: { path: /etc/passwd } }\n  sink: { type: stdout, config: {} }\n";
        for (what, config) in [("singer", singer), ("env", env), ("file", file)] {
            for tool in ["validate_config", "preview"] {
                let out =
                    crate::mcp::tools::call_tool(&ctx, tool, &json!({ "config": config })).await;
                assert_eq!(out["isError"], true, "{what} via {tool}: {out}");
            }
        }
    }

    #[test]
    fn allow_mutations_keeps_local_trust() {
        assert!(stdio_context(catalog(), true).submitted.is_none());
        assert!(stdio_context(catalog(), false).submitted.is_some());
    }
}
