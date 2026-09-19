//! `faucet serve` — thin command layer: parse args into a `ServeConfig`, load
//! the startup `.env`, and hand off to `serve::run_server`.

use crate::cli::ServeArgs;
use crate::error::CliResult;
use crate::serve::{McpServeSettings, ServeConfig};

pub async fn run(
    args: ServeArgs,
    log_level: String,
    log_format: crate::cli::LogFormat,
) -> CliResult<()> {
    let cwd = std::env::current_dir()?;
    let env_path =
        crate::env_loader::resolve_env_file(args.env_file.as_deref(), args.no_env_file, &cwd)?;
    crate::env_loader::load_env_file_if_present(env_path.as_deref())?;

    // Capture the MCP flags before `from_args` consumes `args`. (The `/mcp`
    // route is compiled only with the `mcp` feature; in a non-`mcp` build these
    // are accepted but inert.)
    let mcp = McpServeSettings {
        enabled: args.mcp,
        allow_mutations: args.mcp_allow_mutations,
    };

    let mut config = ServeConfig::from_args(args)?;
    config.log_level = log_level;
    config.log_format = log_format;
    crate::serve::run_server(config, mcp).await
}
