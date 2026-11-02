#![no_main]

use std::sync::LazyLock;

use faucet_cli::auth_catalog::build_auth_catalog;
use faucet_cli::mcp::protocol::JsonRpcRequest;
use faucet_cli::mcp::{McpContext, handle_message};
use libfuzzer_sys::fuzz_target;
use tokio::runtime::Runtime;

static RT: LazyLock<Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("a runtime")
});

// `tools/call` is not driven: its handlers build connectors and read the
// host's environment and files.
fuzz_target!(|raw: &str| {
    if let Ok(req) = serde_json::from_str::<JsonRpcRequest>(raw) {
        if req.method == "tools/call" {
            return;
        }
        let _ = req.is_notification();
    }
    let ctx = McpContext::new(
        build_auth_catalog(None).expect("an empty auth catalog"),
        false,
    );
    let _ = RT.block_on(handle_message(&ctx, raw));
});
