# MCP server (agent tools)

faucet can expose itself as an **MCP (Model Context Protocol) server**, so an
LLM agent (Claude Desktop / Code, or any MCP client) can *operate* faucet:
discover connectors, read their config schemas, scaffold and validate a
pipeline YAML, preview sample records, and — behind an explicit opt-in — run a
pipeline.

MCP is **not** a data connector (there is no `faucet-source-mcp`); it is a
second front-door onto the operations `faucet serve` already implements. The
MCP layer adds no pipeline capability — it re-exposes existing,
schema-introspective surfaces in the shape an agent speaks.

Build with the `mcp` feature (off by default; included in `full`):

```bash
cargo install faucet-cli --features mcp
```

## Two transports

### stdio — `faucet mcp`

For a local agent. Reads newline-delimited JSON-RPC on stdin, writes responses
on stdout (logs go to stderr):

```bash
faucet mcp                     # read-only tools
faucet mcp --allow-mutations   # also expose run_pipeline
faucet mcp --template-store sqlite:./faucet-templates.db   # + the template tools
```

Claude Desktop config (`claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "faucet": { "command": "faucet", "args": ["mcp"] }
  }
}
```

stdio is **local-trust**: there is no bearer/RBAC layer, so `run_pipeline` is
gated only by `--allow-mutations`. Do not expose it remotely — use the HTTP
transport with auth for that.

### Streamable HTTP — `faucet serve --mcp`

Mounts a `/mcp` route on the running control plane. It inherits serve's
bearer-auth + RBAC + audit — an MCP request is authenticated, authorized, and
recorded exactly like any other API call:

```bash
faucet serve --mcp --auth-token "$TOKEN"
faucet serve --mcp --mcp-allow-mutations --auth-config rbac.yaml
```

```bash
curl -s localhost:8080/mcp -H "Authorization: Bearer $TOKEN" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'
```

## Tools

Read-only tools are always available; the mutating `run_pipeline` tool appears
only when the server is started with `--allow-mutations` **and** (on HTTP) the
caller holds the `RunWrite` RBAC scope — so a `Viewer` token can never mutate,
even on a mutation-enabled server.

| Tool | Mutating? | What it does |
|------|-----------|--------------|
| `list_connectors` | no | Sources, sinks, transforms, state stores + conformance tier. |
| `get_connector_schema` | no | JSON Schema for a connector / transform config. |
| `scaffold_config` | no | A commented YAML skeleton for a source→sink pair. |
| `validate_config` | no | Full load-time validation (matrix **or** topology). |
| `preview` | no | Up to 100 sample records from the first source (source side only). |
| `run_pipeline` | **yes** | Run an inline config. Pass `dry_run: true` to validate + preview only. |
| `list_templates` | no | Registered [pipeline templates](./templates.md) and the typed params each takes. |
| `get_template` | no | One template: declared params, stored config body, and its release state (status, `stable` / `previous` / `newest`, channel pointers, launch log). |
| `register_template` | **yes** | Register a template document as a new version — `kind: source-template`, `kind: sink-template`, or `kind: pipeline`. Inert by default — pass `launch: true` to make it live. |
| `run_template` | **yes** | Run a template with given `params` / `env`, at a version or named channel (default `stable` — the launched version). A source template also takes `sink` (a registered sink template) + `sink_version`, and optionally `overlay` (a registered deployment id or an inline mapping) + `overlay_version`. `dry_run: true` materializes + validates only and reports the per-stream write-mode plan. |

The four template tools appear **only when a registry is wired** — `faucet serve
--mcp` uses its own `--history` backend; `faucet mcp` needs
`--template-store <url>`. Without one they are not advertised at all, so an agent
never sees a tool it cannot use.

Templates are the ergonomic shape for agent-driven runs: the agent discovers the
typed parameter surface with `list_templates` / `get_template` and then supplies
only the values that change, instead of composing (and possibly mis-composing) a
whole config. A `secret: true` param is echoed back as `"***"`.

Every MCP call over HTTP is written to the audit log; secret material is
redacted from any tool output.

## Protocol

A JSON-RPC 2.0 subset: `initialize`, `tools/list`, `tools/call`,
`resources/list`, `ping`. The advertised protocol version is `2024-11-05`.

```jsonc
// → initialize
{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}
// ← {"result":{"protocolVersion":"2024-11-05","serverInfo":{"name":"faucet",…}}}

// → discover + generate + check, then (with permission) run
{"jsonrpc":"2.0","id":2,"method":"tools/call",
 "params":{"name":"scaffold_config","arguments":{"source":"rest","sink":"bigquery"}}}
```

## Security model

- **Read-only by default.** `run_pipeline`, `register_template`, and
  `run_template` are absent from `tools/list` unless mutations are enabled.
- **HTTP inherits serve auth.** Bearer/RBAC + audit apply to `/mcp` as to any
  route; mutations additionally require the `RunWrite` scope.
- **`preview` is bounded** (≤100 rows) — never a full extract.
- **Secrets never leak** — tool output is run through the same redactor as the
  rest of the control plane.
