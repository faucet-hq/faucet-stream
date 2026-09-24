# CLI commands

The `faucet` binary exposes these commands. Pass `--log-level <level>` (or set
`FAUCET_LOG`) to control logging, and `--log-format text|json` (or
`FAUCET_LOG_FORMAT`) to control how it is rendered.

**`--log-format json`** emits one JSON object per line on stderr, so an
orchestrator's log pipeline (Datadog, Elastic, Loki, CloudWatch, Splunk) can
ingest it without regex parsing. The span fields faucet already records —
`pipeline`, `row`, `run_id`, `connector`, `records_written`, error `kind` —
become first-class fields instead of being rendered into a message.

Under `json`, the end-of-run human status block (`…: 1 invocation, 1 ok, …`,
the per-row timing table, the peak-RSS line) is **not** printed: the same
numbers already leave as structured events, and a prose line in the middle
would break a strict consumer. `text` is the default and is byte-identical to
previous releases.

This is separate from `faucet run --output json|ndjson`, which is a
machine-readable *result* contract on **stdout**; logs are always on stderr.
For `faucet mcp`, logs stay on stderr under either format — stdout carries the
JSON-RPC stream.

| Command | What it does |
|---------|--------------|
| `faucet run [config]` | Run the pipeline(s) in a config file. |
| `faucet validate [config]` | Parse, expand, and validate a config without running it. |
| `faucet preview [config]` | Run only the source side and print records to stdout. |
| `faucet schema <target>` | Print the JSON Schema for the whole config (`config`), a connector, a transform, or any block. |
| `faucet list` | List every compiled-in source, sink, and transform with a one-line description. |
| `faucet init [name]` | Scaffold a commented config skeleton from connector schemas. |
| `faucet new connector <name> --kind <source\|sink>` | Scaffold a ready-to-build connector crate. |
| `faucet search <term>` | Search the connector registry for connectors by name/keyword. |
| `faucet install <name>` | Print how to enable/obtain a connector from the registry. |
| `faucet conformance [name]` | Score each connector against the SDK contract; print its maturity tier + capabilities. |
| `faucet plan [config]` | Read-only preview of what a config would do — zero writes. |
| `faucet dev <config> --sample <f>` | Watch + re-run a sample on save with a live diff (`cli-dev`). |
| `faucet doctor [config]` | Probe every connector (auth/network/permissions) and print a checklist. |
| `faucet test <specs…>` | Run fixture-based offline pipeline tests from one or more spec files. |
| `faucet replicate [config]` | Bulk-snapshot a table, then hand off to CDC for a gap-free mirror. |
| `faucet schedule [config]` | Run a pipeline on a cron schedule (long-running foreground process). |
| `faucet serve` | Run a long-running HTTP control plane: submit / poll / cancel pipeline runs over REST. |
| `faucet completions <shell>` | Print a shell tab-completion script (bash / zsh / fish / powershell / elvish). |
| `faucet migrate [config]` | Upgrade a config written against an older grammar to the current shape (idempotent). |
| `faucet doctor --offline [config]` | Static, credential-free config lints (no network) — dangling/unused auth, unused vars, no-op sink `batch_size`. |
| `faucet fmt [config] [--check]` | Canonicalize a config (stable key order); `--check` is a CI gate. |
| `faucet explain [config]` | Plain-English narration of what a pipeline does (offline, zero I/O). |
| `faucet history [config]` | Terminal view of the run history in a config's `catalog:` store. |
| `faucet run … --output json\|ndjson` | Machine-readable end-of-run summary (per-row + totals) for scripting. |

`[config]` is optional for `run` / `validate` / `preview` / `doctor` / `replicate` / `schedule`: if
omitted, faucet auto-discovers `faucet.yaml` → `.yml` → `.json` in the current directory.

## `run`

```bash
faucet run pipeline.yaml
faucet run                              # auto-discover faucet.yaml in cwd
faucet run --from-env                   # build the pipeline entirely from FAUCET_* env vars
faucet run pipeline.yaml --env-file prod.env
faucet run pipeline.yaml --no-env-file
faucet run pipeline.yaml --clock 2026-03-01          # backfill: set ${now.*} clock to midnight UTC
faucet run pipeline.yaml --clock 2026-03-01T02:00:00-08:00  # backfill: precise RFC 3339 timestamp
```

Flags:

| Flag | Purpose |
|------|---------|
| `--clock <value>` | Override the clock used by `${now.*}` tokens. Accepts an RFC 3339 timestamp (`2026-03-01T00:00:00Z`) or a bare date (`2026-03-01`, treated as midnight UTC). Default: process start time in UTC. Use this for backfills — run the same config with a different date without changing the file. |
| `--concurrency <n>` | Override this run's **connector** concurrency — how many concurrent connections/fetches the source and sink may use — whatever the config says. Maps onto whichever knob the connector declares (`max_connections` / `partition_concurrency` / `shard_concurrency` / `concurrency`); a connector with none ignores it. Does **not** change matrix parallelism (`execution.max_concurrent`), and it caps only the *client* side — it cannot raise what the upstream will accept. Must be > 0. |
| `--profile <name>` | Select a named overlay from the config's `profiles:` block (see [Config composition](config.md#config-composition)). Overrides `FAUCET_PROFILE`. |
| `--env-file <path>` / `--no-env-file` | Same `.env` handling as `validate` / `preview`. |
| `--from-env` | Build the pipeline entirely from `FAUCET_*` environment variables; mutually exclusive with a positional config path. |
| `--select <id>` / `--only <glob>` / `--skip <id\|glob>` | Runtime matrix-row selection by id. `--select`/`--only` force-include by name (bypassing the status gate); `--skip` removes last. See [Row selection](config.md#row-selection). Env: `FAUCET_SELECT` / `FAUCET_SKIP`. |
| `--status <tier>` | Additively widen the eligible readiness set beyond `{mandatory, active}`: `available` / `draft` / `archived`. Env: `FAUCET_STATUS`. |
| `--tag <t>` | Narrow the eligible set to rows carrying any listed tag (union). Env: `FAUCET_TAGS`. |
| `--include-parents <off\|eligible\|all>` | Parent/`depends_on` inclusion policy for a narrowed run set (default `off`). Overrides `selection.include_parents:`. Env: `FAUCET_INCLUDE_PARENTS`. |
| `--param <NAME=VALUE>` | Supply a value for a declared [`params:`](config.md#params) entry. Repeatable; coerced to the declared type. A `required` param with no value is an error naming it. |
| `--param-env <NAME[=VALUE]>` | Override an environment variable for this run's `${env:VAR}` resolution only. Bare `NAME` takes the value from the caller's environment (so a secret stays out of the process arguments). The process environment is not modified. Repeatable. |
| `--source <id\|path> --sink <id\|path>` | Template Hub: compose a `source-template` with a `sink-template` and run the result instead of loading a config file. Ids resolve under `--hub` / `$FAUCET_HUB` / `./hub`. See [`hub`](#hub). |
| `--overlay <id\|path>` | With `--source` / `--sink`: apply a `kind: deployment` overlay — state, DLQ, notifications, SLA and other operational blocks — over the composition. A path, or an id under `<hub>/deployments/`. See [Deployment overlays](../cookbook/template-hub.md#deployment-overlays). |
| `--tui` | Show a live full-screen terminal UI while the pipeline runs: per-invocation source→sink route, records in/out, records/s, errors, DLQ counts, bookmark age, and a scrolling log pane. Press `q` (or `Ctrl-C`) to cancel cooperatively — in-flight invocations stop at their next page boundary and flush their sinks. Requires a binary built with the `cli-tui` feature (`cargo install faucet-cli --features cli-tui`); on a non-TTY stdout (CI, pipes) the flag logs a notice and runs normally. When the config has an `observability.prometheus` block, the `/metrics` endpoint stays up alongside the TUI; OTLP *metrics* export is skipped under `--tui` (traces are unaffected). |
| `--quiet` | Suppress the inline live progress line. |

### Live progress line

On an interactive terminal, `faucet run` shows a lightweight inline progress
line per active matrix row — `row_id  src→sink  <in> in / <out> out  <r>/s
page <p>  <elapsed>` — updated a few times a second and drawn on **stderr** so
piped stdout stays clean for records. It is auto-disabled on a non-TTY stdout
(CI, pipes) and under `--quiet`, both of which fall back to the periodic
`tracing` progress logs; `--tui` (when built in) supersedes it. Requires the
`cli-progress` build feature, which ships in the `default` build. The numbers
come from the same in-process Prometheus recorder the TUI samples — no extra
hot-path cost.

## `validate`

Reports one line per expanded matrix row. Use it in CI to catch config errors
before deploying.

```bash
faucet validate pipeline.yaml
```

When the config contains secrets-manager directives (`${vault:…}`, `${aws-sm:…}`,
etc.), `faucet validate` resolves them as a real preflight and prints one
confirmation line per reference (never the value):

```
secret: vault:secret/data/faucet/api#token → resolved
ok: 'my-pipeline' rows=1 (roots=1, children=0) execution=(defaults)
  - default [root] source=rest sink=jsonl delivery=at-least-once
```

Each row line ends with the **derived end-to-end delivery guarantee** for that
row's source × sink × config — `at-least-once`,
`effectively-once (atomic watermark)`, or `effectively-once (keyed upsert)` —
computed regardless of the requested `delivery:` mode, so an upsert-keyed row
is reported as effectively-once even without `delivery: exactly_once`. See
[`delivery`](./config.md#delivery).

Pass `--no-secrets` to validate grammar and structure only, skipping all secret
fetches. This is useful in CI environments that lack credentials, or in local
development before vault access is available:

```bash
faucet validate --no-secrets pipeline.yaml
```

A config declaring [`params:`](config.md#params) reports its trigger surface and
validates against **type-shaped placeholders** for any `required` param, so a
parameterized config passes CI without inventing values:

```
params: 4 declared (required: api_token, tenant_id) — validated against placeholders; pass --param NAME=VALUE to bind for real
```

Pass `--param NAME=VALUE` (repeatable) to switch to strict binding and check one
concrete invocation; `--param-env NAME[=VALUE]` overrides an environment variable
for the validation only.

### JSON output

Pass `--json` to emit a structured summary instead of the prose report, so CI can
assert on it programmatically. The prose lines (secret confirmations, per-block
`valid` notes, the `ok:`/row lines) are suppressed and a single JSON object is
printed:

```bash
faucet validate pipeline.yaml --json
```

```json
{
  "valid": true,
  "mode": "matrix",
  "name": "my-pipeline",
  "row_count": 1,
  "roots": 1,
  "children": 0,
  "selection_active": false,
  "rows": [
    {
      "id": "default",
      "source": "rest",
      "sink": "jsonl",
      "role": "root",
      "parent_id": null,
      "parent_key": null,
      "depends_on": [],
      "delivery": "at-least-once",
      "status": "active",
      "tags": [],
      "decision": null
    }
  ]
}
```

Each row's `decision` is `"run"`/`"skip"` when a selector or the readiness ladder
is active, otherwise `null`. A topology-mode config emits `"mode": "topology"`
with `nodes`/`edges` counts and any inert-block `warnings`.

### Composition flags

When a config uses [composition](config.md#config-composition) (`extends:` /
`profiles:` / `!include`), `validate` resolves it like `run` does:

```bash
faucet validate app.yaml --profile prod        # select a named overlay
faucet validate app.yaml --show-composed       # print the fully merged config
```

- `--profile <name>` selects a named overlay from `profiles:` (also settable via
  `FAUCET_PROFILE`; the flag wins). An undeclared name is a clear load-time error.
- `--show-composed` prints the fully composed document — bases merged, the
  selected profile applied, `!include` fragments substituted, and the
  `extends:` / `profiles:` metadata stripped — *before* `${...}` interpolation.
  It's the fastest way to confirm a multi-file setup resolves to what you expect.

`validate` accepts the same [row-selection](config.md#row-selection) flags as `run`
(`--select`/`--only`/`--skip`/`--status`/`--tag`/`--include-parents`). When the config
uses the readiness ladder or tags — or a selector is passed — it prints a run-selection
report listing each row's resolved `status`, its tags, and whether the selection would
`RUN` or `skip` it, and surfaces selection errors (empty run set, missing ancestor,
unknown token) in CI without a run.

## `discover`

```bash
faucet discover conn.yaml                      # print a generated config to stdout
faucet discover conn.yaml -o pipeline.yaml     # write it to a file (--force to overwrite)
faucet discover conn.yaml --include 'public.*' --exclude '*.tmp_*'
faucet discover conn.yaml --source warehouse   # introspect a named pipeline.sources template
faucet discover conn.yaml --json               # machine-readable dataset list
```

Connects to the config's source, enumerates the datasets behind it (tables /
collections / indices / object-store prefixes), and emits a ready-to-run config
with **one matrix row per dataset** — the input document with its `matrix:`
block replaced, secrets echoed as raw `${…}` references. The generated config
passes `faucet validate`. Supported sources: `postgres`, `mysql`, `mssql`,
`sqlite`, `mongodb`, `elasticsearch`, `bigquery`, `snowflake`, `s3`, `gcs`.

| Flag | Purpose |
|------|---------|
| `--source <name>` | Which `pipeline.sources` template to introspect (default `default`, the singular `pipeline.source`). |
| `--include <glob>` / `--exclude <glob>` | Repeatable `*`-wildcard filters on dataset names (no includes = everything; excludes win). |
| `-o, --output <file>` / `--force` | Write the generated config to a file instead of stdout; `--force` overwrites. |
| `--json` | Emit the discovered `DatasetDescriptor` list as JSON instead of a config. |
| `--profile` / `--env-file` / `--no-env-file` | Same semantics as `run` / `validate`. |

See the [source discovery cookbook](../cookbook/discover.md).

## `preview`

Runs the first root row's source and prints records (via the stdout sink).
Children aren't previewed because they need parent records to resolve
`${parent.path}` tokens.

```bash
faucet preview pipeline.yaml --limit 10
faucet preview app.yaml --profile dev --limit 5   # preview with a named profile overlay
```

`--profile <name>` / `FAUCET_PROFILE` selects a named overlay from `profiles:` before
previewing. Same semantics as `run` and `validate`.

## `plan`

A read-only "what would this config change" preview — it runs the sink's
non-mutating `check()` probe and pure schema/lineage analysis but **never writes
to any sink**.

```bash
faucet plan pipeline.yaml
faucet plan pipeline.yaml --sample fixtures.jsonl        # preview output schema/volume offline
faucet plan pipeline.yaml --live --limit 20 --json       # capped read-only source pull, JSON out
faucet plan pipeline.yaml --diff                         # config-change diff vs the last run
faucet plan pipeline.yaml --diff --json                  # machine-readable diff (CI gate)
```

Reports, for the selected row (`--row`, default the first root): the resolved
source/sink/write-mode/delivery guarantee, the transform chain in lifecycle
order, which quality/contract/masking/drift policies are in effect, and the
lineage column ops. Given a sample (`--sample <fixture>` offline, or `--live
--limit N` for a capped read-only source pull), it also reports the inferred
output schema, the sink schema delta (adds / widenings / incompatible via
`diff_schema` when the sink exposes `current_schema()`; "schemaless — no delta"
otherwise), and a volume estimate. The data pass runs through the offline
harness, so no sink is ever written. Offline by default; `--resolve-secrets`
opts into the real secrets path.

### `plan --diff` — config-change preview (#374)

A `terraform plan`-style diff of the current config against **what last ran**.
On every successful `faucet run` / `replicate` / `schedule --once`, a redacted
snapshot of the *resolved + expanded* config is recorded into the catalog store
(best-effort — recording never fails a run). `faucet plan --diff` re-expands the
current config, loads the last snapshot, and renders a per-row semantic diff:

```text
Pipeline: hibob   (last run 2026-07-18 09:12 UTC)

  + people             NEW ROW — will be created
  ~ payroll            CHANGED
      source.config.page_size      100  ->  500
      source.config.path           /v1/pay  ->  /v1/payroll
  ~ timeoff            CHANGED
      source.config.token          (secret rotated)
  - benefits           REMOVED — no longer in the run set
  = employees          unchanged

Summary: 1 to create, 2 to change, 1 removed, 1 unchanged.
```

Because the diff operates on the **resolved + expanded** model, a one-line
`${vars.x}` edit that fans out across many rows shows up as the real per-row
effect, and two textually-different files that resolve to the same movement show
no diff. Requires a `catalog:` block (`faucet schema catalog`) and the `catalog`
build feature. `--diff` resolves secrets so the diff matches what `run` recorded;
every secret-sourced value is stored only as a stable `<secret:sha256:…>` token,
so a rotated credential surfaces as "secret rotated" and no secret is ever
persisted. On a first run (nothing recorded yet) every row is reported as new.

## `dev`

A watch-and-diff authoring loop (requires the `cli-dev` build feature). Re-runs
a sample through the offline harness on every config save and prints the schema,
DLQ count, errors, and a **diff vs the previous run**.

```bash
faucet dev pipeline.yaml --sample fixtures.jsonl
```

Watches the config file's directory and the directories of any `extends:` /
`!include` fragments, so editing an included fragment re-triggers a run. In a
non-TTY (CI) or with `--once` it runs a single pass and exits. Debounce the
watcher with `--debounce-ms`.

## `schema`

```bash
faucet schema --list          # enumerate every valid target in this binary
faucet schema config          # the WHOLE config document (top-level grammar)
faucet schema source rest
faucet schema sink bigquery
faucet schema transform keys_case
faucet schema dlq
faucet schema execution
faucet schema contract
faucet schema masking
faucet schema sla
faucet schema notifications
faucet schema secrets
faucet schema triggers
faucet schema catalog
faucet schema params
faucet schema partition
```

`faucet schema --list` prints every valid `<target>` compiled into this binary
(feature-gated targets appear only when their feature is on), so you can discover
the set without reading the docs — `source`, `sink`, and `transform` are shown
with a `<name>` placeholder because they take a connector/transform name.

`faucet schema config` prints a composed JSON Schema for the **entire**
`faucet.yaml` / `faucet.json` document — the top-level grammar (`version`,
`name`, `vars`, `auth`, `pipeline`, `matrix`, `execution`, and every optional
block such as `schedule` / `lineage` / `quality` / `dlq` / `resilience` that is
compiled into your binary) plus per-connector `type` discrimination: the
`source` / `sink` positions become a `oneOf` over the connector kinds your
binary knows, each branch embedding that connector's own config schema. Point an
editor at it for autocomplete and validation as you type — see
[Editor setup](./editor-setup.md).

`faucet schema transform <name>` prints the inline config schema for a
transform (e.g. `keys_case` lists the valid `mode:` values). Run
`faucet list` to see which transforms are compiled into your binary.

`faucet schema execution` prints the schema for the top-level `execution:`
block, including concurrency, error handling, and adaptive batch sizing.

`faucet schema masking` prints the JSON Schema for the `pipeline.masking:`
(PII detection + column-masking) block — see [masking](../cookbook/masking.md).

`faucet schema sla` prints the schema for the top-level `sla:`
(freshness/volume SLA) block — see [SLA monitoring](../cookbook/sla.md).

`faucet schema params` prints the schema for **one entry** of the top-level
`params:` (typed run parameters) block — see
[Parameters & pipeline templates](../cookbook/templates.md).

`faucet schema secrets` prints the directive grammar and auth requirements for
all four secrets-manager backends in machine-readable JSON — useful for tooling
that needs to understand the interpolation syntax without reading the docs.

`faucet schema triggers` prints the JSON Schema for the `--triggers` file format
(the `TriggersFile` / `TriggerSpec` / `TriggerKind` types). Requires the
`triggers` Cargo feature.

`faucet schema catalog` prints the JSON Schema for the top-level `catalog:`
(Data Movement Catalog store) block — see
[the catalog cookbook](../cookbook/catalog.md). Requires the `catalog` Cargo
feature.

## `init`

```bash
faucet init my_pipeline --source postgres --sink bigquery
```

Required fields are surfaced with a typed placeholder and a `# REQUIRED` marker;
optional fields are commented out so connector defaults apply. The interactive
mode (`--interactive`) is gated behind the `cli-interactive` feature.

**Singer discovery.** For the [Singer bridge](connectors.md) source, add
`--discover --executable <tap>` to run the tap's `--discover`, write the returned
catalog to `catalog.json`, and scaffold a config that inlines the catalog and
lists the discovered streams (with `stream:` left empty for you to choose):

```bash
faucet init --source singer --discover --executable tap-github -o pipeline.yaml
```

`faucet doctor` then verifies the tap resolves on `PATH` and that the selected
`stream` exists in the catalog.

## `new`

Scaffold a new **connector crate** (not a config) that follows every repo
convention — ready to `cargo build` and publish:

```bash
faucet new connector acme --kind source            # → faucet-source-acme/
faucet new connector acme --kind sink --common      # + a faucet-common-acme/ crate
faucet new connector acme --kind source -o crates/  # write into crates/
```

The generated crate has the standard module layout (`config.rs`, `stream.rs` or
`sink.rs`), a `JsonSchema`-deriving config, `config_schema()` / `connector_name()`
overrides, the `#![cfg_attr(docsrs, feature(doc_cfg))]` crate-root line, the
`[package.metadata.docs.rs]` block, system-name-first crates.io keywords, a
README, and a passing unit test — so `cargo test` is green out of the box with a
trivial passthrough. Fill in the `TODO`s, then publish. See
[Authoring a connector](../extending/authoring-connectors.md).

## `search` / `install` / `list --available`

Discover connectors from the [connector registry](../extending/marketplace.md) —
a curated, feature-independent index of every built-in connector plus community
`faucet-source-*` / `faucet-sink-*` crates.

```bash
faucet search kafka              # matches on name, description, keywords, crate
faucet search cdc --json         # machine-readable
faucet list --available          # the whole registry; ● = in this binary, ○ = installable
faucet install bigquery --kind sink
faucet install my-connector --index ./my-registry.json
```

`faucet install <name>` never runs anything — it prints the recipe:

- a **built-in** already compiled in → "already available";
- a **built-in** not compiled in → `cargo install faucet-cli --features <kind>-<name>`;
- a **community** connector → a copy-pasteable custom-binary snippet (see
  [Custom binaries](../../cli/README.md#custom-binaries-with-third-party-connectors)).

`--index <path>` points any of these at a custom/mirror index instead of the
built-in one. Ambiguous names (a connector that is both a source and a sink,
e.g. `postgres`) need `--kind source|sink`.

Both `faucet list` and `faucet list --available` accept `--json` for a
machine-readable listing — `list --json` emits `{ sources, sinks, transforms,
state_stores }` (each connector entry carries `name`, `description`, and a
maturity `tier`), and `list --available --json` emits the registry rows with a
`compiled` flag per connector.

## `conformance`

Score every compiled-in connector against the faucet SDK contract and print its
**maturity tier** — 🟢 `Stable`, 🟡 `Experimental`, 🟠 `Beta`, ⚪ `Draft` — plus its
capability badges (exactly-once, discover, upsert, schema-evolution).

```bash
faucet conformance                     # score every connector, highest first
faucet conformance --all               # same; explicit form for CI
faucet conformance --kind sink         # sinks only
faucet conformance postgres            # a detailed scorecard (+ badge URL) for one connector
faucet conformance --json              # machine-readable scorecards
faucet conformance --min-tier stable   # CI gate: non-zero exit if any connector is below Stable
```

The score (0–100) is computed from authoritative, instantiation-free signals: a
verified `cli/connectors/registry.json` entry (40) + a real config schema (30)
form the `Stable` gate at 70; documentation, exactly-once delivery, and the
kind-specific capability (source discovery / sink upsert + schema evolution) are
bonuses on top. Every conforming built-in is `Stable` with capability badges; an
incomplete third-party connector (missing a verified entry or a schema) lands at
`Experimental` / `Beta`.

`--min-tier <tier>` turns the report into an **opt-in CI gate**: the command
exits non-zero if any scored connector is below the named tier — combine it with
`--kind` / a `NAME` to scope the gate. A single-connector scorecard also prints a
shields.io **badge URL** third-party authors can drop into their crate README.
The per-connector tier is mirrored in `cli/connectors/registry.json` (validated
against this score in CI) and shown in `faucet list` and the
[connector conformance & tiers](./conformance.md) page.

## `doctor`

```bash
faucet doctor pipeline.yaml                  # checklist; exit code = # of failed probes
faucet doctor pipeline.yaml --timeout-secs 5 # per-probe timeout (default 10)
faucet doctor pipeline.yaml --json           # machine-readable, for CI gating
faucet doctor app.yaml --profile prod        # probe with a named profile overlay applied
```

Runs a fast, **non-mutating** preflight against every connector in the config so
misconfiguration surfaces before a real run. For each root invocation it probes
the source, sink, and state store and prints a green/red checklist with elapsed
times; the **exit code equals the number of failed probes** (clamped to 255).

- **Sources** reuse the real read path — the probe pulls a single page and stops
  (never the full dataset). Sources whose first page would block or mutate use a
  targeted probe instead: `webhook` (port bindable), `websocket` (TCP connect),
  `postgres-cdc` (slot reachable), `kafka` (cluster metadata).
- **Sinks** run a read-only connect/auth/metadata call — `SELECT 1`, `HeadBucket`,
  `PING`, `tables.get`, cluster health, `fetch_metadata`, or a directory-writable
  check for file sinks. Never a real write.
- **State stores** do a sentinel `put`/`get`/`delete` that leaves no residue.
- **SLA** (when a top-level [`sla:` block](config.md#sla) is configured) reads the
  persisted run history and reports staleness of the last successful run vs
  `max_staleness_secs` and volume-baseline warm-up state — read-only.

Child invocations (parent/child matrix rows) are listed but not probed — their
configs depend on parent records that only exist at run time. Probe messages are
scrubbed for resolved secrets before printing.

`--profile <name>` / `FAUCET_PROFILE` selects a named overlay from `profiles:` before
probing (same semantics as `run` and `validate`).

See the [Troubleshooting](../cookbook/troubleshooting.md) cookbook page for
reading the output and common failures.

## `test`

```bash
faucet test tests/*.yaml                    # run every case; exit code = # of failed cases
faucet test tests/orders.yaml --filter null # only cases whose name contains "null"
faucet test tests/*.yaml --json             # machine-readable { total, passed, failed, tests }
faucet test tests/*.yaml --clock 2026-03-01 # default ${now.*} clock for cases without clock:
```

Runs fixture-based, **fully-offline** pipeline tests. Each case in a spec file
feeds sample records through the real transform → quality → contract path with
an in-memory source, sink, and DLQ — the configured source and sink are never
built or contacted — and asserts the output records, DLQ routing, counts, or an
expected failure. The **exit code equals the number of failed cases** (clamped
to 255), so CI gates on it directly.

Flags:

| Flag | Purpose |
|------|---------|
| `--filter <substring>` | Run only cases whose name contains the substring. |
| `--json` | Emit the JSON report instead of the human checklist. |
| `--clock <value>` | Default `${now.*}` clock for cases without `clock:` (RFC 3339 or `YYYY-MM-DD`). |
| `--profile <name>` | Profile overlay applied to referenced configs (same semantics as `run`). |
| `--resolve-secrets` | Resolve secrets-manager directives in referenced configs. Default: offline, directives stay unresolved. |
| `--env-file <path>` / `--no-env-file` | Same `.env` handling as `run` / `validate`. |

`faucet schema test` prints the spec file's JSON Schema. See the
[Testing pipelines](../cookbook/testing.md) cookbook page for the spec grammar,
matching semantics, and a CI recipe.

## `dlq`

Inspect, replay, and discard the dead-letter-queue envelopes a pipeline's `dlq:`
sink wrote. A DLQ *location* is a local `.jsonl` file, a directory of `*.jsonl`
files, or a glob.

```bash
faucet dlq inspect ./dlq/breaches.jsonl                          # breakdown + sample
faucet dlq replay pipeline.yaml --from ./dlq/breaches.jsonl --dry-run
faucet dlq replay pipeline.yaml --from ./dlq/breaches.jsonl      # re-feed through the pipeline
faucet dlq discard ./dlq/breaches.jsonl --reason contract --before 7d
```

**`faucet dlq inspect <location>`** — group envelopes by reason and error kind
with a sample.

| Flag | Effect |
|------|--------|
| `--reason <r>` | Only include envelopes with this reason (`partial` / `dlq_all` / `quality` / `schema_drift` / `contract`). |
| `--limit <n>` | Sample size. Default: 5. |
| `--encryption-key <k>` | Key for a DLQ sealed at rest by the jsonl sink's `encryption` block; repeat for rotated keys. Sealed lines without a matching key are counted as *encrypted*, never mistaken for malformed. Requires an `encryption`-feature build. |
| `--json` | Emit a JSON summary. |

**`faucet dlq replay <config> --from <location>`** — re-feed the quarantined
payloads through the config's transforms → quality → contract → sink. Rows that
fail again go to a *fresh* DLQ, never back to the source.

| Flag | Effect |
|------|--------|
| `--from <location>` | DLQ location to replay from (required). |
| `--reason <r>` | Replay only envelopes with this reason. |
| `--encryption-key <k>` | Key for a sealed DLQ (repeatable). When omitted, the config's own `dlq:` jsonl `encryption` block is used automatically. |
| `--failed-dlq <path>` | Where re-failed rows go. Default: a `replay-failed.jsonl` sibling of the source. |
| `--row <id>` | Which root of the config to replay through. Default: the first root. |
| `--dry-run` | Report what would be replayed without writing. |
| `--json` | Emit a JSON result. |
| `--env-file <path>` / `--no-env-file` / `--profile <name>` | Same config-load handling as `run`. |

**`faucet dlq discard <location>`** — remove processed envelopes.

| Flag | Effect |
|------|--------|
| `--reason <r>` | Only discard envelopes with this reason. |
| `--before <when>` | Only discard envelopes older than an RFC 3339 timestamp or a relative age (`7d` / `24h` / `30m`). |
| `--delete` | Permanently delete instead of archiving to a `<file>.archived.jsonl` sibling. |
| `--encryption-key <k>` | Key for a sealed DLQ (repeatable). Kept/archived lines stay sealed verbatim; decryption happens only in memory for filtering. |
| `--json` | Emit a JSON result. |

See the [Dead-letter queues](../cookbook/dlq.md) cookbook page for the envelope
shape and the inspect → fix → replay → discard workflow.

## `contract`

```bash
faucet contract pipeline.yaml                       # validate + human summary
faucet contract pipeline.yaml --export contract     # canonical contract JSON
faucet contract pipeline.yaml --export json-schema  # standalone JSON Schema
faucet contract pipeline.yaml --export openlineage  # OpenLineage schema facet
```

Validates the config's `pipeline.contract:` block (a malformed contract exits
non-zero with the compile error) and prints a summary of the promised fields,
constraints, and breach policy — or, with `--export`, a machine-readable
artifact for downstream consumers. Offline-safe: secrets are never fetched.
Requires the `contract` Cargo feature (in the default build). See the
[Data contracts](../cookbook/contracts.md) cookbook page.

## `masking`

```bash
faucet masking pipeline.yaml     # validate + per-destination rule breakdown
faucet masking                   # auto-discover faucet.yaml in cwd
```

Validates the config's `pipeline.masking:` block (a malformed policy exits
non-zero with the compile error) and prints, per destination sink, which rules
apply — the fast way to confirm `applies_to` scoping. Offline-safe: secrets are
never fetched. Requires the `masking` Cargo feature (in the default build). See
the [masking](../cookbook/masking.md) cookbook page.

## `catalog`

*(requires the `catalog` build feature — included in `full`)*

```bash
faucet catalog datasets --config pipeline.yaml                 # list catalogued datasets
faucet catalog datasets --config pipeline.yaml --kind csv --q users --json
faucet catalog show 3f2a9c1e0b7d4a55 --config pipeline.yaml    # detail (id prefix ok)
faucet catalog lineage --config pipeline.yaml --root 3f2a9c1e0b7d4a55 --depth 3
```

Browses the [Data Movement Catalog](../cookbook/catalog.md) named by the
config's `catalog:` block: the dataset list (newest activity first, `--kind` /
`--q` filters), one dataset's detail (schema timeline with diffs, recent
volume, upstream/downstream edges), and the lineage graph. All subcommands
accept `--json`; `--config` auto-discovers `faucet.yaml` in cwd when omitted.
Read-only — it never mutates the store.

## `template`

*(requires the `templates` build feature — included in `full`)*

```bash
faucet template register  tenant-sync.yaml --store sqlite:./faucet-templates.db
faucet template register  tenant-sync.yaml --id tenant-sync --tag dev --description "per-tenant events"
faucet template register  tenant-sync.yaml --launch            # register AND make live
faucet template register  hub/source-templates/acme/billing.yaml --launch   # kind: source-template → id = its hub id
faucet template register  hub/sink-templates/faucet-hq/bigquery.yaml --launch         # kind: sink-template
faucet template register  ops/prod.yaml --launch               # kind: deployment
faucet template list      --store sqlite:./faucet-templates.db
faucet template list      --kind sink-template                  # one kind only
faucet template show      tenant-sync --store sqlite:./faucet-templates.db --version 2
faucet template promote   tenant-sync --tag prod --version dev  # move an environment channel
faucet template launch    tenant-sync --version pre-prod        # move `stable` (the release lever)
faucet template rollback  tenant-sync                           # re-launch `previous`
faucet template deprecate tenant-sync --reason "superseded"      # retire (`--undo` revives)
faucet template run       tenant-sync --store sqlite:./faucet-templates.db \
  --version prod --param tenant_id=acme --param-env API_HOST=eu.example.com
faucet template run       acme/billing --sink faucet-hq/bigquery --sink-version stable \
  --param api_token="$TOKEN" --param bq_project=my-project    # source × sink, composed at run time
faucet template run       acme/billing --sink faucet-hq/bigquery --overlay prod \
  --param state_dsn="$STATE_DSN"                              # + a deployment overlay
faucet template delete    tenant-sync --store sqlite:./faucet-templates.db --version 1
faucet template test      suite.yaml                            # suite names a config path — no registry
faucet template test      suite.yaml --store sqlite:./faucet-templates.db --select prod
faucet template sync      --store sqlite:./faucet-templates.db --config sync.yaml --dry-run   # pull remote origins (RFC 0006)
faucet template sync      --store sqlite:./faucet-templates.db --config sync.yaml --origin platform
faucet template publish   platform-nightly --store sqlite:./faucet-templates.db --config sync.yaml --origin platform
```

Register a template **once**, then trigger runs by id — the register-once /
trigger-by-id model. The registry holds four kinds of document, told apart by
their `kind:` line: a **`source-template`** (one system — connector, shared
transforms, streams with write preferences), a **`sink-template`** (one
destination), a **`deployment`** (the operational blocks — state, DLQ,
notifications, SLA — overlaid on a composed run with `--overlay`), and a
complete **`pipeline`**. A source template is run with
`--sink <id>` and composes with that registered sink template at run time (the
[Template Hub](../cookbook/template-hub.md) model); a pipeline runs alone; a sink
template is never run on its own. A document without `kind:` still registers
as a pipeline but prints a deprecation notice — add `kind: pipeline`. See the
[Parameters & pipeline templates](../cookbook/templates.md) cookbook page.

| Flag | Purpose |
|------|---------|
| `--store <url>` | Registry location: `sqlite:<path>`, a `postgres://…` URL, or `memory`. Same grammar as `catalog.url` and `faucet serve --history` — point `serve` at the same URL to trigger these templates over HTTP/MCP. Env: `FAUCET_TEMPLATE_STORE`. SQL stores need `serve-history-sqlite` / `serve-history-postgres`. |
| `--id <slug>` | *(register)* Registry id (`^[a-z0-9][a-z0-9_-]*$`). Derived from the config's `name:` when omitted. A source / sink template is always registered under its own `name` — an explicit `--id` must match it. |
| `--kind <source-template\|sink-template\|deployment\|pipeline>` | *(list)* Show only templates of one kind. `list` prints a KIND column either way. |
| `--sink <id>` | *(run)* For a source template: the registered sink template to compose with. Required for a source template; refused for a pipeline. |
| `--sink-version <n\|channel>` | *(run)* Version of the sink template. Default `stable`. |
| `--overlay <id\|path>` | *(run)* A deployment overlay for the composed run: a registered `kind: deployment` id, or a path to a deployment file. |
| `--overlay-version <n\|channel>` | *(run)* Version of a registered overlay. Default `stable`. |
| `--description <text>` | *(register)* Shown by `list` / `show`. Carried forward from the previous version when omitted. |
| `--launch` | *(register)* Launch the new version immediately, making it `stable`. Off by default — registering a build must never move existing callers. |
| `--tag <channel>` | *(register)* Point an assignable channel at the new version; repeatable. *(promote)* The channel to move. One of the closed set: `dev`, `test`, `staging`, `pre-prod`, `canary`, `prod`. The derived channels (`stable`, `previous`, `newest`) cannot be assigned — `stable` moves only via `launch`. |
| `--version <n\|channel>` | *(show / run / delete / promote / launch)* Version selector: an exact number or a channel name. Defaults to `stable` for `show`/`run`/`promote` and to `newest` for `launch`. For `delete`, omitting it removes **every** version; giving one removes just that version. For `promote`, it is the *target* — `--tag prod --version dev` copies whatever `dev` names today. |
| `--reason <text>` | *(deprecate)* Why the template is being retired; surfaced to anyone who triggers it. |
| `--undo` | *(deprecate)* Revive instead of retire. |
| `--param <NAME=VALUE>` | *(run)* Supply a declared param. Repeatable. |
| `--param-env <NAME[=VALUE]>` | *(run)* Override an environment variable for this materialization only. Repeatable. |
| `--limit <n>` | *(run)* Stop after writing this many records. |
| `--suite <path>` | *(test)* Positional: the suite file (YAML or JSON). `faucet schema template-test` prints its schema. |
| `--select <n\|channel>` | *(test)* Override the suite's own `select:`. Ignored when the suite's `template:` is a path. A suite for a source template names its sink under `sink:` (a registered id, or a path when `template:` is a path) and `sink_select:`; every case then exercises the composed pipeline. |
| `--filter <pattern>` | *(test)* Run only cases whose name matches; `*` wildcards, otherwise an exact match. |
| `--config <path>` | *(sync / publish)* The sync file naming the remote origins — the same file `faucet serve --templates-sync` takes. `faucet schema templates-sync` prints its schema. Requires the `templates-sync` feature. |
| `--origin <name>` | *(sync)* Pull only this origin (default: all). *(publish)* The origin to write to (required). |
| `--dry-run` | *(sync)* Plan and print what would change without touching the registry. *(run)* Materialize and validate without writing to any sink. |
| `--json` | Machine-readable output for every subcommand. |

Every `register` appends a new **numeric version** (auto-incrementing from 1) and
**does not move existing callers** — the 20 most recent per id are kept. Making a
version live is the separate `launch` step, so a template is `draft` until
something is launched, then `launched`, and `deprecated` once retired (a
deprecated template keeps serving pinned and `stable` callers, but every trigger
warns; `delete` is the hard stop).

On top of the numbers sits a **closed set of channels**. Three are **derived** and
never assignable: `stable` (the launched version — the default when no
`--version` is given), `previous` (the rollback target), and `newest` (the build
tip). Six are **assignable** with `promote`: `dev`, `test`, `staging`,
`pre-prod`, `canary`, `prod`. There is deliberately no `latest` — it means both
"newest build" and "current release", so it is rejected with a message naming
`stable` and `newest`. An unknown channel name is rejected with the valid list;
deleting a version drops any channel and launch-log entry aimed at it.

Promote a version up the channels as it earns trust, then `launch` it when it
should become what unpinned callers get; `rollback` re-launches `previous`.
`list` shows each template's status, live version, and build tip; `show` prints
every version with the channels pointing at it plus the launch history.
`faucet template show <id> --clean` instead prints **only** the pure template
config — comments stripped, re-emitted as canonical YAML — so it pipes cleanly to
a file (`… --clean > template.yaml`); `${param.…}` placeholders are preserved.
`faucet template run` executes through the identical path as `faucet run`, so
observability, lineage, notifications, the catalog, and SLA evaluation all behave
the same. The stored body is verbatim — `${env:…}` / `${vault:…}` resolve at
trigger time, never at registration. For a source template, `run --sink <id>`
composes the two registered documents (params merged, each stream's write mode
resolved against the sink's capabilities) and prints the per-stream plan before
running; the composed pipeline is named after the source template, so its state
keys survive swapping the sink.

`faucet template test` sweeps a template's **parameter space** offline: each case
materializes the template exactly as a real trigger would, then expands it and
compiles each row's transform chain (or validates the graph, in topology mode).
No network, no data, no sink. Cases come from three places — hand-written
`cases:`, a generated `combine:` product (with `exclude:` and an all-pairs
`pairwise:` reduction), and `auto:` cases derived from the template's own
`params:` — and a `behavioral:` block runs fixture records through the real
pipeline with `faucet test`'s matchers. When the suite's `template:` names a
readable config path, no registry is involved at all, so a template can be tested
before it is ever registered. The exit code is the failed-case count, mirroring
`faucet test`. See
[Testing the parameter space](../cookbook/templates.md#testing-the-parameter-space).

## `hub`

```bash
faucet hub list      [--hub ./hub] [--sort name|stars|updated] [--json]
faucet hub check     --source faucet-hq/example-rest-api --sink faucet-hq/bigquery [--overlay ops/prod.yaml]  # per-stream write modes; exit≠0 if incompatible
faucet hub compose   --source faucet-hq/example-rest-api --sink faucet-hq/sqlite [--overlay ops/prod.yaml] --out my-pipeline.yaml
faucet hub matrix    --format table|markdown|json [--out FILE]
faucet hub lint      [--hub ./hub] [FILE…]                   # publishability lint
faucet run           --source faucet-hq/example-csv --sink faucet-hq/jsonl                   # runs offline
faucet validate      --source faucet-hq/example-rest-api --sink faucet-hq/bigquery [--show-composed]
faucet schema source-template | sink-template | deployment
```

The Template Hub composes a `kind: source-template` (one system, its shaping,
its streams and their write preferences) with a `kind: sink-template` (one
destination and its `per_stream` addressing) into an ordinary pipeline config
at run time. See the [Template Hub cookbook](../cookbook/template-hub.md) and
the generated [source × sink matrix](./template-hub-matrix.md).

| Flag | Purpose |
|------|---------|
| `--source <id\|path>` / `--sink <id\|path>` | The pairing. A path is used as-is; an id resolves to `<hub>/source-templates/<id>.yaml` / `<hub>/sink-templates/<id>.yaml`, where `id` is `owner/name` (a community template under `source-templates/<owner>/`) or a bare `name` (shorthand for the official `faucet-hq/name`). An optional `@stable` (default) / `@newest` / `@N` selects a catalog version when the hub's `index.json` records history. |
| `--hub <dir\|github:owner/repo[@ref][/path]\|URL>` | Where ids resolve. A directory, or a GitHub repository laid out like `hub/` (`github:faucet-hq/template-hub`, `github:acme/catalog@v2/hub`, `https://github.com/acme/catalog/tree/main/hub`), fetched through the GitHub contents API and cached under `~/.cache/faucet/hub/` pinned to the ref's commit — one request per run when unchanged, the cached snapshot with a warning when offline (`FAUCET_HUB_OFFLINE=1` skips the network). `GITHUB_TOKEN` is used when set. Default: `$FAUCET_HUB`, else `./hub` when it exists, else the public hub `github:faucet-hq/template-hub`. |
| `--sort name\|stars\|updated` | *(list)* Order by id, by stars (most starred first), or by the newest version's date. Stars, dates and open issues come from the catalog's `index.json` (`trust`) and are shown as columns; `--json` includes each entry's `trust` block. |
| `--overlay <id\|path>` | *(compose / check, and `run` / `validate`)* A `kind: deployment` overlay applied over the pairing: a path, or an id under `<hub>/deployments/`. `check` also verifies every stream it names exists; `lint` accepts deployment files and flags literal credentials. |
| `--out <file>` | *(compose / matrix)* Write to a file instead of stdout. |
| `--format table\|markdown\|json` | *(matrix)* Terminal table, the docs page, or `index.json`. |
| `--json` | Machine-readable output. |

`check` and `lint` exit non-zero on any incompatible stream / finding, so both
gate a catalog in CI.

## `notify`

*(requires the `notify` build feature)*

```bash
faucet notify test pipeline.yaml --event run_failure
faucet notify test --event circuit_open        # auto-discover faucet.yaml
```

Fires one **synthetic** event through the config's `notifications:` rules using
the real delivery path (no pipeline runs) — the fast way to confirm a Slack /
PagerDuty / webhook channel is wired correctly. `--event` accepts any event
kind (`run_failure`, `run_success`, `sla_breach`, `circuit_open`,
`contract_abort`, `dlq_threshold`, `scheduler_stuck`). See the
[Notifications](../cookbook/notifications.md) cookbook page.

## `replicate`

```bash
faucet replicate pipeline.yaml                 # bulk snapshot, then stream CDC; Ctrl-C to stop
faucet replicate                               # auto-discover faucet.yaml in cwd
faucet replicate pipeline.yaml --env-file prod.env
faucet replicate pipeline.yaml --no-env-file
faucet replicate app.yaml --profile prod       # apply a named profile overlay
```

Bulk-snapshots a database table and then hands off to **change-data-capture from
a position captured *before* the snapshot**, producing a true mirror (no gap, no
duplicate rows) when paired with `write_mode: upsert`. The config must contain a
top-level `replication:` block (see [config reference](config.md#replication));
`faucet run` ignores that block, exactly as it ignores `schedule:`.

It runs two phases in order:

1. **Bulk snapshot** — the `replication.snapshot.source` (a non-CDC query reader)
   back-fills the destination through the same sink and pipeline-level transforms.
2. **CDC handoff** — the `pipeline.source` CDC connector streams every change
   committed after the captured position over the snapshot baseline.

When `replication.continuous` is `true` (the default) the CDC phase is a
**long-running foreground process** — stop it with Ctrl-C or SIGTERM; the
in-flight page flushes at the next page boundary before the process exits. With
`continuous: false` it drains CDC once and exits. A [durable state
backend](../cookbook/state.md#state-stores) (`file` / `redis` / `postgres`, not
`memory`) is required so an interrupted run resumes correctly.

Flags:

| Flag | Purpose |
|------|---------|
| `--profile <name>` | Select a named overlay from `profiles:` (also settable via `FAUCET_PROFILE`; the flag wins). Same semantics as `run` / `validate`. |
| `--env-file <path>` / `--no-env-file` | Same `.env` handling as `run` / `validate`. |

See the [replication cookbook](../cookbook/replication.md) for the correctness
model, the resume behaviour, and the per-database retention caveats.

## `backfill`

```bash
faucet backfill pipeline.yaml --from 2026-06-01 --to 2026-07-01 --window 1d
faucet backfill pipeline.yaml --from 2026-06-01 --to 2026-07-01 --window 1d --dry-run
faucet backfill pipeline.yaml --from 2026-06-01 --to 2026-07-01 --window 1d --resume
faucet backfill pipeline.yaml --from-bookmark '42' --to-bookmark '99' --bookmark-field seq
```

Replays a bounded historical window: chunks `[from, to)` into contiguous
half-open window units, runs each through the normal pipeline path with its
`${backfill.*}` tokens substituted and the `${now.*}` clock set to the window
start, and records durable, resumable progress in the config's `state:` store.
Unit state keys are namespaced (`{name}::backfill::{unit}`) so the forward-sync
bookmark is never touched; delivery is forced to at-least-once (pair with
`write_mode: upsert`). Exits non-zero with the failed-unit count.

| Flag | Purpose |
|------|---------|
| `--from` / `--to` | Wall-clock range: RFC3339 or `YYYY-MM-DD` (midnight in `--timezone`). Half-open. |
| `--window <dur>` | Chunk size (`45s`, `30m`, `6h`, `1d`, `1w`). Default: the config's `backfill.window`; omitted = one unit. |
| `--from-bookmark <v>` | Bookmark mode: seed the scoped state key with this value (JSON or bare string) and run one unit. Requires a `state:` block. |
| `--to-bookmark <v>` / `--bookmark-field <f>` | Upper bookmark bound: drop records whose field orders after the bound. |
| `--concurrency <n>` | Max window units in flight. Default: `backfill.concurrency`, else 1. |
| `--timezone <IANA>` | Date-boundary / `${now.*}` timezone. Default: `backfill.timezone`, else UTC. |
| `--row <id>` | Which root row to backfill (required when the config has several). |
| `--into <sink>` | Redirect writes to a named `pipeline.sinks` template (staging-first). |
| `--dry-run` | Print the planned units without executing. |
| `--resume` / `--restart` | Continue a prior backfill of the same range / discard its marker and start over. |
| `--json` | Machine-readable plan/report. |
| `--profile` / `--env-file` / `--no-env-file` | Same semantics as `run` / `validate`. |

See the [backfill cookbook](../cookbook/backfill.md) and the
[`backfill:` config block](config.md#backfill).

## `schedule`

```bash
faucet schedule pipeline.yaml                  # run on cron schedule, foreground; Ctrl-C to stop
faucet schedule pipeline.yaml --once           # run exactly once now, then exit
faucet schedule pipeline.yaml --env-file prod.env
faucet schedule pipeline.yaml --no-env-file
faucet schedule app.yaml --profile prod        # schedule with a named profile overlay applied
```

Runs a pipeline on a recurring cron schedule in a **long-running foreground process**. The config
must contain a top-level `schedule:` block (without one, faucet errors and suggests `faucet run`).
Requires the `schedule` Cargo feature (included in `full`).

- Stop with Ctrl-C or SIGTERM; the in-flight run drains for up to `shutdown_grace_secs` (default 30)
  before the process exits.
- `--once` ignores cron timing and runs the pipeline exactly once immediately — handy for testing
  a scheduled config or for one-shot container invocations.
- Missed ticks are skipped, not backfilled. A run that starts late emits
  `faucet_schedule_run_lateness_seconds` for monitoring.

Flags:

| Flag | Purpose |
|------|---------|
| `--once` | Run exactly once now, then exit. Ignores cron timing. |
| `--profile <name>` | Select a named overlay from `profiles:` (also settable via `FAUCET_PROFILE`; the flag wins). Same semantics as `run` / `validate`. |
| `--env-file <path>` / `--no-env-file` | Same `.env` handling as `run` / `validate`. |

See the [scheduling cookbook](../cookbook/scheduling.md) for worked examples, the overlap-policy
decision tree, the resilience/supervisor model, and the full metric set to scrape.

## `serve`

```bash
FAUCET_SERVE_AUTH_TOKEN=s3cret faucet serve --listen 0.0.0.0:8080
faucet serve --no-auth                             # explicit opt-in; required if no token
faucet serve --history sqlite:/var/lib/faucet/runs.db --default-config defaults.yaml
```

Runs a **long-running HTTP control plane** that accepts pipeline configs over REST, executes them
under bounded concurrency (reusing the same executor as `faucet run`), and exposes status / cancel /
list / SSE-logs endpoints plus `/healthz`, `/readyz`, and `/metrics`. Requires the `serve` Cargo
feature (included in `full`).

Unlike the other commands, `serve` takes **no config file** — configs arrive per request. Auth is
mandatory: pass `--auth-token`/`FAUCET_SERVE_AUTH_TOKEN`, the `--read-token`/`--write-token`/`--admin-token` trio, or `--no-auth` to explicitly disable it
(absent both, startup fails).

Selected flags (`faucet serve --help` for the full list):

| Flag | Purpose |
|------|---------|
| `--listen <addr>` | Bind address (default `127.0.0.1:8080`; env `FAUCET_SERVE_LISTEN`). |
| `--auth-token <t>` / `--no-auth` | Bearer token (prefer the env var) or explicit no-auth opt-in. |
| `--auth-config <path>` | RBAC principals file (`{ name, token, role }`; roles `viewer`/`operator`/`admin`) — enables role enforcement + the `GET /v1/audit` log. Mutually exclusive with `--auth-token`/`--no-auth`. |
| `--read-token <t>` / `--write-token <t>` / `--admin-token <t>` | The three-token shorthand for the same RBAC (`viewer` / `operator` / `admin`) with no file to author — prefer the env vars `FAUCET_SERVE_{READ,WRITE,ADMIN}_TOKEN`. Any subset may be set; mutually exclusive with `--auth-token` / `--auth-config` / `--no-auth`. See the [role × route matrix](http-api.md#role--route-matrix). |
| `--max-concurrent-runs <n>` / `--max-queued-runs <n>` | Concurrency + queue caps (429 past the queue). |
| `--history <url>` | `postgres://…` / `sqlite:…` for durable run history (feature-gated; default in-memory). |
| `--default-config <path>` | Workspace defaults merged under every submitted run. |
| `--cors-origin <origin>` | Allow-list a browser origin (repeatable; CORS off by default). |
| `--lease-ttl-secs <n>` | Run-ownership lease TTL (default 30) for multi-instance orphan fencing on a shared persistent backend — set above worst-case stalls. See the [serve cookbook](../cookbook/serve.md#multi-instance-orphan-recovery-run-ownership-leases). |
| `--cluster` | Enable cluster mode: instances pull-balance `pending` runs from the shared `--history` DB and provide crash-failover. Requires a persistent `--history` backend (postgres or sqlite). See [Running a cluster](../cookbook/cluster.md). |
| `--cluster-poll-secs <n>` | Claim-loop poll interval in seconds (default `2`). Also the maximum lag before a cross-instance cancel is propagated to the executing instance. |
| `--cluster-max-attempts <n>` | Maximum total attempts (including crash-failovers) before a run is poisoned and marked `failed` (default `3`). |
| `--body-limit-bytes` / `--shutdown-grace-secs` / `--retain-terminal-runs-secs` / `--idempotency-retention-secs` | Tuning knobs. |
| `--no-ui` | Disable the embedded web console at runtime even when the binary was built with `serve-ui`. |
| `--local-output-retention-days <n>` | How long the local files a run's sinks wrote (jsonl/csv/parquet) are kept before the retention GC reclaims them (default `7`; env `FAUCET_LOCAL_SINK_OUTPUT_RETENTION_DAYS`). `0` disables the automatic sweep. See [Local output retention](#local-output-retention). |
| `--local-output-in-flight-grace-secs <n>` | Never delete a local output touched within this many seconds — the guard against unlinking a file a run is still writing (default `60`; env `FAUCET_LOCAL_SINK_OUTPUT_IN_FLIGHT_GRACE_SECS`). Raise it above the longest expected gap between a slow source's pages; `0` disables it. |
| `--preview-local-outputs` | Serve **dataset previews** of the local files this server's sinks wrote — read their first N rows back into the console (env `FAUCET_SERVE_PREVIEW_LOCAL_OUTPUTS`). **Off by default**: it returns file *contents* over HTTP, so it is a local-testing convenience. See [Dataset preview](#dataset-preview-of-local-outputs). |
| `--preview-default-rows <n>` | Rows a preview loads when the request omits `row_count_to_load` — the soft cap (default `500`; env `FAUCET_SERVE_PREVIEW_DEFAULT_ROWS`). `0` = the whole dataset by default. |
| `--preview-max-rows <n>` | Ceiling on one preview's rows — the hard cap (default `5000`; env `FAUCET_SERVE_PREVIEW_MAX_ROWS`). A larger `row_count_to_load` is clamped to it, never honoured. **`0` lifts the ceiling**, which is what makes `row_count_to_load=all` load an entire dataset. |
| `--triggers <path>` | Path to a YAML triggers file that defines event-driven watchers (object-arrival / webhook / queue-depth). Requires the `triggers` Cargo feature. See [Triggers reference](./triggers.md). |
| `--callback-allow-host <host>` | Restrict per-run completion callbacks to these hosts. Repeatable. Unset = any host except link-local / cloud-metadata addresses, which are always refused unless named here. See [Completion callbacks](./http-api.md#completion-callbacks). |

### Optional embedded web console (`serve-ui`)

When built with the `serve-ui` Cargo feature, `faucet serve` also serves a
browser-based web console at `/` (and static assets at `/assets/*`):

```bash
cargo install faucet-cli --features serve-ui
FAUCET_SERVE_AUTH_TOKEN=s3cret faucet serve --listen 127.0.0.1:8080
# Open http://127.0.0.1:8080/ in a browser.
```

The static shell is public; all `/v1` data is bearer-gated as usual. The
browser is prompted for the token on first load; it is stored in `localStorage`
and sent on every `/v1` call. Pass `--no-ui` to disable the console at runtime
without rebuilding.

`serve-ui` implies `serve` and is included in the `full` aggregate. It ships
three additional bearer-gated endpoints:

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/v1/schemas` | Catalog of compiled sources, sinks, transforms, and state-store kinds. |
| `GET` | `/v1/schemas/{kind}/{name}` | JSON Schema for one connector or transform (`kind` ∈ `source`/`sink`/`transform`). 404 for unknown. |
| `POST` | `/v1/doctor` | Validate + probe a submitted config without running it. 200 (pass) / 422 (fail). Body: `{ "config": "…", "config_format": "yaml" }`. |

These endpoints require `serve` and are available regardless of `--no-ui`. See
the [web console guide](../cookbook/web-console.md) for the full walkthrough and
the [HTTP API reference](./http-api.md) for the complete endpoint/schema
reference.

### Local output retention

A long-running `serve` used for local iteration accumulates real files —
`out.jsonl`, `rows.csv`, directories of rolled parquet parts. `serve` therefore
runs a **retention GC** for them: faucet records every local file its sinks open,
and a background sweeper deletes the ones past their window (default **7 days**,
`--local-output-retention-days` / `FAUCET_LOCAL_SINK_OUTPUT_RETENTION_DAYS`; `0`
disables the sweep). A pipeline can override the window for its own outputs with
the [`local_outputs:` block](./config.md#local_outputs). Requires the `catalog`
feature; the ledger lives in the `--history` backend, so use a persistent one for
it to survive a restart.

**The one guarantee that matters:** it deletes *only* files faucet recorded as
its own sink outputs. Never a glob, never a directory — not even for "clean all"
— and never a file faucet *wrote to* but did not *create*. Point a sink at an
existing export and its record is marked `external`, which no scope will delete.

**Nor a file that is still being written.** Two checks guard that: an output whose
run is currently executing is skipped, and — because a *new* run rewriting a path
the ledger still attributes to the previous run has an id no row names yet — so is
any file touched within `--local-output-in-flight-grace-secs` (default 60). That
is a bound rather than a lock: a writer that goes quiet for longer than the grace
between pages can still, rarely, have its file taken, so raise the window for slow
sources.

Run history, catalog entries, and lineage are never touched. Data artifacts are
disposable; the record of what ran is durable — so a cleaned output keeps its
record, marked `expired`, and its run still shows in the Runs tab.

On-demand cleanup is available three ways:

| Surface | What it does |
|---|---|
| Console → **Datasets** → *Local outputs* | Per-output "delete now", "purge older than N days", and "clean all" (confirmed). Read for `viewer`; deleting needs `operator`. |
| `POST /v1/local-outputs/cleanup`, `DELETE /v1/local-outputs/{id}` | The same, over HTTP — see the [HTTP API reference](./http-api.md#local-sink-outputs). |
| [`faucet cleanup`](#cleanup) | The same, from the CLI, against a `catalog:` store. |

### Dataset preview of local outputs

"12 records written" and "here are the 12 records" are different pieces of
information, and only one of them tells you whether the transform did what you
meant. With `--preview-local-outputs`, the console's **Datasets → Local outputs**
panel grows a *Preview* control on each tracked jsonl / csv / parquet file: it
reads the first N rows back and renders them as a table, so a local iteration loop
never leaves the browser.

It is a **source-backed capped read**, not a file reader. The server builds the
matching *source* connector for the output's kind (`csv` → `source-csv`,
`parquet` → `source-parquet`, `jsonl` → its JSON Lines reader), pulls one page,
and stops — so previewing a 4 GiB `out.jsonl` reads its first few kilobytes.
Rows past the cap are never decoded, and a preview always says when it capped.
Each source is given the connector's defaults, which are the matching sink's
defaults — so faucet reads its own output back exactly. (jsonl and parquet are
self-describing; a CSV file does not carry its delimiter or whether row 1 is a
header, so a csv output is read comma-delimited with a header row, as the csv
sink writes it.) `.gz` / `.zst` outputs are decompressed on the way in.

**Local testing only, and off by default.** The endpoint is inert unless the flag
is set; without it every request is a `403` naming the flag, for every role. Two
properties make it safe to switch on locally:

- **No path ever comes from the request.** A preview names the *ledger id* of an
  output the server already tracks; the path comes from the row the sink wrote —
  so pointing the read at another file is not blocked, it is unrepresentable.
- **Every read is bounded**, by the two caps below and by a 30-second ceiling.

| Knob | Env | Default | Meaning |
|---|---|---|---|
| `--preview-default-rows` | `FAUCET_SERVE_PREVIEW_DEFAULT_ROWS` | 500 | Rows loaded when `row_count_to_load` is omitted (soft cap). |
| `--preview-max-rows` | `FAUCET_SERVE_PREVIEW_MAX_ROWS` | 5000 | Ceiling; a larger `row_count_to_load` is **clamped**, never honoured. |

Both are surfaced in the Helm chart (`serve.preview.*`), so a deployment can raise
or lower them without a rebuild, and both are reported on
`GET /v1/local-outputs` so the console labels its own control with the server's
real limits. Setting the soft cap above the hard cap clamps it with a warning
rather than refusing to boot — the ceiling always wins.

#### Limit, not limit/offset

There is no `offset` and no cursor, and that is a reading of what these sources
are rather than a missing feature. A `.jsonl` or `.csv` file is a sequential byte
stream with no row index, so `OFFSET 500` can only be implemented as *read the
first 500 records and throw them away* — exactly what asking for 1000 records and
keeping the tail costs. Paging would add a cursor, a state contract, and a "did
the file change between pages?" problem while buying nothing. **"Show me more" is
spelled "raise the limit"**, and because the engine *stops* rather than
truncating, raising it is cheap.

#### Reading a whole dataset

`row_count_to_load=all` (or `0`) asks for every row. Whether that is served in
full is the operator's call, not the client's:

```bash
# Previews capped at 5000 rows — a client cannot ask for more.
faucet serve --preview-local-outputs

# No ceiling: `all` really means all. The console's "All rows" button then
# loads the whole file.
faucet serve --preview-local-outputs --preview-max-rows 0
```

With a ceiling configured, `all` resolves *to the ceiling* — which is the point of
having one. With `--preview-max-rows 0` the response reports `row_limit: null`
and `preview_max_rows: null`, and the console offers "All rows" as something that
will genuinely load everything.

**Unlimited is not unbounded.** An uncapped read is unlimited in *rows* only: it
is still paged, and still stops at a response-size budget (64 MiB) and a
30-second deadline. A read stopped by any of the three bounds comes back as a
*partial answer that names the bound* — `capped_by` is `rows`, `bytes`, or
`time`, and absent when the response is the whole dataset. So asking for a
dataset larger than the server can hold gets you as much of it as fits plus the
reason it stopped, never an out-of-memory or a silently clipped table. (Only a
single page that never returns at all — 60s — fails the request, as a `503`.)

The paging is what makes those bounds work, and it is not incidental: both are
checked as pages arrive, so a read that arrives all at once cannot be
interrupted. That is why an unlimited preview asks its source for pages of 1000
rows rather than for one unbounded page.

**An `external` output is never previewed.** faucet wrote to that file but did not
create it, so its contents are not faucet's to serve — the read-side twin of the
retention GC's refusal to delete it. The output stays listed, with state
`external`, and the console renders no Preview control for it. Every served
preview also writes a `local_output.preview` audit entry (principal, output, row
count): this is the one read on the control plane that returns pipeline *data*
rather than metadata, so it is the one read worth recording.

These caps govern the **serve** preview only. `faucet preview` is a local,
deliberate, single-user command with its own `--limit` flag: a different trust
model, deliberately not sharing a knob.

Reading needs `LocalOutputRead` (`viewer` and up) — the same scope that already
lists these files. That means enabling the flag lets every viewer see the data
those pipelines wrote, which is the other reason it is opt-in. An output cleaned
by retention previews as a `409` explaining that the file is gone and the run
record was kept, never a 500. See
[`GET /v1/local-outputs/{id}/preview`](./http-api.md#local-sink-outputs).

> ⚠️ `serve` executes arbitrary client-supplied configs with the server's identity (secrets, files,
> network egress). Run single-tenant, authenticated, behind egress controls. See the
> [serve cookbook](../cookbook/serve.md) for the security model and the
> [HTTP API reference](./http-api.md) for endpoints.

## Environment-only mode

`faucet run --from-env` assembles a pipeline from a `FAUCET_*` snapshot
(`FAUCET_SOURCE_*`, `FAUCET_SINK_*`, `FAUCET_STATE_*`, `FAUCET_TRANSFORM_<N>_*`),
which is handy for containerized deployments where everything comes from the
environment. Nested/tagged-enum fields use a `*_JSON` suffix.

> The complete config grammar (matrix, templates, vars, execution) lives in
> [`cli/README.md`](https://github.com/faucet-hq/faucet-stream/blob/main/cli/README.md).

## Shell completions

`faucet` supports tab-completion in two ways.

**Static script** — generate a completion script for your shell and install it
the way that shell expects:

```bash
faucet completions bash   > /etc/bash_completion.d/faucet     # bash
faucet completions zsh    > ~/.zfunc/_faucet                  # zsh (dir on $fpath)
faucet completions fish   > ~/.config/fish/completions/faucet.fish
faucet completions powershell >> $PROFILE                     # powershell
```

This completes subcommand names, flags, fixed-choice values, and file paths.

**Dynamic (recommended)** — let the binary compute completions at completion
time, so it stays in sync with the build and becomes **config-aware**. Add one
line to your shell rc:

```bash
echo 'source <(COMPLETE=bash faucet)' >> ~/.bashrc     # bash
echo 'source <(COMPLETE=zsh  faucet)' >> ~/.zshrc      # zsh
echo 'COMPLETE=fish faucet | source'  >> ~/.config/fish/config.fish
```

With the dynamic hook enabled you get runtime-aware candidates:

- `faucet schema source <TAB>` / `sink` / `transform` — the connectors and
  transforms **compiled into this binary** (a slim build lists only its own).
- `faucet run --select <TAB>` / `--only` / `--skip` — the **matrix row ids**
  from the `faucet.yaml` in the current directory.
- `faucet run --status <TAB>` — the readiness ladder
  (`mandatory active available draft archived`).
- `faucet run --tag <TAB>` — the tags present in the current config.

The config-aware providers are best-effort and read-only: they parse and expand
the local config but never resolve secrets, hit the network, or open a
connector, and fall back to no suggestions if no config is present.

## `migrate`

`faucet migrate [config]` upgrades a config written against an older `faucet`
grammar to the current shape, in place. It is **idempotent** — running it on an
already-current config changes nothing.

```bash
faucet migrate                 # migrate the discovered faucet.yaml
faucet migrate old.yaml        # migrate a specific file (rewrites it)
faucet migrate old.yaml --stdout   # print the migrated config, don't write
faucet migrate --check         # exit non-zero if a migration is needed (CI)
```

Rules applied today:

- **Top-level `source:` / `sink:` → `pipeline:`** — the pre-`pipeline` block
  shape is wrapped into a `pipeline:` map (moving `transforms:` / `state:` too).
- **Legacy auth → `{ type, config }`** — an `auth:` / `credentials:` block of
  the old `{ type, <fields…> }` shape has its fields folded into a `config:`
  sub-map, matching the current adjacently-tagged form.

Each rule is a pure, unit-tested transform. Comments are not preserved (the
config is parsed and re-serialized).

## `doctor --offline`

Beyond its connectivity probes, `faucet doctor` can run a **static, offline**
config lint — no network, no credentials — ideal for CI:

```bash
faucet doctor --offline            # lint the discovered config
faucet doctor --offline --json     # machine-readable findings
```

It flags: a connector `auth: { ref }` that points at a provider missing from the
`auth:` catalog (**error**); an `auth:` provider nothing references (**warning**);
a `vars:` entry never interpolated (**warning**); and a file/append sink
(`jsonl`/`csv`/`stdout`) with `batch_size: 0`, which is a no-op (**warning**).
The command exits non-zero on any lint *error* (warnings don't fail). Secret and
`${env:…}` resolution is validated separately at config load (and by
`faucet validate`).

## `fmt`

`faucet fmt [config…]` rewrites a config into a canonical form — a stable key
order (a curated priority for well-known blocks like `version`/`name`/`pipeline`,
then alphabetical), so diffs stay meaningful and reviews stay quiet. Running it
twice is always a no-op, so `--check` is a cheap CI gate.

```bash
faucet fmt pipeline.yaml            # rewrite in place
faucet fmt pipeline.yaml --stdout   # print, don't write
faucet fmt pipeline.yaml --check    # exit non-zero if not already canonical (CI)
```

Comments are not preserved (the file is parsed and re-serialized).

## `explain`

`faucet explain [config]` narrates, in plain English, what a pipeline does —
source → transforms → sink, write mode/key, matrix expansion, delivery
guarantee. It is built entirely from the resolved config: **fully offline, zero
I/O, no source touched**, and secrets are never printed (only a curated
allowlist of structural fields is surfaced, and the output is scrubbed).

```bash
faucet explain pipeline.yaml          # prose
faucet explain pipeline.yaml --json   # structured
faucet explain pipeline.yaml --rows   # narrate every row of a large matrix
```

## `history`

`faucet history [config]` prints the recent run history recorded in the config's
`catalog:` store (the same backend `faucet serve` history and `faucet plan
--diff` use) — status, duration, throughput — without standing up `faucet
serve`. Read-only; requires the `catalog` build feature.

```bash
faucet history                 # table, newest first (default 20)
faucet history --limit 50      # more rows
faucet history --row us        # only runs with an invocation for row `us`
faucet history --json          # machine-readable
```

Run records are written by `faucet serve`; point `history` at the same store.

## `cleanup`

*(requires the `catalog` build feature — included in `full`)*

Reclaims the **local files** a pipeline's sinks wrote — `out.jsonl`, `rows.csv`,
a directory of rolled parquet parts. The manual half of the retention GC
`faucet serve` runs on a timer (see
[Local output retention](#local-output-retention)).

```bash
faucet cleanup                          # outputs past their retention window (7d default)
faucet cleanup --older-than-days 3      # regardless of per-pipeline overrides
faucet cleanup --dataset 3f2a9c1e0b7d4a55   # one dataset's outputs
faucet cleanup --run 01a033bc-30f1-74a2-…   # clean up after one run
faucet cleanup --output 9f2b1c4d5e6f7a8b    # one file
faucet cleanup --all --dry-run          # what "clean all" would remove
faucet cleanup --all --yes              # every tracked output (confirmed)
faucet cleanup --store sqlite:./faucet-catalog.db --json
faucet cleanup --all --yes --in-flight-grace-secs 0   # nothing is running
```

The ledger of outputs lives in the config's `catalog:` store — the same one
`faucet run` / `schedule` / `replicate` record into and `faucet serve --history`
browses — so `--store` can point at a server's store directly.

**`--retention-days` vs `--older-than-days`** — easy to conflate, and they do
different things:

| Flag | Kind | Meaning |
|---|---|---|
| `--retention-days <n>` | *policy* | The window the bare (expired-only) sweep measures against, overriding the config's `local_outputs.retention_days`. Reads `FAUCET_LOCAL_SINK_OUTPUT_RETENTION_DAYS` when unset, so it matches the `faucet serve` default. `0` = keep forever. Per-pipeline overrides still apply. |
| `--older-than-days <n>` | *scope* | Selects everything older than `n` days **ignoring every retention setting**, including per-pipeline overrides. `0` matches every output and needs `--yes`. |

So `--retention-days 3` means "treat 3 days as this store's policy and collect
what that policy has expired"; `--older-than-days 3` means "delete anything older
than 3 days, whatever the policy says".

**What it will and will not delete.** Only paths faucet recorded as its own sink
outputs. Never a glob, never a directory, and never a file faucet *wrote to* but
did not *create* — point a sink at an existing export and its ledger row is
marked `external`, which no scope (including `--all`) will delete. Run history,
catalog entries, and lineage are untouched: a cleaned output keeps its record,
marked `expired`.

`--all` deletes files that are still inside their retention window, so it
requires `--yes` (or `--dry-run`) — as does `--older-than-days 0`, which matches
every output and is `--all` under another name. Every scope reports what it
skipped and why — a "0 files" answer always comes with the reason.

**Files being written are skipped.** An output touched within
`--in-flight-grace-secs` (default 60) is left alone and reported as `in_flight`,
because a writer may still hold it — including a `faucet serve` in another
process sharing this store, whose runs this command cannot see. Pass
`--in-flight-grace-secs 0` when you know nothing is running.

## `run --output`

`faucet run … --output json` (or `ndjson`) emits a machine-readable end-of-run
summary instead of the human line, keeping stdout otherwise clean (logs stay on
stderr) so `faucet run` is composable in CI / cron / Slack:

```bash
faucet run pipeline.yaml --output json      # one JSON document: per-row + totals
faucet run pipeline.yaml --output ndjson     # one JSON object per matrix row
```

Each row reports `rows_in` / `rows_out` / `duration_ms` / `dlq_count` / `status`
/ `bookmark`; the exit code is unchanged (non-zero on failure). Secret material
is scrubbed from the output.
