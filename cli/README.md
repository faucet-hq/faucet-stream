# faucet-cli

[![Crates.io](https://img.shields.io/crates/v/faucet-cli.svg)](https://crates.io/crates/faucet-cli)
[![Docs.rs](https://docs.rs/faucet-cli/badge.svg)](https://docs.rs/faucet-cli)
[![MSRV](https://img.shields.io/crates/msrv/faucet-cli.svg)](https://github.com/faucet-hq/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-cli.svg)](https://github.com/faucet-hq/faucet-stream#license)

`faucet` — config-driven runner for [`faucet-stream`](https://crates.io/crates/faucet-stream) pipelines.

Write a YAML or JSON file describing a source, optional transforms, a sink, and (optionally) a state store. Run it with the `faucet` binary. No Rust code required.

## Install

```bash
cargo install faucet-cli
```

To build a slim binary with only the connectors you need:

```bash
cargo install faucet-cli --no-default-features \
    --features source-rest,sink-jsonl,sink-stdout,transforms
```

## Commands

| Command | What it does |
|---------|--------------|
| `faucet run <config>` | Execute the pipeline end-to-end. Supports `--dry-run`, `--limit N`, `--state-path PATH`, `--param NAME=VALUE` / `--param-env NAME[=VALUE]` (typed run parameters), `--tui` (live full-screen progress view; `cli-tui` build feature), and `--quiet` (suppress the inline progress line). On an interactive terminal it shows a lightweight inline per-row progress line (records in/out, rows/s, pages, elapsed) on stderr; auto-disabled on a non-TTY stdout or under `--quiet` (`cli-progress` build feature, in the default build). |
| `faucet validate <config>` | Parse + validate without running. Exits non-zero on error. `--param NAME=VALUE` binds declared params strictly; without it, required params validate against type-shaped placeholders. |
| `faucet schema source|sink|transform <name>` | Print the JSON Schema for a connector's or transform's config. |
| `faucet schema dlq` | Print the JSON Schema for the dead-letter-queue spec. |
| `faucet verify <config> [--row R] [--repair] [--allow-delete] [--dry-run] [--json]` | Prove a destination matches its source **by content** (#701): rows matched on the sink's `key` (or `verify.key`), key ranges compared by digest — inside the database when both sides are the same SQL backend, so matching ranges ship no rows — bisected down to the differing keys (`missing_in_dest` / `extra_in_dest` / `changed` / `duplicate`). `--repair` re-syncs exactly those keys through the row's sink (`write_mode: upsert`; deletes only with `--allow-delete`). Exit code = differing keys. A top-level `verify:` block runs the same check after every run. |
| `faucet profiling show\|reset <config> [--row R] [--column C] [--full] [--json]` | Inspect or re-baseline the learned column profiles a config's `profiling:` block keeps in its `state:` store (#708): `show` prints per root row the baseline depth, the latest run's per-column statistics and drift findings; `reset` forgets the history (or one column's) so the next `min_history` runs learn the new normal. |
| `faucet rollback <config> --run <id> [--row R] [--dry-run] [--force]` · `--list` | Undo a run (#706): delete the rows it appended (by `_faucet_run_id`), restore the journaled before-images of the keys it upserted, or swap back the table it overwrote — then rewind the row's bookmark and exactly-once watermark so the next run re-reads what was undone. Needs a top-level `rollback:` block (journal + kept previous table + pre-run marker in a durable `state:`) on a postgres / sqlite / mysql column-mode sink. A key a later run changed is a conflict that blocks the rollback unless `--force`. `faucet run` prints each row's run id. |
| `faucet usage [--since W] [--until W] [--by pipeline\|row\|dataset\|sink\|day] [--pipeline P] [--json]` · `faucet run --max-records N --max-bytes B --max-duration-secs S --allowed-sink X` | Cost & usage accounting (#704) and run budgets (#703). Every invocation is metered — records, estimated bytes, backend round trips, connector cost signals (BigQuery bytes billed, S3/GCS requests) — priced against the `usage:` rate table (shipped defaults = public list prices, all overridable) and compared to a per-row-priced hosted ELT equivalent; estimates are labelled as estimates and a connector that reports nothing is *compute not reported*, never zero. `faucet run` prints a usage line per row, `--output json` and serve run records carry the record, `faucet usage` / `GET /v1/usage` / the console's Usage page aggregate the `catalog:` store. A `budget:` block (or the run flags) puts hard ceilings on one invocation: the page that would cross `max_records` / `max_bytes` is refused whole (bookmark untouched), `max_duration_secs` cancels at the next page boundary, `allowed_sinks` refuses before anything runs. Requires the `catalog` build feature for `faucet usage`. |
| `faucet list` | List every compiled-in source, sink, transform, and state-store backend, each with its conformance maturity tier. |
| `faucet conformance [name] [--kind K] [--json] [--min-tier T]` | Score connectors against the SDK contract, print a scorecard + maturity tier (Stable/Experimental/Beta/Draft) and capability badges. `--min-tier` exits non-zero as an opt-in CI gate. |
| `faucet preview <config> --limit N` | Run only the source side and emit the first N records to stdout as JSONL. |
| `faucet plan <config> [--sample F\|--live] [--diff] [--impact [--depth N]] [--policy F] [--json]` | Read-only "what would this do" preview (resolved pipeline, output schema, sink delta — never writes). `--diff` shows a `terraform plan`-style per-row config diff against the last recorded run (needs a `catalog:` block + `catalog` feature; secrets stored only as stable `<secret:sha256:…>` tokens). `--impact` (#707) walks the catalog's lineage graph downstream of the row's sink and reports the datasets, contracts, owners and declared consumers the planned schema affects, with a severity each (`breaking` / `additive` / `unknown`). A `policy:` block / `--policy` adds the row's data-flow-policy verdict. |
| `faucet policy <config> [--policy F] [--row R] [--json]` | Evaluate a data-flow policy (#702) — classifications that label columns + rules about which sinks a label may reach — against a config: per row, the labelled columns heading into each sink (by name, through a rename, or conservatively past an opaque transform; whether masking provably masks them) and every violated rule. Exit code = violations. The same verdict is reported by `validate` / `plan` / `doctor` (`--policy`), refuses `run`, and is enforced by `faucet serve --policy`. `faucet schema policy` prints the schema. |
| `faucet init [name] [--source X] [--sink Y]` | Scaffold a pipeline.yaml from each connector's JSON Schema. |
| `faucet doctor <config> [--timeout-secs N] [--json]` | Probe every connector (auth/network/permissions/reachability) and print a checklist. Exits with the failed-probe count. |
| `faucet test <specs…> [--filter S] [--json] [--clock C]` | Run fixture-based **offline** pipeline tests: stream sample records through a config's transforms/quality/contract with in-memory source/sink/DLQ and assert the output. Exits with the failed-case count. `faucet schema test` prints the spec-file JSON Schema. |
| `faucet contract <config> [--export contract\|json-schema\|openlineage]` | Validate the `pipeline.contract:` block and print a summary, or export the data contract as canonical JSON / JSON Schema / an OpenLineage schema facet. `faucet schema contract` prints the block's own JSON Schema. |
| `faucet discover <config> [--include G] [--exclude G] [-o F] [--json]` | Connect to the config's source, enumerate the datasets behind it (tables / collections / indices / prefixes), and emit a ready-to-run config with one matrix row per dataset. Supported: postgres, mysql, mssql, sqlite, mongodb, elasticsearch, bigquery, snowflake, s3, gcs. |
| `faucet backfill <config> --from A --to B [--window W] [--resume]` | Replay a bounded historical window as resumable, bookmark-isolated window units (`${backfill.*}` tokens scope the source; durable progress marker; `--dry-run` to preview; bookmark mode via `--from-bookmark`). `faucet schema backfill` prints the defaults-block schema. Exits with the failed-unit count. |
| `faucet schedule <config> [--once]` | Run a pipeline on a cron schedule (long-running foreground process). Requires a `schedule:` block. |
| `faucet catalog datasets\|show\|lineage\|annotate [--config C] [--json]` | Browse the Data Movement Catalog accumulated by a config's `catalog:` store: dataset list, per-dataset schema timeline / volume / edges / owners / consumers, and the lineage graph; `annotate <id> --owner … --consumer NAME[=KIND] [--contact] [--columns] [--replace]` sets a dataset's owners and declared consumers (what `plan --impact` names). Requires the `catalog` build feature. `faucet schema catalog` prints the block's JSON Schema. |
| `faucet hub list\|check\|compose\|matrix\|lint` · `faucet run\|validate --source X --sink Y` | Template Hub (#571): compose a `kind: source-template` (one system — auth, pagination, shared `transforms`, and its **streams** with per-stream `write: [overwrite, upsert]` preferences) with a `kind: sink-template` (one destination + `per_stream: { table_id: "${stream}" }`) into an ordinary pipeline at run time. The composer picks the first write mode the sink supports (from the connector registry, or a declared `write_mode_aliases` such as jsonl's `overwrite: append`) and fails per stream when none fits. Ids resolve under `--hub` / `$FAUCET_HUB` / `./hub`, else the **public hub** `github:faucet-hq/template-hub` (any GitHub repo laid out like `hub/` works: `--hub github:owner/repo[@ref][/path]`, fetched via the contents API and cached per commit under `~/.cache/faucet/hub/`, offline-safe). The repo ships the layout, bigquery/postgres/sqlite/jsonl sink templates, and two example source templates under `hub/`; real source templates are published to the public hub by PR and browsed at faucet-hq.github.io/hub. `faucet serve --templates-sync cli/examples/templates/hub-sync.yaml` mirrors the hub into a server's registry, and `GET /v1/templates/matrix` / the console's Compatibility grid show which registered pairings compose. A `kind: deployment` overlay (`--overlay FILE\|ID` on `run` / `validate` / `hub compose` / `hub check`) adds the operational blocks neither template owns — `state`, `dlq`, `notifications`, `sla`, `profiling`, `policy`, `resilience`, `execution`, `delivery`, `schedule`, plus per-stream `sla` / `dlq` / `delivery` — and is refused if it would change connectors or streams. `faucet schema source-template\|sink-template\|deployment` print the schemas. |
| `faucet template register\|list\|show\|launch\|rollback\|deprecate\|promote\|delete\|run\|test --store URL` | Register a template **once**, then trigger runs by id + `--param name=value`. The registry is kind-aware: a `kind: source-template` and a `kind: sink-template` (the Template Hub documents) register under their `name` and compose at run time — `faucet template run <source> --sink <sink> [--sink-version stable]` — a `kind: deployment` is applied with `--overlay <id>`; while a `kind: pipeline` is a complete config that runs alone; `list --kind` filters, and a document without `kind:` registers as a pipeline with a deprecation notice. Versions auto-increment and a register **moves nobody** — `launch` is the one step that changes what an unpinned run gets (`rollback` re-launches the previous one), so a template is `draft` until launched, then `launched`, and `deprecated` once retired. Three channels are derived (`stable` = the launched version and the default selector, `previous`, `newest`); six are assignable with `--tag` (`dev`/`test`/`staging`/`pre-prod`/`canary`/`prod`). `--version <n\|channel>` selects one. Point `faucet serve --history` at the same store and the same templates are triggerable over HTTP/MCP/the web console. Requires the `templates` build feature. `faucet template test <suite>` sweeps a template's parameter space offline (no registry needed when the suite's `template:` is a config path). `faucet schema params` / `faucet schema template-test` print the JSON Schemas. With the `templates-sync` feature, `faucet template sync --config sync.yaml [--origin X] [--dry-run]` pulls templates from remote **origins** (GitHub, S3, GCS, Azure Blob — RFC 0006) into the registry, appending versions and never deleting, and `faucet template publish <id> --origin X` writes one version back; `faucet serve --templates-sync sync.yaml` does the same on start, on `POST /v1/templates/sync`, and on each origin's `interval_secs`. `faucet schema templates-sync` prints the sync-file schema. |
| `faucet completions <bash\|zsh\|fish\|powershell\|elvish>` | Print a shell tab-completion script. For registry- and config-aware **dynamic** completion, enable the `COMPLETE` hook instead (see [`faucet completions`](#faucet-completions)). |
| `faucet migrate [config] [--check\|--stdout]` | Upgrade an old-grammar config to the current shape in place (idempotent): wraps top-level `source:`/`sink:` into `pipeline:`, folds legacy `auth`/`credentials` into `{ type, config }`. `--check` exits non-zero if a migration is needed (CI); `--stdout` previews without writing. |
| `faucet doctor --offline [config]` | Static, credential-free config lints (no network): dangling / unreferenced `auth:` providers, unused `vars:`, no-op sink `batch_size: 0`. Exits non-zero on any lint error. |
| `faucet fmt [config] [--check\|--stdout]` | Canonicalize a config in place (stable key order); idempotent. `--check` is a CI gate (non-zero if not canonical); `--stdout` previews. Comments are not preserved. |
| `faucet explain [config] [--json\|--rows]` | Plain-English narration of a pipeline (source → transforms → sink, matrix, delivery). Fully offline; never prints secrets. |
| `faucet history [config] [--limit N\|--row R\|--json]` | Terminal view of the run history in the config's `catalog:` store (status/duration/throughput), newest first. Read-only; requires the `catalog` feature. |
| `faucet run … --output <text\|json\|ndjson>` | End-of-run summary format. `json`/`ndjson` emit a machine-readable per-row + totals summary (clean stdout, logs on stderr) for CI/cron/Slack. |

Pass `--log-level debug` (or set `FAUCET_LOG=debug`) for verbose tracing. Logs are written to stderr; pipeline records and command output go to stdout.

### `faucet doctor`

`faucet doctor <config>` runs a fast, **non-mutating** preflight against every connector in a config before you commit to a real run — so a misconfigured credential, an unreachable host, or a missing permission surfaces in seconds with a clear remediation hint, instead of failing mid-run and polluting your metrics.

For each root invocation it probes the source, sink, and state store:

- **Sources** reuse the real read path — the probe pulls a *single page* (DNS + TLS + auth + the first request + first-record decode) and stops, never paginating the full dataset. A handful of sources whose first page would block or have side effects use a targeted probe instead: `webhook` checks the port is bindable, `websocket` does a TCP connect, `postgres-cdc` checks the replication slot is reachable, `kafka` fetches cluster metadata.
- **Sinks** run a non-mutating connect/auth/metadata call (e.g. `SELECT 1`, `HeadBucket`, `PING`, `tables.get`, cluster health, `fetch_metadata`) — never a real write. File sinks check the target directory is writable; `stdout` always passes.
- **State stores** do a sentinel `put`/`get`/`delete` round-trip that leaves no residue.
- **SLA** (when an [`sla:` block](#sla-optional) is configured) probes the persisted run history read-only: staleness of the last successful run vs `max_staleness_secs`, and volume-baseline warm-up state.

```bash
faucet doctor pipeline.yaml                      # checklist, exit code = # of failed probes
faucet doctor pipeline.yaml --timeout-secs 5     # per-probe timeout (default 10)
faucet doctor pipeline.yaml --json               # machine-readable, for CI gating
```

Example output:

```text
✓ Config parses and interpolates                                 8 ms
✓ Matrix expands to 2 invocations                    0 skipped (children)

▸ Invocation default::us-east  (source=postgres, sink=bigquery)
  ✓ source [postgres] read                                      42 ms
  ✗ sink   [bigquery] auth (dataset us_east not found)         410 ms
        hint: check bigquery credentials and that the dataset exists

Summary: 1 passed, 1 failed, 0 skipped       total elapsed 0.5s
```

Flags:

| Flag | Purpose |
|------|---------|
| `--timeout-secs <N>` | Per-probe timeout in seconds (default 10). |
| `--json` | Emit a `{ config, invocations, summary }` JSON document instead of the checklist. |
| `--env-file <path>` / `--no-env-file` | Same `.env` handling as `run` / `validate`. |

**Exit code** = the number of failed probes, clamped to 255 (so `0` means all probes passed). **Child invocations** (parent/child matrix rows) are listed but not probed — their configs depend on parent records that only exist at run time. Probe `reason`/`hint` text is scrubbed for resolved secrets before printing, but third-party connectors should never place credentials in a probe message.

**Probe contract for connector authors:** `Source::check` / `Sink::check` / `StateStore::check` (in `faucet-core`) default to a generic probe (source) or "not implemented" skip (sink / state). Override them with a probe that is **idempotent and side-effect-free** and never echoes credentials. Return probe-level failures as `ProbeStatus::Fail` inside an `Ok(CheckReport)`; reserve `Err` for "couldn't run any probe".

### `faucet test`

`faucet test <specs…>` runs fixture-based, **fully-offline** pipeline tests. A spec file declares sample input records, the pipeline logic under test (a config file's transforms/quality/contract — or the same declared inline), and the expected outcome; the runner streams the fixtures through the real per-page pipeline path with an in-memory source, sink, and DLQ. No configured source or sink is ever built or contacted, so pipeline logic is assertable in CI with nothing but the `faucet` binary.

```bash
faucet test tests/*.yaml                       # run every case in every spec
faucet test tests/orders.yaml --filter null    # only cases whose name contains "null"
faucet test tests/*.yaml --json                # machine-readable report
faucet test tests/*.yaml --clock 2026-03-01    # default ${now.*} clock for cases without clock:
```

Spec grammar (see `faucet schema test` for the full JSON Schema):

```yaml
version: 1
tests:
  - name: null order ids quarantined   # unique per spec file
    config: ../pipeline.yaml           # config under test (relative to the spec file)…
    # pipeline: { transforms: […], quality: …, contract: … }   # …or inline logic instead
    # row: shaped                      # matrix row id when the config expands to several
    # page_size: 100                   # chunk fixtures into pages (0 = one page, default)
    # clock: 2026-02-01T00:00:00Z      # pin ${now.*} for deterministic inline transforms
    input:                             # inline records, or a .jsonl/.json/.yaml file path
      - { OrderId: 1, Amount: 9.5 }
      - { OrderId: null, Amount: 3.0 }
    expect:                            # every set field is asserted; at least one required
      records: [ { order_id: 1, amount: 9.5 } ]   # exact sink output, in order
      dlq: [ { order_id: null, amount: 3.0 } ]    # quarantined payloads (envelope metadata ignored)
      # records_written: 1 / dlq_count: 1         # count-only alternatives
      # error: "Contract v1.0.0 violated"         # the run must FAIL with this substring
      # unordered: true                           # compare records/dlq as multisets
      # match: subset                             # expected records name only the fields they assert
```

Failures print a structured path diff (`records[0].amount: expected 9.5, got 3.0`); the **exit code** is the failed-case count clamped to 255. The `schema:` (drift) block is inert offline; referenced configs load without contacting secrets managers (pass `--resolve-secrets` to opt in). A runnable example lives in [`examples/tests/`](examples/tests/); the [testing cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/testing.html) has the full walkthrough and CI recipe.

Flags:

| Flag | Purpose |
|------|---------|
| `--filter <substring>` | Run only cases whose name contains the substring. |
| `--json` | Emit a `{ total, passed, failed, tests }` JSON report. |
| `--clock <value>` | Default `${now.*}` clock for cases without their own `clock:` (RFC 3339 or `YYYY-MM-DD`). |
| `--profile <name>` | Profile overlay applied to referenced configs (as in `run`). |
| `--resolve-secrets` | Resolve `${vault:…}`-style directives in referenced configs (network). Default: offline. |
| `--env-file <path>` / `--no-env-file` | Same `.env` handling as `run` / `validate`. |

### `faucet schedule`

`faucet schedule <config>` runs a pipeline on a cron schedule in a **long-running foreground process**. Stop it with Ctrl-C or SIGTERM; an in-flight run drains gracefully before the process exits.

```bash
faucet schedule pipeline.yaml                        # run on cron schedule, foreground
faucet schedule pipeline.yaml --once                 # run exactly once now, then exit
faucet schedule pipeline.yaml --env-file prod.env    # inject env before loading config
faucet schedule pipeline.yaml --no-env-file          # disable .env auto-loading
```

The config must contain a top-level `schedule:` block (a config without one is rejected with a clear hint pointing to `faucet run`). Requires the `schedule` Cargo feature (included in the default `full` build).

#### `schedule:` block grammar

```yaml
schedule:
  cron: "0 2 * * *"               # REQUIRED. 5-field standard Unix cron, or 6-field with leading seconds.
  timezone: "UTC"                 # IANA name (e.g. America/Los_Angeles). Default UTC.
  overlap_policy: skip            # skip (default) | queue | forbid
  max_runs: null                  # null = forever; N = stop cleanly (exit 0) after N *successful* runs
  max_consecutive_failures: null  # null = never exit on failure; N = exit non-zero after N straight failures
  on_failure: continue            # continue (default) | stop (exit non-zero on first failed run)
  start_immediately: false        # run once on startup before waiting for the first tick
  run_timeout_secs: null          # optional per-run kill switch (seconds); a timed-out run counts as failed
  shutdown_grace_secs: 30         # SIGTERM: await the in-flight run this long, then abort
```

**Cron syntax:** standard 5-field Unix cron (`MIN HOUR DOM MON DOW`) or 6-field with a leading seconds field (`SEC MIN HOUR DOM MON DOW`). Examples:

| Expression | Meaning |
|------------|---------|
| `0 2 * * *` | Every night at 02:00 |
| `*/15 * * * *` | Every 15 minutes |
| `0 9 * * 1-5` | Weekdays at 09:00 |
| `*/30 * * * * *` | Every 30 seconds (6-field) |

Bad cron expressions, unknown timezones, `max_runs: 0`, and a cron that can never fire all fail fast with a clear `config error: schedule: …` message.

**Timezone and DST:** all ticks are computed on UTC instants with timezone-correct wall times via `chrono-tz`. Fall-back repeated hours fire once; spring-forward skipped hours roll to the next valid time. The loop re-checks the wall clock every ≤30 s so NTP steps and VM freezes can't drift a fire by more than ~30 s.

**Missed ticks:** skipped, not backfilled. If a run ran long or the box was down, the scheduler fires at the next due time — no catch-up storm.

#### Overlap policy

| Policy | Behaviour |
|--------|-----------|
| `skip` (default) | Drop the tick if a run is already in flight. Increment `faucet_schedule_overlaps_total`. |
| `queue` | Buffer one missed tick; run it when the current run finishes. Further misses collapse to one queued tick (in-memory only — lost on restart). |
| `forbid` | Exit non-zero immediately if a second run would overlap. |

#### Failure model

Two independent knobs control what happens when a run fails:

| `on_failure` | `max_consecutive_failures` | Behaviour |
|---|---|---|
| `continue` (default) | `null` | Tolerates all failures; never exits on failure alone. Alert on `faucet_schedule_consecutive_failures`. |
| `continue` | `N` | Tolerates up to N−1 straight failures; exits non-zero after N consecutive failures (a success resets the counter). Pair with a supervisor (`systemd Restart=on-failure`, Kubernetes) for "tolerate blips, restart on sustained outage". |
| `stop` | any | Exits non-zero on the **first** failed run. |

#### Connectors and state

Connectors are rebuilt fresh per run so no idle connections pool up. The shared `auth:` catalog (cached tokens) is reused across ticks. Resumability rides faucet's existing per-page `StateStore` bookmark: the scheduler itself keeps no run-state across restarts, so a crash or SIGKILL resume from the last persisted bookmark on the next start.

#### Metrics

All metrics carry the `{pipeline}` label. Register a Prometheus listener via the `observability:` block as usual.

| Metric | Type | Description |
|--------|------|-------------|
| `faucet_schedule_runs_total{pipeline,outcome}` | Counter | `outcome` ∈ `{ok, err, skipped}` |
| `faucet_schedule_overlaps_total{pipeline,policy}` | Counter | Overlap events; `policy` ∈ `{skip, queue, forbid}` |
| `faucet_schedule_next_tick_unix_seconds{pipeline}` | Gauge | Unix timestamp of the next scheduled tick |
| `faucet_schedule_runs_in_flight{pipeline}` | Gauge | 0 or 1 — whether a run is currently executing |
| `faucet_schedule_consecutive_failures{pipeline}` | Gauge | Resets to 0 on a successful run |
| `faucet_schedule_heartbeat_unix_seconds{pipeline}` | Gauge | Updated every loop wake (≤30 s); alert `time() − heartbeat > 90` to detect a stuck scheduler |
| `faucet_schedule_last_run_started_unix_seconds{pipeline}` | Gauge | |
| `faucet_schedule_last_run_completed_unix_seconds{pipeline}` | Gauge | |
| `faucet_schedule_last_run_duration_seconds{pipeline}` | Gauge | |
| `faucet_schedule_run_lateness_seconds{pipeline}` | Histogram | `actual_start − scheduled_for` — how late the run fired |

Each run also emits a `faucet.schedule.run` tracing span (attributes: `run_ordinal`, `scheduled_for_unix_seconds`, `tick_unix_seconds`) wrapping the inner pipeline spans.

#### Exit codes

| Condition | Exit code |
|-----------|-----------|
| `max_runs` reached | 0 |
| SIGTERM / SIGINT graceful drain | 0 |
| `on_failure: stop` — first run failed | non-zero |
| `max_consecutive_failures` reached | non-zero |
| `overlap_policy: forbid` overlap | non-zero |
| Bad cron / timezone / config | non-zero |

#### Build feature

```bash
cargo install faucet-cli                         # schedule included (in full)
cargo install faucet-cli --features schedule     # explicit, no-default-features build
```

### `faucet serve`

`faucet serve` runs a **long-running HTTP control plane**: it accepts pipeline configs over REST, executes them under bounded concurrency (reusing the same executor as `faucet run`), and exposes submit / poll / list / cancel / SSE-log endpoints plus `/healthz`, `/readyz`, and `/metrics`. It takes **no config file** — configs arrive per request.

```bash
FAUCET_SERVE_AUTH_TOKEN=s3cret faucet serve --listen 0.0.0.0:8080      # bearer auth (preferred)
faucet serve --no-auth                                                 # explicit no-auth opt-in (required if no token)
faucet serve --history sqlite:/var/lib/faucet/runs.db                  # durable run history
faucet serve --default-config defaults.yaml                            # merge workspace defaults under every run
```

Auth is mandatory: without `--auth-token`/`FAUCET_SERVE_AUTH_TOKEN` **and** without `--no-auth`, startup fails (an unauthenticated server is never accidental). The default bind is loopback.

| Flag | Purpose |
|------|---------|
| `--listen <addr>` | Bind address (default `127.0.0.1:8080`; env `FAUCET_SERVE_LISTEN`). |
| `--auth-token <t>` / `--no-auth` | Bearer token (prefer the env var) or explicit no-auth opt-in. |
| `--auth-config <path>` | RBAC principals file (`{ name, token, role }`; roles `viewer`/`operator`/`admin`) — role enforcement + admin-only `GET /v1/audit`. Mutually exclusive with `--auth-token`/`--no-auth`. |
| `--max-concurrent-runs` / `--max-queued-runs` | Concurrency + queue caps (submit past the queue → 429 + `Retry-After`). |
| `--history <url>` | `postgres://…` / `sqlite:…` for durable history (`serve-history-postgres` / `serve-history-sqlite`; default in-memory). |
| `--default-config <path>` | Workspace defaults merged **under** every submitted run. |
| `--cors-origin <o>` | Allow-list a browser origin (repeatable; CORS off by default). |
| `--lease-ttl-secs <n>` | Run-ownership lease TTL (default `30`). Set above your worst-case GC/IO stall to avoid false-reclaim of paused instances. |
| `--cluster` | Enable clustered execution: all instances sharing the same `--history` DB pull-balance `pending` runs and provide crash-failover. Requires a persistent `--history` backend. See the [cluster cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/cluster.html). |
| `--cluster-poll-secs <n>` | Claim-loop poll interval in seconds (default `2`). Also the maximum cross-instance cancel propagation lag. |
| `--cluster-max-attempts <n>` | Maximum attempts per run (including crash-failovers) before it is poisoned as `failed` (default `3`). |
| `--triggers <path>` | Path to a triggers file (YAML) defining event-driven watchers. Requires the `triggers` Cargo feature. See [Event-driven triggers](#event-driven-triggers-triggers). |
| `--local-output-retention-days <n>` / `--local-output-in-flight-grace-secs <n>` | Retention GC for the local files a run's sinks wrote (jsonl/csv/parquet): the window (default `7`; `0` disables the sweep) and the mid-write guard (default `60`). Only files faucet recorded as its own sink outputs are ever deleted. Requires the `catalog` feature. |
| `--preview-local-outputs` | Serve **dataset previews** of those local files — the console reads a tracked output's first N rows back (`FAUCET_SERVE_PREVIEW_LOCAL_OUTPUTS`). **Off by default**: it returns file contents over HTTP, so it is a local-testing convenience. Caps: `--preview-default-rows` (soft, default `500`) and `--preview-max-rows` (hard, default `5000`; a larger `row_count_to_load` — including `all` — is clamped to it). `--preview-max-rows 0` lifts the ceiling so `row_count_to_load=all` reads a whole dataset; the read is still bounded by a response-size budget and a deadline, and a partial answer names the bound that stopped it. |
| `--body-limit-bytes`, `--shutdown-grace-secs`, `--retain-terminal-runs-secs`, `--idempotency-retention-secs`, `--probe-timeout-secs` | Tuning knobs. |

#### Event-driven triggers (`--triggers`)

`--triggers <file>` loads a static triggers file at startup and spawns long-lived watcher tasks.
When a watcher fires, it enqueues a run through the same pipeline as `POST /v1/runs`, reusing the
full queue / idempotency / history / metrics machinery.

Three trigger types are available:

| Type | What it watches | Requires feature |
|------|----------------|-----------------|
| `object_arrival` | New S3 or GCS objects under a prefix | `triggers-object-store` |
| `webhook` | `POST /v1/triggers/{name}` (bearer-gated) | `triggers` |
| `queue_depth` | Redis list/stream depth or Kafka consumer-group lag | `triggers-redis` / `triggers-kafka` |

```yaml
# triggers.yaml
version: 1
triggers:
  # Fire for every new S3 object; ${trigger.object_key} is injected into the run config
  - name: load-files
    type: object_arrival
    config: ./my_pipeline.yaml
    store: { type: s3, bucket: my-bucket, prefix: incoming/, region: us-east-1 }
    poll_interval_secs: 30
    mode: per_object
    start_at: now

  # Fire when POST /v1/triggers/sync-hook is called
  - name: sync-hook
    type: webhook
    config: ./csv_to_jsonl.yaml
    dedupe_header: Idempotency-Key

  # Fire when a Redis list depth reaches >= 1
  - name: drain-jobs
    type: queue_depth
    config: ./redis_to_sqlite.yaml
    queue: { type: redis, url: redis://localhost:6379, key: jobs, kind: list }
    threshold: 1
    poll_interval_secs: 15
```

```bash
# Start with event-driven triggers
FAUCET_SERVE_AUTH_TOKEN=s3cret \
faucet serve --listen 0.0.0.0:8080 --triggers triggers.yaml

# Fire the webhook trigger manually
curl -XPOST http://localhost:8080/v1/triggers/sync-hook \
     -H "Authorization: Bearer s3cret" \
     -H "Idempotency-Key: run-001" -d '{}'

# Print the JSON Schema for the triggers file format
faucet schema triggers
```

Each trigger emits `faucet_serve_triggers_fired_total{trigger,type}`,
`faucet_serve_trigger_healthy{trigger,type}`, and related Prometheus metrics.
`GET /readyz` includes a `triggers` array showing per-watcher health.

See the [triggers cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/triggers.html)
for detailed walkthroughs and the [triggers reference](https://faucet-hq.github.io/faucet-stream/reference/triggers.html)
for the full field reference.

Requires the `triggers` feature family (included in `full`):

```bash
cargo install faucet-cli --features "triggers,triggers-object-store,triggers-redis,triggers-kafka"
cargo install faucet-cli --features full    # all features
```

#### Clustered execution (`--cluster`)

`--cluster` turns a fleet of `faucet serve` processes into a pull-balanced, self-healing
cluster. Instances share a single Postgres or SQLite history database; submissions are written
as `pending` in the shared DB and any instance with spare capacity atomically claims and runs
them. If an instance crashes, a survivor's lease loop detects the expired-lease run and
re-queues it (up to `--cluster-max-attempts`). All instances must be **homogeneous** — same
container image, env vars, and secrets access — because the claiming instance re-resolves
`${env:…}`/`${secret:…}` directives with its own credentials at execution time.

```bash
# Node A
FAUCET_SERVE_AUTH_TOKEN=s3cret \
faucet serve --cluster \
             --history 'postgres://faucet:pw@db/faucet' \
             --listen 0.0.0.0:8080

# Node B (same DB)
FAUCET_SERVE_AUTH_TOKEN=s3cret \
faucet serve --cluster \
             --history 'postgres://faucet:pw@db/faucet' \
             --listen 0.0.0.0:8081
```

See the [cluster cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/cluster.html)
for the full lifecycle, delivery guarantees, and Kubernetes deployment notes.

Submit a run:

```bash
curl -XPOST localhost:8080/v1/runs -H "Authorization: Bearer s3cret" \
  -H 'content-type: application/json' \
  -d '{"config":"version: 1\npipeline:\n  source: {type: csv, config: {path: in.csv}}\n  sink: {type: jsonl, config: {path: out.jsonl}}\n","name":"adhoc","idempotency_key":"k1"}'
```

> ⚠️ **Security:** `serve` executes arbitrary client-supplied configs with the server's identity — secrets, files, and network egress (SSRF). Run single-tenant, authenticated, behind egress controls; terminate TLS at a proxy. See the [serve cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/serve.html) and [HTTP API reference](https://faucet-hq.github.io/faucet-stream/reference/http-api.html).

Requires the `serve` Cargo feature (included in `full`):

```bash
cargo install faucet-cli --features serve
cargo install faucet-cli --features "serve,serve-history-postgres,serve-history-sqlite"
```

#### Optional embedded web console (`serve-ui`)

Build with `serve-ui` to serve a browser-based web console at `/` alongside the
REST API. The console gives you a Runs dashboard, Run detail with live SSE logs,
a Submit view (raw YAML/JSON editor + schema-driven wizard), and a Schemas
explorer — all backed by the same bearer-gated `/v1` API.

```bash
cargo install faucet-cli --features serve-ui    # serve-ui implies serve
FAUCET_SERVE_AUTH_TOKEN=s3cret faucet serve --listen 127.0.0.1:8080
# Open http://127.0.0.1:8080/ in a browser; paste the bearer token when prompted.
```

Pass `--no-ui` to disable the console at runtime without rebuilding. The `serve-ui`
feature also adds three bearer-gated endpoints: `GET /v1/schemas` (connector
catalog), `GET /v1/schemas/{kind}/{name}` (one JSON Schema), and `POST /v1/doctor`
(validate + probe a config without running it). These endpoints are available
regardless of `--no-ui`.

With the `catalog` feature the console's **Datasets** page also lists the local
files the server's sinks wrote, with per-output cleanup controls — and, on a server
started with `--preview-local-outputs`, a **Preview** that renders each tracked
jsonl / csv / parquet output's rows as a table — 500 by default, any number up to
the server's ceiling, or **All rows**. The preview is a *source-backed capped
read* (the file is read back through the matching source connector and the read
stops at the bound rather than truncating afterwards), there is no paging
(sequential files have no row index, so "more" is just a larger limit), it never
accepts a path from the request — only the id of an output the server already
tracks — and it is off by default because it returns file contents over HTTP.

See the [web console guide](https://faucet-hq.github.io/faucet-stream/cookbook/web-console.html)
for the full walkthrough.

#### Change approvals (`--require-approval`)

`POST /v1/changes` stores a proposed `run` / `template_register` /
`template_launch` with its plan and runs it only once the approvers named by
the `approvals:` block of `--auth-config` approve it (#703). faucet re-plans
at approval and refuses to run a change whose plan moved underneath the
approver (`invalidated`). `faucet serve --require-approval run` turns every
`POST /v1/runs` and template trigger into a pending request; the MCP
`propose_run` / `propose_template` tools let agents file requests. See the
[approvals cookbook](../docs/book/src/cookbook/approvals.md).

### `faucet mcp` / `faucet serve --mcp`

Expose faucet as an **MCP (Model Context Protocol) server** so an LLM agent can
discover connectors, read config schemas, scaffold + validate + preview a
pipeline, and — behind an explicit opt-in — run one. Requires a build with the
`mcp` feature (included in `full`).

- **stdio** — `faucet mcp` (add `--allow-mutations` to expose `run_pipeline`).
  Newline-delimited JSON-RPC 2.0 on stdin/stdout; logs go to stderr. For local
  agents (Claude Desktop / Code):

  ```json
  { "mcpServers": { "faucet": { "command": "faucet", "args": ["mcp"] } } }
  ```

- **HTTP** — `faucet serve --mcp` mounts a `/mcp` route that inherits serve's
  bearer-auth + RBAC + audit. Add `--mcp-allow-mutations` for the mutating tool
  (a caller still needs the `RunWrite` scope).

Tools: `list_connectors`, `get_connector_schema`, `scaffold_config`,
`validate_config`, `preview` (read-only, ≤100 rows), and the gated
`run_pipeline` (`dry_run: true` validates + previews only). Secret material is
redacted from all tool output. See the
[MCP guide](https://faucet-hq.github.io/faucet-stream/cookbook/mcp.html).

### `faucet completions`

`faucet completions <shell>` prints a static tab-completion script for `bash`,
`zsh`, `fish`, `powershell`, or `elvish`:

```bash
faucet completions zsh > ~/.zfunc/_faucet     # then ensure ~/.zfunc is on $fpath
faucet completions bash > /etc/bash_completion.d/faucet
```

For **dynamic** completion — computed by the binary at completion time, so it
reflects the connectors compiled into *this* build and reads the local config —
enable the `COMPLETE` hook instead of installing a static script:

```bash
echo 'source <(COMPLETE=zsh  faucet)' >> ~/.zshrc      # zsh
echo 'source <(COMPLETE=bash faucet)' >> ~/.bashrc     # bash
echo 'COMPLETE=fish faucet | source'  >> ~/.config/fish/config.fish
```

Dynamic completion then offers runtime-aware candidates:

- `faucet schema source|sink|transform <TAB>` — the connectors/transforms in this build.
- `faucet run --select|--only|--skip <TAB>` — matrix row ids from the `faucet.yaml` in the cwd.
- `faucet run --status <TAB>` — the readiness ladder; `--tag <TAB>` — the config's tags.

The config-aware providers are read-only and best-effort: they parse + expand
the local config but never resolve secrets, touch the network, or open a
connector, and yield no suggestions when no config is present.

### `faucet init`

`faucet init` writes a starter `pipeline.yaml` by walking each selected connector's JSON Schema. Required fields are surfaced with a `# REQUIRED` comment and a typed placeholder (`""`, `0`, `false`, `[]`, `{}`); optional fields are commented out so connector-level defaults stay in force. Enum-typed fields list valid values in the trailing comment. Tagged-enum blocks (the `#[serde(tag = "type")]` shape used by `auth:`, `pagination:`, BigQuery `credentials:`, etc.) inline the chosen variant and emit every other variant as a commented-out "Alternative variants" block right below it — so users can switch auth modes (or pagination, or credentials) without leaving the file to consult `faucet schema`. Run `faucet init --interactive` (requires `--features cli-interactive`) to be prompted for each variant up front.

```bash
faucet init                                              # rest → jsonl, name = my-pipeline
faucet init my-job                                       # rest → jsonl, name = my-job
faucet init my-job --source postgres --sink bigquery     # postgres → bigquery
faucet init --source rest --sink jsonl -o config.yaml    # custom output path
faucet init --force                                      # overwrite pipeline.yaml in cwd
faucet init --interactive                                # TTY prompts (requires --features cli-interactive)
```

Flags:

| Flag | Purpose |
|------|---------|
| `name` (positional) | Pipeline name written to the generated file's `name:`. Defaults to `my-pipeline`. |
| `--source <kind>` | Source connector to scaffold (e.g. `rest`, `postgres`, `s3`). Defaults to `rest`. |
| `--sink <kind>` | Sink connector to scaffold (e.g. `jsonl`, `bigquery`). Defaults to `jsonl`. |
| `--output, -o <path>` | Output file path. Defaults to `pipeline.yaml`. |
| `--force` | Overwrite an existing file at the output path. |
| `--interactive` | Prompt for kinds via `inquire` on a TTY; falls back to `--source`/`--sink` otherwise. Requires the `cli-interactive` build feature. |

Run `faucet list` to see every kind that's compiled into your build of `faucet`. Use `faucet schema source <kind>` (or `sink <kind>`, or `transform <name>`) to see the full JSON Schema if a field's truncated description doesn't tell you enough.

### Config + `.env` auto-discovery

`run`, `validate`, and `preview` all auto-discover their inputs from the current directory:

| What | Behaviour |
|------|-----------|
| Config path omitted | Probe `faucet.yaml` → `faucet.yml` → `faucet.json` in cwd; first match wins. |
| `.env` in cwd | Loaded automatically before any `${env:VAR}` interpolation runs. |
| `--env-file <path>` | Forces a specific file. The file must exist or the command errors. Works in both YAML mode and `--from-env`. |
| `--no-env-file` | Disables `.env` auto-loading. Cannot be combined with `--env-file`. |
| Process env vs `.env` | Process env always wins — `.env` only fills in unset variables. |

So `cd into-your-project && faucet run` is the short form for `faucet run --env-file .env faucet.yaml` whenever both files are present.

## Named source and sink templates

Declare reusable connector definitions under `pipeline.sources` and
`pipeline.sinks`, then pick from them per matrix row via `ref: <name>`.
Combined with the top-level `vars:` block, this is the recommended shape
for any config with more than one matrix row.

```yaml
version: 1
name: api_ingest

vars:                                # optional shared constants
  api_base: https://api.example.com
  api_token: ${env:API_TOKEN}

pipeline:
  sources:                           # named source templates
    api:
      type: rest
      config:
        base_url: ${vars.api_base}
        auth: { type: Bearer, token: ${vars.api_token} }
        records_path: $.data[*]
  sinks:                             # named sink templates
    archive:
      type: jsonl
      config: { append: false }

matrix:
  - id: users
    source: { ref: api, config: { path: /v1/users } }
    sink:   { ref: archive, config: { path: users.jsonl } }
  - id: orders
    source: { ref: api, config: { path: /v1/orders } }
    sink:   { ref: archive, config: { path: orders.jsonl } }
```

### Resolution order

Load-time interpolation runs in this order:

1. `${env:VAR}` / `${file:PATH}` / `${secret:VAR}` — resolved during the raw text pass.
2. `${vars.X}` — resolved against the top-level `vars:` block. Vars may reference other vars; cycles surface as `InterpolationCycle`.
3. `${sources.NAME.PATH}` and `${sinks.NAME.PATH}` — resolved against the post-vars-substitution template bodies. Useful for copying constants between templates without restating them. A template may reference another template (including across the source/sink namespaces), and such chains are followed to their terminal value; mutual or circular references surface as `InterpolationCycle` rather than resolving to literal token text.
4. `${row_id.path}` — left literal; resolved at runtime against parent records (per-record fan-out).

### Backwards compatibility

The legacy singular `pipeline.source:` / `pipeline.sink:` continues to work
unchanged. Internally they register as a template named `default`. A
matrix row without a `ref:` field inherits the `default` template
(matching the pre-templates merge semantics). You can mix the two
styles — declare some templates via `pipeline.sources.*` and a
fallback via `pipeline.source:` — but the `default` slot can only be
defined once.

See [`examples/templates_dry_rest.yaml`](examples/templates_dry_rest.yaml) and
[`examples/templates_users_posts.yaml`](examples/templates_users_posts.yaml) for
end-to-end examples of this pattern.

## Config composition

Factor shared connection / sink / transform pieces out of each file and
recombine them at load time. Three mechanisms, all resolved when the file is
read (**before** any `${...}` interpolation):

| Mechanism | Form | Effect |
|-----------|------|--------|
| `extends:` | `extends: ./base.yaml` (or a list) | Inherit one or more base files; the child deep-merges on top. |
| `profiles:` | `profiles: { dev: {…}, prod: {…} }` | Named overlays, selected with `--profile NAME` / `FAUCET_PROFILE` (flag wins). |
| `!include` | `key: !include ./frag.yaml` | Substitute a YAML fragment at any node (**YAML only**). |

```yaml
# app.yaml — inherits a base, then pulls in a reusable transform chain.
extends: ./base.yaml
pipeline:
  transforms: !include ./transforms.yaml
```

```bash
faucet run app.yaml --profile prod                 # select an overlay
faucet validate app.yaml --show-composed --profile prod   # print the merged config
```

Precedence (last wins): `extended base → child document → profile → matrix row`,
all via the same deep-merge as `matrix` rows. `faucet validate --show-composed`
prints the fully composed document (bases merged, profile applied, fragments
substituted, `extends:`/`profiles:` metadata stripped) before interpolation.

**Composition is file-loads-only** — `extends`/`profiles`/`!include` apply to
configs read from disk (`run`/`validate`/`preview`/`doctor`/`schedule`), **not**
to configs submitted to `faucet serve` over HTTP (a submitted body is a single
self-contained document with no filesystem access). See
[`examples/compose/`](examples/compose/) for an end-to-end example, and the
[docs-site composition cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/composition.html)
for the full walkthrough.

## Config shape

```yaml
version: 1
name: github_to_jsonl

pipeline:
  source:
    type: rest
    config:
      base_url: https://api.github.com
      path: /repos/faucet-hq/faucet-stream/issues
      method: GET
      auth:
        type: ApiKey
        header: Authorization
        value: Bearer ${env:GITHUB_TOKEN}
      query_params: {state: open}
      pagination:
        type: LinkHeader
      max_retries: 3
      retry_backoff: 1
      tolerated_http_errors: []
      replication_method: { type: FullTable }
      primary_keys: ["id"]
      partitions: []
      schema_sample_size: 100
  transforms:
    - type: keys_case
      config: { mode: snake }
  sink:
    type: jsonl
    config:
      path: ./out/issues.jsonl
  state:
    type: file
    config:
      path: ./.faucet-state
```

`pipeline:` is the only required block. Anything you would have written at the top level pre-#54 (`source:`, `transforms:`, `sink:`, `state:`) now lives one level deeper inside `pipeline:`. Validation rejects the old shape with a clear hint.

### Matrix mode — run many invocations from one config

Add a `matrix:` block to run multiple invocations from the same base. Each row is **deep-merged** into `pipeline:` (objects merge recursively, arrays replace wholesale, scalars replace). Rows with `parent:` become children that fan out one invocation per record produced by the parent row. Rows with `depends_on:` wait for other rows to finish before starting (pure ordering, no record hand-off). Rows with `discover:` / `for_each:` fan out over the **cartesian product** of value-sets enumerated at run time (see below).

```yaml
version: 1
name: api_to_warehouse

pipeline:
  source:
    type: rest
    config:
      base_url: https://api.example.com
      auth: { type: Bearer, token: ${env:API_TOKEN} }
      pagination: { type: PageNumber, param_name: page, page_size: 100 }
  sink:
    type: bigquery
    config:
      service_account_key_path: ${env:GCP_SA_PATH}
      project_id: my-project

matrix:
  # Independent roots — different paths/tables, shared auth + sink type.
  - id: users
    source: { config: { path: /v1/users } }
    sink:   { config: { dataset: raw, table: users } }
  - id: products
    source: { config: { path: /v1/products } }
    sink:   { config: { dataset: raw, table: products } }

  # DAG fan-out — one child invocation per parent record.
  - id: user_posts
    parent: users
    source: { config: { path: /v1/users/${users.id}/posts } }
    sink:   { config: { dataset: raw, table: user_posts } }

  # Completion ordering — starts only after `users` AND `products` succeed.
  - id: order_facts
    depends_on: [users, products]
    source: { config: { path: /v1/orders } }
    sink:   { config: { dataset: marts, table: order_facts } }

execution:
  max_concurrent: 8
  on_error: continue   # or `stop`
```

#### `discover:` / `for_each:` — discovery-driven fan-out (#501)

A `discover:` row enumerates a **value-set at run time** (build source → drain →
project `select` → dedup, no sink), and a `for_each: [dims]` row runs once per
tuple of the **cartesian product** of those dimensions, with `${<dim>.<alias>}`
substituted into its config — "sync this report once per `{subsidiary} ×
{custom-field}`". The discovery source is a `{ ref }` to a `pipeline.sources`
template or a standalone `{ type, config }`; the product is bounded by
`MAX_MATRIX_PRODUCT` (10 000). See `docs/book/src/reference/config.md` and
`cli/examples/discovery_matrix.yaml`.

#### `depends_on:` — completion ordering between rows

`depends_on: [row_id, …]` makes a row wait until **every listed row's
invocations finish successfully** before it starts. Unlike `parent:`, no
records are consumed and there is no per-record fan-out — it is pure run
ordering ("load dimensions, then facts"; "ingest raw, then run the rollup
whose source reads what the first row wrote").

- Rows whose dependencies are all satisfied run concurrently as usual.
- A failed or skipped dependency **skips** the dependent row (and, in turn,
  its own children and dependents). The skip is logged; the run's exit code
  reflects the original failure.
- Waiting on a row means waiting for that row's own invocations. To also wait
  for its per-record children, list the child rows explicitly
  (`depends_on: [dims, dim_details]`).
- `parent:` and `depends_on:` compose on the same row; the parent edge is
  itself an implicit dependency.
- Unknown ids, self-dependencies, and cycles through any mix of `parent:` /
  `depends_on:` edges are rejected at load time (`faucet validate` catches
  them).

#### Selecting which rows run — `status`, `tags`, and the selection flags

Register many rows, run a few. Selection is resolved **after** expansion and never
changes a row's state key (`{name}::{row_id}`), so bookmarks stay identical across a
full run and any selected subset. Four axes compose through one formula:

```
1. eligible  = status gate ({mandatory, active} ∪ --status)
2. narrowed  = (eligible ∩ --tag) ∪ (--select / --only by id)
3. parents   = apply include_parents policy to narrowed
4. run set   = parents − (--skip)
```

**Readiness ladder (`status:`)** — a field on the row's **source** (template or
`source:` override; deep-merges like any scalar). Default when absent is `active`, so
existing configs are unchanged.

| `status` | runs when… |
|---|---|
| `mandatory` | **always** — removable only by an explicit `--skip <id>` |
| `active` | **by default** (bare `faucet run`) — the absent-default |
| `available` | only under `--status available` |
| `draft` | only under `--status draft` |
| `archived` | only under `--status archived` |

**Tags (`tags:`)** — free-form `^[a-z0-9][a-z0-9_-]*$` labels on a row (union-merged
with the source template's `tags:`). Orthogonal to `status`: tags *narrow within* the
eligible set — `--tag finance` never resurrects a parked (`available`/`draft`/`archived`)
row; raise the gate with `--status` for that.

**Flags** (all repeatable and/or comma-joined; each has an env var; the flag wins):

| Flag | Env | Effect |
|---|---|---|
| `--select <id>` | `FAUCET_SELECT` | Run rows whose id exactly matches — force-included **regardless of status/tags**. |
| `--only <glob>` | — | Like `--select` but glob-matched (`--only 'timeoff_*'`); also bypasses the status gate. |
| `--skip <id\|glob>` | `FAUCET_SKIP` | Remove matching rows, applied last. A `mandatory` row only via exact `--skip <id>`. |
| `--status <tier>` | `FAUCET_STATUS` | Additively widen the eligible set (`{mandatory, active}` ∪ these). |
| `--tag <t>` | `FAUCET_TAGS` | Keep eligible rows carrying any listed tag (union). |
| `--include-parents <off\|eligible\|all>` | `FAUCET_INCLUDE_PARENTS` | Ancestor policy (below). Also `selection.include_parents:` in config. |

Unknown tokens, an empty run set, and a missing required ancestor are **hard, fail-fast
errors** (no partial run). Available on `run` (applies), `validate` (applies + prints
each row's status/tags and the RUN/skip decision), and `preview` (first root of the
selected set).

```yaml
matrix:
  - id: people
    source: { ref: hibob, status: active,    config: { path: /v1/people } }   # runs by default
    tags: [core, daily]
  - id: payroll
    source: { ref: hibob, status: mandatory, config: { path: /v1/payroll } }  # always runs
    tags: [finance]
  - id: audit
    source: { ref: hibob, status: available, config: { path: /v1/audit } }    # opt-in
    tags: [finance]
  - id: beta
    source: { ref: hibob, status: draft,     config: { path: /v2/beta } }     # dev-only

selection:
  include_parents: off   # off (default) | eligible | all
```

| Command | Runs |
|---|---|
| `faucet run cfg.yaml` | people, payroll |
| `faucet run cfg.yaml --tag finance` | payroll *(audit is finance but `available`)* |
| `faucet run cfg.yaml --status available --tag finance` | payroll, audit |
| `faucet run cfg.yaml --select beta` | beta *(draft, forced by name)* |
| `faucet run cfg.yaml --only 'p*'` | people, payroll |

#### `include_parents` — parent/dependency inclusion policy

When a selected row's `parent:` / `depends_on:` ancestor is **not** independently in the
run set, this single policy decides the outcome (config `selection.include_parents:` or
`--include-parents`, flag > env > config > built-in default `off`):

| policy | behaviour |
|---|---|
| `off` (**default**, strict) | Hard error naming every `dependent → ancestor` pair; select the ancestor by id or loosen the policy. |
| `eligible` | Auto-include required ancestors whose status is eligible (logged); error if a required ancestor is parked. |
| `all` | Include every required ancestor regardless of status (parked ones pulled in with a warning); never errors on ancestors. |

`--select <id>` by name always satisfies a dependency regardless of policy. Skipping a
row that a surviving row depends on is rejected (no orphaned children).

#### Deep-merge rules

- Objects merge recursively (overlay keys win on collision).
- Arrays replace wholesale — no element-merging, no concat. If a row needs to add to an inherited list, redeclare it.
- Scalars / `null` / numbers / booleans replace.

#### Two-stage interpolation

Tokens are resolved in two passes:

| Token | When |
|-------|------|
| `${env:VAR}` | Load-time, before YAML parsing. |
| `${file:./path}` | Load-time. File contents trimmed of trailing whitespace. Capped at 1 MiB — this is for small token/secret/cert files, not bulk data. |
| `${secret:VAR}` | Load-time. Alias for `${env:VAR}` today (no at-rest redaction). |
| `${vault:<path>[#field]}` | Load-time. HashiCorp Vault KV v2. Requires `VAULT_ADDR` + `VAULT_TOKEN`. `#field` extracts one key from a JSON secret. Build with `--features secrets-vault`. |
| `${aws-sm:<name-or-ARN>[#field]}` | Load-time. AWS Secrets Manager. Auth: `aws-config` default chain (env / profile / instance / web-identity). Build with `--features secrets-aws-sm`. |
| `${gcp-sm:projects/<p>/secrets/<s>/versions/<v>}` | Load-time. GCP Secret Manager (`versions/latest` ok). Auth: Application Default Credentials. Build with `--features secrets-gcp-sm`. |
| `${azure-kv:<vault>/<secret>[/<version>]}` | Load-time. Azure Key Vault. Auth: `AZURE_*` env / managed identity / `az login`. Build with `--features secrets-azure-kv`. |
| `${row_id.dotted.path}` | Run-time, per parent record. The `row_id` must be the id of another matrix row. |
| `${now.*}` | Run-time, per invocation. Injects the run's wall time into source and sink config values. See below. |

A token's form decides its meaning: a **colon** marks a load-time directive (`${env:VAR}`), while a **dot or nothing** marks a deferred row-id reference (`${users.id}`). The same rule is used by both `faucet validate` and `faucet run`, so a token like `${env.foo}` (a dot, not a colon) is consistently treated as a reference to row id `env` and rejected at validate-time rather than failing only at run-time.

`$${` escapes a literal `${`. Reserved row ids that can never appear in `matrix.id`: `env`, `file`, `secret`, `matrix`, `pipeline`, `now`.

#### `${now.*}` — run-clock interpolation

Inject the invocation's wall time into any **source or sink** config value. Common use case: writing to a dated output path so each scheduled run lands in its own partition.

| Token | Example | Notes |
|-------|---------|-------|
| `${now.date}` | `2026-03-08` | `YYYY-MM-DD` |
| `${now.datetime}` / `${now.iso}` | `2026-03-08T14:05:09+00:00` | RFC 3339 |
| `${now.year}` | `2026` | Zero-padded |
| `${now.month}` | `03` | Zero-padded (01–12) |
| `${now.day}` | `08` | Zero-padded (01–31) |
| `${now.hour}` | `14` | Zero-padded (00–23) |
| `${now.minute}` | `05` | Zero-padded (00–59) |
| `${now.second}` | `09` | Zero-padded (00–59) |
| `${now.unix}` | `1741442709` | Epoch seconds |
| `${now.strftime.<fmt>}` | `2026/03/08/14` | Arbitrary chrono strftime, e.g. `${now.strftime.%Y/%m/%d/%H}` |

An unknown token (e.g. `${now.foo}`) is a config error at run time. `${now.*}` is **not** resolved in `state:`, `dlq:`, `transforms:`, or the `auth:` / `vars:` blocks.

**Clock source:**

- `faucet run` — process start time in UTC, or `--clock <RFC3339|YYYY-MM-DD>` for backfills (a bare date means midnight UTC).
- `faucet schedule` — the tick's scheduled time in the schedule's `timezone`; `${now.date}` therefore matches the timezone the cron fires in, not UTC.

**Backfills:**

```bash
faucet run --clock 2026-03-01 pipeline.yaml          # midnight UTC
faucet run --clock 2026-03-01T02:00:00-08:00 pipeline.yaml  # precise timestamp
```

**Local file sinks** (JSONL, CSV) create missing parent directories automatically, so dated subdirectory paths like `./data/dt=${now.date}/part.jsonl` work without pre-creating the tree.

**Security note.** Pipeline configs are trusted input: `${file:...}` reads any path the process can access (capped at 1 MiB), and `${env:}`/`${secret:}` inject process environment values. Connector-config deserialization errors are scrubbed (double-quoted values redacted, length-capped) before they reach logs so an injected secret can't leak through an error message, but treat configs and their resolved values as sensitive.

#### Execution

- `max_concurrent` bounds total in-flight invocations (roots + per-parent-record children compete for one budget). Default: `min(num_cpus, 4)`.
- `on_error: continue` (default) — a failed invocation is logged, its subtree is skipped, every sibling already running keeps running to completion. The process exits non-zero if any invocation failed.
- `on_error: stop` — first failure halts the entire run. In-flight invocations are **cooperatively cancelled**: each stops at its next page boundary and flushes its sink, so a buffered sink (e.g. Parquet, whose footer is only written on flush) commits the rows written so far rather than orphaning the whole file (#146 H16). Any invocation still stuck *mid-write* after a short flush grace is then hard-aborted, and pending invocations waiting on a permit stop before doing real work. Honours `max_concurrent` like `continue` does.

> **Caveat for `stop`:** even with the cooperative flush, cancelling between pages can leave partial state in the sink — only the pages written-and-flushed before the cancel are durable, and a sink hard-aborted mid-write (past the flush grace) may leave a half-written file, an open transaction, or a connection that closed before the server's response was read. Idempotent sinks (JSONL append, S3 put with a fixed key, BigQuery streaming insert with `insertId`, upsert-style writes) handle re-runs cleanly. Non-idempotent sinks (`HTTP POST` without dedupe headers, `INSERT` with auto-id) may double-write on retry. If you can't tolerate that, prefer `on_error: continue` and reconcile failed rows after the fact.

#### Adaptive batch sizing

The optional `adaptive_batch_size:` sub-block under `execution:` enables the
AIMD controller that auto-tunes the effective write batch size from observed sink
latency and error rate. Default `enabled: false` (opt-in).

```yaml
execution:
  adaptive_batch_size:
    enabled: true
    min: 500               # lower bound (rows)
    max: 10000             # upper bound; inert above the source page size
    increase_step: 500     # additive growth per clean, fast batch
    decrease_factor: 0.5   # multiplicative shrink on error or high latency
    cooldown_batches: 5    # batches to skip after a shrink before growing again
    target_latency_ms: 1000  # optional write-latency target (ms)
    error_threshold: 0.01  # per-batch error rate that triggers a shrink
```

**Caveats:**

- **Error-driven shrink requires a `dlq:` block.** The error signal comes from
  per-row outcomes reported via the DLQ path. Without a DLQ the controller sees
  no errors; only `target_latency_ms` can drive shrinks.
- **Effective ceiling = source page size (within-page only in v1).** The
  controller reslices pages it already received — it cannot buffer across pages.
  Raise the source `batch_size` to allow bigger write batches.

See the [Adaptive batching cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/adaptive-batching.html)
for the full field reference, AIMD trajectory example, and the four Prometheus
metrics (`faucet_pipeline_adaptive_batch_*`).

#### State keys

- Root invocations: `{name}::{row_id}`.
- Child invocations: `{name}::{row_id}::{parent_record_key}` where `parent_record_key` is the value at `parent_key` (default `id`) in the parent record.

A state-key collision among siblings sharing a parent is detected upfront and errors with both offenders named.

### Topology mode — fan-out (tee), fan-in (merge), and joins

For pipelines that outgrow a single source → sink line, declare an explicit
**node graph** instead of a matrix. Set `pipeline.nodes` (a map of node id →
node) and `pipeline.edges` (producer → consumer connections). Topology mode is
**mutually exclusive** with `matrix:` — setting both is an error.

Each node is typed by `kind`:

| `kind` | in | out | fields |
|--------|----|-----|--------|
| `source` | 0 | 1 | `ref:` (a `pipeline.sources` template) + optional `type`/`config` overrides |
| `transform` | 1 | 1 | `transforms:` (same list syntax as elsewhere) |
| `tee` | 1 | N | `channel_capacity` (default 4), optional `fanout` (must equal the outgoing-edge count) |
| `merge` | N | 1 | — |
| `join` | 2 | 1 | see below |
| `sink` | 1 | 0 | `ref:` (a `pipeline.sinks` template) + optional `type`/`config` overrides |

```yaml
version: 1
name: fan_out
pipeline:
  sources:
    orders: { type: csv, config: { path: ./data/orders.csv } }
  sinks:
    warehouse: { type: jsonl, config: { path: ./out/warehouse.jsonl } }
    archive:   { type: jsonl, config: { path: ./out/archive.jsonl } }
  nodes:
    src:  { kind: source, ref: orders }
    fan:  { kind: tee, channel_capacity: 4, fanout: 2 }
    w1:   { kind: sink, ref: warehouse }
    w2:   { kind: sink, ref: archive }
  edges:
    - { from: src, to: fan }
    - { from: fan, to: w1 }
    - { from: fan, to: w2 }
```

**Execution.** Nodes run concurrently, connected by bounded channels — the
slowest sink paces its producer (backpressure). A `tee` clones each page to
every downstream edge; a `merge` forwards pages from all inputs in arrival
order. Sink nodes reuse the normal streaming write path, so DLQ, bookmarks, and
metrics work unchanged (`faucet_tee_records_total`, `faucet_merge_records_total`,
`faucet_join_*`, labelled `pipeline` + `node`).

**State.** Each terminal sink owns a bookmark under `{name}::{node_id}`. On
restart the source resumes from the **minimum** across every sink's stored
bookmark (only when all sinks have one), so a lagging sink is never skipped —
sinks whose bookmarks have diverged must be idempotent.

**`on_error`.** `execution.on_error: stop` aborts the whole topology on the
first node failure; `continue` lets healthy branches finish and reports the
failures at the end.

#### `join:` — enrich one stream from another by key

A `join` node hash-joins two upstreams: the **build** (right) side is buffered
into an in-memory index keyed by `build.key`, then the **probe** (left) side is
streamed and each record enriched with the `project`ed fields of its match. The
join's two incoming edges carry `as:` labels matching `build.edge` /
`probe.edge`.

```yaml
  nodes:
    enrich:
      kind: join
      mode: left                 # `inner` drops non-matches; `left` keeps them
      build: { edge: customers_in, key: id }
      probe: { edge: orders_in,    key: customer_id }
      project:
        - { from: tier, as: customer_tier }
      on_missing: null           # left-mode fill when no match
      on_duplicate: first        # or `cartesian` (one row per build match)
      on_collision: overwrite    # or `skip` / `error`
      key_normalize: preserve    # or `stringify` ("42" == 42)
      max_build_records: 10000000
  edges:
    - { from: fetch_customers, to: enrich, as: customers_in }
    - { from: fetch_orders,    to: enrich, as: orders_in }
    - { from: enrich,          to: write }
```

The build side is fully materialized before probing begins, so pair a large
dimension table with a fast local source (SQLite/Parquet) rather than a slow
remote API. Runnable examples:
`cli/examples/topology_{tee_users,merge_files,join_orders_countries}.yaml`.

### State stores

```yaml
state:
  type: file              # or: memory, redis, postgres
  config:
    path: ./.faucet-state
```

The Redis and PostgreSQL backends ship behind the `state-redis` and `state-postgres` features.

### `dlq:` (optional)

Sibling of `source`, `sink`, `transforms`, `state` under `pipeline:`.

| Field | Type | Default | Notes |
|---|---|---|---|
| `sink` | ConnectorSpec | required | Any sink — typically `jsonl`, `s3`, `kafka`, `http`. |
| `on_batch_error` | `propagate` \| `dlq_all` | `propagate` | What to do when the main sink fails wholesale (no per-row info). |
| `max_failures_per_page` | integer | unset (unlimited) | Abort if a single page produces more than this many DLQ records. |
| `max_failures_total` | integer | unset (unlimited) | Abort if the run-wide DLQ count exceeds this. |
| `include_original_payload` | bool | `true` | Reserved for a future headers-only mode. Always `true` in v1. |

Matrix rows can override the inherited `dlq:` wholesale, or disable
inherited DLQ for that row with `dlq: null`.

Example:

```yaml
pipeline:
  source: { type: rest, config: { base_url: "https://api.example.com", path: "/v1/users" } }
  sink:
    type: bigquery
    config:
      project_id: my-project
      dataset_id: prod
      table_id: users
  dlq:
    sink:
      type: jsonl
      config: { path: ./dlq/users.jsonl }
    on_batch_error: propagate
    max_failures_per_page: 100
    max_failures_total: 10000
```

### `contract:` (optional)

Sibling of `source`, `sink`, `transforms`, `state` under `pipeline:` — a
versioned **data contract** for the pipeline's output (required fields, types,
nullability, enum sets, patterns, numeric/length bounds), enforced per page
after transforms and quality checks. `on_breach: fail` (default) aborts the
run on the first breach; `quarantine` routes breaching records to the DLQ
(requires a `dlq:` block, validated at load time); `warn` logs + counts but
writes everything. Requires the `contract` feature (in the default build).

```yaml
pipeline:
  contract:
    version: "1.0.0"
    on_breach: quarantine
    fields:
      - { name: order_id, type: string, min_length: 1 }
      - { name: status, type: string, enum: [open, shipped, cancelled] }
      - { name: amount, type: number, min: 0, required: false, nullable: true }
```

Inspect or publish it with `faucet contract <config> [--export
contract|json-schema|openlineage]`; `faucet schema contract` prints the
block's JSON Schema. Full model:
[Data contracts](https://faucet-hq.github.io/faucet-stream/cookbook/contracts.html).

### `sla:` (optional)

**Top-level** block (sibling of `pipeline:`, like `resilience:`) declaring a
freshness/volume SLA, evaluated after every root invocation by
`run`/`schedule`/`serve`/`replicate`. Violations emit
`faucet_pipeline_sla_violations_total{pipeline,row,kind}` + a WARN log and
**never fail the run**; `faucet doctor` probes staleness/baseline health
read-only.

```yaml
sla:
  max_staleness_secs: 7200   # stale when no successful run within 2h (needs state:)
  min_rows_per_run: 1        # a successful run writing fewer records violates
  volume_anomaly:            # learned baseline over recent successful runs (needs state:)
    method: zscore           # zscore | iqr
    min_history: 5           # successful runs before detection starts
```

`faucet schema sla` prints the block's JSON Schema. Full model:
[SLA monitoring](https://faucet-hq.github.io/faucet-stream/cookbook/sla.html).

### `profiling:` (optional)

**Top-level** block (sibling of `pipeline:`) declaring learned column
profiles with drift detection (#708). Every root invocation profiles the
records it wrote — per column: null rate, type mix, distinct estimate
(HyperLogLog), numeric / string summaries, top values — in bounded memory,
compares the profile with a rolling baseline of earlier runs kept in the
`state:` store, and reports a statistically significant change per column
(z-score / IQR on the numeric metrics; new / vanished values, type mix and a
population-stability index for categoricals). No thresholds to write.
`on_drift: warn | notify | fail` decides the consequence; profiles never hold
unmasked values (the pass runs after masking). Needs a `state:` block.

```yaml
profiling:
  min_history: 5             # baseline runs before detection starts
  window: 20                 # rolling baseline size
  on_drift: warn             # warn (log + metric) | notify (+ profile_drift event) | fail
```

`faucet profiling show|reset` inspects / re-baselines; `faucet doctor` reports
the baseline depth; `faucet schema profiling` prints the JSON Schema. Full
model: [column profiling](https://faucet-hq.github.io/faucet-stream/cookbook/profiling.html).

### `policy:` (optional)

**Top-level** block (sibling of `pipeline:`) declaring a data-flow policy
(#702): `classifications` label columns by name (`fields` — full dot-path or
leaf key —, `field_pattern` regex) or by value (`value_detector`: the masking
detectors), and `rules` say which sinks a label may reach — `require` sink
`attributes` such as `residency: eu`, accept a `mask` action, or `deny`.
Decided before any data moves (`validate` / `policy` / `plan` / `doctor`
report; `run` and the `serve` submit path refuse) and backed at run time by
the value detectors on the real records (`on_runtime: fail | quarantine`).
A `--policy <file>` merges on top. Sinks carry free-form `attributes:` the
rules reason about.

```yaml
policy:
  classifications:
    - { label: pii, fields: [email, phone], value_detector: email }
  rules:
    - { name: pii-eu, when: { label: pii }, require: { residency: [eu] }, mask: [hash] }
pipeline:
  sink:
    type: postgres
    attributes: { residency: eu, environment: prod }
    config: { … }
```

`faucet schema policy` prints the JSON Schema. Full model:
[data-flow policies](https://faucet-hq.github.io/faucet-stream/cookbook/policies.html).

### `catalog:` (optional)

**Top-level** block (sibling of `pipeline:`) naming the Data Movement Catalog
store — the persistent, cross-run record of every dataset the pipeline
touches: identity, a deduplicated schema timeline (with per-version diffs),
per-run volume + last-success freshness, and source→sink lineage edges.
Recorded after every successful root invocation by `run`/`schedule`/
`replicate`; **never fails a run**. `faucet serve` ignores this block and
records into its `--history` backend automatically. Requires the `catalog`
build feature (in `full`); SQL stores additionally need
`serve-history-sqlite` / `serve-history-postgres`.

```yaml
catalog:
  url: sqlite:./faucet-catalog.db   # sqlite:<path> | postgres://… | memory
  sample_records: 100               # schema-inference sample per side
```

Browse with `faucet catalog datasets|show|lineage`, `GET /v1/catalog/*` on
`faucet serve`, or the web console's Datasets / Lineage views.
`faucet schema catalog` prints the block's JSON Schema. Full model:
[Data Movement Catalog](https://faucet-hq.github.io/faucet-stream/cookbook/catalog.html).

### `usage:` / `budget:` (optional)

Cost & usage accounting (#704) is always on — every invocation reports
records, estimated bytes, backend round trips and connector cost signals,
priced against the `usage:` rate table (shipped defaults = public list prices;
override `currency`, `egress_per_gb`, `object_storage.*`, `warehouse.*`,
`hosted_elt_per_million_rows` inline or via `pricing_file`). `faucet run`
prints a usage line per row, `--output json` carries the record, and with a
`catalog:` store `faucet usage --by pipeline|row|dataset|sink|day` aggregates
across runs. A `budget:` block sets hard ceilings on one invocation
(`max_records`, `max_bytes`, `max_duration_secs`, `allowed_sinks`, #703): the
page that would cross a ceiling is refused whole and the run fails with
`budget_exceeded`; `faucet run --max-records/--max-bytes/--max-duration-secs/
--allowed-sink` merge with it (stricter wins). See the
[usage cookbook](../docs/book/src/cookbook/usage.md).

```yaml
usage:
  pricing: { currency: EUR, egress_per_gb: 0.09 }
budget:
  max_records: 1000000
  allowed_sinks: [warehouse]
```

### `params:` (optional)

**Top-level** block (sibling of `pipeline:`) declaring the config's
**trigger-time** surface: the values that change per run, each typed. Referenced
anywhere in the config as `${param.NAME}` and bound *before* the config is
parsed, so a param can never alter the document's structure and never reaches a
connector unresolved.

```yaml
params:
  tenant_id: { type: string, required: true, description: "Tenant to sync" }
  since:     { default: "1970-01-01" }        # type defaults to string
  page_size: { type: int, default: 500 }
  api_token: { required: true, secret: true } # redacted everywhere, never persisted
  region:    { default: us, values: [us, eu, apac] }  # closed set: anything else is rejected
pipeline:
  source:
    type: rest
    config:
      url: "https://api.example.com/${param.tenant_id}/events?since=${param.since}"
      auth: { type: bearer, config: { token: "${param.api_token}" } }
```

```bash
faucet run tenant-sync.yaml --param tenant_id=acme --param api_token="$TOKEN"
faucet validate tenant-sync.yaml          # required params → type-shaped placeholders
```

Types are real: when `${param.NAME}` is a value's *entire* text the declared type
survives (`page_size` arrives as the number `500`); embedded in a longer string it
is stringified. Values arrive as JSON (`500`) or strings (`"500"`) and are coerced
to the declared type, so CLI and HTTP behave identically. A missing `required`
param, a type mismatch, an undeclared `--param`, or an undeclared `${param.x}`
reference is an error naming the param.

`values:` declares a closed set: a value outside it is rejected at bind time
naming the alternatives, instead of binding happily and failing downstream. It
also makes the axis enumerable, which is what `faucet template test`'s
`auto.enum_coverage` sweeps.

`--param-env NAME[=VALUE]` overrides an environment variable for one run's
`${env:VAR}` resolution without mutating the process environment. Always
available (no build feature); `faucet schema params` prints one entry's JSON
Schema. To register a parameterized config once and trigger it by id, see
`faucet template` and
[Parameters & pipeline templates](https://faucet-hq.github.io/faucet-stream/cookbook/templates.html).

`faucet template test <suite>` sweeps a template's whole parameter space
offline — each case materializes the template as a real trigger would, then
expands it and compiles its transform chain. Cases come from hand-written
`cases:`, a generated `combine:` product (`exclude:`, all-pairs `pairwise:`), and
`auto:` cases derived from the template's own `params:`; an optional
`behavioral:` block runs fixture records through the real pipeline with `faucet
test`'s matchers. When the suite's `template:` is a config **path** no registry
is involved, so a template can be tested before it is ever registered. Exit code
is the failed-case count. Example:
[`examples/tests/template_suite.yaml`](examples/tests/template_suite.yaml);
`faucet schema template-test` prints the suite schema.

### Transforms

Eleven built-in transforms are exposed as `type:` values: `flatten`,
`rename_keys`, `keys_case`, `spell_symbols`, `select`, `drop`, `set`,
`rename_field`, `cast`, `redact`, `value_case`. They run in declared
order. The
[record transforms cookbook page](../docs/book/src/cookbook/transforms.md)
has the full reference.

```yaml
transforms:
  - type: flatten
    config: { separator: "__" }
  - type: select
    config:
      fields: [id, name, email]
  - type: cast
    config:
      fields: { id: string }
      on_error: error
  - type: redact
    config:
      fields: [email]
      mask: "***"
  - type: set
    config:
      values:
        _source: my-api
```

### Compression

File-shaped connectors (JSONL/CSV/S3/GCS source and sink) accept a `compression` field. Default `auto` detects `.gz` and `.zst` from the file path or object key.

```yaml
version: 1
pipeline:
  source:
    type: csv
    config:
      path: data.csv.gz
      compression: auto      # or 'gzip', 'zstd', 'none'
  sink:
    type: jsonl
    config:
      path: out.jsonl.zst
      compression: auto
```

Build the CLI with the feature enabled:

```bash
cargo install --path cli --features compression
```

## Secrets-manager interpolation

Pull secret values directly from a secrets manager using `${scheme:reference}`
directives anywhere in your config. Resolution happens at config-load time:
values are fetched concurrently (up to 8 in parallel), de-duplicated, and
substituted in place. They are never written to disk.

```yaml
auth:
  type: bearer
  config:
    token: "${vault:secret/data/myapp/api#token}"
```

| Backend | Directive | Auth |
|---------|-----------|------|
| HashiCorp Vault KV v2 | `${vault:<path>[#field]}` | `VAULT_ADDR` + `VAULT_TOKEN` (+ optional `VAULT_NAMESPACE`) |
| AWS Secrets Manager | `${aws-sm:<name-or-ARN>[#field]}` | `aws-config` default chain |
| GCP Secret Manager | `${gcp-sm:projects/<p>/secrets/<s>/versions/<v>}` | Application Default Credentials |
| Azure Key Vault | `${azure-kv:<vault>/<secret>[/<version>]}` | `AZURE_*` env / managed identity / `az login` |

The `#field` selector (Vault and AWS) parses the secret body as JSON and returns
one key. Omit it to receive the full secret body as a string.

**Build features** — none compiled in by default:

```bash
cargo install faucet-cli --features secrets          # all four backends
cargo install faucet-cli --features secrets-vault    # Vault only
cargo install faucet-cli --features secrets-aws-sm   # AWS only
cargo install faucet-cli --features secrets-gcp-sm   # GCP only
cargo install faucet-cli --features secrets-azure-kv # Azure only
```

**Validation flags:**

- `faucet validate pipeline.yaml` — resolves all secrets as a preflight; prints
  `secret: <scheme>:<reference> → resolved` per reference (never the value).
- `faucet validate --no-secrets pipeline.yaml` — grammar / structure only; no
  network or credentials required.
- `faucet schema secrets` — prints the grammar descriptor as JSON.

**Redaction:** faucet scrubs every resolved secret value from its own tracing,
log, and error output via a `RedactingWriter` on the tracing subscriber.
This boundary covers faucet's own output only — connector libraries that
debug-log deserialized config fields are outside it; never enable debug logging
on connectors that hold resolved secrets.

**Known limitation:** secret directives are resolved in connector configs,
transforms, state, dlq, and matrix rows. They are **not** resolved in the
top-level `auth:` catalog or `vars:` block. Put secrets in a connector's inline
`auth:` config instead of the shared catalog until this is lifted.

See the [docs-site secrets cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/secrets.html)
for full examples and details.

## Running from environment variables (`--from-env`)

`faucet` can build and run a pipeline entirely from `FAUCET_*` environment variables — no YAML file required. This mode is designed for container / Kubernetes / Airflow deployments where every config value naturally flows through the orchestrator's env-var interface.

```bash
faucet run --from-env
```

`--from-env` is mutually exclusive with a positional config path; you pick one source of truth or the other. Mixing them is rejected at argument-parse time.

### Variable schema

| Variable | Purpose |
|---|---|
| `FAUCET_SOURCE` | Source kind — same string keys as the YAML `source.type:` field (`rest`, `csv`, `postgres`, `postgres-cdc`, …). |
| `FAUCET_SOURCE_<KIND>_<FIELD>` | Scalar source-config fields. Scope is keyed by `<KIND>` so two different sources can't collide. |
| `FAUCET_SINK` | Sink kind. |
| `FAUCET_SINK_<KIND>_<FIELD>` | Scalar sink-config fields. |
| `FAUCET_STATE` | Optional. State store kind (`file`, `memory`, `redis`, `postgres`). |
| `FAUCET_STATE_<KIND>_<FIELD>` | State-store config. |
| `FAUCET_TRANSFORM_<N>` | Optional. Indexed transforms — `FAUCET_TRANSFORM_1=keys_case`, `FAUCET_TRANSFORM_2=flatten`. Indices must be contiguous starting at 1. |
| `FAUCET_TRANSFORM_<N>_<FIELD>` | Per-transform config (e.g. `FAUCET_TRANSFORM_2_SEPARATOR=__`). |
| `FAUCET_NAME` | Optional pipeline name (used in log messages). |

Field names are case-insensitive: write env vars in `SCREAMING_SNAKE_CASE`; they are lowercased before being matched against connector field names. Hyphens in connector kinds (e.g. `postgres-cdc`) become underscores in the env scope (`FAUCET_SOURCE_POSTGRES_CDC_*`). Empty values for `FAUCET_SOURCE` / `FAUCET_SINK` / `FAUCET_STATE` / `FAUCET_NAME` are treated as unset.

### Scalar values

Scalar fields go through a JSON-parse-then-string-fallback coercion: `30` is a number, `true` is a bool, `null` is JSON null, and anything that doesn't parse as JSON is treated as a plain string. This matches how the same value would be typed in YAML.

### Nested / tagged-enum fields (`*_JSON` escape hatch)

Tagged-enum config fields (`auth`, `pagination`, `replication_method`, `column_mapping`, …) don't flatten cleanly into env-var names because different variants have different sub-fields. For those, set the entire value as JSON under a `*_JSON` suffix:

```bash
FAUCET_SOURCE=rest \
FAUCET_SOURCE_REST_BASE_URL=https://api.github.com \
FAUCET_SOURCE_REST_PATH=/repos/faucet-hq/faucet-stream/issues \
FAUCET_SOURCE_REST_AUTH_JSON='{"type":"Bearer","token":"ghp_xxx"}' \
FAUCET_SOURCE_REST_PAGINATION_JSON='{"type":"LinkHeader"}' \
FAUCET_SINK=jsonl \
FAUCET_SINK_JSONL_PATH=./issues.jsonl \
  faucet run --from-env
```

Setting both `FAUCET_SOURCE_REST_AUTH=...` and `FAUCET_SOURCE_REST_AUTH_JSON=...` for the same field is a hard error — pick one. The error names both variables.

### Loading a `.env` file first

Use `--env-file PATH` to load a `.env` file into the process environment before the env walker runs. Existing process-env values always win (12-factor convention). `--env-file` only works together with `--from-env`.

```bash
faucet run --from-env --env-file ./pipeline.env
```

## Examples

[`examples/`](examples/) ships YAML pipelines for every `faucet-stream/examples/*.rs` use case — the same source → sink combinations the library docs cover, expressed as config.

CLI-only smoke tests:

- [`csv_to_jsonl.yaml`](examples/csv_to_jsonl.yaml) — read a CSV, write JSONL (zero external deps)
- [`rest_to_stdout_preview.yaml`](examples/rest_to_stdout_preview.yaml) — pipe REST records into `jq`
- [`matrix_depends_on.yaml`](examples/matrix_depends_on.yaml) — `depends_on` completion ordering: two staging rows, then a report row that waits for both (zero external deps)

Mirrors of the Rust examples (one `.yaml` per `.rs`):

- REST: [`rest_to_jsonl`](examples/rest_to_jsonl.yaml), [`rest_to_bigquery`](examples/rest_to_bigquery.yaml), [`rest_to_postgres`](examples/rest_to_postgres.yaml), [`rest_to_s3`](examples/rest_to_s3.yaml), [`rest_streaming`](examples/rest_streaming.yaml)
- GraphQL: [`graphql_to_bigquery`](examples/graphql_to_bigquery.yaml), [`graphql_to_postgres`](examples/graphql_to_postgres.yaml)
- XML/SOAP: [`xml_to_s3`](examples/xml_to_s3.yaml), [`xml_to_mongodb`](examples/xml_to_mongodb.yaml)
- gRPC: [`grpc_to_elasticsearch`](examples/grpc_to_elasticsearch.yaml), [`grpc_to_http`](examples/grpc_to_http.yaml)
- Databases: [`postgres_to_bigquery`](examples/postgres_to_bigquery.yaml), [`postgres_to_elasticsearch`](examples/postgres_to_elasticsearch.yaml), [`postgres_to_s3`](examples/postgres_to_s3.yaml), [`postgres_to_snowflake`](examples/postgres_to_snowflake.yaml), [`mysql_to_bigquery`](examples/mysql_to_bigquery.yaml), [`mysql_to_postgres`](examples/mysql_to_postgres.yaml), [`mysql_to_snowflake`](examples/mysql_to_snowflake.yaml), [`sqlite_to_jsonl`](examples/sqlite_to_jsonl.yaml), [`sqlite_to_csv`](examples/sqlite_to_csv.yaml)
- Document stores: [`mongodb_to_postgres`](examples/mongodb_to_postgres.yaml), [`mongodb_to_elasticsearch`](examples/mongodb_to_elasticsearch.yaml), [`mongodb_to_redis`](examples/mongodb_to_redis.yaml)
- Search / cache: [`elasticsearch_to_redis`](examples/elasticsearch_to_redis.yaml), [`elasticsearch_to_s3`](examples/elasticsearch_to_s3.yaml), [`redis_to_mysql`](examples/redis_to_mysql.yaml), [`redis_to_sqlite`](examples/redis_to_sqlite.yaml)
- Object storage: [`s3_to_bigquery`](examples/s3_to_bigquery.yaml), [`s3_to_mongodb`](examples/s3_to_mongodb.yaml), [`s3_to_postgres`](examples/s3_to_postgres.yaml), [`s3_to_snowflake`](examples/s3_to_snowflake.yaml)
- CSV in: [`csv_to_bigquery`](examples/csv_to_bigquery.yaml), [`csv_to_mysql`](examples/csv_to_mysql.yaml), [`csv_to_sqlite`](examples/csv_to_sqlite.yaml)
- Webhook receiver: [`webhook_to_csv`](examples/webhook_to_csv.yaml), [`webhook_to_http`](examples/webhook_to_http.yaml), [`webhook_to_postgres`](examples/webhook_to_postgres.yaml)
- DAG parent leg: [`dag_users_posts`](examples/dag_users_posts.yaml) — parent only (multi-node DAGs require the library API today)
- Named templates: [`templates_dry_rest`](examples/templates_dry_rest.yaml) — shared REST source template across multiple matrix rows; [`templates_users_posts`](examples/templates_users_posts.yaml) — templates with parent/child DAG fan-out

Every auth shape — Bearer, Basic, API key, OAuth2, custom headers, gRPC metadata — round-trips through YAML/JSON, so the YAML examples are 1:1 with the Rust ones.

## Observability (Prometheus + tracing)

Optional top-level block in `faucet.yaml`:

```yaml
version: 1
name: github-issues-sync
observability:
  prometheus:
    listen: "127.0.0.1:9464"        # recommended bind; 0.0.0.0 is opt-in
    buckets: [0.001, 0.01, 0.1, 1.0, 10.0, 60.0]  # optional; sensible defaults if unset
  tracing:
    level: "info"                   # falls back to RUST_LOG / FAUCET_LOG / --log-level
pipeline: { ... }
```

When `prometheus.listen` is set, `faucet run` exposes a `/metrics` HTTP endpoint at that address using `metrics-exporter-prometheus`. **The endpoint is unauthenticated** — bind to `127.0.0.1` (the default in examples) and put a reverse proxy or network ACL in front if you need to expose it to other hosts.

**Default histogram buckets** (when `buckets` is unset): `0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0` seconds. Covers sub-millisecond writes through five-minute batch loads.

**Per-command behavior:**

| Command | Installs Prometheus? | Installs `tracing-subscriber`? | Notes |
|---------|----------------------|-------------------------------|-------|
| `run` | Yes (when `prometheus.listen` set) | Yes | The only command that runs pipelines. |
| `validate` | No | Yes (basic fmt layer) | Short-lived; metrics meaningless. |
| `preview` | No | Yes | Short-lived. |
| `schema`, `list`, `init` | No | Yes | Pure metadata commands. |

**Tracing level precedence:** `--log-level` flag > `FAUCET_LOG` env > `RUST_LOG` env > YAML `observability.tracing.level` > default.

### Bridging to OpenTelemetry

`faucet-stream` emits stable `tracing` spans (`faucet.pipeline.run`, `faucet.source.page`, `faucet.sink.write`, `faucet.transform.apply`, `faucet.state.get|put|delete`). To export them to an OTel collector, install `tracing-opentelemetry` + `opentelemetry-otlp` in your own binary:

```rust
use tracing_subscriber::prelude::*;
let tracer = opentelemetry_otlp::new_pipeline()
    .tracing()
    .install_batch(opentelemetry_sdk::runtime::Tokio)?;
let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);
tracing_subscriber::registry().with(otel_layer).init();
// then call faucet_cli::run_main(...) (or run_from_yaml_str) as usual
```

Faucet does not bundle an OTel exporter — wire your own to keep dependencies minimal.

## Custom binaries with third-party connectors

The stock `faucet` binary knows only the connectors it was compiled with. To use
a **third-party** `faucet-source-*` / `faucet-sink-*` connector (or one of your
own) from a `faucet.yaml` config, build your own `faucet` binary that registers
it. This is connector *plugin loading* — no dynamic ABI, no subprocess, same
in-process speed as a built-in.

Depend on `faucet-cli` as a library plus your connector crate, then write a
`main.rs` that hands a [`PluginRegistry`] to `run_main`:

```rust,no_run
use faucet_cli::registry::PluginRegistry;
use faucet_source_lorem::LoremSource; // your third-party connector crate

fn main() -> std::process::ExitCode {
    let registry = PluginRegistry::with_builtins()
        .register_source("lorem", |cfg| Ok(Box::new(LoremSource::from_value(cfg)?)))
        // .register_sink("other", |cfg| Ok(Box::new(OtherSink::from_value(cfg)?)))
        ;
    faucet_cli::run_main(registry)
}
```

```toml
# Cargo.toml
[dependencies]
faucet-cli = "1"
faucet-core = "1"
faucet-source-lorem = "1"
```

Now `source: { type: lorem }` works in a config, and the connector shows up in
`faucet list` and `faucet schema source lorem` — exactly like a built-in, across
every command (`run`, `validate`, `schema`, `list`, `preview`, `serve`, …).

- `register_source` / `register_sink` take a **synchronous** factory
  `Fn(config) -> Result<Box<dyn Source|Sink>>`; connectors that need to connect
  eagerly should do so lazily on first use (the pattern the built-ins follow).
- Use `register_source_with` / `register_sink_with` to also supply a config
  JSON-Schema closure and a one-line description for `faucet list` / `faucet
  schema`.
- Registering a name that collides with a built-in (or a duplicate) is rejected
  at startup with a clear error.
- Custom connectors receive their `config` verbatim; the shared top-level
  `auth:` catalog (`auth: { ref: … }`) is **not** injected into them — a custom
  connector manages its own auth.

A complete, runnable example lives in
[`cli/examples/custom-cli/`](examples/custom-cli/main.rs) (build it with
`cargo run --example custom-cli -- list`).

[`PluginRegistry`]: https://docs.rs/faucet-cli/latest/faucet_cli/registry/struct.PluginRegistry.html

## Troubleshooting / FAQ

**"No config file found" / wrong file picked up.** With no path argument, `faucet` auto-discovers `faucet.yaml`, then `faucet.yml`, then `faucet.json` in the current directory. Pass the path explicitly (`faucet run path/to/pipeline.yaml`) when the file lives elsewhere or has a different name.

**Environment variables / `.env` not applied.** `faucet` loads a `.env` from the current directory by default; point at another with `--env-file path/.env`, or disable loading entirely with `--no-env-file`. `${env:VAR}` / `${file:PATH}` placeholders resolve at config-load time, so the var must be set (or the `.env` loaded) before the command runs.

**"unknown source/sink type" or a connector seems missing.** Connectors are feature-gated. A slim build (`--no-default-features --features …`) only includes the connectors you compiled in. Run `faucet list` to see exactly which sources, sinks, transforms, and state backends are present in your binary; reinstall with the needed `--features` (or the `full` feature) if one is absent.

**`faucet validate` fails.** Validation parses the config, expands the matrix, and checks every connector/transform spec (plus effectively-once and write-mode gates) without running. The error names the offending matrix row and field. Use `faucet validate --show-composed` to print the fully merged document (after `extends:` / `!include` / profiles) and `faucet schema source|sink|transform <name>` to confirm the expected field shape.

**Secrets not resolving (`${vault:…}` / `${aws-sm:…}` / `${gcp-sm:…}` / `${azure-kv:…}`).** Secrets resolution is feature-gated — install with the matching `secrets-*` feature (or the `secrets` aggregate). Run `faucet validate` (without `--no-secrets`) as a real preflight: it fetches each reference and prints `secret: <scheme>:<reference> → resolved`, surfacing missing credentials (e.g. `VAULT_ADDR`/`VAULT_TOKEN`, the AWS default chain, GCP ADC, Azure env/managed identity) before a run. Use `--no-secrets` to validate offline without contacting any secrets manager.

**Pipeline runs but I see no records / no logs.** Pipeline records and command output go to **stdout**; logs go to **stderr**. Raise verbosity with `--log-level debug` or `FAUCET_LOG=debug`. Never enable debug logging on a pipeline whose connector configs hold resolved secrets — third-party connector debug output is outside faucet's redaction boundary.

## See also

- [`faucet-stream`](https://crates.io/crates/faucet-stream) — the umbrella library this CLI is built on.
- [`faucet-core`](https://crates.io/crates/faucet-core) — shared traits, pipeline orchestration, and error types.
- [Documentation site](https://faucet-hq.github.io/faucet-stream/) — guides, the connector capability matrix, and the config-file grammar reference.
- [GitHub repository](https://github.com/faucet-hq/faucet-stream) — source, examples (`cli/examples/`), and issue tracker.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](../LICENSE-APACHE) or <https://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](../LICENSE-MIT) or <https://opensource.org/licenses/MIT>)

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
