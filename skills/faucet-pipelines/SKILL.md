---
name: faucet-pipelines
description: >-
  Use when writing, editing, reviewing or debugging a faucet pipeline config
  (YAML/JSON run by the `faucet` CLI): moving data from one system to another
  (database, API, files, object storage, queue, warehouse), setting up an
  incremental sync, a CDC stream or a snapshot-then-CDC mirror, choosing write
  modes (append, upsert, delete, overwrite) and keys, adding transforms, PII
  masking, data-quality checks, data contracts, schema-drift handling or a
  data-flow policy, configuring state, dead-letter queues or exactly-once
  delivery, scheduling or backfilling a pipeline, generating per-table configs
  with discovery, and wiring credentials through secret references.
license: MIT OR Apache-2.0
---

# faucet pipelines

`faucet` runs data pipelines declared in a YAML (or JSON) file: a source,
optional transforms and governance passes, a sink, plus optional state, DLQ,
schedule and runtime blocks. This skill is the procedure for writing those
files so they are right the first time and stay right when they run
unattended.

## Step 0: use the project's faucet version

The project decides which faucet to use, not whatever is on the `PATH`: a
config written for a newer faucet can fail or be misread on an older one.

1. Find the pin: the `"github:faucet-hq/faucet-stream"` entry under `[tools]`
   in the project's `mise.toml` (`faucet init` writes it). A config may also
   carry `requires_faucet: ">=X.Y"`.
2. Run `faucet --version`. Use that binary if it equals the pin (with no pin:
   if it satisfies every `requires_faucet` in the project).
3. Otherwise, if [mise](https://mise.jdx.dev) is installed, run `mise install`
   in the project and prefix every command with `mise exec --`.
4. Otherwise use the pinned container image; its entrypoint is `faucet`:
   `docker run --rm --user "$(id -u):$(id -g)" -v "$PWD:/work" -w /work ghcr.io/faucet-hq/faucet-stream:<version> --version`.
5. Otherwise install exactly that version:
   `curl --proto '=https' --tlsv1.2 -LsSf https://github.com/faucet-hq/faucet-stream/releases/download/faucet-cli-v<version>/faucet-cli-installer.sh | sh`.
6. With no pin at all, install the latest release (the same installer from
   `releases/latest/download/`) and pin it as described in
   [Pinning the faucet version](https://faucet-hq.github.io/faucet-stream/operations/pinning.html).

Every version-specific fact (which connectors and blocks exist, config keys,
types, defaults, commands and flags) comes from that binary: `faucet list`,
`faucet schema --help`, `faucet schema source|sink|transform <name>`,
`faucet <command> --help`. Never from memory or from this skill. When a config
passes `faucet validate`, set its `requires_faucet:` to `">=<major>.<minor>"`
of that binary; if validate rejects `requires_faucet` as an unknown field, the
binary predates it, so leave it out.

## Workflow (follow in order)

1. **See what this build has.**
   ```bash
   faucet list
   faucet schema --help
   ```
   `faucet list` names the sources, sinks, transforms and state stores compiled
   in; `faucet schema --help` names every documented config block. A connector
   or block missing from either is not in this build: use a build that has it
   rather than rewriting the config around the gap.
2. **Pick the connectors by mechanism** (what the data is and how it changes;
   see "Decisions" below), then read their schemas in full:
   ```bash
   faucet schema source postgres
   faucet schema sink bigquery
   faucet schema transform cast
   ```
   The JSON Schema's `required` list, each property's `default` and
   description, and the `oneOf` / `const` entries are the only authority for
   keys, types and enum values. If the schema does not have a key, the
   connector does not support it.
3. **Scaffold, don't type from memory.**
   ```bash
   faucet init --source postgres --sink bigquery -o pipeline.yaml
   faucet discover conn.yaml --include 'public.*' -o pipeline.yaml
   ```
   `init` writes a commented config with every field of both connectors.
   `discover` turns a connection-only config into one matrix row per table,
   collection or prefix when the source supports discovery. Or start from the
   closest file in `cli/examples/` (see "Examples").
4. **Fill it in**, one block at a time, reading `faucet schema <block>` for
   each top-level block you add. Every credential is a reference (Hard rules).
5. **Validate until clean**, after every edit:
   ```bash
   faucet validate --no-secrets pipeline.yaml
   ```
   `--no-secrets` skips secrets-manager lookups but still resolves environment
   and file references, so export placeholders for those (or keep a
   non-secret `.env`). Read every row line: it ends with the delivery
   guarantee that row actually gets.
6. **Confirm intent before touching real systems.**
   ```bash
   faucet explain pipeline.yaml --rows
   faucet plan pipeline.yaml --sample fixtures.jsonl
   ```
   `explain` narrates the config offline; `plan` shows the resolved chain, the
   output schema and the sink delta for sample records, writing nothing.
7. **Probe the systems** once credentials exist:
   ```bash
   faucet doctor pipeline.yaml
   ```
   It checks auth, network and permissions per connector; `--offline` gives
   the credential-free lints.
8. **Test the logic offline** whenever the config transforms, masks, checks
   quality or carries a contract: write a spec (`faucet schema test`; samples in
   `cli/examples/tests/pipeline_tests.yaml`) and run `faucet test tests/*.yaml`.
9. **Run small, then for real.** `faucet run pipeline.yaml --limit 100` (or
   `--dry-run`) first, then the real run, `faucet schedule` for a cron
   process, `faucet mirror` for snapshot-then-CDC, `faucet backfill` for a
   historical range. Each command's flags: `faucet <command> --help`.

## Hard rules

- **Never inline a secret.** Every password, token, key and credentialed URL is
  a reference: environment and file references always work, and
  `faucet schema secrets` prints the secrets-manager grammar this build
  resolves. If validate says a reference's backend is not built in, use an
  environment reference filled by the scheduler instead. Never run with debug
  logging on a config that holds secrets, and never put a secret in a field
  that becomes a name, label or path.
- **Never invent a key, value or flag.** Read it from `faucet schema` or
  `faucet <command> --help`. Unknown keys fail at validate, often with a
  "did you mean" hint; take the hint, don't guess again.
- **Repeat runs read only what changed.** Use the source's own bookmark mode
  when its schema has one; otherwise follow "Incremental reads" below. A
  bookmark only survives between runs with a durable `state:` store (read
  `faucet list` for the stores; the in-memory one forgets at exit).
- **Keep a `name` and row ids stable** once a pipeline has run: they form the
  state key, so renaming one restarts that row from scratch.
- **Do not claim exactly-once** unless `faucet validate` prints an
  effectively-once guarantee on that row.
- **Rows that are quarantined need somewhere to go.** If a quality, contract,
  drift or policy setting quarantines rows, validate requires a `dlq:` block;
  never point the DLQ at a data sink's path, and never replay a DLQ before the
  cause is fixed.
- **Bind listeners to localhost** (metrics endpoints, `faucet serve`) unless the
  user asks for wider exposure and puts auth in front of it.
- **A config is not done until `faucet validate` passes** (and `faucet doctor`,
  once credentials exist).

## Decisions

Make each choice from the schema of the connector in front of you, not from
memory. The engine behaviour behind each is on the linked page.

**Incremental reads.** In `faucet schema source <name>`, look for a bookmark
mode (a replication block or key, an incremental-files setting, consumer
offsets, a CDC log position). If the source has one, use it and push the
bookmark to the server where the schema offers a binding or placeholder, so
the server does not scan everything each run. If it has none, scope the query
with the run clock (`${now.*}` tokens) and pair it with a keyed write mode, so
`faucet run --clock` and `faucet backfill` can replay windows; tell the user a
skipped run leaves a gap until backfilled. When deletes or every change
matter, use the source system's CDC connector, and `faucet mirror` (read
`faucet schema mirror`) when existing rows need a snapshot first.
See [Incremental replication & state](https://faucet-hq.github.io/faucet-stream/cookbook/state.html),
[Mirror](https://faucet-hq.github.io/faucet-stream/cookbook/replication.html),
[Backfill](https://faucet-hq.github.io/faucet-stream/cookbook/backfill.html).

**Write mode.** Read the modes the sink's schema lists.
- Events or logs that never change: append.
- Entity tables from an incremental or windowed source, and anything that may
  be replayed (backfill, DLQ replay, retries): a keyed upsert on the natural
  key. SQL sinks need their column-per-field mapping for keyed modes, and the
  destination needs a unique key on those columns.
- CDC into a table: unwrap the change envelope (`faucet list` shows the
  transform) and upsert with a delete marker.
- A small table re-read in full: overwrite (swapped in only after success).
- Rows deleted at the source that no feed reports: scoped cleanup on a
  complete fetch.
See [Upsert / mirror tables](https://faucet-hq.github.io/faucet-stream/cookbook/upsert.html).

**Delivery guarantee.** Leave the default unless the user needs
effectively-once; then require it in the config and let validate say whether
the source, sink, state and DLQ combination qualifies. A keyed upsert sink
qualifies with any source; the alternative (atomic watermark) has stricter
requirements, listed on
[Effectively-once delivery](https://faucet-hq.github.io/faucet-stream/cookbook/state.html#effectively-once-delivery).

**Shaping and governance.** Transforms run in the order listed: unwrap and
reshape first, rename next, trim and filter, fix values, stamp constants last.
Masking, quality, contract and drift run after the whole chain, so write their
rules against final field names. Use
[masking](https://faucet-hq.github.io/faucet-stream/cookbook/masking.html) for
PII, [quality checks](https://faucet-hq.github.io/faucet-stream/cookbook/quality.html)
for bad rows, a [contract](https://faucet-hq.github.io/faucet-stream/cookbook/contracts.html)
for the promised output shape,
[schema drift](https://faucet-hq.github.io/faucet-stream/cookbook/schema-drift.html)
for destination changes, and a
[policy](https://faucet-hq.github.io/faucet-stream/cookbook/policies.html) for
which labelled columns may reach which sinks.

**Many datasets.** Declare each connection once as a named template and add a
matrix row per dataset, each with its own destination; use `depends_on` for
ordering and parent rows for per-record fan-out. Run a subset with the
selection flags in `faucet run --help`.
See [Multi-pipeline DAGs](https://faucet-hq.github.io/faucet-stream/tutorials/matrix-dag.html)
and [Source discovery](https://faucet-hq.github.io/faucet-stream/cookbook/discover.html).

## When validate fails

- **Unknown field / key, with a list or a "did you mean":** a typo; use a listed
  name.
- **Missing field:** the schema's `required` list names it.
- **Missing environment variable:** export it (a placeholder is fine for
  `--no-secrets`) or add it to `.env`.
- **Built without a feature:** this binary lacks that connector, block or
  secrets backend. Report it and use a build that has it.
- **A gate between blocks** (delivery, write mode, state, DLQ, overwrite): the
  message names the conflicting settings; change the config, never the
  guarantee you promised the user.

## Reference pages

| Task | Page |
|---|---|
| Top-level grammar, interpolation, `params` | [Configuration file format](https://faucet-hq.github.io/faucet-stream/reference/config.html) |
| `extends`, `profiles`, `!include` | [Config composition](https://faucet-hq.github.io/faucet-stream/cookbook/composition.html) |
| Choosing a connector, capabilities | [Choosing a connector](https://faucet-hq.github.io/faucet-stream/reference/choosing.html), [Connector catalog](https://faucet-hq.github.io/faucet-stream/reference/connectors.html) |
| Auth blocks, shared `auth:` providers | [Authentication](https://faucet-hq.github.io/faucet-stream/cookbook/auth.html) |
| REST pagination | [Pagination styles](https://faucet-hq.github.io/faucet-stream/cookbook/pagination.html) |
| Transforms and their order | [Record transforms](https://faucet-hq.github.io/faucet-stream/cookbook/transforms.html) |
| State stores, `faucet state` / `faucet status` | [Pipeline state & status](https://faucet-hq.github.io/faucet-stream/cookbook/state-and-status.html) |
| Dead-letter queues | [Dead-letter queues](https://faucet-hq.github.io/faucet-stream/cookbook/dlq.html) |
| Retries, circuit breaker | [Resilience](https://faucet-hq.github.io/faucet-stream/cookbook/resilience.html) |
| Secret references | [Secrets-manager interpolation](https://faucet-hq.github.io/faucet-stream/cookbook/secrets.html) |
| Cron | [Scheduling](https://faucet-hq.github.io/faucet-stream/cookbook/scheduling.html) |
| Freshness and volume checks | [SLA monitoring](https://faucet-hq.github.io/faucet-stream/cookbook/sla.html) |
| Offline tests | [Testing pipelines](https://faucet-hq.github.io/faucet-stream/cookbook/testing.html) |
| Every command and flag | [CLI commands](https://faucet-hq.github.io/faucet-stream/reference/cli.html) |

## Examples

Validated example configs live in the faucet-stream repository under
`cli/examples/`, at the tag matching the binary (`faucet-cli-v<version>`; the
latest is at <https://github.com/faucet-hq/faucet-stream/tree/main/cli/examples>).
Start from the closest one and re-read every key against `faucet schema`:

- Incremental SQL with a stored bookmark: `cli/examples/postgres_incremental_to_jsonl.yaml`, `cli/examples/mssql_to_jsonl.yaml`
- Incremental REST into an upsert sink: `cli/examples/rest_to_postgres.yaml`; windowed: `cli/examples/rest_windowed_incremental.yaml`
- Quality checks with a DLQ: `cli/examples/rest_to_postgres_with_quality.yaml`
- CDC into an upsert mirror: `cli/examples/postgres_cdc_to_postgres_upsert.yaml`; snapshot then CDC: `cli/examples/postgres_replicate_snapshot_cdc.yaml`
- Effectively-once from a log: `cli/examples/kafka_to_postgres_exactly_once.yaml`
- New files only: `cli/examples/file_to_jsonl.yaml`
- Masking, contract, policy: `cli/examples/csv_to_jsonl_with_masking.yaml`, `cli/examples/csv_to_jsonl_with_contract.yaml`, `cli/examples/csv_to_jsonl_with_policy.yaml`
- Matrix ordering and backfill: `cli/examples/matrix_depends_on.yaml`, `cli/examples/backfill_sqlite_to_jsonl.yaml`
- Overwrite: `cli/examples/csv_to_sqlite_overwrite.yaml`; cron: `cli/examples/scheduled_nightly.yaml`
- Offline test specs: `cli/examples/tests/pipeline_tests.yaml`
