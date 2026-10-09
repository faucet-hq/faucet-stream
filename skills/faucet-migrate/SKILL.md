---
name: faucet-migrate
description: >-
  Use when migrating an existing Meltano project, Singer taps and targets, or
  Airbyte connections to faucet: reading and converting a meltano.yml
  (extractors, loaders, mappers, select and metadata, jobs, schedules,
  environments), choosing a native faucet connector, a Template Hub template or
  the Singer bridge for each tap and target, running an existing Singer tap or
  target inside faucet, moving incremental bookmarks and Singer state into
  faucet state so the first run does not re-sync, running old and new
  pipelines side by side, comparing them with `faucet verify`, and cutting over
  with a rollback path.
license: MIT OR Apache-2.0
---

# Migrating to faucet from Meltano, Singer or Airbyte

A migration is four jobs: translate the configs, carry the bookmarks over,
prove the new pipeline produces the same data, and switch without losing the
way back. This skill is the procedure for the parts specific to arriving from
Meltano, Singer or Airbyte. Writing each faucet config follows the
`faucet-pipelines` skill; operating and debugging them follows `faucet-debug`;
hub templates follow `faucet-templates`. What the old tools' files mean, and
how faucet treats bookmarks during a migration, is on
[Migrating from Meltano, Singer or Airbyte](https://faucet-hq.github.io/faucet-stream/operations/migrating.html).

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

## Hard rules

- **Never copy a credential value** out of `.env`, `meltano.yml`, a Singer
  config or Airbyte into a faucet config, a commit or the chat. Reference the
  environment variable that already holds it, or ask the user to export a new
  one.
- **Never delete or disable the old pipeline** (project, plugins, state,
  connections, tables, replication slots) until `faucet verify` passes and the
  user signs off. The old schedule is disabled at cutover, not before.
- **Export faucet state before every state change**, and run the change with
  `--dry-run` first:
  ```bash
  faucet state export pipeline.yaml -o state-before-migration.json
  ```
- **Never write to the old pipeline's tables during the parallel run.** faucet
  gets its own schema or dataset and its own state store.
- **Never `faucet run` or `--repair` a comparison-only config** (one whose
  source reads the old tables).
- **Do not claim parity you have not verified.** Name every gap (step 2, 3) to
  the user.

## Workflow (follow in order)

1. **Inventory the old setup.** Produce one line per stream: source, stream or
   table, replication method and key, primary key, selected columns, mapper
   steps, destination table, schedule, where its bookmark lives, and what reads
   the table downstream. For Meltano, follow every `include_paths` file and the
   production environment's overrides, and use Meltano's own commands to
   resolve what the YAML patterns mean (`meltano select <extractor> --list --all`,
   `meltano config <plugin> list`, `meltano state list`). Record each credential
   by the name of the variable that holds it. Export every old bookmark to a
   file now (`meltano state get <state_id>`, the Singer `state.json`, or the
   Airbyte connection state). [examples/meltano.yml](examples/meltano.yml) is a sample
   input to practise on.
2. **Choose a replacement for each tap**, by what it reads, never by its name:
   - a SQL database: check `faucet list` for that database's query source and
     read `faucet schema source <name>` for a bookmark mode; for `LOG_BASED`
     streams, or when deletes matter, the database's CDC source started with
     `faucet mirror`;
   - a REST or GraphQL API: look for a Template Hub source template that covers
     it (`faucet hub list`, then compare its streams with the tap's selected
     streams using `faucet hub rows <template>`); otherwise the `rest` or
     `graphql` source, translating the tap's base URL, paths, record path,
     pagination and replication key against `faucet schema source rest`;
   - files or object storage, queues and event streams: the matching source in
     `faucet list`;
   - anything still unmapped: the `singer` source, which runs the tap unchanged
     (start with `faucet init --source singer --discover --executable <tap> --stream <stream>`).

   Then each target: the native sink for its destination (check
   `faucet schema sink <name>` for the write modes the old target relied on,
   such as a merge on key properties), else the `singer` sink to keep the
   target. Search the registry with `faucet search <keyword>` and
   `faucet list --available` before concluding a connector does not exist.
   Tell the user which streams change shape (templates name and shape streams
   their own way) and which features have no equivalent.
   ```bash
   faucet list
   faucet hub list
   faucet schema source rest
   ```
3. **Write the faucet configs** with the `faucet-pipelines` skill: one matrix
   row per stream, a durable state store, a keyed write mode where the old
   target merged on keys. Translate `select` into the query's column list (SQL)
   or select / drop transforms (APIs), mappers into transforms or masking, and
   schedules into a `schedule:` block or the existing orchestrator calling
   `faucet run`. For the parallel run, point every sink at a separate schema or
   dataset.
4. **Carry bookmarks over**, only for streams that will continue into tables
   the old pipeline already filled. Stop the old schedule for that stream, take
   its last bookmark, and bring it in the way the migration page describes for
   that source type: an initial-value setting in the config when faucet has no
   state yet, otherwise `faucet state set`; a backfill from the old bookmark
   for sources without one; nothing for CDC (faucet takes its own position).
   Prefer a bookmark slightly behind the old one over one ahead of it.
   ```bash
   faucet state export pipeline.yaml -o state-before-migration.json
   faucet state set pipeline.yaml --row orders --bookmark '"2026-10-06T00:00:00Z"' --dry-run
   faucet state show pipeline.yaml --row orders
   ```
   When unsure what shape a row's bookmark has, point a copy of the config at a
   scratch state directory, run it once, and read the shape with
   `faucet state show` before setting the real row.
5. **Validate, probe and test** before any real run:
   ```bash
   faucet validate --no-secrets pipeline.yaml
   faucet explain pipeline.yaml --rows
   faucet doctor pipeline.yaml
   faucet preview pipeline.yaml --limit 10
   faucet run pipeline.yaml --limit 100
   ```
   A `--limit` run stores no bookmark. Add `faucet test` specs when transforms
   or masking replace a mapper.
6. **Run side by side and compare.** Load the full history into faucet's own
   dataset, run on schedule next to the old pipeline for several cycles, then
   compare by content: faucet against the source with the real config, and
   faucet against the old tables with a comparison-only config (start from
   `cli/examples/verify_previous_pipeline_tables.yaml`). Exclude both tools'
   metadata columns and anything computed differently on purpose, compare in a
   quiet window, and explain every difference to the user before moving on.
   ```bash
   faucet verify pipeline.yaml --row orders
   faucet verify verify.yaml --row orders --json
   ```
7. **Cut over and keep the way back.** With the user, pick one: repoint readers
   at faucet's dataset, or stop the old schedule, carry the final bookmark over
   (step 4) and point faucet at the original tables. Export faucet state after
   the first production run, watch `faucet status`, and keep the old pipeline
   runnable (schedule disabled, nothing deleted) until the user signs off. Only
   then remove old schedules, replication slots, plugins and tables, in that
   order, each with the user's go-ahead.

## Gaps to check and tell the user about

Check each against the inventory and the binary, and say plainly which apply:

- A source with no bookmark mode in its schema: the old incremental stream
  becomes a clock-scoped window with a keyed write mode, history comes from
  `faucet backfill`, and a skipped run leaves a gap until backfilled.
- The Singer bridge: the tap's runtime must exist on every host, it runs at
  tap speed, one stream per row, and resumes at the tap's STATE granularity,
  so it needs a keyed sink to avoid duplicates.
- Mappers with arbitrary expressions or date math, and hashes the transforms
  in `faucet list` cannot reproduce (a changed hash breaks joins on it).
- A target that merged on keys, where the replacement sink's schema lists no
  keyed write mode.
- Metadata columns (`_sdc_*`, `_airbyte_*`) readers depend on that faucet's
  metadata columns do not reproduce.
- Plugins with no data-movement role (dbt, utilities, orchestrators) stay
  outside faucet.

## Examples

Validated configs live in the faucet-stream repository under `cli/examples/`,
at the tag matching the binary (`faucet-cli-v<version>`; the latest is at
<https://github.com/faucet-hq/faucet-stream/tree/main/cli/examples>):

- Database table, incremental by a column: `cli/examples/postgres_incremental_to_jsonl.yaml`
- REST API, incremental into an upsert sink: `cli/examples/rest_to_postgres.yaml`
- Snapshot then CDC: `cli/examples/postgres_replicate_snapshot_cdc.yaml`
- Running a tap: `cli/examples/singer_to_jsonl.yaml`; keeping a target: `cli/examples/csv_to_singer_target.yaml`
- Metadata columns: `cli/examples/metadata_columns.yaml`
- Comparison-only config for the parallel run: `cli/examples/verify_previous_pipeline_tables.yaml`
