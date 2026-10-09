# Migrating from Meltano, Singer or Airbyte

A migration is four jobs: translate the configs, carry the bookmarks over,
prove the new pipeline produces the same data, and switch without losing the
way back. This page covers what is specific to arriving from a Singer-protocol
setup (Meltano or plain `tap | target`) or from Airbyte. Writing the faucet
configs themselves is the same as for any pipeline: start from
`faucet init --source <x> --sink <y>`, read `faucet schema source|sink <name>`,
and iterate on `faucet validate`.

The old pipeline stays authoritative until faucet has matched it for long
enough to trust. Nothing old is deleted before that.

## 1. Read the old setup

Write down one line per stream: source system, stream / table, replication
method and key, primary key, selected columns, mapper steps, destination
table, schedule, where its bookmark lives, and what reads the table
downstream. Note credentials by the **name** of the variable or secret that
holds them, never by value.

### Meltano

| `meltano.yml` block | What it tells you |
|---|---|
| `include_paths` | Other files the project is split across; read them all. |
| `plugins.extractors[]` | Each tap: `config`, `select`, `metadata`, `schema` overrides, `inherit_from` (resolve the parent first). |
| `plugins.loaders[]` | Each target: destination schema, load method, whether it adds record metadata. |
| `plugins.mappers[]` | `stream_maps` that change rows between tap and target. |
| `jobs[]` | Ordered task chains (`tap mapper target`, `dbt:run`). |
| `schedules[]` | `interval` (cron or `@hourly` / `@daily` ...) per job. |
| `environments[]` | Per-environment overrides; check the production values, not only the base. |

- `select` patterns are `<stream>.<property>` with `*` wildcards and a leading
  `!` to exclude; `meltano select <extractor> --list --all` prints the resolved
  set.
- `metadata` per stream holds `replication-method` (`FULL_TABLE`,
  `INCREMENTAL`, `LOG_BASED`) and `replication-key`. A stream without an entry
  uses the tap's default, often full table.
- Settings resolve from environment variables first, then the project `.env`,
  then the active environment, then the base plugin block. The variable for a
  setting is `<PLUGIN_NAME>_<SETTING_NAME>`, upper-cased with `-` turned into
  `_` (`tap-postgres` / `sqlalchemy_url` → `TAP_POSTGRES_SQLALCHEMY_URL`).
  `meltano config <plugin> list` shows where each value comes from. A
  SQLAlchemy URL may carry a driver suffix (`postgresql+psycopg2://`) that a
  faucet connection URL does not accept, so a new variable holding a plain URL
  is often needed.
- Bookmarks: `meltano state list` names every state id;
  `meltano state get <state_id>` prints `{"singer_state": {...}}`. The inner
  `singer_state` object is what the tap receives as `--state`.

### Plain Singer

The tap's `config.json` holds connection settings; the catalog
(`catalog.json` / `properties.json`) lists `streams[]` with `tap_stream_id`,
`key_properties` and `metadata[]` (the `breadcrumb: []` entry carries
`selected`, `replication-method`, `replication-key`); `state.json` holds the
last STATE value. Find the wrapper that runs `tap | target` to learn the
schedule and which state file it passes.

### Airbyte

Per connection, record the source and destination settings, the schedule,
namespace and stream-prefix settings, and for every enabled stream its sync
mode, cursor field and primary key.

| Airbyte sync mode | faucet |
|---|---|
| Full refresh, Overwrite | full read, `write_mode: overwrite` |
| Full refresh, Append | full read, `write_mode: append` |
| Incremental, Append | bookmark on the cursor field, `write_mode: append` |
| Incremental, Append + Deduped | bookmark on the cursor field, `write_mode: upsert`, `key` = primary key |

Check that the destination sink lists the mode in its schema
(`faucet schema sink <name>`); see [Upsert / mirror tables](../cookbook/upsert.md).

### Downstream readers

For every destination table, list what reads it and which columns it relies
on, including the old tool's metadata columns (`_sdc_*`, `_airbyte_*`) and
the table naming. A change of table name, column type or metadata column
breaks those readers even when the rows match.

## 2. Choose a replacement for each tap and target

Decide by **what the tap reads**, not by its name:

| The tap reads | Replace it with |
|---|---|
| A SQL database, periodic or by an `updated_at`-style column | The database's query source, with its bookmark mode where it has one ([Incremental replication & state](../cookbook/state.md)) |
| A SQL database, every change including deletes (`LOG_BASED`) | The database's CDC source, started with `faucet mirror` ([Mirror](../cookbook/replication.md)) |
| A REST or GraphQL API | A Template Hub source template if one covers the API (`faucet hub list`, [Template Hub](../cookbook/template-hub.md)), else the `rest` / `graphql` source |
| Files, local or in object storage | The `file` source or the object-store source ([File formats](../cookbook/file-formats.md)) |
| A queue or event stream | The matching queue source |
| Anything with no faucet equivalent | The `singer` source, running the tap unchanged (below) |

`faucet list` shows which connectors this build has; `faucet list --available`
shows the whole registry. A target maps the same way: the native sink for the
destination, or the `singer` sink to keep the target
([vs. Meltano](../comparison/meltano.md#keep-a-singer-target-you-depend-on)).

Moving from a tap to a template changes more than the runner: stream names,
field shapes and per-stream sync modes are the template's own, and credentials
become template params. Compare the template's rows (`faucet hub rows`) with
the tap's selected streams before committing to it.

### Running a tap through the `singer` source

The bridge is a stepping stone. It runs the tap as a child process, so the tap
and its Python runtime must be installed on every host that runs the pipeline,
and throughput is the tap's. It emits **one stream per config row** (records
for other streams are dropped), so a multi-stream tap becomes one matrix row
per stream. `faucet init --source singer --discover --executable <tap> --stream <stream>`
runs the tap's discovery and writes a catalog with that stream selected;
without a selected stream most taps sync nothing and exit cleanly.

faucet stores the tap's STATE value as the row's bookmark, only after the sink
confirms every record before it, and passes it back as `--state`. A crash
replays from the last persisted STATE, and many taps re-send rows at the
bookmark, so pair the bridge with a keyed sink (`write_mode: upsert`). See the
[`faucet-source-singer` README](https://github.com/faucet-hq/faucet-stream/tree/main/crates/source/singer).

## 3. Translate selection and mappers

- **Column selection**: on a SQL source, list the columns in the query (an
  excluded column never leaves the database); on an API source, use the
  `select` / `drop` transforms.
- **Stream maps** become [transforms](../cookbook/transforms.md): removing,
  renaming, casting, constant columns, fallbacks, filters, flattening and
  exploding all have one. Arbitrary Python expressions, date math and
  cross-field arithmetic do not, short of the [SQL transform](../cookbook/sql-transform.md)
  in builds that include it; otherwise move that logic downstream.
- **Hashing**: the `hash` transform offers SHA-256 and BLAKE3, not md5, so a
  hashed join key changes value. Rebuild joins on it, or exclude it from the
  comparison in step 6.
- **PII**: a mapper that existed to hide data is better expressed as a
  [`masking:`](../cookbook/masking.md) block, which runs before any sink or DLQ
  sees a row.
- **Metadata columns**: Singer targets with record metadata write
  `_sdc_extracted_at`, `_sdc_batched_at`, `_sdc_deleted_at` and others.
  faucet's `metadata_columns:` block stamps its own set under a configurable
  prefix (`faucet schema config`); a `_sdc` prefix keeps
  `_sdc_extracted_at`-style names, but there is no batched or deleted
  equivalent. Tell the owners of readers that use them.

## 4. Translate schedules

| Today | After |
|---|---|
| Meltano's scheduler | a `schedule:` block run by `faucet schedule` under a supervisor ([Scheduling](../cookbook/scheduling.md)) |
| An orchestrator calls `meltano run` | keep the orchestrator; call `faucet run` instead ([Orchestration](../cookbook/orchestration.md)) |
| A job that runs dbt after the load | the orchestrator runs `faucet run`, then dbt; faucet does extract-load only |

Meltano intervals translate to five-field cron: `@hourly` → `0 * * * *`,
`@daily` → `0 0 * * *`, `@weekly` → `0 0 * * 0`, `@monthly` → `0 0 1 * *`,
`@yearly` → `0 0 1 1 *`; `@once` is a single `faucet run`. Set the schedule's
timezone explicitly to the one the old scheduler evaluated cron in. Ordered
extract-loads within one job become `depends_on` between matrix rows.

## 5. Carry bookmarks over

Only for streams that will continue into tables the old pipeline already
filled. A parallel run into a fresh schema usually wants a full initial load
instead, so step 6 compares complete tables.

1. Stop the old schedule for that stream first; a bookmark read while it keeps
   running is already stale.
2. Export faucet state (`faucet state export`) before any change, and run each
   `faucet state set` / `faucet state import` with `--dry-run` first
   ([Pipeline state & status](../cookbook/state-and-status.md)).
3. Prefer a bookmark slightly **behind** the old one: behind re-reads a little
   (harmless with upsert), ahead silently skips rows.

| faucet source | How the old bookmark comes over |
|---|---|
| `singer` | The bookmark *is* the tap's STATE: set the inner `singer_state` object unchanged with `faucet state set`. |
| `rest` / `graphql` | The bookmark is the bare `replication_key` value. With no faucet state yet, put it in `start_replication_value` (used only while the row has no stored bookmark); otherwise `faucet state set` it, as a JSON value (a timestamp is a JSON string). Translate it if the tap stored a different format. |
| A SQL source with a `replication:` block | `replication.initial_value` with no faucet state yet; otherwise `faucet state set` with a bookmark shaped like the one `faucet state show` prints for that row after a scratch run. |
| A source without a stored bookmark | Nothing to carry. The old bookmark is the start of a `faucet backfill` that closes the gap to faucet's first scheduled run ([Backfill](../cookbook/backfill.md)). |
| A CDC source | Do not reuse the tap's log position or replication slot. Give faucet its own and start with `faucet mirror`, which snapshots and then streams from a position captured before the snapshot. |
| A templated or windowed stream | Compose or copy the config with `state:` pointed at a scratch directory, run it once, read the shape with `faucet state show`, then set the real row. |

Airbyte state blobs are not a faucet format: take the cursor value out of a
stream's state and use it as above. Never hand-edit state files or tables;
`faucet state set` writes the versioned envelope the source expects.
`faucet state import --overwrite` of the export from step 2 is the undo for any
carry-over mistake.

## 6. Run side by side and compare

- Point every faucet sink at a **separate schema or dataset**, never at the
  tables the old pipeline writes, and give faucet its own state store.
- Load the full history (or backfill the same range the old tables hold), then
  run on schedule next to the old pipeline for several cycles, including the
  busiest period.
- Compare by content with [`faucet verify`](../cookbook/verify.md), twice:
  faucet against the source system (the real config), and faucet against the
  old pipeline's tables with a **comparison-only** config whose source reads
  the old table and whose sink is faucet's
  ([`cli/examples/verify_previous_pipeline_tables.yaml`](https://github.com/faucet-hq/faucet-stream/blob/main/cli/examples/verify_previous_pipeline_tables.yaml)).
  Never `faucet run` or `--repair` a comparison-only config: it would copy the
  old rows into faucet's table.
- Make the comparison fair: exclude both tools' metadata columns and any column
  computed differently on purpose, compare during a quiet window (rows changing
  mid-scan show as differences), and avoid `${now.*}`-windowed source queries
  in the comparison (everything outside the window reads as extra).

| Difference | Likely cause during a migration |
|---|---|
| missing in destination | faucet's window or backfill did not cover it, a filter differs, or the row went to faucet's DLQ |
| extra in destination | the old pipeline never propagated a delete, or its selection was narrower |
| changed columns | a type or format difference, a transform difference, or a late update one side has not read yet |
| duplicate | an append-only side replayed a page; use a keyed write mode |

## 7. Cut over and keep the way back

Pick one:

- **Readers move to faucet's schema.** Repoint models, views and dashboards,
  then stop the old schedule. Reversible by repointing back.
- **faucet takes over the original tables.** Stop the old schedule, carry the
  final bookmark over (step 5), point the sink at the original tables,
  validate, run once, verify, then start the schedule. Readers stay put, but
  the table shapes must match what they expect.

Either way, export faucet state after the first production run and watch
`faucet status` for the first scheduled runs. Until the owner signs off, keep
the old pipeline runnable (schedule disabled; project, plugins, state,
connections and tables kept). An unread Postgres replication slot of an old
`LOG_BASED` tap retains WAL on the source, so watch disk and drop it only
after sign-off. A faucet run that loaded bad data into a sink with a
`rollback:` block can be undone with [`faucet rollback`](../cookbook/rollback.md).
