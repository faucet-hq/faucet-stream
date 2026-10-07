# Pipeline state and status

Two commands answer the questions an operator asks when a pipeline misbehaves,
without hand-editing JSON files or SQL rows:

- **`faucet status`** — *is it healthy, when did it last succeed, what failed,
  and where will the next run resume?*
- **`faucet state`** — *show me its bookmark; move it; reset it; back it up;
  move it to another state store.*

Both read the pipeline's `state:` store. Every real run keeps two small
markers next to each row's bookmark, so the facts are there even without a
run-history server:

| Key | Written | Holds |
|---|---|---|
| `{name}::{row}` | per committed page | the bookmark (an exactly-once envelope under `delivery: exactly_once`) |
| `{name}::{row}::__status__` | after every real run | last success (time, run id, records) and last failure (time, run id, error kind + redacted message), consecutive failures |
| `{name}::{row}::__lease__` | while a run is in flight | the run id, pid and host; renewed every 20 s, expires 60 s after the last renewal |

Preview runs (`--dry-run`, `--limit`), shard executions and `memory` stores
keep neither marker. The existing markers (`::__sla__`, `::__profiling__`,
`::__rollback__…`, `{name}::__replication__`, `{name}::__backfill__::…`)
live under the same namespace, so the tools below see all of them.

## Status: one screen per pipeline

```bash
faucet status orders.yaml
```

```text
pipeline orders (3 rows) — FAILED    state: file
  row        status    last success            bookmark               lag  dlq  next run resumes at
  customers  ok        2026-09-26 06:10 (2h)   updated_at=2026-09-26  —    0    updated_at=2026-09-26
  orders     FAILED    2026-09-25 23:00 (9h)   lsn=0/3A00F128         412 MiB 17   lsn=0/3A00F128
             └ last error: Sink: deadlock detected (2026-09-26 02:14, run 01a0…)
             └ SLA staleness: last success 32400s ago exceeds max_staleness_secs 21600
             └ DLQ: 17 record(s), oldest 2026-09-26 02:14 (6h)
  refunds    warming   never                   —                      —    0    full snapshot
```

The exit code makes it a check you can schedule: **0** healthy (`ok`,
`running`, `warming`), **1** degraded or unknown, **2** failed.

```bash
# cron / Nagios-style
faucet status orders.yaml --json > /var/lib/faucet/orders-status.json || page-oncall "$?"
```

What each row reports:

- **last success / last failure** — from the run-outcome marker, the SLA
  history, and, when the config has a [`catalog:`](catalog.md) store, the run
  records `faucet serve` wrote there (`source` in `--json` says which).
- **bookmark and its age**, and **where the next run resumes**: the bookmark,
  or `full snapshot` when there is none. In topology mode a graph resumes only
  when it has one source node and every sink node's bookmark agrees — the
  screen says `full replay` otherwise.
- **exactly-once** — the envelope's committed sequence. `--probe` also reads
  the sink's committed watermark (read-only) and reports `Agree`,
  `SinkAhead` (a crash landed between the sink commit and the state write; the
  next run re-anchors to the sink's position, and the screen shows that
  position), `StateAhead` (the sink lost or rewound committed pages — degraded),
  or `NoToken`.
- **DLQ backlog** — envelopes for this row in a local `jsonl` DLQ (`${now.*}`
  path tokens match every dated file), with the oldest. Other DLQ sinks are
  noted as not countable here.
- **SLA** — staleness against `max_staleness_secs`, the last run's volume
  against `min_rows_per_run` and the learned baseline.
- **profiling** drift in the latest run and **rollback** markers (undoable runs).
- **overwrite staging** (`write_mode: overwrite` rows) — with `--probe` the
  sink is asked whether its `…__faucet_ovw` staging table / collection exists
  (postgres, sqlite, mysql, mssql, mongodb, bigquery): `present` means a
  crashed or aborted overwrite left it behind (the row is degraded; the next
  overwrite run replaces it), `absent` means clean, `unknown` means the sink
  could not tell or could not be reached. Without `--probe`, a failed last run
  shows `unknown` labelled *unverified*.
- **children** — a child row's per-parent invocations fold under their parent
  as a bookmark count, failed invocations and the worst health.
- **running** — a live run lease (pid / host / since). An *expired* lease
  means a run stopped without releasing it — it most likely crashed — and the
  row is degraded until the next run.
- **lag** — how far the source is behind its head
  ([#733](https://github.com/faucet-hq/faucet-stream/issues/733)): unread
  Postgres WAL or MySQL binlog, unconsumed Kafka messages, the age of the oldest
  unread MongoDB / SQL Server / Oracle / Kinesis / DynamoDB Streams change. The value the source reported
  when the last run ended is kept on the status marker; `--probe` asks the
  source again now, from the stored bookmark (the gauge between scheduled runs
  is stale). `—` for sources without a head. A `max_lag_*`
  [SLA threshold](sla.md#source-lag) marks the row degraded when exceeded.
- **batches** — how the last run's sink writes ended: all committed, some
  rows per row to the DLQ, whole writes to the DLQ (`dlq_all`), or failed
  ([#737](https://github.com/faucet-hq/faucet-stream/issues/737)). A run that
  sent writes to the DLQ marks the row degraded even when the DLQ sink cannot be
  counted here.
- **state format** — how the stored bookmark relates to what this release's
  source reads ([#736](https://github.com/faucet-hq/faucet-stream/issues/736)):
  `current`, `legacy` (stored before versioning; the next run rewrites it),
  `migrate` (an older bookmark shape the next run — or
  `faucet migrate --state` — migrates), or `incompatible` (written by a newer
  faucet or another source; the next run refuses it and the row is degraded).
  See [Upgrading faucet safely](../operations/upgrading.md).

Every field is read on its own: an unreachable state backend, run-history
store or DLQ shows up as a note on the row, never as a failure of the command.
A `memory` store is reported as `unknown` — there is nothing durable to read
between runs.

The same report is `GET /v1/status?template=orders` (or `?config=…`) on
[`faucet serve`](../reference/http-api.md#pipeline-status-and-state), and the
web console shows it as the **Health** card on a pipeline template's page.

## State: show, move, reset

```bash
faucet state show orders.yaml               # every row's bookmark + markers
faucet state show orders.yaml --row orders --json
```

Every bookmark is stored in a versioned envelope naming the source that owns
its shape and that shape's version; `show` prints it as a `state format` line
per row and `set` / `reset` write the envelope for you. `faucet migrate
--state` upgrades every row's stored bookmark ahead of a run
([Upgrading faucet safely](../operations/upgrading.md)).

**Replay from last Tuesday:**

```bash
faucet state set orders.yaml --row orders \
  --bookmark '{"updated_at":"2026-09-22T00:00:00Z"}'
```

`set` prints the before / after and asks for confirmation (`--yes` to skip,
`--dry-run` to stop at the plan). Without a terminal it refuses unless `--yes`
is given, so a script never changes state by accident.

**Re-sync one row from scratch:**

```bash
faucet state reset orders.yaml --row refunds --yes
faucet state reset orders.yaml --row refunds --include-markers --yes   # also forget SLA / profiling baselines
faucet state reset orders.yaml --row lines --parent-key 42 --yes       # one child invocation
```

Both refuse while a run holds the row — its lease is live, or the config's
`catalog:` store lists a run of the pipeline in flight — so a bookmark is
never moved under a running pipeline. `--force` overrides it when that run is
known to be gone.

### Exactly-once rows

A row under `delivery: exactly_once` stores its bookmark inside an envelope
with the committed page sequence, and the sink commits the same sequence with
each page. On resume the higher of the two wins. Writing a bare bookmark over
the envelope — or lowering its sequence — would let the sink's watermark
override the change, or make the next run skip pages it believes are already
committed.

So on an exactly-once row `set` keeps the envelope, reads the sink's watermark,
and writes the higher of the two sequences: the new bookmark is honoured and
every page after it gets a fresh, higher sequence. `reset` keeps the envelope
with a null bookmark at that sequence (the next run re-reads from the start),
or with `--rewind-token` deletes the sink's commit token and the envelope
together (postgres / sqlite / mysql sinks). If the watermark cannot be read,
the command refuses rather than guess; `--skip-watermark-check` writes anyway
and says what that risks.

## Backup, restore, and moving between backends

```bash
faucet state export orders.yaml -o orders-state.json
```

```json
{
  "version": 1,
  "pipeline": "orders",
  "exported_at": "2026-09-26T08:00:00Z",
  "keys": {
    "orders::customers": { "updated_at": "2026-09-26T06:09:00Z" },
    "orders::customers::__sla__": { "last_success_unix": 1790402940, "volumes": [120, 118] },
    "orders::orders": { "__faucet_eo": 1, "bookmark": { "lsn": "0/3A00F128" }, "seq": 17 }
  }
}
```

Every key under the namespace, exactly as stored (run leases excluded — they
describe a process, not a position) — values are written verbatim, never run
through log redaction, and `-o` creates the file owner-only (`0600`). The format is versioned; a frozen v1
document is part of the release compatibility suite, and an import of a newer
version is refused rather than half-read.

**Move from file state to Postgres:**

```bash
faucet state export orders.yaml -o orders-state.json
faucet state import orders.yaml orders-state.json \
  --to-state postgres://faucet@db/faucet --yes
# then point the config's `state:` block at Postgres
```

**Restore after losing the volume:**

```bash
faucet state import orders.yaml orders-state.json --yes
```

`import` refuses a namespace that already holds state unless `--overwrite`
(keys absent from the export are then deleted, so the namespace ends up exactly
as exported), and a document for another pipeline. Redis, Postgres and memory
stores import all-or-nothing; a file store writes key by key and, on a
failure, lists exactly which keys landed. `--to-state` accepts
`postgres://…`, `redis://…`, `file:DIR` (or a directory path), `memory`, or a
`{type, config}` document.

The next run on the target store resumes from the same position — including
the exactly-once sequence — which the reliability suite checks for both
delivery modes.

## Over HTTP

On `faucet serve`, `GET|PUT|DELETE /v1/state/{pipeline}/{row}` does the same
for one row — admin-only and audited. See the
[HTTP API reference](../reference/http-api.md#pipeline-status-and-state).
