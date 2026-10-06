# Backfill (bounded historical replay)

`faucet backfill` replays a **bounded historical window** of a pipeline — "reload
June for this table" — as one command instead of a hand-written throwaway
script. The range is chunked into independent window **units**, each unit
re-runs the pipeline scoped to its window, progress is recorded durably so an
interrupted backfill **resumes**, and the forward sync's bookmark is never
touched.

```bash
# Replay June 2026, one day at a time, at most 4 windows in flight
faucet backfill pipeline.yaml --from 2026-06-01 --to 2026-07-01 --window 1d --concurrency 4

# Preview the plan without running anything (the range above plans 30 units)
faucet backfill pipeline.yaml --from 2026-06-01 --to 2026-07-01 --window 1d --dry-run

# Continue an interrupted backfill: done units are skipped, failed + pending re-run
faucet backfill pipeline.yaml --from 2026-06-01 --to 2026-07-01 --window 1d --resume
```

## Scoping the source to the window

Each unit substitutes `${backfill.*}` tokens in the source **and sink** configs
before running, and sets the run's `${now.*}` clock to the unit's window start.
Your source must reference at least one scoping token — otherwise every window
would replay identical data, and the plan is rejected with a typed error
(`faucet validate` enforces the same whenever a `backfill:` block is present).

| Token | Renders as |
|-------|-----------|
| `${backfill.start}` / `${backfill.end}` | Window bounds, RFC3339 (half-open: `start` inclusive, `end` exclusive) |
| `${backfill.start_date}` / `${backfill.end_date}` | `YYYY-MM-DD` in the backfill timezone |
| `${backfill.start_unix}` / `${backfill.end_unix}` | Epoch seconds |
| `${backfill.unit}` | The unit id (`20260601T000000Z`) — handy for per-window output paths |

```yaml
version: 1
name: orders
pipeline:
  source:
    type: sqlite
    config:
      database_url: sqlite:./app.db
      query: >-
        SELECT id, day, amount FROM events
        WHERE day >= '${backfill.start_date}' AND day < '${backfill.end_date}'
  sink:
    type: sqlite
    config:
      database_url: sqlite:./mirror.db
      table_name: events_out
      column_mapping: auto_map
      write_mode: upsert      # replays converge instead of duplicating
      key: [id]
  state:
    type: file
    config: { path: ./.faucet-state }

backfill:          # defaults for `faucet backfill` (flags override)
  window: 1d
  concurrency: 4
  timezone: UTC
```

Because the `${now.*}` clock is set per unit, **dated object-store prefixes are
the partition pattern**: a source reading `prefix: raw/dt=${now.date}/` backfills
one partition per one-day window with no extra configuration. (`faucet run`
rejects a config whose source still holds a `${backfill.*}` token, pointing you
back at `faucet backfill`.)

## Windows, timezones, DST

`--window` takes `45s`, `30m`, `6h`, `1d`, or `1w`; omitted (and no
`backfill.window` default) the whole range runs as a single unit. Windows are
contiguous half-open slices of `[from, to)` — the last one truncates at `--to`.
Date boundaries like `--from 2026-06-01` are midnight in `--timezone` (IANA
name; default UTC). Units never gap or overlap. A plan above 1,000 units warns;
above 10,000 it is rejected (use a larger window).

**`1d`/`1w` are calendar steps; `24h`/`168h` are elapsed time.** The two are
identical except across a DST transition, where they deliberately differ:

| window | across US spring-forward (2026-03-08) |
|---|---|
| `1d` | every unit starts at **local midnight**; 03-08 is 23 elapsed hours |
| `24h` | units drift — 03-09 onward start at **01:00** local |

Use `1d` for date-partitioned backfills, so `${backfill.start_date}` always
names exactly the local day its window covers. Use `24h` when you want a fixed
amount of elapsed time regardless of the calendar. A fall-back (repeated) hour
resolves to the earliest instant, matching how `--from`/`--to` dates resolve.

> Changing between `1d` and `24h` changes the unit boundaries, so it also changes
> the progress-marker hash — an in-flight backfill will not resume across that
> switch. Absolute windows keep the hash they had before calendar stepping
> existed, so upgrades do not disturb a running backfill.

## Progress, resume, restart

A durable marker at `{name}::__backfill__::{range-hash}` in the pipeline's
`state:` store records each unit's terminal outcome. Re-running the same range:

- without a flag → an error telling you the marker exists (`N done, M failed`) —
  pass `--resume` or `--restart`;
- `--resume` → done units are skipped; failed and pending units re-run;
- `--restart` → the marker is discarded and everything re-runs.

Interruption is safe: Ctrl-C / SIGTERM cancels cooperatively (in-flight units
flush at their next page boundary), interrupted units are *not* marked done, and
the exit code equals the failed-unit count. Without a `state:` block the marker
is in-memory only (a warning tells you `--resume` won't survive a restart).

## Idempotency: pair with upsert

Backfill forces **at-least-once** delivery per unit. Replaying an overlapping
window into an append-only sink duplicates rows — the command warns loudly.
The recommended shape is `write_mode: upsert` with a `key` (see
[upsert / mirror tables](upsert.md)), which makes any replay converge. To be
extra careful, redirect the backfill at a staging sink first:

```bash
faucet backfill pipeline.yaml --from 2026-06-01 --to 2026-07-01 --into staging
```

`--into <name>` swaps the destination for the named template under
`pipeline.sinks`.

## Bookmark-range mode

For sources whose replication key is not time-shaped, replay between two
explicit bookmark values instead:

```bash
faucet backfill pipeline.yaml --from-bookmark '"2026-06-01T00:00:00Z"' \
  --to-bookmark '"2026-07-01T00:00:00Z"' --bookmark-field updated_at
```

`--from-bookmark` seeds the backfill's *scoped* state key (the source's own
incremental logic reads forward from it; requires a `state:` block);
`--to-bookmark` drops records whose `--bookmark-field` orders after the bound
before they reach transforms or the sink. Values parse as JSON first (numbers,
quoted strings), falling back to a bare string. Bookmark mode always runs as a
single unit. The live `{name}::{row}` bookmark is untouched either way — every
unit runs under `{name}::backfill::{unit}`.

## Backfill over HTTP (`faucet serve`)

`POST /v1/backfill` plans the same window units server-side and submits **one
tracked run per unit** — each with the full run lifecycle (history record, SSE
logs, cancel, `timeout_secs`, cluster pull-balancing):

```bash
curl -s -X POST http://127.0.0.1:8080/v1/backfill \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d "$(jq -n --rawfile cfg pipeline.yaml \
        '{config: $cfg, from: "2026-06-01", to: "2026-07-01", window: "1d"}')"
```

Unit runs are named `{name}-backfill-{unit}` and labelled
`backfill=<range-hash>` + `backfill_unit=<unit>`; the pipeline `name` is
rewritten per unit so state keys stay namespaced, and delivery is forced to
at-least-once. Deterministic idempotency keys (`backfill:{hash}:{unit}`) make
re-POSTing the same body replay-safe: already-submitted units replay,
unsubmitted ones proceed. With a `state:` block the range is recorded, and a
later POST of it is refused with `409` unless it sets `"resume": true`
(continue it — the HTTP form of `--resume`) or `"restart": true` (delete each
unit's scoped state and run every unit again — `--restart`), so a re-backfill
can never silently start past its old bookmark. A full queue marks the
remainder `not_submitted`; re-POST with `"resume": true`. A config carrying `shard: { count }`
makes each unit a sharded run tracked via shard progress, so a single wide
window scales horizontally under `serve --cluster`. Bookmark-range backfills
are CLI-only. Requires the `RunWrite` permission (operator); audited as
`backfill.submit`. Full shapes: [HTTP API](../reference/http-api.md).

## Metrics

| Metric | Meaning |
|--------|---------|
| `faucet_backfill_units_total{pipeline,outcome}` | Units finished, `outcome` ∈ `ok` \| `err` \| `skipped` (resume) |
| `faucet_backfill_progress_ratio{pipeline}` | Done fraction of the planned units (0.0–1.0) |

## Reference

- Config block: [`backfill:`](../reference/config.md#backfill) ·
  `faucet schema backfill`
- Command flags: [CLI reference](../reference/cli.md#backfill)
- Example: `cli/examples/backfill_sqlite_to_jsonl.yaml`
