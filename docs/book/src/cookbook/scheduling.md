# Scheduling pipelines with `faucet schedule`

`faucet schedule` runs a pipeline on a cron schedule in a **long-running
foreground process**. It is designed for server-side deployment: drop it into
systemd, Kubernetes, or any supervisor that can restart it on failure, and the
pipeline fires on time every time.

```bash
faucet schedule pipeline.yaml           # foreground; Ctrl-C or SIGTERM to stop
faucet schedule pipeline.yaml --once    # run exactly once now, then exit
```

The config must include a `schedule:` block alongside the usual `pipeline:`.
Configs without one are rejected with a hint to use `faucet run` instead.

## A runnable example

The following config runs a CSV→JSONL pipeline every night at 02:00
America/Los_Angeles. Save it as `nightly.yaml` and start it with
`faucet schedule nightly.yaml`:

```yaml
# nightly.yaml — run at 02:00 Pacific every night
version: 1
name: nightly-rollup

schedule:
  cron: "0 2 * * *"
  timezone: "America/Los_Angeles"
  overlap_policy: skip            # don't pile up if a run runs long
  max_consecutive_failures: 5     # exit non-zero after 5 straight failures (supervisor restarts)
  on_failure: continue
  shutdown_grace_secs: 30

pipeline:
  source:
    type: file
    config:
      path: ./events.csv
  sink:
    type: file
    config:
      path: ./events.jsonl
```

See [`cli/examples/scheduled_nightly.yaml`](https://github.com/faucet-hq/faucet-stream/blob/main/cli/examples/scheduled_nightly.yaml)
for the canonical copy.

## Cron syntax

faucet uses a standard Unix cron expression, validated at config-load time.
A bad expression or an expression that can never fire produces a clear error
before the process starts.

**5-field form** (`MIN HOUR DOM MON DOW`):

| Expression | Meaning |
|------------|---------|
| `0 2 * * *` | Every night at 02:00 |
| `*/15 * * * *` | Every 15 minutes |
| `0 9 * * 1-5` | Weekdays at 09:00 |
| `0 0 1 * *` | First of every month at midnight |
| `0 */6 * * *` | Every 6 hours |

**6-field form** (`SEC MIN HOUR DOM MON DOW`) — add a leading seconds field
for sub-minute intervals:

| Expression | Meaning |
|------------|---------|
| `*/30 * * * * *` | Every 30 seconds |
| `0 */5 * * * *` | Every 5 minutes (explicit seconds=0) |

Field ranges follow standard cron semantics: `*` (every), `*/N` (every N),
`a-b` (range), `a,b,c` (list). Month and day-of-week names (`JAN`, `MON`,
etc.) are accepted. Special strings like `@daily` and `@hourly` are **not**
supported — use the numeric form.

## Timezone and DST

Set `timezone` to any [IANA timezone name](https://www.iana.org/time-zones)
(e.g. `America/Los_Angeles`, `Europe/Berlin`, `Asia/Tokyo`). The default is
`UTC`.

All tick times are computed on UTC monotonic instants with timezone-correct
wall-clock interpretation, so DST transitions behave correctly:

- **Fall-back (clocks go back):** a repeated wall-clock hour fires **once**.
- **Spring-forward (clocks skip ahead):** a wall-clock time in the skipped
  hour is treated as if it were in the hour immediately after the gap — the
  next valid tick.

The scheduler loop re-checks the wall clock at least every 30 seconds, so
NTP steps, VM freeze/thaw, and DST shifts can never drift a scheduled fire
by more than ~30 seconds.

## Missed-tick behavior

The scheduler advances from the **scheduled** tick, not the wall clock, so a
single occurrence is **not skipped** just because dispatch latency pushed the
clock a little past it — it fires promptly (slightly late) and the schedule
resumes. But if many ticks elapsed (the process was down, or a run took longer
than several cron periods), the backlog is **collapsed to a single catch-up**:
the scheduler fires once at the next due time and moves on. There is no
catch-up storm and no flood of backfilled runs.

To find out how late a run fired, scrape
`faucet_schedule_run_lateness_seconds` (histogram: `actual_start −
scheduled_for`).

## Overlap policy

The overlap policy controls what happens when a tick fires while a run is
already executing.

| Policy | When to use |
|--------|-------------|
| `skip` (default) | The tick is dropped and a `faucet_schedule_overlaps_total{policy=skip}` counter is incremented. Use when it is acceptable to miss a cycle if the previous one ran long. Most pipelines. |
| `queue` | One missed tick is buffered and fires immediately when the current run finishes. Further misses during that same run collapse into the single queued tick (in-memory only — lost on restart). Use when missing a cycle is unacceptable but strict concurrency still must be preserved. |
| `forbid` | The process exits non-zero the moment an overlap would occur. Use when overlapping runs would produce corrupt output or you want a hard guarantee that no two instances run simultaneously — pair with a supervisor that alerts or pages on non-zero exit. |

**Choosing between skip and queue:** if your pipeline is idempotent and
catching up after a long run matters (e.g. incremental replication with
state), use `queue`. If occasional missed cycles are harmless and you prefer
simplicity, use `skip`.

## Failure model and supervisor integration

Two independent knobs govern what happens when a run fails:

| `on_failure` | `max_consecutive_failures` | Behaviour |
|---|---|---|
| `continue` (default) | `null` | Tolerates all failures indefinitely. Alert via `faucet_schedule_consecutive_failures` gauge. |
| `continue` | `N` | Tolerates up to N−1 straight failures; exits non-zero when the Nth consecutive failure occurs. A successful run resets the counter to 0. |
| `stop` | any | Exits non-zero immediately on the **first** failure. |

The recommended production pattern is `on_failure: continue` with
`max_consecutive_failures: N` (5–10 depending on how quickly you want a
supervisor restart):

```yaml
schedule:
  cron: "*/5 * * * *"
  on_failure: continue
  max_consecutive_failures: 5   # restart after 5 straight failures
```

### Systemd unit example

```ini
# /etc/systemd/system/nightly-rollup.service
[Unit]
Description=faucet nightly rollup
After=network.target

[Service]
Type=simple
ExecStart=/usr/local/bin/faucet schedule /opt/pipelines/nightly.yaml
Restart=on-failure
RestartSec=30s
# Env vars for the pipeline
EnvironmentFile=/opt/pipelines/nightly.env

[Install]
WantedBy=multi-user.target
```

`Restart=on-failure` means systemd restarts the process whenever it exits
with a non-zero code, which is exactly the condition `max_consecutive_failures`
produces. `RestartSec=30s` adds a brief cooldown between restarts to avoid
hammering a broken upstream.

### Kubernetes CronJob vs long-running Deployment

`faucet schedule` is designed for a **Deployment** (or long-running Pod):
one process, always running, fires on cron. This keeps token caches warm
and avoids cold-start latency on every tick.

If you need Kubernetes to manage the schedule itself, use a Kubernetes
**CronJob** with `faucet run` instead — each invocation is ephemeral and
the scheduler handles missed/overlapping pods at the platform level.

## Graceful shutdown and SIGTERM

On SIGTERM or Ctrl-C:

1. faucet stops accepting new ticks.
2. With no run in flight, the process exits 0.
3. A run in flight is cancelled cooperatively: each row stops at its next page
   boundary and flushes its sink (Parquet footers, multipart uploads,
   overwrite staging). faucet waits up to `shutdown_grace_secs` (default 30)
   for that, then exits 130 — the run was interrupted.
4. If the run has not stopped after the grace period, it is aborted. The
   per-page `StateStore` bookmark means the next start resumes from the last
   confirmed write — no data is lost, but the partial page since the last
   bookmark is re-fetched on the next run. Whether that causes duplicates
   depends on your sink's idempotency.

`run_timeout_secs` stops a run the same way: the run is cancelled at its next
page boundary, given `shutdown_grace_secs` to flush, and counted as failed.

Increase `shutdown_grace_secs` for long-running pages (e.g. a BigQuery batch
that takes several minutes to flush):

```yaml
schedule:
  cron: "0 * * * *"
  shutdown_grace_secs: 120
```

## Hot config reload (SIGHUP)

Edit the config and send the scheduler **SIGHUP** to reload it in place — no
restart, no dropped ticks, and any in-flight run keeps running:

```bash
kill -HUP $(pgrep -f 'faucet schedule')
```

On SIGHUP faucet re-reads and re-validates the config file (cron, timezone,
pipeline, execution, resilience, SLA) and atomically swaps the schedule for the
next tick. If the new config is **invalid** (bad cron, missing `schedule:`,
unknown connector, …) the reload is rejected, an error is logged, and the
scheduler keeps running on the previous config. The consecutive-failure counter
and run ordinal are preserved across a reload. Each attempt is counted in
`faucet_schedule_reloads_total{pipeline,outcome=ok|error}`.

The shared `auth:` catalog (cached tokens), lineage emitter, notifier, and
catalog handle are **not** rebuilt on reload — they hold pooled connections /
tokens reused across ticks, so an `auth:` change needs a restart. (SIGHUP is a
Unix signal; on other platforms use a restart.)

## Dated outputs with `${now.*}`

`${now.*}` tokens let you inject the run's wall time into source and sink config
values — so a scheduled pipeline can write to a different file or object-storage
prefix on every tick without any manual bookkeeping.

The headline use case is a dated partition path:

```yaml
# nightly_partitioned.yaml — write to a new dated partition every night
version: 1
name: nightly-events

schedule:
  cron: "0 2 * * *"
  timezone: "America/Los_Angeles"
  overlap_policy: skip
  max_consecutive_failures: 5

pipeline:
  source:
    type: rest
    config:
      base_url: https://api.example.com
      path: /v1/events
  sink:
    type: file
    config:
      # ${now.date} reflects the schedule's timezone (America/Los_Angeles),
      # so the partition label matches the business date of the run.
      path: "./warehouse/dt=${now.date}/events.jsonl"
```

When the cron fires at 02:00 on 2026-03-09 Pacific time, `${now.date}` resolves
to `2026-03-09` and faucet writes to `./warehouse/dt=2026-03-09/events.jsonl`.
The parent directory is created automatically — local file sinks (JSONL, CSV)
create missing parent directories so dated subdirectory paths work without
pre-creating the tree.

The full token set:

| Token | Example | Use case |
|-------|---------|----------|
| `${now.date}` | `2026-03-08` | Daily partition key |
| `${now.year}` / `${now.month}` / `${now.day}` | `2026` / `03` / `08` | Hive-style `year=…/month=…/day=…` paths |
| `${now.hour}` | `14` | Hourly partitions |
| `${now.unix}` | `1741442709` | Unique epoch-based filenames |
| `${now.strftime.<fmt>}` | `2026/03/08/14` | Arbitrary layout — e.g. `${now.strftime.%Y/%m/%d/%H}` |
| `${now.datetime}` / `${now.iso}` | `2026-03-08T14:05:09+00:00` | RFC 3339 timestamp in a filename or object key |

### Clock semantics

`faucet schedule` uses the **tick's scheduled time** rendered in the schedule's
`timezone` — not the actual wall clock when the run started. This means
`${now.date}` is deterministic: re-running the same tick (e.g. after a restart)
produces the same path.

`faucet schedule --once` uses the current wall clock in the schedule's timezone.

### Backfilling with `faucet run --clock`

To backfill a range of dates, use `faucet run` with the `--clock` flag instead
of `faucet schedule`. `--clock` overrides the process start time used by
`${now.*}`:

```bash
# Backfill three nightly partitions
faucet run --clock 2026-03-01 nightly_partitioned.yaml
faucet run --clock 2026-03-02 nightly_partitioned.yaml
faucet run --clock 2026-03-03 nightly_partitioned.yaml
```

A bare date (`2026-03-01`) is treated as midnight UTC. An RFC 3339 timestamp
(`2026-03-01T02:00:00-08:00`) sets the clock precisely. Unknown `${now.*}`
tokens are config errors; the token set is validated at run start before any
I/O begins.

## Health metrics to scrape

Register a Prometheus listener via the `observability:` block:

```yaml
observability:
  prometheus:
    listen: "127.0.0.1:9464"
```

Key metrics for a scheduling health dashboard:

| Metric | What to alert on |
|--------|-----------------|
| `faucet_schedule_heartbeat_unix_seconds` | `time() - value > 90` → scheduler loop is stuck or process crashed |
| `faucet_schedule_consecutive_failures` | `> 0` → at least one recent failure; `>= max_consecutive_failures` → imminent exit |
| `faucet_schedule_next_tick_unix_seconds` | `value - time() > 2 * expected_interval` → scheduler is not advancing |
| `faucet_schedule_runs_total{outcome="err"}` | Increasing counter → runs are failing |
| `faucet_schedule_overlaps_total` | Repeated increments → runs are taking longer than the cron period |
| `faucet_schedule_run_lateness_seconds` | p99 > threshold → runs are starting significantly late |

Full metric reference:

| Metric | Type | Description |
|--------|------|-------------|
| `faucet_schedule_runs_total{pipeline,outcome}` | Counter | `outcome` ∈ `{ok, err, skipped}` |
| `faucet_schedule_overlaps_total{pipeline,policy}` | Counter | Overlap events by policy |
| `faucet_schedule_next_tick_unix_seconds{pipeline}` | Gauge | Unix timestamp of the next scheduled tick |
| `faucet_schedule_runs_in_flight{pipeline}` | Gauge | 0 or 1 |
| `faucet_schedule_consecutive_failures{pipeline}` | Gauge | Resets to 0 on success |
| `faucet_schedule_heartbeat_unix_seconds{pipeline}` | Gauge | Updated every loop wake (≤30 s) |
| `faucet_schedule_last_run_started_unix_seconds{pipeline}` | Gauge | |
| `faucet_schedule_last_run_completed_unix_seconds{pipeline}` | Gauge | |
| `faucet_schedule_last_run_duration_seconds{pipeline}` | Gauge | |
| `faucet_schedule_run_lateness_seconds{pipeline}` | Histogram | `actual_start − scheduled_for` |

Each run also emits a `faucet.schedule.run` tracing span (attributes:
`run_ordinal`, `scheduled_for_unix_seconds`, `tick_unix_seconds`) that wraps
the inner pipeline spans, so distributed tracing carries the scheduling
context through the full pipeline.
