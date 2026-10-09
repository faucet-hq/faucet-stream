---
name: faucet-debug
description: >-
  Use when a faucet pipeline misbehaves: a `faucet run` failed or exited
  non-zero, a run is slow, stuck or behind its source (CDC / stream lag), the
  source is rate-limited or throttled, rows are missing or duplicated, rows
  landed in the dead-letter queue, wrong values landed in the destination, a
  bookmark needs moving or resetting, stored state will not read after an
  upgrade, a scheduled run was skipped, a run refuses to start because another
  holds the row, or a connector fails with an auth, permission or network
  error. Covers `faucet doctor`, `status`, `dlq`, `state`, `verify`,
  `rollback`, `profiling`, `explain`, `plan`, logs and Prometheus metrics.
license: MIT OR Apache-2.0
---

# Debugging faucet pipelines

faucet records what you need: per-row status, the bookmark, the dead-letter
queue (DLQ), metrics and logs. Read those first; change state or data only
once you know the cause.

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

## Golden rules

1. **Read before you write.** `faucet status`, `faucet doctor`,
   `faucet state show`, `faucet dlq inspect`, `faucet explain` and
   `faucet plan` change nothing. Run them first, every time.
2. **Export state before you move it.** Before any `state set`, `state reset`,
   `state import` or upgrade, take a backup, and run the mutating command
   with `--dry-run` first:

   ```bash
   faucet state export pipeline.yaml -o state-backup.json
   ```

3. **Never `dlq replay` until the cause is fixed.** A replay re-feeds the same
   payloads through the same checks and sink; if nothing changed, they fail
   again into a fresh DLQ file.
4. **Prefer `verify` before `rollback`.** `faucet verify` names exactly which
   keys differ, and `verify --repair` is often the smaller fix. A rollback
   undoes a whole run and rewinds the bookmark.
5. **Know the bookmark rule.** The bookmark is saved only after the sink
   confirms the page, so a crash never loses data; it can replay the page in
   flight, which at-least-once delivery may write twice
   ([how bookmarks advance](https://faucet-hq.github.io/faucet-stream/cookbook/state.html#how-bookmarks-advance)).
6. **Never edit state files, keys or tables by hand.** Use `faucet state`; it
   respects run leases and exactly-once watermarks.

## Where to look

Each command's flags: `faucet <command> --help`. What each one reports, and
how to read it:

| Question | Command | Docs |
|---|---|---|
| Which row is unhealthy, when did it last succeed, where will it resume? | `faucet status pipeline.yaml` (add `--probe` to ask the sink and source now) | [Pipeline state and status](https://faucet-hq.github.io/faucet-stream/cookbook/state-and-status.html) |
| Can every connector and the state store be reached? | `faucet doctor pipeline.yaml` (`--offline` for static lints only) | [Troubleshooting with `faucet doctor`](https://faucet-hq.github.io/faucet-stream/cookbook/troubleshooting.html) |
| What does the config actually do? | `faucet explain pipeline.yaml`, `faucet validate pipeline.yaml --show-composed` | [CLI reference](https://faucet-hq.github.io/faucet-stream/reference/cli.html) |
| What would a run produce, without writing? | `faucet plan pipeline.yaml --live` | [CLI reference](https://faucet-hq.github.io/faucet-stream/reference/cli.html) |
| What failed, and what kind of failure? | `faucet run pipeline.yaml --output json` (per-row `status` and `error`), `--log-format json` logs (`kind` field) | [Error kinds](https://faucet-hq.github.io/faucet-stream/operations/troubleshooting.html#error-kinds), [run --output](https://faucet-hq.github.io/faucet-stream/reference/cli.html#run---output) |
| Why did rows land in the DLQ? | `faucet dlq inspect ./dlq/` | [Dead-letter queues](https://faucet-hq.github.io/faucet-stream/cookbook/dlq.html) |
| What is stored for a row? | `faucet state show pipeline.yaml` | [State: show, move, reset](https://faucet-hq.github.io/faucet-stream/cookbook/state-and-status.html#state-show-move-reset) |
| Which keys differ between source and destination? | `faucet verify pipeline.yaml` | [Content verification](https://faucet-hq.github.io/faucet-stream/cookbook/verify.html) |
| Which runs can be undone? | `faucet rollback pipeline.yaml --list` | [Undoing a run](https://faucet-hq.github.io/faucet-stream/cookbook/rollback.html) |
| Did the shape of the data drift? | `faucet profiling show pipeline.yaml` | [Column profiling](https://faucet-hq.github.io/faucet-stream/cookbook/profiling.html) |
| Which row is slow, throttled, behind or losing rows? | Prometheus `/metrics` | [Finding the problem from metrics](https://faucet-hq.github.io/faucet-stream/operations/observability.html#finding-the-problem-from-metrics) |

Exit codes are part of what these commands report (`faucet status` and
`faucet doctor` exit codes are usable as checks on their own); see the
command's page above or its `--help`. Add `--json` to a read command when you
want to parse its output.

## Symptom recipes

Replace `pipeline.yaml` and `orders` (a matrix row id) with the real names.

### Run exited non-zero

```bash
faucet status pipeline.yaml
faucet run pipeline.yaml --select orders --log-level debug --output json
```

Find the failed row's `last error:` line in `status` and its `error` in the
JSON summary. The error kind names the next step; look it up in the
[error kinds](https://faucet-hq.github.io/faucet-stream/operations/troubleshooting.html#error-kinds)
table, then follow the matching recipe below. Configuration errors come from
`faucet validate pipeline.yaml`; a connector the binary lacks shows up as an
unknown type (check `faucet list`).

### A run refuses to start because another run holds the row

```bash
faucet status pipeline.yaml
```

Look for a `running` lease (pid, host, since). A live lease means another
process is running that row: wait for it. An expired lease means a run
crashed; the next run takes it over once it lapses. Pass `--force` to
`faucet run` only when you know the other run is gone.

### Auth, permission or network failure

```bash
faucet doctor pipeline.yaml --timeout-secs 5
faucet validate pipeline.yaml --show-composed
```

Find the failed probe and its `hint:` line. A failing source probe means
credentials, DNS / TLS or reachability. A failing sink probe usually means a
missing dataset, table or grant. A failing state probe means no run can save
progress: fix it before anything else. An empty `${env:VAR}` means the
variable is unset or `.env` was not loaded (see `--env-file`).

### Slow run

```bash
faucet run pipeline.yaml --output json
```

Find the slowest row by its duration, then compare its source page time with
its sink write and flush time in the metrics
([finding the problem from metrics](https://faucet-hq.github.io/faucet-stream/operations/observability.html#finding-the-problem-from-metrics)):
whichever dominates is the bottleneck. Rule out rate-limit waits first (next
recipe). The knobs are batch size, connector concurrency and how many matrix
rows run in parallel ([Throughput tuning](https://faucet-hq.github.io/faucet-stream/cookbook/tuning.html));
read the exact key names from `faucet schema source <kind>`,
`faucet schema sink <kind>` and `faucet run --help`.

### Rate-limited or throttled

Look for the run-end warning that the source spent a share of the run waiting
on rate limits, and for the throttling metrics by row. Fixes: lower
concurrency, stagger schedules so rows do not share a quota window, or teach
the source the API's reset signal
([source-side throttling](https://faucet-hq.github.io/faucet-stream/cookbook/resilience.html#source-side-throttling)).
A rate-limit failure means retries ran out; allow more only if the quota
really resets.

### CDC or stream lag

```bash
faucet status pipeline.yaml --probe
faucet doctor pipeline.yaml
```

`--probe` asks the source for its lag now; between scheduled runs the stored
value is stale. Lag that grows while every run succeeds means the pipeline
drains slower than the source produces: run more often, or speed up the sink
(slow-run recipe). On a Postgres CDC source, lag growing while nothing runs
means the replication slot is holding WAL: run the pipeline or drop the slot.
See [Source lag](https://faucet-hq.github.io/faucet-stream/cookbook/sla.html#source-lag).

### Rows in the DLQ

```bash
faucet status pipeline.yaml
faucet dlq inspect ./dlq/ --limit 10
```

Read the breakdown by reason and error kind: it says which stage rejected the
rows ([the envelope](https://faucet-hq.github.io/faucet-stream/cookbook/dlq.html#the-envelope)).
Fix that cause (data, transform, contract, destination schema) first. Then:

```bash
faucet dlq replay pipeline.yaml --from ./dlq/ --dry-run
faucet dlq replay pipeline.yaml --from ./dlq/
faucet dlq discard ./dlq/ --before 7d
```

A replay is a fresh write: on an append-only sink it can duplicate rows that
partly landed before the failure. A run that aborted with many DLQ rows means
something upstream broke; inspect before re-running.

### Duplicates after a crash or retry

```bash
faucet explain pipeline.yaml
faucet verify pipeline.yaml
```

`explain` prints the delivery guarantee and write mode. Under at-least-once
delivery the page in flight at a crash may be written twice; that is expected.
A DLQ replay into an append-only sink can also duplicate. To make the
destination immune, use a keyed upsert write mode or effectively-once delivery
([effectively-once delivery](https://faucet-hq.github.io/faucet-stream/cookbook/state.html#effectively-once-delivery)).
`verify` lists each duplicated key; it does not dedupe an append table for you.

### Missing rows

```bash
faucet status pipeline.yaml
faucet state show pipeline.yaml --row orders
faucet dlq inspect ./dlq/
faucet verify pipeline.yaml --max-differences 50
```

Check, in order: rows sitting in the DLQ; a bookmark that moved past the data
(a manual `state set`, or a replication key that is not monotonic); records
arriving without their replication key (misspelled or nested key); and a
transform filter dropping rows (transform records out below records in). The
metrics for the last two are in
[finding the problem from metrics](https://faucet-hq.github.io/faucet-stream/operations/observability.html#finding-the-problem-from-metrics).
`verify` lists the keys missing in the destination, and `verify --repair`
re-syncs them.

### Wrong values landed

```bash
faucet verify pipeline.yaml
faucet profiling show pipeline.yaml
faucet rollback pipeline.yaml --list
faucet rollback pipeline.yaml --run 0199a3f2-example --dry-run
```

If a few keys differ, use `faucet verify pipeline.yaml --repair`. If a whole
run was bad (a wrong parameter, a bad upstream extract), roll it back, newest
run first, after fixing the cause so the next run does not reload the same bad
data. Rollback has prerequisites (a `rollback:` block, durable state, a
capable sink): see [Undoing a run](https://faucet-hq.github.io/faucet-stream/cookbook/rollback.html).
A blocked rollback means later runs changed the same keys; read the
`--dry-run` output before reaching for `--force`.

### Schema drift failure

A schema-drift error means the page's shape diverged from the destination
under a `fail` policy. Confirm the new shape with
`faucet plan pipeline.yaml --live`, then change the destination or the
`schema:` policy (`faucet schema config` documents the block;
[Schema drift](https://faucet-hq.github.io/faucet-stream/cookbook/schema-drift.html)).
Do not reset state for drift: the bookmark is fine.

### Profile drift or SLA violation

`faucet status pipeline.yaml` and `faucet doctor pipeline.yaml` show SLA and
drift findings per row. A profile-drift failure marks a run whose data was
already written: if the data is wrong, follow the wrong-values recipe; if the
change is legitimate, re-baseline with `faucet profiling reset`. A volume or
row-count violation after a bookmark move usually means the bookmark moved
too far. See [SLA monitoring](https://faucet-hq.github.io/faucet-stream/cookbook/sla.html)
and [Column profiling](https://faucet-hq.github.io/faucet-stream/cookbook/profiling.html).

### State unreadable after an upgrade

```bash
faucet status pipeline.yaml
faucet state export pipeline.yaml -o before-migrate.json
faucet migrate --state pipeline.yaml --check
faucet migrate --state pipeline.yaml
```

Read the `state format` per row. An `incompatible` row was written by a newer
faucet or by a different source: run the release that wrote it, or restore an
export taken before the upgrade. Reset the row only after deciding where it
should resume. See [Upgrading faucet safely](https://faucet-hq.github.io/faucet-stream/operations/upgrading.html).

### Scheduled run skipped

A tick that fires while the previous run is still going follows the
schedule's overlap policy. Compare the last run's duration with the cron
period in the scheduler metrics. Fix: a faster run, a wider period, or a
different overlap policy (`faucet schema schedule` lists them). An open
circuit breaker also delays the next tick. See
[Overlap policy](https://faucet-hq.github.io/faucet-stream/cookbook/scheduling.html#overlap-policy).

### A bookmark needs moving or resetting

```bash
faucet state export pipeline.yaml -o state-backup.json
faucet state show pipeline.yaml --row orders --json
faucet state set pipeline.yaml --row orders --bookmark '{"updated_at":"2026-09-22T00:00:00Z"}' --dry-run
faucet state set pipeline.yaml --row orders --bookmark '{"updated_at":"2026-09-22T00:00:00Z"}' --yes
faucet state reset pipeline.yaml --row orders --dry-run
```

Copy the bookmark's shape from `state show --json`; the example above is only
a shape. Moving a bookmark backwards re-reads data (an append sink gets
duplicates); moving it forwards skips data for good; a reset means a full
re-sync. Both commands refuse while a run holds the row; use `--force` only
when that run is known to be gone. Exactly-once rows have extra rules:
[Exactly-once rows](https://faucet-hq.github.io/faucet-stream/cookbook/state-and-status.html#exactly-once-rows).
