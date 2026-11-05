# Schedules

A **schedule** runs a registered [template](./templates.md) on a cron, inside
`faucet serve`. It is separate from [triggers](./triggers.md), which react to
events (an object landing, a webhook, a queue backing up).

| You want | Use |
|---|---|
| One pipeline on a cron in its own process | [`faucet schedule`](./scheduling.md) |
| Templates on a cron, managed in a running server | a schedule (this page) |
| A run when something happens | a [trigger](./triggers.md) |

Schedules need a server built with the `schedule` and `templates` features
(the release binaries, the Homebrew formula and the `full` image have both).

## Where schedules live

- **In the console or API.** Create, edit, pause and delete them on the
  **Schedules** page or through `/v1/schedules`. They are stored in the run
  history and never purged, so use a persistent `--history`
  (`sqlite:` or `postgres://`). Every replica sharing that history picks a
  change up within a few seconds, with no restart.
- **In a file.** `faucet serve --schedules schedules.yaml` loads schedules
  from a file (`faucet schema schedules` prints its schema). They show in the
  console marked *from schedules file*: they can be run now, but editing,
  pausing and deleting happen in the file, followed by a restart.

A name in the file wins over a stored schedule with the same name.

```yaml
version: 1
schedules:
  - name: nightly-rollup
    cron: "0 2 * * *"            # 5 fields, or 6 with seconds first
    timezone: Europe/Berlin      # IANA name, DST-correct; default UTC
    overlap: skip                # skip | queue | forbid
    template:
      id: billing-rollup
      version: stable            # a channel or a version number
      params: { region: eu, day: "${schedule.tick}" }
    run:
      labels: { team: finance }
      timeout_secs: 2700
```

The full example is
[`cli/examples/schedules/schedules.yaml`](https://github.com/faucet-hq/faucet-stream/blob/main/cli/examples/schedules/schedules.yaml).

## Fields

| Field | Meaning |
|---|---|
| `name` | Letters, digits, `-` and `_`, at most 64. Cannot change after creation. |
| `description` | Free text shown in the console. |
| `cron`, `timezone` | When it ticks. The console previews the next ticks in the timezone and in UTC. |
| `catch_up` | Default `true`: after a gap (no server running, or the server fell behind), run the latest missed tick once. `false` skips missed ticks. Either way missed ticks are counted. |
| `overlap` | What a tick does while a run from an earlier tick is still going: `skip` it (default), `queue` it until that run ends, or `forbid` (record the tick as failed). |
| `paused` | Start paused. |
| `template.id`, `version` | A registered template and a version selector (`stable` by default, resolved at each tick; a number pins it). |
| `template.params` | Param values. `${schedule.tick}`, `${schedule.name}` and `${schedule.fired_at}` are filled in at each tick. |
| `template.sink`, `sink_version` | The sink template a source template runs with. |
| `template.overlay`, `overlay_version` | A deployment overlay. |
| `tenants` | Operators only: `all` or a list of tenant ids. Each tick runs once per tenant, admitted against that tenant's limits. |
| `run.name` | The run name; `{name}` and `{tick}` are replaced. |
| `run.labels`, `timeout_secs`, `selection`, `pool` | As on any run. |

Secret params (`secret: true` in the template) are sealed with the server's
vault key (`--vault-key`) and never shown again: responses carry
`{"$sealed": true}` in their place, and sending that marker back keeps the
stored value. A server without a vault key refuses a schedule with a secret
param.

## Ticks, runs and failures

Each tick submits a run through the same path as any template run: approval
policy, budgets, tenant admission, audit. Runs carry the labels
`schedule=<name>` and `schedule_tick=<tick>`, so `GET /v1/runs?label=schedule:<name>`
lists them. A tick's idempotency key is `sched:<name>:<tick>` (with
`:<tenant>` for a fan-out), so replicas sharing the history start exactly one
run per tick.

A tick that is refused (an unknown template, a bad param, a tenant at a limit)
is recorded as **failed** with its reason and shown on the schedule; the next
tick fires as usual. Schedules are never paused automatically. A tick that
cannot start for a passing reason (the run queue is full, the history is
unreachable) is retried with backoff until it starts.

**Run now** starts one tick immediately, exactly as a scheduled one (without
the overlap rule). It leaves the next scheduled tick unchanged. Under
`--require-approval run` it becomes a change request.

**Pause** stops ticks until resumed; ticks skipped while paused are not run
later, and the count is logged in the schedule's history.

## Tenants

A tenant's own principal (a `tenant:` entry in `--auth-config`) may create
schedules for its tenant only:

- never a fan-out (`tenants:` is refused with `403`);
- at most `max_schedules` of them (a [tenant limit](./embedded-integrations.md);
  paused schedules count), else `422` naming the limit;
- ticking no more often than `--tenant-schedule-min-interval-secs` (default
  300; `0` turns it off), else `422` naming the interval.

Another tenant's schedule is a `404`. A tenant principal also sees the
operator's fan-out schedules that include its tenant, read-only, with only its
own runs.

## Approvals

`faucet serve --require-approval schedule` turns every create, edit and delete
into a [change request](./approvals.md) (`202` with `change_id`); the change
applies once approved. Pause and resume are not gated. New secret param values
cannot travel in a change request: create the schedule without them, then set
them once it exists.

## API

| Route | Permission |
|---|---|
| `GET /v1/schedules[?tenant=]` | `ScheduleRead` (viewer+) |
| `GET /v1/schedules/{name}[?tenant=]` | `ScheduleRead` |
| `POST /v1/schedules/preview` | `ScheduleRead` |
| `POST /v1/schedules`, `PUT`/`DELETE /v1/schedules/{name}` | `ScheduleWrite` (operator+) |
| `POST /v1/schedules/{name}/pause`, `/resume`, `/run` | `ScheduleWrite` |

Every call is audited as `schedule.<action>`. See the
[HTTP API reference](../reference/http-api.md#v1schedules-schedules).

## Metrics

| Metric | Meaning |
|---|---|
| `faucet_serve_schedules_active` | Schedules this server ticks. |
| `faucet_serve_schedule_ticks_total{schedule,outcome}` | Ticks by outcome: `started`, `skipped`, `failed`, `retried`. |
| `faucet_serve_schedule_missed_ticks_total{schedule}` | Ticks that never ran (no server up, or collapsed into one catch-up). |
| `faucet_serve_schedule_tenant_failures_total{schedule,tenant}` | Fan-out ticks refused for one tenant while the others ran. |
| `faucet_serve_schedule_healthy{schedule}` | `0` while a tick is retrying. |
| `faucet_serve_schedule_last_tick_unix_seconds{schedule}` | The last tick that started. |

The shipped alerts (`observability/prometheus/alerts.yml`) cover missed ticks,
repeated failures and a tick stuck retrying.

## Moving from `type: schedule` triggers

Earlier releases ran cron schedules as a trigger type. A triggers file with a
`type: schedule` entry now fails to load, naming the entry. Move each one to a
schedules file (or create it in the console):

```yaml
# before (--triggers)            # after (--schedules)
triggers:                         schedules:
  - name: nightly                   - name: nightly
    type: schedule                    cron: "0 2 * * *"
    cron: "0 2 * * *"                 template: { id: crm }
    template: { id: crm }             tenants: all
    tenants: all
```

Runs are labelled `schedule` / `schedule_tick` instead of
`faucet.trigger.name` / `faucet.trigger.tick`, and the tick record moves from
`trigger_tick:<name>` to `schedule_tick:<name>`, so the first start after the
move begins from the next tick.
