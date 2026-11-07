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
    cron: "0 2 * * *"            # 5 fields; ticks at most every 5 minutes
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
| `cron`, `timezone` | When it ticks: a five-field cron (or an alias such as `@hourly`). A schedule ticks at most every 5 minutes. The console previews the next ticks in the timezone and in UTC. |
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
stored value. A value is sealed to the schedule's stable id, its tenant and
its template, so it opens for that schedule only: an edit that changes the
template is refused with `422` naming the secret params to supply again. A
server without a vault key refuses a schedule with a secret param.

### At most every 5 minutes

No schedule may tick more often than every 5 minutes, whoever made it: the
operator, a tenant, or the `--schedules` file. A cron that would (`*/2 * * * *`)
is refused with `422` naming the rule, a schedules file with one refuses to
start, and a six-field cron (one with a seconds field) is refused outright.
For anything more frequent, use a [trigger](./triggers.md) or a streaming
source.

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
cannot start for a passing reason (the run queue is full, the server is
draining) is retried with backoff until it starts.

While the run history is unreachable the server admits nothing, so a due
tick is **held**: nothing is recorded (the status lives in the history) and
the health gauge is left alone — the outage shows through
`faucet_serve_history_degraded`. Once the history answers, a held tick runs;
if later ticks fell due meanwhile, `catch_up: true` runs the latest once and
counts the rest as missed, and `catch_up: false` drops the stale tick and
waits for the next one.

One schedule's step (resolving the tick, firing every target) is cut off after
five minutes and its tick retried
(`faucet_serve_schedule_step_timeouts_total`); its runs dedupe on the tick's
idempotency key. Due schedules step side by side and take turns, so a slow one
never holds up the others.

**Run now** starts one tick immediately, exactly as a scheduled one (without
the overlap rule). It leaves the next scheduled tick unchanged. Under
`--require-approval run` it is refused (see [Trust and approvals](#trust-and-approvals)).

**Pause** stops ticks until resumed; ticks skipped while paused are not run
later, and the count is logged in the schedule's history.

## Tenants

A tenant's own principal (a `tenant:` entry in `--auth-config`) may create
schedules for its tenant only:

- never a fan-out (`tenants:` is refused with `403`);
- at most `max_schedules` of them (a [tenant limit](./embedded-integrations.md);
  paused schedules count), else `422` naming the limit;
- ticking no more often than `--tenant-schedule-min-interval-secs` (default
  and minimum 300: the flag can only raise the floor), else `422` naming the
  interval.

Another tenant's schedule is a `404`. A tenant principal also sees the
operator's fan-out schedules that include its tenant, read-only: only its own
runs, its own result of each tick (outcome, error and streak of failed ticks;
`unknown` when the latest tick did not include it), its own tick errors and
skip reasons, and none of the operator's params or change history.

## Trust and approvals

A schedule's params are its creator's. A schedule made in the console or
over the API is held to the same rules as the creator triggering the template
by hand: its params meet the server's caller
[reference policy](../reference/http-api.md#reference-policy) (no server
files, no internal URLs unless the operator allowed them) when it is created
or edited, and again at every tick. The check at create and edit resolves
nothing (`${env:}`, `${file:}` and secret-manager references stay
unresolved, a sealed value is a placeholder) and records nothing; a refused
param answers `403`. Only schedules in the `--schedules` file run as the
operator's own, the way a triggers file does.

`faucet serve --require-approval schedule` turns every create, edit and delete
into a [change request](./approvals.md) (`202` with `change_id`); the change
applies once approved. Secret param values travel sealed to the schedule the
change names (the server picks its id when the change is proposed); every
view of the change request shows them as `{"$sealed": true}`. A change whose
schedule was deleted and made again since it was proposed is invalidated.

`--require-approval run` gates runs, and a schedule starts runs on its own,
so under it:

- creating or editing a schedule is a change request too (deleting is not),
  and it must pin numeric versions for its template, sink and overlay: a
  channel (`stable`, `prod`) answers `422` "pin a version while runs need
  approval", so an approved schedule runs exactly what was approved;
- a schedule change needs approvals that meet the `schedule` and the `run`
  rule each on its own, and the server refuses to start when no principal in
  `--auth-config` could meet one of them (with only `schedule` gated, just
  the `schedule` rule);
- a schedule's ticks run with standing approval only when the schedule was
  stored through an approved change, or comes from the `--schedules` file;
  any other schedule — and one that still follows a channel when the gate is
  turned on — is not ticked: its status reads `needs_approval` with the reason;
- resuming a schedule that was not approved is refused with `409`: propose an
  update to it, and resume it once approved;
- **Run now** is refused with `409` (start the template through a change
  request instead).

Pausing is never gated.

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
| `faucet_serve_schedule_ticks_total{schedule,outcome}` | Ticks by outcome: `started`, `skipped`, `failed`, `retried`, `paused` (held while the run history is unreachable), `needs_approval`. |
| `faucet_serve_schedule_step_timeouts_total{schedule}` | Steps cut off after five minutes; the tick is retried. |
| `faucet_serve_schedule_missed_ticks_total{schedule}` | Ticks that never ran (no server up, or collapsed into one catch-up). |
| `faucet_serve_schedule_tenant_failures_total{schedule,tenant}` | Fan-out ticks refused for one tenant while the others ran. |
| `faucet_serve_schedule_healthy{schedule}` | `0` while a tick is retrying or the schedule needs approval; left alone while the run history is unreachable. |
| `faucet_serve_schedule_last_tick_unix_seconds{schedule}` | The last tick that started. |

The shipped alerts (`observability/prometheus/alerts.yml`) cover missed ticks,
repeated failures, a tick stuck retrying and step timeouts. The unhealthy
alert is held back on a replica whose run history is unreachable.

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
