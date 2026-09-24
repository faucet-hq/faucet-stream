# Notifications (Slack / PagerDuty / webhook)

The top-level `notifications:` block fans pipeline **lifecycle** and **health**
events out to Slack, PagerDuty, or a generic signed webhook — so a failure,
SLA breach, or tripped circuit breaker reaches your team without you having to
stand up Prometheus + Alertmanager first.

It is fully opt-in and requires the `notify` build feature
(`cargo install faucet-cli --features notify`, or `--features full`). With no
block, nothing changes.

> **Delivery never fails a run.** Each event is delivered with a short bounded
> retry; a channel outage is logged, counted
> (`faucet_notifications_dropped_total`), and swallowed — the pipeline is never
> blocked or failed by a notification. This is the same log-and-continue
> contract as lineage and SLA monitoring.

## Events

| Event | Fires when | Severity |
|-------|-----------|----------|
| `run_failure` | a run (or its final flush) failed | error |
| `run_success` | a run completed successfully | info |
| `sla_breach` | a post-run SLA check was violated (staleness / min_rows / volume) | warning |
| `circuit_open` | the resilience circuit breaker tripped | critical |
| `contract_abort` | a data-contract breach aborted the run (`on_breach: fail`) | error |
| `dlq_threshold` | a run routed rows to the DLQ at/over the rule's threshold | warning |
| `scheduler_stuck` | `faucet schedule` is exiting on consecutive failures | critical |

Events fire from every runtime — `faucet run`, `faucet schedule`,
`faucet serve`, and `faucet mirror` — because the emit sites live in the
shared executor (plus the scheduler's `scheduler_stuck` signal). They are
scoped to real, whole-pipeline **root** runs: `--dry-run`, `--limit`, sharded,
and cancelled runs do not notify.

## A rule

Each entry in the list is one rule: which events (`on:`), an optional severity
floor, an optional coalesce window, and one delivery `channel:`. The channel
uses the project-wide adjacently-tagged `{ type, config }` shape — the same
shape as connector `auth:`.

```yaml
notifications:
  - name: oncall-pagerduty
    on: [run_failure, circuit_open, contract_abort, scheduler_stuck]
    channel:
      type: pagerduty
      config:
        routing_key: "${env:PAGERDUTY_ROUTING_KEY}"

  - name: slack-alerts
    on: [run_failure, sla_breach, dlq_threshold]
    dedupe_window_secs: 300      # coalesce repeats within 5 minutes
    channel:
      type: slack
      config:
        webhook_url: "${env:SLACK_WEBHOOK_URL}"
        channel: "#data-alerts"

  - name: internal-webhook
    min_severity: warning        # info | warning | error | critical
    # empty `on:` = every event kind
    channel:
      type: webhook
      config:
        url: "https://ops.internal.example.com/hooks/faucet"
        hmac_secret: "${env:FAUCET_WEBHOOK_SECRET}"
```

### Fields

| Field | Meaning |
|-------|---------|
| `name` | Unique rule name (metric label, dedupe key, logs). |
| `on` | Event kinds to fire on. **Empty = all kinds.** |
| `min_severity` | Only deliver events at/above this severity. Default `info`. |
| `dedupe_window_secs` | Leading-edge coalesce: drop an identical event (same rule + pipeline + row) within this window. Absent / `0` = no coalescing. |
| `dlq_threshold` | For `dlq_threshold` only: minimum DLQ rows before firing. Default `1`. |
| `channel` | The delivery channel — `{ type, config }`. |

## Channels

### Slack

```yaml
channel:
  type: slack
  config:
    webhook_url: "${env:SLACK_WEBHOOK_URL}"   # incoming-webhook URL
    channel: "#alerts"                        # optional override
    username: "faucet"                        # optional override
```

### PagerDuty

Uses the Events API v2. A failure-class event **opens** an incident; the next
`run_success` on the same pipeline/row automatically sends a matching
**resolve** (correlated by dedup key), so incidents self-close.

```yaml
channel:
  type: pagerduty
  config:
    routing_key: "${env:PAGERDUTY_ROUTING_KEY}"
    source: "orders-pipeline"     # optional; defaults to the pipeline name
```

### Generic webhook

Posts a stable JSON envelope. If `hmac_secret` is set, the body is signed with
HMAC-SHA256 and the lowercase-hex digest is sent in `signature_header`
(default `X-Faucet-Signature`) so the receiver can verify authenticity.

```yaml
channel:
  type: webhook
  config:
    url: "https://ops.example.com/hooks/faucet"
    method: POST                              # default POST
    headers: { X-Env: prod }                  # optional extra headers
    hmac_secret: "${env:FAUCET_WEBHOOK_SECRET}"
    signature_header: "X-Faucet-Signature"    # default
    extra_fields:                             # optional static body fields
      tenant: "${vars.tenant}"
      environment: prod
```

#### Payload

```json
{
  "event": "run_success",
  "severity": "info",
  "pipeline": "contacts-sync",
  "row": "associations",
  "run_id": "0199f3c1-8a2e-7c40-9f1b-2d7e5a6c3b04",
  "invocation_id": "0199f3c1-8a31-7d02-b8aa-91c4e0f7d215",
  "started_at": "2026-08-13T09:14:02.117Z",
  "finished_at": "2026-08-13T09:16:41.402Z",
  "duration_secs": 159.285,
  "title": "Pipeline `contacts-sync` succeeded",
  "message": "Run completed, 14203 records written.",
  "details": { "records_written": 14203 }
}
```

| Field | Notes |
|---|---|
| `event` | One of the event kinds listed above. |
| `severity` | `info` / `warning` / `error` / `critical`. |
| `pipeline`, `row` | Config identity. **Not unique per run** — see `run_id`. |
| `run_id` | Correlates to the submitted run. Under `faucet serve` this is exactly the id returned by `POST /v1/runs`, so a completion callback can be matched to the submission. |
| `invocation_id` | This matrix row's own id. One submitted run emits one notification per row, all sharing `run_id` and differing here. |
| `started_at`, `finished_at` | RFC 3339 UTC. |
| `duration_secs` | Monotonic elapsed seconds — never negative, even across a clock step. |
| `details` | Per-event structured context (e.g. `records_written`, `error_kind`, `records_dlq`). |

Every key is always present; identity and timing fields are `null` for events
with no owning invocation (e.g. `scheduler_stuck`, emitted by the scheduler loop
itself). Receivers can therefore rely on a stable key set.

`extra_fields` merges static values into the top level of that body — useful for
tagging a callback with a tenant or an external job id. Values go through the
normal interpolation pass. A key that collides with any field above is
**rejected by `faucet validate`** rather than silently dropped, so a typo can
never spoof the `event` a receiver keys off.

> Using this as a job-status callback? `run_id` is the correlation key. Two
> overlapping runs of one pipeline — a `schedule` with `overlap: queue`, a
> cluster with several workers, or a backfill fanning out per window — produce
> notifications identical in `pipeline` and `row`, so keying off those will
> mis-attribute status.

## Secrets

Supply channel credentials via `${env:...}` / `${file:...}` / `${secret:...}`,
which are resolved over the raw config at load time and **registered for log
redaction** — never inline a webhook URL or routing key. (These universal
directives work anywhere in the config; cloud secrets-manager schemes like
`${vault:...}` are resolved for the connector-config surfaces documented under
[Secrets-manager interpolation](./secrets.md).)

## Testing your setup

Fire a synthetic event through a config's rules — no pipeline runs, real
delivery — to confirm a channel is wired correctly:

```bash
faucet notify test pipeline.yaml --event run_failure
```

`--event` accepts any event kind (`run_failure`, `run_success`, `sla_breach`,
`circuit_open`, `contract_abort`, `dlq_threshold`, `scheduler_stuck`).

## Metrics

| Metric | Labels | Meaning |
|--------|--------|---------|
| `faucet_notifications_sent_total` | `channel`, `event`, `outcome` | Deliveries attempted (`outcome` = `ok`/`error`). |
| `faucet_notifications_dropped_total` | `channel`, `reason` | Not delivered (`reason` = `coalesced`/`channel_error`). |
| `faucet_notification_dispatch_duration_seconds` | `channel` | Per-delivery latency. |

## Relationship to Prometheus alerting

This block is a **self-contained** notifier — it needs no external monitoring
stack. It is complementary to shipping Prometheus alert rules against faucet's
metrics: use notifications for immediate, per-run incident routing, and
Prometheus/Alertmanager for threshold- and duration-based alerting across your
fleet.
