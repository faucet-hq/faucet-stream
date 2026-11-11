# Event-driven triggers

`faucet serve --triggers <file>` turns `faucet serve` into an event-driven
pipeline orchestrator: long-lived watcher tasks listen for external events and
automatically enqueue runs, reusing the full queue/idempotency/history
machinery as `POST /v1/runs`.

This cookbook walks through three trigger types with worked examples. See the
[Triggers reference](../reference/triggers.md) for the complete field reference,
`${trigger.*}` token table, idempotency-key shapes, and metrics.

All examples use the file at
[`cli/examples/triggers/triggers.yaml`](https://github.com/faucet-hq/faucet-stream/blob/main/cli/examples/triggers/triggers.yaml).

## Walkthrough 1 — S3 object arrival → load pipeline

**Use-case:** a file lands in `s3://my-bucket/incoming/`. You want to load it
into Postgres using the key as a runtime parameter.

### triggers.yaml

```yaml
version: 1
triggers:
  - name: load-dropped-files
    type: object_arrival
    config: ./pipelines/s3_load.yaml   # or an inline pipeline doc
    store:
      type: s3
      bucket: my-bucket
      prefix: incoming/
      region: us-east-1
    poll_interval_secs: 30
    mode: per_object    # one run per new object (use `batch` for one run for all)
    start_at: now       # ignore objects already in the prefix at startup
    run:
      name: "load:{name}:{object_key}"
```

### Pipeline template (`pipelines/s3_load.yaml`)

The trigger injects `${trigger.object_key}` and `${trigger.bucket}` into the
config at fire time. Use them as you would any `${…}` token (the file only
validates through its trigger, which binds them):

<!-- faucet:no-validate -->
```yaml
version: 1
name: s3-load
pipeline:
  source:
    type: s3
    config:
      bucket: "${trigger.bucket}"
      prefix: "${trigger.object_key}"   # exact key → single-object read
      region: us-east-1
      file_format: json_lines
  sink:
    type: postgres
    config:
      connection_url: "${env:PG_URL}"
      table_name: events_raw
      column_mapping: { jsonb: { column: payload } }
```

### Start the server

```bash
FAUCET_SERVE_AUTH_TOKEN=s3cret \
cargo run -p faucet-cli --features "triggers,triggers-object-store" -- \
  serve --listen 0.0.0.0:8080 \
  --triggers ./triggers.yaml
```

Drop a file into the bucket (or use `aws s3 cp`) — within
`poll_interval_secs` the watcher detects it, creates a deterministic
idempotency key (`trig:load-dropped-files:<bucket>:<key>:<last_modified>`),
and enqueues a run. Re-listing the same object version never enqueues a
duplicate.

Check the run:

```bash
curl -s -H "Authorization: Bearer s3cret" \
     http://127.0.0.1:8080/v1/runs | jq '.runs[0]'
```

## Walkthrough 2 — Webhook → sync pipeline

**Use-case:** a CI system, a SaaS webhook, or a CI workflow calls your server
to trigger a data sync. You want idempotent delivery and to pass request
metadata into the pipeline.

### triggers.yaml

```yaml
version: 1
triggers:
  - name: sync-hook
    type: webhook
    config: ./pipelines/sync.yaml   # path relative to this triggers file
    methods: [POST]
    dedupe_header: Idempotency-Key
```

The `dedupe_header` field is optional but strongly recommended for external
callers. When set, the named header's value becomes the idempotency key
(as `trig:<trigger-name>:<value>`, so two triggers receiving one event id both run) —
if the caller retries with the same key, they get back the original run_id
rather than a new run.

> **Security note:** the dedupe key is trusted verbatim. Only use
> `dedupe_header` when callers are trusted or the header is verified
> upstream (e.g. HMAC-signed by the sender).

Values the request supplies (`${trigger.body}`, `${trigger.header.*}`,
`${trigger.query.*}`) are checked where they land in `sync.yaml`, under the
same rules as a caller's submitted config: a header placed in a Kafka
`brokers` list or a connection string must name a host the server allows
(`--allow-url-host`), even when the rest of that value is your own text. A
refused request gets `422` and an audit entry (`policy.denied`, target
`trigger values`). See
[Triggers → substitution](../reference/triggers.md) for the full rules.

### Fire the webhook

```bash
# No idempotency key — a fresh run is created each time
curl -XPOST http://127.0.0.1:8080/v1/triggers/sync-hook \
     -H "Authorization: Bearer s3cret" \
     -H "Content-Type: application/json" \
     -d '{}'

# With an idempotency key — idempotent delivery
curl -XPOST http://127.0.0.1:8080/v1/triggers/sync-hook \
     -H "Authorization: Bearer s3cret" \
     -H "Idempotency-Key: run-20260612-001" \
     -H "Content-Type: application/json" \
     -d '{"dataset":"orders"}'
```

The endpoint takes JSON bodies (`application/json` and the `+json` types such
as `application/cloudevents+json`) by default; any other `Content-Type` gets
`415`. A sender that posts form data, or a body with no `Content-Type`, needs
its type listed on the trigger:

```yaml
  - name: form-hook
    type: webhook
    config: ./pipelines/load.yaml
    content_types: [application/json, application/x-www-form-urlencoded]
```

The server returns `202 Accepted` with a `{run_id, status}` body. A second
call with the same `Idempotency-Key` returns the same `run_id`.

### Use request data in the pipeline

`${trigger.body}`, `${trigger.header.<name>}`, and `${trigger.query.<name>}` are
available in the pipeline config:

```yaml
# pipeline that uses the request body as a REST source filter
pipeline:
  source:
    type: rest
    config:
      url: "https://api.example.com/orders?dataset=${trigger.query.dataset}"
      auth: { type: bearer, config: { token: "${env:API_TOKEN}" } }
  sink:
    type: file
    config:
      path: "./out/${trigger.fired_at}.jsonl"
```

### Disabling a trigger without restarting

Set `enabled: false` in the triggers file and restart `faucet serve`. The
trigger is listed in `/readyz` as healthy but its watcher is not spawned, so
the webhook path returns 404.

## Walkthrough 3 — Redis queue depth → drain pipeline

**Use-case:** a Redis list accumulates tasks pushed by another process. When
it crosses a threshold, you want to drain it with a pipeline.

### triggers.yaml

```yaml
version: 1
triggers:
  - name: drain-jobs
    type: queue_depth
    config: ./pipelines/drain.yaml   # path relative to this triggers file
    queue:
      type: redis
      url: redis://localhost:6379
      key: jobs
      kind: list
    threshold: 1        # fire when list length >= 1
    poll_interval_secs: 15
```

The watcher is **edge-triggered**: it fires once when `LLEN jobs` first
crosses 1. It will not fire again until the depth falls back below the
threshold and rises again. This prevents repeated fires while the drain
pipeline is still running.

The injected token `${trigger.depth}` contains the observed length, and
`${trigger.queue}` contains the key name.

### Start the server

```bash
FAUCET_SERVE_AUTH_TOKEN=s3cret \
cargo run -p faucet-cli --features "triggers,triggers-redis" -- \
  serve --triggers ./triggers.yaml
```

Push a job:

```bash
redis-cli RPUSH jobs '{"id":"1","task":"import"}'
```

Within `poll_interval_secs` the watcher fires, the pipeline drains the list
into SQLite, and `/v1/runs` shows the completed run.

## Running on a cron

Triggers react to events. To run a template on a cron — nightly, hourly, on
the first of the month — use a [schedule](./schedules.md) instead: create it
in the console's **Schedules** page, through `POST /v1/schedules`, or in a
file passed to `faucet serve --schedules`. A `type: schedule` entry in a
triggers file is refused with a pointer there.

## Monitoring

Every trigger emits Prometheus metrics. To watch trigger health:

```bash
# Live metric scrape (or point Prometheus at /metrics)
curl -s http://127.0.0.1:8080/metrics | grep faucet_serve_trigger
```

Key signals:

| What | Metric |
|------|--------|
| Fire rate | `faucet_serve_triggers_fired_total{trigger,type}` |
| Watcher health | `faucet_serve_trigger_healthy{trigger}` (0 = in backoff) |
| Coalesced fires | `faucet_serve_trigger_runs_coalesced_total{trigger}` (webhook debounce / idempotency-conflict no-op) |
| Dropped fires | `faucet_serve_trigger_runs_dropped_total{trigger,reason}` (run queue full, `reason="queue_full"`) |
| Last fire time | `faucet_serve_trigger_last_fire_unix_seconds{trigger}` |

Set up an alert on `faucet_serve_trigger_healthy == 0` or on
`time() - faucet_serve_trigger_last_fire_unix_seconds > <expected_interval * 3>`
to detect a stalled watcher.

See the [reference page](../reference/triggers.md) for the complete metric list
and the [observability guide](../operations/observability.md) for the full
Prometheus/Grafana setup.
