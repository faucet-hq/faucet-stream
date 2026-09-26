# Event-driven triggers (`faucet serve`)

`faucet serve --triggers <file>` loads a static triggers file at startup and
spawns long-lived watcher tasks. When a watcher fires, it enqueues a run
through the same `runner::submit` pipeline as a normal `POST /v1/runs`,
inheriting the full queue/semaphore/idempotency/history machinery.

Requires the `triggers` Cargo feature (included in `full`):

```bash
cargo install faucet-cli --features triggers                   # framework + webhook only
cargo install faucet-cli --features "triggers,triggers-object-store"   # + S3/GCS
cargo install faucet-cli --features "triggers,triggers-redis"          # + Redis queue-depth
cargo install faucet-cli --features full                               # everything
```

## Triggers file grammar

```yaml
version: 1          # required; must be 1
triggers:
  - name: <string>          # unique; used in metrics, idempotency keys, webhook path
    enabled: true           # optional; default true — set false to disable without deleting
    config: <path|inline>   # pipeline config: a file path string OR an inline pipeline doc
    template:               # …or a registered template instead of `config` (exactly one)
      id: <template id>
      version: stable       # optional: a number or a channel
      params: {}            # optional; string values may use ${trigger.*}
      sink: <sink template> # optional: for a source template
      overlay: <overlay id> # optional deployment overlay
    tenants: all            # optional: `all` or [tenant ids] — one run per tenant
    run:                    # optional run-shaping
      name: <template>      # run name; supports {name}, {object_key}, {bucket}, etc.
      labels: {}            # static labels merged with the auto-derived trigger labels
      timeout_secs: null    # per-run timeout in seconds
    type: <trigger type>    # required; one of object_arrival, webhook, queue_depth, schedule
    # … type-specific fields below
```

The `config:` field accepts either a **path string** (resolved **relative to the
triggers file**, not the process CWD) or an **inline pipeline document**
(`{ pipeline: … }`). `template:` runs a registered
[template](../cookbook/templates.md) instead (needs the `templates` feature).
`tenants:` fans each fire out to one run per tenant, each run **as** that
tenant — its connections, `${tenant.*}` values, state namespace and limits
([embedded integrations](../cookbook/embedded-integrations.md); needs the
`tenants` feature). Each tenant's idempotency key is the trigger's key
suffixed `:<tenant>`; a tenant that is suspended, at its limit, or missing a
connection is skipped and logged.

The triggers file is **validated strictly at load time**: an unknown or
misspelled field on a trigger entry (e.g. `debounce_sec` for `debounce_secs`) or
inside its nested `store:` / `queue:` block fails fast with an error naming the
field and the trigger, rather than being silently dropped. Keys inside an inline
`config:` pipeline document are validated by the pipeline loader, not here.

## Trigger types

### `object_arrival`

Polls an object store (S3 or GCS) for new objects under a prefix.

Requires the `triggers-object-store` Cargo feature.

```yaml
type: object_arrival
store:
  type: s3                  # s3 | gcs
  bucket: my-bucket         # required
  prefix: incoming/         # key prefix to watch (optional; defaults to root)
  region: us-east-1         # S3 only (optional)
  endpoint: null            # S3 only — override endpoint URL for S3-compatible stores
poll_interval_secs: 30      # how often to list the prefix (default 30)
mode: per_object            # per_object (one run per new object) | batch (one run for all new objects)
start_at: now               # now (only objects seen after startup) | beginning (all objects, incl. existing)
```

**`${trigger.*}` tokens injected into the run config:**

*`mode: per_object` — one token set per object:*

| Token | Value |
|-------|-------|
| `${trigger.name}` | The trigger's `name` field |
| `${trigger.type}` | `object_arrival` |
| `${trigger.fired_at}` | ISO 8601 timestamp when the trigger fired |
| `${trigger.object_key}` | The S3/GCS object key |
| `${trigger.bucket}` | The S3/GCS bucket name |
| `${trigger.size}` | Object size in bytes |
| `${trigger.last_modified}` | RFC 3339 last-modified timestamp of the object |

*`mode: batch` — one token set for the entire batch of new objects:*

| Token | Value |
|-------|-------|
| `${trigger.name}` | The trigger's `name` field |
| `${trigger.type}` | `object_arrival` |
| `${trigger.fired_at}` | ISO 8601 timestamp when the trigger fired |
| `${trigger.bucket}` | The S3/GCS bucket name |
| `${trigger.object_count}` | Number of new objects in the batch |

> `${trigger.object_key}`, `${trigger.size}`, and `${trigger.last_modified}` are
> **not available** in `mode: batch` (they are per-object fields).

**Idempotency key:**
- `mode: per_object`: `trig:<name>:<bucket>:<object_key>:<last_modified>` — deterministic per
  object version; re-listing a processed object does not enqueue a duplicate run.
- `mode: batch`: `trig:<name>:<watermark>` where `<watermark>` is the maximum
  `last_modified` timestamp across the batch.

**`start_at: now` behaviour:** on first startup the watcher records the current set of keys as
its cursor; only keys seen in subsequent polls are treated as new. Set `start_at: beginning` to
fire for all objects currently in the prefix (use `mode: batch` to coalesce them into one run).

### `webhook`

Exposes `POST /v1/triggers/{name}` on the `faucet serve` listener. The
endpoint is bearer-authenticated (same token as `/v1/runs`). Returns 202 on
success, 404 for an unknown trigger name, and 400 when the HTTP method is not
in the configured `methods` list.

No additional Cargo features are required (the route is part of the base
`triggers` feature, which implies `serve`).

```yaml
type: webhook
methods: [POST]             # allowed HTTP methods (default [POST]); PUT also supported
dedupe_header: null         # header used as idempotency key (optional; else a per-request UUID)
debounce_secs: 0            # leading-edge debounce window in seconds (default 0 = off)
```

> **Leading-edge debounce:** when `debounce_secs > 0`, the first request is
> accepted and any further requests that arrive within `debounce_secs` of that
> accepted fire are coalesced — they return `200 { "status": "coalesced" }` and
> enqueue no run. The window re-arms once `debounce_secs` have fully elapsed
> since the last accepted fire. Debounce is webhook-only; polling triggers
> (`object_arrival`, `queue_depth`) pace themselves via `poll_interval_secs`.

> **`dedupe_header` trust boundary:** the caller-supplied header value is used
> verbatim as the run's idempotency key. A caller who controls this value can
> suppress a legitimate run by reusing a key from a prior run. Only set
> `dedupe_header` when callers are trusted or the header value is verified
> upstream (e.g. by a gateway signing scheme or HMAC validation).

> **Disallowed methods:** a request whose HTTP method is not in `methods` returns
> 400 in v1 (not 405). This is intentional: the route itself exists for all
> methods; the 400 carries a descriptive message.

**`${trigger.*}` tokens:**

| Token | Value |
|-------|-------|
| `${trigger.name}` | The trigger's `name` field |
| `${trigger.type}` | `webhook` |
| `${trigger.fired_at}` | ISO 8601 timestamp when the trigger fired |
| `${trigger.method}` | HTTP method of the request (`POST`, `PUT`, …) |
| `${trigger.body}` | Raw request body (string) |
| `${trigger.header.<name>}` | Value of HTTP request header `<name>` |
| `${trigger.query.<name>}` | Value of query parameter `<name>` |

**Idempotency key:** the raw value of the `dedupe_header` when configured and
present in the request (no prefix or name segment — the header value is used
verbatim); otherwise a fresh per-request UUID (also bare, no prefix).

Fire the webhook with `curl`:

```bash
curl -XPOST http://127.0.0.1:8080/v1/triggers/sync-hook \
     -H "Authorization: Bearer s3cret" \
     -H "Idempotency-Key: run-20260612-001" \
     -H "Content-Type: application/json" \
     -d '{}'
```

### `queue_depth`

Polls a Redis list/stream or a Kafka consumer group lag metric. When the
observed depth crosses `threshold`, the watcher fires once (edge-triggered).
It will not fire again until the depth drops below the threshold and rises back.

```yaml
type: queue_depth
queue:
  type: redis               # redis | kafka
  # Redis fields:
  url: redis://localhost:6379
  key: jobs                 # list key or stream name
  kind: list                # list | stream (default list)
  # Kafka fields:
  # brokers: localhost:9092
  # topic: events
  # group: my-consumer-group
threshold: 1                # fire when depth >= threshold (default 1)
poll_interval_secs: 30      # polling interval (default 30)
```

Redis requires the `triggers-redis` feature; Kafka requires `triggers-kafka`.

**`${trigger.*}` tokens:**

| Token | Value |
|-------|-------|
| `${trigger.name}` | The trigger's `name` field |
| `${trigger.type}` | `queue_depth` |
| `${trigger.fired_at}` | ISO 8601 timestamp when the trigger fired |
| `${trigger.queue}` | The queue key / topic name |
| `${trigger.depth}` | Observed depth (as a string) that crossed the threshold |

**Idempotency key:** `trig:<name>:edge:<monotonic_edge_ordinal>` — the
ordinal increments on each rising edge, producing a unique key per fire.

### `schedule`

Fires on a cron schedule, evaluated with the same compiler as
[`faucet schedule`](../cookbook/scheduling.md) (5 or 6 fields,
timezone- and DST-correct). Needs the `schedule` feature.

```yaml
type: schedule
cron: "0 2 * * *"           # 5 fields, or 6 with seconds first
timezone: Europe/Berlin     # IANA name; default UTC
```

A tick whose fire fails (queue full, a store error) is retried with the
watcher's backoff until it commits; a server that was down across several
ticks fires one catch-up run, not a backlog.

**`${trigger.*}` tokens:**

| Token | Value |
|-------|-------|
| `${trigger.name}` | The trigger's `name` field |
| `${trigger.type}` | `schedule` |
| `${trigger.fired_at}` | ISO 8601 timestamp when the trigger fired |
| `${trigger.tick}` | The scheduled instant (RFC 3339, UTC) |

**Idempotency key:** `trig:<name>:<tick>` (`…:<tick>:<tenant>` with
`tenants:`) — derived from the scheduled tick, so every cluster instance
computes the same key and one run goes through per tick.

## Labels on enqueued runs

Every trigger-fired run receives these automatic labels (visible in
`GET /v1/runs` responses and Prometheus metrics):

| Label | Value |
|-------|-------|
| `faucet.trigger.name` | Trigger name |
| `faucet.trigger.type` | Trigger type (`object_arrival`, `webhook`, `queue_depth`, `schedule`) |
| `faucet.trigger.tick` | The scheduled tick (`schedule` only) |

Additional labels can be added per trigger via `run.labels:`.

## `/readyz` — trigger health

`GET /readyz` includes a `triggers` array when `--triggers` is active:

```json
{
  "status": "ready",
  "history_ok": true,
  "queue_ok": true,
  "cluster": { "enabled": false, "instances": 0 },
  "triggers": [
    { "name": "load-dropped-files", "healthy": true },
    { "name": "sync-hook",          "healthy": true },
    { "name": "drain-jobs",         "healthy": true }
  ]
}
```

A degraded watcher (crashed and backing off) sets its `healthy` flag to `false`
but does **not** flip the top-level `status` to `not_ready` — the server keeps
accepting runs from the other trigger paths.

## Schema introspection

```bash
faucet schema triggers     # print the JSON Schema for the triggers file
```

## Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `faucet_serve_triggers_active` | Gauge | — | Number of enabled, running trigger watchers |
| `faucet_serve_trigger_healthy` | Gauge | `trigger` | 1 = healthy, 0 = in error backoff |
| `faucet_serve_trigger_last_fire_unix_seconds` | Gauge | `trigger` | Unix timestamp of last fire |
| `faucet_serve_triggers_fired_total` | Counter | `trigger`, `type` | Total trigger fires |
| `faucet_serve_trigger_runs_enqueued_total` | Counter | `trigger` | Runs successfully enqueued |
| `faucet_serve_trigger_runs_coalesced_total` | Counter | `trigger` | Fires coalesced — webhook debounce, or an idempotency-conflict no-op |
| `faucet_serve_trigger_runs_dropped_total` | Counter | `trigger`, `reason` | Fires dropped because the run queue was full (`reason="queue_full"`) |
| `faucet_serve_trigger_errors_total` | Counter | `trigger`, `type` | Watcher errors (poll failures, etc.) |

## Cluster note

When running a cluster (`--cluster` + shared `--history` DB), every instance
loads the same `--triggers` file and spawns independent watchers. Idempotency
keys are deterministic (derived from object key + last_modified, the
dedupe header value, or a rising-edge ordinal), so concurrent fires from
multiple instances resolve to a single run via the shared idempotency claim.
No additional coordination is required.

## Feature flags

| Feature | Contents |
|---------|----------|
| `triggers` | Framework, supervisor, webhook trigger (implies `serve`) |
| `triggers-object-store` | `object_arrival` watcher (S3/GCS listing) |
| `triggers-redis` | `queue_depth` watcher backed by Redis |
| `triggers-kafka` | `queue_depth` watcher backed by Kafka consumer-group lag |
| `schedule` (with `triggers`) | the `schedule` trigger type |
| `templates` (with `triggers`) | `template:` on a trigger |
| `tenants` (with `triggers`) | `tenants:` on a trigger |

All four are included in `full` and none are in `default`.

## See also

- [Event-driven triggers](../cookbook/triggers.md) — the task-oriented recipe for
  wiring up file-arrival, webhook, and queue-depth triggers end to end.
