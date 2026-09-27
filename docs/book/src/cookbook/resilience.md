# Resilience: retry, circuit breaker, and poison-pill

The top-level `resilience:` block gives a pipeline one declarative place to say
how it should behave under transient and persistent failure. It is **fully
opt-in**: with no `resilience:` block a pipeline behaves exactly as before — no
sink-write retry, and source connectors keep their built-in retry defaults.

```yaml
resilience:
  retry:
    max_attempts: 5            # total tries including the first (1 = no retry)
    backoff: exponential       # none | fixed | exponential
    base_ms: 200
    max_ms: 30000              # per-sleep cap, before jitter
    jitter: true
  retry_on: [http_5xx, rate_limited, connection, timeout]
  circuit_breaker:
    consecutive_failures: 5
    cooldown_secs: 60
  poison:
    max_row_attempts: 3
    action: dlq                # dlq | drop | fail
```

A runnable example lives at `cli/examples/rest_to_jsonl_resilient.yaml`.

## What the policy wraps

The policy is applied at two layers:

- **Sink side (the pipeline loop):** `flush`, state-store `put`, and the
  effectively-once `write_batch_idempotent` path are wrapped with retry + the circuit
  breaker. A plain `write_batch` / `write_batch_partial` is retried **only when the
  sink supports idempotent writes** (the effectively-once protocol) — see the caveat
  below.

> **Plain `write_batch` retry is gated on sink idempotency.** A non-idempotent
> sink's `write_batch` is **not** pipeline-retried: a write that failed because
> the *response* was lost (the rows actually landed) would, on retry, duplicate
> every row. Only sinks that support idempotent writes (`postgres`, `mysql`,
> `mssql`, `sqlite`, `iceberg`, `bigquery`, `kafka`) have their batch writes
> retried by the policy. The effectively-once `write_batch_idempotent` path is always
> retried (the commit token makes a replay safe), as are `flush` and `state_put`
> for every sink. A transient failure on a non-idempotent sink still surfaces —
> handle it with effectively-once delivery, an upsert write mode, or downstream
> deduplication.
- **Source side (the connector):** the `retry` policy is injected into the
  connectors that retry their own requests (`rest`, `xml`, `graphql`), replacing
  their ad-hoc retry settings with one shared configuration.

> The pipeline cannot retry a *source page-poll* itself — once a streaming
> source yields an error mid-stream, the page cannot be replayed by re-polling.
> Source-side retry therefore lives inside the connector, governed by the same
> `retry` policy.

## `retry`

| Field | Default | Meaning |
|-------|---------|---------|
| `max_attempts` | `5` | Total attempts including the first. `1` disables retry. |
| `backoff` | `exponential` | `none` (no delay), `fixed` (constant `base_ms`), or `exponential` (`base_ms * 2^attempt`). |
| `base_ms` | `200` | Base delay. |
| `max_ms` | `30000` | Per-sleep cap (before jitter). |
| `jitter` | `true` | Apply `[0.5, 1.5)` decorrelated jitter to each sleep. |

### `retry_on`

The set of transient error classes that are retried. Anything not in the set
(and anything that doesn't classify as transient — auth errors, config errors,
JSON parse errors, 4xx other than 429) fails fast and is **never** retried.

| Class | Matches |
|-------|---------|
| `http_5xx` | HTTP 5xx server errors |
| `rate_limited` | HTTP 429 / rate-limit signals |
| `connection` | connection-level failures (DNS, refused, reset) |
| `timeout` | request timeouts |

Default (omit `retry_on`) = all four. An empty list is rejected at config load.

## `circuit_breaker`

Counts **consecutive fully-failed pages** (a page whose write ultimately failed
after retries). A page with any success resets the counter. When the count
reaches `consecutive_failures`, the run **fails fast** with a `CircuitOpen`
error rather than continuing.

This only changes behavior on the DLQ / poison path — without a DLQ the first
exhausted-retry write already aborts the run. Its real job is to stop a wedged
destination from silently draining the entire source into the dead-letter queue.

`cooldown_secs` is **advisory for the orchestration layer**: when a
[`faucet schedule`](../reference/cli.md) run fails with `CircuitOpen`, the
scheduler waits at least `cooldown_secs` before the next tick. A one-shot
`faucet run` simply exits non-zero; `faucet serve` records the run as failed
(no automatic re-run).

> The cooldown only delays the scheduler's next cron-tick re-entry. An overlap
> run that is **already queued** (`overlap: queue`) starts immediately when the
> active run finishes — it is not delayed by the cooldown.

## `poison`

Per-row handling for the DLQ path. When `write_batch_partial` reports individual
row failures, the still-failing, retriable rows are re-submitted up to
`max_row_attempts` times before the terminal `action` is applied:

| `action` | Effect |
|----------|--------|
| `dlq` | Route the row to the DLQ (the default). **Requires a `dlq:` block** — validated at config load. |
| `drop` | Discard the row (counted; logged once per run). |
| `fail` | Propagate the row error and abort the run. |

## Composition

- **Effectively-once delivery** — retry wraps `write_batch_idempotent`; a retried
  idempotent write is safe because the commit token makes it idempotent.
- **Adaptive batch sizing** — retry wraps each adaptive chunk; the breaker
  counts page-level failures.
- **Cancellation** — a backoff sleep is abandoned immediately on a shutdown /
  timeout cancel, so the policy never wedges a graceful drain.

## REST precedence

The `rest` source predates this unified policy and has its own `max_retries` /
`retry_backoff` config fields. When you leave both at their defaults
(`max_retries: 3`, `retry_backoff: 1s`), the pipeline `resilience.retry` policy
governs the REST source. If you set either field explicitly, the per-connector
value wins — an explicit setting is never silently overridden by a pipeline-wide
default. (Because REST keeps its own 429/`Retry-After`-aware runner, only the
policy's `max_attempts` and `base` apply to REST; `retry_on`/`max`/`jitter` are
honored on the `xml`/`graphql` sources and on every sink-side write.)

## Metrics

| Metric | Type | Labels |
|--------|------|--------|
| `faucet_resilience_retries_total` | counter | `pipeline, row, op, class` |
| `faucet_resilience_retry_sleep_seconds` | histogram | `pipeline, row, op` |
| `faucet_resilience_giveup_total` | counter | `pipeline, row, op` |
| `faucet_resilience_circuit_state` | gauge (0/1) | `pipeline, row` |
| `faucet_resilience_circuit_opened_total` | counter | `pipeline, row` |
| `faucet_resilience_poison_rows_total` | counter | `pipeline, row, action` |

`op` is one of `sink_write`, `flush`, `state_put`. Source-side retries have
their own metrics (below).

## Source-side throttling

The `rest`, `graphql` and `xml` sources meter the rate limiting they hit, so a
pipeline that took three hours because it spent two of them sleeping on `429`s
is distinguishable from one that is slow for any other reason:

| Metric | Type | Labels | Meaning |
|--------|------|--------|---------|
| `faucet_source_throttled_total` | counter | `pipeline, row, connector` | Rate-limit responses received (`429`, a `RateLimited` error) — every one, retried or not. |
| `faucet_source_throttle_wait_seconds` | histogram | `pipeline, row, connector` | Time actually slept after each one. |
| `faucet_source_retries_total` | counter | `pipeline, row, connector, class` | Every source-side retry by class (`rate_limited`, `http_5xx`, `connection`, `timeout`) — the source-side mirror of `faucet_resilience_retries_total`. |

The wait is **measured**, not read from the header: `rest` sleeps the server's
`Retry-After` (seconds or an HTTP date), `graphql` and `xml` sleep the policy's
backoff, and either way the recorded figure is the time that passed. A sleep cut
short by cancellation, a timeout or a dropped run records the partial wait.

The totals also land on the run's [usage record](./usage.md) (`throttled`,
`throttle_wait_secs`, `source_retries`), so `faucet run` and `faucet usage` show
`throttled 312× · waited 41 min`. When the cumulative wait exceeds 10 % of the
run, the run logs one warning:

```text
WARN pipeline=orders row=default source spent 2460.0s of a 3600.0s run (68%) waiting on rate limits (312 throttled responses); lower concurrency, stagger schedules or raise the quota
```

The [`FaucetSourceThrottled`](./dashboards.md) alert fires when a row spends
more than a quarter of 15 minutes rate-limited.

Other sources with their own throttle handling — `databricks`, `dynamodb`,
`kinesis` — do not report through these metrics yet. A connector (including a
third-party one) opts in through the round-trip recorder the pipeline installs:
`RecorderSlot::throttled()` per rate-limit response, `RecorderSlot::retry(class)`
per retry, and `RecorderSlot::throttle_wait_timer()` (or
`faucet_core::observability::throttle_sleep`) around the sleep; a connector that
retries through `faucet_core::execute_with_policy_recorded` gets all three for
free.

## Inspecting the schema

```bash
faucet schema resilience
```
