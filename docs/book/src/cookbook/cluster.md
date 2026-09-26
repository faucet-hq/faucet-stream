# Running a cluster

`faucet serve --cluster` turns a fleet of identical `faucet serve` processes
into a **pull-balanced, self-healing cluster**. Each instance monitors a shared
SQL history database for `pending` runs, claims them exclusively, and executes
them locally. When a node crashes, a survivor reclaims its runs and re-executes
them up to a configurable attempt cap.

This is **Mode A** — a simple, coordinator-free design where any node can run
any submitted pipeline. Mode B (source-shard rebalancing, dedicated coordinator)
is a future follow-up ([#197](https://github.com/faucet-hq/faucet-stream/issues/197)).

> **Use clustered serve when:** you have more concurrent pipeline runs than one
> node can handle, or when you need resilience against single-node failure.
> Single-node deployments do **not** need `--cluster` — the default `faucet serve`
> already handles orphan recovery on restart via the existing lease mechanism.

## Requirements

### 1. Shared persistent SQL history backend

All cluster instances must point `--history` at the **same** database:

```bash
faucet serve --cluster --history 'postgres://user:pw@db/faucet' \
             --listen 0.0.0.0:8080

# Second instance (same DB, different port / host)
faucet serve --cluster --history 'postgres://user:pw@db/faucet' \
             --listen 0.0.0.0:8081
```

Cluster mode is rejected if `--history` is omitted (in-memory store) or
not a persistent SQL URL:

```
--cluster requires a persistent --history backend (postgres://… or sqlite:…); the in-memory store is single-process
```

Requires the matching SQL history feature:
```bash
cargo install faucet-cli --features "serve,serve-history-postgres"
# or
cargo install faucet-cli --features "serve,serve-history-sqlite"
```

### 2. Homogeneous deployment (shared env + secrets)

When a run is submitted, the config is stored **verbatim** (with `${env:…}`,
`${secret:…}`, `${vault:…}` directives unresolved) in the shared DB. The
instance that **claims** the run re-resolves those directives with its own
environment and credential chain at execution time.

**This means every cluster instance must have the same env vars, secrets-manager
access, and `--default-config` workspace defaults** — the same container image,
the same `.env` file, the same IAM role, etc. An instance that cannot resolve a
directive will fail the run with a config error rather than silently producing
wrong results.

## Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--cluster` | (disabled) | Enable cluster mode. Requires a persistent `--history` backend. |
| `--cluster-poll-secs` | `2` | How often (seconds) each instance polls for pending runs and propagates cross-instance cancels. Also the maximum cancel-propagation lag between instances. |
| `--cluster-max-attempts` | `3` | Maximum number of times a run will be attempted across all instances. After `max-attempts` failures (including crash-failovers) the run is marked `failed` (poisoned). |
| `--lease-ttl-secs` | `30` | Run-ownership lease TTL. An instance heartbeats its own in-flight runs at ~⅓ of this interval. A run whose owner's lease expires is eligible for reclaim. Tune this above your worst-case GC/IO stall — a longer TTL is safer but increases the time before a dead node's runs are requeued. |

## Run lifecycle in cluster mode

Cluster mode adds one state before execution: **`pending`**. A run lives in
`pending` in the shared DB until an instance claims it.

```
submit → pending → [claim] → running → completed
                           ↘ failed
                           ↘ cancelled
```

Step by step:

1. **Submit** (`POST /v1/runs`) — any instance validates and interpolates the
   config synchronously, writes the run as `pending` (raw config stored), and
   kicks the local claim loop. Returns immediately with `status: pending`.
2. **Claim** — the claim loop on each instance polls every `--cluster-poll-secs`
   seconds. It atomically claims up to `available_capacity` pending runs
   (Pending → Running, exclusive). Only one instance can claim a given run.
3. **Execute** — the claiming instance re-resolves `${env:…}` / `${secret:…}`
   directives with its own credentials, then runs the pipeline via the same
   executor as `faucet run`. The run record is heartbeated (lease renewed) while
   in flight.
4. **Complete / fail** — the run is marked `completed` or `failed`. The attempt
   count is incremented.
5. **Failover** — if the owner's lease expires (the instance crashed or stopped
   heartbeating), a survivor's next lease tick calls `reclaim_orphans`:
   - If `attempt_count < --cluster-max-attempts` → requeued back to `pending`.
   - If `attempt_count >= --cluster-max-attempts` → marked `failed` (poisoned).

## Cross-instance cancel

`POST /v1/runs/{id}/cancel` works correctly regardless of which instance receives
the request:

- **Pending run** (not yet claimed): the run is cancelled directly in the DB —
  no coordination needed.
- **Running on the same instance**: the local cancel token fires immediately;
  flush-completing cancel behaviour (page boundary + sink flush) applies as
  normal.
- **Running on a peer instance**: the cancel flag is written to the DB. The
  peer's claim loop picks it up on its next `pending_cancellations` poll (latency
  ≈ `--cluster-poll-secs`, default 2 s) and fires the local cancel token.

## Health check and metrics

### `/readyz` body

In cluster mode, `/readyz` returns a JSON body with the cluster section
populated:

```json
{
  "status": "ready",
  "history_ok": true,
  "queue_ok": true,
  "cluster": {
    "enabled": true,
    "instances": 3
  }
}
```

`instances` is the count of live cluster members (those whose membership
heartbeat has not yet expired). A single-instance deployment returns `1`;
a node that loses its DB connection may report stale counts.

### Prometheus metrics

| Metric | Type | Description |
|--------|------|-------------|
| `faucet_serve_cluster_enabled` | gauge | `1` if this instance started with `--cluster`, `0` otherwise. |
| `faucet_serve_cluster_instances` | gauge | Count of live cluster members (from last membership heartbeat). Alert on `faucet_serve_cluster_instances < expected_count`. |
| `faucet_serve_runs_claimed_total` | counter | Total runs claimed (transitioned from `pending` to `running`) by this instance. |
| `faucet_serve_runs_reclaimed_total{outcome="requeued"}` | counter | Orphaned runs requeued by this instance after a peer's lease expired. |
| `faucet_serve_runs_reclaimed_total{outcome="failed"}` | counter | Orphaned runs poisoned after reaching `--cluster-max-attempts`. |

Useful alert expressions:

- `faucet_serve_cluster_instances < N` — fewer nodes alive than expected.
- `increase(faucet_serve_runs_reclaimed_total{outcome="failed"}[5m]) > 0` — a
  run was poisoned; investigate the per-run error.
- `faucet_serve_history_degraded == 1` — history backend is down; cluster
  coordination is impaired (instances continue locally but cannot share runs).

## Delivery guarantees and double-run boundary

### What is guaranteed

- **Claim exclusivity:** at most one instance ever *starts* executing a given
  `pending` run. The atomic SQL claim (`UPDATE … WHERE status = 'pending' LIMIT
  N … RETURNING`) ensures two instances never both transition the same run to
  `running`.
- **Crash-failover is clean:** if an instance crashes after claiming a run but
  before writing output, a survivor re-queues the run and a fresh instance
  executes it from scratch. No partial results from the crashed run pollute the
  destination (assuming the pipeline had not yet flushed a page to the sink).

### What is NOT guaranteed without effectively-once delivery

An instance that was **paused** (e.g. a long GC pause, network partition, heavy
I/O stall) longer than `--lease-ttl-secs` may have its run stolen by a survivor
while the original instance is still alive. The original instance is
**owner-fenced** — it cannot update the run *record* after its lease expires —
but any sink *writes* already issued before the fencing cannot be recalled.

The survivor then re-runs the pipeline from the last persisted bookmark, which
may overlap with writes the paused instance already made. This means a run can
be **executed twice** (partial overlap) if the original instance was paused-not-crashed.

**To fully close this window,** pair the pipeline with
[effectively-once delivery](./state.md#effectively-once-delivery): a CDC source
(`postgres-cdc`, `mysql-cdc`, `mongodb-cdc`) plus an idempotent SQL or Iceberg
sink. The sink's atomic commit token deduplicates replayed pages regardless of
how many instances attempted them.

> **Clustered runs are at-least-once.** Both `--cluster` failover and Mode B
> shard reclaim can re-execute work, so an **append**-mode sink can end up with
> duplicate rows. When a run is submitted to a clustered or sharded server,
> faucet logs a warning recommending [`write_mode: upsert`](./upsert.md) (or
> [`delivery: exactly_once`](./state.md#effectively-once-delivery)) so any replay is
> idempotent. The run still proceeds — the warning is a reminder, not a gate.

**Practical sizing advice:** set `--lease-ttl-secs` comfortably above your
worst-case GC/IO stall. A 30-second default is appropriate for most JVM-free
workloads; bump to 60–120 s if you observe false-reclaim events in the metrics.

## Two-instance example

Terminal 1 (node A):

```bash
export FAUCET_SERVE_AUTH_TOKEN=s3cret
faucet serve \
  --cluster \
  --history 'postgres://faucet:pw@db:5432/faucet' \
  --listen 0.0.0.0:8080 \
  --max-concurrent-runs 8
```

Terminal 2 (node B — same DB, different port/host):

```bash
export FAUCET_SERVE_AUTH_TOKEN=s3cret
faucet serve \
  --cluster \
  --history 'postgres://faucet:pw@db:5432/faucet' \
  --listen 0.0.0.0:8081 \
  --max-concurrent-runs 8
```

Both instances now compete to claim submitted runs. Submit a run to either
endpoint — whichever instance has capacity first will pick it up:

```bash
curl -XPOST http://node-a:8080/v1/runs \
  -H "Authorization: Bearer s3cret" \
  -H 'content-type: application/json' \
  -d '{"config":"version: 1\npipeline:\n  source: {type: csv, config: {path: in.csv}}\n  sink: {type: jsonl, config: {path: out.jsonl}}\n","name":"my-pipeline"}'
```

Check cluster membership via `/readyz`:

```bash
curl http://node-a:8080/readyz | jq .cluster
# { "enabled": true, "instances": 2 }
```

## Kubernetes / Helm deployment

Deploy N replicas behind a Service; all replicas share the same `--history`
connection string and the same environment (ConfigMap + Secret). The Service
load-balances submissions across replicas; each replica independently claims
from the shared DB.

See the operator/Helm chart for faucet — TBD (#197).

## Distributing one big source across workers (Mode B)

Everything above is **Mode A**: whole *runs* pull-balance across instances, but a
single large source still runs entirely on one worker. **Mode B** splits *one
source* into shards that different workers process concurrently, so a single
logical pipeline over a huge table or object prefix scales horizontally.

### Enabling it

Add a top-level `shard:` block to the submitted config and run the cluster as
usual (Mode B requires `--cluster` + a SQL history backend — it builds on the
same lease/claim machinery):

```yaml
version: 1
name: big-table-mirror
shard:
  count: 8            # split the source into ~8 shards
pipeline:
  source:
    type: postgres
    config:
      connection_url: ${env:PG_URL}
      query: "SELECT * FROM events"
      shard: { key: id }   # integer column to range-partition on
  sink:
    type: postgres
    config:
      connection_url: ${env:WAREHOUSE_URL}
      table: events
      write_mode: upsert
      key: [id]
  state: { type: postgres, config: { connection_url: ${env:PG_URL} } }
```

When a sharded run is submitted, the instance that claims it acts as an
(ephemeral) **coordinator**: it enumerates the shards and inserts them into the
shared `faucet_serve_shards` table (idempotently — no leader election). Every
instance's claim loop then pulls shard rows up to its free capacity, narrows its
source to that shard, and runs it. The parent run is marked `sharded` and is
finalized to `completed`/`failed` once every shard is terminal.

### Shardable sources

| Source | Strategy | How to enable |
|--------|----------|---------------|
| `postgres` | primary-key range (`WHERE key >= lo AND key < hi`) | `source.config.shard: { key: <int column> }` |
| `mysql` | primary-key range | `source.config.shard: { key: <int column> }` |
| `mssql` | primary-key range | `source.config.shard: { key: <int column> }` |
| `sqlite` | primary-key range | `source.config.shard: { key: <int column> }` |
| `s3` | hash-of-object-key modulo N | automatic (no config) |
| `gcs` | hash-of-object-key modulo N | automatic (no config) |
| `parquet` | hash-of-file-path modulo N | automatic (no config) |
| `kafka` | native consumer-group membership | automatic (no config) |

> **NULL shard keys are not dropped.** Rows whose `shard` key column is `NULL`
> fall outside every range predicate, so the SQL sharders assign them to
> exactly one shard (alongside its range) — they are mirrored once, never
> silently lost.

PK-range notes: the shard `key` must be an integer-typed column present in the
query's output. On `mssql`, a sharded query must not end in a top-level
`ORDER BY` (T-SQL forbids it inside the derived table the shard predicate
wraps — and ordering across concurrent shards is meaningless anyway). Sharding
a `sqlite` source across workers requires every worker to reach the same
database file (e.g. a shared volume). On `mssql` with incremental replication,
shard bounds are computed over the not-yet-synced slice (the `@bookmark`
binding is honoured during enumeration).

A non-shardable source (or a matrix pipeline) ignores `shard:` and runs whole — Mode B is fully backward compatible.

### Kafka: native consumer-group sharding

Kafka already solves work distribution inside the broker, so the `kafka`
source does not enumerate data slices like the sharders above
([#261](https://github.com/faucet-hq/faucet-stream/issues/261)). Each shard
is a **membership slot**: `shard.count: N` makes N workers each run one more
consumer with the pipeline's `group_id`, and Kafka's consumer-group protocol
assigns the topic's partitions across them — killing a worker triggers a
broker-side rebalance onto the survivors immediately (well before the shard
lease even expires), and the reclaimed membership slot simply rejoins the
group on another worker.

```yaml
version: 1
name: orders-fanin
shard:
  count: 4            # four cooperating members of the consumer group
pipeline:
  source:
    type: kafka
    config:
      brokers: broker-1:9092,broker-2:9092
      topics: [orders]
      group_id: faucet-orders     # ALL members share this group
      idle_timeout: 60
  sink:
    type: postgres
    config:
      connection_url: ${env:WAREHOUSE_URL}
      table_name: orders
      column_mapping: auto_map
      write_mode: upsert
      key: [id]
  state: { type: postgres, config: { connection_url: ${env:STATE_URL} } }
```

How it differs from the other sharders:

- **The broker decides the split.** Which member consumes which partition is
  Kafka's choice, not faucet's; the member count is capped at the
  subscription's total partition count (an extra member would sit idle).
- **Offset continuity is Kafka-managed.** In member mode each consumer
  *commits offsets to the group* at durable page boundaries (after the sink
  confirmed the page and its bookmark persisted; plus a synchronous commit at
  stream end). A partition that migrates to another member — rebalance,
  worker death, shard reclaim — resumes from the last committed (= durable)
  position instead of `auto.offset.reset`. The per-shard state-store bookmark
  remains the safety net for the tiny durable-write→commit crash window: a
  member seeks to its bookmark only when it is *ahead* of the committed
  offset, never behind.
- **The boundary is at-least-once on membership change.** A crash between a
  durable page and its commit makes the partition's next owner re-read that
  page. Pair with `write_mode: upsert` or an idempotent destination, as with
  every clustered run.
- **Termination:** each member stops on its own `idle_timeout` /
  `max_messages` (`max_messages` is per member — N members consume up to N ×
  `max_messages` in total). `idle_timeout` is the natural terminator for
  shared consumption; a non-cluster Kafka run is completely unchanged.

### Per-shard resume and rebalancing

- **Per-shard bookmarks:** each shard has its own state key (`{run}::{shard}`),
  so a reassigned shard resumes where its dead owner left off, independent of its
  siblings.
- **Rebalancing:** a shard whose owning instance's lease expires is reclaimed by
  the lease loop — requeued to another worker, or poisoned (failed) past
  `--cluster-max-attempts`. New members pick up unassigned/reclaimed shards on
  their next claim tick.
- **Correctness boundary:** the same paused-not-crashed double-processing window
  as Mode A applies per shard. Pair with `write_mode: upsert` (or effectively-once
  delivery) so a reassigned shard's overlap is idempotent — as in the example
  above.

Mode B metrics: `faucet_serve_shards_claimed_total`,
`faucet_serve_shards_reclaimed_total{outcome}`.

## Rolling upgrades and stored state

Members share one state store, so during a rolling upgrade an old and a new
member can resume the same pipeline. Every member advertises the newest state
format it reads (the `faucet_serve_instance_caps` table, written with each
heartbeat); while a live member predates versioned state (#736), the others
write bookmarks bare so it can still read them, and refuse to run a pipeline
whose source bookmark shape is past schema 0. When the last old member stops
heartbeating, the versioned envelope is written again — nothing to configure.
See [Upgrading faucet safely](../operations/upgrading.md).

## Related pages

- [Running faucet as a service](./serve.md) — `faucet serve` fundamentals,
  security model, idempotency, concurrency, and the orphan-recovery lease
  mechanism that cluster mode extends.
- [Incremental replication and effectively-once delivery](./state.md) — use a CDC
  source + idempotent sink to close the double-run window.
- [Observability](../operations/observability.md) — all `faucet_serve_*` metrics.
