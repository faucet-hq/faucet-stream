# Topology mode (tee / merge / join)

The default pipeline moves records from one source to one sink. **Topology
mode** generalizes that to an explicit graph of typed nodes, so a single run
can:

- **fan-out (tee)** — fetch a source once and route the same records to several
  sinks (no refetch, no divergence);
- **fan-in (merge)** — concatenate several sources into one sink;
- **join** — enrich one stream with fields looked up from another by key.

Declare `pipeline.nodes` (a map of node id → node) and `pipeline.edges`
(producer → consumer connections). Topology mode is **mutually exclusive** with
`matrix:`.

## Node kinds

| `kind` | in | out | fields |
|--------|----|-----|--------|
| `source` | 0 | 1 | `ref:` (a `pipeline.sources` template) + optional `type` / `config` overrides |
| `transform` | 1 | 1 | `transforms:` (the usual transform list) |
| `tee` | 1 | N | `channel_capacity` (default 4), optional `fanout` sanity-check |
| `merge` | N | 1 | — |
| `join` | 2 | 1 | see [Joins](#joins) |
| `sink` | 1 | 0 | `ref:` (a `pipeline.sinks` template) + optional `type` / `config` overrides |

## Fan-out (tee)

```yaml
version: 1
name: fan_out
pipeline:
  sources:
    orders: { type: csv, config: { path: ./data/orders.csv } }
  sinks:
    warehouse: { type: jsonl, config: { path: ./out/warehouse.jsonl } }
    archive:   { type: jsonl, config: { path: ./out/archive.jsonl } }
  nodes:
    src:  { kind: source, ref: orders }
    norm: { kind: transform, transforms: [ { type: keys_case, config: { mode: snake } } ] }
    fan:  { kind: tee, channel_capacity: 4, fanout: 2 }
    w1:   { kind: sink, ref: warehouse }
    w2:   { kind: sink, ref: archive }
  edges:
    - { from: src,  to: norm }
    - { from: norm, to: fan }
    - { from: fan,  to: w1 }
    - { from: fan,  to: w2 }
```

Nodes run concurrently, connected by bounded channels: the slowest sink paces
its producer (backpressure). The `tee` clones each page to every downstream
edge.

## Fan-in (merge)

```yaml
  nodes:
    a: { kind: source, ref: orders }
    b: { kind: source, ref: returns }
    m: { kind: merge }
    w: { kind: sink, ref: combined }
  edges:
    - { from: a, to: m }
    - { from: b, to: m }
    - { from: m, to: w }
```

`merge` forwards pages from all inputs in arrival order.

## Joins

A `join` node hash-joins two upstreams. The **build** (right) side is buffered
into an in-memory index keyed by `build.key`; then the **probe** (left) side is
streamed and each record enriched with the `project`ed fields of its match. The
join's two incoming edges carry `as:` labels that match `build.edge` /
`probe.edge`.

```yaml
  nodes:
    fetch_customers: { kind: source, ref: customers }
    fetch_orders:    { kind: source, ref: orders }
    enrich:
      kind: join
      mode: left                 # `inner` drops non-matches; `left` keeps them
      build: { edge: customers_in, key: id }
      probe: { edge: orders_in,    key: customer_id }
      project:
        - { from: tier, as: customer_tier }
      on_missing: null           # left-mode fill when there is no match
      on_duplicate: first        # or `cartesian` (one output row per build match)
      on_collision: overwrite    # or `skip` / `error`
      key_normalize: preserve    # or `stringify` so "42" matches 42
      max_build_records: 10000000
    write: { kind: sink, ref: warehouse }
  edges:
    - { from: fetch_customers, to: enrich, as: customers_in }
    - { from: fetch_orders,    to: enrich, as: orders_in }
    - { from: enrich,          to: write }
```

The build side is fully materialized before probing begins, so pair a large
dimension table with a fast local source (SQLite / Parquet) rather than a slow
remote API, and keep `max_build_records` as a guardrail.

## State and errors

Each terminal sink owns a bookmark under `{name}::{node_id}`. On restart the
source resumes from a stored position only when **both** hold:

1. the graph has exactly **one source node**, and
2. **every** sink's stored bookmark is identical.

Otherwise the source replays in full and logs why. That is deliberately
conservative. A sink's bookmark records the position of whichever source fed its
pages, and nothing in the graph records which one that was — so in a multi-source
graph one source's position would be applied to another. And bookmarks are
compared for *equality*, never ordered: a resume position is frequently structured
(a CDC LSN map, a Kafka offset map), and ordering those falls back to comparing
serialized text, which is unrelated to replication progress — an ordered "minimum"
can sit *ahead* of the true minimum and skip the lagging sink's records.

Replaying costs duplicates; skipping loses data. So when a graph is resumed
routinely, make the sinks idempotent (`write_mode: upsert` with a `key`) or turn
on [exactly-once delivery](#exactly-once-delivery), which replaces both rules
above with a real ordering.

## Exactly-once delivery

`delivery: exactly_once` works in topology mode. Each sink node keeps its own
commit watermark, so a restart resumes from the **lowest** committed sequence
across the sinks — the one that is furthest behind — and every sink that is
already ahead of that point skips the pages it has committed. Unlike the
at-least-once rules above this is a genuine total order (the sequence is a
monotonic counter the pipeline assigns, not an opaque bookmark), so no sink is
ever resumed past its own progress and no sink re-writes a page it already has.

Five requirements are checked at config-load time, so `faucet validate` catches
a violation before anything runs:

1. exactly **one** source node — with several, one source's position would be
   applied to another;
2. that source must support replay from a bookmark (`postgres-cdc`, `mysql-cdc`,
   `mongodb-cdc`, `kafka`);
3. **every** sink node must support idempotent writes (`postgres`, `mysql`,
   `mssql`, `sqlite`, `snowflake`, `bigquery`, `redis`, `mongodb`, `kafka`,
   `spanner`, `iceberg`) — one non-idempotent sink is enough to lose the
   guarantee for the whole graph;
4. a durable `state:` block (not `memory`);
5. no `dlq:` block — a quarantined row is by definition not committed with the
   page, so the two cannot both hold.

The error message names which side is the limiting one, and suggests the
keyed-upsert alternative when the sink supports it:

```yaml
version: 1
name: cdc-mirror
delivery: exactly_once
state: { type: file, config: { path: ./state } }
pipeline:
  sources:
    changes: { type: postgres-cdc, config: { ... } }
  sinks:
    warm: { type: postgres, config: { ..., write_mode: upsert, key: [id] } }
    cold: { type: sqlite,   config: { ..., write_mode: upsert, key: [id] } }
  nodes:
    src:  { kind: source, ref: changes }
    fan:  { kind: tee, fanout: 2 }
    w1:   { kind: sink, ref: warm }
    w2:   { kind: sink, ref: cold }
  edges:
    - { from: src, to: fan }
    - { from: fan, to: w1 }
    - { from: fan, to: w2 }
```

`execution.on_error: stop` aborts the whole topology on the first failure —
signalling the other nodes so they stop at a page boundary and **flush** (a
buffered Parquet/S3 sink commits rather than orphaning its upload), then aborting
anything still running after a grace window. `continue` lets healthy branches
finish and reports the failures at the end.

Each node runs as its own task, so a synchronous stage (the DuckDB `sql`
transform, a `wasm` transform) does not stall the rest of the graph.

## Observability

Topology runs emit the standard sink/transform/state metrics plus
`faucet_tee_records_total`, `faucet_merge_records_total`, and the
`faucet_join_*` family (`build_records`, `probe_records`, `matches`, `misses`,
`duplicates`, `build_nulls`, `project_misses`, `build_duration_seconds`),
labelled `pipeline` + `node`.

Every top-level governance and reporting block applies, each scoped to the node
where it makes sense:

| Block | How it applies to a graph |
|---|---|
| `masking:` / `contract:` / `quality:` / `schema:` | per sink node, on the records that reach it — `masking`'s `applies_to` matches the sink's template name or kind, exactly as in matrix mode |
| `resilience:` | per sink node (retry / circuit breaker / poison-pill on its writes) |
| `sla:` | per sink node, with its own history under `{name}::{node_id}` — so a slow branch of a tee is reported on its own, not averaged away |
| `notify:` | per sink node: `run_success` / `run_failure` / `sla_breach`, with the node id in the event |
| `lineage:` | one OpenLineage job per sink node, named `{pipeline}.{node_id}`. Its **inputs are every source that reaches that sink**, so a merge emits a job with several inputs. Column lineage is emitted only for a single-input sink — with several inputs the per-column derivation is not knowable from the graph alone, and it is left out rather than guessed |
| `catalog:` | one dataset per source and per sink, plus one edge per (source, sink) pair that the graph actually connects; a merge sink's per-edge volume is the contributing source's own record count |

A sink whose branch produced no records still reports — an empty branch is a
result, not a missing one.

## Runnable examples

- `cli/examples/topology_tee_users.yaml` — fan-out to three sinks.
- `cli/examples/topology_merge_files.yaml` — fan-in of two CSV sources.
- `cli/examples/topology_join_orders_countries.yaml` — left-join enrichment.

```bash
faucet validate cli/examples/topology_join_orders_countries.yaml
faucet run      cli/examples/topology_join_orders_countries.yaml
```
