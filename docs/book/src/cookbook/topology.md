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
    orders: { type: file, config: { path: ./data/orders.csv } }
  sinks:
    warehouse: { type: file, config: { path: ./out/warehouse.jsonl } }
    archive:   { type: file, config: { path: ./out/archive.jsonl } }
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

### Fan one bulk job out to several sinks

A REST `async_job` with `records_route` (for example a GraphQL bulk
export job, see the [REST source README](https://github.com/faucet-hq/faucet-stream/tree/main/crates/source/rest#graphql-bulk-export-jobs-768))
returns parents and children in one file and stamps each row with its stream
in `_stream`. When the API runs one bulk job per account at a time, the
streams must share one job: fetch once, `tee`, and give each branch a
`filter` on `_stream` plus a `drop` of the marker before its sink.

```yaml
pipeline:
  sources:
    bulk_export: { type: rest, config: { … async_job + records_route … } }
  sinks:
    orders: { type: postgres, config: { table: orders, write_mode: upsert, key: [id] } }
    line_items: { type: postgres, config: { table: order_line_items, write_mode: upsert, key: [id] } }
  nodes:
    bulk:  { kind: source, ref: bulk_export }
    split: { kind: tee, fanout: 2 }
    only_orders:
      kind: transform
      transforms:
        - { type: filter, config: { path: _stream, op: eq, value: orders } }
        - { type: drop, config: { fields: [_stream] } }
    only_items:
      kind: transform
      transforms:
        - { type: filter, config: { path: _stream, op: eq, value: order_line_items } }
        - { type: drop, config: { fields: [_stream, __parentId] } }
    write_orders: { kind: sink, ref: orders }
    write_items:  { kind: sink, ref: line_items }
  edges:
    - { from: bulk,        to: split }
    - { from: split,       to: only_orders }
    - { from: split,       to: only_items }
    - { from: only_orders, to: write_orders }
    - { from: only_items,  to: write_items }
```

The bulk job's bookmark (the job's start time) rides the final page through
every branch, so both sinks store the same bookmark and the next run resumes
from it. `records_route.only` selects a subset of streams when one run should
emit only some of them.

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

A merge can also join the branches of one source back together — a `tee` fans the
source out, each branch transforms its copy, and the `merge` combines them. Then
the merge forwards a source position (the bookmark the sink persists) only once
**every** branch has delivered it, so a sink never records a position while
another branch's copy of that page is still on its way. A merge that mixes such
branches with an unrelated source forwards no positions at all, and the next run
replays rather than skips.

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

The two sides must come from different upstream nodes. A graph that feeds both
sides from one `tee` is refused when it is built: the join reads its whole build
side before the probe side, so the shared tee would block on the full probe
channel and the run would never end.

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

A sink node with `write_mode: overwrite` stages its rows before the first write
and swaps them in — and only then persists its position — once **every** node of
the graph has succeeded and the run was not cancelled. If any node fails, or the
run is cancelled, every overwrite sink discards its staging and its destination
is left exactly as it was.

A sink whose input stopped because an upstream node failed is reported as
**failed** — in the run summary, its notification, its SLA history, its lineage
event and its run marker — not as a successful run of the records it happened to
receive. That holds under `execution.on_error: stop` too: the run still reports
per sink node before it returns the error.

## Exactly-once delivery

`delivery: exactly_once` works in topology mode. Each sink node keeps its own
commit watermark. On restart, each sink's committed position is read from its
state — or from the position embedded in its own commit token, when the sink
committed a page the state store never recorded. The source resumes from the
position furthest behind, and every sink ahead of it skips the replayed pages it
has already committed:

- by **position**, when the source can order its positions (the CDC sources), so
  the replay does not need to repeat the original page boundaries;
- otherwise by its **commit sequence**, with every sink started at the
  furthest-behind sink's sequence so the sequences keep counting the same pages.

No sink is resumed past its own progress, and no sink re-writes a page it already
has.

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

Topology runs emit the standard source/sink/transform/state metrics — with the
node id as `row` — including round-trip counts and one
`faucet_pipeline_runs_total` / `faucet_pipeline_run_duration_seconds` per sink
node (`source` is the feeding source's kind, or `multiple`), plus
`faucet_tee_records_total`, `faucet_merge_records_total`, and the
`faucet_join_*` family (`build_records`, `probe_records`, `matches`, `misses`,
`duplicates` (duplicated build keys), `build_nulls`, `project_misses`,
`build_duration_seconds`), labelled `pipeline` + `node`.

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

| `budget:` (and `faucet run --max-*` / `--allowed-sink`) | per sink node: each real sink is wrapped in the budget, a page that would cross a ceiling is refused whole and stops the run, a duration ceiling fails the node, and `allowed_sinks` is checked against every sink node's template name and kind before anything runs |
| `metadata_columns:` | per sink node: the `_faucet_*` columns are stamped on every row a sink writes; `_faucet_source` names the kind of every source that reaches the sink (`csv+jsonl` for a merge of the two) |
| `reconcile:` | per sink node: each sink's written count is compared with the probe's authoritative count, and a shortfall fails that node |

A sink whose branch produced no records still reports — an empty branch is a
result, not a missing one.

Two blocks are **refused** in topology mode, with an error from `faucet validate`:
`verify:` (a graph sink is fed through tees, merges and joins rather than one
source and one transform chain, so there is no source to compare it with) and
`rollback:` (runs are journaled and undone per matrix row). `usage:` is accepted
but not applied — a graph run records no usage or cost estimate — and `faucet
validate` says so.

## Runtimes

`faucet run`, `faucet schedule` and `faucet serve` run a topology config as its
node graph, with the same cancellation, clock, budget and run id a matrix run
gets. A server refuses a topology config for a **tenant** run, because tenant
state is namespaced per matrix row.

Commands that work on matrix rows — `doctor`, `plan` (and `POST /v1/plan`),
`backfill`, `verify`, `rollback`, `mirror`, change requests and serve's
`doctor_first` — refuse a topology config with an explicit error, rather than
acting on a synthetic row built from the `default` templates.

## Runnable examples

- `cli/examples/topology_tee_users.yaml` — fan-out to three sinks.
- `cli/examples/topology_merge_files.yaml` — fan-in of two CSV sources.
- `cli/examples/topology_join_orders_countries.yaml` — left-join enrichment.

```bash
faucet validate cli/examples/topology_join_orders_countries.yaml
faucet run      cli/examples/topology_join_orders_countries.yaml
```
