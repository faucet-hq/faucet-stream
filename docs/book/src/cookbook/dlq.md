# Dead-letter queues

A dead-letter queue (DLQ) keeps a pipeline running when a handful of records
fail to write, instead of aborting the whole run. Failing rows are wrapped in a
fixed-shape envelope and routed to a separate DLQ sink before the page's bookmark
advances.

## When it helps

Sinks whose underlying API reports **per-row** results — BigQuery `insertAll`,
Elasticsearch `_bulk` — can tell exactly which records failed. The DLQ captures
just those, while the good rows commit normally.

## Configure a DLQ

Add a `dlq:` block naming a sink to receive the bad rows and the policy for
sinks that can't report per-row outcomes:

```yaml
pipeline:
  source: { type: rest, config: { /* … */ } }
  sink:   { type: bigquery, config: { batch_size: 0, /* … */ } }   # one insertAll per write
  dlq:
    on_batch_error: dlq_all      # or `propagate`
    sink:
      type: file
      config:
        path: ./dead-letters.jsonl
```

## The envelope

Each dead-lettered record is wrapped in a fixed-shape envelope — the original
record plus the metadata needed to inspect, fix, and replay it:

```json
{
  "error": { "kind": "ContractViolation", "message": "status: value not in enum" },
  "reason": "contract",
  "payload": { "order_id": "A-17", "status": "backordered" },
  "ts_ms": 1751760000000,
  "sink": "jsonl",
  "pipeline": "orders_csv_with_contract",
  "row": "",
  "record_index": 3
}
```

- `payload` — the record **as it entered the write path**: after the transform
  chain and after masking, since those passes run before the sink. This is what a
  replay re-feeds, and it is why a replay does not re-run them (see
  [Replaying](#replaying)).
- `reason` — which stage quarantined the row: `quality`, `contract`,
  `schema_drift`, or `partial` / `dlq_all` for a sink-side row failure. This is
  the value the `--reason` filter matches.
- `error.kind` / `error.message` — the typed failure and its message.
- `record_index` — the row's position within its original page.

## `on_batch_error` policy

For a sink that can only succeed or fail a whole batch (no per-row detail):

- `propagate` — a batch failure aborts the run (the default, fail-fast behavior).
- `dlq_all` — route every row in the failed batch to the DLQ and keep going.

Sinks that *do* report per-row results (BigQuery, Elasticsearch, and the HTTP
sink in `Individual` mode) override the partial-write path so only the genuinely
failed rows are dead-lettered — the already-delivered rows are not duplicated
into the DLQ.

## Batch atomicity and `dlq_all`

`dlq_all` sends *every* row of a failed write to the DLQ. That is only safe
when the failed write landed nothing: a DLQ replay writes every routed row
again, so rows that had already committed would land twice — silently.

So every sink declares what a failed write leaves behind:

| Batch atomicity | A failed write… | `dlq_all` |
|---|---|---|
| `atomic` | landed nothing (one statement, a transaction around every chunk, one object upload, one table commit) | allowed |
| `per_row` | reports which rows failed; a whole-write error means nothing from that write landed | allowed |
| `best_effort` | may have landed some rows and cannot say which | **refused** |

Several sinks are all-or-nothing only in some configurations — typically
`batch_size: 0`, which makes one page one statement or one request. The
[capability matrix](../reference/connectors.md#sinks) lists each sink and the
config that makes it atomic. A third-party sink that does not declare anything
counts as `best_effort`.

`faucet validate` and `faucet run` (before reading anything) refuse `dlq_all`
on a `best_effort` sink:

```text
row 'orders': dlq: on_batch_error 'dlq_all' is unsafe with sink 'jsonl' (batch
atomicity 'best_effort'): a failed batch may already have written some rows, and
replaying the DLQ would write them again. Use on_batch_error 'propagate',
configure the sink with write_mode 'upsert' and a key so a replay overwrites
instead of duplicating, or set allow_duplicates_on_dlq_all: true to accept the
duplicates
```

The ways out, safest first:

1. **Make the write atomic** — e.g. `batch_size: 0` on BigQuery, SQLite,
   Snowflake, ClickHouse or Redshift `COPY`.
2. **Write by key** — `write_mode: upsert` (or `delete`) with a `key`: a
   replayed row overwrites itself, so duplicates cannot happen.
3. **Use `propagate`** — fail the run and let the next one retry from the
   bookmark.
4. **Accept the duplicates** — set `allow_duplicates_on_dlq_all: true` on the
   `dlq:` block when the destination tolerates them (downstream dedup, an
   append-only audit log):

   ```yaml
   dlq:
     sink: { type: file, config: { path: ./dead-letters.jsonl } }
     on_batch_error: dlq_all
     allow_duplicates_on_dlq_all: true
   ```

### Seeing what happened to each batch

Every sink write is counted by outcome — `committed`, `dlq_partial` (some rows
went to the DLQ per row), `dlq_all` (the whole write went to the DLQ) or
`failed` (the error propagated):

- the `faucet_batch_outcomes_total{pipeline,row,sink,outcome}` counter;
- `faucet run --output json` (`rows[].batches`);
- `faucet status`, which marks a row degraded when its last run sent writes to
  the DLQ (`last run: of 40 sink write(s), 2 went to the DLQ whole (dlq_all)`);
- the run-history record (`invocations[].batches`) and the **batches** column
  of the web console's run detail.

## Failure budgets

A DLQ keeps a run going through *occasional* bad rows, but a flood of failures
usually means something is broken upstream. Two optional budgets turn the DLQ
into a circuit breaker:

```yaml
  dlq:
    sink: { type: file, config: { path: ./dead-letters.jsonl } }
    max_failures_per_page: 50    # abort if a single page dead-letters > 50 rows
    max_failures_total: 500      # abort once the run has dead-lettered > 500 rows
```

When a budget trips, the run aborts — but only **after the page that crossed the
threshold is fully committed**: its surviving rows are written to the main sink,
its failed rows are routed to the DLQ, and (if the page carried one) the bookmark
advances. So the committed survivors are *not* re-delivered when you fix the
upstream problem and re-run, and the failed rows are preserved in the DLQ for
replay rather than dropped. The run still stops, so you get alerted.

## Inspecting the DLQ

`faucet dlq inspect` reads a DLQ location back and groups it by reason and error
kind, with a sample — so you can see *why* rows failed before deciding what to do:

```console
$ faucet dlq inspect ./dlq/contract_breaches.jsonl
DLQ inspect: ./dlq/contract_breaches.jsonl
  files read: 1   envelopes: 42   malformed: 0   non-envelope: 0
  by reason:
    contract       42
  by error kind:
    ContractViolation    42
  sample (5 of 42):
    [contract/ContractViolation] status: value not in enum
      {"order_id":"A-17","status":"backordered"}
```

The location may be a single `.jsonl` file, a directory of `*.jsonl` files, or a
glob. Blank, malformed, and non-envelope lines are counted (`malformed` /
`non-envelope`) but never abort the read. Add `--reason contract` to restrict the
breakdown, `--limit N` to size the sample, or `--json` for a machine-readable
summary.

## Replaying

Once you've fixed the root cause — a transform, a contract, the destination
schema — `faucet dlq replay` re-feeds the quarantined **payloads** through a
pipeline config (quality → contract → sink):

```console
$ faucet dlq replay orders.yaml --from ./dlq/contract_breaches.jsonl --dry-run
DLQ replay (dry-run): 42 candidate record(s) from ./dlq/contract_breaches.jsonl
would be re-fed; 42 would reach the sink. Failures would go to
./dlq/contract_breaches.replay-failed.jsonl.

$ faucet dlq replay orders.yaml --from ./dlq/contract_breaches.jsonl
DLQ replay: 42 candidate record(s) re-fed; 42 written to the sink. …
```

**A replay skips the transform chain and the masking pass**, because the payload
already went through both before it was captured, and neither is idempotent:
re-running the `hash` transform or masking's `hash`/`tokenize` action would
produce `H(H(x))` — breaking the joinability masking exists to guarantee — a
`set` stage would re-stamp `${now.*}` with the replay time, and `cast` /
`json_parse` / `split` would re-run against already-converted values. The quality
and contract passes *are* re-applied: they are pure checks over the record, so
re-checking is idempotent, and a replay is exactly when you want them enforced.

Rows that fail **again** on replay are quarantined to a *fresh* DLQ — a
`replay-failed.jsonl` sibling of the source by default (override with
`--failed-dlq`) — never back to the source, so a replay can't loop. `--dry-run`
reports what would be replayed without writing; `--reason` replays only matching
envelopes; `--row` picks a specific root when the config has several.

> **Make replay idempotent.** A replay is a fresh run — if some of a page
> originally landed before the failure, replaying can duplicate it on an
> append-only sink. Use [`write_mode: upsert`](./upsert.md) on the target so a
> replayed row overwrites rather than duplicates.

## Discarding

Once envelopes are handled (replayed, or known-bad), `faucet dlq discard` clears
them so the DLQ doesn't grow unbounded:

```console
$ faucet dlq discard ./dlq/contract_breaches.jsonl --reason contract --before 7d
DLQ discard: archived 42 envelope(s) across 1 file(s) → ./dlq/contract_breaches.archived.jsonl
```

By default discarded envelopes are moved to a `<file>.archived.jsonl` sibling;
`--delete` removes them outright. `--reason` and `--before` (an RFC3339 timestamp
or a relative age like `7d` / `24h` / `30m`) select what to discard — everything
else, including non-envelope lines, is left untouched.

## Encryption at rest

DLQ envelopes carry failed records **verbatim** — on a shared or
compliance-scoped host that can be a plaintext-at-rest gap. When the DLQ sink
is a `file` sink writing JSON Lines (or the deprecated `jsonl` sink), seal every
envelope line with AES-256-GCM (requires a build with the `encryption` feature —
included in `--features full`):

```yaml
dlq:
  sink:
    type: file
    config:
      path: ./dlq/failed.jsonl
      encryption:
        key: ${vault:secret/faucet#dlq-key}
        # previous_keys: ["${env:OLD_KEY}"]   # rotation: read-only candidates
```

Each record line is encrypted individually and written base64-encoded, so the
file stays line-oriented and append-safe. Keep the DLQ uncompressed: with
`compression` (or a `.gz` / `.zst` path) the file sink seals the whole file
instead of each line, which `faucet dlq` and `faucet status` cannot read line by
line — so a `file` DLQ with both `compression` and `encryption` is refused when
the config loads. (The jsonl sink refuses the combination outright.)

The `faucet dlq` verbs handle sealed files transparently:

- `inspect` / `discard` — pass `--encryption-key <KEY>` (repeat the flag to
  also try rotated keys). Without a key, sealed lines are counted and reported
  as *encrypted* — never mistaken for malformed lines, never mangled.
- `replay` — picks the key up **automatically** from the config's own
  `dlq:` block's `encryption` (a JSON Lines `file` sink or a `jsonl` sink);
  `--encryption-key` overrides.
- `discard` keeps and archives lines **verbatim** (still sealed) — filtering
  decrypts only in memory; nothing is ever re-written in plaintext.

The same `encryption` block also seals `file` state-store bookmarks — see
[State & resumability](./state.md#encryption-at-rest-file-backend).

> The full design is in
> [`docs/superpowers/specs/2026-05-24-dlq-design.md`](https://github.com/faucet-hq/faucet-stream/blob/main/docs)
> and the `faucet_core::dlq` module on docs.rs.
