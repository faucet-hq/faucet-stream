# Content verification (`faucet verify`)

A pipeline can report green while its destination quietly diverges from the
source: a missed change event, a hand edit downstream, a retried page that
landed twice, a partial load that left a table half-old. Row counts
([`reconcile:`](../reference/config.md#reconcile)) cannot see a row that exists
on both sides with different values, or a missing key balanced by an extra
one. `faucet verify` proves a destination matches its source **by content**,
reports exactly which keys differ, and — opt-in — repairs only those keys
through the pipeline's own write path.

```bash
faucet verify pipeline.yaml                      # exit code = number of differing keys
faucet verify pipeline.yaml --json               # the full report, machine-readable
faucet verify pipeline.yaml --repair             # re-sync missing / changed keys
faucet verify pipeline.yaml --repair --allow-delete   # also delete destination-only rows
faucet verify pipeline.yaml --row orders         # one root row of a matrix config
```

## What is compared

Verification is **keyed**: rows are matched on the sink's `key` (`write_mode:
upsert`) or an explicit `verify.key`. A keyless table is refused — there is no
stable identity to bisect on.

The source side is passed through the row's **transforms and masking** first,
so what is compared is what the pipeline would have written. A deterministic
mask (`hash`, `tokenize`, `redact`, `partial`) therefore matches on both sides
instead of being reported as a difference. Columns are compared after
normalisation (`verify.normalize`: float tolerance, timestamps as UTC
microseconds, numeric strings); the `_faucet_*` metadata columns a run stamps
are excluded by default (`verify.exclude`).

The destination is read back through a faucet **source**: the SQL sinks
(`postgres`, `sqlite`, `mysql` in column mode) describe their own read-back;
any other sink needs `verify.destination: { type: <source>, config: {…} }`.

## How it stays cheap: digests and bisection

With a single integer key on two range-readable SQL sources (`postgres`,
`mysql`, `sqlite`, `mssql`) the verifier never reads the whole table:

1. **Plan** the key space into `verify.ranges` contiguous ranges (the same
   PK-range planner the cluster sharding uses).
2. **Digest** each range — `count(*)`, an order-independent fold of a per-row
   hash, and the key bounds. When both backends report the same digest
   algorithm (Postgres ↔ Postgres, MySQL ↔ MySQL) the digest is computed
   **inside the database** and a matching range ships no rows at all;
   otherwise both sides are streamed and hashed client-side.
3. **Bisect** the ranges that disagree until a range holds at most
   `verify.leaf_rows` rows.
4. **Diff** the leaf rows per key → `missing_in_dest`, `extra_in_dest`,
   `changed` (with the differing columns), `duplicate`.

Any other key shape, or a source that cannot read a key range (a file, an
API), compares the whole dataset in one keyed pass. `verify.max_rows_scanned`
caps how much either side may read; a capped report is marked `truncated`.

```
verify [orders]: DIFFERENT — sqlite:///app.db#orders vs sqlite:///mirror.db#orders (key id, range mode, 21 range(s) compared, 3 differing)
  digests: client-side   rows fetched: 2,048 source / 2,047 destination
  3 differing key(s): 1 missing in destination, 1 extra in destination, 1 changed, 0 duplicated
    {"id":2} → changed: amount
    {"id":3} → missing in destination
    {"id":9} → extra in destination
```

## Repair

`--repair` re-reads the differing keys from the source and writes them through
the row's own sink with `write_mode: upsert`, so quality and contract checks —
and the DLQ — still apply. Rows that exist only in the destination are left
alone unless `--allow-delete` (a delete is not undoable). `--dry-run` plans
the repair without writing. The sink must support keyed writes (see the
[capability matrix](../reference/connectors.md)). A second `faucet verify`
after a repair reports zero differences.

## Verifying every run: the `verify:` block

```yaml
verify:
  key: [id]                 # default: the sink's upsert key
  exclude: ["_faucet_*"]    # default
  ranges: 16                # first-pass ranges
  leaf_rows: 1000           # bisect down to this many rows
  max_differences: 1000     # report cap (the count keeps going)
  normalize:
    float_tolerance: 0.0
    timestamps: true
    numeric_strings: false
  after_run: true           # verify after every successful root run
  fail_on_difference: true  # …and fail the run on a mismatch
  repair: false             # …or re-sync the differences first
  allow_delete: false
  # destination: { type: postgres, config: { connection_url: …, query: "SELECT * FROM t" } }
```

With the block present, `faucet run` / `schedule` / `serve` verify the
destination after every successful root invocation (`after_run`). A mismatch
fails the run — the same posture as `reconcile:` — unless
`fail_on_difference: false`, in which case it is logged, counted, and the run
stays green. `repair: true` heals the drift inside the run before deciding.
The command's flags (`--repair`, `--allow-delete`, `--max-differences`)
override the block for that invocation; without a block, `faucet verify` uses
the defaults above.

## Over HTTP

`POST /v1/verify` with `{config, row?, repair?, allow_delete?, dry_run?}`
returns the same report (`RunWrite`, operator+; audited as `verify`). A
mismatch is a result, not an error: the 200 body carries the differences.

## Metrics

| Metric | Labels | Meaning |
|---|---|---|
| `faucet_verify_runs_total` | `pipeline,row,outcome` | verifications, `equal` / `different` |
| `faucet_verify_ranges_total` | `pipeline,row,outcome` | key ranges compared by digest |
| `faucet_verify_differences_total` | `pipeline,row,kind` | differing keys by kind |
| `faucet_verify_duration_seconds` | `pipeline,row` | wall-clock of one verification |

## Limits

- Range mode needs a **single integer key**; composite or text keys compare the
  whole dataset (bounded by `max_rows_scanned`).
- A source changing during the scan can be reported as differing; re-run, or
  verify against a snapshot query where the backend offers one.
- Server-side digests compare only within one backend family; a Postgres →
  MySQL pair digests client-side (still correct, just streamed).

See also: [`faucet rollback`](./rollback.md) to undo a run, and
[`reconcile:`](../reference/config.md#reconcile) for the cheaper count check.
