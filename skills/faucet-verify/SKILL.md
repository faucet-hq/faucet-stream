---
name: faucet-verify
description: >-
  Use when proving a faucet copy is correct: "did it copy everything",
  comparing a source table with its destination, signing off a migration or
  a mirror before cutover, a row-count, sum or null-count mismatch, columns
  that look like the wrong type in the destination (decimals, dates or
  timestamps stored as text), duplicate keys after a retry or replay,
  running `faucet verify` or `verify --repair`, adding a `verify:` or
  `reconcile:` block so every run is checked, and producing a pass/fail
  report the user can rerun.
license: MIT OR Apache-2.0
---

# Proving a copy is correct

A green run says the pipeline finished, not that the destination matches the
source. This skill is how to prove it, table by table, and how to tell a real
difference from an expected type mapping.

When a difference is real, the `faucet-debug` skill finds why. Cutting over
from another tool is `faucet-migrate`.

## Step 0: use the project's faucet version

The project decides which faucet to use, not whatever is on the `PATH`: a
config written for a newer faucet can fail or be misread on an older one.

1. Find the pin: the `"github:faucet-hq/faucet-stream"` entry under `[tools]`
   in the project's `mise.toml` (`faucet init` writes it). A config may also
   carry `requires_faucet: ">=X.Y"`.
2. Run `faucet --version`. Use that binary if it equals the pin (with no pin:
   if it satisfies every `requires_faucet` in the project).
3. Otherwise, if [mise](https://mise.jdx.dev) is installed, run `mise install`
   in the project and prefix every command with `mise exec --`.
4. Otherwise use the pinned container image; its entrypoint is `faucet`:
   `docker run --rm --user "$(id -u):$(id -g)" -v "$PWD:/work" -w /work ghcr.io/faucet-hq/faucet-stream:<version> --version`.
5. Otherwise install exactly that version:
   `curl --proto '=https' --tlsv1.2 -LsSf https://github.com/faucet-hq/faucet-stream/releases/download/faucet-cli-v<version>/faucet-cli-installer.sh | sh`.
6. With no pin at all, install the latest release (the same installer from
   `releases/latest/download/`) and pin it as described in
   [Pinning the faucet version](https://faucet-hq.github.io/faucet-stream/operations/pinning.html).

Every version-specific fact (which connectors and blocks exist, config keys,
types, defaults, commands and flags) comes from that binary: `faucet list`,
`faucet schema --help`, `faucet schema source|sink|transform <name>`,
`faucet <command> --help`. Never from memory or from this skill. When a config
passes `faucet validate`, set its `requires_faucet:` to `">=<major>.<minor>"`
of that binary; if validate rejects `requires_faucet` as an unknown field, the
binary predates it, so leave it out.

## Step 1: use `faucet verify` where it applies

`faucet verify` compares source and destination **by content**, keyed, and
names every key that differs
([Content verification](https://faucet-hq.github.io/faucet-stream/cookbook/verify.html)).
It needs a key (the sink's upsert key, or one set in a `verify:` block) and a
way to read the destination back.

```bash
faucet verify --help
faucet verify pipeline.yaml --json
faucet verify pipeline.yaml --row orders --max-differences 50
```

1. Run it read-only first. Its exit code is the number of differing keys.
2. Read the report: missing in the destination, extra in the destination,
   changed (with the columns), duplicated.
3. Repair (`--repair`) only after the user agrees, and only once the cause is
   known (`faucet-debug`); deleting destination-only rows is a separate
   opt-in. Read the flags from `faucet verify --help`.
4. To check every run, add a `verify:` block (`faucet schema verify`), or the
   cheaper count check (the `reconcile:` block in `faucet schema config`,
   [`reconcile:`](https://faucet-hq.github.io/faucet-stream/reference/config.html#reconcile)).
   A worked example is `cli/examples/sqlite_mirror_verify_rollback.yaml`.

Use the manual comparison below when `faucet verify` cannot run: a keyless
table, a destination with no read-back source in this build (`faucet list`),
a sign-off the user wants as queries they can rerun themselves, or a type
question.

## Step 2: map each source table to its destination

Before comparing anything, write down for each table:

1. **Where it landed.** A template's sink names each stream's destination
   (per-stream table ids, stream suffixes); a matrix row's sink config names
   it for a pipeline. Read it from the composed config
   (`faucet validate --show-composed`, `faucet hub compose`) or the run's
   per-stream plan, never by guessing.
2. **The write mode** (`faucet explain pipeline.yaml`). Overwrite replaces the
   table, so counts must match exactly. Append keeps every run's rows, so a
   rerun or an at-least-once replay leaves duplicates; compare distinct keys,
   not rows. Upsert keeps one row per key.
3. **The slice.** An incremental row reads from a bookmark; compare the same
   window on both sides, or the whole table after a full refresh.
4. **What the pipeline changes on purpose:** transforms, masking, filters,
   metadata columns. Compare what the pipeline would have written, or exclude
   those columns.

## Step 3: compare per table

Run each query on both sides, over the same slice:

1. Row count.
2. Distinct count of the key (and the count of null keys).
3. Null count per column.
4. Per numeric column: sum, min and max. When the destination stored the
   column as text, cast it to an exact decimal type before summing; never
   through a float.
5. When exactness matters: an ordered checksum over the key and the compared
   columns, on a sample of keys chosen on the source side.

Keep the exact query text for each side; the report needs it.

## Step 4: compare column types

Read the destination's column types and compare them with the source's.

- When the source **declares** its column types and the sink created the
  table, decimals, dates, timestamps and UUIDs land as typed columns. When
  the sink **infers** the types from the first page (the source declares
  nothing, a transform or mask rewrote the column, or the table already
  existed), they land as text holding the exact rendering. Both are by
  design: see
  [Declared column types](https://faucet-hq.github.io/faucet-stream/reference/value-fidelity.html#declared-column-types)
  and [canonical renderings](https://faucet-hq.github.io/faucet-stream/reference/value-fidelity.html#canonical-renderings).
- Report such a column as a **known mapping**, with the rendering it holds,
  not as data loss. It is data loss only when the values themselves differ
  (Step 3 on the cast column).
- An existing table is never retyped; changing it is a schema-drift decision
  ([Schema drift](https://faucet-hq.github.io/faucet-stream/cookbook/schema-drift.html)).
- Non-finite floats arrive as null and are counted; a null-count difference
  on a float column may be that
  ([Non-finite values](https://faucet-hq.github.io/faucet-stream/reference/value-fidelity.html#non-finite-values)).

## Step 5: report

One row per table: source, destination, write mode, each check with both
values and pass / fail, and the type mappings found. Under the table, the
exact query used on each side, so the user can rerun every check. A table
with any failing check is not signed off; hand it to `faucet-debug`.
