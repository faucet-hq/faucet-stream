# RFC 0009 — One meaning per word: `partition` and `replication`

*Give each of the two remaining overloaded config words a single meaning by renaming the minority usages — the REST source's `partitions:` / `partition_concurrency:` / `odata.partition:` and the snapshot→CDC `replication:` block / `faucet replicate` — while keeping every old spelling working.*

| | |
|---|---|
| **RFC** | 0009 |
| **Title** | Vocabulary renames for `partition` and `replication` |
| **Status** | Accepted |
| **Authors** | faucet-stream maintainers |
| **Related issues** | #670 (M21 — this RFC) · #654 (principles audit) · #650 (PRINCIPLES.md §8, the vocabulary registry) · epic #38 |
| **Related ADRs** | — |

## Summary

PRINCIPLES.md §8 asks that each config word mean exactly one thing, checked
against the vocabulary registry in `.claude/rules/architecture.md`. Two words
still carry more than one meaning:

- **`partition`** names the source-agnostic `partition:` block that splits one
  matrix row into N invocations, *and* two mechanisms inside the REST source:
  `partitions:` (a list of per-request path substitutions, fetched with
  `partition_concurrency`) and `odata.partition:` (an in-process key-range
  reader that overlaps what `shard` means).
- **`replication`** names bookmark-based incremental reads
  (`replication_method`, `faucet_core::replication`) *and* the snapshot→CDC
  handoff (`replication:` block, `faucet replicate`).

This RFC keeps the majority meaning of each word and renames the minority:

| Old spelling | New spelling | Where |
|---|---|---|
| `partitions:` | `requests:` | REST source config |
| `partition_concurrency:` | `request_concurrency:` | REST source config |
| `odata.partition:` | `odata.key_ranges:` | REST source `odata:` block |
| `replication:` (top level) | `mirror:` | pipeline config |
| `faucet replicate` | `faucet mirror` | CLI |
| `faucet schema replication` | `faucet schema mirror` | CLI |

## Motivation

A word with two meanings makes a config ambiguous to read and a feature hard to
search for. `partition_concurrency` looks like it tunes the `partition:` block;
it does not. "Replication" in a config review could mean "incremental reads" or
"the one-time snapshot handoff", which fail in different ways and need
different state. Each rename makes a key say what it does:

- a REST `requests:` entry is one request's substitution context, and
  `request_concurrency` is how many of them run at once;
- `key_ranges` is what the OData reader actually splits on, and it no longer
  borrows `partition` from the row-splitting block or blurs into `shard`;
- `mirror` is what the snapshot→CDC orchestration produces — the cookbook and
  the docs already described it that way.

## Guide-level explanation

New configs use the new spellings. Existing configs keep working unchanged:
every old spelling is a serde alias, the connector-key gate accepts it (via the
`x-faucet-aliases` schema extension), and `faucet replicate` /
`faucet schema replication` stay as CLI aliases. When a config still uses an
old key, loading it logs one warning per key naming the replacement:

```
WARN rest source: `partitions` is now `requests` (the old key still works)
WARN `replication:` is now `mirror:` (the old key still works)
```

## Reference-level explanation

- Rust field names are unchanged (`RestStreamConfig::partitions`,
  `partition_concurrency`, `ODataConfig::partition`,
  `PipelineConfig::replication`), so library callers are unaffected; only the
  serde names move (`rename = "<new>"`, `alias = "<old>"`). Serialization and
  the JSON Schema use the new names.
- Aliases are invisible to schemars, so each renamed struct lists its old
  names under `x-faucet-aliases`, which `registry::collect_schema_keys` already
  honours for the unknown-key gate (#654 H9).
- `cli/src/vocabulary.rs` scans the raw document before the typed parse (serde
  aliases erase which spelling was used) and warns. REST keys are only flagged
  on a connector whose effective `type` is `rest`.
- The run-level concurrency override (#610) prefers `request_concurrency`,
  and still knows `partition_concurrency` for connectors that declare it.
- Generated output uses the new names too: the OData discovery `config_patch`
  emits `odata.key_ranges`.

## Drawbacks

- Two spellings exist for a while; an old config read side by side with a new
  one looks inconsistent until it is updated. The warning points at the fix.
- Search for the old names in chat logs and issues still finds pre-rename
  material.

## Rationale and alternatives

- **Rename `replication_method` to `sync_mode` instead of the handoff.** It
  would touch every source config and every template in every catalog, and it
  would lose the Singer-familiar term. Renaming the handoff touches one block
  and one command.
- **Fold `odata.partition` into the engine's `shard:`.** Sharding distributes
  work across instances; the OData reader splits within one process. Merging
  them would turn an in-process reader into a cross-instance one — a behaviour
  change, not a rename. Out of scope here.
- **Break compatibility outright.** Rejected: every existing REST config with
  `partitions: []` would stop loading.

## Exceptions

`faucet-source-snowflake`'s `partition_concurrency` keeps its name: it counts
Snowflake's own result-set *partitions* (the SQL API's term), not a faucet
mechanism.

## Unresolved questions

- When the old spellings stop being accepted. Proposed: not before the next
  major version, announced one minor release ahead.
