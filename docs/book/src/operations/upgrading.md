# Upgrading faucet safely

A resumable pipeline keeps its position between runs — a replication
bookmark, a CDC position (a Postgres LSN, a MySQL binlog file/offset, a
MongoDB resume token), an exactly-once watermark — in its `state:` store.
That position outlives the release that wrote it, so every upgrade reads state
an older faucet wrote. If the new release misread it, the pipeline would
restart from the beginning (duplicating everything downstream) or skip a range
(losing it), and the run would still be green.

faucet makes that boundary explicit: every stored position is **versioned**,
and a value the running release cannot read is **refused, never guessed**.

## What is stored

Each bookmark is written inside an envelope that names the connector owning
its shape and that connector's shape version:

```json
{
  "faucet_state": 1,
  "owner": "postgres-cdc",
  "schema": 0,
  "data": { "last_lsn": "0/16B3748" }
}
```

- `faucet_state` — the envelope version.
- `owner` — the source kind whose shape `data` is.
- `schema` — that source's bookmark-shape version. It only changes when a
  release changes the shape, and that release ships a migration.
- `data` — the bookmark itself; under `delivery: exactly_once` the
  exactly-once wrapper (`{"__faucet_eo": 1, "bookmark": …, "seq": …}`) with the
  bookmark inside it.

## What happens on the first run after an upgrade

| Stored value | What the run does |
|---|---|
| a bare value from a release before versioning | reads it as schema 0 of the row's source — no rewrite needed first — and stores it in the envelope at the next bookmark |
| an envelope at the source's current schema | reads it as-is |
| an envelope at an **older** schema | the source migrates it forward, then resumes; the migrated value replaces the old one at the next bookmark |
| an envelope at a **newer** schema (a downgrade), a newer `faucet_state`, or another `owner` | **refuses**, before anything is read from the source |

A refusal is a typed `StateIncompatible` error naming the key, what was found
and what was expected:

```text
state 'orders::cdc' is incompatible: found 'postgres-cdc' state schema 2,
expected 'postgres-cdc' state schema 1 or older — it was written by a newer
faucet or by a different source; run the release that wrote it, or reset the
row (`faucet state reset`) after confirming where it should resume
```

Migrations are pure: nothing is written until the next page's bookmark, so a
crash between reading and writing leaves the old value valid and the next run
migrates it again.

## Before you upgrade

```bash
faucet migrate --state orders.yaml --check   # exit 0 = every key is current
faucet state show orders.yaml                # owner / schema / status per key
faucet doctor orders.yaml                    # a `state` probe per row
```

`faucet migrate --state` rewrites every row's bookmark into the current
envelope and schema ahead of the first run (`--check` only reports, and exits
non-zero when a key needs work; `--row` narrows it to one row). A key it cannot
read is listed with the reason and left untouched.

## Downgrading

A newer release may write a newer schema for a source. Rolling back past that
release makes the older binary refuse the state instead of misreading it. Keep
a `faucet state export` from before the upgrade if a rollback is possible:

```bash
faucet state export orders.yaml -o before-upgrade.json
# … upgrade, run, decide to roll back …
faucet state import orders.yaml before-upgrade.json --overwrite --yes
```

## Clusters (`faucet serve --cluster`)

Members of one cluster share the state store, and during a rolling upgrade
old and new members run side by side. Every member advertises the newest state
format it reads; while any live member predates versioning, the others keep
writing bare values it can read, and refuse to run a pipeline whose source
bookmark shape is past schema 0 (an old member would misread it). Once the
last old member is gone, the envelope is written again. Nothing to configure.

## Guarantee and how it is tested

- A golden fixture of every source's released bookmark shapes — bare, in the
  envelope, and inside the exactly-once wrapper — is checked by CI on every
  change (`crates/source/*/tests/state_compat.rs`,
  `crates/conformance/tests/compat_state_format.rs`). A fixture is never edited
  to make a test pass; a shape change ships a migration instead.
- The reliability suite drives a real `Pipeline::run` from a legacy bookmark
  through a migration and asserts it resumes with no gap and no duplicate
  (`reliability_state_migration.rs`), and asserts a newer schema or another
  owner is refused before the source is read.
- `mongodb-cdc` shipped the first real migration (schema 0 → 1: the
  invalidate flag became explicit); its integration test resumes a pipeline
  from a released schema-0 bookmark against a live replica set.
