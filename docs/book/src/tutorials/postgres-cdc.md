# PostgreSQL CDC → JSONL

Change data capture (CDC) streams every `INSERT`/`UPDATE`/`DELETE` from a
PostgreSQL table by reading its write-ahead log via logical replication — no
polling, no `updated_at` column required.

## Prepare Postgres

CDC needs logical replication enabled (`wal_level = logical`) and a publication
for the tables you want to follow:

```sql
CREATE TABLE IF NOT EXISTS users (id int4 PRIMARY KEY, name text);
CREATE PUBLICATION faucet_pub FOR TABLE users;
```

The bundled [`examples/docker-compose.yml`](https://github.com/faucet-hq/faucet-stream/tree/main/examples)
starts a Postgres already configured for logical replication.

## Config

```yaml
version: 1
pipeline:
  source:
    type: postgres-cdc
    config:
      connection_url: postgres://faucet:faucet@localhost:5432/appdb
      slot_name: faucet_slot
      publication_name: faucet_pub
      create_slot_if_missing: true
      idle_timeout: 30
  sink:
    type: file
    config:
      path: ./out/changes.jsonl
      if_exists: append
  state:
    type: file
    config:
      path: ./state
```

```bash
faucet run postgres_cdc_to_jsonl.yaml
```

Open a `psql` session and `INSERT`/`UPDATE`/`DELETE` some rows — the connector
drains them every fetch cycle until `idle_timeout` fires.

## Why the state store matters here

The CDC source advances Postgres's `confirmed_flush_lsn` (the point up to which
Postgres may recycle WAL) **only from a durable bookmark** — i.e. after the
pipeline has persisted the position. It never confirms WAL for changes that
haven't been written to the sink. That means a crash mid-run cannot lose data:
on restart the source resumes from the last persisted bookmark. The tradeoff is
that WAL is retained until the next run advances the bookmark, so don't point a
CDC slot at a table and then never run it.

The state key is `postgres-cdc:<slot>`. Use a durable backend (`redis` /
`postgres`) in production so the bookmark survives the loss of the local disk.

## Slot lifecycle

- Only `slot_type: permanent` (the default) works; it retains WAL until you
  drop it. `temporary` is refused at config load — a temporary slot is dropped
  with the session that creates it, before replication starts. For an
  experiment, use a permanent slot and drop it afterwards.
- `tcp_keepalive` is accepted but has no effect: a dead replication connection
  is detected through `status_update_interval` and the server's
  `wal_sender_timeout`.
- Free an abandoned slot's WAL with `PostgresCdcSource::drop_slot()` (library)
  or by dropping the replication slot in Postgres.
- `tls.mode: from_url | disable | require | verify_ca | verify_full` configures
  the replication connection and the `faucet doctor` probe (default `from_url`:
  the URL's `sslmode`; use `verify_full` over untrusted networks).

## See also

- [Upsert / mirror tables](../cookbook/upsert.md) — turn the raw CDC envelopes
  this tutorial emits into an up-to-date mirror (inserts, updates, deletes).
- [Incremental replication & state](../cookbook/state.md) — how bookmarks make
  the run resumable.
