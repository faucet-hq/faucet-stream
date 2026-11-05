# Backing up the server database

`faucet serve` keeps everything the control plane knows in one database. By
default (#1049) that is a SQLite file, **`$FAUCET_DATA_DIR/history.db`**:
`~/.faucet/history.db` when `FAUCET_DATA_DIR` is unset, and
`/var/lib/faucet/history.db` in the container image (declared as a volume) and
the Helm chart (on its PVC). `--history sqlite:<path>` or a `postgres://…` URL
chooses another store; use Postgres for several replicas or `--cluster`.

| Started with | The history | After a restart |
|---|---|---|
| no `--history` | `$FAUCET_DATA_DIR/history.db`, created (`0700`) on first start | everything below is still there |
| `--history sqlite:<path>` / `postgres://…` | that file / database | everything below is still there |
| `--history memory` | this process only | **gone** — the server warns at start, `/readyz` reports `history.persistent: false`, the console shows a banner, and policy edits are refused |

A default directory that cannot be created or written refuses to start, naming
`$FAUCET_DATA_DIR` and `--history memory`; the server never falls back to
memory on its own. Two servers on one machine need their own
`FAUCET_DATA_DIR` (or `--history`): the second one refuses to share the
default file.

What the database holds:

| Table family | What it holds | Lost without a backup |
|---|---|---|
| `faucet_serve_runs`, `_shards`, `_idem`, `_run_logs` | Run records, shard leases, idempotency claims, persisted logs | Run history; a retried keyed submission may run again |
| `faucet_serve_audit` (+ `_audit_tenants`) | The audit log | Who did what |
| `faucet_templates`, `_template_tags`, `_template_launches`, `_template_deprecations`, `_template_version_deprecations` | The template registry and its launch history | Every registered template version |
| `faucet_catalog_*` | The Data Movement Catalog (datasets, schema timelines, lineage, profiles) | Cross-run history; rebuilt only by new runs |
| `faucet_serve_changes` | Change requests and their approvals | The approval record |
| `faucet_usage` | Cost & usage records | Usage reports |
| `faucet_tenants`, `_tenant_connections`, `_connect_sessions`, `_tenant_runs`, `_tenant_state_refs` | Tenants and their **sealed** connections | Every customer's connection (they would have to reconnect) |
| `faucet_serve_schedules` | Schedules | Every schedule made in the console or API |
| `faucet_policy_versions` | Server and tenant policy versions | The policy in force and its history |
| `faucet_serve_schema` | The schema version (see below) | — |

Pipeline **bookmarks are not here** — they live in each pipeline's `state:`
backend. Back that up on its own schedule.

## The vault key is part of the backup

Tenant connections and tenant notification settings are sealed with the
`--vault-key` (`FAUCET_VAULT_KEY`). A restored database is unreadable without
the key that sealed it, so store the key (and any `--vault-previous-key`
still needed) in your secret manager alongside the backup procedure — never
next to the database dump itself.

## Taking a backup

- **SQLite:** use SQLite's online backup so a running server's WAL is
  included: `sqlite3 /var/lib/faucet/history.db ".backup '/backups/history-$(date +%F).db'"`.
  Copying the file alone while the server runs can miss the `-wal` file.
- **Postgres:** `pg_dump --format=custom --table='faucet_*' "$HISTORY_URL" > history.dump`,
  or rely on your managed database's point-in-time recovery.

## Restoring

Stop every `faucet serve` instance that uses the database, restore it
(`sqlite3 history.db ".restore backup.db"` / `pg_restore --clean`), make sure
the same vault key is configured, and start the servers. Runs that were in
flight when the backup was taken read as `running` until their lease expires;
the lease loop then marks them failed (or, in cluster mode, requeues them).

## Schema version

The database records the schema version that last migrated it in
`faucet_serve_schema`. On startup a server adds any columns a newer faucet
introduced (`ALTER TABLE … ADD COLUMN`, idempotent and safe to run from
several instances at once) and stamps the version. A database stamped by a
**newer** faucet is refused at startup — rather than written by code that does
not know its columns — so a rollback to an older binary needs the backup taken
before the upgrade. See [Upgrading faucet safely](./upgrading.md).
