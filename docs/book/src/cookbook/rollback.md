# Undoing a run (`faucet rollback`)

A load that should not have happened — a bad deploy, a wrong parameter, a
corrupt upstream extract — is usually cleaned up under pressure with
hand-written SQL. `faucet rollback` undoes exactly what one run wrote, and
rewinds the row's bookmark so the next run re-reads it.

```bash
faucet run      pipeline.yaml                 # prints each row's run id
faucet rollback pipeline.yaml --list           # the undoable runs
faucet rollback pipeline.yaml --run <id> --dry-run
faucet rollback pipeline.yaml --run <id>
faucet rollback pipeline.yaml --run <id> --force   # restore keys a later run changed too
```

## Making runs undoable: the `rollback:` block

```yaml
rollback:
  journal: true         # before-images for upsert / delete runs (default true)
  keep_previous: true   # keep the table an overwrite replaces (default true)
  retain: 10            # undoable runs kept per row (default 10)
```

With the block present, every real root run of a rollback-capable sink
(`postgres`, `sqlite`, `mysql` in column mode):

- stamps the **run-id column** (`_faucet_run_id`; `metadata_columns` gains
  `run_id` automatically if you did not list it);
- **journals** the before-image of every key an upsert or delete touches, in
  the **same transaction** as the write (`_faucet_run_journal`), so the
  journal can never disagree with the data;
- **keeps the replaced table** of an overwrite as `<table>__faucet_prev`;
- writes a **pre-run marker** into the row's state store: the bookmark and the
  exactly-once watermark *before* the run.

The block needs a durable `state:` (`file` / `redis` / `postgres` — the
marker lives there) and is refused at load time for a sink that cannot undo
its writes or when `metadata_columns` is disabled. Only the last `retain` runs
per row stay undoable; older journals and markers are dropped as new runs
complete.

## What a rollback does, per write mode

| Run wrote with | Undo |
|---|---|
| `append` | `DELETE … WHERE _faucet_run_id = <run>` — only this run's rows go |
| `upsert` / `delete` | keys the run **created** are deleted; keys it **changed or deleted** get their journaled before-image written back |
| `overwrite` | the kept `<table>__faucet_prev` is swapped back in (one transaction, or one atomic `RENAME` on MySQL) |

Then, and only if the destination was undone, the row's **bookmark** is reset
to its pre-run value (or cleared) and, for an exactly-once row, the sink's
**commit token** is rewound — so the next run re-reads exactly the window that
was undone instead of skipping it. The run's journal rows and marker are then
dropped.

Undo is per dataset and **all-or-nothing per dataset**: a matrix run that
wrote several tables is undone one row at a time (`--row`), each in one
transaction where the backend allows it.

## The conflict guard

A key that a *later* run changed since (its `_faucet_run_id` no longer matches)
is a **conflict**: restoring it would clobber newer data. Without `--force`
the whole dataset is left untouched and the command exits with the conflict
count:

```
rollback BLOCKED: run 019… on row 'orders' (sqlite sqlite:///mirror.db#orders, upsert mode)
  delete 3 key(s) the run created, restore 12 before-image(s)
  2 key(s) were changed by a later run
  note: 2 key(s) were changed by a later run; pass --force to restore them anyway
```

For an overwrite the check is whole-table: if the target no longer holds this
run's rows, a later overwrite replaced them, and the kept copy is that run's
input — not yours.

## Over HTTP and in the console

`POST /v1/runs/{id}/rollback` undoes one invocation of a run submitted to
`faucet serve` (`{invocation_id?, row?, config?, dry_run, force}` — the
config is taken from the stored run when the server keeps it, otherwise pass
it). Admin-only (`Rollback` permission), audited as `run.rollback`. The web
console's run detail page shows each invocation's run id and a **Roll back…**
panel with the same dry-run / force controls.

## Limits

- Rollback covers the SQL sinks in column mode. A JSON/JSONB-column sink,
  files, queues and warehouses are not undoable (the block is refused at load
  time for them).
- Rows a run quarantined into the DLQ are not touched.
- Fan-out child rows are stamped but not journaled; undo applies to root rows.

See also: [`faucet verify`](./verify.md) to prove the destination matches the
source after an undo, and [Upsert / mirror tables](./upsert.md) for the
write modes above.
