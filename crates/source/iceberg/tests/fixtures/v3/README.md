# Format-v3 fixture tables

Three small Apache Iceberg format-v3 tables written by another engine, because
iceberg-rust cannot write deletion vectors or variant columns:

| Table | What it holds |
|---|---|
| `db.dv` | two data files of four rows; two deletes, the second rewriting the first one's deletion vector. Live ids: 1, 4, 5, 6, 8 |
| `db.upgraded` | written as format v2 with Parquet position deletes, upgraded to v3, then deleted from again: one data file keeps its Parquet position delete, the other has a deletion vector that took over the earlier delete. Live ids: 1, 4, 5, 7, 8 |
| `db.variant` | `id` and a `variant` column `payload`; three rows, id 3 deleted |

`warehouse/` is generated: do not edit it. `./generate.sh` (needs Docker) runs
`tables.sql` through Spark 4.0.1 with Apache Iceberg 1.10.0 and rewrites
`warehouse/` and `SHA256SUMS`. File names and ids change on every run, so a
regeneration replaces the whole tree. `tests/v3_fixture_files.rs` checks every
file against `SHA256SUMS`.

The table metadata stores absolute file paths, so the tables only read at the
path they were written at, `/tmp/faucet-fixtures/iceberg-v3`. The tests copy
the tree there first.
