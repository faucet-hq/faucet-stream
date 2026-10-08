# Orchestration (Airflow / Dagster + dbt)

faucet is an **EL** engine: it moves data fast and reliably from a source into a
destination. It is **complementary to dbt**, not a replacement — the idiomatic
ELT stack loads with faucet and transforms with dbt, scheduled by an
orchestrator. Because faucet is a single static binary, "orchestrate faucet" is
just "run a shell command" — there is no plugin runtime to install on your
workers and no Python-version matrix to keep green.

```
   REST API ──faucet run──▶ Postgres (raw, JSONB)
                                │
                                └──dbt build──▶ analytics.stg_charges (typed, tested)
                                                     ▲
                    Airflow DAG / Dagster job runs both steps in order
```

| Stage | Owner | What it does |
|-------|-------|--------------|
| Extract + Load | **faucet** | Pull from the source, land raw lossless rows in the warehouse. Incremental replication + a durable [state bookmark](./state.md) mean each run only fetches new data. |
| Transform | **dbt** | Build typed, tested models on the raw landing table, inside the warehouse. |
| Schedule | **Airflow / Dagster** | Run the two steps in order; retry on failure. |

## The runnable recipe

A complete, working example lives at
[`examples/orchestration/`](https://github.com/faucet-hq/faucet-stream/tree/main/examples/orchestration):

| File | Role |
|------|------|
| `faucet_pipeline.yaml` | EL step — REST → Postgres raw JSONB landing table (based on the shipped [`rest_to_postgres.yaml`](https://github.com/faucet-hq/faucet-stream/blob/main/cli/examples/rest_to_postgres.yaml)). |
| `dbt/` | A minimal dbt project — a source over the raw table and a `stg_charges` model that unpacks the JSONB into typed, tested columns. |
| `airflow_dag.py` | Airflow DAG: `faucet run` → `dbt build`, chained with `>>`. |
| `dagster_defs.py` | Dagster equivalent: two assets with a dependency edge. |

### Load with faucet

faucet lands the source payload verbatim in a single JSONB column, so the load
stays lossless and schema-agnostic — dbt does the typed unpacking downstream:

```yaml
pipeline:
  source:
    type: rest
    config:
      base_url: https://api.example.com/v1
      path: /charges
      auth:
        type: bearer
        config: { token: ${env:SOURCE_API_TOKEN} }
      pagination:
        type: Cursor
        next_token_path: $.next_page
        param_name: starting_after
      replication_method: { type: Incremental }
      replication_key: created
      primary_keys: ["id"]
      state_key: charges:raw
  sink:
    type: postgres
    config:
      connection_url: ${env:PG_URL}
      table_name: charges_raw       # dbt reads this raw landing table
      column_mapping: { jsonb: { column: data } }
  state:
    type: file
    config: { path: ./.faucet-state }
```

### Transform with dbt

The staging model reads faucet's raw table as a dbt `source` and casts the JSONB
fields into typed columns:

```sql
-- stg_charges.sql — faucet did the lossless load; dbt does the typing.
select
    data ->> 'id'                              as charge_id,
    (data ->> 'amount')::bigint                as amount_cents,
    data ->> 'currency'                        as currency,
    data ->> 'status'                          as status,
    to_timestamp((data ->> 'created')::bigint) as created_at
from {{ source('raw', 'charges_raw') }}
```

### Schedule with Airflow

The DAG is two `BashOperator`s with a dependency edge — `faucet run` exits
non-zero on failure, so Airflow's task-failure handling works with no extra glue:

```python
extract_load = BashOperator(
    task_id="faucet_extract_load",
    bash_command=f"faucet run {FAUCET_CONFIG}",
)
dbt_transform = BashOperator(
    task_id="dbt_build",
    bash_command=f"dbt build --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}",
)
extract_load >> dbt_transform
```

### Or with Dagster

The same shell-out pattern, expressed as two assets:

```python
@asset
def charges_raw(context):
    subprocess.run(["faucet", "run", str(FAUCET_CONFIG)], check=True)

@asset(deps=[charges_raw])
def stg_charges(context):
    subprocess.run(["dbt", "build", "--project-dir", str(DBT_DIR),
                    "--profiles-dir", str(DBT_DIR)], check=True)
```

## Why frequent scheduling is cheap

faucet's [incremental replication + durable bookmark](./state.md) mean a run
scheduled every few minutes only fetches rows newer than the last persisted
position — not a full re-scan. The bookmark advances only after the sink confirms
the batch, so a crashed run resumes exactly where it left off. That is what makes
a `*/15 * * * *` schedule practical rather than wasteful.

## Observe the EL half

Every faucet run emits Prometheus metrics automatically, and faucet ships
ready-made **Grafana dashboards** and **Prometheus alert rules** — so the EL
stage of an orchestrated pipeline is observable out of the box (run outcomes,
throughput, bookmark staleness, retries, DLQ traffic). See
[Dashboards & alerts](./dashboards.md) for the full set. Bring up the
pre-provisioned stack alongside the recipe:

```bash
docker compose -f examples/docker-compose.yml up -d prometheus grafana
```

Enable the exporter in `faucet_pipeline.yaml`:

```yaml
observability:
  prometheus:
    listen: 0.0.0.0:9464
```

## See also

- [Scheduling](./scheduling.md) — faucet's own built-in cron (`faucet schedule`),
  if you'd rather not run a separate orchestrator for simple cases.
- [Incremental replication & state](./state.md) — how the durable bookmark works.
- [Dashboards & alerts](./dashboards.md) — the shipped Grafana/Prometheus artifacts.
- [vs. Meltano](../comparison/meltano.md) — how the faucet + dbt split compares to
  Singer + dbt.
