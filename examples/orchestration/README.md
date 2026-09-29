# Orchestration: faucet + dbt (Airflow / Dagster)

**Load with faucet, transform with dbt, schedule with your orchestrator.**

faucet is deliberately an **EL** tool — it moves data fast and reliably from a
source into a destination. It does not replace dbt; the two are complementary.
The idiomatic ELT stack is:

- **faucet** — Extract + Load. Pulls from the source and lands raw, lossless
  rows in your warehouse. Incremental replication + a durable bookmark mean each
  run only fetches new data.
- **dbt** — Transform. Builds typed, tested models on top of the raw landing
  table, in the warehouse.
- **Airflow / Dagster** — orchestration. Because faucet is a single static
  binary, "orchestrate faucet" is just "run a shell command" — no plugin
  runtime, no Python-version matrix to keep green.

```
   REST API ──faucet run──▶ Postgres (raw.charges_raw, JSONB)
                                │
                                └──dbt build──▶ analytics.stg_charges (typed, tested)
                                                     ▲
                    Airflow DAG / Dagster job runs both steps in order
```

## What's here

| File | Role |
|------|------|
| `faucet_pipeline.yaml` | The EL step — REST → Postgres raw JSONB landing table (based on [`cli/examples/rest_to_postgres.yaml`](../../cli/examples/rest_to_postgres.yaml)). |
| `dbt/` | A minimal dbt project — a source over `charges_raw` and a `stg_charges` staging model that unpacks the JSONB into typed, tested columns. |
| `airflow_dag.py` | Airflow DAG: `faucet run` → `dbt build`, chained with `>>`. |
| `dagster_defs.py` | Dagster equivalent: two assets with a dependency edge. |

## Prerequisites

```bash
cargo install faucet-cli          # the `faucet` binary
pip install dbt-postgres          # the `dbt` binary
# plus apache-airflow or dagster, whichever you orchestrate with
```

A Postgres to load into — the [examples Docker stack](../README.md) provides one:

```bash
docker compose -f examples/docker-compose.yml up -d postgres
export PG_URL=postgres://faucet:faucet@localhost:5432/appdb
export SOURCE_API_TOKEN=...           # bearer token for the REST API
```

## Run it by hand first

Prove each step works before handing it to an orchestrator:

```bash
# 1. EL — faucet lands raw rows in public.charges_raw
faucet run examples/orchestration/faucet_pipeline.yaml

# 2. T — dbt builds + tests the staging model in the analytics schema
cd examples/orchestration/dbt
dbt build --profiles-dir .
```

`faucet run` exits non-zero on failure, so any orchestrator's task-failure
handling works without extra glue.

## Then schedule it

- **Airflow** — drop `airflow_dag.py` in your `dags/` folder. It schedules every
  15 minutes; faucet's durable bookmark makes frequent runs cheap (only new rows
  are fetched).
- **Dagster** — `dagster dev -f examples/orchestration/dagster_defs.py`, then
  materialize the assets or add a schedule.

Both simply shell out to `faucet run` then `dbt build`.

## Observe it

Every faucet run emits Prometheus metrics automatically. faucet ships
ready-made **Grafana dashboards** and **Prometheus alert rules** (under
[`observability/`](../../observability)) so the EL half of this pipeline is
observable out of the box — run outcomes, throughput, bookmark staleness,
retries, and DLQ traffic. See the
[Dashboards & alerts cookbook](../../docs/book/src/cookbook/dashboards.md) and
the [Orchestration cookbook page](../../docs/book/src/cookbook/orchestration.md)
for the full walkthrough. Bring up the pre-provisioned stack with:

```bash
docker compose -f examples/docker-compose.yml up -d prometheus grafana
```
