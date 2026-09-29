"""Airflow DAG: EL with faucet, T with dbt.

faucet is a single static binary, so orchestration is just "shell out to it" —
no plugin runtime, no Python-version matrix. This DAG runs the faucet EL config
to land raw rows in Postgres, then runs `dbt build` to transform + test them.

Drop this file in your Airflow `dags/` folder. It assumes the `faucet` and
`dbt` binaries are on the worker's PATH (`cargo install faucet-cli`;
`pip install dbt-postgres`) and that the pipeline's env vars (PG_URL,
SOURCE_API_TOKEN, and the dbt PG* vars) are set in the worker environment.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator

HERE = Path(__file__).resolve().parent
FAUCET_CONFIG = HERE / "faucet_pipeline.yaml"
DBT_DIR = HERE / "dbt"

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="faucet_charges_elt",
    description="EL with faucet, T with dbt",
    schedule="*/15 * * * *",  # faucet's durable bookmark makes frequent runs cheap
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["faucet", "dbt", "elt"],
) as dag:
    # Extract + Load: faucet fetches new charges and lands them in Postgres.
    # `faucet run` exits non-zero on failure, so Airflow marks the task failed.
    extract_load = BashOperator(
        task_id="faucet_extract_load",
        bash_command=f"faucet run {FAUCET_CONFIG}",
    )

    # Transform + test: dbt builds the staging models over the raw load.
    dbt_transform = BashOperator(
        task_id="dbt_build",
        bash_command=f"dbt build --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}",
        env={**os.environ, "DBT_PROFILES_DIR": str(DBT_DIR)},
    )

    extract_load >> dbt_transform
