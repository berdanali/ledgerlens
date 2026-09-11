"""
LedgerLens dbt orchestration DAG.

Runs every hour:
  dbt_run  →  dbt_test

Both tasks share an on_failure_callback that writes a structured ERROR log
entry — cheap alerting without switching to PythonOperator.

dbt reads Parquet files directly from MinIO via httpfs (docker profile),
which works because this code runs inside a Linux container — no Windows
Smart App Control issue.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

log = logging.getLogger(__name__)

_DBT_DIR = "/opt/airflow/dbt_project"
_DBT_FLAGS = f"--profiles-dir {_DBT_DIR} --target docker"


def _on_failure(context: dict) -> None:
    ti = context["task_instance"]
    log.error(
        "LedgerLens | TASK FAILED | dag=%s task=%s run_id=%s execution_date=%s | %s",
        ti.dag_id,
        ti.task_id,
        context["dag_run"].run_id,
        context["execution_date"].isoformat(),
        context.get("exception", "no exception captured"),
    )


_DEFAULT_ARGS: dict = {
    "owner": "ledgerlens",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": _on_failure,
}

with DAG(
    dag_id="ledgerlens_dbt",
    description="Run and test all dbt models on a schedule",
    schedule_interval="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=_DEFAULT_ARGS,
    tags=["dbt", "ledgerlens"],
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"dbt run {_DBT_FLAGS}",
        cwd=_DBT_DIR,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"dbt test {_DBT_FLAGS}",
        cwd=_DBT_DIR,
    )

    dbt_run >> dbt_test
