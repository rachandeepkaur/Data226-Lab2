"""
Lab 2 — Run dbt models, tests, and snapshots against Lab 1 Snowflake tables.

Uses the same snowflake_default connection as Lab 1. Builds env vars for dbt from that connection
so passwords are not hard-coded in profiles.yml.
"""

from __future__ import annotations

import os
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator

_ROOT = Path(__file__).resolve().parent.parent
DBT_DIR = Path(os.environ.get("DBT_PROJECT_DIR", str(_ROOT / "dbt")))


def _snowflake_env_from_airflow() -> dict[str, str]:
    conn = BaseHook.get_connection("snowflake_default")
    extra = conn.extra_dejson or {}
    account = (extra.get("account") or "").strip()
    warehouse = (extra.get("warehouse") or "").strip()
    database = (extra.get("database") or "").strip()
    role = (extra.get("role") or "").strip() or "PUBLIC"
    schema = (conn.schema or "").strip()
    if not schema:
        raise ValueError("snowflake_default connection must set Schema (Snowflake schema for dbt + sources).")
    env = {
        "SNOWFLAKE_ACCOUNT": account,
        "SNOWFLAKE_USER": (conn.login or "").strip(),
        "SNOWFLAKE_PASSWORD": conn.password or "",
        "SNOWFLAKE_WAREHOUSE": warehouse,
        "SNOWFLAKE_DATABASE": database,
        "SNOWFLAKE_SCHEMA": schema,
        "SNOWFLAKE_ROLE": role,
        "DBT_PROFILES_DIR": str(DBT_DIR),
    }
    missing = [k for k, v in env.items() if k != "SNOWFLAKE_PASSWORD" and not v]
    if missing:
        raise ValueError(f"Missing snowflake_default / env fields required for dbt: {missing}")
    return env


def _run_dbt(args: list[str], **context) -> None:
    if not DBT_DIR.is_dir():
        raise FileNotFoundError(f"dbt project directory not found: {DBT_DIR}")

    profiles = DBT_DIR / "profiles.yml"
    if not profiles.is_file():
        raise FileNotFoundError(
            f"Missing {profiles}. Copy profiles.yml.example to profiles.yml "
            "or let Airflow inject only via env (dbt still needs profiles.yml with env_var placeholders)."
        )

    env = os.environ.copy()
    env.update(_snowflake_env_from_airflow())

    cmd = ["dbt", *args]
    try:
        subprocess.run(cmd, cwd=str(DBT_DIR), env=env, check=True)
    except subprocess.CalledProcessError:
        raise


def dbt_deps(**context) -> None:
    _run_dbt(["deps"], **context)


def dbt_run(**context) -> None:
    _run_dbt(["run"], **context)


def dbt_test(**context) -> None:
    _run_dbt(["test"], **context)


def dbt_snapshot(**context) -> None:
    _run_dbt(["snapshot"], **context)


with DAG(
    dag_id="dbt_weather_elt_daily",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["lab2", "dbt", "elt"],
) as dag:
    t_deps = PythonOperator(task_id="dbt_deps", python_callable=dbt_deps)
    t_run = PythonOperator(task_id="dbt_run", python_callable=dbt_run)
    t_test = PythonOperator(task_id="dbt_test", python_callable=dbt_test)
    t_snap = PythonOperator(task_id="dbt_snapshot", python_callable=dbt_snapshot)

    t_deps >> t_run >> t_test >> t_snap
