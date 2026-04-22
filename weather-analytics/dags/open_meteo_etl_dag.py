from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.feature_engineering import raw_rows_to_staging_dataframe
from src.open_meteo_client import fetch_daily_rows


DAG_ID = "open_meteo_etl_daily"


def _get_open_meteo_base_url() -> str:
    """
    Reads the HTTP Airflow Connection:
      Conn Id: open_meteo_api
      Either set Host to the full URL (https://api.open-meteo.com) or
      Host=api.open-meteo.com with Schema=https.
    """
    conn = BaseHook.get_connection("open_meteo_api")
    host = (conn.host or "").strip().rstrip("/")
    if host.startswith("http://") or host.startswith("https://"):
        return host
    schema = (conn.schema or "https").strip().rstrip(":/")
    host = host.lstrip("/")
    return f"{schema}://{host}"


def fetch_open_meteo_daily(**context) -> List[Dict[str, Any]]:
    """
    Calls Open-Meteo API for each configured location and returns daily data rows
    mapped to your Snowflake WEATHER_RAW_DAILY column names.
    """
    locations = json.loads(Variable.get("weather_locations"))
    history_days = int(Variable.get("weather_history_days"))  # you will set this to 60 in UI

    base_url = _get_open_meteo_base_url()
    rows = fetch_daily_rows(base_url, locations, history_days)

    context["ti"].xcom_push(key="raw_rows", value=rows)
    return rows


def upsert_weather_raw_to_snowflake(**context) -> None:
    """
    Loads fetched rows into Snowflake:
      - stage rows in a temp table
      - in one transaction: DELETE matching keys, then INSERT (idempotent; same idea as
        DELETE FROM raw WHERE date = %s, then INSERT, per run date / batch)
    Uses SQL transaction + try/except with rollback.
    """
    rows = context["ti"].xcom_pull(key="raw_rows", task_ids="fetch_open_meteo_daily")
    if not rows:
        return

    df = raw_rows_to_staging_dataframe(rows)

    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    conn = hook.get_conn()
    cur = conn.cursor()

    try:
        cur.execute("""
            CREATE OR REPLACE TEMP TABLE TMP_WEATHER_RAW_DAILY (
              LOCATION_NAME STRING,
              LATITUDE FLOAT,
              LONGITUDE FLOAT,
              DATE DATE,
              TEMP_MAX FLOAT,
              TEMP_MIN FLOAT,
              TEMP_MEAN FLOAT,
              PRECIP_MM FLOAT
            );
        """)

        cur.execute("BEGIN;")
        cur.execute("TRUNCATE TABLE TMP_WEATHER_RAW_DAILY;")

        insert_sql = """
            INSERT INTO TMP_WEATHER_RAW_DAILY
              (LOCATION_NAME, LATITUDE, LONGITUDE, DATE, TEMP_MAX, TEMP_MIN, TEMP_MEAN, PRECIP_MM)
            VALUES
              (%(LOCATION_NAME)s, %(LATITUDE)s, %(LONGITUDE)s, %(DATE)s, %(TEMP_MAX)s, %(TEMP_MIN)s, %(TEMP_MEAN)s, %(PRECIP_MM)s);
        """
        cur.executemany(insert_sql, df.to_dict("records"))

        delete_sql = """
            DELETE FROM WEATHER_RAW_DAILY t
            USING TMP_WEATHER_RAW_DAILY s
            WHERE t.LOCATION_NAME = s.LOCATION_NAME
              AND t.DATE = s.DATE;
        """
        cur.execute(delete_sql)

        insert_target_sql = """
            INSERT INTO WEATHER_RAW_DAILY
              (LOCATION_NAME, LATITUDE, LONGITUDE, DATE, TEMP_MAX, TEMP_MIN, TEMP_MEAN, PRECIP_MM, INGESTED_AT)
            SELECT
              s.LOCATION_NAME,
              s.LATITUDE,
              s.LONGITUDE,
              s.DATE,
              s.TEMP_MAX,
              s.TEMP_MIN,
              s.TEMP_MEAN,
              s.PRECIP_MM,
              CURRENT_TIMESTAMP()
            FROM TMP_WEATHER_RAW_DAILY s;
        """
        cur.execute(insert_target_sql)
        cur.execute("COMMIT;")

    except Exception as e:
        cur.execute("ROLLBACK;")
        raise e
    finally:
        cur.close()
        conn.close()


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["lab1", "open-meteo", "etl"],
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_open_meteo_daily",
        python_callable=fetch_open_meteo_daily,
    )

    load_task = PythonOperator(
        task_id="upsert_weather_raw_to_snowflake",
        python_callable=upsert_weather_raw_to_snowflake,
    )

    trigger_forecast = TriggerDagRunOperator(
        task_id="trigger_forecast_dag",
        trigger_dag_id="weather_forecast_daily",
        wait_for_completion=False,
    )

    fetch_task >> load_task >> trigger_forecast
