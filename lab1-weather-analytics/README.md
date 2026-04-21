# Lab 1 — Weather Analytics

Airflow pipelines that ingest daily weather from [Open-Meteo](https://open-meteo.com/), load into Snowflake, run a simple rolling-mean forecast, and rebuild a final union table.

## Layout

| Path | Purpose |
|------|---------|
| `dags/` | Airflow DAG definitions |
| `sql/` | Snowflake DDL and reference MERGE/UNION scripts |
| `src/` | Shared Python helpers imported by the DAGs |
| `report/` | Lab write-up and screenshots |

## Airflow configuration

1. **Connection** `open_meteo_api`: set **Host** to `https://api.open-meteo.com` (no secret required for the public API).
2. **Connection** `snowflake_default`: Snowflake account, user, password, warehouse, database, schema, role as required by your environment.

   **Recommended (local):** do not paste passwords into chat. Copy `lab1-weather-analytics/.env.example` to `lab1-weather-analytics/.env`, fill in your values, then run:

   ```bash
   chmod +x scripts/load_snowflake_connection.sh
   ./scripts/load_snowflake_connection.sh
   ```

   This registers `snowflake_default` in your local `AIRFLOW_HOME` (see Local CLI test below).
3. **Variables** (JSON unless noted):
   - `weather_locations` — e.g. `[{"name": "San Jose", "lat": 37.34, "lon": -121.89}]`
   - `weather_history_days` — e.g. `60`
   - `weather_forecast_horizon_days` — e.g. `7`
   - `weather_target_metric` — `TEMP_MAX`

## Snowflake setup

Run `sql/01_create_tables.sql` in your target database/schema before the first DAG run.

## Deploying DAGs

Point Airflow’s `dags_folder` at `lab1-weather-analytics/dags` (or symlink/copy these files into your scheduler’s DAGs directory).

Ensure the scheduler can import the `src` package: set `PYTHONPATH` to the `lab1-weather-analytics` directory (parent of `src` and `dags`), or install the project as a package.

Example:

```bash
export PYTHONPATH="/path/to/lab1-weather-analytics:${PYTHONPATH}"
```

## DAGs

- **`open_meteo_etl_daily`** — daily schedule; fetches data, merges into `WEATHER_RAW_DAILY`, triggers the forecast DAG.
- **`weather_forecast_daily`** — on-demand (triggered); writes `WEATHER_FORECAST_DAILY` and rebuilds `WEATHER_FINAL_DAILY`.

## Python dependencies

Use `requirements.txt` in the same environment where Airflow runs (or bake into your Docker image).

## Local CLI test (optional)

From `lab1-weather-analytics/`, create a venv, install Airflow (see [official install docs](https://airflow.apache.org/docs/apache-airflow/stable/installation/installing-from-pypi.html)), then:

```bash
export AIRFLOW_HOME="$(pwd)/.airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$(pwd)/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES="False"
export PYTHONPATH="$(pwd)"
airflow db migrate
```

Add the Open-Meteo connection (HTTP: schema `https`, host `api.open-meteo.com`) and the Airflow Variables listed above. Define a **`snowflake_default`** connection with your account credentials, or the load and forecast tasks will not run.

Run a single task (no Snowflake needed):

```bash
airflow tasks test open_meteo_etl_daily fetch_open_meteo_daily 2026-04-21
```

Run an entire DAG (requires Snowflake):

```bash
airflow dags reserialize   # once: registers DAGs in the metadata DB so TriggerDagRunOperator can find them
airflow dags test open_meteo_etl_daily 2026-04-21
airflow dags test weather_forecast_daily 2026-04-21
```

Without `reserialize`, `open_meteo_etl_daily`’s last task may fail with `DagNotFound` when using only the CLI (no scheduler). A running Airflow scheduler keeps this in sync automatically.
