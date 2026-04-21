# Weather analytics — unified Airflow deployment (Lab 1 + Lab 2)

Single project root for **all DAGs**: ETL/forecast (Lab 1) and **dbt ELT** (Lab 2). Point Airflow at **`weather-analytics/dags`** only — do not add `lab1-*` / `lab2-*` copies elsewhere or DAGs will load twice.

## Layout

| Path | Purpose |
|------|---------|
| `dags/` | **All** Python DAGs (`open_meteo_etl_daily`, `weather_forecast_daily`, `dbt_weather_elt_daily`) |
| `src/` | Shared helpers for Lab 1 DAGs |
| `sql/` | Snowflake DDL + reference SQL |
| `dbt/` | dbt project `weather_elt` (models, tests, snapshots) + `profiles.yml` (env vars only) |
| `scripts/` | Helper scripts (e.g. load Snowflake connection from `.env`) |
| `report/` | Lab write-up and screenshots |

## Install dependencies (Airflow + dbt)

Use one environment (venv, conda, or Docker image) for the process that runs **scheduler + workers**:

```bash
cd weather-analytics
python3 -m venv .venv
source .venv/bin/activate   # or .venv\Scripts\activate on Windows
pip install -r requirements.txt
```

**Docker:** add to `_PIP_ADDITIONAL_REQUIREMENTS` (or equivalent), for example:

`dbt-snowflake apache-airflow-providers-snowflake`

Confirm `dbt --version` works in the same shell/container as `airflow`.

## Airflow configuration

1. **`AIRFLOW__CORE__DAGS_FOLDER`** = absolute path to `weather-analytics/dags`.
2. **`PYTHONPATH`** = absolute path to **`weather-analytics`** (parent of `src` and `dags`).
3. **`DBT_PROJECT_DIR`** (optional) = absolute path to `weather-analytics/dbt`. Defaults to `<project_root>/dbt`. In Docker use `/opt/airflow/dbt` and mount `./dbt:/opt/airflow/dbt`.

### Connections and variables

- **`open_meteo_api`**: HTTP, schema `https`, host `api.open-meteo.com` (or full URL in host — see Lab 1 DAG).
- **`snowflake_default`**: Snowflake; **Schema** must be set (same schema as `WEATHER_*` tables and dbt sources).
- **Variables**: `weather_locations`, `weather_history_days`, `weather_forecast_horizon_days`, `weather_target_metric` (`TEMP_MAX`).

Register `snowflake_default` from `.env` locally:

```bash
cp .env.example .env   # if needed
chmod +x scripts/load_snowflake_connection.sh
./scripts/load_snowflake_connection.sh
```

## Docker (course pattern)

In `docker-compose`, alongside `dags`, `logs`, `plugins`:

```yaml
volumes:
  - ${AIRFLOW_PROJ_DIR:-.}/dags:/opt/airflow/dags
  - ${AIRFLOW_PROJ_DIR:-.}/dbt:/opt/airflow/dbt
environment:
  _PIP_ADDITIONAL_REQUIREMENTS: dbt-snowflake apache-airflow-providers-snowflake
```

Set in the container: `PYTHONPATH=/opt/airflow`, `DBT_PROJECT_DIR=/opt/airflow/dbt` (if your project root is mounted as `/opt/airflow` with `dags` + `dbt` under it — adjust to match your layout).

## DAGs (all visible in one UI)

| DAG ID | Role |
|--------|------|
| `open_meteo_etl_daily` | Open-Meteo → Snowflake; triggers `weather_forecast_daily` |
| `weather_forecast_daily` | Forecast + `WEATHER_FINAL_DAILY` union |
| `dbt_weather_elt_daily` | `dbt deps` → `run` → `test` → `snapshot` on Lab 1 tables |

Run Lab 1 first to populate Snowflake, then trigger **`dbt_weather_elt_daily`**.

## Airflow UI on localhost (Airflow 3.x)

Airflow **3.x** no longer has `airflow webserver`. Use one of these:

### Option A — `standalone` (simplest for class / local dev)

From `weather-analytics/` with your venv activated:

```bash
export AIRFLOW_HOME="$(pwd)/.airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$(pwd)/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES="False"
export PYTHONPATH="$(pwd)"
airflow standalone
```

- Keep this terminal open. On the **first** run, the log prints **`Password for user 'admin': ...`**. If it says the password was already generated, open **`$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated`** (under `.airflow/`) and use the value there; username is **`admin`**.
- Open the UI in a browser: **`http://localhost:8080`** (default port; see `airflow.cfg` → `[api]` `port` if you changed it).
- Stop with `Ctrl+C` in that terminal.

### Option B — API server + scheduler (multiple terminals)

```bash
# Terminal 1
export AIRFLOW_HOME=... PYTHONPATH=... AIRFLOW__CORE__DAGS_FOLDER=...
airflow api-server --port 8080

# Terminal 2
airflow scheduler

# Terminal 3
airflow dag-processor
```

You still need a valid auth setup (e.g. admin user created by `standalone` once, or your org’s auth). For most students, **Option A** is enough.

## Local CLI quick test

```bash
export AIRFLOW_HOME="$(pwd)/.airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$(pwd)/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES="False"
export PYTHONPATH="$(pwd)"
airflow db migrate
airflow dags reserialize
airflow dags list
```

## Snowflake

Run `sql/01_create_tables.sql` once in your database/schema before the first ETL run.

## BI (Lab 2)

Point Superset / Preset / Tableau at dbt mart **`mart_weather_daily_analytics`** in the same schema (after `dbt run`).

## Troubleshooting

### `sqlite3.OperationalError: no such column: connection.team_name`

Your `AIRFLOW_HOME/airflow.db` was almost certainly created with **Airflow 2.x**, but the CLI is now **Airflow 3.x** (which expects extra columns on the `connection` table).

1. From `weather-analytics/` with the same venv:

   ```bash
   export AIRFLOW_HOME="$(pwd)/.airflow"
   export AIRFLOW__CORE__DAGS_FOLDER="$(pwd)/dags"
   airflow db migrate
   ```

   Answer **`y`** if asked to confirm.

2. If migrate still errors or loops, reset local metadata (you will lose Airflow connections/variables in that SQLite file and must re-add them):

   ```bash
   rm -f .airflow/airflow.db
   airflow db migrate
   ./scripts/load_snowflake_connection.sh
   ```

   Re-create Variables (`weather_locations`, etc.) and the `open_meteo_api` connection if needed.

### `load_snowflake_connection.sh` runs `db migrate` first

The script now runs **`airflow db migrate`** before `connections add` so the schema stays aligned with your installed Airflow.

### UI shows only one DAG (e.g. only `dbt_weather_elt_daily`)

In **Airflow 3.x**, DAGs can be marked **`is_stale`** in the metadata DB until the DAG processor finishes parsing or you refresh serialization. The web UI often **hides stale DAGs**, so you may see fewer DAGs than `airflow dags list`.

**Fix (safe, run from `weather-analytics/` with venv + env vars set):**

```bash
export AIRFLOW_HOME="$(pwd)/.airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$(pwd)/dags"
export PYTHONPATH="$(pwd)"
airflow dags reserialize
```

Then **reload the browser** (hard refresh). You should see all DAG files under `dags/` (e.g. three: ETL, forecast, dbt).

Also confirm **`airflow dags list`** shows the same count; if it does but the UI does not, the issue is almost always stale flags or a UI filter—check any **stale / active** toggles in the DAG list.
