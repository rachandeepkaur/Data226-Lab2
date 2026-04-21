#!/usr/bin/env bash
# Registers Airflow connection snowflake_default from weather-analytics/.env
set -euo pipefail

LAB_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$LAB_ROOT"

if [[ ! -f .env ]]; then
  echo "Missing $LAB_ROOT/.env — copy .env.example to .env and add your values." >&2
  exit 1
fi

# shellcheck source=/dev/null
set -a
source .env
set +a

for v in SNOWFLAKE_ACCOUNT SNOWFLAKE_USER SNOWFLAKE_PASSWORD SNOWFLAKE_WAREHOUSE SNOWFLAKE_DATABASE SNOWFLAKE_SCHEMA; do
  if [[ -z "${!v:-}" ]]; then
    echo "Set $v in .env" >&2
    exit 1
  fi
done

export AIRFLOW_HOME="${AIRFLOW_HOME:-$LAB_ROOT/.airflow}"
export AIRFLOW__CORE__DAGS_FOLDER="${AIRFLOW__CORE__DAGS_FOLDER:-$LAB_ROOT/dags}"
export PYTHONPATH="${PYTHONPATH:-$LAB_ROOT}"

AIRFLOW_BIN="${LAB_ROOT}/.venv/bin/airflow"
if [[ ! -x "$AIRFLOW_BIN" ]]; then
  AIRFLOW_BIN="$(command -v airflow)"
fi

# Metadata DB schema must match the installed Airflow (Airflow 3.x adds columns like connection.team_name).
# If migrate was skipped or the DB came from Airflow 2.x, `connections add` fails with "no such column: ...".
echo "Running: airflow db migrate (confirm with y if prompted) ..."
"$AIRFLOW_BIN" db migrate

EXTRA_JSON="$(
  python3 <<'PY'
import json, os
keys = {
    "account": "SNOWFLAKE_ACCOUNT",
    "warehouse": "SNOWFLAKE_WAREHOUSE",
    "database": "SNOWFLAKE_DATABASE",
}
extra = {k: os.environ[v] for k, v in keys.items() if os.environ.get(v)}
if os.environ.get("SNOWFLAKE_REGION"):
    extra["region"] = os.environ["SNOWFLAKE_REGION"]
if os.environ.get("SNOWFLAKE_ROLE"):
    extra["role"] = os.environ["SNOWFLAKE_ROLE"]
print(json.dumps(extra))
PY
)"

"$AIRFLOW_BIN" connections delete snowflake_default 2>/dev/null || true
"$AIRFLOW_BIN" connections add snowflake_default \
  --conn-type snowflake \
  --conn-login "$SNOWFLAKE_USER" \
  --conn-password "$SNOWFLAKE_PASSWORD" \
  --conn-schema "$SNOWFLAKE_SCHEMA" \
  --conn-extra "$EXTRA_JSON"

echo "Connection snowflake_default is configured (stored in $AIRFLOW_HOME)."
