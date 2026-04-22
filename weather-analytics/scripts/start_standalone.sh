#!/usr/bin/env bash
# Start Airflow 3 standalone with the correct DAGs folder (project dags/, not AIRFLOW_HOME/dags).
set -euo pipefail
LAB_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$LAB_ROOT"
export AIRFLOW_HOME="${AIRFLOW_HOME:-$LAB_ROOT/.airflow}"
export AIRFLOW__CORE__DAGS_FOLDER="$LAB_ROOT/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES="False"
export PYTHONPATH="$LAB_ROOT"

AIRFLOW_BIN="${LAB_ROOT}/.venv/bin/airflow"
if [[ ! -x "$AIRFLOW_BIN" ]]; then
  AIRFLOW_BIN="$(command -v airflow)"
fi

echo "AIRFLOW_HOME=$AIRFLOW_HOME"
echo "DAGS_FOLDER=$AIRFLOW__CORE__DAGS_FOLDER"
exec "$AIRFLOW_BIN" standalone
