#!/usr/bin/env bash
# Trigger the csv_to_oracle_ingest DAG for a given dataset + config path via Airflow's REST
# API, reusing the exact /auth/token auth flow already proven in
# scripts/verify_environment.py (AIRFLOW_AUTH_TOKEN_URL / AIRFLOW_USER /
# AIRFLOW_PASSWORD -- same endpoint, same admin/admin credential, same payload
# shape, no new auth mechanism invented).
#
# Usage:
#   scripts/trigger_dag.sh <dataset> <config_path>
#
# Example:
#   scripts/trigger_dag.sh customers configs/datasets/customers.json
#   RUN_ID=$(scripts/trigger_dag.sh orders configs/datasets/orders.json)
#
# Prints the triggered dag_run_id to stdout (only), so a caller can capture it
# via command substitution. All other diagnostic output (the auth/trigger
# request URLs) goes to stderr.
set -euo pipefail

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <dataset> <config_path>" >&2
  exit 1
fi

DATASET="$1"
CONFIG_PATH="$2"

AIRFLOW_BASE_URL="${AIRFLOW_BASE_URL:-http://localhost:8080}"
AIRFLOW_AUTH_TOKEN_URL="${AIRFLOW_BASE_URL}/auth/token"
AIRFLOW_TRIGGER_URL="${AIRFLOW_BASE_URL}/api/v2/dags/csv_to_oracle_ingest/dagRuns"
AIRFLOW_USER="admin"
AIRFLOW_PASSWORD="admin"

echo "Requesting auth token: POST ${AIRFLOW_AUTH_TOKEN_URL}" >&2
JWT_TOKEN=$(curl -s -X POST "${AIRFLOW_AUTH_TOKEN_URL}" \
  -H "Content-Type: application/json" \
  -d "{\"username\": \"${AIRFLOW_USER}\", \"password\": \"${AIRFLOW_PASSWORD}\"}" \
  | jq -r '.access_token')

if [ -z "${JWT_TOKEN}" ] || [ "${JWT_TOKEN}" = "null" ]; then
  echo "ERROR: failed to obtain access_token from ${AIRFLOW_AUTH_TOKEN_URL}" >&2
  exit 1
fi

echo "Triggering DAG run: POST ${AIRFLOW_TRIGGER_URL}" >&2
# Airflow 3.3.1's TriggerDAGRunPostBody schema marks logical_date as a
# required (but nullable) field -- passing an explicit null lets Airflow
# auto-assign the trigger time, matching UI/CLI-triggered runs.
DAG_RUN_ID=$(curl -s -X POST "${AIRFLOW_TRIGGER_URL}" \
  -H "Authorization: Bearer ${JWT_TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{\"conf\": {\"dataset\": \"${DATASET}\", \"config_path\": \"${CONFIG_PATH}\"}, \"logical_date\": null}" \
  | jq -r '.dag_run_id')

if [ -z "${DAG_RUN_ID}" ] || [ "${DAG_RUN_ID}" = "null" ]; then
  echo "ERROR: failed to obtain dag_run_id from ${AIRFLOW_TRIGGER_URL}" >&2
  exit 1
fi

echo "${DAG_RUN_ID}"
