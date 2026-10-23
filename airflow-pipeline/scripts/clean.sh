#!/bin/bash

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && cd .. && pwd)"

cd $PROJECT_DIR

SECRET_ENV_FILE="/opt/apps/secrets/airflow.env"
if [[ -f "$SECRET_ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$SECRET_ENV_FILE"
  set +a
fi

docker-compose down
docker-compose rm
docker volume rm airflow-datapipeline_postgres-db-volume
docker volume rm airflow-pipeline_postgres-db-volume

sudo rm -rf logs