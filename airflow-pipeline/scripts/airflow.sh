#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

#
# Run airflow command in container
#

#PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && cd .. && pwd)"

cd $PROJECT_DIR

if [ -z $CENTRAL_PROJECT_NAME ]
then
    echo "PROJECT_NAME variable needed"
    exit 1
fi

mkdir $PROJECT_DIR/logs 2> /dev/null
chmod 777 $PROJECT_DIR/logs

set -euo pipefail
SECRET_ENV_FILE="/opt/apps/secrets/airflow.env"
if [[ -f "$SECRET_ENV_FILE" ]]; then
    set -a
    # shellcheck disable=SC1090
    source "$SECRET_ENV_FILE"
    set +a
fi
export _AIRFLOW_WWW_USER_USERNAME=airflow
export _AIRFLOW_WWW_USER_PASSWORD=airflow
export AIRFLOW__PROJECT__NAME=$CENTRAL_PROJECT_NAME
export COMPOSE_FILE="${PROJECT_DIR}/docker-compose.yaml"
docker-compose build
if [ -z "${@}" ]
then
    docker-compose up airflow-init
    docker-compose run --rm airflow-worker airflow users create --password airflow --role Admin --username airflow --email EMAIL --firstname firstname --lastname lastname
    exec docker-compose up
else
    exec docker-compose run --rm -e CONNECTION_CHECK_MAX_COUNT=0 airflow-worker "${@}"
fi
