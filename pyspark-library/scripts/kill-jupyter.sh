#!/bin/bash
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && cd .. && pwd)"
cd $PROJECT_DIR


docker kill pyspark-library-jupyter-container
docker rm pyspark-library-jupyter-container