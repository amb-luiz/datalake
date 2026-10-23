#!/bin/bash
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && cd .. && pwd)"
cd $PROJECT_DIR

rm -rf jupyter/artifact
mkdir jupyter/artifact 2> /dev/null

cp -r artifact/* jupyter/artifact/

docker build jupyter -t pyspark-library-jupyter