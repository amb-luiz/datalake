#!/bin/bash
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && cd .. && pwd)"
cd $PROJECT_DIR

rm -rf dist/* spark-dist/* build/* jars/* artifact/* jupyter/artifact/* 2> /dev/null 

python3 setup.py clean
