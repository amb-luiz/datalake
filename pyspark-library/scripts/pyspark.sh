#!/bin/bash
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && cd .. && pwd)"
cd $PROJECT_DIR

docker run -it --rm -e PYSPARK_DRIVER_PYTHON=python3 \
        -e PYSPARK_DRIVER_PYTHON_OPTS="" \
        --name  pyspark-library-pyspark$$ \
        -v ${PWD}/notebooks:/spark-pyutil/notebooks \
        -v ${PWD}/data:/spark-pyutil/data \
	-v ${PWD}/test/test_parms:/spark-pyutil/test/test_parms \
	-v ${PWD}/test/test_data/salts:/spark-pyutil/test/test_data/salts \
	-v ${PWD}/metastore_db:/spark-pyutil/notebooks/metastore_db \
        pyspark-library-jupyter
