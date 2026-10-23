#!/bin/bash
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && cd .. && pwd)"

cd $PROJECT_DIR

if [ -z $PROJECT_NAME ]
then
    echo "PROJECT_NAME variable needed"
    exit 1
fi

if [ -z $ENV ]
then
    echo "ENV variable needed"
    exit 1
fi

if [ -z $REGION ]
then
    echo "REGION variable needed"
    exit 1
fi

if [ -z $ACCOUNT ]
then
    echo "ACCOUNT variable needed"
    exit 1
fi

echo "Upload config artifacts"
./scripts/get-config.sh

echo "Build Jupyter"
./scripts/build-jupyter.sh 
if [ $? -ne 0 ]
then
    echo "Error building Jupyter"
    exit 1
fi

docker ps | grep pyspark-library-jupyter-container 2> /dev/null > /dev/null
if [ $? -ne 0 ]
then
    docker kill pyspark-library-jupyter-container 2> /dev/null
    docker rm pyspark-library-jupyter-container 2> /dev/null
    docker run -d --name  pyspark-library-jupyter-container \
        -p 8080:8080 \
        -v ${PWD}/notebooks:/pyspark-library/notebooks \
        -v ${PWD}/data:/pyspark-library/data \
	    -v ${PWD}/metastore_db:/pyspark-library/notebooks/metastore_db \
        pyspark-library-jupyter
    sleep 10
fi

LOGS="`docker logs pyspark-library-jupyter-container 2>&1`"
echo "$LOGS"
echo
echo "$LOGS" | grep 127.0.0.1 | head -1 | awk '{print "##URL: " $(NF)}'
echo
echo "$LOGS" | grep token | head -1 |  awk -F "=" '{print "##Token: "$(NF)}'
