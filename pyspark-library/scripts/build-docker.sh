#!/bin/bash

BUILD=test
CURRENT_UID=`id -u`
CURRENT_GID=`id -g`

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && cd .. && pwd)"

cd $PROJECT_DIR

rm -rf artifact container_data metastore_db 2> /dev/null

docker build . -t pyspark-library
if [ $? -ne 0 ]; then echo "Error building pyspark-library"; exit 1; fi;

mkdir artifact 2> /dev/null

docker run -v $PROJECT_DIR/artifact:/artifact pyspark-library bash -c "/bin/cp -r /pyspark-library/artifact /;chown -R $CURRENT_UID:$CURRENT_GID /artifact"
if [ $? -ne 0 ]; then echo "Error copping artifacts from pyspark-library from container!"; exit 1; fi;

find artifact/ -name "*aws*" -exec rm {} \;

exit 0
