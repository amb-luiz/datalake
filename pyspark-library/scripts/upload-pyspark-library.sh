#!/bin/bash
set -x
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && cd .. && pwd)"

cd $PROJECT_DIR

if ! [ -z "$AWS_PROFILE" ]
then
	PROFILE="--profile $AWS_PROFILE"
fi

BUCKET_ARTIFACT=$1
if [ -z $BUCKET_ARTIFACT ]
then
    echo "[Bucket artifact] needed!"
    exit 1
fi  

aws s3 $PROFILE cp artifact/spark-dist/pyspark*.zip $BUCKET_ARTIFACT/spark-dist/pyspark_library.zip
if [ $? -ne 0 ]; then echo "Error uploading artifacts to S3!"; exit 1; fi;

aws s3 $PROFILE cp artifact/python-dist/pyspark*.whl $BUCKET_ARTIFACT/python-dist/pyspark_library-0.0.0-py3-none-any.whl
if [ $? -ne 0 ]; then echo "Error uploading artifacts to S3!"; exit 1; fi;

exit 0
