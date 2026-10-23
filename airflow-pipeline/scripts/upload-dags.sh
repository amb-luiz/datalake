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


PROJECT_NAME=$2
if [ -z $PROJECT_NAME ]
then
    echo "[PROJECT_NAME] needed!"
    exit 1
fi 

PROJECT_NAME=$(echo "$PROJECT_NAME" | tr - _)

#VERSAO INDEPENDENTE POR PISTA
aws s3 $PROFILE rm $BUCKET_ARTIFACT/airflow/dags/$PROJECT_NAME/pipelines --recursive
if [ $? -ne 0 ]; then echo "Error uploading DAGS to S3!"; exit 1; fi;

aws s3 $PROFILE cp dags/$PROJECT_NAME/pipelines $BUCKET_ARTIFACT/airflow/dags/$PROJECT_NAME/pipelines/ --recursive
if [ $? -ne 0 ]; then echo "Error uploading pipelines parameters to S3!"; exit 1; fi;


# aws s3 $PROFILE rm $BUCKET_ARTIFACT/airflow/dags/pipelines --recursive
# if [ $? -ne 0 ]; then echo "Error uploading DAGS to S3!"; exit 1; fi;

# aws s3 $PROFILE cp dags/pipelines $BUCKET_ARTIFACT/airflow/dags/pipelines/ --recursive
# if [ $? -ne 0 ]; then echo "Error uploading pipelines parameters to S3!"; exit 1; fi;

exit 0