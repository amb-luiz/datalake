#!/bin/bash

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && cd .. && pwd)"
cd $PROJECT_DIR

if [ -z $PROJECT_NAME ]
then
    echo "PROJECT_NAME variable needed"
    exit 1
fi

if [ -z $CENTRAL_PROJECT_NAME ]
then
    echo "CENTRAL_PROJECT_NAME variable needed"
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

ARTIFACT_PREFIX=s3://$CENTRAL_PROJECT_NAME-$REGION-$ACCOUNT-$ENV-artifact


if ! [ -z "$AWS_PROFILE" ] 
then
	PROFILE="--profile $AWS_PROFILE"
fi

echo "Uploading plugins..."
mkdir plugins 2> /dev/null
cd plugins
aws s3 $PROFILE cp $ARTIFACT_PREFIX/airflow/plugins.zip .
if [ $? -ne 0 ]
then
    echo "Error uploading plugins..."
    exit 1
fi
unzip -o plugins.zip
rm plugins.zip

cd ..
echo "Uploading variables..."
aws s3 $PROFILE cp $ARTIFACT_PREFIX/airflow/dags/variables.json dags/
if [ $? -ne 0 ]
then
    echo "Error uploading plugins..."
    exit 1
fi

echo "Uploading dag sync variables..."
aws s3 $PROFILE cp $ARTIFACT_PREFIX/airflow/dags/dag_sync_variables.py dags/
if [ $? -ne 0 ]
then
    echo "Error uploading dag sync plugins..."
    exit 1
fi

