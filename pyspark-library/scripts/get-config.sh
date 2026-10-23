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

ARTIFACT_PREFIX=s3://$PROJECT_NAME-$REGION-$ACCOUNT-$ENV-artifact

if ! [ -z "$AWS_PROFILE" ] 
then
	PROFILE="--profile $AWS_PROFILE"
fi

echo "Uploading artifacts..."
mkdir -p artifact/spark-dist 2> /dev/null
mkdir -p artifact/ext-jars 2> /dev/null
aws s3 $PROFILE cp $ARTIFACT_PREFIX/spark-pyutil/spark-dist artifact/spark-dist/ --recursive
aws s3 $PROFILE cp $ARTIFACT_PREFIX/spark-pyutil/ext-jars artifact/ext-jars/ --recursive
if [ $? -ne 0 ]
then
    echo "Error uploading artifacts..."
    exit 1
fi
