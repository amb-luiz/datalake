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
export AWS_DEFAULT_REGION=$REGION

BUCKET_ARTIFACT=s3://$PROJECT_NAME-$REGION-$ACCOUNT-$ENV-landing

aws s3 cp data/mock s3://$PROJECT_NAME-$REGION-$ACCOUNT-$ENV-landing/mock --recursive

