#!/bin/bash
set -x
DIRNAME=$(pwd)

if [ "$#" -ne 1 ] 
then
	echo "Illegal number of arguments"
	exit 1
fi

ARTIFACT_PREFIX=$1

PYTHONPATH=`ls -df $ARTIFACT_PREFIX/../.venv/lib/python3*/* | grep site-packages | sort -r | head -1`

#PYSPARK ENVS
#export PYSPARK_DRIVER_PYTHON="jupyter"
#export PYSPARK_DRIVER_PYTHON_OPTS="notebook --allow-root --ip 0.0.0.0 --port 8080"

SPARK_SUBMIT_OPTIONS="$PYTHONPATH/pyspark/bin/pyspark"

# botocore does not work with zip,whls and python files
PACKAGE=`ls $ARTIFACT_PREFIX/spark-dist/ | awk '$0 !~ "^boto" && $0 !~ "pyspark" && $0 ~ ".*\.zip$" { print prefix $NF }' prefix=$ARTIFACT_PREFIX/spark-dist/`
JARS=`ls $ARTIFACT_PREFIX/jars/ 2> /dev/null | awk '$0 ~ ".*\.jar$" { print prefix $NF }' prefix=$ARTIFACT_PREFIX/jars/`
EXT_JARS=`ls $ARTIFACT_PREFIX/ext-jars/ 2> /dev/null | awk '$0 ~ ".*\.jar$" { print prefix $NF }' prefix=$ARTIFACT_PREFIX/ext-jars/`

PYFILES=`echo $PACKAGE | tr -s ' ' ','`
JARS=`echo $JARS | tr -s ' ' ','`
EXT_JARS=`echo $EXT_JARS | tr -s ' ' ':'`

if [ -z $JARS ]
then
	SUBMIT_JARS=""
else
	SUBMIT_JARS="--jars $JARS"
fi
if [ -z $EXT_JARS ]
then
	SUBMIT_EXT_JARS=""
else
	SUBMIT_EXT_JARS="--conf spark.driver.extraClassPath=$EXT_JARS --conf spark.executor.extraClassPath=$EXT_JARS"
fi

$SPARK_SUBMIT_OPTIONS $EXTRA_CONF --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
	--py-files $PYFILES $SUBMIT_EXT_JARS $SUBMIT_JARS $SUBMIT_CONF

