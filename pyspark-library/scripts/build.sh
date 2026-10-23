#!/bin/bash

echo "++++ Begin build.sh"
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && cd .. && pwd)"
cd $PROJECT_DIR

. .venv/bin/activate
pip install pip --upgrade
pip install wheel
python setup.py bdist_wheel

if ! [ -d spark-dist ] 
then
	mkdir spark-dist 
fi

if ! [ -d jars ] 
then
	mkdir jars 
fi

#generate requiriments wheels
mkdir tmp 2> /dev/null
cat requirements.txt | grep -v pyspark | grep -v boto > tmp/requirements.txt
cd dist  
pip wheel -r ../tmp/requirements.txt
cd ..

mkdir artifact 2> /dev/null
cp -r spark-dist artifact/

# botocore does not work with zip, whls and spark extrar files
ls ./dist/ | egrep .*\.whl | awk -F '.' 'BEGIN { OFS = "."} $0 !~ "^(boto)" {$NF="";print }' | while read file
do
	cp  dist/${file}whl artifact/spark-dist/${file}zip
done

cp -r dist artifact/python-dist

echo "++++ end build.sh"
