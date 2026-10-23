FROM pyspark-library

COPY ./requirements.txt /tmp/

RUN . /pyspark-library/.venv/bin/activate && \
    /pyspark-library/.venv/bin/python -m pip install -r /tmp/requirements.txt
RUN echo ". /pyspark-library/.venv/bin/activate" >> ~/.bashrc && \
    echo "alias pyspark=python" >> ~/.bashrc

ENV SHELL=/bin/bash

COPY ./artifact /pyspark-library/artifact 

ENV MAVEN_URL=https://repo1.maven.org/maven2

ENV AWS_JAVA_SDK_VERSION=1.11.375
ENV HADOOP_AWS_VERSION=3.2.0
ENV CALCITE_VERSION=1.27.0

ENV EXT_JAR_DIR=/pyspark-library/artifact/ext-jars/

RUN wget $MAVEN_URL/com/amazonaws/aws-java-sdk-bundle/$AWS_JAVA_SDK_VERSION/aws-java-sdk-bundle-$AWS_JAVA_SDK_VERSION.jar -P $EXT_JAR_DIR
RUN wget $MAVEN_URL/org/apache/hadoop/hadoop-aws/$HADOOP_AWS_VERSION/hadoop-aws-$HADOOP_AWS_VERSION.jar -P $EXT_JAR_DIR
RUN wget $MAVEN_URL/org/apache/calcite/calcite-core/$CALCITE_VERSION/calcite-core-$CALCITE_VERSION.jar -P $EXT_JAR_DIR

WORKDIR /pyspark-library/notebooks

ENV EXTRA_CONF="--conf spark.hadoop.hive.metastore.schema.verification=false --conf spark.hadoop.datanucleus.autoCreateSchema=true --conf spark.hadoop.datanucleus.fixedDatastore=false --conf spark.hadoop.datanucleus.schema.autoCreateTables=true --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.InstanceProfileCredentialsProvider --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem  --conf spark.driver.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4=true --conf spark.executor.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4=true --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"

COPY ./pyspark-library-jupyter.sh /pyspark-library/scripts/pyspark-library-jupyter.sh

ENV PYSPARK_DRIVER_PYTHON="jupyter"
ENV PYSPARK_DRIVER_PYTHON_OPTS="notebook --allow-root --ip 0.0.0.0 --port 8080"

CMD . /pyspark-library/.venv/bin/activate && /pyspark-library/scripts/pyspark-library-jupyter.sh /pyspark-library/artifact
                
