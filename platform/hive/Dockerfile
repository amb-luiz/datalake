FROM apache/hive:4.0.1

USER root

ADD https://jdbc.postgresql.org/download/postgresql-42.7.5.jar /opt/hive/lib/postgresql-42.7.5.jar
ADD https://jdbc.postgresql.org/download/postgresql-42.7.5.jar /opt/hadoop/share/hadoop/common/lib/postgresql-42.7.5.jar

RUN cp /opt/hadoop/share/hadoop/tools/lib/hadoop-aws-3.3.6.jar /opt/hadoop/share/hadoop/common/lib/hadoop-aws-3.3.6.jar \
    && cp /opt/hadoop/share/hadoop/tools/lib/aws-java-sdk-bundle-1.12.367.jar /opt/hadoop/share/hadoop/common/lib/aws-java-sdk-bundle-1.12.367.jar \
    && cp /opt/hadoop/share/hadoop/tools/lib/hadoop-aws-3.3.6.jar /opt/hive/lib/hadoop-aws-3.3.6.jar \
    && cp /opt/hadoop/share/hadoop/tools/lib/aws-java-sdk-bundle-1.12.367.jar /opt/hive/lib/aws-java-sdk-bundle-1.12.367.jar \
    && chmod 644 /opt/hive/lib/postgresql-42.7.5.jar /opt/hadoop/share/hadoop/common/lib/postgresql-42.7.5.jar \
    /opt/hadoop/share/hadoop/common/lib/hadoop-aws-3.3.6.jar /opt/hadoop/share/hadoop/common/lib/aws-java-sdk-bundle-1.12.367.jar \
    /opt/hive/lib/hadoop-aws-3.3.6.jar /opt/hive/lib/aws-java-sdk-bundle-1.12.367.jar

USER hive
