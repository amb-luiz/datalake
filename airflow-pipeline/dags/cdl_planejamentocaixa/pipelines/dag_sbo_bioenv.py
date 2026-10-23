from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators.local_spark import LocalSparkJobOperator

from constructors.pipeline_dag_creator import PipelineDagCreator
from cdl_planejamentocaixa.pipelines.parameters.sbo_bioenv.jobs import landing_jobs
from cdl_planejamentocaixa.pipelines.parameters.sbo_bioenv.pipeline_landing import (
    pipeline,
)

SQLSERVER_JDBC_PACKAGE = "com.microsoft.sqlserver:mssql-jdbc:10.2.0.jre11"
PYSPARK_PACKAGES = f"--packages org.apache.hadoop:hadoop-aws:3.2.0,com.amazonaws:aws-java-sdk-bundle:1.11.901,{SQLSERVER_JDBC_PACKAGE} pyspark-shell"


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}


dag = None
