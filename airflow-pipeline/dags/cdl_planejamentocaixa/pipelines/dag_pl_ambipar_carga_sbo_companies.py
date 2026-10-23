from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators.local_spark import LocalSparkJobOperator

from constructors.pipeline_dag_creator import PipelineDagCreator
from cdl_planejamentocaixa.pipelines.parameters.pl_ambipar_carga_sbo_companies.common import (
    COMPANIES,
)
from cdl_planejamentocaixa.pipelines.parameters.pl_ambipar_carga_sbo_companies.jobs import (
    build_landing_jobs,
)
from cdl_planejamentocaixa.pipelines.parameters.pl_ambipar_carga_sbo_companies.pipeline_landing import (
    build_pipeline,
)

SQLSERVER_JDBC_PACKAGE = "com.microsoft.sqlserver:mssql-jdbc:10.2.0.jre11"
HADOOP_AWS_PACKAGE = "org.apache.hadoop:hadoop-aws:3.2.0"
AWS_JAVA_BUNDLE_PACKAGE = "com.amazonaws:aws-java-sdk-bundle:1.11.901"
HADOOP_AZURE_PACKAGE = "org.apache.hadoop:hadoop-azure:3.2.0"
AZURE_STORAGE_PACKAGE = "com.microsoft.azure:azure-storage:8.6.6"


def _build_pyspark_submit_args():
    packages = [
        HADOOP_AWS_PACKAGE,
        AWS_JAVA_BUNDLE_PACKAGE,
        SQLSERVER_JDBC_PACKAGE,
        HADOOP_AZURE_PACKAGE,
        AZURE_STORAGE_PACKAGE,
    ]
    return f"--packages {','.join(packages)} pyspark-shell"

PYSPARK_PACKAGES = _build_pyspark_submit_args()


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}


def _create_company_dag(company):
    dag_name = company["dag_name"]
    pipeline = build_pipeline(dag_name)
    landing_jobs = build_landing_jobs(company)

    with DAG(
        dag_name,
        start_date=datetime(2026, 7, 13),
        max_active_runs=1,
        concurrency=1,
        schedule_interval=None,
        default_args=default_args,
        catchup=False,
    ) as dag:
        t_start = DummyOperator(task_id="start")
        t_end = DummyOperator(task_id="end")

        pdc = PipelineDagCreator(pipeline, dag)
        for landing_job in landing_jobs:
            pdc.add_job("Landing", landing_job, landing_job["DatasetName"])

        pipeline_task_group = pdc.create_pipeline_task_group()
        for task in dag.tasks:
            if not isinstance(task, LocalSparkJobOperator):
                continue
            if not task.task_id.startswith(f"{dag_name}."):
                continue
            task.script_args["--execution-datetime"] = "{{ ds_nodash }}"
            task.env_vars["PYSPARK_SUBMIT_ARGS"] = PYSPARK_PACKAGES
            task.env_vars["AZURE_ADLS_SECRET_TYPE"] = "oauth"

        t_start >> pipeline_task_group >> t_end

    return dag


for company in COMPANIES:
    globals()[company["dag_name"]] = _create_company_dag(company)
