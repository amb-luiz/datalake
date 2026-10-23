from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from constructors.pipeline_dag_creator import PipelineDagCreator
from operators.local_spark import LocalSparkJobOperator

from cdl_planejamentocaixa.pipelines.parameters.pl_ambipar_carga_sbo_waste.common import DAG_ID
from cdl_planejamentocaixa.pipelines.parameters.pl_ambipar_carga_sbo_waste.jobs import landing_jobs
from cdl_planejamentocaixa.pipelines.parameters.pl_ambipar_carga_sbo_waste.pipeline_landing import pipeline


PYSPARK_PACKAGES = (
    "--packages org.apache.hadoop:hadoop-aws:3.2.0,"
    "com.amazonaws:aws-java-sdk-bundle:1.11.901,"
    "com.microsoft.sqlserver:mssql-jdbc:10.2.0.jre11,"
    "org.apache.hadoop:hadoop-azure:3.2.0,"
    "com.microsoft.azure:azure-storage:8.6.6 pyspark-shell"
)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    DAG_ID,
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
        task.script_args["--execution-datetime"] = "{{ ds_nodash }}"
        task.env_vars["PYSPARK_SUBMIT_ARGS"] = PYSPARK_PACKAGES
        task.env_vars["AZURE_ADLS_SECRET_TYPE"] = "oauth"

    t_start >> pipeline_task_group >> t_end
