from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from constructors.pipeline_dag_creator import PipelineDagCreator
from cdl_planejamentocaixa.pipelines.parameters.pipeline_acqua_cotacoes.jobs import landing_jobs
from cdl_planejamentocaixa.pipelines.parameters.pipeline_acqua_cotacoes.pipeline_landing import pipeline


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    "Pipeline_ACQUA_COTACOES_LANDING",
    start_date=datetime(2026, 4, 29),
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
        pdc.add_job("Landing", landing_job, "PIPELINE_ACQUA_COTACOES")

    pipeline_task_group = pdc.create_pipeline_task_group()

    t_start >> pipeline_task_group >> t_end
