"""Orquestra o Job existente Malha DRE, somente por disparo manual."""

from datetime import datetime, timezone

from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator


with DAG(
    dag_id="pl_ambibar_JOB_DRE_DATABRICKS",
    description="Executa e acompanha o Job Databricks Malha DRE.",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["databricks", "teste"],
) as dag:
    executar_job = DatabricksRunNowOperator(
        task_id="executar_job_malha_dre",
        databricks_conn_id="databricks_dre",
        job_id=73441839188806,
        wait_for_termination=True,
        polling_period_seconds=30,
        retries=0,
    )
