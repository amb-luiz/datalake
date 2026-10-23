"""Fourteen Excel extraction DAGs generated from the reviewed ADF copy specifications."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from cdl_planejamentocaixa.pipelines.parameters.ambipar_excel_sourcedata.transfer import (
    load_specs,
    run_copy,
)


for dag_id, copies in load_specs().items():
    with DAG(
        dag_id,
        description="Le Excel via SMB e substitui Parquet na pasta diaria do Azure raw/sourcedata.",
        start_date=datetime(2026, 9, 25),
        max_active_runs=1,
        concurrency=max(1, len(copies)),
        schedule_interval=None,
        catchup=False,
        is_paused_upon_creation=False,
        default_args={
            "owner": "airflow",
            "depends_on_past": False,
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 0,
            "retry_delay": timedelta(minutes=5),
        },
        tags=["ambipar", "excel", "sourcedata"],
        doc_md=(
            "Carga manual de Excel do compartilhamento SMB configurado na conexao "
            "`ambipar_excel_centro_de_custo_smb`. Usa as mesmas variaveis Azure "
            "das duas DAGs Excel existentes. Grava Parquet em "
            "`ambipar/datalake/delta/raw/sourcedata/<dataset>/AAAAMMDD/`. "
            "Cada task prepara o Parquet antes de apagar somente a pasta do dia "
            "de sua propria saida; uma nova execucao no mesmo dia substitui a anterior. "
            "As especificacoes de origem, aba e mapeamento estao em `specs.json`."
        ),
    ) as dag:
        for copy in copies:
            PythonOperator(
                task_id=copy["task_id"],
                python_callable=run_copy,
                op_kwargs={"dag_id": dag_id, "task_id": copy["task_id"]},
            )
    globals()[dag_id] = dag
