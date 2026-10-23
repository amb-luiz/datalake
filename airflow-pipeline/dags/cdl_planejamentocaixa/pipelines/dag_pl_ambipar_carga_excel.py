from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


CHILD_DAG_IDS = [
    "pl_ambipar_carga_excel_geral_versaoAnalise",
]


with DAG(
    "pl_ambipar_carga_excel",
    description="Executa a orquestracao Excel geral das 16 cargas e aguarda sua conclusao.",
    start_date=datetime(2026, 9, 11),
    max_active_runs=1,
    concurrency=1,
    schedule_interval=None,
    catchup=False,
    is_paused_upon_creation=False,
    default_args={"owner": "airflow", "depends_on_past": False, "retries": 0},
    doc_md="""
    Orquestracao manual: chama `pl_ambipar_carga_excel_geral_versaoAnalise`
    e aguarda o sucesso das 16 cargas Excel na sequencia configurada no mestre.
    """,
) as dag:
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    previous_task = start
    for child_dag_id in CHILD_DAG_IDS:
        trigger_task = TriggerDagRunOperator(
            task_id=f"trigger_{child_dag_id.lower()}",
            trigger_dag_id=child_dag_id,
            wait_for_completion=True,
            poke_interval=30,
            allowed_states=["success"],
            failed_states=["failed"],
        )
        previous_task >> trigger_task
        previous_task = trigger_task

    previous_task >> end
