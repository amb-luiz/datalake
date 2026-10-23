"""Executa a Malha DRE em sequência, mesmo após falhas, por disparo manual."""

from datetime import datetime, timezone

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.state import State
from airflow.utils.trigger_rule import TriggerRule


CHILD_DAG_IDS = (
    "pl_ambipar_carga_protheus",
    "pl_ambipar_carga_excel",
    "pl_ambipar_carga_SBO",
    "pl_ambibar_JOB_DRE_DATABRICKS",
)
TRIGGER_TASK_IDS = tuple(f"trigger_{child_dag_id.lower()}" for child_dag_id in CHILD_DAG_IDS)


def check_child_results(dag_run, **_):
    """Registra falhas somente depois que todas as etapas foram executadas."""
    task_states = {task.task_id: task.state for task in dag_run.get_task_instances()}
    unsuccessful = [
        f"{task_id} ({task_states.get(task_id, 'missing')})"
        for task_id in TRIGGER_TASK_IDS
        if task_states.get(task_id) != State.SUCCESS
    ]
    if unsuccessful:
        raise AirflowException("Etapas da Malha DRE sem sucesso: " + ", ".join(unsuccessful))


with DAG(
    dag_id="01_pl_ambipar_MALHA_DRE",
    description="Executa Protheus, Excel, SBO e o Job Malha DRE em sequência, mesmo após falhas.",
    start_date=datetime(2026, 9, 30, tzinfo=timezone.utc),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    is_paused_upon_creation=False,
    default_args={"owner": "airflow", "depends_on_past": False, "retries": 0},
    tags=["dre", "databricks"],
    doc_md="""
    Aguarda cada DAG filha terminar e dispara a próxima mesmo se a anterior falhar.
    Após a etapa Databricks, consolida os resultados e sinaliza falha na malha
    se alguma etapa não teve sucesso, sem impedir a execução das demais.
    """,
) as dag:
    previous_task = None
    for child_dag_id in CHILD_DAG_IDS:
        trigger_task = TriggerDagRunOperator(
            task_id=f"trigger_{child_dag_id.lower()}",
            trigger_dag_id=child_dag_id,
            wait_for_completion=True,
            poke_interval=30,
            allowed_states=["success"],
            failed_states=["failed"],
            trigger_rule=TriggerRule.ALL_DONE,
        )
        if previous_task is not None:
            previous_task >> trigger_task
        previous_task = trigger_task

    end = PythonOperator(
        task_id="end",
        python_callable=check_child_results,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    previous_task >> end
