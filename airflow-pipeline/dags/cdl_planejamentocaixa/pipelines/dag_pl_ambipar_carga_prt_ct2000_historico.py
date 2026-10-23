"""Orquestra as cargas historicas de CT2000, de 2021 a 2025."""

from datetime import datetime, timezone

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.state import State
from airflow.utils.trigger_rule import TriggerRule


CHILD_DAG_IDS = tuple(
    f"pl_ambipar_carga_PRT_CT2000_REALIZADO_FULL_{year}"
    for year in range(2021, 2026)
)
TRIGGER_TASK_IDS = tuple(f"trigger_{child_dag_id.lower()}" for child_dag_id in CHILD_DAG_IDS)


def check_child_results(dag_run, **_):
    """Sinaliza falhas depois que todas as cargas anuais foram tentadas."""
    task_states = {task.task_id: task.state for task in dag_run.get_task_instances()}
    unsuccessful = [
        f"{task_id} ({task_states.get(task_id, 'missing')})"
        for task_id in TRIGGER_TASK_IDS
        if task_states.get(task_id) != State.SUCCESS
    ]
    if unsuccessful:
        raise AirflowException(
            "Cargas historicas CT2000 sem sucesso: " + ", ".join(unsuccessful)
        )


with DAG(
    dag_id="pl_ambipar_carga_PRT_CT2000_REALIZADO_HISTORICO",
    description="Executa as cargas CT2000 de 2021 a 2025 em sequencia.",
    start_date=datetime(2026, 10, 22, tzinfo=timezone.utc),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    is_paused_upon_creation=False,
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 0,
    },
    tags=["ambipar", "protheus", "ct2000", "historico"],
    doc_md="""
    Disparo manual das DAGs pl_ambipar_carga_PRT_CT2000_REALIZADO_FULL_2021
    ate pl_ambipar_carga_PRT_CT2000_REALIZADO_FULL_2025, nesta ordem.

    Aguarda cada DAG filha terminar antes de disparar a proxima, mesmo em caso
    de falha. A tarefa end consolida os resultados e falha se qualquer carga
    anual nao tiver sucesso.

    A data logica da orquestradora e repassada as filhas, mantendo a mesma
    subpasta de execucao AAAAMMDD para os cinco anos. As DAGs filhas precisam
    estar ativas (nao pausadas) no Airflow.
    """,
) as dag:
    previous_task = None
    for child_dag_id in CHILD_DAG_IDS:
        trigger_task = TriggerDagRunOperator(
            task_id=f"trigger_{child_dag_id.lower()}",
            trigger_dag_id=child_dag_id,
            execution_date="{{ execution_date }}",
            wait_for_completion=True,
            poke_interval=60,
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
