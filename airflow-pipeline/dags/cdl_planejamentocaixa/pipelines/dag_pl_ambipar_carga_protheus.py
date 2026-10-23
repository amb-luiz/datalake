from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.state import State
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from cdl_planejamentocaixa.pipelines.parameters.pl_ambipar_carga_prt_queries.common import (
    QUERY_SPECS,
)


# As cargas historicas pesadas ficam fora da orquestracao rotineira.
HISTORICAL_CT2000_DAG_IDS = {
    f"pl_ambipar_carga_PRT_CT2000_REALIZADO_FULL_{year}"
    for year in range(2020, 2026)
}
CATALOG_DAG_IDS = [
    f"pl_ambipar_carga_PRT_{spec['folder_name']}" for spec in QUERY_SPECS
]
CHILD_DAG_IDS = [
    dag_id for dag_id in CATALOG_DAG_IDS
    if dag_id not in HISTORICAL_CT2000_DAG_IDS
]
MAX_PARALLEL_DAGS = 3
# Preserve os numeros das ondas e os task_ids das cargas que permanecem.
WAVES = [
    [
        dag_id for dag_id in CATALOG_DAG_IDS[index:index + MAX_PARALLEL_DAGS]
        if dag_id in CHILD_DAG_IDS
    ]
    for index in range(0, len(CATALOG_DAG_IDS), MAX_PARALLEL_DAGS)
]
TRIGGER_TASK_IDS = {
    f"wave_{index}.trigger_{child_dag_id.lower()}"
    for index, wave in enumerate(WAVES, start=1)
    for child_dag_id in wave
}

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}


def check_child_results(dag_run, **_):
    task_states = {task.task_id: task.state for task in dag_run.get_task_instances()}
    unsuccessful = [
        f"{task_id} ({task_states.get(task_id, 'missing')})"
        for task_id in sorted(TRIGGER_TASK_IDS)
        if task_states.get(task_id) != State.SUCCESS
    ]
    if unsuccessful:
        raise AirflowException("Cargas Protheus sem sucesso: " + ", ".join(unsuccessful))


with DAG(
    "pl_ambipar_carga_protheus",
    description="Dispara cargas PRT em ondas de ate tres DAGs paralelas.",
    start_date=datetime(2026, 7, 13),
    max_active_runs=1,
    concurrency=MAX_PARALLEL_DAGS,
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
) as dag:
    t_start = DummyOperator(task_id="start")
    t_end = PythonOperator(
        task_id="end",
        python_callable=check_child_results,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    previous_wave = t_start
    for index, wave in enumerate(WAVES, start=1):
        if not wave:
            continue
        with TaskGroup(group_id=f"wave_{index}") as wave_group:
            for child_dag_id in wave:
                TriggerDagRunOperator(
                    task_id=f"trigger_{child_dag_id.lower()}",
                    trigger_dag_id=child_dag_id,
                    execution_date="{{ execution_date }}",
                    wait_for_completion=True,
                    poke_interval=60,
                    allowed_states=["success"],
                    failed_states=["failed"],
                    trigger_rule=TriggerRule.ALL_DONE,
                )

        previous_wave >> wave_group
        previous_wave = wave_group

    previous_wave >> t_end
