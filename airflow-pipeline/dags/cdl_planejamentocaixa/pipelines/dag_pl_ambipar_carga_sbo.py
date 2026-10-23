from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.state import State
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}


MAX_PARALLEL_DAGS = 2
EXCLUSIVE_DAGS = {
    "pl_ambipar_carga_SBO_EMERGENCIA",
    "pl_ambipar_carga_SBO_RESPONSE",
    "pl_ambipar_carga_SBO_MULTIFILIAL_3GA",
}


WAVES = [
    [
        "pl_ambipar_carga_SBO_WASTE",
        "pl_ambipar_carga_SBO_ECO_PRODUCTS",
    ],
    [
        "pl_ambipar_carga_SBO_BIOENV",
        "pl_ambipar_carga_SBO_CONTROLPAR",
    ],
    [
        "pl_ambipar_carga_SBO_CSAFETY",
        "pl_ambipar_carga_SBO_CTA",
    ],
    [
        "pl_ambipar_carga_SBO_CTANK",
        "pl_ambipar_carga_SBO_DRACARES",
    ],
    [
        "pl_ambipar_carga_SBO_EMERGENCIA",
    ],
    [
        "pl_ambipar_carga_SBO_EKMAN",
        "pl_ambipar_carga_SBO_EMERGENCIA_ACTIVES",
    ],
    [
        "pl_ambipar_carga_SBO_ES",
        "pl_ambipar_carga_SBO_GIRASSOL",
    ],
    [
        "pl_ambipar_carga_SBO_FENIX",
        "pl_ambipar_carga_SBO_FLYONE",
    ],
    [
        "pl_ambipar_carga_SBO_GEOCIENCIAS",
        "pl_ambipar_carga_SBO_GEOWEB",
    ],
    [
        "pl_ambipar_carga_SBO_INSURANCE",
    ],
    [
        "pl_ambipar_carga_SBO_JM_SOLUCOES",
        "pl_ambipar_carga_SBO_MDTECH",
    ],
    [
        "pl_ambipar_carga_SBO_MULTIFILIAL_3GA",
    ],
    [
        "pl_ambipar_carga_SBO_OGTEC",
        "pl_ambipar_carga_SBO_ORBITGEO",
    ],
    [
        "pl_ambipar_carga_SBO_PLIMSOL",
        "pl_ambipar_carga_SBO_RGSP",
    ],
    [
        "pl_ambipar_carga_SBO_RESPONSE",
    ],
    [
        "pl_ambipar_carga_SBO_3GA",
        "pl_ambipar_carga_SBO_RGCBA",
    ],
    [
        "pl_ambipar_carga_SBO_SEGMIR",
        "pl_ambipar_carga_SBO_SERMAGE",
    ],
    [
        "pl_ambipar_carga_SBO_WWCONTROL",
        "pl_ambipar_carga_SBO_ZENITH",
    ],
]


for exclusive_dag in EXCLUSIVE_DAGS:
    if sum(exclusive_dag in wave for wave in WAVES) != 1 or [exclusive_dag] not in WAVES:
        raise ValueError(f"{exclusive_dag} must run alone in one wave")

if any(len(wave) > MAX_PARALLEL_DAGS for wave in WAVES):
    raise ValueError(f"Each wave can trigger at most {MAX_PARALLEL_DAGS} DAGs")


TRIGGER_TASK_IDS = {
    f"wave_{idx}.trigger_{child_dag_id.lower()}"
    for idx, wave in enumerate(WAVES, start=1)
    for child_dag_id in wave
}


def check_child_results(dag_run, **_):
    task_states = {task.task_id: task.state for task in dag_run.get_task_instances()}
    unsuccessful = [
        f"{task_id} ({task_states.get(task_id, 'missing')})"
        for task_id in sorted(TRIGGER_TASK_IDS)
        if task_states.get(task_id) != State.SUCCESS
    ]
    if unsuccessful:
        raise AirflowException("Cargas SBO sem sucesso: " + ", ".join(unsuccessful))


with DAG(
    "pl_ambipar_carga_SBO",
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
    for idx, wave in enumerate(WAVES, start=1):
        with TaskGroup(group_id=f"wave_{idx}") as wave_group:
            for child_dag_id in wave:
                TriggerDagRunOperator(
                    task_id=f"trigger_{child_dag_id.lower()}",
                    trigger_dag_id=child_dag_id,
                    execution_date="{{ execution_date }}",
                    wait_for_completion=True,
                    poke_interval=60,
                    trigger_rule=TriggerRule.ALL_DONE,
                )

        previous_wave >> wave_group
        previous_wave = wave_group

    previous_wave >> t_end
