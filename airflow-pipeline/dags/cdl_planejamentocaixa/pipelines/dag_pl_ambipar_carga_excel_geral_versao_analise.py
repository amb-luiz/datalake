from datetime import datetime

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.state import State
from airflow.utils.trigger_rule import TriggerRule

from cdl_planejamentocaixa.pipelines.parameters.ds_dl_ambipar_excel_centro_de_custo.common import DAG_NAME as CENTRO_DE_CUSTO_DAG
from cdl_planejamentocaixa.pipelines.parameters.pl_ambipar_carga_excel_evento_x_cc.common import DAG_NAME as EVENTO_DAG


CHILD_DAGS = [
    ("pl_ambipar_carga_excel_centro_de_custo", CENTRO_DE_CUSTO_DAG),
    ("pl_ambipar_carga_excel_evento_X_cc", EVENTO_DAG),
    ("pl_ambipar_carga_excel_grupo_controladoria_X_cc", "pl_ambipar_carga_excel_grupo_controladoria_X_cc"),
    ("pl_ambipar_carga_excel_plan_orc_versoes_copy1", "pl_ambipar_carga_excel_plan_orc_versoes_copy1"),
    ("pl_ambipar_carga_excel_plan_orcamentario_copy1", "pl_ambipar_carga_excel_plan_orcamentario_copy1"),
    ("pl_ambipar_carga_excel_po_depara_cc", "pl_ambipar_carga_excel_po_depara_cc"),
    ("pl_ambipar_carga_excel_po_depara_cc_frota_beneficios", "pl_ambipar_carga_excel_po_depara_cc_frota_beneficios"),
    ("pl_ambipar_carga_excel_protheus_po", "pl_ambipar_carga_excel_protheus_po"),
    ("pl_ambipar_carga_excel_dictionary_planorcamentario", "pl_ambipar_carga_excel_dictionary_planorcamentario"),
    ("pl_ambipar_carga_excel_empresa_eventos", "pl_ambipar_carga_excel_empresa_eventos"),
    ("pl_ambipar_carga_excel_po_lancamento_gerencial", "pl_ambipar_carga_excel_po_lancamento_gerencial"),
    ("pl_ambipar_carga_excel_desvio_manter_analitico", "pl_ambipar_carga_excel_desvio_manter_analitico"),
    ("pl_ambipar_carga_excel_plan_versoes_3ga", "pl_ambipar_carga_excel_plan_versoes_3ga"),
    ("pl_ambipar_carga_excel_realiza_moeda_5", "pl_ambipar_carga_excel_realiza_moeda_5"),
    ("pl_ambipar_carga_excel_status_fechamento", "pl_ambipar_carga_excel_status_fechamento"),
    ("pl_ambipar_carga_excel_fopag_copy1", "pl_ambipar_carga_excel_fopag_copy1"),
]


def check_child_results(dag_run, **_):
    child_task_ids = {task_name for task_name, _ in CHILD_DAGS}
    unsuccessful = [
        f"{task.task_id} ({task.state})"
        for task in dag_run.get_task_instances()
        if task.task_id in child_task_ids and task.state != State.SUCCESS
    ]
    if unsuccessful:
        raise AirflowException("Cargas Excel sem sucesso: " + ", ".join(unsuccessful))


with DAG(
    "pl_ambipar_carga_excel_geral_versaoAnalise",
    start_date=datetime(2026, 9, 11),
    max_active_runs=1,
    concurrency=1,
    schedule_interval=None,
    catchup=False,
    is_paused_upon_creation=False,
    default_args={"owner": "airflow", "depends_on_past": False, "retries": 0},
    tags=["ambipar", "excel", "master"],
    doc_md="""
    Mestre manual das 16 cargas Excel. Executa os filhos na ordem informada e
    aguarda a conclusao de cada filho antes do proximo, mesmo quando algum falha.
    Ao final, registra falha no mestre se houve carga sem sucesso.
    Cada saida grava Parquet em
    `ambipar/datalake/delta/raw/sourcedata/<dataset>/AAAAMMDD/`.
    A carga de centro de custo usa o ID existente `ds_dl_ambipar_excel_centro_de_custo`.
    As DAGs filhas devem estar despausadas para processar os disparos.
    """,
) as dag:
    start = DummyOperator(task_id="start")
    previous = start
    for task_name, child_dag_id in CHILD_DAGS:
        trigger = TriggerDagRunOperator(
            task_id=task_name,
            trigger_dag_id=child_dag_id,
            execution_date="{{ execution_date }}",
            wait_for_completion=True,
            poke_interval=30,
            allowed_states=["success"],
            failed_states=["failed"],
            trigger_rule=TriggerRule.ALL_DONE,
        )
        previous >> trigger
        previous = trigger
    end = PythonOperator(
        task_id="end",
        python_callable=check_child_results,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    previous >> end
