"""Manual Datanomik cash-flow ETL; requires the isolated Airflow 3 runtime."""

import os
from datetime import timedelta

import pendulum
from airflow.exceptions import AirflowFailException
from airflow.sdk import Param, dag, get_current_context, task

CONCURRENCY = int(os.getenv("DATANOMIK_ACCOUNT_CONCURRENCY", "3"))
if CONCURRENCY < 1:
    raise ValueError("DATANOMIK_ACCOUNT_CONCURRENCY must be positive")


def _execute(function, *args, **kwargs):
    # Imports and credential reads occur only inside task execution.
    from datanomik_client import CredentialsError, DatanomikError

    try:
        return function(*args, **kwargs)
    except CredentialsError as error:
        raise AirflowFailException(str(error)) from None
    except DatanomikError as error:
        if error.permanent:
            raise AirflowFailException(str(error)) from None
        raise


@dag(
    dag_id="pl_ambipar_Datanomik_Fluxo_Caixa",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Sao_Paulo"),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    max_active_tasks=CONCURRENCY,
    default_args={
        "owner": "airflow", "retries": 2,
        "retry_delay": timedelta(minutes=1), "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=15), "execution_timeout": timedelta(hours=6),
    },
    params={
        "account_ids": Param(None, type=["null", "array"], items={"type": "string", "minLength": 1}),
        "cnpj": Param(None, type=["null", "string"]),
        "date_from": Param(None, type=["null", "string"], pattern=r"^\d{4}-\d{2}-\d{2}$"),
        "date_to": Param(None, type=["null", "string"], pattern=r"^\d{4}-\d{2}-\d{2}$"),
    },
    tags=["ambipar", "datanomik", "fluxo_caixa"],
    doc_md="""Carga manual. JSONL bruto, Parquet tratado e data_quality.json em volume
    persistente. Deduplicação global por external_id + account.id após todas as extrações.
    Duas retentativas além da tentativa inicial; erros HTTP permanentes falham sem retry.
    Não copiar este DAG para o Airflow legado 2.2.2.""",
)
def datanomik_transactions_etl():
    @task(multiple_outputs=True, show_return_value_in_logs=False)
    def selecionar_contas():
        from datanomik_pipeline import prepare_run

        context = get_current_context()
        logical_date = context.get("logical_date") or context["dag_run"].run_after
        return _execute(prepare_run, context["params"], logical_date, context["run_id"])

    @task(max_active_tis_per_dag=CONCURRENCY, show_return_value_in_logs=False)
    def extrair_conta(account_id, run_dir):
        from datanomik_pipeline import extract_account

        return _execute(extract_account, account_id, run_dir)

    @task(trigger_rule="none_failed", show_return_value_in_logs=False)
    def tratar_e_publicar(run_dir, manifests):
        from datanomik_pipeline import transform_run

        return transform_run(run_dir, manifests)

    selection = selecionar_contas()
    manifests = extrair_conta.partial(run_dir=selection["run_dir"]).expand(account_id=selection["accounts"])
    tratar_e_publicar(selection["run_dir"], manifests)


dag = datanomik_transactions_etl()
