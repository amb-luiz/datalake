from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from cdl_planejamentocaixa.pipelines.parameters.ds_dl_ambipar_excel_centro_de_custo.common import DAG_NAME
from cdl_planejamentocaixa.pipelines.parameters.ds_dl_ambipar_excel_centro_de_custo.transfer import copy_excel_files


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    DAG_NAME,
    start_date=datetime(2026, 9, 8),
    max_active_runs=1,
    concurrency=1,
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
    tags=["ambipar", "excel", "sourcedata"],
    doc_md="""
    Le os arquivos `Centro de Custo -*.xlsx` de
    `QlikviewSAP/Arquivos_DRE` e grava um Parquet no container Azure `ambipar`,
    em `datalake/delta/raw/sourcedata/centro_de_custo/AAAAMMDD/centro_de_custo.parquet`.

    Execucao manual, com uma execucao ativa por vez. A pasta usa a data UTC
    no inicio da tarefa de copia, independentemente da data logica do Airflow.
    Uma nova execucao no mesmo dia substitui o Parquet e remove arquivos
    obsoletos nessa pasta, preservando as pastas de outros dias.

    Conexao SMB: `ambipar_excel_centro_de_custo_smb` (host, login, senha e porta).
    Azure: mesmas variaveis `AZURE_ADLS_ACCOUNT_NAME`, `AZURE_TENANT_ID`,
    `AZURE_CLIENT_ID` e `AZURE_CLIENT_SECRET` usadas pelas DAGs PRT.

    A ausencia de arquivos ou falha na leitura da origem interrompe a tarefa
    antes de alterar o destino. A substituicao ocorre arquivo a arquivo;
    se um upload falhar, reexecute a DAG para concluir a pasta do dia.
    """,
) as dag:
    t_start = DummyOperator(task_id="start")
    t_copy = PythonOperator(
        task_id="copy_excel_files_to_landing",
        python_callable=copy_excel_files,
    )
    t_end = DummyOperator(task_id="end")

    t_start >> t_copy >> t_end
