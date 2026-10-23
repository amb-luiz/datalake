from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from cdl_planejamentocaixa.pipelines.parameters.pl_ambipar_carga_excel_evento_x_cc.common import DAG_NAME
from cdl_planejamentocaixa.pipelines.parameters.pl_ambipar_carga_excel_evento_x_cc.transfer import copy_excel_to_parquet


with DAG(
    DAG_NAME,
    start_date=datetime(2026, 9, 11),
    max_active_runs=1,
    concurrency=1,
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
    tags=["ambipar", "excel", "parquet"],
    doc_md="""
    Le `Qlikview/Fontes de Dados/EventoXContaContabil.xlsx` via SMB, aba
    `EVENTO_CONTA`, com a primeira linha como cabecalho. Gera um unico Parquet
    com `CONTA_CONTABIL`, `NOME_EVENTO` e `EVENTO_ID` como strings, preservando
    zeros a esquerda existentes em celulas de texto e valores nulos.

    Destino: `datalake/delta/raw/sourcedata/ods_evento_x_conta_contabil/yyyyMMdd/`
    `evento_x_conta_contabil.parquet`. A particao usa a data UTC no inicio da tarefa.

    A tarefa `cd_evento_conta_contabil` baixa e valida o Excel, prepara o Parquet,
    apaga somente o conteudo da pasta diaria e faz Copy somente apos sucesso
    de todas as exclusoes. As etapas ficam na mesma tarefa para usar o mesmo
    arquivo temporario no worker. Falha de Delete impede o upload; falha de
    upload exige reexecucao para repor a particao. Outros dias sao preservados.

    Usa a conexao `ambipar_excel_centro_de_custo_smb` e as variaveis
    `AZURE_ADLS_ACCOUNT_NAME`, `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`,
    `AZURE_CLIENT_SECRET`. Sem agendamento automatico. No mestre
    `pl_ambipar_carga_excel_geral_versaoAnalise`, executa apos sucesso de centro de custo.
    """,
) as dag:
    start = DummyOperator(task_id="start")
    copy = PythonOperator(task_id="cd_evento_conta_contabil", python_callable=copy_excel_to_parquet)
    end = DummyOperator(task_id="end")
    start >> copy >> end
