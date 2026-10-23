from pyspark.sql import functions as f
from pyspark.sql.functions import *
import logging


def execute(spark, inputs, parameters=None, dynamic_parameters=None, write=True):

    df_bi_negativados = inputs['df_bi_negativados']

    logging.info("Executando transformações na tabela bi_negativados")

    df_bi_negativados = (df_bi_negativados.select(
        f.col("NOME_ACAO".strip()).cast("string").alias("NOM_ACAO"),
        f.col("ID_CLIENTE".strip()).cast("string").alias("COD_CLIENTE"),
        f.col("ID_CONTRATO".strip()).cast("string").alias("COD_CONTRATO_NEGATIVADO"),
        f.col("STATUS".strip()).cast("string").alias("STA_ACAO"),
        f.col("MANUAL".strip()).cast("string").alias("DSC_MANUAL"),
        f.col("DATA_EXECUCAO".strip()).cast("string").alias("DAT_EXECUCAO"),
        f.col("DT_PROCESSAMENTO".strip()).cast("string").alias("DAT_PROCESSAMENTO")
    ))
    
    #distinct layout
    df_bi_negativados = df_bi_negativados.select(f.col('COD_CONTRATO_NEGATIVADO'),f.col('NOM_ACAO'))

    
    return df_bi_negativados