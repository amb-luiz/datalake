from pyspark.sql.functions import lit
from pyspark.sql import functions as f
from pyspark.sql.functions import *
import logging


def execute(spark, inputs, parameters=None, dynamic_parameters=None, write=True):

    df_bi_impeditivos = inputs['df_bi_impeditivos']

    logging.info("Executando transformações na tabela bi_impeditivos")
    
    df_bi_impeditivos = (df_bi_impeditivos.select(
        f.col("ID_CONTRATO_RAID").cast("string").alias("COD_CONTRATO_RAID"),
        f.col("MOTIVO").cast("string").alias("DSC_MOTIVO")
    ))
    
    return df_bi_impeditivos