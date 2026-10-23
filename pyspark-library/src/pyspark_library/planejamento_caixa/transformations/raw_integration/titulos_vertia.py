from pyspark.sql import functions as f
from pyspark.sql.functions import *
import logging


def execute(spark, inputs, parameters=None, dynamic_parameters=None, write=True):

    df_titulos_vertia = inputs['df_titulos_vertia']

    logging.info("Executando transformações na tabela bi_titulos_vertia")
    
    df_titulos_vertia = (df_titulos_vertia.select(
        f.col("ID_TITULO").cast("string").alias("COD_TITULO"),
        f.col("ID_CONTRATO").cast("string").alias("COD_CONTRATO"),
        f.col("IC_CARTEIRAVERTIA").cast("string").alias("IND_CARTEIRAVERTIA"),
        f.col("DT_PAGAMENTO").cast("string").alias("DAT_PAGAMENTO"),
        f.col("ST_TITULO").cast("string").alias("STA_TITULO"),
        f.col("cpfcgc_pes").cast("string").alias("COD_CPFCGC_PES")
    ))

    return df_titulos_vertia