from pyspark.sql import functions as f
from pyspark.sql.functions import *
import logging


def execute(spark, inputs, parameters=None, dynamic_parameters=None, write=True):

    df_bi_pep = inputs['df_bi_pep']

    logging.info("Executando transformações na tabela bi_pep")
    
    df_bi_pep = (df_bi_pep.select(
        f.col("UNIDADE").cast("string").alias("COD_UNIDADE"),
        f.col("COD_ALUNO").cast("string").alias("COD_ALUNO"),
        f.col("ESPCOD").cast("string").alias("COD_ESPCOD"),
        f.col("cpfcgc_pes".strip()).cast("string").alias("COD_RA_PEP"),
        f.col("BOLETAGEM").cast("string").alias("DSC_BOLETAGEM"),
        f.col("ORIGEM").cast("string").alias("DSC_ORIGEM"),
        f.col("SAFRA").cast("string").alias("DSC_SAFRA"),
        f.col("PROD".strip()).cast("string").alias("DSC_PROD_PEP"),
        f.col("SISTEMA").cast("string").alias("DSC_SISTEMA"),
        f.col("FLAG_CONTRATO").cast("string").alias("IND_CONTRATO"),
        f.col("PAGOU_ALGUM_TITULO_POS_FORMATURA").cast("string").alias("DSC_PAGOU_ALGUM_TITULO_POS_FORMATURA"),
        f.col("STATUS").cast("string").alias("STA_PEP"),
        f.col("DEFINICAO").cast("string").alias("DSC_DEFINICAO"),
        f.col("POSSUI_ACEITE").cast("string").alias("DSC_POSSUI_ACEITE"),
        f.col("MES_EVASAO").cast("string").alias("DSC_MES_EVASAO"),
        f.col("ASISITUACAO").cast("string").alias("DSC_ASISITUACAO"),
        f.col("MGP_EVADIDO").cast("string").alias("DSC_MGP_EVADIDO"),
        f.col("DT_PROCESSAMENTO").cast("string").alias("DAT_PROCESSAMENTO")
    ))
    
    df_bi_pep = (df_bi_pep.withColumn("DSC_PROD_PEP",f.concat(f.concat(f.col('DSC_PROD_PEP'),f.lit('-')),f.col('IND_CONTRATO'))))
    
    df_bi_pep = df_bi_pep.select(f.col('COD_RA_PEP'),f.col('DSC_PROD_PEP'))
    
    return df_bi_pep