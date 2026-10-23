import pyspark
from pyspark.sql import functions as f
from pyspark.sql.functions import *

from pyspark.sql.types import StringType
from pyspark.sql import SparkSession
from pyspark.sql import Window

def execute(spark, inputs, parameters=None, dynamic_parameters=None, write=True):
    
    df_analitic_cd_carteira = inputs['cd_carteira']
    
   # df_analitic_cd_carteira = spark.read.parquet("s3a://cdl-planejamentocaixa-us-east-1-660894085742-dev-business/CD_CARTEIRA/part-00000-7b767c50-420f-496b-be2d-18ec6824f6be-c000.snappy.parquet", encoding='ISO-8859-1', header=True, sep=";")
    
    df_analitic_cd_carteira = df_analitic_cd_carteira.select('DAT_PROCESSAMENTO','DSC_SITUACAO_CONTRATO','TIP_CURSO','TIP_MATRICULA','VLR_SALDO_DEV_VENC').withColumn('VLR_SALDO_DEV_VENC',f.round(f.col('VLR_SALDO_DEV_VENC'),2)).withColumn('DATA',df_analitic_cd_carteira['DAT_PROCESSAMENTO'].cast(StringType())).filter(f.col('DATA').isNotNull()|f.col('DSC_SITUACAO_CONTRATO').isNotNull())
    
    df_analitic_cd_carteira = df_analitic_cd_carteira.groupBy('DATA','DSC_SITUACAO_CONTRATO','TIP_CURSO','TIP_MATRICULA').count().select('DATA','DSC_SITUACAO_CONTRATO',
                                         'TIP_CURSO','TIP_MATRICULA',
                         f.col('count').alias('FreqAb')).orderBy('DATA','DSC_SITUACAO_CONTRATO','TIP_CURSO','TIP_MATRICULA')
    
    pi = Window.partitionBy('DATA','TIP_CURSO','TIP_MATRICULA')
    
    df_analitic_cd_carteira = df_analitic_cd_carteira.select('DATA','DSC_SITUACAO_CONTRATO','TIP_CURSO','TIP_MATRICULA','FreqAb',f.sum('FreqAb').over(pi).alias('FreqRel')).orderBy('DATA','TIP_MATRICULA','DSC_SITUACAO_CONTRATO','TIP_CURSO')
    
    df_analitic_cd_carteira = df_analitic_cd_carteira.select('DATA','DSC_SITUACAO_CONTRATO','TIP_CURSO','TIP_MATRICULA','FreqAb','FreqRel',(f.round(f.col('FreqAb')/f.col('FreqRel')*100,1).alias('Percent')).cast("float")).distinct().orderBy('DATA','TIP_CURSO','TIP_MATRICULA','DSC_SITUACAO_CONTRATO')

    return df_analitic_cd_carteira