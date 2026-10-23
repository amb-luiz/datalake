from pyspark.sql.functions import lit
from pyspark.sql import functions as f
from pyspark.sql.functions import *
from pyspark.sql import Window
from pyspark.sql import types as T
import pandas as pd
import logging


def execute(spark, inputs, parameters=None, dynamic_parameters=None):
    
    df_bi_originais = inputs['bi_originais']
    df_ciclo_remat = inputs['agg_ciclo_remat']

   
   #-- 1� JOIN ORIGINAIS X CICLO REMAT
def join_remat(df_bi_originais, df_ciclo_remat):
    df = (df_bi_originais.filter(
                                (f.col('NOM_NATUREZA_TITULO') == 'MENSALIDADE') &
                                (f.col('IND_NATUREZA_REMATRICULA') == 'S' ) &
                                (f.col('NUM_ANO_COMPETENCIA') == f.year(f.current_date()))
                            ))
    
    df = df.withColumn("SISTEMA", when(f.substring(f.col('COD_CONTRATO_ORI'),1,6) == "OLIMPO","OLIMPO")
                                                        .when((f.substring(f.col('COD_CONTRATO_ORI'),1,9) == "COLABORAR"), "COLABORAR")
                                                        .otherwise(""))
    
    df = (df.join(df_ciclo_remat,
                                  (df['SISTEMA'] == df_ciclo_remat['NOM_CICLO']), "left"))
      
    
    return (df
            .drop(df_ciclo_remat['NOM_SISTEMA'])
            .drop(df_ciclo_remat['NUM_ANO'])
            .drop(df_ciclo_remat['NUM_SEMESTRE'])
            .drop(df_ciclo_remat['DAT_INICIO_REMAT'])
           )