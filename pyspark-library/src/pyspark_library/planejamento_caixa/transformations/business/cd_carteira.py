from pyspark.sql import functions as f
from pyspark.sql.functions import *
import logging

def join_carteira(bi_carteira_cobranca):
    df_carteira_cobranca = bi_carteira_cobranca
    return df_carteira_cobranca

def join_negativados(bi_carteira_cobranca, bi_negativados):
    df_negativados = (bi_carteira_cobranca.join(bi_negativados,
                                  bi_carteira_cobranca['COD_CONTRATO'] == bi_negativados['COD_CONTRATO_NEGATIVADO'], "inner"))
    df_negativados = df_negativados.select(f.col('COD_CONTRATO_NEGATIVADO'),f.col('NOM_ACAO'))
    
    return df_negativados
    
def join_pep(bi_carteira_cobranca, bi_pep):
    df_pep = (bi_carteira_cobranca.join(bi_pep,
                                  bi_carteira_cobranca['COD_CPFCGC_PES'] == bi_pep['COD_RA_PEP'], "inner"))
    df_pep = df_pep.select(f.col('COD_RA_PEP'),f.col('DSC_PROD_PEP'))
    return df_pep
    
def join_pma(bi_carteira_cobranca, bi_pma):
    df_pma = (bi_carteira_cobranca.join(bi_pma,
                                  bi_carteira_cobranca['COD_CPFCGC_PES'] == bi_pma['COD_RA_PMA'], "inner"))
    df_pma = df_pma.select(f.col('COD_RA_PMA'),f.col('DSC_CLUSTER_PMA'),f.col('DSC_CD_STATUS_COBRANCA'))
    return df_pma

def join_isencao(bi_carteira_cobranca, isencao_raid):
    df_isencao = (bi_carteira_cobranca.join(isencao_raid,
                                  bi_carteira_cobranca['COD_CONTRATO'] == isencao_raid['COD_CONTRATO_ISENCAO'], "inner"))    
    df_isencao = df_isencao.select(f.col('COD_CONTRATO_ISENCAO'),f.col('DSC_MOTIVO_ISENCAO'))
    
    return df_isencao

def join_isencao_matricula_nao_paga(bi_carteira_cobranca, bi_isencao_matricula_nao_paga):
    df_isencao_matricula = (bi_carteira_cobranca.join(bi_isencao_matricula_nao_paga,
                                  bi_carteira_cobranca['COD_CPFCGC_PES'] == bi_isencao_matricula_nao_paga['COD_RA_IMNP'], "inner"))
    df_isencao_matricula = df_isencao_matricula.select(f.col('COD_CONTRATO').alias('COD_CONTRATO_MAT'),f.col('DSC_MOTIVO_MNP').alias('DSC_MOTIVO_MAT'))
    
    return df_isencao_matricula

def join_isencao_cmd(bi_carteira_cobranca, bi_isencao_credito_maior_debito):
    df_isencao_cmd = (bi_carteira_cobranca.join(bi_isencao_credito_maior_debito,
                                  bi_carteira_cobranca['COD_CPFCGC_PES'] == bi_isencao_credito_maior_debito['COD_RA_ICMD'], "inner"))
    df_isencao_cmd = df_isencao_cmd.select(f.col('COD_CONTRATO').alias('COD_CONTRATO_CRE'),f.col('STA_COBRANCA_CMD').alias('DSC_MOTIVO_CRE'))
    
    return df_isencao_cmd
    
def join_base_zeus(bi_carteira_cobranca, base_zeus):
    df_cd_zeus = (bi_carteira_cobranca.join(base_zeus,
                                  bi_carteira_cobranca['COD_CPFCGC_PES'] == base_zeus['COD_RA_ZEUS'], "inner"))
    df_cd_zeus = df_cd_zeus.select(f.col('COD_RA_ZEUS'),f.col('TIP_CURSO_ZEUS'),f.col('STA_OPERACAO_ZEUS'))
    
    return df_cd_zeus
    

def execute(spark, inputs, parameters=None, dynamic_parameters=None, write=True):
    bi_carteira_cobranca = inputs['bi_carteira_cobranca']
    bi_isencao_credito_maior_debito = inputs['bi_isencao_credito_maior_debito']
    base_zeus = inputs['base_zeus']
    bi_negativados = inputs['bi_negativados']
    bi_isencao_matricula_nao_paga = inputs['bi_isencao_matricula_nao_paga']
    bi_pep = inputs['bi_pep']
    bi_pma = inputs['bi_pma']
    isencao_raid = inputs['isencao_raid']

    df_carteira_cobranca = join_carteira(bi_carteira_cobranca)
    df_isencao_cmd = join_isencao_cmd(bi_carteira_cobranca, bi_isencao_credito_maior_debito)
    df_cd_zeus = join_base_zeus(bi_carteira_cobranca, base_zeus)
    df_negativados = join_negativados(bi_carteira_cobranca, bi_negativados)
    df_isencao_matricula = join_isencao_matricula_nao_paga(bi_carteira_cobranca, bi_isencao_matricula_nao_paga)
    df_pep = join_pep(bi_carteira_cobranca, bi_pep)
    df_pma = join_pma(bi_carteira_cobranca, bi_pma)
    df_isencao = join_isencao(bi_carteira_cobranca, isencao_raid)
    
    df_cd_carteira = (df_carteira_cobranca.join(df_cd_zeus,
                                  df_carteira_cobranca['COD_CPFCGC_PES'] == df_cd_zeus['COD_RA_ZEUS'], "left"))
                                  
                                  
   # df_isencao_total = df_isencao_matricula.union(df_isencao_cmd)

    #df_isencao_total = df_isencao_total.union(df_isencao)
 
    #df_isencao_total = df_isencao_total.select(f.col('COD_CONTRATO_ISENCAO'),f.col('DSC_MOTIVO'))
    
    df_cd_carteira = (df_cd_carteira.join(df_isencao,
                                  df_cd_carteira['COD_CONTRATO'] == df_isencao['COD_CONTRATO_ISENCAO'], "left"))
                                  
    df_cd_carteira = (df_cd_carteira.join(df_isencao_matricula,
                                  df_cd_carteira['COD_CONTRATO'] == df_isencao_matricula['COD_CONTRATO_MAT'], "left"))
    
    df_cd_carteira = (df_cd_carteira.join(df_isencao_cmd,
                                  df_cd_carteira['COD_CONTRATO'] == df_isencao_cmd['COD_CONTRATO_CRE'], "left"))
                                  
    df_cd_carteira = (df_cd_carteira.join(df_pep,
                                  df_cd_carteira['COD_CPFCGC_PES'] == df_pep['COD_RA_PEP'], "left"))
   
    df_cd_carteira = (df_cd_carteira.join(df_pma,
                                  df_cd_carteira['COD_CPFCGC_PES'] == df_pma['COD_RA_PMA'], "left"))
                                  

    df_cd_carteira = (df_cd_carteira.join(df_negativados,
                                  df_cd_carteira['COD_CONTRATO'] == df_negativados['COD_CONTRATO_NEGATIVADO'], "left"))
                                  
  
    df_cd_carteira = df_cd_carteira.dropDuplicates(['COD_CONTRATO'])
    
    
    
    df_cd_carteira = (df_cd_carteira
                        .withColumn("DSC_SITUACAO_COBRANCA", 
                            f.when(f.col("DSC_MOTIVO_MAT").isNotNull(), 'MATRICULA NAO PAGA')
                            .when((f.col("DSC_MOTIVO_ISENCAO").isNotNull()),f.col("DSC_MOTIVO_ISENCAO"))
                            .when((f.col("CLS_SITUACAO_CONTRATO_ALUNO").isin('12','14','17','20','22','23','33','36','37','Err')),"FORA COBRANCA SITUACAO ACADEMICA")
                            .when((f.col("DSC_MOTIVO_CRE").isNotNull()), 'CREDITO MAIOR QUE DEBITO')
                            .otherwise(f.lit('EM COBRANCA'))
                                    )
                     )
    
                                            
    #NEGATIVADO SERASA 
    df_cd_carteira =  (df_cd_carteira.withColumn("DSC_NEGATIVADO_SERASA", f.when(f.col("NOM_ACAO") == f.lit("Negativacao Serasa"),
                                                     f.lit("SIM"))
                                            .otherwise(
                                                f.lit("NAO")
                                            )))
                                            
    #NEGATIVADO BOA VISTA
    df_cd_carteira =  (df_cd_carteira.withColumn("DSC_NEGATIVADO_BOA_VISTA", f.when(f.col("NOM_ACAO") == f.lit("Negativacao Boa Vista"),
                                                     f.lit("SIM"))
                                            .otherwise(
                                                f.lit("NAO")
                                            )))
                                            
                                            
    df_cd_carteira = df_cd_carteira.dropDuplicates(['COD_CONTRATO'])
    
    df_cd_carteira = df_cd_carteira.dropDuplicates(['COD_CPFCGC_PES'])
    
    return df_cd_carteira