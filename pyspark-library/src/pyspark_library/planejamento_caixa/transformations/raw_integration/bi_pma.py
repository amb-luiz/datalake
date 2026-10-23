from pyspark.sql import functions as f
import logging


def execute(spark, inputs, parameters=None, dynamic_parameters=None, write=True):

    df_bi_pma = inputs['df_bi_pma']

    logging.info("Executando transformações na tabela bi_pma")
    
    df_bi_pma = (df_bi_pma.select(
        f.col("cpf".strip()).cast("string").alias("cod_cpf"""),
        f.col("ra_cob".strip()).cast("string").alias("COD_RA_PMA"),
        f.col("semestre_aluno").cast("string").alias("dsc_semestre_aluno"),
        f.col("CLUSTER".strip()).cast("string").alias("DSC_CLUSTER_PMA"),
        f.col("estrategia").cast("string").alias("dsc_estrategia"),
        f.col("campanha_c").cast("string").alias("campanha_c"),
        f.col("campanha_c").cast("string").alias("campanha_s"),
        f.col("cod_empresa").cast("string").alias("cod_empresa"),
        f.col("zeus_operacao_nome_unidade").cast("string").alias("dsc_zeus_operacao_nome_unidade"),
        f.col("zeus_operacao_marca").cast("string").alias("dsc_zeus_operacao_marca"),
        f.col("modalidade").cast("string").alias("DSC_CD_STATUS_COBRANCA"),
        f.col("modalidade_ead").cast("string").alias("dsc_modalidade_ead"),
        f.col("regional").cast("string").alias("dsc_regional"),
        f.col("canal").cast("string").alias("nom_canal"),
        f.col("zeus_operacao_nome_curso").cast("string").alias("dsc_zeus_operacao_nome_curso"),
        f.col("zeus_operacao_descricao_situacao_academica").cast("string").alias("dsc_zeus_operacao_descricao_situacao_academica"),
        f.col("acordos_total").cast("string").alias("num_acordos_total"),
        f.col("saldo_divida").cast("string").alias("vlr__saldo_divida"),
        f.col("zeus_operacao_dias_atraso").cast("string").alias("num_zeus_operacao_dias_atraso"),
        f.col("zeus_operacao_nome_escritorio").cast("string").alias("dsc_zeus_operacao_nome_escritorio"),
        f.col("ID_CLIENTE").cast("string").alias("COD_ID_CLIENTE"),
        f.col("EMAT_CD").cast("string").alias("COD_EMAT_CD"),
        f.col("NM_CLIENTE").cast("string").alias("NOM_CLIENTE"),
        f.col("DT_PROCESSAMENTO").cast("string").alias("DAT_PROCESSAMENTO")
    ))
    
    df_bi_pma = df_bi_pma.select(f.col('COD_RA_PMA'),f.col('DSC_CLUSTER_PMA'),f.col('DSC_CD_STATUS_COBRANCA'))
    
    return df_bi_pma