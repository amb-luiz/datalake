from pyspark.sql import functions as f
from pyspark.sql.functions import *
from datetime import datetime
import logging

#spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")

def execute(spark, inputs, parameters=None, dynamic_parameters=None, write=True):

    df_bi_carteira_cobranca = inputs['df_bi_carteira_cobranca']

    logging.info("Executando transformações na tabela bi_carteira_cobranca")

    df_bi_carteira_cobranca = (df_bi_carteira_cobranca.select(
        f.col("ID_REGISTRO").cast("integer").alias("COD_REGISTRO"),
        trim(upper(f.col("TP_MATRICULA".strip()))).cast("string").alias("TIP_MATRICULA"),
        f.col("MARCA_CONTRATO").cast("string").alias("CLS_MARCA_CONTRATO"),
        f.col("ID_CLIENTE".strip()).cast("string").alias("COD_CLIENTE"),
        f.col("NM_CLIENTE").cast("string").alias("NOM_CLIENTE"),
        trim(upper(f.col("ID_CONTRATO".strip()))).cast("string").alias("COD_CONTRATO"),
        f.col("CPF_CNPJ".strip()).cast("string").alias("DSC_CPF_CNPJ"),
        f.col("CIDADE").cast("string").alias("LCL_CIDADE"),
        f.col("UF").cast("string").alias("EST_UF"),
        f.col("DT_NASCIMENTO").cast("string").alias("DAT_NASCIMENTO"),
        f.col("TP_CONTRATO").cast("string").alias("TIP_CONTRATO"),
        f.col("PF_CONTATO").cast("string").alias("DSC_PF_CONTATO"),
        f.col("ST_CONTATO").cast("string").alias("STA_ST_CONTATO"),
        f.col("LOGRADOURO").cast("string").alias("END_LOGRADOURO"),
        f.col("COMPLEMENTO").cast("string").alias("END_COMPLEMENTO"),
        f.col("BAIRRO").cast("string").alias("END_BAIRRO"),
        f.col("CEP").cast("string").alias("DSC_CEP"),
        f.col("EMAIL").cast("string").alias("EML_EMAIL"),
        f.col("TELEFONE_FIXO").cast("string").alias("DSC_TELEFONE_FIXO"),
        f.col("TELEFONE_CELULAR").cast("string").alias("DSC_TELEFONE_CELULAR"),
        f.col("TP_PESSOA").cast("string").alias("TIP_PESSOA"),
        f.col("DT_CADASTRO").cast("timestamp").alias("DAT_CADASTRO"),
        f.col("SEXO").cast("string").alias("DSC_SEXO"),
        f.col("RG").cast("string").alias("DSC_RG"),
        f.col("ESTADO_CIVIL").cast("string").alias("DSC_ESTADO_CIVIL"),
        f.col("NACIONALIDADE").cast("string").alias("NAC_NACIONALIDADE"),
        f.col("SITUACAO_CONTRATO").cast(
            "string").alias("CLS_SITUACAO_CONTRATO_ALUNO"),
        f.col("SERIE_ACADEMICA").cast("string").alias("NUM_SERIE_ACADEMICA"),
        f.col("CD_CURSO").cast("string").alias("COD_CD_CURSO"),
        f.col("NM_CURSO").cast("string").alias("NOM_CURSO"),
        f.col("TP_CURSO").cast("string").alias("TIP_CURSO"),
        trim(upper(f.col("CD_FILIAL".strip()))).cast("string").alias("COD_FILIAL"),
        f.col("DESC_FILIAL").cast("string").alias("DSC_FILIAL"),
        f.col("UF_FILIAL").cast("string").alias("EST_UF_FILIAL"),
        f.col("PRODUTO_PEP").cast("string").alias("CLS_PRODUTO_PEP"),
        f.col("IC_FNU").cast("string").alias("DSC_IC_FNU"),
        f.col("IC_FIES").cast("string").alias("DSC_IC_FIES"),
        f.col("DT_INICIO_CURSO").cast("string").alias("DAT_INICIO_CURSO"),
        f.col("DT_FIM_CURSO").cast("string").alias("DAT_FIM_CURSO"),
        f.col("DT_INICIO_MATRICULA").cast(
            "string").alias("DAT_INICIO_MATRICULA"),
        f.col("DT_FIM_MATRICULA").cast("string").alias("DAT_FIM_MATRICULA"),
        f.col("MODALIDADE_MATRICULA").cast(
            "string").alias("CLS_MODALIDADE_MATRICULA"),
        f.col("CNPJ_FILIAL").cast("string").alias("CGC_CNPJ_FILIAL"),
        f.col("ENDERECO_FILIAL").cast("string").alias("END_ENDERECO_FILIAL"),
        f.col("BAIRRO_FILIAL").cast("string").alias("DSC_BAIRRO_FILIAL"),
        f.col("CIDADE_FILIAL").cast("string").alias("LCL_CIDADE_FILIAL"),
        f.col("CEP_FILIAL").cast("string").alias("DSC_CEP_FILIAL"),
        f.col("DESC_MATRIZ").cast("string").alias("DSC_MATRIZ"),
        f.col("CNPJ_MATRIZ").cast("string").alias("CGC_CNPJ_MATRIZ"),
        f.col("ENDERECO_MATRIZ").cast("string").alias("END_ENDERECO_MATRIZ"),
        f.col("NM_RESPONSAVEL").cast("string").alias("NOM_NM_RESPONSAVEL"),
        f.col("CPF_RESPONSAVEL").cast("string").alias("DSC_CPF_RESPONSAVEL"),
        f.col("CLASSE_DEBITO").cast("string").alias("CLS_CLASSE_DEBITO"),
        f.col("SCORE_COLLECTION").cast("string").alias("CLS_SCORE_COLLECTION"),
        f.col("SCORE_GLOBAL").cast("string").alias("CLS_SCORE_GLOBAL"),
        f.col("SCORE_ENGAJAMENTO").cast(
            "string").alias("CLS_SCORE_ENGAJAMENTO"),
        f.col("DESC_CLUSTER").cast("string").alias("CLS_CLUSTER"),
        f.col("DESC_SITUACAO_CONTRATO").cast(
            "string").alias("DSC_SITUACAO_CONTRATO"),
        f.col("SALDO_CREDITO").cast("double").alias("VLR_SALDO_CREDITO"),
        f.col("SISTEMA_ORIGEM").cast("string").alias("ORI_SISTEMA_ORIGEM"),
        f.col("BOLETAGEM").cast("string").alias("DSC_BOLETAGEM"),
        f.col("ORIGEM").cast("string").alias("ORI_ORIGEM"),
        f.col("SAFRA").cast("string").alias("DSC_SAFRA"),
        f.col("IC_ASSINATURA_CONTRATO").cast(
            "string").alias("DSC_IC_ASSINATURA_CONTRATO"),
        f.col("IC_PGTO_TITULO_POS_FORMADO").cast(
            "string").alias("DSC_IC_PGTO_TITULO_POS_FORMADO"),
        f.col("ST_PEP").cast("string").alias("DSC_ST_PEP"),
        f.col("DEFINICAO_ACAO").cast("string").alias("STA_DEFINICAO_ACAO"),
        f.col("IC_ACEITE").cast("string").alias("DSC_IC_ACEITE"),
        f.col("MES_EVASAO").cast("string").alias("NUM_MES_EVASAO"),
        f.col("MGP_EVADIDO").cast("string").alias("DSC_MGP_EVADIDO"),
        f.col("ID_TITULO").cast("string").alias("COD_TITULO"),
        f.col("NO_TITULO").cast("string").alias("COD_NO_TITULO"),
        trim(upper(f.col("NATUREZA_TITULO"))).cast("string").alias("TIP_NATUREZA_TITULO"),
        trim(upper(f.col("DESC_NATUREZA_TITULO"))).cast(
            "string").alias("DSC_NATUREZA_TITULO"),
        f.col("MES_COMPETENCIA").cast("string").alias("NUM_MES_COMPETENCIA"),
        f.col("ANO_COMPETENCIA").cast("string").alias("NUM_ANO_COMPETENCIA"),
        f.col("VL_LIQUIDO".strip()).cast("double").alias("VLR_LIQUIDO"),
        f.col("VL_SEM_DESC_PONTUAL").cast("double").alias("VLR_SEM_PONTUAL"),
        f.to_date(trim(f.col("DT_VENCIMENTO")), "dd/MM/yyyy").alias("DAT_VENCIMENTO"),
        f.col("NO_PARCELA").cast("double").alias("NUM_NO_PARCELA"),
        f.col("ID_PARCELAMENTO".strip()).cast("string").alias("COD_PARCELAMENTO"),
        f.col("DT_EMISSAO").cast("string").alias("DAT_EMISSAO"),
        f.col("NATUREZA_REMATRICULA").cast(
            "string").alias("TIP_NATUREZA_REMATRICULA"),
        f.col("DT_PAGAMENTO").cast("string").alias("DAT_PAGAMENTO"),
        f.col("VL_PAGAMENTO").cast("double").alias("VLR_PAGAMENTO"),
        trim(upper(f.col("ST_TITULO"))).cast("string").alias("STA_TITULO"),
        f.col("NO_MANUTENCAO_NEGATIVACAO").cast(
            "string").alias("NUM_MANUTENCAO_NEGATIVACAO"),
        f.col("DT_MODIFICACAO_ORIGEM").cast(
            "timestamp").alias("DAT_MODIFICACAO_ORIGEM"),
        f.col("ST_PROCESSAMENTO").cast("integer").alias("STA_PROCESSAMENTO"),
        f.col("DT_CRIACAO").cast("string").alias("DAT_CRIACAO"),
        f.col("US_CRIACAO").cast("string").alias("NOM_US_CRIACAO"),
        f.col("DT_MODIFICACAO").cast("string").alias("DAT_MODIFICACAO"),
        f.col("US_MODIFICACAO").cast("string").alias("NOM_US_MODIFICACAO"),
        f.col("VL_TITULO").cast("double").alias("VLR_TITULO"),
        f.col("VL_PRINCIPAL").cast("double").alias("VLR_PRINCIPAL"),
        f.col("VL_DESC_PRINCIPAL").cast("double").alias("VLR_DESC_PRINCIPAL"),
        f.col("VL_MULTA").cast("double").alias("VLR_MULTA"),
        f.col("VL_DESC_MULTA").cast("double").alias("VLR_DESC_MULTA"),
        f.col("VL_JUROS_MORA").cast("double").alias("VLR_JUROS_MORA"),
        f.col("VL_DESC_JUROS_MORA").cast(
            "double").alias("VLR_DESC_JUROS_MORA"),
        f.col("VL_CORRECAO_MONETARIA").cast(
            "double").alias("VLR_CORRECAO_MONETARIA"),
        f.col("VL_DESC_CORRECAO_MONETARIA").cast(
            "double").alias("VLR_DESC_CORRECAO_MONETARIA"),
        f.col("FL_STATUS").cast("integer").alias("IND_STATUS"),
        f.to_date(trim(f.col("DT_PROCESSAMENTO")), "dd/MM/yyyy").alias("DAT_PROCESSAMENTO"),
        #date_format(f.col('DT_PROCESSAMENTO'),'MM/dd/yyyy').alias('DAT_PROCESSAMENTO'),
        f.col("AGENCIA_EXTERNA".strip()).cast("string").alias("NOM_AGENCIA_EXTERNA"),
        trim(upper(f.col("ID_CONTRATO_ORIGEM".strip()))).cast("string").alias("COD_CONTRATO_ORIGEM"),
        f.lit('1').alias('QDE_REGISTROS').cast('integer')
    ))
    
    df_bi_carteira_cobranca_total = df_bi_carteira_cobranca
    
    # filtro exclui titulos de remat 
    df_bi_carteira_cobranca = (df_bi_carteira_cobranca.filter((f.col('STA_TITULO') == 'ABERTO') &
                                                              #(f.to_date(f.col('DAT_PROCESSAMENTO'), 'dd/MM/yyyy') == f.current_date()) &
                                                              (f.to_date(f.col('DAT_VENCIMENTO'), 'dd/MM/yyyy') < f.current_date())))
                                                              
    df_bi_carteira_cobranca = (df_bi_carteira_cobranca.filter((~((f.col('TIP_NATUREZA_REMATRICULA').isin('S','O')) & (f.col('TIP_NATUREZA_TITULO') == 'MENSALIDADE')))))  
    
    #df_bi_carteira_cobranca = df_bi_carteira_cobranca.where(~ f.col('COD_CLIENTE').like('%1899%'))
    
    #df_bi_carteira_cobranca = df_bi_carteira_cobranca.where(~ f.col('COD_CLIENTE').like('%1900%'))
    
    #df alunos
    df_bi_alunos = df_bi_carteira_cobranca
    
    #INLCLUI COLUNA CPFCGC_PES
    df_bi_alunos =  (df_bi_alunos.withColumn("COD_CPFCGC_PES", f.when(f.col("TIP_MATRICULA") == f.lit("PRESENCIAL"),
                                                     f.concat(f.lpad(f.col("COD_FILIAL").cast('string'),3,'0'), f.substring(f.col('COD_CONTRATO_ORIGEM'), 2, 15)))
                                            .otherwise(
                                                f.concat(f.lit("EAD"), f.col("COD_CONTRATO_ORIGEM"))
                                            )))
                                         
    #group by
    df_bi_carteira_cobranca =  (df_bi_carteira_cobranca.groupBy('COD_CONTRATO').agg(f.sum(f.col('VLR_LIQUIDO').cast('float')).alias('VLR_SALDO_DEV_VENC'), (f.min(f.to_date(f.col('DAT_VENCIMENTO'), 'dd/MM/yyyy'))).alias('DAT_MENOR_VENCIMENTO'))) 
    
    #rename
    df_bi_carteira_cobranca = (df_bi_carteira_cobranca.select(f.col('COD_CONTRATO').alias('COD_ID_CONTRATO'),f.col('VLR_SALDO_DEV_VENC'),f.col('DAT_MENOR_VENCIMENTO')))
     
    #left join                                     
    df_bi_carteira_cobranca = (df_bi_carteira_cobranca.join(df_bi_alunos,df_bi_carteira_cobranca['COD_ID_CONTRATO'] == df_bi_alunos['COD_CONTRATO'], "left"))
   
    #dias atraso
    df_bi_carteira_cobranca = (df_bi_carteira_cobranca.withColumn("NUM_DIAS_ATRASO", 
                      f.floor(
                          f.datediff(f.current_date(), f.to_date(f.to_date("DAT_MENOR_VENCIMENTO")))
                      ))
                )
    #layout
    #distinct layout
    df_bi_carteira_cobranca = df_bi_carteira_cobranca.select(f.col('NOM_AGENCIA_EXTERNA'),f.col('DAT_PROCESSAMENTO'),f.col('CLS_MARCA_CONTRATO'),f.col('COD_FILIAL'),f.col('DSC_FILIAL'),f.col('CLS_SITUACAO_CONTRATO_ALUNO'),f.col('DSC_SITUACAO_CONTRATO'),f.col('COD_CLIENTE'),f.col('COD_CONTRATO'),f.col('COD_CONTRATO_ORIGEM'),f.col('COD_CPFCGC_PES'),f.col('NOM_CLIENTE'),f.col('CLS_SCORE_COLLECTION'),f.col('CLS_CLASSE_DEBITO'),f.col('CLS_CLUSTER'),f.col('CLS_SCORE_GLOBAL'),f.col('CLS_SCORE_ENGAJAMENTO'),f.col('DAT_INICIO_CURSO'),f.col('DAT_FIM_CURSO'),f.col('DAT_INICIO_MATRICULA'),f.col('DAT_FIM_MATRICULA'),f.col('DSC_CPF_CNPJ'),f.col('DSC_CPF_RESPONSAVEL'),f.col('COD_CD_CURSO'),f.col('TIP_CURSO'),f.col('NOM_CURSO'),f.col('TIP_CONTRATO'),f.col('NUM_SERIE_ACADEMICA'),f.col('LCL_CIDADE'),f.col('EST_UF'),f.col('DAT_NASCIMENTO'),f.col('END_LOGRADOURO'),f.col('END_COMPLEMENTO'),f.col('END_BAIRRO'),f.col('DSC_CEP'),f.col('EML_EMAIL'),f.col('DSC_TELEFONE_FIXO'),f.col('DSC_TELEFONE_CELULAR'),f.col('DSC_SEXO'),f.col('DSC_RG'),f.col('DSC_ESTADO_CIVIL'),f.col('NAC_NACIONALIDADE'),f.col('VLR_SALDO_DEV_VENC'),f.col('DAT_MENOR_VENCIMENTO'),f.col('NUM_DIAS_ATRASO'),f.col('COD_REGISTRO'),f.col('TIP_MATRICULA'),f.col('EST_UF_FILIAL'),f.col('CLS_PRODUTO_PEP'),f.col('END_ENDERECO_FILIAL'),f.col('DSC_BAIRRO_FILIAL'),f.col('LCL_CIDADE_FILIAL'),f.col('DSC_CEP_FILIAL'),f.col('DSC_MATRIZ'),f.col('CGC_CNPJ_MATRIZ'),f.col('END_ENDERECO_MATRIZ'))
    
    #retira duplicidades
    df_bi_carteira_cobranca = df_bi_carteira_cobranca.dropDuplicates(['COD_CONTRATO'])
    
    
    # ATRASO MENSALIDADES
    df_bi_carteira_cobranca_mens = (df_bi_carteira_cobranca_total.filter((((f.col('STA_TITULO') == 'ABERTO'))) &
                                                                  (f.col('TIP_NATUREZA_TITULO') == 'MENSALIDADE') 
                                                                  #(f.to_date(f.col('DAT_PROCESSAMENTO'), 'dd/MM/yyyy') == f.current_date()) 
                                                             ))
    														 
    														 
    df_bi_carteira_cobranca_mens = df_bi_carteira_cobranca_mens.groupBy(f.col('COD_CONTRATO').alias("COD_CONTRATO_MENS")) \
                                                                .agg(sum("VLR_LIQUIDO").alias("VLR_SALDO_MENSALIDADES"), \
                                                                     sum("QDE_REGISTROS").alias("QDE_TOTAL_MENSALIDADES"), \
                                                                     min("DAT_VENCIMENTO").alias("DAT_VENCIMENTO_MIN") \
                                                                 )
    # MENSALIDADES dias atraso
    df_bi_carteira_cobranca_mens = (df_bi_carteira_cobranca_mens.withColumn("NUM_AGING_ATRASO_MENSALIDADES", 
                          f.floor(
                              f.datediff(f.current_date(), f.to_date(f.to_date("DAT_VENCIMENTO_MIN")))
                          ))
                    )
    
    df_bi_carteira_cobranca_mens = df_bi_carteira_cobranca_mens.dropDuplicates(['COD_CONTRATO_MENS'])
    
    df_bi_carteira_cobranca = (df_bi_carteira_cobranca.join(df_bi_carteira_cobranca_mens,
                                      df_bi_carteira_cobranca['COD_CONTRATO'] == df_bi_carteira_cobranca_mens['COD_CONTRATO_MENS'], "left"))
    								  
    df_bi_carteira_cobranca = df_bi_carteira_cobranca.drop(f.col('COD_CONTRATO_MENS')) 
    
    df_bi_carteira_cobranca = df_bi_carteira_cobranca.drop(f.col('DAT_VENCIMENTO_MIN')) 
    
    
    # ATRASO ACORDOS
    df_bi_carteira_cobranca_acor = (df_bi_carteira_cobranca_total.filter((((f.col('STA_TITULO') == 'ABERTO'))) &
                                                                  (f.col('TIP_NATUREZA_TITULO').isin('ACORDO COB','ACORDO RAID','ACORDO OLIMPO','ACORDO COLAB')) 
                                                                  #(f.to_date(f.col('DAT_PROCESSAMENTO'), 'dd/MM/yyyy') == f.current_date()) 
                                                             ))
    														 
    df_bi_carteira_cobranca_acor = df_bi_carteira_cobranca_acor.groupBy(f.col('COD_CONTRATO').alias("COD_CONTRATO_ACOR")) \
                                                                .agg(sum("VLR_LIQUIDO").alias("VLR_SALDO_ACORDOS"), \
                                                                     sum("QDE_REGISTROS").alias("QDE_TOTAL_ACORDOS"), \
                                                                     min("DAT_VENCIMENTO").alias("DAT_VENCIMENTO_MIN") \
                                                                 )
    															 
    # ACORDOS dias atraso
    df_bi_carteira_cobranca_acor = (df_bi_carteira_cobranca_acor.withColumn("NUM_AGING_ATRASO_ACORDOS", 
                          f.floor(
                              f.datediff(f.current_date(), f.to_date(f.to_date("DAT_VENCIMENTO_MIN")))
                          ))
                    )
    				
    df_bi_carteira_cobranca_acor = df_bi_carteira_cobranca_acor.dropDuplicates(['COD_CONTRATO_ACOR'])
    
    df_bi_carteira_cobranca = (df_bi_carteira_cobranca.join(df_bi_carteira_cobranca_acor,
                                      df_bi_carteira_cobranca['COD_CONTRATO'] == df_bi_carteira_cobranca_acor['COD_CONTRATO_ACOR'], "left"))
    								
    df_bi_carteira_cobranca = df_bi_carteira_cobranca.drop(f.col('COD_CONTRATO_ACOR')) 
    
    df_bi_carteira_cobranca = df_bi_carteira_cobranca.drop(f.col('DAT_VENCIMENTO_MIN')) 
    
    # ATRASO TAXAS
    df_bi_carteira_cobranca_taxa = (df_bi_carteira_cobranca_total.filter((((f.col('STA_TITULO') == 'ABERTO'))) &
                                                                  (f.col('TIP_NATUREZA_TITULO').isin('SERVICO','SERVICO PARCELAMENTO')) 
                                                                  #(f.to_date(f.col('DAT_PROCESSAMENTO'), 'dd/MM/yyyy') == f.current_date()) 
                                                             ))
    														
    df_bi_carteira_cobranca_taxa = df_bi_carteira_cobranca_taxa.groupBy(f.col('COD_CONTRATO').alias("COD_CONTRATO_TAXA")) \
                                                                .agg(sum("VLR_LIQUIDO").alias("VLR_SALDO_TAXAS"), \
                                                                     sum("QDE_REGISTROS").alias("QDE_TOTAL_TAXAS"), \
                                                                     min("DAT_VENCIMENTO").alias("DAT_VENCIMENTO_MIN") \
                                                                 )
    
    # MENSALIDADES dias atraso
    df_bi_carteira_cobranca_taxa = (df_bi_carteira_cobranca_taxa.withColumn("NUM_AGING_ATRASO_TAXAS", 
                          f.floor(
                              f.datediff(f.current_date(), f.to_date(f.to_date("DAT_VENCIMENTO_MIN")))
                          ))
                    )
    
    df_bi_carteira_cobranca_taxa = df_bi_carteira_cobranca_taxa.dropDuplicates(['COD_CONTRATO_TAXA'])
    
    df_bi_carteira_cobranca = (df_bi_carteira_cobranca.join(df_bi_carteira_cobranca_taxa,
                                      df_bi_carteira_cobranca['COD_CONTRATO'] == df_bi_carteira_cobranca_taxa['COD_CONTRATO_TAXA'], "left"))
    								  
    df_bi_carteira_cobranca = df_bi_carteira_cobranca.drop(f.col('COD_CONTRATO_TAXA')) 
    
    df_bi_carteira_cobranca = df_bi_carteira_cobranca.drop(f.col('DAT_VENCIMENTO_MIN')) 
    
    return df_bi_carteira_cobranca