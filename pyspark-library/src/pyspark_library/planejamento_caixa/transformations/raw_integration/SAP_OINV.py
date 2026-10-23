from pyspark.sql import functions as f
from pyspark.sql.functions import input_file_name, regexp_extract
import logging


def _ensure_execution_ts(df):
    if "execution_ts" in df.columns:
        return df
    return df.withColumn(
        "execution_ts",
        regexp_extract(input_file_name(), r"execution_ts=([^/]+)", 1),
    )


def _latest_execution_only(df):
    latest_execution = df.select(f.max("execution_ts").alias("execution_ts")).collect()[0]["execution_ts"]
    return df.filter(f.col("execution_ts") == f.lit(latest_execution))


def execute(spark, inputs, parameters=None, dynamic_parameters=None, write=True):
    df_sap_oinv = inputs["df_sap_oinv"]

    logging.info("Executando transformacoes na tabela SAP_OINV")

    df_sap_oinv = _ensure_execution_ts(df_sap_oinv)
    df_sap_oinv = _latest_execution_only(df_sap_oinv)

    df_sap_oinv = df_sap_oinv.select(
        f.col("COD_FILIAL").cast("string").alias("COD_FILIAL"),
        f.col("DocEntry").cast("int").alias("DocEntry"),
        f.col("Serial").cast("int").alias("Serial"),
        f.col("DocStatus").cast("string").alias("DocStatus"),
        f.col("DocNum").cast("int").alias("DocNum"),
        f.col("CANCELED").cast("string").alias("CANCELED"),
        f.col("DocTotal").cast("decimal(19,6)").alias("DocTotal"),
        f.col("DocDate").cast("timestamp").alias("DocDate"),
        f.col("CardCode").cast("string").alias("CardCode"),
        f.col("CardName").cast("string").alias("CardName"),
        f.col("TaxDate").cast("timestamp").alias("TaxDate"),
        f.col("Installmnt").cast("smallint").alias("Installmnt"),
        f.col("SlpCode").cast("int").alias("SlpCode"),
        f.col("ShipToCode").cast("string").alias("ShipToCode"),
        f.col("CtlAccount").cast("string").alias("CtlAccount"),
        f.col("U_statuscob").cast("string").alias("U_statuscob"),
        f.col("U_ObsCompl").cast("string").alias("U_ObsCompl"),
        f.col("TransID").cast("int").alias("TransID"),
        f.col("DocDueDate").cast("timestamp").alias("DocDueDate"),
        f.col("DraftKey").cast("int").alias("DraftKey"),
        f.col("U_addedIDContrato").cast("string").alias("U_addedIDContrato"),
        f.col("ObjType").cast("string").alias("ObjType"),
        f.col("Status_Cobranca").cast("string").alias("Status_Cobranca"),
        f.col("SeriesStr").cast("string").alias("SeriesStr"),
        f.col("execution_ts").cast("string").alias("execution_ts"),
    )

    return df_sap_oinv
