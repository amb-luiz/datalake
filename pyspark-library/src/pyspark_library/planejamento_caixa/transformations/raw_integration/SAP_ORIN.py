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
    df_sap_orin = inputs["df_sap_orin"]

    logging.info("Executando transformacoes na tabela SAP_ORIN")

    df_sap_orin = _ensure_execution_ts(df_sap_orin)
    df_sap_orin = _latest_execution_only(df_sap_orin)

    df_sap_orin = df_sap_orin.select(
        f.col("COD_FILIAL").cast("string").alias("COD_FILIAL"),
        f.col("DocEntry").cast("int").alias("DocEntry"),
        f.col("DocNum").cast("int").alias("DocNum"),
        f.col("DocType").cast("string").alias("DocType"),
        f.col("CANCELED").cast("string").alias("CANCELED"),
        f.col("DocStatus").cast("string").alias("DocStatus"),
        f.col("InvntSttus").cast("string").alias("InvntSttus"),
        f.col("Transfered").cast("string").alias("Transfered"),
        f.col("ObjType").cast("string").alias("ObjType"),
        f.col("DocDate").cast("timestamp").alias("DocDate"),
        f.col("DocDueDate").cast("timestamp").alias("DocDueDate"),
        f.col("CardCode").cast("string").alias("CardCode"),
        f.col("CardName").cast("string").alias("CardName"),
        f.col("Address").cast("string").alias("Address"),
        f.col("DocCur").cast("string").alias("DocCur"),
        f.col("DocRate").cast("decimal(19,6)").alias("DocRate"),
        f.col("DocTotal").cast("decimal(19,6)").alias("DocTotal"),
        f.col("DocTotalFC").cast("decimal(19,6)").alias("DocTotalFC"),
        f.col("PaidToDate").cast("decimal(19,6)").alias("PaidToDate"),
        f.col("PaidFC").cast("decimal(19,6)").alias("PaidFC"),
        f.col("Comments").cast("string").alias("Comments"),
        f.col("JrnlMemo").cast("string").alias("JrnlMemo"),
        f.col("TransId").cast("int").alias("TransId"),
        f.col("ReceiptNum").cast("int").alias("ReceiptNum"),
        f.col("GroupNum").cast("smallint").alias("GroupNum"),
        f.col("DocTime").cast("smallint").alias("DocTime"),
        f.col("execution_ts").cast("string").alias("execution_ts"),
    )

    return df_sap_orin
