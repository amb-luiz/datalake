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
    df_sap_rin1 = inputs["df_sap_rin1"]

    logging.info("Executando transformacoes na tabela SAP_RIN1")

    df_sap_rin1 = _ensure_execution_ts(df_sap_rin1)
    df_sap_rin1 = _latest_execution_only(df_sap_rin1)

    df_sap_rin1 = df_sap_rin1.select(
        f.col("COD_FILIAL").cast("string").alias("COD_FILIAL"),
        f.col("DocEntry").cast("int").alias("DocEntry"),
        f.col("BaseEntry").cast("int").alias("BaseEntry"),
        f.col("BaseType").cast("int").alias("BaseType"),
        f.col("execution_ts").cast("string").alias("execution_ts"),
    )

    return df_sap_rin1
