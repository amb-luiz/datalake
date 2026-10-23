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


def _prefix_columns(df, prefix):
    return df.select([f.col(column_name).alias(f"{prefix}_{column_name}") for column_name in df.columns])


def execute(spark, inputs, parameters=None, dynamic_parameters=None, write=True):
    sap_oinv = _latest_execution_only(_ensure_execution_ts(inputs["sap_oinv"]))
    sap_rin1 = _latest_execution_only(_ensure_execution_ts(inputs["sap_rin1"]))
    sap_orin = _latest_execution_only(_ensure_execution_ts(inputs["sap_orin"]))

    logging.info("Executando joins da tabela fat_sap_oinv_rin1_orin")

    df_oinv = _prefix_columns(sap_oinv, "OINV")
    df_rin1 = _prefix_columns(sap_rin1, "RIN1")
    df_orin = _prefix_columns(sap_orin, "ORIN")

    df_fat_sap = (
        df_oinv.join(
            df_rin1,
            df_oinv["OINV_DocEntry"] == df_rin1["RIN1_BaseEntry"],
            "left",
        )
        .join(
            df_orin,
            df_rin1["RIN1_DocEntry"] == df_orin["ORIN_DocEntry"],
            "left",
        )
    )

    df_fat_sap = df_fat_sap.withColumn(
        "execution_ts",
        f.coalesce(
            f.col("OINV_execution_ts"),
            f.col("RIN1_execution_ts"),
            f.col("ORIN_execution_ts"),
        ),
    )

    return df_fat_sap
