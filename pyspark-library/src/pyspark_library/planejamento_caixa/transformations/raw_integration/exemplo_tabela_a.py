from pyspark.sql import functions as f
from pyspark.sql.functions import input_file_name, regexp_extract


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
    df_exemplo_tabela_a = inputs["df_exemplo_tabela_a"]

    df_exemplo_tabela_a = _ensure_execution_ts(df_exemplo_tabela_a)
    df_exemplo_tabela_a = _latest_execution_only(df_exemplo_tabela_a)

    df_exemplo_tabela_a = df_exemplo_tabela_a.select(
        f.col("id").cast("int").alias("id"),
        f.col("descricao").cast("string").alias("descricao"),
        f.col("execution_ts").cast("string").alias("execution_ts"),
    )

    return df_exemplo_tabela_a
