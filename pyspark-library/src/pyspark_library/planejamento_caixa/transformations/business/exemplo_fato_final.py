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
    exemplo_tabela_a = _latest_execution_only(_ensure_execution_ts(inputs["exemplo_tabela_a"]))
    exemplo_tabela_b = _latest_execution_only(_ensure_execution_ts(inputs["exemplo_tabela_b"]))

    df_final = (
        exemplo_tabela_a.join(
            exemplo_tabela_b,
            exemplo_tabela_a["id"] == exemplo_tabela_b["id_tabela_a"],
            "left",
        )
        .select(
            exemplo_tabela_a["id"].alias("id"),
            exemplo_tabela_a["descricao"].alias("descricao"),
            exemplo_tabela_b["valor"].alias("valor"),
            f.coalesce(
                exemplo_tabela_a["execution_ts"],
                exemplo_tabela_b["execution_ts"],
            ).alias("execution_ts"),
        )
    )

    return df_final
