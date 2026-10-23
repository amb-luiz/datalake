SOURCE_SYSTEM = "pipeline_acqua_cotacoes"


TABLE_NAMES = [
    "bi_acqua_cotacoes",
]


TABLE_SPECS = [
    {
        "table_name": table_name,
        "db_object": f"dbo.{table_name}",
        "landing_dataset": f"{table_name}_Landing",
        "integration_dataset": f"{table_name}_Integration",
        "business_dataset": f"{table_name}_Business",
    }
    for table_name in TABLE_NAMES
]
