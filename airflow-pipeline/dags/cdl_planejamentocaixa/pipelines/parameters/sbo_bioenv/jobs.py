from cdl_planejamentocaixa.pipelines.parameters._shared.jdbc_jobs import build_landing_job
from cdl_planejamentocaixa.pipelines.parameters.sbo_bioenv.common import (
    SOURCE_SYSTEM,
    TABLE_SPECS,
)


def _dbtable_for_spec(spec):
    if "sql_query" in spec:
        return f"({spec['sql_query']}) AS src"
    return spec["db_object"]


landing_jobs = [
    build_landing_job(
        source_system=SOURCE_SYSTEM,
        dataset_name=spec["landing_dataset"],
        database_name="SBO_BIOENV",
        conn_id="sap",
        dbtable=_dbtable_for_spec(spec),
    )
    for spec in TABLE_SPECS
]
