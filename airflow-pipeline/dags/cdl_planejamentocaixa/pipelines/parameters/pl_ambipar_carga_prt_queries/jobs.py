from cdl_planejamentocaixa.pipelines.parameters._shared.jdbc_jobs import build_landing_job
from cdl_planejamentocaixa.pipelines.parameters.pl_ambipar_carga_prt_queries.common import (
    CONN_ID,
    DATABASE_NAME,
    SOURCE_SYSTEM,
)


def build_landing_job_for_query(spec):
    return build_landing_job(
        source_system=SOURCE_SYSTEM,
        dataset_name=spec["folder_name"],
        database_name=DATABASE_NAME,
        conn_id=CONN_ID,
        dbtable=f"({spec['sql_query']}) AS src",
    )
