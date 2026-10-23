from cdl_planejamentocaixa.pipelines.parameters._shared.jdbc_jobs import build_landing_job
from cdl_planejamentocaixa.pipelines.parameters.pipeline_35_oinv_158.common import (
    SOURCE_SYSTEM,
    TABLE_SPECS,
)


landing_jobs = [
    build_landing_job(
        source_system=SOURCE_SYSTEM,
        dataset_name=spec["landing_dataset"],
        database_name="SBO_BIOENV",
        conn_id="sap",
        dbtable=spec["db_object"],
    )
    for spec in TABLE_SPECS
]
