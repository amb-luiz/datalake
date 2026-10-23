from cdl_planejamentocaixa.pipelines.parameters._shared.jdbc_jobs import build_landing_job
from cdl_planejamentocaixa.pipelines.parameters.pl_ambipar_carga_sbo_eco_products.common import (
    CARGAS_ECO,
    DATABASE_NAME,
    SOURCE_SYSTEM,
)


landing_jobs = [
    build_landing_job(
        source_system=SOURCE_SYSTEM,
        dataset_name=carga["nome"],
        database_name=DATABASE_NAME,
        conn_id="sap",
        dbtable=f"({carga['sql'].strip()}) AS src",
    )
    for carga in CARGAS_ECO
]
