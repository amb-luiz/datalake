from cdl_planejamentocaixa.pipelines.parameters.pl_ambipar_carga_sbo_companies.pipeline_landing import (
    build_pipeline,
)
from cdl_planejamentocaixa.pipelines.parameters.pl_ambipar_carga_sbo_waste.common import DAG_ID


# The shared RAW writer appends SOURCE_SYSTEM, dataset and {{ ds_nodash }}.
pipeline = build_pipeline(DAG_ID)
