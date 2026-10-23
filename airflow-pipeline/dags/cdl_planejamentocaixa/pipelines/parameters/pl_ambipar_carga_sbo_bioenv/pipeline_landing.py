pipeline = {
    "PipelineName": "pl_ambipar_carga_SBO_BIOENV",
    "Flows": [
        {
            "FlowName": "Landing",
            "Stages": [
                {
                    "StageName": "Extract",
                    "ComputeProfile": "jdbc2landing",
                    "ExecutionPartitionLabel": "execution_ts",
                    "Output": {
                        "StorageLayer": "raw",
                        "StorageBackend": "azure_adls",
                        "StoragePathPrefix": "datalake/delta/raw",
                        "MergeType": "Replace/ExecutionDatetime",
                        "PersistExecutionPartitionColumn": True,
                        "PersistExecutionPartitionWithoutLabel": True,
                        "Coalesce": 1,
                    },
                }
            ],
        }
    ],
}
