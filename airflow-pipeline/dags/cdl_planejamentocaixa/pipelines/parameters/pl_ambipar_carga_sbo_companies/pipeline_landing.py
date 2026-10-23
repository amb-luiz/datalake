def build_pipeline(dag_name):
    return {
        "PipelineName": dag_name,
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
