pipeline = {
    "PipelineName": "Pipeline_35_OINV_BUSINESS",
    "Flows": [
        {
            "FlowName": "Landing",
            "Stages": [
                {
                    "StageName": "Reference",
                    "ComputeProfile": "landing2raw",
                    "ExecutionPartitionLabel": "execution_ts",
                    "Output": {
                        "StorageLayer": "landing",
                        "MergeType": "Replace/ExecutionDatetime",
                        "Coalesce": 1,
                    },
                }
            ],
        },
        {
            "FlowName": "Integration",
            "Stages": [
                {
                    "StageName": "Upsert",
                    "ComputeProfile": "landing2raw",
                    "ExecutionPartitionLabel": "execution_ts",
                    "Input": {
                        "StorageLayer": "landing",
                        "InputType": "ExecutionDatetime",
                        "ReadFormat": "parquet",
                        "InputAlias": "df_input",
                    },
                    "Output": {
                        "StorageLayer": "integration",
                        "MergeType": "Replace",
                        "Coalesce": 1,
                    },
                }
            ],
        },
        {
            "FlowName": "Business",
            "Stages": [
                {
                    "StageName": "Transform",
                    "ComputeProfile": "integration2business",
                    "Input": {
                        "InputType": "Full"
                    },
                    "Output": {
                        "StorageLayer": "business",
                        "MergeType": "Replace",
                        "Coalesce": 1,
                    },
                }
            ],
        }
    ],
}
