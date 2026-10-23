pipeline = {
    'PipelineName': 'PipelinePlanejamentoCaixa',
    'Flows':[
    {
        'FlowName': 'Integration',
        'Stages': [
        {
            'StageName': 'Datamask',
            'ExecutionPartitionLabel': 'execution_ts',
            'ComputeProfile': 'landing2raw',
            'Input':{
                'ReadFormat':'csv',
                "StorageLayer":"landing",
                "Delete": False,
                "RepartitionValue": 5,
                "InputType": "ExecutionDatetime",
                "InputAlias": "df_input",
                'ReadOptions':[
                    { 'OptionName': 'header', 'OptionValue': 'true'}    
                ]
            },
            'ExternalFunction':{
                'Module': 'spark_pyutil_functions.datamask',
                 'Parameters': 
	                [{# Adicionar macros com variaveis de airflow
	                 "ReverseDatasetPath": "{{ var.value.ReverseDatasetPath.replace('cdl-cogna-lakehouse','cdl-planejamentocaixa') }}",
                     "DomainsPath": "{{ var.value.DomainsPath }}",
                     "SaltsPath": "{{ var.value.SaltsPath }}" 
    				}]
            },
            'Output': {
                'MergeType':'Replace/ExecutionDatetime',
                "StorageLayer":"raw",
                'Coalesce': 1
            }
        },
        {
            'StageName': 'Upsert',
            'ComputeProfile': 'raw2integration',
            'ExecutionPartitionLabel': 'execution_ts',
            'Input':{
                "StorageLayer":"raw",
                "InputType": "ExecutionDatetime",
                'ReadFormat':'parquet',
                'InputAlias': 'df_input'
            },        
            'ExternalFunction':{
                'Module': 'spark_pyutil_functions.rename_cast',
            },
            'Output': {
                "StorageLayer":"integration",
                'MergeType':'Replace',
                'Coalesce': 1
            }
        }
        ]
    },
    {
        'FlowName': 'Business',
        'Stages':[
            { 
            'StageName': 'Transform',
            'ComputeProfile': 'integration2business',
            'Input': {
                'InputType':'Full'
            },
            'Output': {
                "StorageLayer":"business",
                'MergeType':'Replace',
                'Coalesce': 1
                }
            }
        ]
    }
    ]
}
