schema =  {
    "title": "Pipeline template schema",
    "type": "object",
    "description": "JSON schema to template pipeline", 
    "properties": {
        'PipelineName': { "type": "string", "description": "Pipeline Name" },
        'Flows': {
            "description": "List of Flows",
            "type": "array",
            "items": {
                 "type": "object",
                 "properties": {
                     'FlowName': { "type": "string", "description": "Flow Name" },
                     'Stages': {
                        "description": "List of Stages",
                        "type": "array",
                        "items": {
                             "type": "object",
                             "properties": {
                                 'StageName': { "type": "string", "description": "Stage Name" },
                                 'Input' : {
                                     "type": "object",
                                     "properties": {
                                        'StorageLayer': { "type": "string", "description": "Storage Layer" },
                                    },
                                    "additionalProperties": True
                                 }
                             },
                            "additionalProperties": True,
                            "required": ["StageName"] 
                        }
                    }    
                },
                "additionalProperties": False,
                "required": ["FlowName", "Stages"] 
            }
        }
    },
    "additionalProperties": False,
    "required": ["PipelineName", "Flows"] 
}