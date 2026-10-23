schema = {
	"title": "Jobs template",
	"description": "Tasks jobs",
	"type": "object",
	"properties": {
		"DatasetName": {"type": "string", "description": "DatasetName"},
		"SourceSystem":{"type": "string", "description": "Directory to be used after bucketname"},
		"AliasSystem":{"type": "string", "description": "Short name of SourceSystem to be with table name like"},
		"Stages": {
			"type": "array",
			"items": {
					"type": "object",
					"properties": {
						"ExternalFunction": {
							"type": "object",
							"properties": {
								'Module': { "type": "string", "description": "Spark pyutil function" },
								"Parameters": {
									"type": "array",
									"items": {
									  "type": "object",
									  "additionalProperties": True,
									},
								}
							},
							"required": ["Module"]
						}
					},
				  "required": ["ExternalFunction"]
			}
		}
	},
	"required": ["DatasetName","Stages"]
}