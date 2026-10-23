#Schema from spark-pyutil, it must be the same version that the Spark Job is using.
#TODO: Add as package and get reference

schema =  {
            "title": "ConfProcess static parameters",
            "type": "object",
            "description": "JSON schema to the spark-pyutil parameter file", 
            "properties": {
                "Jobs": {
                    "description": "List of Jobs",
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "JobName": { "type": "string", "description": "The Job name" },
                            "ExecutionPartitionLabel": { "type": "string", "description": "It is the partition label to the execution_datetime" },
                            "Active": { "type": "boolean", "description": "If False turn off the job processing" },
                            "ComputeOptions":  {
                                    "type": "object",
                                    "properties": {
                                            "ComputeOption": { "type": "string", "description": "Compute Option Name" },
                                        },
                                    "additionalProperties": True
                                },
                            "JobSparkOptions": {
                                "type": "array",
                                "description": "List of spark option to be aplied on the Job Level",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                            "SparkOptionName": { "type": "string", "description": "Spark Option Name" },
                                            "SparkOptionValue": { "type": "string", "description": "Spark Option Value" }
                                        },
                                    "additionalProperties": False,
                                    "required": ["SparkOptionName", "SparkOptionValue"]
                                    }
                                },
                            "Input": {
                                "type": "array",
                                "description": "List of inputs to be mapped",
                                "items": {
                                    "type": "object",
                                        "properties": {
                                        "InputType": { "type": "string", "description": "'Full' to get all input path, 'ExecutionDatetime' to concatenate execution_datetime to the input_path, 'LastHudiCommit' to get all row from the last hudi commit" },
                                        "InputPath": { "type": "string", "description": "The input path to be read" },
                                        "StagePath": { "type": "string", "description": "The stage path to be read" },
                                        "InputAlias": { "type": "string", "description": "Alias input name" },
                                        "Delete": { "type": "boolean", "description": "The input path to be read" },
                                        "RepartitionValue": { "type": "number", "description": "If set the process will try to repartition the input to better performance" },
                                        "ReadFormat": { "type": "string", "description": "The read format, Examples: parquet, csv " },
                                        "url": { "type": "string", "description": "The database URL with hostname and database name " },
                                        "driver": { "type": "string", "description": "The driver to use in pyspark " },
                                        "user": { "type": "string", "description": "Database user" },
                                        "password": { "type": "string", "description": "Password user" },
                                        "dbtable": { "type": "string", "description": "The table source name" },
                                        "query": { "type": "string", "description": "Query source name" },
                                        "ReadOptions": { 
                                            "type": "array", 
                                            "items": {
                                                "type": "object",
                                                "description": "The read options to use in the read process",
                                                "properties": {
                                                    "OptionName": { "type": "string", "description": "Option name" },
                                                    "OptionValue": { "type": "string", "description": "Option Value" }
                                                    },
                                                    "additionalProperties": False,
                                                    "required": ["OptionName", "OptionValue"]
                                                } 
                                            }
                                        },
                                    "additionalProperties": True,
                                    "required": ["ReadFormat","InputAlias"]
                                    }
                                },
                            "Output": {
                                "type": "object",
                                "description": "Output properties",
                                "properties": {
                                    "OutputPath": { "type": "string", "description": "The output path to write"  },
                                    "OutputTable": { "type": "string", "description": "If set the process will try to update the table metadata"   },
                                    "OutputDatabase": { "type": "string", "description": "If set the process will try to update the metadata, it needs to be set with  OutputTable"   },
                                    "Coalesce": { "type": "number", "description": "If set the process will coalesce partitions before write in the output. This is usually the number of files to be write in the ouput path."   },
                                    "Compression": { "type": "string", "description": "If set define the compression type to use with parquet files. (none, uncompressed, snappy, gzip, lzo, brotli, lz4, zstd) Default: snappy"   },
                                    "PartitionKeyList": {
                                        "type": "array",
                                        "description": "List of partition Key",
                                        "items": { "type": "string"}
                                        },
                                    "MergeType": {"type": "string", "description": "Upsert/Delta: Merge with MergesKeys and MergeOrderKey (Delta Lake), Upsert/Hudi: Merge with MergesKeys and MergeOrderKey (Apache Hudi), Replace: Full replace/Trucate and insert (Parquet), Replace/Partition: replace and truncate only the partition (Parquet), Replace/ExecutionDatetime: replace and truncate only the partition of the currently execution date (Parquet)"},
                                    "MergeKeys": { 
                                        "type": "array", 
                                        "description": "Merge Key to be used in the merge process",
                                        "items": {"type": "string"}
                                    },
                                    "MergeOrderKey": {"type": "string", "description": "Key field to be used to order the data before apply the upsert process"},
                                    },
                                "additionalProperties": False,
                                "required": ["OutputPath","MergeType"]
                                },
                            "ExternalFunction": {
                                "type": "object",
                                "description": "External function properties",
                                    "type": "object",
                                    "properties": {
                                        "Module": { "type": "string", "description": "Module name to be imported"   },
                                        "Parameters": { 
                                            "type": "array", 
                                            "description": "List of external function parameters",
                                            "items": {
                                                "type": "object",
                                                "description": "External function parameter",
                                                "properties": {
                                                    "ParameterName": { "type": "string", "description": "ParameterName"   },
                                                    "ParameterValue": { "type": "string", "description": "Parameter Value"  },
                                                    "ParameterType": { "type": "string", "description": "Parameter Type"  }
                                                },
                                                "additionalProperties": True,
                                                }
                                            }
                                        },
                                    "additionalProperties": False,
                                    "required": ["Module"] 
                                    }
                            },
                            "additionalProperties": False,
                            "required": ["JobName", "Active", "Input", "Output", "ExternalFunction"]
                        }
                }
            }, 
            "additionalProperties": False,
            "required": ["Jobs"]
        }