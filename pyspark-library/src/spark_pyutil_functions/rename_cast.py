# -*- coding: utf-8 -*-
"""

"""
import logging
from jsonschema import validate

_logger = logging.getLogger(__name__) 
class RenamenCastError(Exception): 
    pass
schema_parameters = { 
        "type": "array", 
        "description": "List of udf parameters",
        "items": {
            "type": "object",
            "description": "Udf parameter",
                "properties": {
                    "ColumnName": { "type": "string", "description": "Current Column Name" },
                    "NewColumnName": { "type": "string", "description": "New Column Name" },
                    "NewDataType": { "type": "string", "description": "Type to cast column to" }
                },
            "additionalProperties": False,
            "required": ["ColumnName", "NewColumnName", "NewDataType"] 
            }
        }
            

def execute (spark, array_input, parameters,dynamic_parameters):
    _logger.info("*** Starting udf execution, [{}],[{}],[{}] ***".format(spark, array_input, parameters))
    
    if len(array_input) != 1:
        raise  RenamenCastError("Rename and cast UDF is compatible with only one dataframe")
    df_output = array_input["df_input"]
    
    if parameters is not None:
        validate(instance=parameters, schema=schema_parameters)
        for col in parameters:
            df_output = df_output.withColumn(col['ColumnName'], df_output[col['ColumnName']].cast(col['NewDataType'])).withColumnRenamed(col['ColumnName'], col['NewColumnName'])
    else:
        _logger.warn("parameters is None")
    return df_output
        
