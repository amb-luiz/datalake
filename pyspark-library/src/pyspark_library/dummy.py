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
    # Dummy function return the last dataframe from the dict
    array = []
    for idx, key in enumerate(array_input):
        array.append(array_input[key])
    return array[-1]
        
