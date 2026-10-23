# -*- coding: utf-8 -*-
"""

"""
import logging
from jsonschema import validate

_logger = logging.getLogger(__name__) 
class DecriptCastError(Exception): 
    pass
schema_parameters = { 
        "type": "array", 
        "description": "List of udf parameters",
        "items": {
            "type": "object",
            "description": "Udf parameter",
                "properties": {
                    "ColumnName": { "type": "string", "description": "Current Column Name" },
                    "ReversePath": { "type": "string", "description": "path of reverse datamask" },
                    "NameReverseField": {"type": "string", "description": "Name of field in reverse file"}
                },
            "additionalProperties": False,
            "required": ["ColumnName", "ReversePath", "NameReverseField"] 
            }
        }
            

def execute (spark, array_input, parameters,dynamic_parameters):
    _logger.info("*** Starting udf execution, [{}],[{}],[{}] ***".format(spark, array_input, parameters))
    
    validate(instance=parameters, schema=schema_parameters)
    
    
    # df_output = spark.read.csv(array_input)
    #try:
    #    df_output = spark.read.parquet(array_input["input"])
    #except Exception as error:
    #    _logger.error(error)
    #    return None
    if len(array_input) != 1:
       raise  DecriptCastError("Decript UDF is compatible with only one dataframe")
    df_output = array_input["df_input"]
    if parameters is not None:
        for col in parameters:
	        reverse_df = spark.read_parquet(col["ReversePath"])
	        result = df_output.join(reverse_df, on=[col["ColumnName"], col["NameReverseField"]], how="inner", suffixes="_new")
	        del result[col["ColumnName"]]
	        result_df = result.withColumnRenamed(col["ColumnName"], col["{column}_new".format(column=col["ColumnName"])])
    
    return result_df
        
