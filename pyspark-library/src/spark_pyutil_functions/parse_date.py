# -*- coding: utf-8 -*-
"""

"""
import logging
from jsonschema import validate
from pyspark.sql.types import DateType


_logger = logging.getLogger(__name__) 
class ParseDateTimeError(Exception): 
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
                    "DateFormat": { "type": "string", "description": "Formato of field date" }
                },
            "additionalProperties": False,
            "required": ["ColumnName", "NewColumnName", "DateFormat"] 
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
       raise  ParseDateTimeError("Parse datetime UDF is compatible with only one dataframe")
    df_output = array_input["df_input"]
    
    if parameters is not None:
        for col in parameters:
	        df_output = df_output.withColumn(col['ColumnName'], df_output[col['ColumnName']].cast(col['NewDataType'])).withColumnRenamed(col['ColumnName'], col['NewColumnName'])
    
    return df_output
    