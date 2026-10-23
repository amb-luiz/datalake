# -*- coding: utf-8 -*-
"""

"""
import logging
from jsonschema import validate
from pyspark.sql.functions import from_json, regexp_replace, explode, col

_logger = logging.getLogger(__name__) 
class ExtractJsonFieldsError(Exception): 
    pass
schema_parameters = { 
        "type": "array", 
        "description": "List of Function parameters",
        "items": {
            "type": "object",
            "description": "Udf parameter",
                "properties": {
                    "ColumnName": { "type": "string", "description": "Current Column Name" }
                },
            "additionalProperties": False,
            "required": ["ColumnName"] 
            }
        }
 
def execute (spark, array_input, parameters,dynamic_parameters):
    _logger.info("*** Starting udf execution, [{}],[{}],[{}] ***".format(spark, array_input, parameters))
    
    if len(array_input) != 1:
        raise  RenamenCastError("Extract json function is compatible with only one dataframe")
    df = array_input["df_input"]
    
    if parameters is not None:
        validate(instance=parameters, schema=schema_parameters)
        for parm in parameters:
            field = parm['ColumnName']
            if field in df.columns:
                df = df.withColumn(field,regexp_replace(field,"True",'true'))
                df = df.withColumn(field,regexp_replace(field,"False",'false'))
                schema = spark.read.json(df.rdd.map(lambda row: row[field])).schema
                df = df.withColumn(field,from_json(field,schema))
                for nested_field in schema:
                    df = df.select(col("{}.{}".format(field,nested_field.jsonValue()["name"])).alias("{}_{}".format(field,nested_field.jsonValue()["name"])),"*") 
                df = df.drop(field)    
            else:
                _logger.warn("Field {} does not exists in dataframe")
        else:        
            _logger.warn("parameters is None")
    return df
        
