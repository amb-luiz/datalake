import logging
from jsonschema import validate

from pyspark.sql import functions as f


_logger = logging.getLogger(__name__) 
class ConcatColumnsError(Exception): 
    pass
schema_parameters = { 
        "type": "array", 
        "description": "List of udf parameters",
        "items": {
            "type": "object",
            "description": "Udf parameter",
                "properties": {
                    "Columns": { 
                        "type": "array", 
                        "description": "Name of columns to concat",
                        "items": {
                            
                        }
                        
                    },
                    "ColumnResult": { "type": "string", "description": "Result column name" }
                },
            "additionalProperties": False,
            "required": ["ColumnA", "ColumnB", "ColumnC"] 
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
       raise  RenamenCastError("Rename and cast UDF is compatible with only one dataframe")
    df_output = array_input["df_input"]
    if parameters is not None:
        for col in parameters:
            column_a = df_output[]
            
	        def ano_mes():
    """
    Ano/Mes = tblTotvsKPIFinanceiro[ColigadaAno]&"/"&FORMAT(DATE(tblTotvsKPIFinanceiro[ColigadaAno],tblTotvsKPIFinanceiro[ColigadaMes],1),"MM")
    """
    coligada_ano = f.col('ColigadaAno')
    coligada_mes = f.col('ColigadaMes')
    _col = f.concat_ws('/', coligada_ano, coligada_mes)

    return _col.cast('string')
    
    return df_output