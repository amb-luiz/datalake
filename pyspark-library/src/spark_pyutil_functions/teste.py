import pytest
import rename_cast
from pyspark.sql import SparkSession


def test_rename_cast():
    
    spark = SparkSession.builder.appName('Test cast').getOrCreate()
    
    inputPath = 'data/input/rent'
    parameters = [
					{"ColumnName":"id", "NewColumnName":"ID", "NewDataType": "Integer"},
					{"ColumnName":"movie", "NewColumnName":"Movie", "NewDataType": "String"},
					{"ColumnName":"movie_genres", "NewColumnName":"Movie_Genres", "NewDataType": "String"}
					
				]
	
	
	assert rename_cast.execute(spark, inputPath, parameters) is not None