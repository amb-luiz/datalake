# -*- coding: utf-8 -*-
"""

"""
from spark_pyutil.utility import exists_files, get_and_validate_json, is_dir, delete_path
from spark_pyutil.glue_connection import GlueJDBCConection
from jsonschema import validate
import datetime 
import json
import logging
import importlib
import hashlib
import re
import uuid

_logger = logging.getLogger(__name__) 

class ConfProcessError(Exception): 
    pass

class ConfProcess:

    """ConfProcess Constructor 
        This is the ConfProcess class to be used in spark processes

        The constructor validate the JSON parameter file format with the schema above and set private variables: domains, salts and job.

        :param config_path:
        :type config_path: str
        Path to the JSON parameter file 
        :param jobName:
        Jobname to be searched inside the parameter file 
        :type jobName: str
    """

    def __init__(self, job_name, config_path, execution_datetime, region,last_execution_date=None):
        """Constructor method"""
        self.__config_schema = {
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
                                        "GlueConnection": { "type": "string", "description": "Name of the existing connection on Glue service" },
                                        "driver": { "type": "string", "description": "The driver to use in pyspark " },
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
                                    "DisableCatalog": {
                                        "type": "boolean",
                                        "description": "If true skip automatic metadata catalog synchronization for this output"
                                    },
                                    "PersistExecutionPartitionWithoutLabel": {
                                        "type": "boolean",
                                        "description": "When true with Replace/ExecutionDatetime, writes into an execution date directory without the partition label prefix"
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

        conf = get_and_validate_json(config_path,self.__config_schema)
        if not self.__set_job(job_name, conf):
            raise ConfProcessError("jobName not found {}".format(job_name))
        
        self.__region = region
        self.__execution_datetime = execution_datetime
        self.__last_execution_datetime = last_execution_date 
        if 'ExecutionPartitionLabel' in self.__job:
            self.__partition_label = self.__job['ExecutionPartitionLabel']
        else: 
            self.__partition_label = 'datetime'
        return
    
    def is_there_one_input (self):
        if len(self.__job['Input']) > 1:
            return False
        else:
            return True
            
    def get_first_input_read_format(self):
        return self.__job['Input'][0]['ReadFormat']
        
    def get_first_stage_path(self):    
        if "StagePath" not in self.__job['Input'][0]:
            return None
        else:
            return self.__job['Input'][0]['StagePath']

    def get_first_input_path(self):    
        return self.__job['Input'][0]['InputPath']

    def get_output_path(self):    
        return self.__job['Output'][0]['OutputPath']

    def get_execution_partition_label(self):    
        return self.__partition_label

    def get_job_name(self):
        return self.__job['JobName']
        
    def __set_job(self, jobName: str, config : str):
        """ Set the Job. 

        :param jobName:
        :type jobName: str
        :param config:
        :type config: str
        :return: True id the job was found and False if is not.
        :rtype: boolean
        """
        for job in config["Jobs"]:
            if jobName == job['JobName']:
                self.__job = job
                return True
        return False

    def is_job_active(self):
        """is_job_active.

        :return: True id the job is enable
        :rtype: boolean
        """
        return self.__job["Active"]
    
    def __get_input_delete(self, job_input):
        """__get_input_delete.

        :return: Return Input Delete 
        """
        if 'Delete' in job_input: 
            return job_input['Delete']
        else:
            return False

    def __get_input_read_format(self):
        """__get_input_read_format.

        :return: Return Input Read Format
        """
        return self.__job['Input']['ReadFormat']

    def __apply_input_read_options_spark(self, spark, read_stm, job_input):
        """__apply_input_read_options_spark.
        Apply all the read options in the job properties

        :param read_stm: Spark read statement 
        :return: return a spark read statement with the read options applied
        """
        
        if 'ReadOptions' in job_input:
            if job_input["ReadFormat"] == 'csv' and spark.version[0] == '2':
                for option in job_input['ReadOptions']:
                    if option['OptionName'] == 'delimiter':
                        if len(option['OptionValue']) > 1:
                            job_input["ReadFormat"]="csv_multidelimiter_spark2"
            if job_input["ReadFormat"] != "csv_multidelimiter_spark2":
                for option in job_input['ReadOptions']:
                    read_stm = read_stm.option(option['OptionName'], option['OptionValue'])
        return read_stm

    def __get_input_repart_value(self,job_input):
        """__get_input_repart_value.

        :return: return the repartition valeu or None if this properties was not set
        """
        if "RepartitionValue" in job_input:
            return job_input["RepartitionValue"]
        else:
            return None

    def __get_input_read_int_partitions(self,job_input):
        """__get_input_read_int_partitions.
        :return: return the list of integer partition keys

        """
        if "IntPartitions" in job_input:
            return job_input['IntPartitions']
        else:
            return []

    def __get_df_external_function_spark(self, spark, input_list): 
        """__get_df_mask_spark.
        Get the mask spark dataframe and update the all the reverse dataset if it was eanble.

        :param spark: Spark session
        :param df_input: The input spark dataframe

        :return: Spark dataframe with the mask field 
        """
        external_function = self.__job["ExternalFunction"]
        external_function_module = external_function["Module"]

        module = importlib.import_module(external_function_module)
        _logger.info("Module list: ")
        _logger.info(dir(module))
        args_external_function=[]
        args_external_function.append(spark)
        args_external_function.append(input_list)
        if "Parameters" in external_function:
            args_external_function.append(external_function["Parameters"])
        else:
            args_external_function.append(None)
        args_external_function.append({"ExecutionDatetime": self.__execution_datetime, "LastExecutionDatetime": self.__last_execution_datetime, "JobName": self.get_job_name()})
        df_output = module.execute(*args_external_function)
        return df_output

    def __write_output_spark (self,spark,df_input):
        from pyspark.sql.functions import lit
        """__write_output_spark.
        Write the output dataset baseed on the properties

        :param df_input: Spark Dataframe input
        """
        if 'Compression' in self.__job["Output"]:
            spark.conf.set("spark.sql.parquet.compression.codec", self.__job["Output"]['Compression'])
        else:
            spark.conf.set("spark.sql.parquet.compression.codec", "snappy")


        partition_keys = [] 
        if "PartitionKeyList" in self.__job["Output"]:
            for p in self.__job["Output"]["PartitionKeyList"]:
                partition_keys.append(p) 

        if "Coalesce" in self.__job["Output"]:
            df_input = df_input.coalesce(self.__job["Output"]["Coalesce"])

        spark_options = {}
        table_format = ''
        table_mode = ''

        df_input = df_input.withColumn(self.__partition_label,lit(self.__execution_datetime))
        if self.__job["Output"]["MergeType"] in ["Replace", "Replace/Partition", "Replace/ExecutionDatetime"]:
            spark_options["path"]=self.__job["Output"]["OutputPath"]
            table_format = 'parquet'
            table_mode = 'overwrite'
            persist_execution_partition_without_label = (
                self.__job["Output"].get("PersistExecutionPartitionWithoutLabel", False)
                and self.__job["Output"]["MergeType"] == "Replace/ExecutionDatetime"
            )
            if self.__job["Output"]["MergeType"] == "Replace":
                spark.conf.set("spark.sql.sources.partitionOverwriteMode","static")
            elif self.__job["Output"]["MergeType"] == "Replace/Partition":          
                spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
            elif self.__job["Output"]["MergeType"] == "Replace/ExecutionDatetime":          
                if persist_execution_partition_without_label:
                    spark.conf.set("spark.sql.sources.partitionOverwriteMode","static")
                    spark_options["path"] = '{}/{}'.format(
                        self.__job["Output"]["OutputPath"],
                        self.__execution_datetime
                    )
                else:
                    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
                if not persist_execution_partition_without_label and self.__partition_label not in partition_keys:
                    partition_keys.append(self.__partition_label )
            _logger.info("spark_options [{}]".format(spark_options))
            write_stm = df_input.write\
                .format(table_format)\
                .options(**spark_options)\
                .mode(table_mode)

            if len(partition_keys) > 0:
                write_stm.partitionBy(*partition_keys)

            if "OutputTable" in self.__job["Output"]\
                    and self.__job["Output"]["OutputTable"] \
                    and "OutputDatabase" in self.__job["Output"] \
                    and self.__job["Output"]["OutputDatabase"] \
                    and not self.__job["Output"].get("DisableCatalog", False):
                write_stm.saveAsTable('{}.{}'.format(self.__job["Output"]["OutputDatabase"], self.__job["Output"]["OutputTable"]))
                spark.sql("ALTER TABLE {}.{} SET TBLPROPERTIES ('classification' = 'parquet')".format(self.__job["Output"]["OutputDatabase"], self.__job["Output"]["OutputTable"]))
            else:
                write_stm.save()

        elif self.__job["Output"]["MergeType"] == "Upsert/Hudi":          
                
            table_name = self.__job["Output"]["OutputTable"]
            database_name = self.__job["Output"]["OutputDatabase"]
            table_format = 'hudi'
            record_key_list_str=",".join(self.__job["Output"]["MergeKeys"])
            partition_list_str=",".join(partition_keys)
            spark_options = {
              'hoodie.table.name': table_name,
              'hoodie.datasource.write.table.name': table_name,
              'hoodie.consistency.check.enabled': True,
              'hoodie.datasource.write.recordkey.field': record_key_list_str,
              'hoodie.datasource.write.operation': 'upsert',
              'hoodie.datasource.write.precombine.field': self.__job["Output"]["MergeOrderKey"],
              'hoodie.index.type': 'GLOBAL_BLOOM', #Global Index
              'hoodie.datasource.hive_sync.use_jdbc': False,
              ##'hoodie.datasource.hive_sync.ignore_exceptions': True,
              'hoodie.datasource.hive_sync.partition_fields': partition_list_str
            }
            if len(self.__job["Output"]["MergeKeys"]) > 0:
                spark_options['hoodie.datasource.write.keygenerator.class'] = 'org.apache.hudi.keygen.ComplexKeyGenerator'
            else:
                spark_options['hoodie.datasource.write.keygenerator.class'] = 'org.apache.hudi.keygen.NonpartitionedKeyGenerator'
                
            if "OutputTable" in self.__job["Output"]\
                    and self.__job["Output"]["OutputTable"] \
                    and "OutputDatabase" in self.__job["Output"] \
                    and self.__job["Output"]["OutputDatabase"]:
                if len(partition_keys) > 0: 
                    spark_options['hoodie.datasource.hive_sync.partition_extractor_class'] = 'org.apache.hudi.hive.MultiPartKeysValueExtractor'
                else:
                    spark_options['hoodie.datasource.hive_sync.partition_extractor_class'] = 'org.apache.hudi.hive.NonPartitionedExtractor'
                spark_options = {**spark_options, 
                  'hoodie.datasource.hive_sync.partition_fields': partition_list_str,
                  'hoodie.datasource.hive_sync.enable': 'true',
                  'hoodie.datasource.hive_sync.table': table_name,
                  'hoodie.datasource.hive_sync.database': database_name
                }
            else:
                spark_options['hoodie.datasource.hive_sync.enable'] = 'false'


            table_mode = 'append'
            _logger.info("spark_options [{}]".format(spark_options))
            write_stm = df_input.write\
                .format(table_format)\
                .options(**spark_options)\
                .mode(table_mode)

            if len(partition_keys) > 0:
                write_stm.partitionBy(*partition_keys)

            write_stm.save(self.__job["Output"]["OutputPath"])

        elif self.__job["Output"]["MergeType"] == "Upsert/Delta":
            from delta.tables import DeltaTable
            spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled","true")
            delta_output_path = self.__job["Output"]["OutputPath"]
            delta_table_merge_keys=self.__job["Output"]["MergeKeys"]
            delta_output_database=self.__job["Output"]["OutputDatabase"]
            delta_output_table=self.__job["Output"]["OutputTable"]
            delta_old_label='old'
            delta_new_label='new'

            delta_exists = DeltaTable.isDeltaTable(spark, delta_output_path)
            old_schema_str = ''
            if delta_exists:
                #raise ConfProcessError("delta_exits is true [{}] ".format(delta_exists))
                merge_statement = ''
                for idx, key in enumerate(delta_table_merge_keys):
                    if idx == 0:
                        merge_statement = "{old}.{key} = {new}.{key}".format(old=delta_old_label,key=key,new=delta_new_label)
                    else:    
                        merge_statement = "{stm} and {old}.{key} = {new}.{key}".format(old=delta_old_label,key=key,new=delta_new_label, stm=merge_statement)

                delta_df = spark.read.format('delta').load(delta_output_path).limit(0)
                old_schema_str = delta_df\
                    .select(sorted(delta_df.columns)) \
                    .schema \
                    .simpleString()

                delta_table = DeltaTable.forPath(spark, delta_output_path)
                delta_table.alias(delta_old_label) \
                    .merge(df_input.alias(delta_new_label), merge_statement) \
                    .whenNotMatchedInsertAll() \
                    .whenMatchedUpdateAll() \
                    .execute()
            else:
                # Create Delta Lake table
                df_input \
                    .write \
                    .format('delta') \
                    .mode('append') \
                    .save(delta_output_path)
            # Get the new schema
            delta_df = spark.read.format('delta') \
                .load(delta_output_path) \
                .limit(0)
            new_schema_str = delta_df \
                .select(sorted(delta_df.columns)) \
                .schema \
                .simpleString()
            create_external_table = (new_schema_str != old_schema_str)

            if "OutputTable" in self.__job["Output"]\
                    and self.__job["Output"]["OutputTable"] \
                    and "OutputDatabase" in self.__job["Output"] \
                    and self.__job["Output"]["OutputDatabase"]:
                if create_external_table:
                    # Drop if Exists
                    spark.sql("""
                    DROP TABLE IF EXISTS {database}.{table}
                    """.format(database=delta_output_database, table=delta_output_table))
                    schema_json = delta_df.schema.json()
                    ddl = spark.sparkContext._jvm.org.apache.spark.sql.types.DataType.fromJson(schema_json).toDDL()
                    spark.sql("""
                        CREATE EXTERNAL TABLE {database}.{table} ({columns})
                        ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                        STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'
                        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
                        LOCATION '{location}/_symlink_format_manifest/'
                        TBLPROPERTIES ('classification'='parquet')
                    """.format(columns=ddl, location=delta_output_path,database=delta_output_database, table=delta_output_table))
                delta_table = DeltaTable.forPath(spark, delta_output_path)
                delta_table.generate("symlink_format_manifest")
        else:
            raise ConfProcessError("[{}] Invalid Merge Type".format(self.__job["Output"]["MergeType"]))
        return
    
    def __get_load_path(self, job_input):
        input_path= job_input['InputPath']
        if job_input['InputType'] == 'ExecutionDatetime':
            execution_path = '{}/{}={}'.format(input_path, self.__partition_label, self.__execution_datetime)
        else:
            execution_path = input_path
        
        if not exists_files(execution_path):
            raise ConfProcessError("ExecutionPath [{}] does not have any files".format(execution_path))
        
        _logger.info("execution_path [{}]".format(execution_path))
        
        return execution_path
    
    def __read_from_file(self, read_stm, job_input):
        from pyspark.sql.functions import lit,col
        """__read_from_file.
        :param spark: Spark session
        :param job_input: Source information
        :return: return a spark dataframe with all the read properties
        """
        schema = {
                    "type": "object",
                    "properties": {
                    "InputType": { "type": "string", "description": "'Full' to get all input path, 'ExecutionDatetime' to concatenate execution_datetime to the input_path, 'LastHudiCommit' to get all row from the last hudi commit" },
                    "InputPath": { "type": "string", "description": "The input path to be read" },
                    "StagePath": { "type": "string", "description": "The stage path to be read" },
                    "InputAlias": { "type": "string", "description": "Alias input name" },
                    "Delete": { "type": "boolean", "description": "The input path to be read" },
                    "RepartitionValue": { "type": "number", "description": "If set the process will try to repartition the input to better performance" },
                    "ReadFormat": { "type": "string", "description": "The read format, Examples: parquet, csv " },
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
                    "additionalProperties": False,
                    "required": ["InputPath", "ReadFormat","InputAlias","InputType"]
                }
                
        validate(job_input, schema)
       
        df = None
        if job_input['ReadFormat'] == 'csv_multidelimiter_spark2': 
            df =  read_stm.format('text').load(self.__get_load_path(job_input))
            delimiter = None
            header = None
            for option in job_input['ReadOptions']:
                if option['OptionName'] == 'header' and option['OptionValue'] == 'true':
                    header = df.first()[0]
                elif option['OptionName'] == 'delimiter':    
                    delimiter = option['OptionValue']      
            if not delimiter:
                raise ConfProcessError("Csv multidelimiter spark2 feature need delimiter option")
            if header:    
                header_cols = header.split(delimiter)
                _logger.info("CSV Header coluns[{}]".format(header_cols))
                df = df.where(col('value') != header)
                _logger.info("CSV Multidelimiter 1")
                r1 = df.rdd        
                _logger.info("CSV Multidelimiter 2")
                r2 = r1.map(lambda x:x[0].split(delimiter))
                _logger.info("CSV Multidelimiter 3")
                df = r2.toDF(header_cols)
                _logger.info("CSV Multidelimiter 4")
            else:
                r1 = df.rdd        
                r2 = r1.map(lambda x:x[0].split(delimiter))
                df = r2.toDF()
        else:
            read_stm = read_stm.format(job_input['ReadFormat'])
            df = read_stm.load(self.__get_load_path(job_input))
        return df    
        
    def __read_from_database(self, read_stm, job_input):
        from pyspark.sql.functions import lit,col
        """_read_from_database.
        :param spark: Spark session
        :param job_input: Source information
        :return: return a spark dataframe with all the read properties
        """
        
        # validate schema according de JDBC rules
        schema = {
                    "type": "object",
                        "properties": {
                        "InputType": { "type": "string", "description": "'Full' to get all input path, 'ExecutionDatetime' to concatenate execution_datetime to the input_path, 'LastHudiCommit' to get all row from the last hudi commit" },
                        "InputAlias": { "type": "string", "description": "Alias input name"},
                        "RepartitionValue": { "type": "number", "description": "If set the process will try to repartition the input to better performance" },
                        "ReadFormat": { "type": "string", "description": "The read format, Examples: parquet, csv " },
                        "GlueConnection": { "type": "string", "description": "Name of the existing connection on Glue service" },
                        "driver": { "type": "string", "description": "The driver to use in pyspark " },
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
                    "required": ["ReadFormat","InputAlias","GlueConnection"]
                }
        validate(job_input, schema)

        # get connection string from glue connections
        gc = GlueJDBCConection(read_stm, job_input, self.__region)
        # read_stm = gc.read_from_odbc()
        df = gc.read_from_odbc()
        
        ##INSERTED IN GLUE_CONNECTION
        # if job_input['ReadFormat'] not in ('mongo'):
        #     if 'dbtable' in job_input and 'query' in job_input:
        #         raise ConfProcessError("For use JDBC connection you must set only one option, dbtable or query")
        #     elif 'dbtable' in job_input:
        #         read_stm = read_stm.option("dbtable", job_input['dbtable'])
        #     elif 'query' in job_input:  
        #         read_stm = read_stm.option("query", job_input['query'])
        #     else:
        #         raise ConfProcessError("'dbtable' or 'query' is a required property")

        # df = read_stm.load()
        
        if job_input['InputType'] == 'ExecutionDatetime':
            df = df.filter(col(self.__partition_label) >=  lit(self.__execution_datetime))

        return df
        
    def __read_input_spark(self, spark, job_input):
        """__read_input_spark.
        :param spark: Spark session
        :return: return a spark dataframe with all the read properties
        """
        if job_input['ReadFormat'] in ('jdbc', 'mongo', 'sharepoint'):
            df = self.__read_from_database(spark, job_input)
        else:
            read_stm = spark.read
            read_stm = self.__apply_input_read_options_spark(spark, read_stm, job_input) #VERIFICAR AQUI PARA COLOCAR O SAMPLE SIZE
            df = self.__read_from_file(read_stm, job_input)
        
        repart_value = self.__get_input_repart_value(job_input)
        
        if repart_value:
            _logger.info("Repartition value [{}]".format(repart_value))
            df = df.repartition(repart_value)
        return df
    
    def __delete_input(self,job_input):
        """__delete_input.
        Delete source path in case of a file
        :param job_input: Source information
        """
        if job_input['ReadFormat'] in ('jdbc', 'mongo', 'sharepoint'):
            return
        
        input_path= job_input['InputPath']
        
        if job_input['InputType'] == 'ExecutionDatetime':
            execution_path = '{}/{}'.format(input_path,self.__execution_datetime)
        else:
            execution_path = input_path
            
        if self.__get_input_delete(job_input):
           delete_path(execution_path)

    def process_spark(self,spark):
        """process_spark.
        Process the data mask pipeline with spark

        :param part_list: Partition Key list to be filtered
        :type part_list: list
        :param spark: Spark Session 
        """
        # spark.sql.sources.partitionColumnTypeInference.enabled column type inference
        # no to pass the spark option in the job context just pass every option here
        if 'JobSparkOptions' in self.__job:
            for option in self.__job['JobSparkOptions']:
                spark.conf.set(option['SparkOptionName'],option['SparkOptionValue']) 
        input_list={}
        
        for job_input in self.__job['Input']:
            input_list[job_input["InputAlias"]] = self.__read_input_spark(spark,job_input)

        df_udf = self.__get_df_external_function_spark(spark, input_list)

        self.__write_output_spark(spark, df_udf)
        
        for job_input in self.__job['Input']:
            self.__delete_input(job_input)

        return
