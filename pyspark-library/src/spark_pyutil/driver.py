# -*- coding: utf-8 -*-
"""

"""
from __future__ import print_function
#from pyspark.sql.functions import date_format, max as max_, lit, from_json, udf,  col, decode, concat, regexp_replace, current_timestamp

from pyspark.sql.utils import AnalysisException
from pyspark.context import SparkContext

import os
import sys
from random import random
from operator import add

from pyspark.sql import SparkSession
import re 
import hashlib

import argparse
import sys
import json
import logging

from spark_pyutil.utility import setup_logging
import spark_pyutil.conf_process  as conf_process
from pyspark_library.storage import build_spark_s3a_conf, load_storage_settings_from_env


def build_spark_azure_conf():
    account_name = os.getenv("AZURE_ADLS_ACCOUNT_NAME", "").strip()
    if not account_name:
        return {}

    account_fqdn = f"{account_name}.dfs.core.windows.net"
    secret_type = os.getenv("AZURE_ADLS_SECRET_TYPE", "oauth").strip().lower() or "oauth"
    conf = {
        "spark.hadoop.fs.azure.impl": "org.apache.hadoop.fs.azure.NativeAzureFileSystem",
        "spark.hadoop.fs.abfss.impl": "org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem",
        "spark.hadoop.fs.AbstractFileSystem.abfss.impl": "org.apache.hadoop.fs.azurebfs.Abfss",
        "spark.hadoop.fs.azure.ssl.channel.mode": "Default_JSSE",
    }

    if secret_type == "account_key":
        account_key = os.getenv("AZURE_ADLS_ACCOUNT_KEY", "").strip()
        if account_key:
            conf[f"spark.hadoop.fs.azure.account.key.{account_fqdn}"] = account_key
        return conf

    tenant_id = os.getenv("AZURE_TENANT_ID", "").strip()
    client_id = os.getenv("AZURE_CLIENT_ID", "").strip()
    client_secret = os.getenv("AZURE_CLIENT_SECRET", "").strip()
    if tenant_id and client_id and client_secret:
        conf.update(
            {
                f"spark.hadoop.fs.azure.account.auth.type.{account_fqdn}": "OAuth",
                (
                    f"spark.hadoop.fs.azure.account.oauth.provider.type.{account_fqdn}"
                ): "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
                f"spark.hadoop.fs.azure.account.oauth2.client.id.{account_fqdn}": client_id,
                f"spark.hadoop.fs.azure.account.oauth2.client.secret.{account_fqdn}": client_secret,
                (
                    f"spark.hadoop.fs.azure.account.oauth2.client.endpoint.{account_fqdn}"
                ): f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
            }
        )
    return conf

from spark_pyutil import __version__

__author__ = "Diogo Kato"
__copyright__ = "Diogo Kato"
__license__ = "mit"

_logger = logging.getLogger(__name__)

class ConfProcessPysparkError(Exception):
    pass


def build_local_spark_session(app_name, disable_glue_catalog):
    builder = SparkSession.builder.appName(app_name).config(
        "spark.serializer", "org.apache.spark.serializer.KryoSerializer"
    )
    for key, value in build_spark_s3a_conf(load_storage_settings_from_env()).items():
        builder = builder.config(key, value)
    for key, value in build_spark_azure_conf().items():
        builder = builder.config(key, value)
    spark_extensions = []
    catalog_mode = os.getenv("SPARK_CATALOG_MODE", "hive").strip().lower() or "hive"
    if os.getenv("SPARK_ENABLE_DELTA", "false").lower() == "true":
        spark_extensions.append("io.delta.sql.DeltaSparkSessionExtension")
        builder = builder.config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
    hive_metastore_uri = os.getenv("SPARK_HIVE_METASTORE_URI", "").strip()
    if hive_metastore_uri:
        builder = builder.config("hive.metastore.uris", hive_metastore_uri)
        builder = builder.config("spark.hadoop.hive.metastore.uris", hive_metastore_uri)
    warehouse_dir = os.getenv("SPARK_SQL_WAREHOUSE_DIR", "").strip()
    if warehouse_dir:
        builder = builder.config("spark.sql.warehouse.dir", warehouse_dir)
    builder = builder.config("spark.sql.catalogImplementation", "hive")
    if catalog_mode == "iceberg" and os.getenv("SPARK_ENABLE_ICEBERG", "false").lower() == "true":
        spark_extensions.append("org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        iceberg_catalog_name = os.getenv("SPARK_ICEBERG_CATALOG_NAME", "spark_catalog").strip() or "spark_catalog"
        builder = builder.config(
            f"spark.sql.catalog.{iceberg_catalog_name}",
            "org.apache.iceberg.spark.SparkSessionCatalog",
        )
        builder = builder.config(
            f"spark.sql.catalog.{iceberg_catalog_name}.type",
            "hive",
        )
        if hive_metastore_uri:
            builder = builder.config(
                f"spark.sql.catalog.{iceberg_catalog_name}.uri",
                hive_metastore_uri,
            )
        if warehouse_dir:
            builder = builder.config(
                f"spark.sql.catalog.{iceberg_catalog_name}.warehouse",
                warehouse_dir,
            )
    if spark_extensions:
        builder = builder.config("spark.sql.extensions", ",".join(spark_extensions))
    if not disable_glue_catalog and os.getenv("SPARK_USE_AWS_GLUE_CATALOG", "false").lower() == "true":
        builder = builder.config(
            "hive.metastore.client.factory.class",
            "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
        )
    return builder.enableHiveSupport().getOrCreate()

def parse_args(args):
    """Parse command line parameters

    Args:
      args ([str]): command line parameters as list of strings

    Returns:
      :obj:`argparse.Namespace`: command line parameters namespace
    """
    parser = argparse.ArgumentParser(
        description="Sql4spark is a pyspark module to execute SQL on spark and merge data with output")
    parser.add_argument(
        "--job-name",
        required=True,
        dest="jobName",
        help="Job Name",
        metavar="STRING")      
    parser.add_argument(
        "--region",
        required=True,
        dest="region",
        help="Region",
        metavar="STRING")
    parser.add_argument(
        "--execution-datetime",
        required=True,
        dest="executionDatetime",
        help="Execution Datetime",
        metavar="STRING")
    parser.add_argument(
        "--last-execution-datetime",
        required=False,
        dest="lastExecutionDatetime",
        help="Last Execution Datetime",
        metavar="STRING")
    parser.add_argument(
        "--config-path",
        required=True,
        dest="configPath",
        help="Config Path",
        metavar="STRING")
    parser.add_argument(
        "--is-glue-job",
        required=False,
        default=False,
        dest="isGlueJob",
        help="Is a Glue Job?",
        action="store_true")
    parser.add_argument(
        "--disable-glue-catalog",
        required=False,
        default=False,
        dest="disableGlueCatalog",
        help="Glue catalog",
        action="store_true")
    parser.add_argument(
        "--version",
        action="version",
        version="spark-pyutil {ver}".format(ver=__version__))
    parser.add_argument(
        "-v",
        "--verbose",
        dest="loglevel",
        help="Set loglevel to INFO",
        action="store_const",
        const=logging.INFO)
    parser.add_argument(
        "-vv",
        "--very-verbose",
        dest="loglevel",
        help="Set loglevel to DEBUG",
        action="store_const",
        const=logging.DEBUG)
    return parser.parse_known_args(args)

def main(argsv):
    """Main entry point allowing external calls

    Args:
      args ([str]): command line parameter list
    """
    args, unknown_args = parse_args(argsv)

    setup_logging(args.loglevel)

    _logger.info("###################################")
    _logger.info("###################################")
    _logger.info("Unknow Parameters")
    _logger.info("{}".format(unknown_args))
    _logger.info("###################################")
    _logger.info("###################################")

    cf = conf_process.ConfProcess(job_name=args.jobName,config_path=args.configPath, execution_datetime=args.executionDatetime, region=args.region ,last_execution_date=args.lastExecutionDatetime)
        
    if not cf.is_job_active():
        message = "Jobname[{}] is not active".format(args.jobName)
        _logger.error(message)
        raise ConfProcessPysparkError(message)

    _logger.debug("Starting datamsk-pyutil-pyspark")
    if args.isGlueJob:
        from awsglue.utils import getResolvedOptions
        from awsglue.context import GlueContext
        from awsglue.job import Job
        args_glue = getResolvedOptions(argsv, ['JOB_NAME'])
        sc = SparkContext()
        glueContext = GlueContext(sc)
        spark = glueContext.spark_session
        job = Job(glueContext)
        job.init(args_glue['JOB_NAME'], args_glue)
    else:
        spark = build_local_spark_session(
            "spark-pyutil-{}".format(cf.get_job_name()),
            args.disableGlueCatalog,
        )


    _logger.info("Initialize spark-pyutil")
    _logger.info("###################################")
    _logger.info("")
    _logger.info("spark: {}".format(spark))
    _logger.info("jobName: {}".format(args.jobName))
    _logger.info("executionDatetime: {}".format(args.executionDatetime))
    _logger.info("lastExecutionDatetime: {}".format(args.lastExecutionDatetime))
    _logger.info("configPath: {}".format(args.configPath))
    _logger.info("isGlueJob: {}".format(args.isGlueJob))
    _logger.info("disableGlueCatalog: {}".format(args.disableGlueCatalog))
    _logger.info("")
    _logger.info("###################################")

    _logger.info("Getting Json Conf")
    
    cf.process_spark(spark)

    spark.stop() 

    _logger.info("Script ends here")

def run():
    """Entry point for console_scripts
    """
    main(sys.argv)


if __name__ == "__main__":
    run()
