import sys
import boto3
from spark_pyutil.utility import setup_logging, split_path_s3, exists_files
from spark_pyutil import __version__
import spark_pyutil.conf_process  as conf_process
import logging
import argparse
import re 
import uuid


_logger = logging.getLogger(__name__) 

class DriverPrepareBatchError(Exception): 
    pass

def parse_args(args):
    """Parse command line parameters

    Args:
      args ([str]): command line parameters as list of strings

    Returns:
      :obj:`argparse.Namespace`: command line parameters namespace
    """
    parser = argparse.ArgumentParser(
        description="Prepare batch files for a process")
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
        help="execution Datetime to concat with outputpath",
        metavar="STRING")
    parser.add_argument(
        "--config-path",
        required=True,
        dest="configPath",
        help="Config Path",
        metavar="STRING") 
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
    
args, unknown_args = parse_args(sys.argv)

setup_logging(args.loglevel)

_logger.info("###################################")
_logger.info("###################################")
_logger.info("Unknow Parameters")
_logger.info("{}".format(unknown_args))
_logger.info("###################################")
_logger.info("###################################")
    
_logger.info("The jobName is: {}".format(args.jobName))
_logger.info("The executionDatetime is: {}".format(args.executionDatetime))
_logger.info("The configPath is: {}".format(args.configPath))

cf = conf_process.ConfProcess(job_name=args.jobName,config_path=args.configPath, execution_datetime=args.executionDatetime, region=args.region)

if not cf.is_there_one_input():
    raise DriverPrepareBatchError("The prepare batch process is compatible with only one input in the job config")
   
re_filter = "{}$".format(cf.get_first_input_read_format())
input_path = cf.get_first_stage_path()
if not input_path:
    raise DriverPrepareBatchError("Job must has Stage path to process the prepare batch")
output_path = cf.get_first_input_path()
partition_label = cf.get_execution_partition_label()
execution_datetime = args.executionDatetime 

bucket_name,key=split_path_s3(input_path)
out_bucket_name,out_key=split_path_s3(output_path)
_logger.info("###################################")
_logger.info("Check if exists input_path: {}".format(input_path))
_logger.info("Bucket: {}".format(bucket_name))
_logger.info("key: {}".format(key))
_logger.info("OutBucket: {}".format(out_bucket_name))
_logger.info("Outkey: {}".format(out_key))
_logger.info("###################################")

if not exists_files(input_path):
    raise DriverPrepareBatchError("InputPath [{}] does not have any files".format(input_path))

s3 = boto3.resource('s3')

bucket = s3.Bucket(bucket_name) 
objects = bucket.objects.filter(Prefix="{}/".format(key))
exec_uuid = uuid.uuid4()
_logger.info("Execution UUID: {} ".format(exec_uuid))
if not list(objects.limit(1)):
    _logger.warning("There is no files to process")
else:
    for idx, o in enumerate(objects):
        if re.search(re_filter, o.key, re.IGNORECASE):
            file_name = o.key.split("/")[-1]
            _logger.info("de: {}, para: {}/{}".format(o.key,out_key,file_name))
            copy_source = {
                'Bucket': bucket_name,
                'Key': o.key
            }
            s3.meta.client.copy(
                copy_source,
                out_bucket_name, 
                "{}/{}={}/{}-{}-{}".format(out_key, partition_label, execution_datetime, idx, exec_uuid, file_name)
            )
            o.delete()
        else:
            _logger.warning("File {} was ignored by the filter {}".format(o.key,re_filter))
            
    
