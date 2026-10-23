# -*- coding: utf-7 -*-
"""

"""
import re 
import hashlib
import boto3
import logging
import json
import sys
import shutil
import os
from jsonschema import validate

_logger = logging.getLogger(__name__)

s3 = boto3.resource('s3')

def setup_logging(loglevel):
    """Setup basic logging

    Args:
      loglevel (int): minimum loglevel for emitting messages
    """
    logformat = "[%(asctime)s] %(levelname)s:%(name)s:%(message)s"
    logging.basicConfig(level=loglevel, stream=sys.stdout,
                        format=logformat, datefmt="%Y-%m-%d %H:%M:%S")

def delete_path_spark(spark, path):
    sc = spark.sparkContext
    fs = (sc._jvm.org
          .apache.hadoop
          .fs.FileSystem
          .get(sc._jsc.hadoopConfiguration())
          )
    fs.delete(sc._jvm.org.apache.hadoop.fs.Path(path), True)

def rename_path_spark(spark, path, new_path):
    sc = spark.sparkContext
    fs = (sc._jvm.org
          .apache.hadoop
          .fs.FileSystem
          .get(sc._jsc.hadoopConfiguration())
          )
    fs.rename(sc._jvm.org.apache.hadoop.fs.Path(path),sc._jvm.org.apache.hadoop.fs.Path(new_path))

def get_json_spark(spark, jsonPath):
    json_lines = spark.read.text(jsonPath).collect()
    json_text = ''
    for line in json_lines:
        json_text = '{} {}'.format(json_text,line['value']) 
    json_content = json.loads(json_text)
    return json_content

def split_path_s3(path):
    path_split = path.split("/")
    bucket = path_split[2]
    key = ""
    first = True
    for token in path_split[3:]:
        if first:
            key = token
            first = False
        else:
            key = "{}/{}".format(key,token)
    _logger.info("path[{}]".format(path))
    _logger.info("bucket[{}]".format(bucket))
    _logger.info("key[{}]".format(key))
    return bucket,key
 
def get_json_s3(jsonPath):
    bucket,key = split_path_s3(jsonPath)
    resp = s3.Object(bucket_name=bucket, key=key)
    return json.loads(resp.get()['Body'].read().decode('utf-8'))

def delete_path_s3(path):
    bucket,key = split_path_s3(path)
    bucket_s3 = s3.Bucket(bucket) 
    delete_next=True
    while delete_next:
        list_delete = bucket_s3.objects.filter(Prefix="{}/".format(key))
        if len(list(list_delete)) < 990:
            delete_next=False
        list_delete.delete()
    return 

def delete_path_local(path):
    shutil.rmtree(path)
    return

def get_json_local(jsonPath):
    _logger.info("###################################")
    _logger.info("jsonPath[{}]".format(jsonPath))
    _logger.info("###################################")
    with open(jsonPath, "r") as f:
        json_local= json.load(f)
    return json_local

def exists_files_s3(input_path):
    """exists_files_s3.
    Process to check if exists files in s3 path

    :param input_path:
    :type input_path: string
    """
    bucket,key=split_path_s3(input_path)
    _logger.info("###################################")
    _logger.info("Check if exists input_path: {}".format(input_path))
    _logger.info("Bucket: {}".format(bucket))
    _logger.info("key: {}".format(key))
    _logger.info("###################################")
    bucket = s3.Bucket(bucket) 
    list_exists = bucket.objects.filter(Prefix="{}/".format(key))
    if list(list_exists.limit(1)):
        exists = True
    else:
        exists = False

    if not exists:
      _logger.info("###################################")
      _logger.info("Path does not exists")
      _logger.info("input_path: {}".format(input_path))
      _logger.info("###################################")
    
    return exists

def exists_files_local(input_path):
    """exists_files_local.
    Process to check if exists files in local path

    :param input_path:
    :type input_path: string
    """
    _logger.info("###################################")
    _logger.info("Check if exists input_path: {}".format(input_path))
    _logger.info("###################################")

    exists = False
    
    for root, subdirs, files in os.walk(input_path):
        if files:
            exists = True
            break

    if not exists:
      _logger.info("###################################")
      _logger.info("Path does not exists")
      _logger.info("input_path: {}".format(input_path))
      _logger.info("###################################")
    
    return exists


def get_and_validate_json(path, schema):
    if path.find("s3") == -1:
        path_json= get_json_local(path)
    else:
        path_json = get_json_s3(path)
    validate(instance=path_json, schema=schema)        
    return path_json
    
def is_dir(prefix, key):
    path = "{}/{}".format(prefix, key)
    if prefix.find("s3") == -1:
        return is_dir_local(path)
    else:
        return is_dir_s3(path)


def is_dir_local(input_path):
    _logger.info("###################################")
    _logger.info("Check if local path is a directory: {}".format(input_path))
    _logger.info("###################################")
    return os.path.isdir(input_path)

def is_dir_s3(input_path):
    _logger.info("###################################")
    _logger.info("Check if s3 path is a kind of directory: {}".format(input_path))
    _logger.info("###################################")
    bucket,key=split_path_s3(input_path)
    ret = False
    bucket = s3.Bucket(bucket) 
    list_exists = bucket.objects.filter(Prefix="{}/".format(key))
    if list(list_exists.limit(1)):
        ret = True
    return ret
           
def is_local_path(path):
    if path.find("s3") == -1:
        return True
    else:
        return False
    
def exists_files(path):   
    if is_local_path(path):
        return exists_files_local(path)
    else:
        return exists_files_s3(path)

def delete_path(path):        
    if is_local_path(path):
        delete_path_local(path)
    else:
        delete_path_s3(path)
        