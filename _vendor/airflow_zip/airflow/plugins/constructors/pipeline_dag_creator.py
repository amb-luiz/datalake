from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from operators.glue import AwsGlueJobOperator
from airflow.utils.task_group import TaskGroup
import logging as log
from jsonschema import validate
from schemas.job import schema as job_schema
from schemas.pipeline import schema as pipeline_schema
from schemas.spark_pyutil import schema as spu_schema
import boto3
import json
import os
import pathlib
import botocore
# import re


class PipelineDagCreatorError(Exception): 
    pass


class PipelineDagCreator:
    def __init__(self, pipeline, dag, execution_ts = '{{ execution_date.strftime("%Y%m%dT%H%M%S") }}'):
        self.__execution_ts ='{{ execution_date.strftime("%Y%m%dT%H%M%S") }}' 
        self.__last_execution_ts ='{{ prev_execution_date.strftime("%Y%m%dT%H%M%S") }}' 
        self.__pipeline = pipeline
        self._full_filepath = dag.full_filepath
        self.__file_path = self._full_filepath
        # raise PipelineDagCreatorError(f"##### FILE PATH = {os.getcwd()}")
        self.__project_name = PipelineDagCreator.get_pista(self.__file_path)
        self.__central_project_name = os.environ['AIRFLOW__PROJECT__NAME']
        self.__aws_conn_id  = PipelineDagCreator.get_aws_conn_id(self.__file_path) 
        self.__access_control = {self.__aws_conn_id: {'can_read', 'can_edit'}}
        dag.access_control = self.__access_control
        self.__pool = self.__aws_conn_id
        self.__jobs = {}
        self.__dag = dag
        # Glue Job reference by name
        self.__env = os.getenv("AIRFLOW_VAR_ENV", os.getenv("ENV", "local"))
        self.__region = os.getenv("AIRFLOW_VAR_REGION", os.getenv("AWS_DEFAULT_REGION", "us-east-1"))
        self.__account = os.getenv("AIRFLOW_VAR_ACCOUNT", os.getenv("ACCOUNT", "local"))
        self.__compute_profile_landing2stage=self.__get_compute_profile("landing2stage")
        self.__storage_layer_prefix = "{}-{}-{}-{}-".format(
            self.__project_name,
            self.__region, 
            self.__account,
            self.__env
            )
        if 'PipelineName' not in self.__pipeline:
            raise PipelineDagCreatorError("PipelineName is required")
        self.__artifact_bucket = PipelineDagCreator.get_bucket_artifact(self.__storage_layer_prefix)   
        self.__config_path_key = "config/pipelines/{}/{}/config.json".format(self.__pipeline["PipelineName"], self.__execution_ts)
        self.__config_path = "s3://{}/{}".format(self.__artifact_bucket,self.__config_path_key)
        # self.__config_jobs = PipelineDagCreator.get_config_variables(self.__artifact_bucket)
        if 'Flows' not in pipeline:
            raise PipelineDagCreatorError("Key 'Flow' is not in dict pipeline")
        for flow in pipeline['Flows']: 
            if 'FlowName' not in flow:
                raise PipelineDagCreatorError("Key 'FlowName' is not in dict flow")
            self.__jobs[flow['FlowName']] = {}
        
    def __get_compute_profile(self, profile):
        return "{}-{}".format(self.__project_name, profile)
        
    def add_job_external(self, pipeline, flow_name, job, group):
        if flow_name not in self.__jobs:
            raise PipelineDagCreatorError("Key 'FlowName' is not in dict flow")
        job['GroupName'] = group if group else 'nop'
        job['External'] = True
        pdc_job = PipelineDagCreator(pipeline, self.__dag)
        job=pdc_job.get_spec_external_job(job, flow_name)
        if group not in self.__jobs[flow_name]:
            self.__jobs[flow_name][group] = []
        self.__jobs[flow_name][group].append(job)
        
    def add_job(self, flow_name, job, group):
        if flow_name not in self.__jobs:
            raise PipelineDagCreatorError("Key 'FlowName' is not in dict flow")
        job['GroupName'] = group
        job['External'] = False
        if group not in self.__jobs[flow_name]:
            self.__jobs[flow_name][group] = []
        self.__jobs[flow_name][group].append(job)
        
    
    def add_job_other_domain(self, flow_name, job, group):
        if flow_name not in self.__jobs:
            raise PipelineDagCreatorError("Key 'FlowName' is not in dict flow")
        job['GroupName'] = group
        job['External'] = True
        
        if group not in self.__jobs[flow_name]:
            self.__jobs[flow_name][group] = []
        self.__jobs[flow_name][group].append(job)    
    
    
    def get_spec_external_job (self, job, flow_name):
        
        if 'Output' in job:
            output_object = job['Output']
        else:
            output_object = {}
        
        flow_job=None
        for flow in self.__pipeline['Flows']:  
            if flow_name == flow['FlowName']:
                flow_job = flow
        if not flow_job:
            raise PipelineDagCreatorError("Flow name could not be found in pipeline flows")
        
        if 'Stages' not in flow_job:
            raise PipelineDagCreatorError("Stages key needed in flow")
        if 'Output' not in flow_job['Stages'][-1]:
            raise PipelineDagCreatorError("Output needed in stage object")
            
        if 'StorageLayer' not in flow_job['Stages'][-1]['Output']:
            raise PipelineDagCreatorError("StorageLayer needed in Output")
        output_object['StorageLayer'] = flow_job['Stages'][-1]['Output']['StorageLayer']
        
        if 'MergeType' not in output_object:
            if 'MergeType' not in flow_job['Stages'][-1]['Output']:
                raise PipelineDagCreatorError("MergeType needed in Output")
            else:
                output_object['MergeType'] = flow_job['Stages'][-1]['Output']['MergeType']
            
        sourceSystem = PipelineDagCreator.set_source_system(job) 
        
        output_object['OutputPath'] = PipelineDagCreator.set_path(self.__storage_layer_prefix,
                                            output_object['StorageLayer'],
                                            job['DatasetName'],
                                            sourceSystem
                                    )
        job['Output'] = output_object                            
        return job
    
    @staticmethod
    def get_aws_conn_id(dag_path):
        # s = dag_path
        # pattern = "dags\/(.*?)\/"
        # return re.search(s, pattern).group(1)
        return dag_path.split("dags/")[1].split("/")[0]
        
    @staticmethod
    def get_pista(dag_path):
        # s = dag_path
        # pattern = "dags\/(.*?)\/"
        # return re.search(s, pattern).group(1)
        return dag_path.split("dags/")[1].split("/")[0].replace("_","-")
    
    @staticmethod
    def merge_json(json_a, json_b):
        if type(json_a) != type(json_b):
            raise PipelineDagCreatorError("There is a problem in the merge json, type incompatible!")
        if type(json_a) is dict:
            json_ret = {}
            log.info("{}".format(json_a))
            log.info("{}".format(json_b))
            json_ret.update(json_b)
            json_ret.update(json_a)
            for key in json_ret:
                if key in json_b and key in json_a:
                    json_ret[key] = PipelineDagCreator.merge_json(json_a[key], json_b[key]) 
                elif key in json_b:    
                    json_ret[key] = json_b[key] 
                else:    
                    json_ret[key] = json_a[key] 
        elif type(json_a) is list:
            json_ret = []
            if len(json_a) > len(json_b):
                for idx, item in enumerate(json_a):
                    if  idx < len(json_b):
                        json_ret.append(PipelineDagCreator.merge_json(json_a[idx], json_b[idx]))
                    else:     
                        json_ret.append(json_a[idx])
            else:            
                for idx, item in enumerate(json_b):
                    if  idx < len(json_a):
                        json_ret.append(PipelineDagCreator.merge_json(json_a[idx], json_b[idx]))
                    else:     
                        json_ret.append(json_b[idx])
        else:
            json_ret = json_b
        return json_ret
    
    
    @staticmethod
    def spark_parameters(pipeline, jobs, project_name, storage_layer_prefix, config_path_key, connection_id):
        log.info("Generate spark parameters and validate")
        log.info("pipeline: {}".format(pipeline))
        log.info("jobs: {}".format(jobs))
        log.info("project_name: {}".format(project_name))
        log.info("storage_layer_prefix: {}".format(storage_layer_prefix))
        
        log.info("Validating pipeline schema")
        validate(instance=pipeline, schema=pipeline_schema)
        # validate each job from individually
    
        j_jobs = {"Jobs":[]}
        output_job_dict = {}
        for flow in pipeline['Flows']:
            log.info("FlowName: {}".format(flow['FlowName']))
            
            for i,stage in enumerate(flow['Stages']):
                log.info("\tStageName: {}".format(stage['StageName']))
                for group, group_jobs in jobs[flow['FlowName']].items():
                    for job in group_jobs:
                        if job['External']: 
                            if 'Pista' in job:
                                s3path = PipelineDagCreator.set_path(
                                    storage_layer_prefix.replace(project_name,job['Pista']),
                                    job['StorageLayer'],
                                    job['DatasetName'],
                                    job['SourceSystem'],
                                    False
                                )
                                #def set_path(storage_layer_prefix, storageLayer, dataSetName, sourceSystem=None, stage=False, execution_ts=None, execution_label=None):
                                #job_parameter['Input'] = input_array
                                job['Output'] = {}
                                job['Output']['MergeType'] = 'Replace'
                                job['Output']['OutputPath'] = s3path
                                
                                log.info("#############################")
                                log.info(s3path)
                                log.info("#############################")
                                
                            
                            output_job_dict[
                                job['DatasetName'] if 'DatasetNameAlias' not in job else job['DatasetNameAlias']] = job['Output']
                        else:    
                            log.info("Validating {}".format(job))
                            validate(instance=job, schema=job_schema)
                            job_parameter = {}
                            
                            jobName = PipelineDagCreator.get_job_name(job['DatasetName'], stage['StageName'])
                            log.info("\t\tJobName: {}".format(jobName))
                            job_parameter['JobName']=jobName
                            
                            sourceSystem = PipelineDagCreator.set_source_system(job) 
                            aliasSystem = job['AliasSystem'] if 'AliasSystem' in job else None
                            
                            job_parameter.update(stage)
                            job_parameter.pop('StageName',None)
                            job_parameter.pop('Input',None)
                            job_parameter.pop('Output',None)
                            job_parameter.pop('ComputeProfile',None)
                            
                            job_stage =  job['Stages'][i]
                            ## External function
                            
                            job_parameter = PipelineDagCreator.merge_json(job_parameter, job_stage)
                            
                            ## Input config
                            input_array = []
                            if 'Input' in job_stage:
                                for idx_input, input_job in  enumerate(job_stage['Input']):
                                    input_object = {} 
                                    if 'Input' in stage:
                                        input_object.update(stage['Input'])
                                    input_object = PipelineDagCreator.merge_json(input_object, input_job)
                                    if 'DatasetName' in input_object:
                                        if input_object['DatasetName'] not in output_job_dict:
                                            raise PipelineDagCreatorError("{} must be added before be used as a Input dependecy".format(input_object['DatasetName']))
                                        input_object['InputPath'] = output_job_dict[input_object['DatasetName']]['OutputPath']
                                        dep_output_type = output_job_dict[input_object['DatasetName']]['MergeType']
                                        if dep_output_type.find('Replace') != -1:
                                            input_object['ReadFormat'] = 'parquet'
                                        elif  dep_output_type.find('Hudi') != -1:
                                            input_object['ReadFormat'] = 'hudi'
                                        elif  dep_output_type.find('Delta') != -1:
                                            input_object['ReadFormat'] = 'delta'
                                        else:
                                            raise PipelineDagCreatorError("Output type [{}] is not valid. Could not infer ReadFormat".format(dep_output_type))
                                        if 'InputAlias' not in input_object:
                                                input_object['InputAlias'] = 'df_{}'.format(input_object['DatasetName'].lower())
                                        input_object.pop('DatasetName',None)
                                    else:
                                        # If is the first Input and the Storage layer is landing, assume that we need to prepare a batch to process
                                        if idx_input == 0 and input_object['StorageLayer'] == 'landing':
                                            path_stage = True
                                            input_object['StagePath'] = PipelineDagCreator.set_path(
                                                                                    storage_layer_prefix,
                                                                                    input_object['StorageLayer'],
                                                                                    job['DatasetName'],
                                                                                    sourceSystem,
                                                                                    False
                                                                            )
                                        else:
                                            path_stage = False
                                            
                                        input_object['InputPath'] = PipelineDagCreator.set_path(
                                                                                    storage_layer_prefix,
                                                                                    input_object['StorageLayer'],
                                                                                    job['DatasetName'],
                                                                                    sourceSystem,
                                                                                    path_stage
                                                                            )
                                    input_object.pop('StorageLayer',None)
                                    input_array.append(input_object)
                            else:
                                input_object = {} 
                                if 'Input' in stage:
                                    input_object.update(stage['Input'])
                                    # If is the first Input and the Storage layer is landing, assume that we need to prepare a batch to process
                                    if input_object['StorageLayer'] == 'landing':
                                        path_stage = True
                                        input_object['StagePath'] = PipelineDagCreator.set_path(
                                                        storage_layer_prefix,
                                                        input_object['StorageLayer'],
                                                        job['DatasetName'],
                                                        sourceSystem,
                                                        False
                                                )
                                    else:
                                        path_stage = False
                                    input_object['InputPath'] = PipelineDagCreator.set_path(
                                                                                    storage_layer_prefix,
                                                                                    input_object['StorageLayer'],
                                                                                    job['DatasetName'],
                                                                                    sourceSystem,
                                                                                    path_stage
                                                                            )
                                    input_object.pop('StorageLayer',None)
                                    input_array.append(input_object)
                                else:
                                    raise PipelineDagCreatorError("There is no inputs to process")
                                # on spark-pyutils Input is an array
                            job_parameter['Input'] = input_array
                           
                                
                            # output config
                            output_object = {}
                            output_object.update(stage['Output'])
                            if 'Output' in job_stage:
                                output_object = PipelineDagCreator.merge_json(output_object, job_stage['Output'])
                            output_object['OutputPath'] = PipelineDagCreator.set_path(storage_layer_prefix,
                                                                                    output_object['StorageLayer'],
                                                                                    job['DatasetName'],
                                                                                    sourceSystem
                                                                            )
                            output_object['OutputDatabase'] = PipelineDagCreator.set_database(project_name, output_object['StorageLayer'])
                            output_object['OutputTable'] = PipelineDagCreator.set_table(job['DatasetName'], aliasSystem)
                            output_object.pop('StorageLayer',None)
                            output_job_dict[job['DatasetName']] = output_object
                            
                            job_parameter['Output'] = output_object
                            job_parameter['Active'] = True
                            job_parameter.pop('StageName',None)
                            j_jobs["Jobs"].append(job_parameter)
                            log.info("Job: {}".format(job_parameter))
                    
        log.info("Json Parameters generated")
        log.info(json.dumps(j_jobs))
        log.info("Validate jobs json parameters")
        validate(instance=j_jobs, schema=spu_schema)
        artifact_bucket = PipelineDagCreator.get_bucket_artifact(storage_layer_prefix)
        # artifact_bucket = self.__artifact_bucket
        log.info("THIS LOG IS NEW {}/{}")
        log.info("Save on artifact {}/{}".format(artifact_bucket, config_path_key))
        
        s3_conn = S3Hook(connection_id)
        s3_conn.load_bytes(bytes(json.dumps(j_jobs).encode('UTF-8')), 
                            config_path_key, 
                            bucket_name=artifact_bucket, 
                            replace=True, 
                            encrypt=False)
        
    @staticmethod
    def get_job_name(dataset_name, stage_name):
        return "{}_{}".format(dataset_name, stage_name)
        
    @staticmethod
    def get_bucket_artifact(storage_layer_prefix):
        return "{}{}".format(storage_layer_prefix, 'artifact')
        
    @staticmethod
    def set_source_system(job):
        return job['SourceSystem'] if "SourceSystem" in job else None
           
    
    @staticmethod
    def set_path(storage_layer_prefix, storageLayer, dataSetName, sourceSystem=None, stage=False, execution_ts=None, execution_label=None):
        path_bucket = 's3://{}{}'.format(storage_layer_prefix, storageLayer) 
        if stage:
            path_part_stage= "{}/stage".format(path_bucket)
        else:
            path_part_stage=path_bucket
        if sourceSystem:
            path_source= "{}/{}".format(path_part_stage, sourceSystem)
        else:
            path_source = path_part_stage
        path_dataset = '{}/{}'.format(path_source,dataSetName)    
        if execution_ts:    
            if execution_label:
                path_part_execution_ts="{}/{}={}".format(path_dataset,execution_label,execution_ts)
            else:
                path_part_execution_ts="{}/{}={}".format(path_dataset,'execution_ts',execution_ts)
        else:
            path_part_execution_ts=path_dataset
        return path_part_execution_ts

    @staticmethod
    def set_database(projectName, storageLayer):
        return projectName.replace("-","_")+"_"+storageLayer
    
    @staticmethod
    def set_table(datasetName, aliasSystem=None):
        if aliasSystem is None:
            return datasetName
        return aliasSystem+"_"+datasetName

    @staticmethod
    def set_udf(udf_name, job_stages):
        for stage in job_stages:
            if stage['ExternalFunction']['Module'] == udf_name:
                return stage
    
    def create_spark_job(self, task_id, job_name, job_profile, num_of_dpus = None):  
        args={
            "--job-name": job_name,
            "--config-path": self.__config_path,
            "--execution-datetime": self.__execution_ts,
            "--last-execution-datetime": self.__last_execution_ts,
            "--region": self.__region,
            "--conf": "{{ var.json.ComputeCommonArgumentsSpark.Conf }}",
            "--extra-jars":  "{{ var.json.ComputeCommonArgumentsSpark.ExtraJars.replace('"+self.__central_project_name+"','"+self.__project_name+"') }}",
            "--extra-py-files": "{{ var.json.ComputeCommonArgumentsSpark.ExtraPyFiles.replace('"+self.__central_project_name+"','"+self.__project_name+"') }}",
            "-v": "",
            "--enable-glue-datacatalog": "",
            "--is-glue-job": ""
        }
        if num_of_dpus:
            glue_task = AwsGlueJobOperator(
                task_id=task_id,  
                job_name=job_profile,
                num_of_dpus=num_of_dpus,
                aws_conn_id=self.__aws_conn_id,
                pool = self.__pool,
                script_args=args,
                dag=self.__dag) 
        else:
            glue_task = AwsGlueJobOperator(
                task_id=task_id,  
                job_name=job_profile,
                aws_conn_id=self.__aws_conn_id,
                pool = self.__pool,
                script_args=args,
                dag=self.__dag)
            
        return glue_task    
                
    def create_task_move_batch_files(self, task_id, job_name):  
        args={
            "--job-name": job_name,
            "--config-path": self.__config_path,
            "--execution-datetime": self.__execution_ts,
            "--last-execution-datetime": self.__last_execution_ts,
            "--region": self.__region,
            "--extra-py-files": "{{ var.json.ComputeCommonArgumentsPython.ExtraPyFiles.replace('"+self.__central_project_name+"','"+self.__project_name+"') }}",
            "-v": ""
        }
        log.info(args)
        glue_task = AwsGlueJobOperator(
            task_id=task_id,  
            job_name=self.__compute_profile_landing2stage,
            script_args=args,
            aws_conn_id=self.__aws_conn_id,
            num_of_dpus=0,
            pool = self.__pool,
            #region_name=self.__region,
            dag=self.__dag) 
        return glue_task   
        
    def create_task_spark_parameters(self):
        task = PythonOperator(
            task_id='GenerateAndValidateSparkParameters',
            python_callable=PipelineDagCreator.spark_parameters,
            op_kwargs={
                'pipeline': self.__pipeline,
                'jobs': self.__jobs,
                'project_name': self.__project_name,
                'config_path_key': self.__config_path_key,
                'storage_layer_prefix': self.__storage_layer_prefix,
                'connection_id': self.__aws_conn_id
                }
        )
        return task
        
    def create_dummy_external_task(self,task_id):
         return DummyOperator(
            task_id='External_{}'.format(task_id)
        )
    
    #def get_jobs_per_group(self, jobs):
    #    output = {}
    #    
    #    #get source system
    #    for x in jobs:
    #        output[x['GroupName']]=[]
    #    for x in jobs:
    #        output[x['GroupName']].append(x)
    #    return output
    
    def create_pipeline_task_group(self):
        if 'PipelineName' not in self.__pipeline:
            raise PipelineDagCreatorError("Key 'PipeliName' is not in dict pipeline")
        task_dict = {} 
        with TaskGroup(self.__pipeline['PipelineName']) as pipeline_task_group:
            with TaskGroup('InitPipeline') as init_pipeline:
                log.info(self.__project_name)
                create_parameter = self.create_task_spark_parameters()

            flow_array=[]    
            for id_flow, flow in enumerate(self.__pipeline['Flows']):
                if 'Stages' not in flow:
                    raise PipelineDagCreatorError("Key 'Stages' is not in dict flow")
                
                with TaskGroup(flow['FlowName']) as flow_instance:
                    flow_array.append(flow_instance)
                    if id_flow == 0:
                        with TaskGroup('PrepareBatch') as prepare_batch:
                            #jobs = self.get_jobs_per_group(self.__jobs[flow['FlowName']])
                            
                            for group,job_value in self.__jobs[flow['FlowName']].items():
                                with TaskGroup(group) as task_group_group:
                                    for job in job_value:
                                        if not job['External']:
                                            source_system = PipelineDagCreator.set_source_system(job)
                                            # Assume that the first stage in the flow has only one Input
                                            if "Input" in job["Stages"][0] and "ReadFormat" in job['Stages'][0]["Input"][0]:
                                                read_format = job["Stages"][0]["Input"][0]["ReadFormat"]
                                            elif "Input" in flow["Stages"][0] and "ReadFormat" in flow["Stages"][0]["Input"]:
                                                read_format =  flow["Stages"][0]["Input"]["ReadFormat"]
                                            else:
                                                raise PipelineDagCreatorError("ReadFormat is needed in Pipeline or Job definition")
                                            if "ExecutionPartitionLabel" in flow["Stages"][0]:
                                                execution_ts_label=flow["Stages"][0]["ExecutionPartitionLabel"]
                                            else:
                                                execution_ts_label=None
                                            if read_format not in ('jdbc', 'mongo'):
                                                job_name = PipelineDagCreator.get_job_name(job['DatasetName'], flow['Stages'][0]["StageName"])                                          
                                                task_dict[job['DatasetName']] = self.create_task_move_batch_files(job['DatasetName'], job_name)
                             
                    for stage_idx, stage in enumerate(flow['Stages']):
                        if 'StageName' not in stage:
                            raise PipelineDagCreatorError("Key 'StageName' is not in dict stage")
                        with TaskGroup(stage['StageName']):
                            #jobs = self.get_jobs_per_group(self.__jobs[flow['FlowName']])
                            
                            for group,job_value in self.__jobs[flow['FlowName']].items():
                                with TaskGroup(group) as task_group_group:
                                    for job in job_value:
                                        if job['DatasetName'] in task_dict:
                                            prev_task =  task_dict[job['DatasetName']]
                                        elif "Input" in job["Stages"][0] and "ReadFormat" in job['Stages'][0]["Input"][0]:
                                            if job['Stages'][0]["Input"][0]['ReadFormat'] in ('jdbc', 'mongo'):
                                                prev_task = init_pipeline                                                                                                                     
                                        else:
                                            prev_task = None
                                        job_name = PipelineDagCreator.get_job_name(job['DatasetName'], stage['StageName'])
                                        # Create task compute spark operator 
                                        if 'ComputeProfile' not in stage:
                                            raise PipelineDagCreatorError("ComputeProfile is needed in stage")
                                        
                                        num_of_dpus = None
                                        if 'NumOfDpus' in job:
                                            num_of_dpus = job['NumOfDpus']    
                                            
                                        if job['External']:
                                            if stage_idx == len(flow['Stages'])-1:
                                                task_name_dict = job['DatasetNameAlias'] if 'DatasetNameAlias' in job else job['DatasetName']
                                                task_dict[task_name_dict] = self.create_dummy_external_task(task_name_dict)
                                                prev_task = init_pipeline
                                    
                                        else:
                                            task_dict[job['DatasetName']] = self.create_spark_job(job['DatasetName'], job_name, self.__get_compute_profile(stage["ComputeProfile"]), num_of_dpus)    
                                        
                                        if not job['External'] and 'Input' in job['Stages'][0]:
                                            for in_dep in job['Stages'][0]['Input']:
                                                if 'DatasetName' in in_dep:
                                                    if in_dep['DatasetName'] not in task_dict:
                                                        raise PipelineDagCreatorError("Key '{}' is not in dict task".format(in_dep['DatasetName']))
                                                    task_dict[in_dep['DatasetName']] >> task_dict[job['DatasetName']]     
                                        if prev_task:
                                            prev_task >> task_dict[job['DatasetNameAlias'] if 'DatasetNameAlias' in job else job['DatasetName']]     
            init_pipeline >> prepare_batch
        return  pipeline_task_group          
