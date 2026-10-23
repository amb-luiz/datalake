# Spark-pyutil.

*Spark python utilities for simple ingestion through JSON parameters.*
This module have some details about how you should adapt this project to work like Directories and Datamask

## Table of contents

- [Dags Directory]
- [Dags development details]
- [Pipeline.py datamask modification]

### Dags Directory 

Your dags should be stored as /dags/<PROJECT_NAME>/pipelines...
Remember changing "-" for "_" when renaming

### Dags development details

When developing your dags, everytime you need to import something inside pipelines, you should add the <PROJECT_NAME> 
Remember changing "-" for "_" when renaming

Example:
- pipelines.parameters.project_datapipeline_mensal.integration.jobs import * ==> <project_name>.pipelines.parameters.project_datapipeline_mensal.integration.jobs import *


### Pipeline.py modification

Inside pipeline.py is defined the default job used by constructor to build your dags.

If datamask function is used, it's need to change the location where ReverseDataSet will be located.

To do this, follow the following steps:

1 - Open pipeline.py 
2 - Look for "ExternalFunction"
3 - Check if inside "ExternalFunction", you are calling "'Module': 'spark_pyutil_functions.datamask'"

Example:
'ExternalFunction':{
    'Module': 'spark_pyutil_functions.datamask',
     'Parameters': 
        [{# Adicionar macros com variaveis de airflow
         "ReverseDatasetPath": "{{ var.value.ReverseDatasetPath }}",
         "DomainsPath": "{{ var.value.DomainsPath }}",
         "SaltsPath": "{{ var.value.SaltsPath }}" 
		}]
}

4 - If the answer to 3 is "yes", change the Parameter 'ReverseDatasetPath' as follows:

'ExternalFunction':{
    'Module': 'spark_pyutil_functions.datamask',
     'Parameters': 
        [{# Adicionar macros com variaveis de airflow
         "ReverseDatasetPath": "{{ var.value.ReverseDatasetPath.replace('<CENTRAL_PROJECT_NAME>','<PROJECT_NAME>') }}",
         "DomainsPath": "{{ var.value.DomainsPath }}",
         "SaltsPath": "{{ var.value.SaltsPath }}" 
		}]
},


Change the "CENTRAL_PROJECT_NAME" and  "PROJECT_NAME" by the values. This can be found executing "env | grep PROJECT_NAME" in Terminal.

Example:
$ env | grep PROJECT_NAME
CENTRAL_PROJECT_NAME=airflow-unificado
PROJECT_NAME=cdk-pista5

Here you do not have to change "-" for "_"

## Production

Production deployment for this repository is documented in `PRODUCTION.md`.

## Lakehouse Local

The local stack for `MinIO`, `Trino` and `dbt` is documented in `MINIO_TRINO_DBT.md`.

## Server Deploy

Server deployment for `airflow-pipeline` and `pyspark-library` is documented in `SERVER_DEPLOY.md`.

## Airflow + MinIO

Server deployment for `airflow-pipeline` with `MinIO` is documented in `MINIO_SERVER_DEPLOY.md`.
