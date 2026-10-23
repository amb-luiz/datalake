# Producao

Este projeto sobe em producao publicando dois itens:

1. a `pyspark-library`, que gera os artefatos Spark/Python e envia para o bucket do projeto
2. o `airflow-pipeline`, que envia os DAGs para o bucket do Airflow central

O `docker-compose` da pasta `airflow-pipeline` serve para desenvolvimento local. Producao aqui e feita por upload para S3.

## Valores do projeto

Pelo que existe hoje no repositorio, estes valores ja podem ser usados:

- `PROJECT_NAME=cdl-planejamentocaixa`
- `ENV=prod`

O nome interno dos DAGs esta em `airflow-pipeline/dags/cdl_planejamentocaixa`. No deploy, o script converte `cdl-planejamentocaixa` para `cdl_planejamentocaixa` automaticamente.

Voce ainda precisa confirmar no seu ambiente:

- `CENTRAL_PROJECT_NAME`
- `REGION`
- `ACCOUNT`
- `AWS_PROFILE` se usar perfil local

Exemplo mais provavel:

```powershell
$env:CENTRAL_PROJECT_NAME = "airflow-unificado"
$env:PROJECT_NAME = "cdl-planejamentocaixa"
$env:ENV = "prod"
$env:REGION = "sa-east-1"
$env:ACCOUNT = "123456789012"
$env:AWS_PROFILE = "prod"
```

## Como publicar

Na raiz do projeto, rode:

```powershell
.\deploy-production.ps1
```

Se quiser subir apenas a library:

```powershell
.\deploy-production.ps1 -SkipAirflow
```

Se quiser subir apenas os DAGs:

```powershell
.\deploy-production.ps1 -SkipLibrary
```

## Para onde os arquivos vao

Library:

```text
s3://cdl-planejamentocaixa-<REGION>-<ACCOUNT>-prod-artifact/spark-pyutil
```

DAGs:

```text
s3://<CENTRAL_PROJECT_NAME>-<REGION>-<ACCOUNT>-prod-artifact/airflow/dags/cdl_planejamentocaixa/pipelines
```

## O que cada parte faz

`pyspark-library/scripts/deploy.sh`

- faz o build da library com Docker
- gera os artefatos em `artifact/spark-dist` e `artifact/python-dist`
- envia os arquivos para o bucket do projeto

`airflow-pipeline/scripts/deploy.sh`

- monta o bucket de artefatos do Airflow central
- remove a versao anterior dos DAGs desse projeto
- envia os DAGs atuais para o prefixo correto

## Pre-requisitos

Antes do deploy, confirme:

- `bash` funcionando no Windows
- `docker` instalado e rodando
- `aws cli` instalado e autenticado
- permissao de escrita nos buckets S3

## Conferencia depois do deploy

Depois de publicar, valide:

1. se os arquivos novos apareceram no S3
2. se o Airflow carregou os DAGs
3. se um DAG do projeto executa sem erro

## Credenciais locais

O arquivo `airflow-pipeline/airflow/credentials` ficou restrito ao uso local. Ele nao e mais copiado para dentro da imagem Docker.

Se precisar rodar o Airflow localmente, use este arquivo como modelo:

`airflow-pipeline/airflow/credentials.example`
