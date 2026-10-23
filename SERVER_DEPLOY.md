# Deploy no servidor

Este projeto pode rodar no servidor apenas com:

- `airflow-pipeline`
- `pyspark-library`

Para esse cenário, voce nao precisa subir `MinIO`, `dbt` nem `Trino`.

Se o seu destino for `MinIO`, use o guia:

`MINIO_SERVER_DEPLOY.md`

## O que vai rodar

No servidor, voce vai subir:

- `postgres`
- `redis`
- `airflow-webserver`
- `airflow-scheduler`
- `airflow-worker`
- `airflow-triggerer`

O codigo da `pyspark-library` fica empacotado no artefato usado pelas pipelines.

## Pre-requisitos do servidor

Instale no servidor:

- Docker
- Docker Compose
- AWS CLI
- Git

Tambem garanta:

- acesso aos buckets S3 do projeto
- credenciais AWS validas
- portas liberadas, pelo menos `8080`

## Estrutura recomendada no servidor

Exemplo:

```text
/opt/apps/datalake
```

## Passo a passo

### 1. Copiar o projeto

```bash
cd /opt
git clone <URL_DO_REPOSITORIO> datalake
cd /opt/apps/datalake
```

Se o projeto ja estiver no servidor:

```bash
cd /opt/apps/datalake
git pull
```

### 2. Criar o arquivo de ambiente do Airflow

Use como base:

`airflow-pipeline/.env.server.example`

Copie para:

`airflow-pipeline/.env`

### 3. Configurar credenciais AWS

Opcao 1, recomendada:

- usar role da instancia

Opcao 2:

- configurar `~/.aws/credentials`

Se quiser manter o modo atual do projeto local, tambem pode criar:

`airflow-pipeline/airflow/credentials`

Mas no servidor o ideal e usar credenciais fora do repositorio.

### 4. Publicar os artefatos do projeto

Da raiz do repositorio:

```bash
export CENTRAL_PROJECT_NAME=airflow-unificado
export PROJECT_NAME=cdl-planejamentocaixa
export ENV=prod
export REGION=sa-east-1
export ACCOUNT=123456789012

powershell -File ./deploy-production.ps1
```

Se o servidor for Linux puro e nao tiver PowerShell, execute os scripts direto:

```bash
cd pyspark-library
./scripts/deploy.sh

cd ../airflow-pipeline
./scripts/deploy.sh
```

### 5. Subir o Airflow no servidor

```bash
cd /opt/apps/datalake/airflow-pipeline
docker compose up -d --build
```

### 6. Validar

Verifique os containers:

```bash
docker compose ps
```

Verifique os logs:

```bash
docker compose logs airflow-webserver
docker compose logs airflow-scheduler
docker compose logs airflow-worker
```

Abra no navegador:

```text
http://IP_DO_SERVIDOR:8080
```

## Variaveis importantes

As principais variaveis para o projeto sao:

- `CENTRAL_PROJECT_NAME`
- `PROJECT_NAME=cdl-planejamentocaixa`
- `ENV=prod`
- `REGION`
- `ACCOUNT`
- `AIRFLOW_UID`
- `AIRFLOW_GID`

## Observacoes

- o `docker-compose.yaml` atual do Airflow veio de um modelo de desenvolvimento
- ele pode rodar no servidor, mas nao e um desenho ideal de producao corporativa
- para colocar de pe rapido, ele resolve
- para ambiente mais serio, o correto depois e separar logs, secrets, banco e executor

## Melhor caminho para o seu caso

Se o objetivo agora e colocar pra rodar rapido no servidor, faca assim:

1. subir o projeto com `docker compose` dentro de `airflow-pipeline`
2. manter os artefatos da `pyspark-library` no S3
3. deixar o Airflow consumir os DAGs e os artefatos como o projeto ja faz hoje

Isso e o menor caminho entre o estado atual do repositorio e um ambiente funcional no servidor.
