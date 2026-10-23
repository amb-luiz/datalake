# Airflow + MinIO no servidor

Para o seu caso, o desenho correto e:

- `Airflow` em Docker
- `Postgres` em Docker
- `Redis` em Docker
- `MinIO` em Docker
- quatro buckets no `MinIO` com os mesmos nomes das camadas:
  - `landing`
  - `raw`
  - `integration`
  - `business`

Voce nao precisa publicar nada na AWS.

## Arquivos usados

- `airflow-pipeline/docker-compose.yaml`
  Stack base do Airflow
- `airflow-pipeline/docker-compose.minio.yml`
  Override para subir o `MinIO`
- `airflow-pipeline/.env.minio.example`
  Exemplo de ambiente para esse modo

## Como subir no servidor

Dentro de `airflow-pipeline`:

```bash
cp .env.minio.example .env
docker compose -f docker-compose.yaml -f docker-compose.minio.yml up -d --build
```

## O que sobe

- `postgres`
- `redis`
- `airflow-webserver`
- `airflow-scheduler`
- `airflow-worker`
- `airflow-triggerer`
- `minio`
- `minio-init`

## Portas

- `8080`: Airflow
- `9000`: API do MinIO
- `9001`: console do MinIO

## Buckets criados automaticamente

O container `minio-init` cria:

- `landing`
- `raw`
- `integration`
- `business`

## O que ja foi integrado

O projeto foi adaptado para:

- executar jobs Spark localmente a partir do worker do Airflow
- parar de depender de `AWS Glue` como executor
- gerar paths nas quatro camadas usando:
  - `s3a://landing`
  - `s3a://raw`
  - `s3a://integration`
  - `s3a://business`
- usar `MinIO` como endpoint S3 compativel

Arquivos principais da adaptacao:

- `airflow-pipeline/plugins/constructors/pipeline_dag_creator.py`
- `airflow-pipeline/plugins/operators/local_spark.py`
- `pyspark-library/src/spark_pyutil/driver.py`
- `pyspark-library/src/spark_pyutil/driver_prepare_batch.py`
- `pyspark-library/src/spark_pyutil/utility.py`

## Como a execucao funciona agora

O fluxo ficou assim:

1. o DAG gera o `config.json` localmente em `/opt/airflow/logs/config/...`
2. o worker do Airflow executa `spark_pyutil.driver` localmente
3. o Spark acessa o `MinIO` via `s3a`
4. os dados sao lidos e gravados nos buckets `landing`, `raw`, `integration` e `business`

## Observacao importante

Ainda podem existir dependencias externas de ambiente, principalmente:

- arquivos de configuracao de `DomainsPath`
- arquivos de configuracao de `SaltsPath`
- conexoes JDBC que antes dependiam de `GlueConnection`

Para pipelines que usam apenas arquivos e transformacoes locais, a integracao principal ja ficou feita.
