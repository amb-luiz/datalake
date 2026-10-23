# MinIO, Trino e dbt

Este repositorio passa a ter dois modos de operacao:

1. `S3`: mantem o comportamento atual para AWS
2. `MinIO`: habilita execucao local ou ambiente compativel com S3 usando `MinIO`, `Trino` e `dbt`

## O que foi adicionado

- `docker-compose.lakehouse.yml`
  Sobe `MinIO`, `Hive Metastore`, `Postgres` para metastore e `Trino`
- `configs/storage/minio.env.example`
  Exemplo de variaveis para backend `MinIO`
- `configs/storage/s3.env.example`
  Exemplo de variaveis para backend `S3`
- `dbt/`
  Base do projeto `dbt` para execucao sobre `Trino`
- `pyspark-library/src/pyspark_library/storage.py`
  Base unica de configuracao de storage para `S3` e `MinIO`

## Subir a stack local

```powershell
docker compose -f docker-compose.lakehouse.yml up -d
```

Servicos expostos:

- `MinIO API`: `http://localhost:9000`
- `MinIO Console`: `http://localhost:9001`
- `Trino`: `http://localhost:8081`
- `Hive Metastore`: `localhost:9083`

Credenciais padrao locais:

- `MinIO user`: `minioadmin`
- `MinIO password`: `minioadmin`

Buckets criados automaticamente:

- `planejamento-landing`
- `planejamento-raw`
- `planejamento-integration`
- `planejamento-business`

## Como pensar a separacao S3 x MinIO

Nao crie duas copias do mesmo codigo de transformacao.

O ponto correto de separacao e:

- configuracao de storage
- configuracao de deploy
- configuracao do Spark
- catalogo analitico para leitura SQL

As transformacoes PySpark devem continuar as mesmas sempre que possivel.

## Proximo passo recomendado

O proximo incremento no codigo deve ser integrar o modulo unico de configuracao de storage para o Spark nos pontos de criacao de sessao, com suporte a:

- `s3`
- `minio`

Esse modulo ja foi criado em `pyspark-library/src/pyspark_library/storage.py` e gera as `Spark configs` de `s3a` conforme o backend escolhido.
