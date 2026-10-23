# dbt

Este diretorio contem a base do projeto `dbt` para modelagem via `Trino`.

## Arquivos principais

- `dbt_project.yml`: configuracao do projeto
- `profiles.yml`: profile local do `dbt` parametrizado por variaveis de ambiente
- `profiles.yml.example`: referencia do mesmo profile
- `requirements.txt`: dependencias do `dbt` para `Trino`
- `models/staging`: modelos de estagio
- `models/marts`: modelos analiticos

## Pre-requisitos

- `Docker Desktop` instalado e com o comando `docker` disponivel no PATH
- `Python 3.13+`

## Bootstrap local no Windows

No diretorio raiz do projeto:

```powershell
.\scripts\setup-dbt-trino.ps1
```

O script:

1. cria `.venv-trino-dbt`
2. instala `dbt-core` e `dbt-trino`
3. define `DBT_PROFILES_DIR` apontando para `dbt`
4. executa `dbt deps`
5. mostra os proximos comandos de validacao

## Variaveis de ambiente

O `profiles.yml` ja funciona com os valores locais padrao da stack:

- `DBT_TRINO_HOST=localhost`
- `DBT_TRINO_PORT=8081`
- `DBT_TRINO_CATALOG=iceberg`
- `DBT_TRINO_SCHEMA=planejamento_caixa`
- `DBT_TRINO_USER=trino`
- `DBT_TRINO_METHOD=none`

Se precisar sobrescrever:

```powershell
$env:DBT_TRINO_HOST = "localhost"
$env:DBT_TRINO_PORT = "8081"
$env:DBT_TRINO_CATALOG = "iceberg"
$env:DBT_TRINO_SCHEMA = "planejamento_caixa"
```

## Execucao manual

```powershell
docker compose -f .\docker-compose.lakehouse.yml up -d
.venv-trino-dbt\Scripts\Activate.ps1
$env:DBT_PROFILES_DIR = (Resolve-Path .\dbt).Path
dbt debug --project-dir .\dbt
dbt run --project-dir .\dbt
dbt test --project-dir .\dbt
```
