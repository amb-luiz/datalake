[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true

$projectRoot = Split-Path -Parent $PSScriptRoot
$venvPath = Join-Path $projectRoot ".venv-trino-dbt"
$dbtPath = Join-Path $projectRoot "dbt"

Write-Host "Validando pre-requisitos..."

if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
    throw "Python nao encontrado no PATH."
}

$dockerAvailable = $null -ne (Get-Command docker -ErrorAction SilentlyContinue)

if (-not (Test-Path $venvPath)) {
    Write-Host "Criando ambiente virtual em $venvPath"
    if ($null -ne (Get-Command virtualenv -ErrorAction SilentlyContinue)) {
        virtualenv $venvPath
    } else {
        python -m virtualenv $venvPath
    }
}

$pythonExe = Join-Path $venvPath "Scripts\python.exe"
$dbtExe = Join-Path $venvPath "Scripts\dbt.exe"

if (-not (Test-Path $pythonExe)) {
    throw "Ambiente virtual invalido: $pythonExe nao encontrado."
}

cmd /c "`"$pythonExe`" -m pip --version >nul 2>nul"
$venvHasPip = $LASTEXITCODE -eq 0

if (-not $venvHasPip) {
    Write-Warning "A virtualenv existente esta sem pip. Recriando com virtualenv --clear."
    if ($null -ne (Get-Command virtualenv -ErrorAction SilentlyContinue)) {
        virtualenv --clear $venvPath
    } else {
        python -m virtualenv --clear $venvPath
    }

    cmd /c "`"$pythonExe`" -m pip --version >nul 2>nul"
    $venvHasPip = $LASTEXITCODE -eq 0

    if (-not $venvHasPip) {
        throw "A virtualenv foi criada sem pip em $venvPath"
    }
}

Write-Host "Atualizando pip..."
& $pythonExe -m pip install --upgrade pip

Write-Host "Instalando dependencias do dbt..."
& $pythonExe -m pip install -r (Join-Path $dbtPath "requirements.txt")

if (-not (Test-Path $dbtExe)) {
    throw "dbt.exe nao foi instalado em $dbtExe"
}

$env:DBT_PROFILES_DIR = $dbtPath

Write-Host "Executando dbt deps..."
& $dbtExe deps --project-dir $dbtPath

Write-Host ""
Write-Host "Bootstrap concluido."
if ($dockerAvailable) {
    Write-Host "Para subir a stack:"
    Write-Host "  docker compose -f `"$projectRoot\docker-compose.lakehouse.yml`" up -d"
} else {
    Write-Warning "Docker nao encontrado no PATH. Instale o Docker Desktop para subir MinIO, Hive Metastore e Trino."
}
Write-Host ""
Write-Host "Para validar o dbt:"
Write-Host "  .venv-trino-dbt\Scripts\Activate.ps1"
Write-Host "  `$env:DBT_PROFILES_DIR = (Resolve-Path .\dbt).Path"
Write-Host "  dbt debug --project-dir .\dbt"
Write-Host "  dbt run --project-dir .\dbt"
