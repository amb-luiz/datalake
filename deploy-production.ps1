param(
    [switch]$SkipLibrary,
    [switch]$SkipAirflow
)

$ErrorActionPreference = "Stop"

$requiredVars = @(
    "CENTRAL_PROJECT_NAME",
    "PROJECT_NAME",
    "ENV",
    "REGION",
    "ACCOUNT"
)

$missingVars = @()
foreach ($name in $requiredVars) {
    if ([string]::IsNullOrWhiteSpace([Environment]::GetEnvironmentVariable($name))) {
        $missingVars += $name
    }
}

if ($missingVars.Count -gt 0) {
    throw "Missing required environment variables: $($missingVars -join ', ')"
}

$root = Split-Path -Parent $MyInvocation.MyCommand.Path

function Invoke-BashDeploy {
    param(
        [Parameter(Mandatory = $true)]
        [string]$RelativeScript
    )

    $scriptPath = Join-Path $root $RelativeScript
    if (-not (Test-Path $scriptPath)) {
        throw "Script not found: $scriptPath"
    }

    & bash $scriptPath
    if ($LASTEXITCODE -ne 0) {
        throw "Script failed with exit code ${LASTEXITCODE}: $scriptPath"
    }
}

if (-not $SkipLibrary) {
    Write-Host "Deploying pyspark-library artifacts..."
    Invoke-BashDeploy "pyspark-library/scripts/deploy.sh"
}

if (-not $SkipAirflow) {
    Write-Host "Deploying Airflow DAGs..."
    Invoke-BashDeploy "airflow-pipeline/scripts/deploy.sh"
}

Write-Host "Production deploy completed."
