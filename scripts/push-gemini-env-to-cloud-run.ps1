# Applica GEMINI_API_KEY (e opz. GEMINI_MODEL) dal .env locale al servizio Cloud Run `dashboard`.
# Prerequisiti: gcloud auth login, progetto mediaexpertdashboard, servizio già deployato.
#
# Uso (dalla root repo):
#   .\scripts\push-gemini-env-to-cloud-run.ps1
#
# Nota: la chiave passa sulla riga di comando del processo locale; per produzione preferire Secret Manager
# e _SET_SECRETS in cloudbuild.yaml (es. GEMINI_API_KEY=gemini-api-key:latest).

$ErrorActionPreference = "Stop"
$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$EnvFile = Join-Path $ProjectRoot ".env"
if (-not (Test-Path $EnvFile)) {
    Write-Error ".env non trovato in $ProjectRoot"
}

$geminiKey = $null
$geminiModel = $null
Get-Content $EnvFile | ForEach-Object {
    $line = $_.Trim()
    if ($line -match '^\s*#' -or $line -eq "") { return }
    if ($line -match '^\s*GEMINI_API_KEY\s*=\s*(.+)$') { $geminiKey = $matches[1].Trim().Trim('"').Trim("'") }
    if ($line -match '^\s*GEMINI_MODEL\s*=\s*(.+)$') { $geminiModel = $matches[1].Trim().Trim('"').Trim("'") }
}

if (-not $geminiKey) {
    Write-Error "GEMINI_API_KEY mancante in .env"
}

$pair = "GEMINI_API_KEY=$geminiKey"
if ($geminiModel) {
    $pair = "$pair,GEMINI_MODEL=$geminiModel"
}

Write-Host "Aggiornamento Cloud Run dashboard (europe-west1, mediaexpertdashboard)..." -ForegroundColor Cyan
gcloud run services update dashboard `
    --project=mediaexpertdashboard `
    --region=europe-west1 `
    --update-env-vars="$pair"
if ($LASTEXITCODE -ne 0) {
    Write-Host "gcloud non riuscito (es. token scaduto). Esegui: .\scripts\gcloud-browser-login.ps1 poi rilancia questo script." -ForegroundColor Red
    exit $LASTEXITCODE
}
Write-Host "Fatto. Verifica: Console Cloud Run > dashboard > Variabili e segreti." -ForegroundColor Green
