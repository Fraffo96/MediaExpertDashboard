# Svuota Redis/cache sul Cloud Run e riscalda (dati aggiornati da BigQuery).
# Prerequisiti: .env in root con PREWARM_TOKEN (stesso secret del servizio).
# Opzionale: DASHBOARD_BASE_URL=https://....run.app  (altrimenti prova gcloud)
#
# Uso da root repo:
#   .\scripts\clear_remote_cache_and_prewarm.ps1
#   .\scripts\clear_remote_cache_and_prewarm.ps1 -FlushRedis#
# -FlushRedis invia {"flush_redis_db":true} (serve ENABLE_ADMIN_REDIS_FLUSHDB=1 su Cloud Run).

param(
    [switch] $FlushRedis,
    [string] $PreWarmToken = "",
    [string] $BaseUrl = ""
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $Root

$envPath = Join-Path $Root ".env"
if (-not (Test-Path $envPath)) { throw "Manca .env in $Root" }

$token = $PreWarmToken.Trim()
if (-not $token -and $env:PREWARM_TOKEN) { $token = $env:PREWARM_TOKEN.Trim() }
if (-not $token) {
    Get-Content $envPath | ForEach-Object {
        if ($_ -match '^\s*PREWARM_TOKEN\s*=\s*(.+)\s*$') {
            $token = $Matches[1].Trim().Trim('"').Trim("'")
        }
    }
}
if (-not $token) {
    throw @"
PREWARM_TOKEN mancante. Scegli una opzione:
  1) Aggiungi PREWARM_TOKEN=... al file .env (stesso valore del secret prewarm-token su Cloud Run / Secret Manager)
  2) `$env:PREWARM_TOKEN = '...' prima di lanciare lo script
  3) .\scripts\clear_remote_cache_and_prewarm.ps1 -PreWarmToken '...'
"@
}

$base = if ($BaseUrl) { $BaseUrl.Trim().TrimEnd('/') } elseif ($env:DASHBOARD_BASE_URL) { $env:DASHBOARD_BASE_URL.Trim().TrimEnd('/') } else { "" }
if (-not $base) {
    $proj = ""
    Get-Content $envPath | ForEach-Object {
        if ($_ -match '^\s*GCP_PROJECT_ID\s*=\s*(.+)\s*$') { $proj = $Matches[1].Trim() }
    }
    if (-not $proj) { $proj = "mediaexpertdashboard" }
    Write-Host "DASHBOARD_BASE_URL assente: interrogo gcloud run services (progetto $proj)..."
    $base = (gcloud run services list --project $proj --platform managed --format="value(status.url)" 2>$null | Select-Object -First 1).Trim().TrimEnd('/')
}
if (-not $base) { throw "URL servizio vuoto. Imposta DASHBOARD_BASE_URL=https://... nel .env" }

Write-Host "Service: $base"
$hdr = @{ "X-Prewarm-Token" = $token; "Content-Type" = "application/json" }

$body = if ($FlushRedis) { '{"flush_redis_db":true}' } else { '{}' }
Write-Host "POST /internal/clear-cache ..."
$clear = Invoke-RestMethod -Uri "$base/internal/clear-cache" -Method Post -Headers $hdr -Body $body
$clear | ConvertTo-Json -Compress | Write-Host

Write-Host "GET /internal/prewarm ..."
$warm = Invoke-RestMethod -Uri "$base/internal/prewarm" -Method Get -Headers @{ "X-Prewarm-Token" = $token }
$warm | ConvertTo-Json -Compress | Write-Host
Write-Host "Fatto."
