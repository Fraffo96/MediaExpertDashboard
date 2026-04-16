# Popola Redis (e RAM sul container) chiamando GET /internal/prewarm su Cloud Run.
# Prerequisiti nel .env (o variabili d'ambiente):
#   PREWARM_TOKEN        — stesso valore di Cloud Run (Secret o env)
#   DASHBOARD_BASE_URL   — opzionale; altrimenti lo script Python risolve l'URL con gcloud
#
# Esempio:
#   .\scripts\prewarm-redis-cache.ps1
#   .\scripts\prewarm-redis-cache.ps1 --prewarm-timeout 900

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
python (Join-Path $root "scripts\remote_admin_flush_cache.py") --prewarm-only @args
exit $LASTEXITCODE
