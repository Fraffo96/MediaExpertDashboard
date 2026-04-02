# Avvio dashboard locale: root repo, PYTHONPATH corretto, porta 8000 libera, niente BRAND_LOGOS_USE_STATIC accidentale.
# Uso: .\scripts\run-local-server.ps1   (dalla root o da scripts)
#
# Loghi: default = GET /brand-logo/id.png (server scarica GCS via httpx). Su rete aziendale spesso Python
# non vede Internet mentre il browser sì → immagine 502. Opzioni:
#   A) Decommentare la riga sotto: stessa cosa che BRAND_LOGOS_SKIP_PROXY=1 in .env (.env.example).
#   B) Impostare HTTP_PROXY / HTTPS_PROXY per il processo Python.
#   C) Lasciare tutto com'è: se GCS fallisce ma esiste app/static/img/brands/<id>.png, il proxy serve quel file.

$ErrorActionPreference = "Stop"
$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location $ProjectRoot

$env:PYTHONPATH = $ProjectRoot
Remove-Item Env:BRAND_LOGOS_USE_STATIC -ErrorAction SilentlyContinue
# $env:BRAND_LOGOS_SKIP_PROXY = "1"

# Libera 8000
Get-NetTCPConnection -LocalPort 8000 -State Listen -ErrorAction SilentlyContinue |
    Select-Object -ExpandProperty OwningProcess -Unique |
    ForEach-Object { Stop-Process -Id $_ -Force -ErrorAction SilentlyContinue }

Start-Sleep -Seconds 1

Write-Host "Root: $ProjectRoot" -ForegroundColor Cyan
Write-Host "PYTHONPATH=$ProjectRoot" -ForegroundColor Gray
if ($env:BRAND_LOGOS_SKIP_PROXY) {
    Write-Host "BRAND_LOGOS_SKIP_PROXY=$env:BRAND_LOGOS_SKIP_PROXY (img punta a HTTPS GCS, non al proxy locale)" -ForegroundColor Yellow
}
Write-Host "Avvio uvicorn http://127.0.0.1:8000 ..." -ForegroundColor Green

python -m uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
