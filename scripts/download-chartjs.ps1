# Scarica Chart.js 3.9.1 nella cartella static se non presente.
# Esegui dalla root del progetto.

$ErrorActionPreference = "Stop"
$root = if ($PSScriptRoot) { Split-Path $PSScriptRoot } else { Get-Location }
$vendorDir = Join-Path $root "app\static\js\vendor"
$chartFile = Join-Path $vendorDir "chart.umd.min.js"

if (Test-Path $chartFile) {
  Write-Host "Chart.js gia' presente: $chartFile" -ForegroundColor Green
  exit 0
}

Write-Host "Download Chart.js 3.9.1..." -ForegroundColor Cyan
New-Item -ItemType Directory -Force -Path $vendorDir | Out-Null

try {
  Invoke-WebRequest -Uri "https://cdn.jsdelivr.net/npm/chart.js@3.9.1/dist/chart.min.js" -OutFile $chartFile -UseBasicParsing
  Write-Host "Chart.js scaricato in $chartFile" -ForegroundColor Green
} catch {
  Write-Host "jsdelivr fallito, provo unpkg..." -ForegroundColor Yellow
  Invoke-WebRequest -Uri "https://unpkg.com/chart.js@3.9.1/dist/chart.min.js" -OutFile $chartFile -UseBasicParsing
  Write-Host "Chart.js scaricato in $chartFile" -ForegroundColor Green
}
