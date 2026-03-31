<#
.SYNOPSIS
  Copia app/static/img/brands/*.png verso gs://<bucket>/brands/ (richiede gsutil).

.PARAMETER BucketName
  Nome bucket senza prefisso gs:// (default: mediaexpertdashboard-brand-logos o da env BRAND_LOGOS_BUCKET).
#>
param(
    [string] $BucketName = ""
)

$ErrorActionPreference = "Stop"
$root = Split-Path $PSScriptRoot -Parent
$localDir = Join-Path $root "app\static\img\brands"

if (-not $BucketName) {
    $BucketName = $env:BRAND_LOGOS_BUCKET
}
if (-not $BucketName) {
    $BucketName = "mediaexpertdashboard-brand-logos"
}

$dest = "gs://$BucketName/brands"
Write-Host "Origine: $localDir"
Write-Host "Destinazione: $dest"

if (-not (Test-Path $localDir)) {
    throw "Cartella non trovata: $localDir"
}

$png = Get-ChildItem -Path $localDir -Filter "*.png" -File
if (-not $png) {
    Write-Warning "Nessun file .png in $localDir — aggiungere i loghi o sincronizzare manualmente."
    exit 0
}

gsutil -m cp "$localDir\*.png" $dest/
Write-Host "Fatto. URL pubblico atteso: https://storage.googleapis.com/$BucketName/brands/<brand_id>.png"
