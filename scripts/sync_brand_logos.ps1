# Copia loghi da una cartella locale verso app/static/img/brands/
# Uso: rinominare (o mappare) ogni PNG come <brand_id>.png
# Esempio:  .\scripts\sync_brand_logos.ps1 -SourceDir "C:\Users\TUO\Downloads\Loghi"
param(
    [string]$SourceDir = "$env:USERPROFILE\Downloads\Loghi",
    [string]$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path,
    [switch]$WhatIf
)
$dest = Join-Path $ProjectRoot "app\static\img\brands"
if (-not (Test-Path $dest)) { New-Item -ItemType Directory -Path $dest | Out-Null }
if (-not (Test-Path $SourceDir)) {
    Write-Host "Cartella non trovata: $SourceDir"
    exit 1
}
# Copia tutti i PNG mantenendo il nome: devono essere già <id>.png oppure rinominare a mano
Get-ChildItem -Path $SourceDir -Filter "*.png" -File | ForEach-Object {
    $target = Join-Path $dest $_.Name
    Write-Host "$($_.FullName) -> $target"
    if (-not $WhatIf) { Copy-Item -LiteralPath $_.FullName -Destination $target -Force }
}
Write-Host "Fatto. Verifica che i nomi file siano brand_id (es. 20.png per Acer)."
