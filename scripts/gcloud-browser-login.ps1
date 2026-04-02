# Avvia gcloud auth login in una finestra CMD visibile e apre il browser di sistema.
# Usare sempre questo script su Windows se "gcloud auth login" da terminale integrato non apre il browser.
#
# Uso: .\scripts\gcloud-browser-login.ps1
#      .\scripts\gcloud-browser-login.ps1 -ProjectId "mio-progetto"

param(
    [string] $ProjectId = "mediaexpertdashboard"
)

$ErrorActionPreference = "Stop"

$candidates = @(
    "$env:LOCALAPPDATA\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd",
    "$env:ProgramFiles\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd",
    "${env:ProgramFiles(x86)}\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd"
)

$gcloudCmd = $candidates | Where-Object { Test-Path -LiteralPath $_ } | Select-Object -First 1
if (-not $gcloudCmd) {
    $fromWhere = (& where.exe gcloud.cmd 2>$null | Select-Object -First 1)
    if ($fromWhere) { $gcloudCmd = $fromWhere }
}

if (-not $gcloudCmd -or -not (Test-Path -LiteralPath $gcloudCmd)) {
    Write-Host "[ERR] gcloud.cmd non trovato. Installa Google Cloud SDK." -ForegroundColor Red
    exit 1
}

$bat = Join-Path $env:TEMP "mediaexpert-gcloud-login.bat"
$lines = @(
    '@echo off',
    'title Google Cloud SDK - Login',
    'echo Avvio login nel browser...',
    ("call `"{0}`" auth login" -f $gcloudCmd),
    'if errorlevel 1 (',
    '  echo Login annullato o errore.',
    '  goto :end',
    ')',
    ("call `"{0}`" config set project {1}" -f $gcloudCmd, $ProjectId),
    'echo.',
    'echo Progetto attivo dopo login: ' + $ProjectId,
    ':end',
    'echo.',
    'pause'
)
$lines | Set-Content -Path $bat -Encoding ASCII

# Esecuzione .bat = nuova console visibile (stesso effetto di doppio clic)
Start-Process -FilePath $bat -WorkingDirectory $env:USERPROFILE

Write-Host ""
Write-Host "Finestra ""Google Cloud SDK - Login"" aperta. Completa il login nel browser." -ForegroundColor Green
Write-Host "Se non si apre il browser, usa quella finestra e leggi eventuali errori." -ForegroundColor Gray
Write-Host ""
