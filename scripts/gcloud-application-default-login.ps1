# Rinnova Application Default Credentials (ADC) in una finestra CMD visibile.
# gcloud auth application-default login apre il browser di sistema (nessun --no-launch-browser).
# Utile quando ADC utente e' scaduto; lo script rimuove GOOGLE_APPLICATION_CREDENTIALS solo per questa sessione
# cosi' gcloud non chiede il conflitto con la chiave SA del repo.
#
# Dopo il login, per usare ADC con i client Python SENZA service account: non impostare GOOGLE_APPLICATION_CREDENTIALS in quella shell.
# Per BigQuery/dashboard con SA: continua a usare .env con GOOGLE_APPLICATION_CREDENTIALS=credentials\bigquery-sa.json
#
# Uso: .\scripts\gcloud-application-default-login.ps1

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

$bat = Join-Path $env:TEMP "mediaexpert-gcloud-adc-login.bat"
$lines = @(
    '@echo off',
    'title Google Cloud SDK - Application Default Credentials',
    'REM Evita conflitto: durante login ADC non usare la variabile che punta al SA',
    'set GOOGLE_APPLICATION_CREDENTIALS=',
    'echo Login ADC — si apre il browser per accedere con il tuo account Google...',
    ("call `"{0}`" auth application-default login" -f $gcloudCmd),
    'if errorlevel 1 (',
    '  echo Login ADC annullato o errore.',
    '  goto :end',
    ')',
    ("call `"{0}`" config set project {1}" -f $gcloudCmd, $ProjectId),
    'echo.',
    'echo Progetto: ' + $ProjectId,
    ':end',
    'echo.',
    'pause'
)
$lines | Set-Content -Path $bat -Encoding ASCII

Start-Process -FilePath $bat -WorkingDirectory $env:USERPROFILE

Write-Host ""
Write-Host "Finestra ""Application Default Credentials"" aperta. Completa il login nel browser." -ForegroundColor Green
Write-Host "Nota: con GOOGLE_APPLICATION_CREDENTIALS nel .env, l'app continua a usare il service account (consigliato)." -ForegroundColor Gray
Write-Host ""
