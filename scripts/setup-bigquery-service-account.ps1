# Setup BigQuery con Service Account - Connessione persistente (non scade)
# Esegui dalla cartella del progetto: .\scripts\setup-bigquery-service-account.ps1
#
# Crea un service account, genera la chiave JSON e configura .env
# Cosi' non serve piu' gcloud auth application-default login (che scade)

# Aggiorna PATH per trovare gcloud (installato da winget/choco)
$env:Path = [System.Environment]::GetEnvironmentVariable("Path","Machine") + ";" + [System.Environment]::GetEnvironmentVariable("Path","User")
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$ProjectId = "mediaexpertdashboard"
$SaName = "bigquery-dashboard"
$SaEmail = "${SaName}@${ProjectId}.iam.gserviceaccount.com"
$CredsDir = Join-Path $ProjectRoot "credentials"
$KeyFile = Join-Path $CredsDir "bigquery-sa.json"

Set-Location $ProjectRoot

Write-Host ""
Write-Host "=== BigQuery: Setup Service Account (connessione persistente) ===" -ForegroundColor Cyan
Write-Host "Progetto: $ProjectId" -ForegroundColor Gray
Write-Host ""

# 1. Verifica gcloud
$gcloud = Get-Command gcloud -ErrorAction SilentlyContinue
if (-not $gcloud) {
    Write-Host "[ERR] gcloud CLI non trovato. Installa: https://cloud.google.com/sdk/docs/install" -ForegroundColor Red
    exit 1
}

# 2. Login se necessario
$account = gcloud config get-value account 2>$null
if (-not $account -or $account -eq "(unset)") {
    Write-Host "Login a Google Cloud (si aprira' il browser)..." -ForegroundColor Yellow
    gcloud auth login
    if ($LASTEXITCODE -ne 0) { exit 1 }
}

$null = gcloud config set project $ProjectId 2>&1 | Out-Null

# 3. Crea service account se non esiste
$null = gcloud iam service-accounts describe $SaEmail --project=$ProjectId 2>&1 | Out-Null
$saExists = ($LASTEXITCODE -eq 0)
if (-not $saExists) {
    Write-Host "Creazione service account: $SaName..." -ForegroundColor Yellow
    gcloud iam service-accounts create $SaName `
        --display-name="BigQuery Dashboard" `
        --project=$ProjectId
    if ($LASTEXITCODE -ne 0) {
        Write-Host "  [ERR] Creazione fallita" -ForegroundColor Red
        exit 1
    }
    Write-Host "  [OK] Service account creato" -ForegroundColor Green
} else {
    Write-Host "Service account $SaName gia' esistente" -ForegroundColor Gray
}

# 4. Assegna permessi BigQuery
Write-Host "Assegnazione permessi BigQuery..." -ForegroundColor Yellow
$roles = @(
    "roles/bigquery.dataViewer",
    "roles/bigquery.jobUser"
)
foreach ($role in $roles) {
    gcloud projects add-iam-policy-binding $ProjectId `
        --member="serviceAccount:$SaEmail" `
        --role=$role `
        --quiet 2>$null
}
Write-Host "  [OK] Permessi assegnati" -ForegroundColor Green

# 5. Crea cartella credentials e genera chiave
if (-not (Test-Path $CredsDir)) {
    New-Item -ItemType Directory -Path $CredsDir | Out-Null
    Write-Host "Cartella credentials creata" -ForegroundColor Gray
}

if (Test-Path $KeyFile) {
    Write-Host ""
    $overwrite = Read-Host "Il file $KeyFile esiste gia'. Sovrascrivere? (s/n)"
    if ($overwrite -ne "s" -and $overwrite -ne "S") {
        Write-Host "Uso il file esistente." -ForegroundColor Gray
    } else {
        Write-Host "Generazione nuova chiave..." -ForegroundColor Yellow
        gcloud iam service-accounts keys create $KeyFile `
            --iam-account=$SaEmail `
            --project=$ProjectId
        if ($LASTEXITCODE -ne 0) { exit 1 }
    }
} else {
    Write-Host "Generazione chiave JSON..." -ForegroundColor Yellow
    gcloud iam service-accounts keys create $KeyFile `
        --iam-account=$SaEmail `
        --project=$ProjectId
    if ($LASTEXITCODE -ne 0) { exit 1 }
    Write-Host "  [OK] Chiave salvata in $KeyFile" -ForegroundColor Green
}

# 6. Crea/aggiorna .env
$envContent = @"
# BigQuery - Service Account (connessione persistente)
GCP_PROJECT_ID=$ProjectId
GOOGLE_APPLICATION_CREDENTIALS=$KeyFile
"@

$envPath = Join-Path $ProjectRoot ".env"
$envContent | Set-Content -Path $envPath -Encoding UTF8
Write-Host ""
Write-Host "  [OK] File .env creato/aggiornato" -ForegroundColor Green

# 7. Imposta variabile d'ambiente per la sessione corrente
$env:GOOGLE_APPLICATION_CREDENTIALS = $KeyFile
$env:GCP_PROJECT_ID = $ProjectId

Write-Host ""
Write-Host "=== Setup completato ===" -ForegroundColor Green
Write-Host ""
Write-Host "La connessione BigQuery e' ora persistente (non scade)." -ForegroundColor Cyan
Write-Host ""
Write-Host "Prossimi passi:" -ForegroundColor Yellow
Write-Host "  1. Verifica: python scripts\diagnose_bigquery.py"
Write-Host "  2. Avvia: python -m uvicorn app.main:app --reload --host 127.0.0.1 --port 8000"
Write-Host ""
Write-Host "Nota: .env viene caricato automaticamente all'avvio dell'app." -ForegroundColor Gray
Write-Host ""
