# Setup BigQuery + Firestore (opzione B, GCP reale) con Service Account - Connessione persistente
# Esegui dalla cartella del progetto: .\scripts\setup-bigquery-service-account.ps1
#
# Crea un service account, assegna BigQuery + Firestore (Datastore User), genera la chiave JSON e configura .env.
# NON impostare FIRESTORE_EMULATOR_HOST: il client usera' Firestore nel progetto GCP.
# Cosi' non serve piu' gcloud auth application-default login (che scade)

param(
    [string] $ProjectId = "mediaexpertdashboard",
    [string] $SaName = "bigquery-dashboard"
)

# Aggiorna PATH per trovare gcloud (installato da winget/choco)
$env:Path = [System.Environment]::GetEnvironmentVariable("Path","Machine") + ";" + [System.Environment]::GetEnvironmentVariable("Path","User")
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$SaEmail = "${SaName}@${ProjectId}.iam.gserviceaccount.com"
$CredsDir = Join-Path $ProjectRoot "credentials"
$KeyFile = Join-Path $CredsDir "bigquery-sa.json"

Set-Location $ProjectRoot

Write-Host ""
Write-Host "=== BigQuery + Firestore: Service Account (connessione persistente) ===" -ForegroundColor Cyan
Write-Host "Progetto: $ProjectId | SA: $SaName" -ForegroundColor Gray
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

Write-Host "Abilitazione API (BigQuery, Firestore)..." -ForegroundColor Yellow
gcloud services enable bigquery.googleapis.com firestore.googleapis.com --project=$ProjectId --quiet 2>$null

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
Write-Host "  [OK] BigQuery: permessi assegnati" -ForegroundColor Green

# 4b. Firestore (login, utenti, ecosistemi) — roles/datastore.user
Write-Host "Assegnazione permessi Firestore (roles/datastore.user)..." -ForegroundColor Yellow
gcloud projects add-iam-policy-binding $ProjectId `
    --member="serviceAccount:$SaEmail" `
    --role="roles/datastore.user" `
    --quiet 2>$null
Write-Host "  [OK] Firestore: roles/datastore.user" -ForegroundColor Green

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

# 6. Crea/aggiorna .env (GCP senza cancellare JWT/Redis; rimuove FIRESTORE_EMULATOR_HOST = opzione B)
$envPath = Join-Path $ProjectRoot ".env"
$outLines = if (Test-Path $envPath) { @(Get-Content -Path $envPath -Encoding UTF8) } else { @() }
$brandLogosBase = "https://storage.googleapis.com/${ProjectId}-brand-logos/brands"
$outLines = @(
    $outLines | Where-Object {
        $_ -notmatch '^\s*GCP_PROJECT_ID\s*=' -and
        $_ -notmatch '^\s*GOOGLE_APPLICATION_CREDENTIALS\s*=' -and
        $_ -notmatch '^\s*FIRESTORE_EMULATOR_HOST\s*=' -and
        $_ -notmatch '^\s*BRAND_LOGOS_PUBLIC_BASE\s*='
    }
)
$outLines += "GCP_PROJECT_ID=$ProjectId"
$outLines += "GOOGLE_APPLICATION_CREDENTIALS=$KeyFile"
$outLines += "BRAND_LOGOS_PUBLIC_BASE=$brandLogosBase"
$outLines | Set-Content -Path $envPath -Encoding UTF8
Write-Host ""
Write-Host "  [OK] File .env aggiornato (GCP_PROJECT_ID, GOOGLE_APPLICATION_CREDENTIALS, BRAND_LOGOS_PUBLIC_BASE come Cloud Run)" -ForegroundColor Green

# 7. Imposta variabile d'ambiente per la sessione corrente
$env:GOOGLE_APPLICATION_CREDENTIALS = $KeyFile
$env:GCP_PROJECT_ID = $ProjectId

Write-Host ""
Write-Host "=== Setup completato ===" -ForegroundColor Green
Write-Host ""
Write-Host "BigQuery e Firestore usano lo stesso JSON (GOOGLE_APPLICATION_CREDENTIALS)." -ForegroundColor Cyan
Write-Host ""
Write-Host "Prossimi passi:" -ForegroundColor Yellow
Write-Host "  1. (Primo deploy DB) Se Firestore non esiste: .\scripts\provision-firestore-and-brand-logos.ps1 -ProjectId $ProjectId"
Write-Host "  2. Verifica BigQuery: python scripts\diagnose_bigquery.py"
Write-Host "  3. Avvia app: python -m uvicorn app.main:app --reload --host 127.0.0.1 --port 8000"
Write-Host "  4. Login locale: utente default expert / test (creato da init_db se assente)"
Write-Host ""
Write-Host "Nota: .env viene caricato automaticamente all'avvio dell'app." -ForegroundColor Gray
Write-Host ""
