# Setup GCP: Service Account, IAM, BigQuery datasets (PowerShell)
# Esegui DOPO: gcloud auth login
# Progetto: mediaexpertdashboard

$ErrorActionPreference = "Stop"
$PROJECT_ID = "mediaexpertdashboard"
$SA_NAME = "metabase-bigquery"
$SA_EMAIL = "${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
$KEY_FILE = "metabase-bq-key.json"

Write-Host "=== Progetto: $PROJECT_ID ===" -ForegroundColor Cyan

# 1. Service Account
Write-Host "--- Creazione Service Account: $SA_NAME ---" -ForegroundColor Yellow
try {
    gcloud iam service-accounts create $SA_NAME `
        --display-name="Metabase BigQuery Reader" `
        --project=$PROJECT_ID
} catch {
    Write-Host "SA gia' esistente o errore (ignoro)." -ForegroundColor Gray
}

# 2. Ruoli
Write-Host "--- Assegnazione ruoli ---" -ForegroundColor Yellow
gcloud projects add-iam-policy-binding $PROJECT_ID `
    --member="serviceAccount:$SA_EMAIL" `
    --role="roles/bigquery.dataViewer"
gcloud projects add-iam-policy-binding $PROJECT_ID `
    --member="serviceAccount:$SA_EMAIL" `
    --role="roles/bigquery.jobUser"

# 3. Chiave JSON
Write-Host "--- Creazione chiave JSON ---" -ForegroundColor Yellow
$keyPath = Join-Path (Join-Path $PSScriptRoot "..") $KEY_FILE
gcloud iam service-accounts keys create $keyPath `
    --iam-account=$SA_EMAIL `
    --project=$PROJECT_ID
Write-Host "Chiave salvata in: $keyPath" -ForegroundColor Green
Write-Host "NON committare questo file (e' in .gitignore)." -ForegroundColor Gray

# 4. Dataset BigQuery
Write-Host "--- Dataset BigQuery ---" -ForegroundColor Yellow
bq mk --dataset --location=EU "${PROJECT_ID}:raw" 2>$null
bq mk --dataset --location=EU "${PROJECT_ID}:mart" 2>$null
Write-Host "Dataset raw e mart creati (o gia' esistenti)." -ForegroundColor Green

Write-Host "`n=== Setup GCP completato ===" -ForegroundColor Cyan
Write-Host "Prossimi passi:" -ForegroundColor Yellow
Write-Host "1. Esegui lo schema BigQuery: in Console BigQuery esegui il contenuto di bigquery/schema_and_seed.sql"
Write-Host "2. Deploy Metabase: .\scripts\deploy-metabase.ps1 oppure gcloud run deploy (vedi docs/METABASE_SETUP.md)"
Write-Host "3. In Metabase aggiungi il database BigQuery usando il file $KEY_FILE" -ForegroundColor Gray
