# Crea i database BigQuery (dataset raw + mart) e ricorda di eseguire schema_and_seed
# Esegui dopo: gcloud auth login && gcloud config set project mediaexpertdashboard

$ErrorActionPreference = "Stop"
$PROJECT_ID = "mediaexpertdashboard"

Write-Host "=== Database su GCP (BigQuery) ===" -ForegroundColor Cyan
Write-Host "Progetto: $PROJECT_ID" -ForegroundColor Gray

# Dataset per la nostra dashboard (schema mart + raw)
Write-Host "`nCreazione dataset BigQuery (raw, mart)..." -ForegroundColor Yellow
bq mk --dataset --location=EU "${PROJECT_ID}:raw" 2>$null
bq mk --dataset --location=EU "${PROJECT_ID}:mart" 2>$null
Write-Host "Dataset raw e mart pronti." -ForegroundColor Green

Write-Host "`n--- Prossimo passo obbligatorio ---" -ForegroundColor Yellow
Write-Host "Apri la Console BigQuery e esegui TUTTO il file: bigquery\schema_and_seed.sql"
Write-Host "  (Crea tabelle dim_*, fact_* e inserisce i dati per la dashboard.)"
Write-Host "  URL: https://console.cloud.google.com/bigquery?project=$PROJECT_ID"
Write-Host ""
