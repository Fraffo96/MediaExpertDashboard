# Crea i database BigQuery (dataset raw + mart) e ricorda di eseguire schema_and_seed
# Esegui dopo: gcloud auth login && gcloud config set project mediaexpertdashboard

$ErrorActionPreference = "Stop"
$PROJECT_ID = "mediaexpertdashboard"

Write-Host "=== Database su GCP (BigQuery) ===" -ForegroundColor Cyan
Write-Host "Progetto: $PROJECT_ID" -ForegroundColor Gray

# Dataset per la nostra dashboard (schema mart + raw)
Write-Host "`nCreazione dataset BigQuery (raw, mart)..." -ForegroundColor Yellow
$ErrorActionPreference = "Continue"
bq mk --dataset --location=EU "${PROJECT_ID}:raw" 2>$null
bq mk --dataset --location=EU "${PROJECT_ID}:mart" 2>$null
$ErrorActionPreference = "Stop"
Write-Host "Dataset raw e mart pronti." -ForegroundColor Green
Write-Host "`nPer tabelle e dati: python scripts\run_bigquery_schema.py (oppure setup-tutto.ps1)"
Write-Host ""
