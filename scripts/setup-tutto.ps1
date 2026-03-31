# Setup completo: login GCP, dataset BigQuery, schema e dati.
# Esegui in PowerShell dalla cartella del progetto (una sola volta).
# Si aprira' il browser per il login.

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot\..

Write-Host "=== 0. Chart.js (se mancante) ===" -ForegroundColor Cyan
.\scripts\download-chartjs.ps1

Write-Host "`n=== 1. Login Google Cloud (si aprira' il browser) ===" -ForegroundColor Cyan
gcloud auth login
if ($LASTEXITCODE -ne 0) { exit 1 }

Write-Host "`n=== 2. Credenziali per applicazioni (si aprira' di nuovo il browser) ===" -ForegroundColor Cyan
gcloud auth application-default login
if ($LASTEXITCODE -ne 0) { exit 1 }

Write-Host "`n=== 3. Progetto e dataset BigQuery ===" -ForegroundColor Cyan
gcloud config set project mediaexpertdashboard
.\scripts\setup-databases-gcp.ps1
if ($LASTEXITCODE -ne 0) { exit 1 }

Write-Host "`n=== 4. Schema e dati BigQuery (tabelle + seed) ===" -ForegroundColor Cyan
pip install -q google-cloud-bigquery
python scripts\run_bigquery_schema.py
if ($LASTEXITCODE -ne 0) { exit 1 }

Write-Host "`n=== Setup completato ===" -ForegroundColor Green
Write-Host "Avvia la dashboard: python -m uvicorn app.main:app --reload --host 127.0.0.1 --port 8000"
Write-Host "Poi apri: http://127.0.0.1:8000" -ForegroundColor Cyan
