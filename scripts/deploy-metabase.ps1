# Deploy Metabase su Cloud Run (PowerShell)
# Progetto: mediaexpertdashboard

$ErrorActionPreference = "Stop"
$PROJECT_ID = "mediaexpertdashboard"
$REGION = "europe-west1"
$SERVICE_NAME = "metabase"

Write-Host "=== Deploy Metabase su Cloud Run ===" -ForegroundColor Cyan
gcloud run deploy $SERVICE_NAME `
    --image=metabase/metabase:latest `
    --platform=managed `
    --region=$REGION `
    --project=$PROJECT_ID `
    --allow-unauthenticated `
    --set-env-vars="MB_DB_FILE=/metabase-data/metabase.db" `
    --memory=1Gi `
    --cpu=1 `
    --min-instances=0 `
    --max-instances=2 `
    --port=3000
Write-Host "`nURL servizio:" -ForegroundColor Yellow
gcloud run services describe $SERVICE_NAME --region=$REGION --project=$PROJECT_ID --format="value(status.url)"
