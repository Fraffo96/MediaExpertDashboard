# =============================================================================
# Cloud Scheduler: job pre-warming cache ogni ora
# Richiede: gcloud configurato, progetto GCP, Cloud Run deployato
# Genera PREWARM_TOKEN e crea il job.
# =============================================================================

param(
    [string]$ProjectId = "mediaexpertdashboard",
    [string]$Region = "europe-west1",
    [string]$ServiceUrl = ""  # es. https://dashboard-xxx.run.app
)

$ErrorActionPreference = "Stop"

# Se ServiceUrl non passato, prova a ricavarlo
if (-not $ServiceUrl) {
    $svc = gcloud run services describe dashboard --region=$Region --project=$ProjectId --format="value(status.url)" 2>$null
    if ($svc) { $ServiceUrl = $svc.Trim() }
}
if (-not $ServiceUrl) {
    Write-Host "ERRORE: Specifica ServiceUrl o deploya prima Cloud Run." -ForegroundColor Red
    exit 1
}

# Genera token se non esiste
$token = [System.Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes((New-Guid).Guid + (Get-Random).ToString()))
Write-Host "Token generato. Aggiungi a Cloud Run:" -ForegroundColor Yellow
Write-Host "  PREWARM_TOKEN=$token" -ForegroundColor Cyan
Write-Host ""
Write-Host "gcloud run services update dashboard --region=$Region --project=$ProjectId --set-env-vars=PREWARM_TOKEN=$token" -ForegroundColor Gray
Write-Host ""

# Crea job Cloud Scheduler (sostituisce se esiste)
$prewarmUrl = "$ServiceUrl/internal/prewarm"
Write-Host "Creando job prewarm-cache su $prewarmUrl ..." -ForegroundColor Green

gcloud scheduler jobs create http prewarm-cache `
    --location=$Region `
    --project=$ProjectId `
    --schedule="0 * * * *" `
    --uri=$prewarmUrl `
    --http-method=GET `
    --headers="X-Prewarm-Token=$token" `
    --attempt-deadline=300s `
    --description="Pre-warm cache Market Intelligence ogni ora" `
    2>$null

if ($LASTEXITCODE -ne 0) {
    Write-Host "Job potrebbe già esistere. Prova update:" -ForegroundColor Yellow
    gcloud scheduler jobs update http prewarm-cache --location=$Region --project=$ProjectId --headers="X-Prewarm-Token=$token"
}

Write-Host "Fatto. Ricorda di impostare PREWARM_TOKEN su Cloud Run." -ForegroundColor Green
