# Concede a un service account gia' esistente il ruolo Firestore (roles/datastore.user).
# Usa questo se hai gia' eseguito setup-bigquery-service-account.ps1 PRIMA che includesse Firestore,
# oppure se il login in locale fallisce con 403 su Firestore.
#
# Esempio: .\scripts\grant-firestore-to-dashboard-sa.ps1
#          .\scripts\grant-firestore-to-dashboard-sa.ps1 -ProjectId "mio-progetto" -SaName "bigquery-dashboard"

param(
    [string] $ProjectId = "mediaexpertdashboard",
    [string] $SaName = "bigquery-dashboard"
)

$env:Path = [System.Environment]::GetEnvironmentVariable("Path","Machine") + ";" + [System.Environment]::GetEnvironmentVariable("Path","User")
$SaEmail = "${SaName}@${ProjectId}.iam.gserviceaccount.com"

Write-Host ">>> Progetto: $ProjectId | Service account: $SaEmail" -ForegroundColor Cyan

$gcloud = Get-Command gcloud -ErrorAction SilentlyContinue
if (-not $gcloud) {
    Write-Host "[ERR] gcloud non trovato." -ForegroundColor Red
    exit 1
}

$null = gcloud iam service-accounts describe $SaEmail --project=$ProjectId 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERR] Service account non trovato. Esegui prima .\scripts\setup-bigquery-service-account.ps1" -ForegroundColor Red
    exit 1
}

Write-Host ">>> Abilitazione Firestore API..." -ForegroundColor Yellow
gcloud services enable firestore.googleapis.com --project=$ProjectId --quiet

Write-Host ">>> IAM: roles/datastore.user (lettura/scrittura Firestore)..." -ForegroundColor Yellow
gcloud projects add-iam-policy-binding $ProjectId `
    --member="serviceAccount:$SaEmail" `
    --role="roles/datastore.user"

if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERR] Binding IAM fallito (permessi insufficienti sul progetto?)" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "[OK] Il JSON in GOOGLE_APPLICATION_CREDENTIALS per questo SA puo' usare Firestore." -ForegroundColor Green
Write-Host "Rimuovi FIRESTORE_EMULATOR_HOST dal .env se presente, riavvia uvicorn, riprova /login." -ForegroundColor Gray
