<#
.SYNOPSIS
  Abilita Firestore (Native), crea bucket loghi brand, IAM lettura pubblica sul bucket,
  ruolo Cloud Datastore User per il service account runtime di Cloud Run (default compute).

  Eseguire da terminale autenticato: gcloud auth login + proprietario/progetto corretto.
  Impostare poi su Cloud Run (o Cloud Build _BRAND_LOGOS_PUBLIC_BASE):
    https://storage.googleapis.com/<NOME_BUCKET>/brands

.NOTES
  Il bucket e' dedicato solo ai loghi: IAM allUsers:objectViewer su tutto il bucket (semplice).
  Per policy piu' restrittive usare bucket separato o signed URL lato server.
#>
param(
    [string] $ProjectId = "mediaexpertdashboard",
    [string] $Region = "europe-west1",
    [string] $BucketName = ""
)

$ErrorActionPreference = "Stop"

if (-not $BucketName) {
    $BucketName = "$ProjectId-brand-logos"
}

Write-Host "Progetto: $ProjectId | Regione Firestore/GCS: $Region | Bucket: $BucketName"

gcloud config set project $ProjectId | Out-Null

Write-Host ">>> Abilitazione API..."
gcloud services enable firestore.googleapis.com --project=$ProjectId
gcloud services enable storage.googleapis.com --project=$ProjectId

Write-Host ">>> Creazione database Firestore (default) se assente..."
# Ignora errore se gia' esiste
gcloud firestore databases create --database="(default)" --location=$Region --project=$ProjectId 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-Host "    (database default gia' presente o errore ignorato — verifica in console)"
}

Write-Host ">>> Bucket GCS..."
$Uri = "gs://$BucketName"
$exists = gsutil ls -b $Uri 2>$null
if (-not $exists) {
    gsutil mb -p $ProjectId -l $Region $Uri
}

Write-Host ">>> Lettura pubblica oggetti (loghi)..."
gsutil iam ch allUsers:objectViewer $Uri

Write-Host ">>> IAM: Cloud Datastore User per Compute default SA (Cloud Run di default)..."
$num = gcloud projects describe $ProjectId --format="value(projectNumber)"
$sa = "$num-compute@developer.gserviceaccount.com"
gcloud projects add-iam-policy-binding $ProjectId `
    --member="serviceAccount:$sa" `
    --role="roles/datastore.user" 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-Warning "Binding IAM potrebbe essere gia' presente o richiedere permessi aggiuntivi."
}

$base = "https://storage.googleapis.com/$BucketName/brands"
Write-Host ""
Write-Host "Imposta su Cloud Run (deploy):"
Write-Host "  BRAND_LOGOS_PUBLIC_BASE=$base"
Write-Host ""
Write-Host "Carica PNG con: .\scripts\sync-brand-logos-to-gcs.ps1 -BucketName $BucketName"
