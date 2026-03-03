#!/bin/bash
# =============================================================================
# Deploy Metabase su Cloud Run (progetto mediaexpertdashboard)
# Immagine: metabase/metabase:latest
# Metadata: MB_DB_FILE (SQLite nel container) - per produzione usare Cloud SQL
# =============================================================================

set -e
PROJECT_ID="${PROJECT_ID:-mediaexpertdashboard}"
REGION="${REGION:-europe-west1}"
SERVICE_NAME="metabase"
IMAGE="metabase/metabase:latest"

echo "=== Deploy Metabase su Cloud Run ==="

# Abilita API
gcloud services enable run.googleapis.com --project="$PROJECT_ID"
gcloud services enable containerregistry.googleapis.com --project="$PROJECT_ID"

# Deploy da immagine ufficiale
gcloud run deploy "$SERVICE_NAME" \
  --image="$IMAGE" \
  --platform=managed \
  --region="$REGION" \
  --project="$PROJECT_ID" \
  --allow-unauthenticated \
  --set-env-vars="MB_DB_FILE=/metabase-data/metabase.db" \
  --memory=1Gi \
  --cpu=1 \
  --min-instances=0 \
  --max-instances=2 \
  --port=3000

echo "--- URL servizio ---"
gcloud run services describe "$SERVICE_NAME" --region="$REGION" --project="$PROJECT_ID" --format='value(status.url)'

echo "=== Completato. Configura BigQuery in Metabase (Admin > Databases) con il JSON del Service Account. ==="
