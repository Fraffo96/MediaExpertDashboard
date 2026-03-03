#!/bin/bash
# =============================================================================
# Setup GCP: Service Account, IAM, BigQuery datasets (opzionale da script)
# Progetto: mediaexpertdashboard
# Esegui: ./scripts/setup-gcp.sh
# =============================================================================

set -e
PROJECT_ID="${PROJECT_ID:-mediaexpertdashboard}"
SA_NAME="metabase-bigquery"
SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
KEY_FILE="${KEY_FILE:-./metabase-bq-key.json}"

echo "=== Progetto: $PROJECT_ID ==="

# 1. Service Account
echo "--- Creazione Service Account: $SA_NAME ---"
gcloud iam service-accounts create "$SA_NAME" \
  --display-name="Metabase BigQuery Reader" \
  --project="$PROJECT_ID" 2>/dev/null || echo "SA già esistente."

# 2. Ruoli a livello progetto (lettura BigQuery)
echo "--- Assegnazione ruoli ---"
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/bigquery.dataViewer" \
  --condition=None

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/bigquery.jobUser" \
  --condition=None

# 3. Chiave JSON (per Metabase connection)
echo "--- Creazione chiave JSON ---"
gcloud iam service-accounts keys create "$KEY_FILE" \
  --iam-account="$SA_EMAIL" \
  --project="$PROJECT_ID"

echo "Chiave salvata in: $KEY_FILE"
echo "Aggiungi $KEY_FILE a .gitignore e usa Secret Manager in produzione."

# 4. Dataset BigQuery (se non esistono)
echo "--- Dataset BigQuery ---"
bq mk --dataset --location=EU "$PROJECT_ID:raw" 2>/dev/null || true
bq mk --dataset --location=EU "$PROJECT_ID:mart" 2>/dev/null || true

echo "=== Setup completato ==="
