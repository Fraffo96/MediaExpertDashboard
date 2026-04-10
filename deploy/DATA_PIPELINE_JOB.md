# Cloud Run Job – pipeline dati (precalc / full seed)

L’API `POST /api/admin/data-jobs` non deve eseguire pipeline lunghe nella richiesta HTTP (timeout Cloud Run 120s). In produzione configurare un **Cloud Run Job** che esegue lo stesso container con comando override.

## Variabili (servizio dashboard + Job)

| Variabile | Descrizione |
|-----------|-------------|
| `CLOUD_RUN_DATA_JOB_NAME` | Nome del Job (es. `dashboard-data-pipeline`) |
| `GCP_REGION` / `CLOUD_RUN_REGION` | Regione (es. `europe-west1`) |
| `GCP_PROJECT_ID` | Progetto GCP |

Il Job riceve in override: `DATA_JOB_ID`, `DATA_JOB_TYPE` (`precalc` \| `full_seed`), opzionale `SEED_PROFILE_JSON`.

## Comando container (Job)

- Immagine: stessa della dashboard (`Dockerfile` include `scripts/` e `bigquery/`, `PYTHONPATH=/app`).
- Entrypoint override:

```bash
python scripts/data_pipeline_worker.py
```

- Timeout task: consigliato ≥ 3600s (full seed + schema + precalc).
- Service account: BigQuery (dataset `mart`), Firestore (lettura/scrittura `dashboard_data_jobs`), stesso progetto.

## Creazione rapida (gcloud)

Adatta progetto, regione e immagine.

```bash
gcloud run jobs create dashboard-data-pipeline \
  --image=REGION-docker.pkg.dev/PROJECT/dashboard/dashboard:TAG \
  --region=REGION \
  --tasks=1 \
  --max-retries=0 \
  --task-timeout=7200 \
  --set-env-vars=GCP_PROJECT_ID=PROJECT \
  --command=python,scripts/data_pipeline_worker.py \
  --service-account=DASHBOARD_RUNTIME_SA@PROJECT.iam.gserviceaccount.com
```

Poi imposta `CLOUD_RUN_DATA_JOB_NAME=dashboard-data-pipeline` sul servizio Cloud Run della dashboard.

## Sviluppo locale

Senza Job, l’API tenta `subprocess` verso `scripts/data_pipeline_worker.py` (richiede Firestore raggiungibile e stesso repo).
