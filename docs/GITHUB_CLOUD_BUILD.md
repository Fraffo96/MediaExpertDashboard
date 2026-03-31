# Repository GitHub e Cloud Build Trigger

## 1. Nuova repository GitHub

1. Su GitHub: **New repository** (es. `media-expert-dashboard` o `MediaExpertDashboard`).
2. Non inizializzare con README se il progetto esiste già in locale.
3. Collega il progetto locale e push:

```bash
cd "c:\Users\franc\Desktop\Projects\MEDIA EXPERT DASHBOARD"
git remote add origin https://github.com/<org>/<repo>.git
git add .
git commit -m "Dashboard: BigQuery mart, Cloud Run, nostra app"
git branch -M main
git push -u origin main
```

Se la repo è già collegata (Cursor collegato a GitHub), dalla root del progetto:

```bash
git add bigquery/ scripts/ docs/ dashboard/ cloudbuild.yaml
git commit -m "GCP: BigQuery mart, nostra dashboard su Cloud Run"
git push origin main
```

---

## 2. Cloud Build e trigger

### 2.1 Connettere GitHub a Cloud Build

1. **Console GCP** → **Cloud Build** → **Triggers**.
2. **Connect repository** (prima connessione):
   - **Source**: GitHub (o GitHub Enterprise).
   - Autorizza e seleziona l’organizzazione/account e la repository.
   - Se richiesto, installa l’app “Google Cloud Build” su GitHub.

### 2.2 Creare il trigger

1. **Create trigger**.
2. **Name:** `deploy-dashboard` (o `dashboardgit`).
3. **Event:** Push to a branch.
4. **Source:** repo connessa, branch `^main$` (o il branch che usi).
5. **Configuration:** Cloud Build configuration file.
6. **Location:** `cloudbuild.yaml` (root del repo).
7. **Substitution variables** (progetto `mediaexpertdashboard`, regione `europe-west1`):
   - `_PROJECT_ID`: `mediaexpertdashboard`
   - `_REGION`: `europe-west1`
   - `_VPC_CONNECTOR`: (vuoto finché non usi Redis privato) oppure nome del **Serverless VPC Access connector**
   - `_EXTRA_ENV_VARS`: (vuoto o variabili non sensibili, separate da virgola) es. `REDIS_URL=redis://10.x.x.x:6379`
   - `_SET_SECRETS`: (vuoto o mappa Secret Manager) es. `JWT_SECRET_KEY=jwt-secret-key:latest,PREWARM_TOKEN=prewarm-token:latest`
8. **Save**.

### 2.3 Comandi da riga di comando

```bash
# Imposta progetto
gcloud config set project mediaexpertdashboard

# Abilita API
gcloud services enable cloudbuild.googleapis.com run.googleapis.com

# Connessione repo (se non già fatta da Console)
# Vai in Cloud Build → Repositories → Link repository

# Crea trigger da gcloud (dopo aver collegato la repo)
gcloud builds triggers create github \
  --name="deploy-dashboard" \
  --repo-name="<nome-repo-github>" \
  --repo-owner="<org-o-username>" \
  --branch-pattern="^main$" \
  --build-config="cloudbuild.yaml" \
  --substitutions="_PROJECT_ID=mediaexpertdashboard,_REGION=europe-west1"
```

Dopo ogni push su `main`, Cloud Build eseguirà `cloudbuild.yaml` e farà il deploy della **nostra dashboard** su Cloud Run (servizio `dashboard`).

---

## 3. Produzione: BigQuery, Secret Manager, Redis (Memorystore)

### 3.1 Service account runtime Cloud Run (BigQuery)

L’immagine **non** usa `GOOGLE_APPLICATION_CREDENTIALS`. BigQuery usa l’identità di default del servizio Cloud Run (di solito `PROJECT_NUMBER-compute@developer.gserviceaccount.com` se non ne imposti un’altra).

Assegna a quell’identità almeno:

- `roles/bigquery.jobUser` (progetto)
- `roles/bigquery.dataViewer` (o più stretto sul dataset `mart`)

### 3.2 Secret Manager (JWT e prewarm)

1. Abilita **Secret Manager API**.
2. Crea i segreti (esempi nomi):
   - `jwt-secret-key` — valore lungo casuale (stesso concetto di `JWT_SECRET_KEY` in `.env`).
   - `prewarm-token` — token per `GET /internal/prewarm` (header `X-Prewarm-Token`).
3. Sul trigger Cloud Build, imposta ad esempio:
   - `_SET_SECRETS`: `JWT_SECRET_KEY=jwt-secret-key:latest,PREWARM_TOKEN=prewarm-token:latest`
4. Al **runtime SA** di Cloud Run concedi `roles/secretmanager.secretAccessor` su questi segreti (Console → Secret → Permissions, oppure IAM).

Formato `gcloud`: nome **variabile ambiente** sul servizio = nome che legge l’app; dopo `=` il nome **secret** in GCP e versione (`:latest`).

### 3.3 Memorystore Redis + VPC connector

1. **Memorystore for Redis** nella stessa regione del servizio (`europe-west1`), rete VPC con IP privato.
2. **Serverless VPC Access**: crea un **VPC connector** (es. `run-vpc-connector`) collegato alla stessa VPC/subnet compatibile con Redis.
3. Sul trigger, imposta:
   - `_VPC_CONNECTOR`: nome del connector (es. `run-vpc-connector`).
4. Passa l’URL Redis al servizio, ad esempio con `_EXTRA_ENV_VARS`:
   - `REDIS_URL=redis://REDIS_PRIVATE_IP:6379`
   (oppure memorizza l’URL in un secret e aggiungilo a `_SET_SECRETS`, es. `REDIS_URL=redis-url:latest`).

Con `_VPC_CONNECTOR` non vuoto, il deploy usa `--vpc-egress=private-ranges-only` per raggiungere solo gli IP privati (es. Redis) senza forzare tutto il traffico in uscita via VPC.

### 3.4 Verifica post-deploy

- `GET https://<url-cloud-run>/health` → `{"status":"ok"}`
- Login e una dashboard Sales; nei log, se Redis è configurato, compare il messaggio di cache Redis da [`app/services/_cache.py`](../app/services/_cache.py).

Dettagli TTL cache: [`CACHE_AND_PERFORMANCE.md`](CACHE_AND_PERFORMANCE.md).
