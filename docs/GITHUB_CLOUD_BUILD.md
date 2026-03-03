# Repository GitHub e Cloud Build Trigger

## 1. Nuova repository GitHub

1. Su GitHub: **New repository** (es. `media-expert-dashboard` o `MediaExpertDashboard`).
2. Non inizializzare con README se il progetto esiste già in locale.
3. Collega il progetto locale e push:

```bash
cd "c:\Users\franc\Desktop\Projects\MEDIA EXPERT DASHBOARD"
git remote add origin https://github.com/<org>/<repo>.git
git add .
git commit -m "Add GCP/Metabase: BigQuery mart, Cloud Run, RLS, dashboard queries"
git branch -M main
git push -u origin main
```

Se la repo è già collegata (Cursor collegato a GitHub), dalla root del progetto:

```bash
git add bigquery/ scripts/ docs/ dashboard/ cloudbuild.yaml
git commit -m "GCP: BigQuery mart, Metabase Cloud Run, RLS, dashboard"
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
2. **Name:** `deploy-metabase`.
3. **Event:** Push to a branch.
4. **Source:** repo connessa, branch `^main$` (o il branch che usi).
5. **Configuration:** Cloud Build configuration file.
6. **Location:** `cloudbuild.yaml` (root del repo).
7. **Substitution variables** (opzionale):
   - `_PROJECT_ID`: `mediaexpertdashboard`
   - `_REGION`: `europe-west1`
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
  --name="deploy-metabase" \
  --repo-name="<nome-repo-github>" \
  --repo-owner="<org-o-username>" \
  --branch-pattern="^main$" \
  --build-config="cloudbuild.yaml" \
  --substitutions="_PROJECT_ID=mediaexpertdashboard,_REGION=europe-west1"
```

Dopo ogni push su `main`, Cloud Build eseguirà `cloudbuild.yaml` e farà il deploy di Metabase su Cloud Run.

---

## 3. File da non committare

Aggiungi a `.gitignore`:

```
metabase-bq-key.json
*.json
!package.json
.env
.env.local
```

Il file della chiave del Service Account (`metabase-bq-key.json`) va tenuto fuori dalla repo; in produzione usa Secret Manager e iniettalo in Cloud Run come file montato o variabile.
