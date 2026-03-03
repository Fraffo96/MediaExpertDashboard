# Esegui prima – per usare l’applicazione

Commit e file sono pronti. Completa questi passi in ordine.

---

## 1. Login Google Cloud

Apri un terminale (PowerShell o CMD) ed esegui:

```powershell
gcloud auth login
gcloud config set project mediaexpertdashboard
```

---

## 2. Setup GCP (Service Account + BigQuery)

Dalla cartella del progetto:

```powershell
cd "c:\Users\franc\Desktop\Projects\MEDIA EXPERT DASHBOARD"
.\scripts\setup-gcp.ps1
```

Verranno creati: Service Account `metabase-bigquery`, chiave in `metabase-bq-key.json`, dataset `raw` e `mart`.

---

## 3. Schema e dati BigQuery

- Apri **Google Cloud Console** → **BigQuery** (progetto `mediaexpertdashboard`).
- Apri il file `bigquery/schema_and_seed.sql` e **esegui tutto** nella console (o a blocchi: prima le CREATE, poi gli INSERT).

In alternativa da terminale (se hai `bq`):

```powershell
bq query --use_legacy_sql=false --max_rows=0 < bigquery\schema_and_seed.sql
```

---

## 4. Deploy Metabase su Cloud Run

```powershell
.\scripts\deploy-metabase.ps1
```

Salva l’URL che viene stampato (es. `https://metabase-xxx.run.app`).

---

## 5. Configurazione Metabase

1. Apri l’URL di Metabase nel browser.
2. Completa il setup iniziale (admin email e password).
3. **Admin** → **Databases** → **Add database**:
   - Tipo: **BigQuery**
   - Project ID: `mediaexpertdashboard`
   - Service Account JSON: incolla il contenuto di `metabase-bq-key.json`
4. **Sync database**.
5. Crea gruppi e utenti (vedi `docs/METABASE_SETUP.md`): Sodastream, Nespresso, Samsung con filtri per `brand_name`.

---

## 6. Push su GitHub (nuova repository)

Se non l’hai ancora fatto:

1. Su **GitHub** crea una nuova repository (es. `MediaExpertDashboard`), senza README.
2. Nella cartella del progetto:

```powershell
cd "c:\Users\franc\Desktop\Projects\MEDIA EXPERT DASHBOARD"
git remote add origin https://github.com/TUO_ORG/MediaExpertDashboard.git
git branch -M main
git push -u origin main
```

Sostituisci `TUO_ORG` con il tuo username o organizzazione GitHub.

3. Per il **trigger Cloud Build**: in GCP → **Cloud Build** → **Triggers** → **Connect repository** (collega la repo) → **Create trigger** su branch `main` con file `cloudbuild.yaml`.

---

Dopo questi passi puoi usare l’applicazione (BigQuery popolato, Metabase online, utenti per brand).
