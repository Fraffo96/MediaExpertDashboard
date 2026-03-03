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

## 6. Push su GitHub e deploy automatico su GCP

### 6a. Crea la repository e fai il primo push

**Opzione A – Da Cursor (consigliato)**  
1. Apri il pannello **Source Control** (icona a sinistra o `Ctrl+Shift+G`).  
2. Clicca **"Publish to GitHub"** (o **"Publish Branch"**).  
3. Scegli nome repository: `MediaExpertDashboard`, visibilità **Public**.  
4. Conferma: Cursor crea la repo e fa il push del branch `main`.

**Opzione B – Da GitHub.com + terminale**  
1. Su **GitHub.com** → **New repository** (nome `MediaExpertDashboard`), **senza** README o .gitignore.  
2. In PowerShell (sostituisci `TUO_ORG` con il tuo username):

```powershell
cd "c:\Users\franc\Desktop\Projects\MEDIA EXPERT DASHBOARD"
git remote add origin https://github.com/TUO_ORG/MediaExpertDashboard.git
git push -u origin main
```

### 6b. Deploy automatico a ogni push (Cloud Build)

Dopo che la repo è su GitHub:

1. **Console GCP** → **Cloud Build** → **Triggers**.  
2. **Connect repository** (prima volta): scegli **GitHub**, autorizza e seleziona la repo `MediaExpertDashboard`.  
3. **Create trigger**:  
   - Nome: `deploy-metabase`  
   - Event: **Push to a branch**  
   - Branch: `^main$`  
   - Config: **Cloud Build configuration file** → `cloudbuild.yaml` (root)  
   - Variabili: `_PROJECT_ID` = `mediaexpertdashboard`, `_REGION` = `europe-west1`  
4. **Save**.

Da quel momento, ogni **push su `main`** farà partire il build e il deploy di Metabase su Cloud Run.

---

Dopo questi passi puoi usare l’applicazione (BigQuery popolato, Metabase online, utenti per brand).
