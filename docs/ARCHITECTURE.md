# Architettura: Media Expert Dashboard su GCP

**Progetto GCP:** `mediaexpertdashboard`  
**Componenti:** BigQuery (data warehouse), Metabase (BI su Cloud Run), Row Level Security per brand.

---

## Schema architettura finale

```
                    ┌─────────────────────────────────────────────────────────┐
                    │                  Google Cloud Project                    │
                    │                 mediaexpertdashboard                     │
                    └─────────────────────────────────────────────────────────┘
                                              │
         ┌────────────────────────────────────┼────────────────────────────────────┐
         │                                    │                                    │
         ▼                                    ▼                                    ▼
┌─────────────────┐                ┌─────────────────┐                ┌─────────────────┐
│    BigQuery     │                │   Cloud Run     │                │  Cloud Build    │
│                 │                │                 │                │                 │
│  Dataset: raw   │                │  Metabase       │◄───────────────│  Trigger GitHub │
│  Dataset: mart  │◄───────────────│  (container     │   deploy       │  (build image   │
│                 │  SA + JSON      │   metabase/     │                 │   + deploy)     │
│  fact_sales_    │                │   metabase)     │                └─────────────────┘
│  daily,         │                │                 │
│  fact_promo_    │                │  MB_DB_FILE o   │
│  performance,   │                │  Cloud SQL      │
│  dim_*          │                │  (metadata)      │
└────────┬────────┘                └────────┬────────┘
         │                                  │
         │                                  │ Utenti Metabase
         │                                  │ (Sodastream, Nespresso, Samsung)
         │                                  │ + Group → filter brand_name
         ▼                                  ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  Row Level Security: Metabase Data Permissions                                   │
│  - Sodastream_group  → filter: brand_name = 'Sodastream'                          │
│  - Nespresso_group   → filter: brand_name = 'Nespresso'                           │
│  - Samsung_group     → filter: brand_name = 'Samsung'                             │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## PARTE 1 — Architettura passo-passo

### 1. Service Account per Metabase

Metabase deve interrogare BigQuery. Creiamo un Service Account dedicato e gli diamo solo i permessi di lettura sui dataset del mart.

**Passi:**

1. **Crea il Service Account**
   ```bash
   gcloud iam service-accounts create metabase-bigquery \
     --display-name="Metabase BigQuery Reader" \
     --project=mediaexpertdashboard
   ```

2. **Ottieni l’email del SA** (sarà `metabase-bigquery@mediaexpertdashboard.iam.gserviceaccount.com`).

3. **Assegna ruoli**
   - **BigQuery Data Viewer** sul progetto (o sui dataset `raw` e `mart`): consente di eseguire query e leggere tabelle.
   - Opzionale: **BigQuery Job User** se vuoi che i job siano visibili sotto il progetto (spesso incluso con Data Viewer per le query).

   Assegnazione a livello progetto:
   ```bash
   gcloud projects add-iam-policy-binding mediaexpertdashboard \
     --member="serviceAccount:metabase-bigquery@mediaexpertdashboard.iam.gserviceaccount.com" \
     --role="roles/bigquery.dataViewer"
   ```

   Per limitare ai soli dataset `raw` e `mart` usa IAM condizionale o dataset-level permissions (vedi script).

4. **Crea e scarica la chiave JSON**
   ```bash
   gcloud iam service-accounts keys create ./metabase-bq-key.json \
     --iam-account=metabase-bigquery@mediaexpertdashboard.iam.gserviceaccount.com \
     --project=mediaexpertdashboard
   ```
   Conserva `metabase-bq-key.json` in modo sicuro (es. Secret Manager) e non committarlo.

---

### 2. Collegare Metabase a BigQuery

1. In Metabase: **Admin** → **Databases** → **Add database**.
2. **Database type:** BigQuery.
3. **Display name:** es. "Media Expert BigQuery".
4. **Connection:**
   - **Authentication:** Service Account JSON (consigliato).
   - **Service Account JSON:** incolla il contenuto del file `metabase-bq-key.json` (o usa un secret in Cloud Run che espandi in un file montato).
5. **Project ID:** `mediaexpertdashboard`.
6. **Dataset (optional):** lascia vuoto per vedere tutti i dataset, oppure imposta `mart` come default se supportato dal driver.
7. Salva e **Sync database** per far comparire tabelle/viste.

La connection usa quindi il Service Account (connection via JSON); non serve un utente “database” aggiuntivo.

---

### 3. Ruoli utilizzati (riepilogo)

| Ruolo                    | Scopo                                      |
|--------------------------|--------------------------------------------|
| `roles/bigquery.dataViewer` | Lettura tabelle/dataset (query, dashboard) |
| (opzionale) `roles/bigquery.jobUser` | Creazione job BigQuery nel progetto    |

Per produzione, preferibile limitare il Data Viewer ai soli dataset `raw` e `mart` (autorizzazioni a livello dataset).

---

## PARTE 3 — Row Level Security (RLS): due opzioni

### Opzione A (consigliata): Metabase Data Permissions

- Le tabelle nel mart hanno una colonna **`brand_name`** (denormalizzata).
- In Metabase crei **3 gruppi**: `Sodastream_group`, `Nespresso_group`, `Samsung_group`.
- Per ogni gruppo, su ogni tabella esposta (es. `fact_sales_daily`, `fact_promo_performance`):
  - **Table permissions** → "Filter this table" (o equivalente "Row-level filter").
  - Filtro: `brand_name = 'Sodastream'` (o `Nespresso`, `Samsung`).
- Assegni ogni utente al rispettivo gruppo.
- **Pro:** nessuna vista aggiuntiva in BigQuery, configurazione solo in Metabase.  
- **Contro:** la colonna `brand_name` deve essere presente (o esposta tramite vista) in tutte le tabelle usate dalle domande/dashboard.

Implementazione: vedi sezione “PARTE 5 — Creazione utenti e gruppi” più sotto.

---

### Opzione B: Viste autorizzate in BigQuery

- Crei 3 viste: `mart.view_sodastream_sales`, `mart.view_nespresso_sales`, `mart.view_samsung_sales` (e analoghe per promo) che filtrano per `brand_id`/`brand_name`.
- In BigQuery assegni ai gruppi (tramite IAM o authorized views) l’accesso solo a una vista per brand.
- In Metabase colleghi 3 database (o 3 connection) ognuno con accesso a una sola vista.

**Pro:** isolamento a livello BigQuery.  
**Contro:** più oggetti da mantenere e più complessità (3 set di viste, permessi IAM per vista/dataset).

Per semplicità si implementa e documenta **Opzione A**; le viste in `bigquery/views_rls.sql` restano disponibili se in futuro vuoi passare a Opzione B.

---

## PARTE 5 — Creazione utenti Metabase e gruppi

1. **Admin** → **People** → **Groups**: crea i gruppi:
   - `Sodastream_group`
   - `Nespresso_group`
   - `Samsung_group`

2. **Admin** → **People** → **Add someone** (o **Invite**):
   - Crea 3 utenti (email o username a scelta, es. `sodastream@humancg.com`, `nespresso@...`, `samsung@...` oppure username `Sodastream`, `Nespresso`, `Samsung`).
   - Password iniziale: `hcg2026`.
   - Assegna ciascun utente al gruppo corrispondente (Sodastream → Sodastream_group, ecc.).

3. **Data permissions** (Admin → Permissions → Data):
   - Seleziona il database BigQuery.
   - Per **Sodastream_group**: su `fact_sales_daily` e `fact_promo_performance` (e altre tabelle con brand) imposta **"Filter this table"** → `brand_name` = `Sodastream`.
   - Stessa cosa per **Nespresso_group** con `brand_name = 'Nespresso'` e **Samsung_group** con `brand_name = 'Samsung'`.

4. **Cambio password al primo login:** in Metabase **Admin** → **Authentication** → **Password**: abilita “Require users to change their password on first login” se disponibile (dipende dalla versione). In alternativa, imposti una policy aziendale e comunichi agli utenti di cambiare password al primo accesso.

---

## Deploy Metabase su Cloud Run (sintesi)

- **Immagine:** `metabase/metabase:latest`.
- **Metadata DB:** variabile `MB_DB_FILE` (file locale nel container) per SQLite embedded, oppure Cloud SQL (Postgres) per produzione.
- **Variabili utili:** `MB_BIGQUERY_DRIVER`, connection a BigQuery tramite Service Account (configurata dall’interfaccia Metabase con il JSON del SA).
- **Autenticazione:** abilita in Metabase “Email and Password” (Admin → Authentication).

Dettaglio comandi in `scripts/deploy-metabase.sh` e in `cloudbuild.yaml`.
