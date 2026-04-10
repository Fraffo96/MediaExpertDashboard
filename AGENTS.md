# Guida per Agenti AI – Media Expert Dashboard

> Riferimento rapido per interpretare e modificare il progetto. Leggere prima di ogni modifica.

## Stack attuale

| Layer | Tecnologia | Path principale |
|-------|------------|-----------------|
| Backend | FastAPI (Python 3.13) | `app/main.py` |
| Database | BigQuery (dataset `mart`) | `bigquery/schema_and_seed.sql` |
| Query | `app/db/client.py` → `app/db/queries/*.py` | `precalc/` (package), `market_intelligence/`, `brand_comparison.py`, `basic.py`, ecc. |
| Frontend | Jinja2 + Chart.js 4.x | `app/templates/`, `app/static/js/` |
| Auth | Firestore (`dashboard_users`, `dashboard_ecosystems`) + JWT | `app/auth/firestore_store.py`, `app/auth/routes.py` |
| Deploy | Cloud Run via Cloud Build | `cloudbuild.yaml`, `Dockerfile` |

**Non usato:** PostgreSQL (rimosso). L'app usa **solo BigQuery** per analytics; **Firestore** per account e permessi. I loghi sono PNG su **GCS** (stesso `BRAND_LOGOS_PUBLIC_BASE` di Cloud Run, vedi `.env.example`) con fallback a `app/static/img/brands/` se la variabile non è impostata.

---

## Flusso dati

```
bigquery/schema_and_seed.sql
  → fact_orders, fact_order_items, product_pool_seg_channel_gender
  → bigquery/derive_sales_from_orders.sql
  → fact_sales_daily, fact_promo_performance

app/db/queries/*.py  →  app/services/*.py (market_intelligence, brand_comparison, ecc.)  →  API /api/market-intelligence, /api/brand-comparison, ecc.
  →  app/templates/*.html + app/static/js/*/  →  Chart.js
```

---

## Dove modificare cosa

| Modifica | File | Azione |
|----------|------|--------|
| **Nuova query Market Intelligence** | `app/db/queries/market_intelligence.py` | Aggiungere funzione, registrarla in `get_market_intelligence()` |
| **Nuovo chart Market Intelligence** | `app/templates/market_intelligence/_section_*.html`, `app/static/js/market_intelligence/charts/*.js` | Partial + update in chart JS |
| **Nuova query Brand Comparison** | `app/db/queries/brand_comparison.py` | Aggiungere funzione, registrarla in `get_brand_comparison()` |
| **Nuova suggestion Promo Creator** | `app/db/queries/promo_creator.py`, `app/services/promo_creator.py` | Query + logica in `get_promo_creator_suggestions()` |
| **Nuova dashboard Sales** | `app/db/queries/<nome>.py`, `app/services/<nome>.py`, `main.py`, `templates/<nome>/`, `static/js/<nome>/` | Seguire pattern modulare (vedi docs/ARCHITECTURE.md) |
| **Query Basic / legacy** | `app/db/queries/basic.py` (o promo, customer) | Aggiungere funzione, registrarla in `app/services/basic.py` |
| **Categorie / brand / segmenti** | `bigquery/schema_and_seed.sql` | Modificare INSERT, poi `python scripts/run_bigquery_schema.py` |
| **Filtri dropdown** | `app/db/queries/shared.py` | `query_categories()`, `query_subcategories()`, ecc. |
| **Query precalc** | `app/db/queries/precalc/` | Package: `base`, `sales`, `promo`, `peak`, `discount`, `prev_year`, `misc` |
| **Costanti admin** | `app/constants.py` | `DP`, `ADMIN_CATEGORIES`, `ADMIN_SUBCATEGORIES`, `ADMIN_BRANDS` |
| **Stile / tema** | `app/static/css/style.css` | Dark theme, variabili CSS |
| **User model / permessi** | `app/auth/models.py` (costanti), `app/auth/firestore_store.py`, `app/auth/routes.py` | `StoredUser`, `allowed_*`, `access_types`; doc utente = `username` |

---

## Struttura modulare (Sales dashboards)

Ogni dashboard Sales ha:
- **Templates:** `templates/<nome>.html` + `templates/<nome>/_*.html`
- **JS:** `static/js/<nome>/core.js`, `filters.js`, `dashboard.js`, `charts/*.js`
- **Query:** `db/queries/<nome>.py` (o package `db/queries/<nome>/` per moduli multipli, es. market_intelligence)
- **Service:** `services/<nome>.py` → `get_<nome>()` (es. `market_intelligence.py` → `get_market_intelligence()`)
- **Route + API:** `main.py` → `GET /<path>`, `GET /api/<nome>`

---

## Convenzioni

1. **BigQuery CTE:** evitare più `UNION ALL SELECT` sulla stessa riga; usare `UNION ALL SELECT` su riga separata.
2. **category_id:** parent = 1–10; subcategory = 101–108, 201–208, ecc. (vedi `docs/CATEGORIES_AND_SUBCATEGORIES.md`).
3. **API Sales:** richiedono cookie `access_token`; usano `user.brand_id` da sessione.
4. **Discount depth:** media pesata (non AVG semplice) nelle query.

---

## Tabelle precalcolate

Le dashboard usano tabelle precalcolate (`mart.precalc_*`) su BigQuery per caricamenti veloci. Aggiornamento:
- **Pulsante**: "Re-calculate" in alto a destra (solo admin)
- **Script**: `python scripts/run_bigquery_schema.py` (fase 4: DDL) + `python scripts/refresh_precalc_tables.py` (popolamento)

Vedi `docs/PRECALC_TABLES.md` per mappatura dashboard → tabelle e come aggiungere nuove dashboard.

**Data ops (admin):** pannello Admin → tab *Data ops* — statistiche `mart.__TABLES__`, schema `INFORMATION_SCHEMA`, link Console, clear cache + prewarm, job asincroni (`dashboard_data_jobs` / `dashboard_seed_profiles` su Firestore). Refresh precalc modulare: `scripts/precalc_refresh/` + `scripts/refresh_precalc_tables.py`. Cleanup oggetti di test: `bigquery/cleanup_idempotent.sql`. Deploy Job lunghi: `deploy/DATA_PIPELINE_JOB.md`.

---

## Comandi essenziali

Il file `.env` nella **root del repository** viene caricato sempre da lì (`app/main.py` e `app/db/client.py`), non dalla directory di lavoro corrente. **`GCP_PROJECT_ID` nel launch di VS/Cursor non deve essere la stringa vuota**: se è presente ma vuota, `os.environ.get("GCP_PROJECT_ID", default)` non applica il default e i loghi finivano su `/static/` (placeholder). Risolto in `app/db/client.py` con `_resolve_gcp_project_id()`. Loghi in UI: sempre URL HTTPS verso GCS (`BRAND_LOGOS_PUBLIC_BASE` o bucket da `PROJECT_ID` o `DEFAULT_GCS_BRAND_LOGOS_BASE` in [`app/constants.py`](app/constants.py)). **Non passano da Firestore** (il browser apre l’URL). Avvio consigliato: [`scripts/run-local-server.ps1`](scripts/run-local-server.ps1).

```bash
# Avvio locale (consigliato su Windows: PYTHONPATH = root repo, libera porta 8000)
# .\scripts\run-local-server.ps1

pip install -r app/requirements.txt
# Da root repo con: $env:PYTHONPATH = (Get-Location).Path
uvicorn app.main:app --reload --host 127.0.0.1 --port 8000

# Firestore in locale: emulatore (opzione A)
# gcloud beta emulators firestore start --host-port=127.0.0.1:8080
# $env:FIRESTORE_EMULATOR_HOST="127.0.0.1:8080"; $env:GCP_PROJECT_ID="demo-local"

# Firestore su GCP reale (opzione B, stesso SA di BigQuery): .env senza FIRESTORE_EMULATOR_HOST
# .\scripts\setup-bigquery-service-account.ps1
# oppure, SA già creato solo per BigQuery: .\scripts\grant-firestore-to-dashboard-sa.ps1

# Login gcloud con browser su Windows (finestra CMD dedicata, usa gcloud.cmd)
# .\scripts\gcloud-browser-login.ps1
# Rinnovo ADC utente (apre il browser; in finestra CMD azzera GOOGLE_APPLICATION_CREDENTIALS per evitare conflitto col SA):
# .\scripts\gcloud-application-default-login.ps1

# Verifica loghi GCS: python scripts/verify_brand_logo_env.py
# Verifica HTML home (logo in topbar): $env:PYTHONPATH = (Get-Location).Path; python scripts/check_landing_logo_html.py
# Admin: GET /api/admin/brand-logo-debug?brand_id=1

# Provisioning GCP (una tantum): API + bucket loghi + IAM
# .\scripts\provision-firestore-and-brand-logos.ps1 -ProjectId mediaexpertdashboard
# .\scripts\sync-brand-logos-to-gcs.ps1 -BucketName mediaexpertdashboard-brand-logos

# Rieseguire seed BigQuery (dopo modifiche a schema_and_seed.sql)
gcloud auth application-default login
python scripts/run_bigquery_schema.py

# Aggiornare tabelle precalcolate (dopo cambio feed dati)
python scripts/refresh_precalc_tables.py

# Alternativa: una sola sequenza (generate_seed → schema + derive → precalc); poi svuotare cache admin
# .\scripts\reseed_full_pipeline.ps1

# Rigenerare prodotti (opzionale)
python scripts/generate_seed_data.py
python scripts/run_bigquery_schema.py
```

---

## GCP / BigQuery (ambiente locale)

- **`gcloud auth login`** e **`gcloud auth application-default login`** salvano le credenziali **sulla macchina dello sviluppatore** (config gcloud + ADC). Non è possibile “memorizzare il login” nella chat tra sessioni Cursor.
- **Per gli agenti:** non chiedere ogni volta di rifare il login. Eseguire direttamente `gcloud`, `bq` o script Python; **solo se** il comando fallisce con errore di autenticazione (es. token scaduto), suggerire `gcloud auth login` e/o `gcloud auth application-default login` (o gli script `scripts/gcloud-browser-login.ps1`, `scripts/gcloud-application-default-login.ps1`).

---

## Documentazione

| File | Contenuto |
|------|-----------|
| `docs/PROJECT_GUIDE.md` | Guida completa: struttura, API, auth, Sales dashboards |
| `docs/ARCHITECTURE.md` | Architettura, flussi, pattern modulare |
| `docs/DATABASE_SCHEMA.md` | Schema BigQuery, tabelle, relazioni |
| `docs/CATEGORIES_AND_SUBCATEGORIES.md` | Elenco 10 categorie + 72 subcategorie |
| `docs/SEED_DATA_SPEC_FOR_GENERATION.md` | Specifica per generate_seed_data.py |
| `docs/CACHE_AND_PERFORMANCE.md` | Cache, pre-warming, Redis, performance su Cloud |
| `docs/PRECALC_TABLES.md` | Tabelle precalcolate, mappatura dashboard, come aggiungere nuove |
| `docs/GITHUB_CLOUD_BUILD.md` | Setup CI/CD Cloud Build |
