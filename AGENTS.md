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

**Non usato:** PostgreSQL (rimosso). L'app usa **solo BigQuery** per analytics; **Firestore** per account e permessi. I loghi brand in cloud usano **URL pubblici GCS** (`BRAND_LOGOS_PUBLIC_BASE` + `/{brand_id}.png`), con fallback a `/static/img/brands/` in locale.

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

---

## Comandi essenziali

```bash
# Avvio locale
pip install -r app/requirements.txt
uvicorn app.main:app --reload

# Firestore in locale: emulatore (altrimenti usa progetto GCP reale — attenzione ai dati)
# gcloud beta emulators firestore start --host-port=127.0.0.1:8080
# $env:FIRESTORE_EMULATOR_HOST="127.0.0.1:8080"; $env:GCP_PROJECT_ID="demo-local"

# Provisioning GCP (una tantum): API + bucket loghi + IAM
# .\scripts\provision-firestore-and-brand-logos.ps1 -ProjectId mediaexpertdashboard
# .\scripts\sync-brand-logos-to-gcs.ps1 -BucketName mediaexpertdashboard-brand-logos

# Rieseguire seed BigQuery (dopo modifiche a schema_and_seed.sql)
gcloud auth application-default login
python scripts/run_bigquery_schema.py

# Aggiornare tabelle precalcolate (dopo cambio feed dati)
python scripts/refresh_precalc_tables.py

# Rigenerare prodotti (opzionale)
python scripts/generate_seed_data.py
python scripts/run_bigquery_schema.py
```

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
