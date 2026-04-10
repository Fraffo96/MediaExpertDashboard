# Media Expert Dashboard – Project Guide

Documentazione di riferimento per agenti AI e sviluppatori.

---

## Cosa fa questo progetto

Dashboard analitica per **Media Expert** (retailer elettronica polacco). Pensata per **Senior Brand Manager** che deve prendere decisioni rapide su vendite, promozioni e confronto con il mercato.

**Direzione:** Sales Dashboard con flusso guidato post-login: landing → Market Intelligence, Brand Comparison, Promo Creator. Ogni utente vede solo il proprio brand e le categorie/subcategorie assegnate.

**Stack:** FastAPI (Python) + BigQuery + Chart.js. Deploy su Cloud Run via Cloud Build.

---

## Flusso post-login

```
Login → Admin? → Sì: /admin
              → No: Landing "What do you need to do?"
                    ├── I want to see how the market is going → /market-intelligence
                    ├── I want to compare my brand to a competitor → /brand-comparison
                    └── I want to plan a new promo → /promo-creator
```

---

## Tech Stack

| Layer | Tecnologia | File principali |
|-------|-----------|----------------|
| **Frontend** | Jinja2 templates + Chart.js 4.x | `app/templates/`, `app/static/js/` |
| **Backend** | FastAPI, Python 3.13 | `app/main.py` |
| **Database** | BigQuery (dataset `mart`) | `bigquery/schema_and_seed.sql`, `derive_sales_from_orders.sql` |
| **Auth** | SQLite locale (`app_data.db`) + JWT | `app/auth/` |
| **Deploy** | Docker + Cloud Build + Cloud Run | `Dockerfile`, `cloudbuild.yaml` |
| **CSS** | Custom design system (dark theme) | `app/static/css/style.css` |

---

## BigQuery: Connessione persistente (locale)

Per evitare che le credenziali scadano (`gcloud auth application-default login` scade ~90 giorni):

```powershell
.\scripts\setup-bigquery-service-account.ps1
```

Lo script crea un service account, genera la chiave JSON in `credentials/`, e configura `.env`. L'app carica automaticamente `.env` all'avvio. La connessione resta valida finché non revochi la chiave.

**Diagnostica:** `python scripts/diagnostics/diagnose_bigquery.py` per verificare connessione e dati.

---

## Struttura file (modulare)

Ogni dashboard segue un pattern modulare: template principale + partials + JS separati per core, filtri e chart.

```
app/
  main.py                         # FastAPI: routes, API endpoints
  db/
    client.py                     # BigQuery client (run_query)
    queries/
      shared.py                   # Filtri: categories, subcategories, brands, promo_types
      basic/                      # Basic Dashboard (legacy), package query
      promo.py, customer.py, simulation.py, why_buy.py, product.py
      market_intelligence/        # Brand vs media category benchmarks (moduli)
        __init__.py               # Re-export per retrocompatibilità
        shared.py                 # params(), where_cat_subcat()
        categories.py             # query_brand_categories, query_brand_subcategories
        sales.py                  # Vendite value/volume, pie category/subcategory
        promo.py                  # Promo share, ROI, incremental YoY
        discount.py               # Discount depth
        peak.py                   # Peak events
      brand_comparison.py         # Competitor comparison, product deep dive
      promo_creator.py            # Suggestions, benchmarks, cannibalization
  services/
    dashboard_data.py             # Orchestrazione: parallelismo, cache 5min
  auth/
    database.py                   # SQLite init, migrazioni
    models.py                     # User (brand_id, allowed_category_ids, allowed_subcategory_ids, access_types)
    routes.py                     # Login/logout, admin CRUD
    security.py                   # JWT, password hashing
  templates/
    base.html                     # Layout: sidebar, topbar, Chart.js defaults
    landing.html                  # Post-login: "What do you need to do?"
    login.html
    admin.html
    help.html
    marketing.html                # Placeholder "Coming soon"
    # Sales dashboards (modulari)
    market_intelligence.html
    market_intelligence/
      _filter_bar.html
      _section_sales.html
      _section_promo.html
      _section_peak.html
    brand_comparison.html
    brand_comparison/
      _filter_bar.html
      _kpi_row.html
      _chart_roi.html
      _products_deep_dive.html
    promo_creator.html
    promo_creator/
      _form.html
      _suggestions.html
    # Legacy dashboards
    basic.html, basic/_*.html
    promo.html, customer.html, simulation.html, compare.html, products.html, ecosystem.html
  static/
    css/style.css
    js/
      market_intelligence/        # core.js, filters.js, dashboard.js, charts/*.js
      brand_comparison/            # core.js, filters.js, dashboard.js
      promo_creator/               # core.js, filters.js, dashboard.js
      basic/                       # core.js, filters.js, dashboard.js, charts/*.js
    data/glossary.json
    img/
bigquery/
  schema_and_seed.sql
  derive_sales_from_orders.sql
  dim_product_generated.sql
scripts/
  run_bigquery_schema.py
  generate_seed_data.py
  setup-databases-gcp.ps1
  setup-tutto.ps1
docs/
  PROJECT_GUIDE.md                 # Questo file
  ARCHITECTURE.md                  # Architettura e flussi
  DATABASE_SCHEMA.md
  CATEGORIES_AND_SUBCATEGORIES.md
  SEED_DATA_SPEC_FOR_GENERATION.md
  GITHUB_CLOUD_BUILD.md
tests/
  test_dashboard.py
  test_api_basic_response.py
```

---

## Sales Dashboards (nuova direzione)

### Market Intelligence (`/market-intelligence`)
7 chart con confronto **brand vs media category/subcategory**:

**Query (package `queries/market_intelligence/`):**
| Modulo | Contenuto |
|--------|-----------|
| `shared.py` | `params()`, `where_cat_subcat()` — utilità condivise |
| `categories.py` | `query_brand_categories`, `query_brand_subcategories` |
| `sales.py` | Vendite value/volume, pie per category/subcategory |
| `promo.py` | Promo share, ROI, incremental YoY |
| `discount.py` | Discount depth |
| `peak.py` | Peak events |
- Category sales by value (PLN)
- Category sales by volume (units)
- Promo share of sales
- Ave incremental YoY
- Promo ROI by type
- Ave discount depth
- Peak events dependence

Filtri: period, category, subcategory. Brand fissato da `user.brand_id`.

### Brand Comparison (`/brand-comparison`)
- Selezione competitor (stessa category/subcategory)
- KPI: vendite, promo share (mio vs competitor)
- Chart ROI: mio vs competitor
- Product deep dive: top 5 prodotti per brand nella subcategory

### Promo Creator (`/promo-creator`)
- Form: period, promo type, discount depth, category, subcategory
- Pannello Suggestions: benchmark vs media category, ROI da promos simili, avviso cannibalization

### Marketing (`/marketing`)
Placeholder "Coming soon".

---

## Auth System

- SQLite locale (`app_data.db`) per utenti
- JWT via cookie `access_token`
- Ruoli: **admin** (accesso completo) e **user** (scope limitato)

**User (non-admin):**
- `brand_id` — obbligatorio, brand dell'utente
- `access_types` — `["sales_intelligence", "marketing_insights"]`
- `allowed_category_ids` — categorie parent (1–10) che può vedere
- `allowed_subcategory_ids` — subcategorie (101+); vuoto = tutte quelle delle categorie consentite

**Tab derivate da access_types:**
- `sales_intelligence` → Market Intelligence, Brand Comparison, Promo Creator
- `marketing_insights` → Marketing (placeholder)

**Admin:** crea utenti con Brand, Access, Categories, Subcategories. Non usa Ecosystems.

---

## API Endpoints

| Endpoint | Auth | Descrizione |
|----------|------|-------------|
| `/api/market-intelligence` | Cookie | Brand vs media (usa user.brand_id) |
| `/api/brand-comparison` | Cookie | Competitors, sales/ROI comparison |
| `/api/promo-creator` | Cookie | Suggestions e benchmarks |
| `/api/basic` | — | Dati Basic Dashboard |
| `/api/promo`, `/api/customer`, `/api/simulation` | — | Legacy dashboards |
| `/api/why-buy`, `/api/compare`, `/api/products` | — | Altri |
| `/api/filters` | — | Categorie, subcategories, brands, promo_types |

Parametri comuni: `period_start`, `period_end`, `category_id`, `subcategory_id`.

---

## Database (BigQuery)

Vedi `docs/DATABASE_SCHEMA.md`.

- **59 brand**, **10 categorie + 72 subcategorie** (vedi `docs/SEED_TAXONOMY_AND_WEIGHTS.md` per tassonomia seed)
- **6 segmenti HCG**, **10 tipi promo**
- **fact_sales_daily**, **fact_promo_performance** derivati da ordini

---

## Come lavorare sul progetto

### Avviare in locale
```bash
pip install -r app/requirements.txt
uvicorn app.main:app --reload
```

### Rieseguire il seed BigQuery
```bash
gcloud auth application-default login
python scripts/generate_seed_data.py   # opzionale
python scripts/run_bigquery_schema.py
```

### Aggiungere una nuova dashboard (modulare)
1. `app/db/queries/<nome>.py` — query
2. `app/services/dashboard_data.py` — servizio `get_<nome>`
3. `app/main.py` — route pagina + API
4. `app/templates/<nome>.html` + `_*.html` — partials
5. `app/static/js/<nome>/` — core.js, filters.js, dashboard.js, charts/*.js

### Aggiungere un chart a Market Intelligence
1. Query nel modulo appropriato in `queries/market_intelligence/` (sales, promo, discount, peak)
2. Chiamata in `get_market_intelligence()` in `services/market_intelligence.py`
3. Partial in `templates/market_intelligence/_section_*.html`
4. Update in `static/js/market_intelligence/charts/*.js`

---

## Riferimenti

| File | Contenuto |
|------|-----------|
| `AGENTS.md` | Guida per agenti AI: dove modificare cosa |
| `docs/ARCHITECTURE.md` | Architettura, flussi, pattern modulare |
| `docs/DATABASE_SCHEMA.md` | Schema BigQuery |
| `docs/CATEGORIES_AND_SUBCATEGORIES.md` | Elenco categorie e subcategorie |
