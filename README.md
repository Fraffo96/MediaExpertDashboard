# Media Expert Dashboard

Dashboard analitica per retailer elettronica (stile Media Expert). Vendite, performance promozioni, dipendenza da eventi stagionali. Pensata per decisioni rapide (<30 s per grafico).

**Stack:** FastAPI + BigQuery + Chart.js | Deploy su Cloud Run

---

## Quick start

### 1. Database (BigQuery)

```powershell
gcloud auth application-default login
.\scripts\setup-databases-gcp.ps1   # Crea dataset raw/mart
python scripts/run_bigquery_schema.py   # Schema + seed
```

### 2. App locale

```bash
pip install -r app/requirements.txt
uvicorn app.main:app --reload
```

Apri `http://localhost:8000`.

### 3. Deploy

Push su `main` → Cloud Build → deploy su Cloud Run.

---

## Struttura

| Path | Descrizione |
|------|-------------|
| `app/` | FastAPI, template, static, auth |
| `app/db/queries/` | Query BigQuery (moduli: basic, promo, `market_intelligence/`, ecc.) |
| `app/services/` | Orchestrazione API (market_intelligence, brand_comparison, ecc.) |
| `bigquery/schema_and_seed.sql` | DDL + seed (fonte unica) |
| `bigquery/derive_sales_from_orders.sql` | Vista + fact_sales_daily, fact_promo_performance |
| `scripts/run_bigquery_schema.py` | Esegue schema + derive su BigQuery |

---

## Documentazione

| File | Contenuto |
|------|-----------|
| `AGENTS.md` | **Guida per agenti AI** – dove modificare, convenzioni |
| `docs/PROJECT_GUIDE.md` | Guida completa progetto |
| `docs/DATABASE_SCHEMA.md` | Schema BigQuery |
| `docs/GITHUB_CLOUD_BUILD.md` | Setup CI/CD |

---

## Progetto GCP

- **Progetto:** `mediaexpertdashboard`
- **Dataset:** `raw`, `mart`
- **Periodo dati:** 2023–2025
