# Media Expert Dashboard – Data Architecture (Retailer PL)

Set di database **verosimili** per un grande retailer omnichannel in Polonia (PLN), stile Media Expert (e-commerce + app + negozi fisici). Include schema OLTP, tracking, marketing e data mart per dashboard analitiche.

## Contenuto

| File | Descrizione |
|------|-------------|
| `migrations.sql` | Schema Postgres: `core_commerce`, `promotions_marketing`, `digital_analytics`, `identity`, tabelle e indici |
| `seed.py` | Generazione dati sintetici (Faker, seed riproducibile): users, products, orders, promos, sessions/events |
| `mart.sql` | Viste (star schema) per `analytics_mart`: dim_date, dim_category, dim_brand, dim_product, dim_promo, dim_channel, dim_customer, fact_sales_daily, fact_promo_performance, fact_customer_activity |
| `dashboard_queries.sql` | Query pronte per BASIC, PROMO PERFORMANCE e CUSTOMER dashboard |
| `docker-compose.yml` | (Opzionale) Postgres 15 per sviluppo locale |

## Requisiti

- **PostgreSQL** 14+ (estensione `uuid-ossp`, `pgcrypto`)
- **Python** 3.10+ per lo seed: `pip install -r requirements.txt`

## Come eseguire

### 1. Database (locale o Docker)

**Opzione A – Docker (consigliato)**

```bash
docker compose up -d
# Crea DB `retailer_pl`, utente postgres/postgres, porta 5432
```

**Opzione B – Postgres già installato**

Crea un database, ad es.:

```sql
CREATE DATABASE retailer_pl ENCODING 'UTF8';
```

### 2. Migrations

```bash
psql -h localhost -U postgres -d retailer_pl -f migrations.sql
```

### 3. Analytics Mart (viste)

```bash
psql -h localhost -U postgres -d retailer_pl -f mart.sql
```

### 4. Seed dati sintetici

```bash
# Variabile d'ambiente (opzionale)
export DATABASE_URL="postgresql://postgres:postgres@localhost:5432/retailer_pl"

# Esecuzione con scale 0.15 (~15% volumi, più veloce)
python seed.py --seed 42 --scale 0.15

# Scale 1.0 per volumi pieni (~500k ordini, ~2M eventi, più lento)
python seed.py --seed 42 --scale 1.0

# Saltare sessioni/eventi (solo ordini e promo) per test rapidi
python seed.py --scale 0.1 --skip-events
```

Parametri `seed.py`:

- `--dsn` – connection string (default: `postgresql://postgres:postgres@localhost:5432/retailer_pl`)
- `--seed` – seed random (default: 42)
- `--scale` – fattore scala 0.01–1.0 (default: 0.15)
- `--skip-events` – non genera sessioni/eventi digital_analytics

### 5. Dashboard queries

Le query in `dashboard_queries.sql` usano parametri nominali (`:period_start`, `:period_end`, `:year_current`, `:year_prior`). In `psql` si possono impostare e poi eseguire, ad es.:

```sql
\set period_start '2025-01-01'
\set period_end   '2025-12-31'
\set year_current 2025
\set year_prior   2024
\i dashboard_queries.sql
```

Oppure sostituire i placeholder con valori fissi o con variabili dell’applicazione (es. BI tool).

## Schema logico

- **core_commerce**: `users` (global_user_id UUID), `user_addresses`, `products`, `brands`, `categories`, `orders`, `order_items`, `stores`, `returns`
- **promotions_marketing**: `promos`, `promo_rules`, `coupons`, `promo_exposures`, `promo_clicks`, `promo_attribution`, `promo_costs`
- **digital_analytics**: `sessions`, `events`, `searches`, `cart_events`
- **identity**: `identity_stitch` (guest_session_id → global_user_id)
- **analytics_mart**: viste dimensionali e fatti (dim_*, fact_*)

Identità: `global_user_id` è la chiave utente condivisa; per i guest si usa `guest_session_id` e, dopo riconoscimento, `identity_stitch`.

## Dashboard coperte

1. **BASIC**: category sales (PLN), promo share of sales, YoY incremental, promo ROI, avg discount depth, peak events (Black Friday, Xmas, Back-to-school, ecc.)
2. **PROMO**: performance per tipo/meccanica e per categoria/brand, uplift vs baseline (4 settimane precedenti, stesso weekday), incremental vs prior period, post-promo (pre vs post stesso numero di giorni)
3. **CUSTOMER**: comportamento d’acquisto per quarter, dipendenza dal calendario promo; placeholder per HCG/segment in `dim_customer.segment_hcg_placeholder`

## Note

- **Baseline** (uplift): media delle 4 settimane precedenti (stesso giorno della settimana); uplift = vendite durante promo − baseline.
- **Post-promo**: confronto pre (stessi giorni prima della promo) vs post (stessi giorni dopo la fine promo).
- Valuta: **PLN**; IVA in colonne dedicate.
- Calendario: `dim_date` con flag per Black Friday, Xmas, Back-to-school, New Year, Easter.

---

## Dashboard BI su Google Cloud (Metabase + BigQuery)

Per una **dashboard BI interattiva** su GCP con **Row Level Security per brand** (Sodastream, Nespresso, Samsung):

| Risorsa | Descrizione |
|--------|-------------|
| `docs/ARCHITECTURE.md` | Architettura GCP, Service Account, connessione Metabase–BigQuery, RLS |
| `docs/METABASE_SETUP.md` | Setup Metabase passo-passo, gruppi, utenti, filtri per brand |
| `docs/GITHUB_CLOUD_BUILD.md` | Nuova repo GitHub e trigger Cloud Build per deploy Metabase |
| `bigquery/schema_and_seed.sql` | Dataset `raw`/`mart`, tabelle, seed 2025 con picchi BF/Xmas |
| `bigquery/views_rls.sql` | (Opzionale) Viste per brand per RLS in BigQuery |
| `scripts/setup-gcp.sh` | Creazione Service Account, IAM, chiave JSON, dataset |
| `scripts/deploy-metabase.sh` | Deploy Metabase su Cloud Run |
| `dashboard/bigquery_queries.sql` | Query dashboard (Category Sales, Promo Share, YoY, ROI, Peak, Pre/During/Post) |
| `cloudbuild.yaml` | Build/deploy Metabase su push (trigger GitHub) |

**Quick start GCP (progetto `mediaexpertdashboard`):**

1. `./scripts/setup-gcp.sh` → crea SA e chiave, dataset BigQuery.
2. Esegui `bigquery/schema_and_seed.sql` nella console BigQuery (o `bq query`).
3. `./scripts/deploy-metabase.sh` → deploy Metabase su Cloud Run.
4. In Metabase: aggiungi database BigQuery con il JSON del SA; crea gruppi e utenti (Sodastream, Nespresso, Samsung), imposta filtri dati per `brand_name`.
5. Collega la repo a Cloud Build e crea trigger su `main` → deploy automatico a ogni push.
