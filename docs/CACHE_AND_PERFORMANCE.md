# Cache e performance – strategia long-term

Guida per ottenere caricamenti quasi istantanei per l'utente finale, anche su Google Cloud con nuovi utenti e dopo deploy/restart.

---

## Strategia attuale (Redis Memorystore + RAM)

La cache è in [app/services/_cache.py](../app/services/_cache.py):

- Con **`REDIS_URL`** impostata su Cloud Run: lettura/scrittura su **Redis** (Cloud Memorystore) con TTL; i dati **sopravvivono** a deploy, restart dell'istanza e **sono condivisi** tra tutte le revisioni/istanze Cloud Run collegate allo stesso Redis.
- **Fallback in-memory**: ogni `set_cached` aggiorna anche la RAM locale per hit veloci nello stesso processo; se Redis non risponde, si usa la RAM.

| Parametro | Valore tipico | Note |
|-----------|---------------|------|
| `REDIS_URL` | `redis://HOST:6379` | Host = IP privata Memorystore (rete `default` + VPC connector) |
| `CACHE_TTL_SECONDS` | 86400 (24h) | Dati standard (MI, BC, basic…) |
| `CACHE_TTL_LONG_SECONDS` | 604800 (7g) | Dati all-years (MI, BC, PC) |
| Cloud Run `min-instances` | 1 | Riduce cold start del processo |
| Serverless VPC Access | `run-vpc-connector` | `--vpc-egress=private-ranges-only` per raggiungere Redis |
| Memorystore | `dashboard-cache`, Basic 1GB | Stessa regione del servizio (`europe-west1`) |

### Comportamento al restart / nuovo deploy

Dopo un deploy, il nuovo container ha RAM vuota ma **Redis conserva le chiavi** ancora dentro il TTL: le API servono **cache hit** da Redis senza rifare BigQuery. Il **prewarm** in background (`app/services/prewarm.py`, `app/main.py`) continua a utile per popolare chiavi mancanti o dopo `clear cache`.

### Anti-stampede e carico BigQuery

Restano attivi **`compute_once`** (path MI/BC pesanti), **semafori** nel prewarm e in promo creator, e il merge **`PREWARM_BRAND_IDS` ∪ brand Firestore**: non entrano in conflitto con Redis.

---

## Flusso caricamento dashboard (Market Intelligence)

1. **Cache hit Redis**: `GET /api/market-intelligence/all-years` → dati da Redis → risposta rapida.
2. **Cache miss**: query BigQuery su `precalc_*`; risultato salvato in Redis (e RAM).
3. **Dropdown year/category/channel**: client-side dove previsto (`window._miDataByYear`), nessuna API aggiuntiva.

---

## Pre-warming della cache

Il prewarm è gestito da `app/services/prewarm.py` e viene triggerato da:

| Trigger | Quando |
|---------|--------|
| **Startup app** | Task async in `app/main.py` |
| **Creazione/aggiornamento utente** | Background in `app/auth/routes.py` |
| **HTTP interno** | `GET /internal/prewarm` con header `X-Prewarm-Token` |
| **Admin** | `POST /api/admin/prewarm` |

Con Redis il prewarm è **complementare** (riempie buchi / nuovi brand), non l'unica difesa contro la lentezza.

**Script remoto:** `python scripts/remote_admin_flush_cache.py` (SCAN/delete prefissi su Redis + RAM, opzionale FLUSHDB).

**Solo prewarm (popola Redis senza clear):** `python scripts/remote_admin_flush_cache.py --prewarm-only` oppure `scripts/prewarm-redis-cache.ps1` (richiede `PREWARM_TOKEN` nel `.env`).

---

## Tabelle precalcolate (implementato)

Le dashboard usano tabelle `mart.precalc_*` su BigQuery per query rapide.

- **DDL**: `bigquery/precalc_tables.sql` → eseguito da `python scripts/run_bigquery_schema.py`
- **Popolamento**: `python scripts/refresh_precalc_tables.py` o job admin

Vedi `docs/PRECALC_TABLES.md` per la mappatura dashboard → tabelle.

---

## Infrastruttura GCP (riferimento)

Creazione tipica (da adattare se ricrei l'istanza):

```bash
# VPC connector (rete default, range dedicato)
gcloud compute networks vpc-access connectors create run-vpc-connector \
  --region=europe-west1 --network=default --range=10.8.0.0/28 \
  --project=mediaexpertdashboard

# Redis Memorystore Basic 1GB
gcloud redis instances create dashboard-cache --size=1 --region=europe-west1 \
  --tier=basic --redis-version=redis_7_2 \
  --network=projects/PROJECT_ID/global/networks/default --project=PROJECT_ID

# Host: gcloud redis instances describe dashboard-cache --region=europe-west1 --format='value(host)'
```

Cloud Run:

```bash
gcloud run services update dashboard --region=europe-west1 --project=PROJECT_ID \
  --update-env-vars="REDIS_URL=redis://REDIS_PRIVATE_IP:6379" \
  --vpc-connector=run-vpc-connector --vpc-egress=private-ranges-only
```

In [cloudbuild.yaml](../cloudbuild.yaml): sostituzioni `_VPC_CONNECTOR` e `REDIS_URL` in `_EXTRA_ENV_VARS` (vedi file; aggiorna l'IP se ricrei Redis).

### Costi indicativi (ordine di grandezza)

| Voce | EUR/giorno (stima) |
|------|---------------------|
| Memorystore Basic 1GB | ~0.5–0.7 |
| VPC Serverless Access connector | ~0.35 |
| Cloud Run (min-instances, CPU/memoria attuali) | variabile |
| BigQuery (on-demand, cache piena = meno query) | variabile |

---

## Riferimenti

- `app/services/_cache.py` – Redis + RAM, TTL, `compute_once`, clear per prefisso
- `app/services/prewarm.py` – pre-warming
- `app/main.py` – avvio prewarm async
- `cloudbuild.yaml` – `_VPC_CONNECTOR`, `_EXTRA_ENV_VARS` con `REDIS_URL`
- `scripts/remote_admin_flush_cache.py` – clear + prewarm da CLI
- `AGENTS.md` – comandi essenziali e stack
