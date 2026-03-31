# Cache e performance – strategia long-term

Guida per ottenere caricamenti quasi istantanei per l’utente finale, anche su Google Cloud con nuovi utenti.

---

## Situazione attuale

- **Cache in-memory** (`app/services/_cache.py`): TTL 15 min, per processo. Su Cloud Run ogni istanza ha la propria cache; quando si scala a zero o si riavvia, la cache si perde.
- **Market Intelligence**: una chiamata `/api/market-intelligence/all-years` carica tutti gli anni in parallelo sul server; i dropdown year sono istantanei da subito.
- **Brand Comparison, Promo Creator**: usano endpoint separati; i filtri dropdown usano dati da `/api/filters` o embedded nel template.

---

## Obiettivi per l’utente finale

1. **Caricamento iniziale veloce**: < 2–3 secondi
2. **Dropdown istantanei**: nessuna attesa dopo il primo load
3. **Nuovi utenti su Cloud**: stesso comportamento del primo utente (cache warm)

---

## Strategia long-term consigliata

### 1. Cache distribuita (Redis / Memorystore)

**Problema**: la cache in-memory non è condivisa tra istanze Cloud Run e si perde al restart.

**Soluzione**: usare Redis (es. Cloud Memorystore) come cache distribuita.

- **Vantaggi**: cache condivisa tra tutte le istanze; sopravvive ai restart; TTL configurabile; adatto a query pesanti (BigQuery).
- **Implementazione**:
  - Aggiungere `redis` o `aioredis` in `requirements.txt`
  - Modificare `app/services/_cache.py` per usare Redis quando `REDIS_URL` è configurato
  - Fallback a cache in-memory se Redis non è disponibile (sviluppo locale)

**Esempio**:
```python
# _cache.py con Redis
import os
REDIS_URL = os.getenv("REDIS_URL")
if REDIS_URL:
    # uso redis
else:
    # fallback in-memory
```

### 2. Pre-warming della cache

**Problema**: il primo utente che arriva su un’istanza cold paga il costo delle query BigQuery.

**Soluzione**: job periodico che pre-riscalda la cache.

- **Cloud Scheduler** + **Cloud Functions** o **Cloud Run Job**: invoca periodicamente (es. ogni ora) gli endpoint che servono i dati più usati.
- **Endpoint**: `/api/market-intelligence/all-years`, `/api/filters`, ecc.
- **Autenticazione**: usare un service account interno o un token di pre-warming.

**Esempio**:
```yaml
# cron ogni ora
schedule: "0 * * * *"
http_target:
  uri: https://your-app.run.app/api/market-intelligence/all-years
  headers:
    Authorization: Bearer ${PREWARM_TOKEN}
```

### 3. Tabelle precalcolate (implementato)

**Problema**: alcune query BigQuery sono complesse e lente.

**Soluzione**: tabelle precalcolate (`mart.precalc_*`) su BigQuery, popolate on-demand.

- **DDL**: `bigquery/precalc_tables.sql` (eseguito da `run_bigquery_schema.py` fase 4)
- **Popolamento**: `python scripts/refresh_precalc_tables.py` oppure pulsante "Re-calculate dashboards" (solo admin)
- L’API legge da queste tabelle invece di eseguire join complessi su `fact_sales_daily`.
- **Lettura**: API usa `app/db/queries/precalc.py` quando periodo = anno intero; fallback a query live. Vedi `docs/PRECALC_TABLES.md`

### 4. Configurazione Cloud Run

- **Min instances**: 1 per evitare cold start per il primo utente
- **CPU**: 1 o 2 per richieste parallele
- **Memory**: 2–4 GB se le query sono pesanti
- **Timeout**: adeguato per query BigQuery (es. 60–120 s)

---

## Priorità implementative

| Priorità | Azione | Effetto |
|----------|--------|---------|
| 1 | Min instances = 1 su Cloud Run | Elimina cold start per il primo utente |
| 2 | Redis (Memorystore) per cache | Cache condivisa e persistente |
| 3 | Pre-warming con Cloud Scheduler | Cache sempre calda |
| 4 | Tabelle precalcolate (precalc_*) | Query BigQuery più veloci – implementato |

---

## Market Intelligence – flusso attuale

1. **Caricamento iniziale**: `GET /api/market-intelligence/all-years` → tutti gli anni in parallelo sul server. Una sola chiamata HTTP.
2. **Dropdown year**: dati già in `window._miDataByYear`; aggiornamento solo client-side.
3. **Dropdown category/channel/metric**: nessuna API; solo `applyViewFromState()`.
4. **Cache**: ogni anno viene cachato da `get_mi_all` (base, sales, promo, peak, discount); il secondo utente che chiede lo stesso anno riceve dalla cache (se stessa istanza o Redis).

---

## Setup Cloud Run (checklist)

Passo-passo deploy, VPC, Secret Manager e substitution sui trigger: **[GITHUB_CLOUD_BUILD.md](GITHUB_CLOUD_BUILD.md)** (sezione 3).

1. **Redis (Memorystore)**: crea istanza, imposta `REDIS_URL` su Cloud Run (variabile o secret).
2. **PREWARM_TOKEN**: genera token sicuro, imposta su Cloud Run e in Cloud Scheduler.
3. **Cloud Scheduler**: `.\scripts\setup-prewarm-scheduler.ps1` oppure crea job manualmente.
4. **Min instances**: già in `cloudbuild.yaml` (`--min-instances=1`).
5. **Tabelle precalcolate**: `python scripts/run_bigquery_schema.py` (fase 4 DDL) + `python scripts/refresh_precalc_tables.py` (popolamento).

---

## Riferimenti

- `app/services/_cache.py` – cache Redis + fallback in-memory
- `app/services/prewarm.py` – logica pre-warming
- `app/main.py` – endpoint `/internal/prewarm`
- `scripts/setup-prewarm-scheduler.ps1` – setup Cloud Scheduler
- `bigquery/materialized_views.sql` – MV per query full-year
- `AGENTS.md` – comandi e stack
