# Cache e performance – strategia long-term

Guida per ottenere caricamenti quasi istantanei per l'utente finale, anche su Google Cloud con nuovi utenti.

---

## Strategia attuale (in-memory con TTL lunghi)

La cache è implementata interamente **in-memory** (`app/services/_cache.py`) con TTL configurabili via variabile d'ambiente.

Con `--min-instances=1` su Cloud Run il processo Python **non viene mai spento**: la cache in-memory sopravvive tra le richieste esattamente come farebbe Redis, ma senza il costo di un'istanza Memorystore separata (~1 EUR/giorno) né di un VPC Connector (~0.36 EUR/giorno).

| Parametro | Valore | Note |
|-----------|--------|------|
| `CACHE_TTL_SECONDS` | 86400 (24h) | Dati standard (MI, BC, basic…) |
| `CACHE_TTL_LONG_SECONDS` | 604800 (7g) | Dati all-years (MI, BC, PC) |
| Cloud Run `min-instances` | 1 | Processo sempre vivo, cache mai persa |
| Cloud Run `cpu` | 1 | Prewarm di background funziona regolarmente |
| Cloud Run `memory` | 512Mi | Sufficiente per Python + cache in-memory |

### Comportamento al restart / nuovo deploy

Dopo un deploy (push su main → Cloud Build), il nuovo container parte con cache vuota. Il **prewarm on-startup** (`app/services/prewarm.py`) viene lanciato automaticamente come task asincrono al boot e ricarica i dati principali da BigQuery (con le tabelle `precalc_*`, il tempo di riscaldamento è ~10-15 secondi). La prima richiesta dopo un deploy può essere leggermente più lenta; tutte le successive vengono servite dalla cache in-memory.

### Redis (rimosso)

Redis/Cloud Memorystore è stato rimosso. La cache in-memory è sufficiente per un'app con `min-instances=1` e traffico limitato. Se in futuro l'app dovesse scalare a più istanze in parallelo con traffico elevato, Redis può essere reintrodotto impostando `REDIS_URL` nelle variabili Cloud Run e ripristinando il VPC Connector.

---

## Flusso caricamento dashboard (Market Intelligence)

1. **Primo accesso / post-deploy**: `GET /api/market-intelligence/all-years` → query BigQuery sulle tabelle `precalc_*`; dati salvati in cache in-memory.
2. **Accessi successivi**: cache hit in-memory → risposta in millisecondi, zero query BigQuery.
3. **Dropdown year/category/channel**: solo client-side (`window._miDataByYear`), nessuna API.

---

## Pre-warming della cache

Il prewarm è gestito da `app/services/prewarm.py` e viene triggerato da:

| Trigger | Quando |
|---------|--------|
| **Startup app** | Al boot del container (task async in `app/main.py`) |
| **Login utente** | Background task in `app/auth/routes.py` |
| **HTTP interno** | `GET /internal/prewarm` con header `X-Prewarm-Token` |
| **Admin** | `POST /api/admin/prewarm` con cookie admin |

Con `min-instances=1`, il prewarm on-startup è sufficiente nella maggior parte dei casi. Il Cloud Scheduler periodico (se configurato) può essere ridotto o rimosso.

**Script remoto:** `python scripts/remote_admin_flush_cache.py` (svuota cache RAM + opzionalmente prewarm).

---

## Tabelle precalcolate (implementato)

Le dashboard usano tabelle `mart.precalc_*` su BigQuery per query rapide.

- **DDL**: `bigquery/precalc_tables.sql` → eseguito da `python scripts/run_bigquery_schema.py`
- **Popolamento**: `python scripts/refresh_precalc_tables.py` oppure pulsante "Re-calculate" (solo admin)

Vedi `docs/PRECALC_TABLES.md` per la mappatura dashboard → tabelle.

---

## Configurazione Cloud Run attuale

```yaml
# cloudbuild.yaml
--memory=512Mi
--cpu=1
--no-cpu-throttling      # CPU sempre allocata per prewarm asincrono
--min-instances=1        # Nessun cold start; cache in-memory sempre calda
--max-instances=3
--timeout=120
```

### Costi stimati (idle, senza traffico)

| Servizio | EUR/giorno |
|----------|-----------|
| Cloud Run (512Mi, CPU 1, min-1, no-throttle) | ~0.15-0.18 |
| BigQuery (query on-demand, TTL 24h riduce i borescan) | ~0.05-0.10 |
| Artifact Registry + Networking | ~0.01 |
| **Totale** | **~0.18-0.25** |

---

## Riferimenti

- `app/services/_cache.py` – cache in-memory con TTL env-driven
- `app/services/prewarm.py` – logica pre-warming
- `app/main.py` – startup prewarm, endpoint `/internal/prewarm`
- `scripts/remote_admin_flush_cache.py` – clear + prewarm da CLI
- `scripts/setup-prewarm-scheduler.ps1` – setup Cloud Scheduler (opzionale)
- `AGENTS.md` – comandi essenziali e stack
