# Media Expert Dashboard – Architettura

Documentazione architetturale: flussi, pattern e convenzioni.

---

## Panoramica

```
┌─────────────┐     ┌──────────────┐     ┌─────────────────┐
│   Browser   │────▶│   FastAPI    │────▶│    BigQuery     │
│  Jinja2+JS  │     │  app/main.py │     │  dataset mart   │
└─────────────┘     └──────┬───────┘     └─────────────────┘
       │                   │
       │                   ▼
       │            ┌──────────────────────────────┐
       │            │  app/services/                │
       │            │  market_intelligence,        │
       │            │  brand_comparison, promo_creator │
       │            └──────┬───────────────────────┘
       │                   │
       │                   ▼
       │            ┌──────────────┐
       └────────────│ app/db/      │
                    │ queries/*.py │
                    └──────────────┘
```

---

## Flusso post-login

```
GET /login
  → Se già autenticato: redirect /
  → Altrimenti: form login

POST /login
  → Admin: redirect /admin
  → User: redirect / (landing)

GET /
  → Admin: redirect /admin
  → User: landing.html "What do you need to do?"
          3 blocchi → /market-intelligence, /brand-comparison, /promo-creator

GET /market-intelligence, /brand-comparison, /promo-creator
  → Richiede auth
  → Verifica can_access_tab(user, tab)
  → Render template con _page_ctx (period, glossary, user, allowed_tabs)
```

---

## Pattern modulare (dashboard)

Ogni dashboard Sales segue la stessa struttura:

### Template
```
<nome>.html              # extends base, include partials
<nome>/
  _filter_bar.html       # Filtri (period, category, subcategory)
  _section_*.html        # Blocchi chart/KPI
```

### JavaScript
```
js/<nome>/
  core.js                # getParams(), buildUrl(), helper (es. barData)
  filters.js             # Cascade category → subcategory
  dashboard.js           # init, loadData, wire pulsanti
  charts/                # (opzionale) update per chart
    sales.js
    promo.js
    ...
```

### Dati
```
app/db/queries/<nome>.py        # Funzioni query (singolo file)
app/db/queries/<nome>/          # Oppure package (es. market_intelligence/)
  __init__.py                     # Re-export
  shared.py, sales.py, promo.py   # Moduli per dominio
app/services/<nome>.py          # get_<nome>(...) con cache
app/main.py                    # Route GET /<path> + GET /api/<nome>
```

---

## Scope utente

Le API Sales (`/api/market-intelligence`, `/api/brand-comparison`, `/api/promo-creator`) leggono `user.brand_id` dal cookie e filtrano automaticamente:

- **Brand:** sempre `user.brand_id`
- **Categories/Subcategories:** in futuro si applicherà `allowed_category_ids` / `allowed_subcategory_ids` alle query (scope dati lato server)

I filtri nei template mostrano tutte le categorie/subcategorie; lo scope utente si applica a livello API.

---

## Cache

I servizi in `app/services/` (es. `market_intelligence.py`, `brand_comparison.py`) usano cache in-memory (TTL 15 min) tramite `app/services/_cache.py`. Chiave: `prefix:param1|param2|...`.

---

## Convenzioni

1. **Modularità:** ogni dashboard ha i propri file; evitare file monolitici.
2. **Naming:** `market_intelligence`, `brand_comparison`, `promo_creator` (snake_case).
3. **API auth:** le API Sales richiedono cookie `access_token` e usano `user.brand_id`.
4. **Chart.js:** variabili globali `BAR_OPT`, `DOUGHNUT_OPT`, `fmt`, `fmtPct` da `base.html`.
