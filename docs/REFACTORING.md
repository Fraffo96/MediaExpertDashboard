# Refactoring – Struttura modulare

Documentazione delle modifiche per ridurre file >300 righe e eliminare inefficienze.

---

## 1. Package `app/db/queries/precalc/`

**Prima:** `precalc.py` monolitico (~1360 righe)

**Dopo:** Package con moduli per dominio:

| Modulo | Righe | Contenuto |
|--------|-------|-----------|
| `common.py` | ~50 | `is_full_year_period`, `get_multi_year_full_years`, `_params_year_cat` |
| `base.py` | ~75 | `query_brand_categories`, `query_brand_subcategories`, `query_competitors` |
| `sales.py` | ~360 | Vendite value/volume, pie category/subcategory, BC |
| `promo.py` | ~400 | Promo share e ROI (BC e MI). Da spezzare in promo_bc + promo_mi se >300 |
| `peak.py` | ~175 | Peak events BC e MI |
| `discount.py` | ~145 | Discount depth BC e MI |
| `prev_year.py` | ~85 | Percentuali vendite anno precedente |
| `misc.py` | ~85 | ROI benchmark, category discount, incremental YoY |
| `__init__.py` | ~120 | Re-export per backward compatibility |

**Import:** `from app.db.queries.precalc import ...` continua a funzionare.

---

## 2. Costanti in `app/constants.py`

**Prima:** `ADMIN_CATEGORIES`, `ADMIN_SUBCATEGORIES`, `ADMIN_BRANDS`, `DP` in `main.py` (~100 righe)

**Dopo:** Spostate in `app/constants.py`

**Import:** `from app.constants import DP, ADMIN_CATEGORIES, ADMIN_SUBCATEGORIES, ADMIN_BRANDS`

---

## 3. File ancora >300 righe (da valutare)

| File | Righe | Priorità split |
|------|-------|----------------|
| `app/main.py` | ~750 | Media: estrarre router pages e API |
| `app/services/market_intelligence.py` | ~640 | Bassa: già refactorato per query consolidate |
| `app/services/brand_comparison.py` | ~590 | Bassa |
| `app/db/queries/basic.py` | ~790 | Media: package `basic/` |
| `app/static/css/style.css` | ~840 | Bassa: @import moduli |
| `app/static/js/basic/core.js` | ~700 | Media: estrarre derive-view |
| `app/auth/routes.py` | ~320 | Bassa |

---

## 4. File potenzialmente inutili

| File | Uso | Azione |
|------|-----|--------|
| `scripts/diagnose_precalc.py` | Diagnostica precalc | Mantenere (utile per debug) |
| `scripts/test_cache_timing.py` | Test cache | Mantenere |
| `scripts/benchmark_api.py` | Benchmark API | Mantenere |
| `bigquery/dim_product_generated.sql` | Generato da script | Non modificare a mano |

---

## 5. Convenzioni post-refactoring

1. **Nessun file >300 righe** per nuovo codice; file esistenti da spezzare gradualmente.
2. **Package per dominio:** `precalc/`, `market_intelligence/`, `basic/` (quando splittato).
3. **Costanti:** `app/constants.py` per valori condivisi.
4. **Re-export:** `__init__.py` mantiene API pubblica invariata.

---

## 6. Riferimenti

- `AGENTS.md` – Guida per agenti, comandi, stack
- `docs/ARCHITECTURE.md` – Architettura e flussi
- `docs/PROJECT_GUIDE.md` – Guida completa
