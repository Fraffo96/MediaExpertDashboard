# Tabelle precalcolate – Media Expert Dashboard

Le tabelle precalcolate (`precalc_*`) su BigQuery contengono **dati già pronti** per i grafici. Le query sono solo `SELECT ... WHERE` (nessuna aggregazione a runtime). Tutti i calcoli (SUM, percentuali, brand vs media) avvengono durante il refresh.

---

## Panoramica

- **Posizione**: dataset `mart` su BigQuery (progetto GCP `mediaexpertdashboard` o da `GCP_PROJECT_ID`)
- **Quando si aggiornano**: on-demand tramite pulsante "Re-calculate dashboards" o `python scripts/refresh_precalc_tables.py`
- **Lavoro locale**: l'app locale e Cloud Run usano le stesse tabelle su GCP (connessione via `gcloud auth application-default login`)

---

## Mappatura dashboard → tabelle

| Dashboard | Tabelle precalc | Contenuto |
|-----------|-----------------|-----------|
| **Market Intelligence** | `precalc_sales_agg`, `precalc_pie_brands_*`, `precalc_prev_year_pct`, `precalc_peak_agg`, `precalc_roi_agg`, `precalc_mi_segment_by_product` | Vendite, pie, prev year, peak, ROI, discount, incremental YoY, Segment by SKU |
| **Brand Comparison** | stesse tabelle (filtro `brand_id IN (brand, competitor)`) | Pie, promo share, ROI, peak, discount depth |
| **Check Live Promo** | `precalc_promo_live_sku` | SKU-level promo performance, partition by date (last 7/30 days) |
| **Basic** | (fase 2) | KPI, vendite – richiede segment/gender in precalc |
| **Promo Creator** | `precalc_promo_creator_benchmark`, `precalc_roi_agg` | Discount benchmark, ROI benchmark |
| **Marketing (Segment Summary, Purchasing)** | `precalc_mkt_segment_categories`, `precalc_mkt_segment_skus`, `precalc_mkt_purchasing_channel`, `precalc_mkt_purchasing_peak` | Top categories/SKUs per segment, channel mix, peak events (full year only) |

---

## Tabelle

| Tabella | Schema | Uso |
|---------|--------|-----|
| `precalc_sales_agg` | year, brand_id, brand_name, category_id, parent_category_id, channel, gross_pln, units, promo_gross, discount_depth_weighted | Vendite, promo share, discount depth |
| `precalc_peak_agg` | year, brand_id, category_id, parent_category_id, channel, peak_event, gross_pln | Peak events |
| `precalc_roi_agg` | year, brand_id, category_id, promo_type, avg_roi, incremental_sales_pln | ROI promo |
| `precalc_incremental_yoy` | year, brand_id, category_id, parent_category_id, total_gross, incremental_sales_pln | Incremental YoY |
| `precalc_pie_brands_category` | year, category_id, brand_id, brand_name, channel, gross_pln, units, pct_value, pct_volume | Pie chart per parent category |
| `precalc_pie_brands_subcategory` | year, category_id, brand_id, brand_name, channel, gross_pln, units, pct_value, pct_volume | Pie chart per subcategory |
| `precalc_prev_year_pct` | year, category_id, brand_id, channel, pct_value_prev | Market share anno precedente |
| `precalc_sales_bar_category` | year, brand_id, category_id, category_name, brand_gross_pln, brand_units, media_gross_pln, media_units | Bar chart value/volume by category (dati pronti, zero aggregazioni) |
| `precalc_sales_bar_subcategory` | year, brand_id, parent_category_id, category_id, category_name, brand_gross_pln, brand_units, media_gross_pln, media_units | Bar chart value/volume by subcategory (dati pronti, zero aggregazioni) |
| `precalc_mi_segment_by_product` | year, product_id, brand_id, segment_id, segment_name, channel, gross_pln, units | MI: breakdown segmenti per SKU (`channel` vuoto = tutti i canali; altrimenti web/app/store) |
| `precalc_promo_creator_benchmark` | year, category_id, brand_id, media_avg_discount, brand_avg_discount | Promo Creator: benchmark discount |
| `precalc_promo_live_sku` | date, product_id, product_name, brand_id, brand_name, category_id, category_name, parent_category_id, promo_id, promo_name, channel, gross_pln, units, order_count | Check Live Promo: SKU-level promo performance. Partition by date per query veloci (last 7/30 days) |
| `precalc_mkt_segment_categories` | year, segment_id, category_id, parent_category_id, category_name, level, gross_pln | Marketing: top categories per segment (level 1=parent, 2=subcategory) |
| `precalc_mkt_segment_skus` | year, segment_id, product_id, product_name, brand_name, category_id, parent_category_id, gross_pln, units | Marketing: top SKUs per segment |
| `precalc_mkt_purchasing_channel` | year, segment_id, segment_name, channel, gross_pln | Marketing: channel mix per segment |
| `precalc_mkt_purchasing_peak` | year, segment_id, segment_name, peak_event, orders_pct, gross_pln | Marketing: peak events per segment |

---

## Come aggiungere una nuova dashboard

1. **Creare DDL** in `bigquery/precalc_tables.sql` per le nuove tabelle
2. **Aggiungere logica di popolamento** in `scripts/refresh_precalc_tables.py` (CREATE OR REPLACE TABLE ... AS SELECT)
3. **Creare query** in `app/db/queries/<nome>/` o `app/db/queries/precalc.py` che leggono da precalc
4. **Registrare** nel servizio `app/services/<nome>.py`, con fallback a query live se periodo non è anno intero
5. **Aggiornare** questa documentazione

---

## Comandi

```bash
# Popolare/aggiornare le tabelle precalcolate
python scripts/refresh_precalc_tables.py

# DDL iniziale (eseguito da run_bigquery_schema.py, fase 4)
python scripts/run_bigquery_schema.py
```

**Pulsante UI**: "Re-calculate" in alto a destra (solo admin). Chiama `POST /api/admin/recalculate`.

---

## Quando usare precalc vs query live

- **Precalc**: quando `period_start` e `period_end` sono un anno intero (es. `2024-01-01` … `2024-12-31`)
- **Query live**: fallback per periodi custom (es. Q1, semestre) o se precalc è vuota o errore

---

## Riferimenti

- `app/db/queries/precalc.py` – query che leggono da precalc
- `app/services/market_intelligence.py` – integrazione precalc per MI
- `app/services/brand_comparison.py` – integrazione precalc per BC
- `AGENTS.md` – comandi e stack
