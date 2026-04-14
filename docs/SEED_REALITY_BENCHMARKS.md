# Benchmark “realtà” per il seed (proxy)

Documento di tracciabilità per i pesi in [`scripts/seed_catalog/brand_parent_revenue_weights.json`](../scripts/seed_catalog/brand_parent_revenue_weights.json). I numeri **non** replicano un singolo report di vendita Media Expert: sono **calibrazioni** per evitare artefatti (es. grandi elettrodomestici quasi assenti per brand fortemente presenti su mobile/TV).

**Ricerca web e prior macro:** sintesi fonti e caveat in [`SEED_MARKET_RESEARCH.md`](SEED_MARKET_RESEARCH.md); implementazione in [`scripts/seed_catalog/market_reality.py`](../scripts/seed_catalog/market_reality.py).

## Fonti pubbliche (orientamento)

- **Samsung Electronics** — comunicati e materiali IR 2024 (Device eXperience: Mobile eXperience vs Visual Display / Digital Appliances aggregati o per trimestre). Link indicativi nel campo `meta.sources` del JSON.
- **Altri brand** — mix qualitativo (LG su TV + bianchi, Bosch/Siemens su 5–6, Apple su2–3–7, ecc.).

## Cosa fa il JSON

- Per ogni `brand_id` elencato, pesi **normalizzati** sulle sole `parent_category_id` che il brand ha già in `BRAND_FOCUS` ([`scripts/seed_catalog/constants.py`](../scripts/seed_catalog/constants.py)).
- **`catalog_share_multiplier`**: moltiplica la “massa” usata per ripartire le **N SKU totali** tra i brand (default `1.0`). Serve perché con ~59 brand a massa simile ogni marchio riceveva pochissime righe e i mix macro (es. grandi elettrodomestici Samsung) restavano numericamente irrisori nel catalogo.
- Brand **non** presenti nel JSON: ripartizione **uniforme** tra le macro del focus (comportamento precedente per macro).
- Override env `SEED_BRAND_FOCUS_JSON` continua a definire *quali* macro; i pesi si applicano solo alle macro effettivamente presenti.

## Coerenza con BigQuery

Anche con catalogo pesato, la **quota revenue** dipende da `product_pool_seg_channel_gender` e `fact_order_items`. Per questo sono stati ampliati `seg_pref` / `ch_pref` e aggiunto un sottile baseline cross-segmento nello [`schema_and_seed.sql`](../bigquery/schema_and_seed.sql). Dettaglio: [`SEED_PIPELINE_AND_WEIGHTS.md`](SEED_PIPELINE_AND_WEIGHTS.md).
