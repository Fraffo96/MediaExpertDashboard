# Performance Benchmark – Market Intelligence / Brand Comparison

## Risultati (cold load, cache vuota)

| Endpoint | Tempo | Note |
|----------|-------|------|
| BigQuery raw (anni) | ~1.4s | Query semplice su fact_sales_daily |
| **MI all-years** | **~99s** | 3 anni × (base + sales + promo + peak + discount + incr_yoy) |
| BC all-years | ~100s | Simile a MI |

## Cosa rallenta

1. **v_sales_daily_by_channel** – vista che fa JOIN di fact_orders (~380k) + fact_order_items (~1M+). Usata per channel web/app/store. Ogni query su questa vista è lenta.
2. **Numero di query** – Per ogni anno: ~20 query per sales (4 channel × 5 tipi) + promo + peak + discount + base. Totale ~100+ query per 3 anni.
3. **fact_sales_daily** (channel "") – più veloce perché pre-aggregata.

## Cosa è stato fatto

- **Cache** per `get_mi_all_years` e `get_bc_all_years` (TTL 1h) → 2ª visita molto più veloce
- **Partitioning + clustering** su schema BigQuery → da applicare con `python scripts/run_bigquery_schema.py` (dopo `gcloud auth application-default login`)
- **Timeout** BigQuery aumentato a 30s
- **Benchmark** in `scripts/benchmark_api.py`

## Come eseguire il benchmark

```bash
python -u scripts/benchmark_api.py
```

## Possibili ottimizzazioni future (non implementate)

1. **Tabelle pre-aggregate** – escluso per scelta
2. **Lazy-load channel** – caricare web/app/store solo quando l’utente seleziona il canale (richiede modifiche al frontend)
3. **Materialized view** per v_sales_daily_by_channel – pre-calcolo periodico
4. **BI Engine** – accelerazione in memoria su GCP
