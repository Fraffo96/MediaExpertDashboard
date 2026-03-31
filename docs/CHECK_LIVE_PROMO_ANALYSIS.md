# Check Live Promo – Analisi dati disponibili

> Analisi preliminare per implementare "Check live promo" nel sales access.

## Cosa richiede l’idea (da conversazione)

- Reazioni dei consumatori al brand **in tempo reale** (es. ultime 24h)
- Dati da **web e app**
- Metriche: **ricerche**, **click-through**, **click sulle promozioni**

## Dati attualmente disponibili (schema BigQuery)

### Tabelle rilevanti

| Tabella | Granularità | Dati promo |
|---------|-------------|------------|
| **fact_sales_daily** | date × brand × subcategory × segment | promo_flag, promo_id, discount_depth_pct |
| **fact_promo_performance** | date × promo × brand × category | attributed_sales, incremental_sales, roi |
| **fact_orders** | date × customer | channel (web/app/store), promo_flag, promo_id |
| **dim_promo** | promo | start_date, end_date, promo_type |

### Cosa non abbiamo

- Nessun dato **real-time** (tutto aggregato a livello giornaliero)
- Nessun **click tracking** (ricerche, click, impressions)
- Nessun **web/app analytics** (Google Analytics, Hotjar, ecc.)
- Nessun **event stream** (last 24h, ultime ore)

## Cosa possiamo implementare con i dati attuali

### 1. **Promo attive ora** (statico)

- Da `dim_promo`: `start_date` e `end_date`
- Widget: "Promozioni attualmente attive" con elenco e date
- Nessun dato di performance

### 2. **Performance promo recente** (daily)

- Da `fact_promo_performance` e `fact_sales_daily`
- Ultimi N giorni (es. 7 o 30 gg)
- KPI: vendite attribuite, incremental, ROI per promo
- Non è real-time, ma “quasi recente”

### 3. **Mix canale per promo** (daily)

- Da `fact_orders` + `v_sales_daily_by_channel`
- Ordini/vendite per promo per canale (web/app/store)
- Ultimi giorni

### 4. **Promo oggi / ieri** (se dati aggiornati)

- Solo se il feed dati è aggiornato ogni giorno
- `fact_sales_daily` e `fact_promo_performance` con `date = CURRENT_DATE() - 1`

## Raccomandazione

Per un vero “Check live promo” (reazioni in tempo reale, ultime 24h, click) servirebbero:

1. **Integrazione con analytics** (GA4, Amplitude, Mixpanel, ecc.)
2. **Eventi da web/app** (click promo, add-to-cart, conversioni)
3. **Pipeline near real-time** (es. BigQuery Streaming, Dataflow)

Con i dati attuali:

- Implementare: **“Promo attive ora”** + **“Performance promo ultimi 7 gg”**
- Posizionare: nuova sezione o link nel sales access (es. “Promo attive” / “Performance recente”)
- Evitare: claim “live” o “real-time” se non supportati dai dati

## Prossimi passi (se si procede)

1. API: `GET /api/promo/active` (da dim_promo)
2. API: `GET /api/promo/recent-performance` (da fact_promo_performance, ultimi 7 gg)
3. UI: nuovo blocco/entry in Brand Comparison o Market Intelligence
4. Naming: “Promo attive” / “Performance promo recente” invece di “Check live promo”
