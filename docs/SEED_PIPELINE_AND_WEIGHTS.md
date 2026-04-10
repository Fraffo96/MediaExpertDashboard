# Pipeline di seed e “pesi” dei dati

Documento di riferimento per il brainstorming: **dove** sono definiti volumi, mix catalogo, segmenti, promo e come far partire un refresh senza UI (solo job/script e configurazione server).

---

## 1. Flusso end-to-end

### Full seed (Cloud Run Job / worker locale)

Ordine eseguito da `scripts/data_pipeline_worker.py` quando `DATA_JOB_TYPE=full_seed`:

1. **`scripts/generate_seed_data.py`**  
   Scrive `bigquery/dim_product_generated.sql` (INSERT `mart.dim_product` con ~`SEED_NUM_PRODUCTS` righe).

2. **`scripts/run_bigquery_schema.py`**  
   Legge `bigquery/schema_and_seed.sql`, applica override numerici (vedi §3), opzionalmente una **patch** da profilo compilato (vedi §4), poi esegue gli statement su BigQuery (dimensioni, fatti, pool prodotti, ecc.).

3. **`scripts/refresh_precalc_tables.py`**  
   Ricalcola le tabelle `mart.precalc_*` usate dalle dashboard.

### Precalc only

Solo il passo 3 (dati mart già presenti).

### Dopo il job

Il worker prova a svuotare la cache app (`clear_service_cache`) così le API non servono subito risposte vecchie.

---

## 2. Stato attuale (admin senza profilo da UI)

Le richieste admin **`POST /api/admin/data-jobs`** creano un job **senza** `profile_inline` e senza `SEED_PROFILE_JSON` sul RunJob.

Quindi il worker:

- **non** entra nel ramo `profile_version == 2` → **non** genera `SEED_COMPILED_PATH` né imposta `SEED_BRAND_FOCUS_JSON` / `SEED_BRAND_PROMO_AFFINITY_JSON` dal compiler;
- applica solo ciò che c’è in **variabili d’ambiente** già definite sul Job / sul `.env` del processo.

In pratica oggi contano soprattutto:

| Variabile | Ruolo | Default tipico |
|-----------|--------|----------------|
| `SEED_NUM_CUSTOMERS` | clienti in `dim_customer` / mod in `fact_orders` | `24000` (se non settata, `run_bigquery_schema` usa 24000 nel replace) |
| `SEED_NUM_ORDERS` | righe ordini | `380000` |
| `SEED_NUM_PRODUCTS` | righe in `generate_seed_data.py` | `1200` |
| `SEED_BRAND_FOCUS_JSON` | override mappa brand → categorie parent nel **catalogo** | vuoto → usa `BRAND_FOCUS` nel Python |
| `SEED_BRAND_PROMO_AFFINITY_JSON` | moltiplicatore sul mix **premium** per brand nel catalogo | vuoto → nessun effetto |
| `SEED_COMPILED_PATH` / `SEED_COMPILED_JSON` | patch SQL segmenti/promo/pool (vedi §4) | assenti → nessuna patch |

I replace esatti per ordini/clienti sono in `scripts/run_bigquery_schema.py` → `apply_seed_numeric_overrides()` (stringhe `24000` / `380000` nello schema).

---

## 3. Catalogo prodotti (`dim_product`) — pesi e mix

File: **`scripts/generate_seed_data.py`**

| Concetto | Dove | Effetto |
|----------|------|---------|
| **Quante SKU** | `SEED_NUM_PRODUCTS` (env) | numero righe generate |
| **Brand × categorie parent** | dict `BRAND_FOCUS` | quali coppie (brand, subcategoria) entrano nel round-robin delle SKU |
| **Override focus** | `SEED_BRAND_FOCUS_JSON` | merge sopra `BRAND_FOCUS` |
| **Prezzo / volatilità / share premium** | `SUBCAT_PRICE[subcat] = (avg_pln, spread_pct, premium_share)` | prezzo arrotondato, variante pseudo-casuale, probabilità `premium_flag` |
| **Brand “mass market”** | `MASS_BRANDS` | prezzo ~×0.7, premium share ×0.5 |
| **Brand specialist** | `SPECIALIST_BRANDS` | prezzo ~×1.2, premium share fino a ×1.3 (cap 0.9) |
| **Affinità promo (premium)** | `SEED_BRAND_PROMO_AFFINITY_JSON` | moltiplica `premium_share` (clamp 0.02–0.95) |

Le subcategorie ammesse per ogni categoria parent sono in `PARENT_TO_SUB`.

Riferimento rapido commentato anche in **`bigquery/seed_config.sql`** (brand/subcategory).

---

## 4. Seed mart (`schema_and_seed.sql`) — clienti, ordini, segmenti, promo

File principale: **`bigquery/schema_and_seed.sql`**

Contiene (tra l’altro):

- **`seg_behavior`**: per ogni segmento 1–6 — `promo_sens`, canali, `loyalty_prob`, `prem`, `inc`, `churn` (CTE usata in `dim_customer` e propagata agli ordini).
- **Assegnazione `customer_id` → segmento**: `CASE` su soglie derivate dal numero clienti (valori nel file SQL).
- **`seg_pref`**: preferenze categoria parent per segmento (pool prodotti / join sugli ordini).
- **`fact_orders`**: date, legame a cliente, logica promo (soglia legata a `promo_sens`, eventi, bias sconto per segmento, ecc.).

Valori di default “centralizzati” in codice per il **compiler** (non usati automaticamente dal job admin attuale) sono in **`scripts/seed_planner/defaults.py`**:

- `DEFAULT_SEG_PREF_ROWS` — coppie (segmento, parent_category_id)
- `DEFAULT_SEGMENT_BEHAVIOR` — stessi campi della CTE SQL
- `DEFAULT_PROMO_CURVE` — `slope` / `intercept` per la soglia promo
- `DEFAULT_DISCOUNT_BIAS` — punti percentuali aggiunti alla profondità sconto per segmento

### Patch da profilo compilato (opzionale, non dall’admin)

`run_bigquery_schema.py` chiama `seed_planner.sql_patch.load_compiled_from_env()`: se `SEED_COMPILED_PATH` o `SEED_COMPILED_JSON` puntano a un JSON prodotto da `compile_seed_profile()`, vengono riscritte parti di SQL (segment boundaries, `seg_behavior`, `seg_pref`, curve promo, ecc.).

Oggi questo si ottiene solo se **configuri tu** quell’env sul worker o lanci gli script a mano con compile; il pannello admin **non** lo imposta più.

---

## 5. Derivati e precalc

- **`bigquery/derive_sales_from_orders.sql`**: aggregati vendite / promo da fatti ordine.
- **`bigquery/precalc_tables.sql`** + **`scripts/refresh_precalc_tables.py`**: tabelle dashboard.

Modificare i pesi “a monte” (fatti + dimensioni) ha effetto sulle precalc al prossimo refresh.

---

## 6. Cosa indicare nel brainstorming (“voglio cambiare X”)

Per ogni obiettivo, il posto giusto è circa:

| Obiettivo | File / meccanismo |
|-----------|-------------------|
| Più/meno ordini, clienti | `SEED_NUM_ORDERS`, `SEED_NUM_CUSTOMERS` (env Job) + coerenza con replace in `run_bigquery_schema.py` |
| Più/meno SKU | `SEED_NUM_PRODUCTS` |
| Mix brand / categorie nel catalogo | `BRAND_FOCUS`, `SEED_BRAND_FOCUS_JSON`, eventualmente `PARENT_TO_SUB` |
| Prezzi e % premium per tipologia prodotto | `SUBCAT_PRICE`, `MASS_BRANDS` / `SPECIALIST_BRANDS`, `SEED_BRAND_PROMO_AFFINITY_JSON` |
| Quote segmenti, comportamento, preferenze categoria, soglia promo | `schema_and_seed.sql` (CTE `seg_behavior`, assegnazione segmenti, `seg_pref`, blocchi `fact_orders`) **oppure** pipeline compile + `SEED_COMPILED_*` |
| Promo meccaniche / uplift eventi | commenti e valori in `seed_config.sql` + SQL negli INSERT `fact_orders` / tabelle promo |

Quando hai una lista di cambiamenti concreti (es. “segmento 4 più sensibile al promo”, “meno premium su TV entry”), si possono tradurre in modifiche puntuali a questi file o in un profilo compilato + env sul job.

---

## 7. Comandi utili (locale)

Da root repo, con credenziali GCP e `PYTHONPATH` impostato:

```bash
python scripts/generate_seed_data.py
python scripts/run_bigquery_schema.py
python scripts/refresh_precalc_tables.py
```

Oppure la sequenza PowerShell/script già documentata in `AGENTS.md` (`reseed_full_pipeline.ps1`, ecc.).
