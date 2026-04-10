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

## 6. Riferimento numerico completo (per tweak e realismo)

Valori effettivi nel codice/SQL **alla data del documento**. Se modifichi un file, aggiorna anche questa sezione o verifica con grep.

### 6.1 Volumi e finestre temporali (default schema)

| Parametro | Valore | Dove |
|-----------|--------|------|
| Clienti | `24000` | `GENERATE_ARRAY(1, 24000)`, mod su `customer_id` negli ordini |
| Ordini | `380000` | `GENERATE_ARRAY(1, 380000)` |
| SKU generate | `1200` | env `SEED_NUM_PRODUCTS` in `generate_seed_data.py` (fallback default) |
| Range date ordini | `2023-01-01` + 0…1460 giorni | `MOD(..., 1461)` su `dim_date` |
| `product_id` seed | partono da `10001` | `generate_seed_data.py` |

Con `SEED_NUM_CUSTOMERS` / `SEED_NUM_ORDERS` diversi, `run_bigquery_schema.py` sostituisce le stringhe `24000` / `380000` nel testo SQL prima dell’esecuzione. Le **soglie segmento** nel `CASE` (3000, 6500, …) restano letterali finché non le cambi a mano o non usi `SEED_COMPILED_*`.

### 6.2 Comportamento segmento (`seg_behavior` in `schema_and_seed.sql`)

Nomi: 1 Liberals, 2 Optimistic Doers, 3 Go-Getters, 4 Outcasts, 5 Contributors, 6 Floaters.

Colonne: `promo_sens`, `ch_web`, `ch_app`, `ch_store`, `loyalty_prob`, `prem`, `inc`, `churn`.

| seg | promo_sens | ch_web | ch_app | ch_store | loyalty_prob | prem | inc | churn |
|-----|------------|--------|--------|----------|--------------|------|-----|-------|
| 1 | 0.35 | 1 | 1 | 0 | 0.65 | 0.5 | high | 0.12 |
| 2 | 0.42 | 1 | 1 | 0 | 0.55 | 0.7 | high | 0.10 |
| 3 | 0.28 | 1 | 1 | 1 | 0.70 | 0.75 | high | 0.05 |
| 4 | 0.58 | 1 | 1 | 0 | 0.25 | 0.2 | low | 0.28 |
| 5 | 0.48 | 1 | 0 | 1 | 0.80 | 0.35 | low | 0.08 |
| 6 | 0.52 | 0 | 0 | 1 | 0.45 | 0.25 | low | 0.15 |

Stessi numeri in `scripts/seed_planner/defaults.py` → `DEFAULT_SEGMENT_BEHAVIOR`.

### 6.3 Quote clienti → segmento (24k clienti, soglie `customer_id`)

| Segmento | Condizione su `customer_id` | Clienti (circa) |
|----------|-----------------------------|-----------------|
| 1 | ≤ 3000 | 3000 |
| 2 | ≤ 6500 | 3500 |
| 3 | ≤ 11800 | 5300 |
| 4 | ≤ 18800 | 7000 |
| 5 | ≤ 22200 | 3400 |
| 6 | > 22200 | 1800 |

Default boundaries compiler (stesso effetto): `[3000, 6500, 11800, 18800, 22200]` in `defaults.py` → `DEFAULT_SEG_BOUNDARIES`.

### 6.4 Curva promo e bias sconto (ordini)

**Soglia probabilità “ordine in promo”** (dentro `fact_orders`, duplicata come costanti nel `CASE` segmento):

- Formula base: `promo_sens × 0.58 + 0.17` (stesso che `DEFAULT_PROMO_CURVE`: slope `0.58`, intercept `0.17`).
- Poi: `ROUND(100 * …)` come intero, clamp tra **22** e **87** (`LEAST(87, GREATEST(22, …))`).
- **Uplift evento** (`peak_event` da `dim_date`): +**15** se Black Friday / Christmas / Cyber Monday / New Year Sales; +**10** se Back to School / Summer Sales / Winter Sales / Spring Cleaning.
- **Bias per cliente**: `MOD(..., 15) - 7` → intervallo **[-7, +7]** punti percentuali sulla soglia.

**Profondità sconto %** (`discount_depth_pct` se `promo_flag`):

- Base: `11.0 + MOD(ABS(FARM_FINGERPRINT(...)), 12)` → circa **11–22** prima del bias segmento.
- Bias segmento (punti % additivi): seg **4 → +4.0**, **5 → +3.0**, **6 → +3.5**, **1 → -1.5**, **2 → -1.0**, **3 → -0.5**.
- Clamp finale: `LEAST(28, GREATEST(6, …))` → tra **6%** e **28%**.

**Meccaniche promo** (`promo_mech`): `promo_id` 1…10 con `discount_depth_pct` rispettivamente **10, 20, 30, 15, 15, 12, 8, 18, 25, 20** (usate altrove / derive).

**Mapping evento → `promo_id` candidato** (se in promo): Black Friday → **9**; Christmas / New Year Sales → **10**; Back to School → **2**; Summer Sales → **6**; Tech Launch → **7**; altrimenti `1 + MOD(order_id, 10)`.

### 6.5 Altri numeri in `dim_customer` (pseudo-realismo)

| Effetto | Soglia / formula |
|---------|------------------|
| `is_registered` | `MOD(..., 100) < 78` → **78%** registrati |
| `has_app` | se canale app: `MOD < 72`; senza app obbligatorio: `MOD < 45` |
| `has_website_account` | `MOD < 75` |
| `customer_status` active | `MOD < (100 - churn×100)` usando `churn` del segmento |
| dormant / churned | ulteriori soglie su `MOD < 92` |
| `preferred_channel` | mix da `ch_web/ch_app/ch_store` + hash |
| `omnichannel_flag` | segmento 3 **oppure** `MOD(customer_id, 7)=0` |
| `has_loyalty_card` | `MOD < loyalty_prob×100` |
| tier gold / silver | `MOD(..., 10) < 2` gold, `<5` silver |
| `segment_confidence` | `0.72 + MOD(..., 28)/100` → circa **0.72–0.999** |
| `marketing_optin_email` | pari/dispari `customer_id` |
| `marketing_optin_push` | segmento in (3,4) **oppure** `MOD(...,3)=0` |
| `marketing_optin_sms` | `MOD(...,5)=0` |

Date sintetiche (giorni da ancore fisse): registrazione da `2020-01-01` + 0…1399; prima acquisto `2022-01-01` + 0…399; ultimo `2023-06-01` + 0…899; loyalty join `2021-01-01` + 0…1199; app first `2022-01-01` + `MOD(id,500)`; app last `2024-01-01` + `MOD(id,400)`.

### 6.6 `fact_orders`: importo e canale

| Campo | Formula |
|-------|---------|
| `gross_pln` | `ROUND(150 + MOD(...)/10.0, 2)` con MOD su 6000 → range grosso **150–750** PLN circa |
| `net_pln` | `gross_pln / 1.23` (IVA indicativa) |
| `units` | `1 + MOD(..., 4)` → **1–4** unità |
| Canale riga ordine | **78%** usa `preferred_channel` del cliente; altrimenti rotazione web/app/store su `MOD(...,3)` |

### 6.7 Preferenze categoria (`seg_pref`, `ch_pref`, `gender_pref`)

**`seg_pref`**: coppie `(segment_id, parent_category_id)` come nello SQL (identiche a `DEFAULT_SEG_PREF_ROWS` in `defaults.py`).

- Seg 1: **8, 9, 3, 2, 6, 1, 7**
- Seg 2: **1, 2, 7, 3, 6, 8, 5, 9**
- Seg 3: **3, 2, 7, 1, 6, 8, 4, 9**
- Seg 4: **2, 7, 4, 1, 8, 6, 3, 5**
- Seg 5: **5, 6, 1, 2, 9, 8**
- Seg 6: **5, 6, 1, 2, 9, 8, 7**

**`ch_pref`** (canale → categorie parent ammesse):

- store: **5, 1, 6, 2, 8**
- app: **2, 4, 7, 3, 8**
- web: **3, 4, 10, 2, 6, 8**

**`gender_pref`**:

- male: **3, 2, 1, 5, 7, 8**
- female: **6, 9, 5, 2, 1, 8**

### 6.8 Pool prodotti (`product_pool_seg_channel_gender`) — pesi impliciti

- Segmenti **4, 5, 6**: nel blocco base **esclusi** i prodotti `premium_flag` (entrano da UNION dedicate).
- Segmenti **1, 2, 3**: prodotti premium ripetuti **10** volte (`GENERATE_ARRAY(1, 10)`) → forte sovrappeso premium nel sort.
- Sottocategorie smartphone premium **201, 202, 204**: **12** duplicati per seg 1–3; stesse subcat **12** duplicati per seg 4–6 (blocco “flagship/foldable”).
- TV premium **102, 103, 105** (OLED/QLED/soundbar): **10** duplicati per seg 4–6.

### 6.9 `fact_order_items`: righe, pool, quantità, prezzo

| Elemento | Valore |
|----------|--------|
| Righe per ordine | `1 + MOD(order_id, 4)` → **1 a 4** righe |
| Uso pool | `MOD(h1, 100) < (82 + MOD(segment_id×7, 15))` → soglia tra **82** e **96** circa |
| Fallback SKU (seg 1–3) | `10001 + MOD(h1 + seg×17, 1200)` |
| Fallback SKU (seg 4–6) | scan prodotti **non premium** con `rn` da hash |
| Swap premium (seg 4–6) | se premium e subcat **non** in (201,202,204,102,103,105): probabilità **55%** (`MOD < 55`) sostituzione con non-premium |
| Quantità per `category_id` | cat **5** → 1; **6** → 1–2; **3** o **4** → 1–3; altri → 1–2 |
| Moltiplicatore quantità | premium ×**1.08** (seg 1–3), ×**0.90** (4–6); non-premium ×**1.52** (4–6), ×**0.80** (1–3) |
| `gross_pln` riga | `price_pln × quantity × (1 - discount/100 se promo) ×` fattore premium **1.22** (seg 1–3) o **0.62** (4–6) × jitter brand **0.68–1.22** × jitter evento **0.72–1.18** |

### 6.10 Catalogo Python — `BRAND_FOCUS` (brand_id → categorie parent 1–10)

Da `generate_seed_data.py` (ogni riga = categorie parent; le SKU girano su tutte le subcat di `PARENT_TO_SUB` per quel parent):

| id | parent cat | id | parent cat | id | parent cat |
|----|------------|----|------------|----|------------|
| 1 | 1,2,5 | 2 | 1,5 | 3 | 1,7,4,10 |
| 4 | 1,6,8,9 | 5 | 1 | 6 | 1 |
| 7 | 1 | 8 | 2,3,7 | 9 | 2,6,8 |
| 10 | 2 | 11 | 2 | 12 | 2 |
| 13 | 2 | 14 | 2 | 15 | 2 |
| 16 | 3 | 17 | 3 | 18 | 3 |
| 19 | 3,4 | 20 | 3 | 21 | 3,4 |
| 22 | 3,4 | 23 | 3,8 | 24 | 4 |
| 25 | 4 | 26 | 4 | 27 | 4 |
| 28 | 4 | 29 | 5,6 | 30 | 5 |
| 31 | 5 | 32 | 5 | 33 | 5 |
| 34 | 5 | 35 | 6 | 36 | 6,9 |
| 37 | 6 | 38 | 6 | 39 | 8 |
| 40 | 8 | 41 | 8 | 42 | 7 |
| 43 | 7 | 44 | 7 | 45 | 7 |
| 46 | 7 | 47 | 9 | 48 | 9 |
| 49 | 10 | 50 | 10 | 51 | 10 |
| 52 | 10 | 53 | 9 | 54 | 9 |
| 55 | 2 | | | | |

**Brand “mass”** (prezzo ×**0.7**, premium share ×**0.5**): **9, 32, 34** (Xiaomi, Beko, Amica).

**Brand “specialist”** (prezzo ×**1.2**, premium share ×**1.3** cap 0.9): **36, 42, 49, 50, 51, 52** (Dyson, Bose, Canon, Nikon, GoPro, DJI).

### 6.11 `SUBCAT_PRICE`: (prezzo medio PLN, spread, share premium)

`spread` = fattore ± sul prezzo; `premium_share` = probabilità `premium_flag` prima dei moltiplicatori brand/env.

| subcat | avg | spread | prem | subcat | avg | spread | prem |
|--------|-----|--------|------|--------|-----|--------|------|
| 101 | 3500 | 0.35 | 0.4 | 102 | 6500 | 0.4 | 0.8 |
| 103 | 5000 | 0.35 | 0.7 | 104 | 4500 | 0.3 | 0.6 |
| 105 | 1200 | 0.5 | 0.3 | 106 | 2500 | 0.5 | 0.5 |
| 107 | 3500 | 0.5 | 0.6 | 108 | 350 | 0.6 | 0.2 |
| 201 | 4500 | 0.4 | 0.7 | 202 | 2200 | 0.45 | 0.3 |
| 203 | 900 | 0.5 | 0.1 | 204 | 6000 | 0.3 | 0.9 |
| 205 | 2800 | 0.4 | 0.5 | 206 | 1200 | 0.5 | 0.4 |
| 207 | 450 | 0.5 | 0.2 | 208 | 150 | 0.7 | 0.1 |
| 301 | 4500 | 0.5 | 0.4 | 302 | 6500 | 0.4 | 0.7 |
| 303 | 5000 | 0.5 | 0.5 | 304 | 1500 | 0.5 | 0.4 |
| 305 | 350 | 0.6 | 0.3 | 306 | 200 | 0.6 | 0.2 |
| 307 | 400 | 0.5 | 0.3 | 308 | 350 | 0.6 | 0.2 |
| 309 | 250 | 0.6 | 0.2 | 310 | 450 | 0.5 | 0.3 |
| 401 | 2200 | 0.4 | 0.6 | 402 | 5500 | 0.45 | 0.7 |
| 403 | 6000 | 0.4 | 0.7 | 404 | 350 | 0.5 | 0.3 |
| 405 | 450 | 0.5 | 0.3 | 406 | 400 | 0.5 | 0.4 |
| 407 | 350 | 0.5 | 0.4 | 408 | 2500 | 0.5 | 0.6 |
| 501 | 4500 | 0.5 | 0.5 | 502 | 3500 | 0.45 | 0.5 |
| 503 | 3000 | 0.5 | 0.4 | 504 | 2800 | 0.45 | 0.5 |
| 505 | 2500 | 0.5 | 0.4 | 506 | 1800 | 0.5 | 0.4 |
| 507 | 3500 | 0.5 | 0.5 | 508 | 1500 | 0.5 | 0.3 |
| 601 | 1200 | 0.5 | 0.4 | 602 | 350 | 0.6 | 0.2 |
| 603 | 550 | 0.5 | 0.3 | 604 | 1200 | 0.5 | 0.4 |
| 605 | 2200 | 0.45 | 0.5 | 606 | 450 | 0.5 | 0.2 |
| 607 | 180 | 0.5 | 0.1 | 608 | 220 | 0.5 | 0.2 |
| 701 | 800 | 0.5 | 0.4 | 702 | 1500 | 0.45 | 0.6 |
| 703 | 450 | 0.5 | 0.3 | 704 | 400 | 0.5 | 0.3 |
| 705 | 2500 | 0.5 | 0.6 | 706 | 1500 | 0.6 | 0.5 |
| 801 | 450 | 0.5 | 0.2 | 802 | 250 | 0.6 | 0.2 |
| 803 | 350 | 0.5 | 0.3 | 804 | 450 | 0.5 | 0.3 |
| 805 | 650 | 0.5 | 0.4 | 806 | 120 | 0.6 | 0.1 |
| 901 | 350 | 0.5 | 0.3 | 902 | 280 | 0.5 | 0.2 |
| 903 | 220 | 0.5 | 0.2 | 904 | 180 | 0.5 | 0.2 |
| 905 | 350 | 0.5 | 0.3 | 1001 | 3500 | 0.5 | 0.5 |
| 1002 | 9000 | 0.45 | 0.8 | 1003 | 2500 | 0.6 | 0.5 |
| 1004 | 1800 | 0.5 | 0.5 | 1005 | 3500 | 0.5 | 0.6 |

Prezzo finale: `var = 1 + spread × ((i×17+31) % 100 - 50)/100`, poi `max(99, int(avg×var/50)×50)`. `premium_flag`: `(i×13+7) % 100 < int(prem_share×100)`.

### 6.12 Env opzionali sul catalogo

| Env | Effetto |
|-----|---------|
| `SEED_BRAND_FOCUS_JSON` | merge chiavi → liste di parent category (override parziale di `BRAND_FOCUS`) |
| `SEED_BRAND_PROMO_AFFINITY_JSON` | `brand_id → float`; moltiplica `premium_share` (clamp 0.02–0.95) |

---

## 7. Cosa indicare nel brainstorming (“voglio cambiare X”)

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

## 8. Comandi utili (locale)

Da root repo, con credenziali GCP e `PYTHONPATH` impostato:

```bash
python scripts/generate_seed_data.py
python scripts/run_bigquery_schema.py
python scripts/refresh_precalc_tables.py
```

Oppure la sequenza PowerShell/script già documentata in `AGENTS.md` (`reseed_full_pipeline.ps1`, ecc.).
