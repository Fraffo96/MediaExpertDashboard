# Specifica campi – Database Media Expert Dashboard

> Riferimento per `scripts/generate_seed_data.py` e per modifiche allo schema.  
> **Fonte:** `bigquery/schema_and_seed.sql`, `derive_sales_from_orders.sql`, `dim_product_generated.sql`

**Quando usare:** rigenerare prodotti, verificare ID/range, modificare campi tabelle. Per schema logico e relazioni vedi `DATABASE_SCHEMA.md`.

---

## Matrice segmento × categoria (coerenza vendite e needstates)

Fonte di verità narrativa tra **`product_pool_seg_channel_gender.seg_pref`** in [`bigquery/schema_and_seed.sql`](../bigquery/schema_and_seed.sql) e i profili in [`app/static/data/needstates_hcg.json`](../app/static/data/needstates_hcg.json). Ogni segmento (1–6) deve avere **affinità non nulla** alle macro (1–10): mix premium/value e propensione promo derivano da `seg_behavior.promo_sens`, non da scarti binari tra gruppi.

| Segment | Focus categoria (parent 1–10) | Premium vs value (seed) | Promo (`promo_sens`) |
|---------|-------------------------------|-------------------------|----------------------|
| 1 Liberals | Smart home, health tech, IT, mobile, small CE — anche TV/audio | Alto premium | 0.35 |
| 2 Optimistic Doers | TV, mobile, audio, IT, small CE, smart home | Alto premium | 0.42 |
| 3 Go-Getters | IT, mobile, audio, TV, small CE, smart home | Alto premium, omnichannel | 0.28 |
| 4 Outcasts | Mobile, audio, gaming, TV, small CE — **anche** IT e large app | Medio (non zero premium) | 0.58 |
| 5 Contributors | Large/small appliances, TV, mobile, health, smart home | Medio/value bias | 0.48 |
| 6 Floaters | Large/small appliances, TV, mobile, health, smart home — **anche** audio | Medio/value bias | 0.52 |

**Needstates:** per ogni `categories["k"]` in JSON, gli score per indice segmento 0..5 devono riflettere la riga sopra (es. dimensioni “value/deal” più alte su 4–6 dove il pool ha più mass-market; “premium/status” più alte su 1–3 senza azzerare gli altri).

**Validazione BQ (post-seed):** share `gross_pln` per `(segment_id, parent_category_id)` non deve essere zero per combinazioni marcate in `seg_pref`; SKU top per `COUNT(DISTINCT segment_id)` idealmente ≥ 4 dove il prodotto non è specialist-only.

---

## Pipeline: rigenerazione completa dati + precalc + cache

Eseguire **in ordine** dopo modifiche a `schema_and_seed.sql` / JSON needstates:

1. (Opzionale) `python scripts/generate_seed_data.py` se cambia `dim_product`.
2. `gcloud auth application-default login` se necessario.
3. `python scripts/run_bigquery_schema.py` — carica seed + fase derive (`derive_sales_from_orders.sql`).
4. `python scripts/refresh_precalc_tables.py` — ricalcola tutte le `precalc_*`.
5. **Cache applicativa:** `POST /api/admin/clear-cache` (utente admin) oppure restart Cloud Run / svuota Redis; oppure `GET /internal/prewarm` (token) per ricaldo.

Su Windows, vedi anche [`scripts/reseed_full_pipeline.ps1`](../scripts/reseed_full_pipeline.ps1).

---

## Elenco valori (nomi completi)

### Brand (55)

| brand_id | brand_name | brand_country | brand_category_focus |
|----------|-------------|---------------|----------------------|
| 1 | Samsung | KR | TV, Large Appliances, Smartphones |
| 2 | LG | KR | TV, Large Appliances |
| 3 | Sony | JP | TV, Audio, Gaming, Photo |
| 4 | Philips | NL | TV, Small Appliances, Smart Home, Health |
| 5 | TCL | CN | TV |
| 6 | Hisense | CN | TV |
| 7 | Panasonic | JP | TV |
| 8 | Apple | US | Smartphones, Computers, Audio |
| 9 | Xiaomi | CN | Smartphones, Small Appliances, Smart Home |
| 10 | Oppo | CN | Smartphones |
| 11 | Realme | CN | Smartphones |
| 12 | Huawei | CN | Smartphones |
| 13 | Motorola | US | Smartphones |
| 14 | OnePlus | CN | Smartphones |
| 15 | Garmin | CH | Smartphones & wearables |
| 16 | Dell | US | Computers |
| 17 | HP | US | Computers |
| 18 | Lenovo | CN | Computers |
| 19 | Asus | TW | Computers, Gaming |
| 20 | Acer | TW | Computers |
| 21 | MSI | TW | Computers, Gaming |
| 22 | Logitech | CH | Computers, Gaming |
| 23 | TP-Link | CN | Computers, Smart Home |
| 24 | Microsoft | US | Gaming |
| 25 | Nintendo | JP | Gaming |
| 26 | Razer | US | Gaming |
| 27 | SteelSeries | DK | Gaming |
| 28 | HyperX | US | Gaming |
| 29 | Bosch | DE | Large Appliances, Small Appliances |
| 30 | Siemens | DE | Large Appliances |
| 31 | Whirlpool | US | Large Appliances |
| 32 | Beko | TR | Large Appliances |
| 33 | Electrolux | SE | Large Appliances |
| 34 | Amica | PL | Large Appliances |
| 35 | Tefal | FR | Small Appliances |
| 36 | Dyson | UK | Small Appliances, Health |
| 37 | DeLonghi | IT | Small Appliances |
| 38 | Krups | DE | Small Appliances |
| 39 | Google | US | Smart Home |
| 40 | Amazon | US | Smart Home |
| 41 | Ring | US | Smart Home |
| 42 | Bose | US | Audio |
| 43 | JBL | US | Audio |
| 44 | Marshall | UK | Audio |
| 45 | Beats | US | Audio |
| 46 | Sennheiser | DE | Audio |
| 47 | Braun | DE | Health & Beauty |
| 48 | Oral-B | US | Health & Beauty |
| 49 | Canon | JP | Photo & Video |
| 50 | Nikon | JP | Photo & Video |
| 51 | GoPro | US | Photo & Video |
| 52 | DJI | CN | Photo & Video |
| 53 | Remington | US | Health & Beauty |
| 54 | Withings | FR | Health |
| 55 | Fitbit | US | Smartphones & wearables |

---

### Categorie parent (10)

| category_id | category_name |
|-------------|---------------|
| 1 | TV & Home Entertainment |
| 2 | Mobile e smartwatches |
| 3 | Computers & IT |
| 4 | Gaming |
| 5 | Large Appliances |
| 6 | Small Appliances |
| 7 | Audio |
| 8 | Smart Home |
| 9 | Health & Beauty Tech |
| 10 | Photo & Video |

---

### Sottocategorie (72)

| category_id | category_name | parent_category_id |
|-------------|---------------|--------------------|
| 101 | LED TV | 1 |
| 102 | OLED TV | 1 |
| 103 | QLED TV | 1 |
| 104 | Mini LED TV | 1 |
| 105 | Soundbars | 1 |
| 106 | Home cinema systems | 1 |
| 107 | Projectors | 1 |
| 108 | Streaming devices | 1 |
| 201 | Smartphones flagship | 2 |
| 202 | Smartphones mid-range | 2 |
| 203 | Smartphones entry | 2 |
| 204 | Foldable smartphones | 2 |
| 205 | Tablets | 2 |
| 206 | Smartwatches | 2 |
| 207 | Fitness trackers | 2 |
| 208 | Phone accessories | 2 |
| 301 | Laptops | 3 |
| 302 | Gaming laptops | 3 |
| 303 | Desktop PCs | 3 |
| 304 | Monitors | 3 |
| 305 | Keyboards | 3 |
| 306 | Mice | 3 |
| 307 | Webcams | 3 |
| 308 | External storage | 3 |
| 309 | Routers | 3 |
| 310 | Mesh WiFi systems | 3 |
| 401 | Consoles | 4 |
| 402 | Gaming PCs | 4 |
| 403 | Gaming laptops | 4 |
| 404 | Controllers | 4 |
| 405 | Gaming headsets | 4 |
| 406 | Gaming keyboards | 4 |
| 407 | Gaming mice | 4 |
| 408 | VR headsets | 4 |
| 501 | Refrigerators | 5 |
| 502 | Washing machines | 5 |
| 503 | Dryers | 5 |
| 504 | Dishwashers | 5 |
| 505 | Ovens | 5 |
| 506 | Induction hobs | 5 |
| 507 | Built-in appliances | 5 |
| 508 | Freezers | 5 |
| 601 | Coffee machines | 6 |
| 602 | Blenders | 6 |
| 603 | Air fryers | 6 |
| 604 | Vacuum cleaners | 6 |
| 605 | Robot vacuums | 6 |
| 606 | Kitchen processors | 6 |
| 607 | Electric kettles | 6 |
| 608 | Toasters | 6 |
| 701 | Wireless headphones | 7 |
| 702 | Noise cancelling headphones | 7 |
| 703 | Earbuds | 7 |
| 704 | Portable speakers | 7 |
| 705 | Hi-Fi systems | 7 |
| 706 | DJ equipment | 7 |
| 801 | Smart speakers | 8 |
| 802 | Smart lighting | 8 |
| 803 | Smart thermostats | 8 |
| 804 | Security cameras | 8 |
| 805 | Smart locks | 8 |
| 806 | Smart plugs | 8 |
| 901 | Electric toothbrushes | 9 |
| 902 | Hair dryers | 9 |
| 903 | Hair straighteners | 9 |
| 904 | Grooming kits | 9 |
| 905 | Smart scales | 9 |
| 1001 | Cameras | 10 |
| 1002 | Mirrorless cameras | 10 |
| 1003 | Lenses | 10 |
| 1004 | Action cameras | 10 |
| 1005 | Drones | 10 |

---

### Prodotti (1200)

Generati da `python scripts/generate_seed_data.py` → `bigquery/dim_product_generated.sql`.  
Range: **product_id 10001–11200**. Coerenti con brand_category_focus, subcategory, SUBCAT_PRICE (avg, spread, premium_share).  
Brand specialist (Dyson, Bose, Canon, Nikon, GoPro, DJI) e mass (Xiaomi, Beko, Amica) con profili prezzo distinti.

---

### Segmenti HCG (6)

| segment_id | segment_name | age_range | income_level | gender_skew | top_driver |
|------------|--------------|-----------|--------------|-------------|------------|
| 1 | Liberals | 45-64 | high | 57% male | wellness |
| 2 | Balancers | 35-54 | high | balanced | status |
| 3 | Go-Getters | 25-44 | very_high | balanced | performance |
| 4 | Outcasts | 18-24 | low | 58% male | entertainment |
| 5 | Contributors | 45-54 | low | 70% female | family |
| 6 | Floaters | 45-54 | low | balanced | necessity |

---

### Promozioni (10)

| promo_id | promo_name | promo_type | funding_type |
|----------|------------|------------|--------------|
| 1 | Rabat -10% | percentage_discount | retailer |
| 2 | Rabat -20% | percentage_discount | retailer |
| 3 | Rabat -30% | percentage_discount | joint |
| 4 | 2+1 Gratis | bundle | brand |
| 5 | Cashback 15% | cashback | brand |
| 6 | Hit Dnia | flash_sale | retailer |
| 7 | Tylko w App | app_only | retailer |
| 8 | Drugi za 1 PLN | bundle | joint |
| 9 | Black Friday | seasonal | retailer |
| 10 | Swieta | seasonal | retailer |

---

### Peak events (dim_date)

Cyber Monday, Singles Day, Black Friday, Christmas, New Year Sales, Valentines Day, Winter Sales, Easter, Spring Cleaning, May Holiday, Summer Sales, Back to School, Tech Launch, Regular

---

### Altri valori enumerati

| Campo | Valori |
|-------|--------|
| channel | web, app, store |
| gender (fact_sales_daily, v_sales) | M, F, other |
| gender (dim_customer) | male, female, unknown |
| customer_status | active, dormant, churned |
| loyalty_tier | none, basic, silver, gold |
| age_band | 18-24, 25-34, 35-44, 45-54, 55-64 |
| region | Mazowieckie, Malopolskie, Slaskie, Wielkopolskie, Other |
| urbanicity | urban, suburban, rural |
| income_band | low, mid, high |

---

## File: `bigquery/schema_and_seed.sql`

### Tabella `mart.dim_brand`

| Campo | Tipo | Descrizione |
|-------|------|-------------|
| brand_id | INT64 NOT NULL | PK |
| brand_name | STRING NOT NULL | |
| brand_country | STRING | Codice paese (KR, US, CN, ecc.) |
| brand_category_focus | STRING | Categorie focus (es. "TV, Large Appliances") |

---

### Tabella `mart.dim_category`

| Campo | Tipo | Descrizione |
|-------|------|-------------|
| category_id | INT64 NOT NULL | PK. Parent: 1–10. Sub: 101–108, 201–208, …, 1001–1005 |
| category_name | STRING NOT NULL | |
| level | INT64 NOT NULL | 1=parent, 2=subcategory |
| parent_category_id | INT64 | NULL per parent, 1–10 per sub |
| category_path | STRING | Es. "TV & Home Entertainment > LED TV" |

---

### Tabella `mart.dim_product`

| Campo | Tipo | Descrizione |
|-------|------|-------------|
| product_id | INT64 NOT NULL | PK. Range 10001+ |
| product_name | STRING NOT NULL | |
| brand_id | INT64 NOT NULL | FK → dim_brand |
| category_id | INT64 NOT NULL | Parent category (1–10) |
| subcategory_id | INT64 NOT NULL | Subcategory (101–1005) |
| price_pln | NUMERIC(10,2) NOT NULL | |
| launch_year | INT64 | |
| premium_flag | BOOL NOT NULL | |

---

### Tabella `mart.dim_date`

| Campo | Tipo | Descrizione |
|-------|------|-------------|
| date_key | STRING | Formato YYYYMMDD |
| date | DATE | |
| week | INT64 | Settimana anno |
| month | INT64 | 1–12 |
| quarter | INT64 | 1–4 |
| year | INT64 | 2023–2025 |
| day_of_week | INT64 | 1–7 |
| is_black_friday_week | BOOL | Nov 22–30 |
| is_xmas_period | BOOL | Dicembre |
| is_back_to_school | BOOL | Ago–15 Set |
| peak_event | STRING | 'Black Friday', 'Christmas', 'Back to School', 'Regular', ecc. |

---

### Tabella `mart.dim_promo`

| Campo | Tipo | Descrizione |
|-------|------|-------------|
| promo_id | INT64 NOT NULL | PK. 1–10 |
| promo_name | STRING NOT NULL | Es. "Rabat -10%" |
| promo_type | STRING NOT NULL | percentage_discount, bundle, flash_sale, seasonal, ecc. |
| promo_mechanic | STRING | |
| funding_type | STRING | retailer, brand, joint |
| start_date | DATE | |
| end_date | DATE | |

---

### Tabella `mart.dim_segment`

| Campo | Tipo | Descrizione |
|-------|------|-------------|
| segment_id | INT64 NOT NULL | PK. 1–6 |
| segment_name | STRING NOT NULL | Liberals, Balancers, Go-Getters, Outcasts, Contributors, Floaters |
| segment_description | STRING | |
| age_range | STRING | Es. "45-64" |
| income_level | STRING | high, low, very_high |
| gender_skew | STRING | Es. "57% male", "70% female" |
| top_driver | STRING | wellness, status, performance, entertainment, family, necessity |

---

### Tabella `mart.dim_customer`

Generazione coerente con segmento (segment_behavior_profile): preferred_channel, has_app, loyalty, churn, income_band, gender, age_band derivati da segment_id.

| Campo | Tipo | Descrizione |
|-------|------|-------------|
| customer_id | INT64 NOT NULL | PK. 1–12000 |
| global_user_id | STRING | |
| is_registered | BOOL NOT NULL | |
| registration_date | DATE | |
| first_purchase_date | DATE | |
| last_purchase_date | DATE | |
| customer_status | STRING NOT NULL | active, dormant, churned |
| has_app | BOOL NOT NULL | |
| app_first_seen_date | DATE | |
| app_last_seen_date | DATE | |
| has_website_account | BOOL NOT NULL | |
| preferred_channel | STRING NOT NULL | web, app, store |
| omnichannel_flag | BOOL NOT NULL | |
| has_loyalty_card | BOOL NOT NULL | |
| loyalty_tier | STRING NOT NULL | none, basic, silver, gold |
| loyalty_join_date | DATE | |
| gender | STRING NOT NULL | male, female, unknown |
| age_band | STRING | 18-24, 25-34, 35-44, 45-54, 55-64 |
| birth_year | INT64 | |
| city | STRING | |
| region | STRING | Mazowieckie, Malopolskie, Slaskie, ecc. |
| urbanicity | STRING | urban, suburban, rural |
| income_band | STRING | low, mid, high |
| segment_id | INT64 NOT NULL | FK → dim_segment |
| segment_confidence | FLOAT64 | 0–1 |
| marketing_optin_email | BOOL | |
| marketing_optin_push | BOOL | |
| marketing_optin_sms | BOOL | |

---

### Tabella `mart.fact_sales_daily`

| Campo | Tipo | Descrizione |
|-------|------|-------------|
| date | DATE NOT NULL | |
| brand_id | INT64 NOT NULL | FK → dim_brand |
| brand_name | STRING NOT NULL | Denormalizzato |
| category_id | INT64 NOT NULL | Subcategory (101–1005) |
| parent_category_id | INT64 NOT NULL | Parent (1–10) |
| segment_id | INT64 NOT NULL | FK → dim_segment |
| gender | STRING NOT NULL | M, F, other |
| gross_pln | NUMERIC(14,2) NOT NULL | |
| net_pln | NUMERIC(14,2) NOT NULL | gross/1.23 |
| units | INT64 NOT NULL | |
| promo_flag | BOOL NOT NULL | |
| promo_id | INT64 | FK → dim_promo, NULL se non promo |
| discount_depth_pct | NUMERIC(5,1) | % sconto, NULL se non promo |

---

### Tabella `mart.fact_promo_performance`

| Campo | Tipo | Descrizione |
|-------|------|-------------|
| promo_id | INT64 NOT NULL | FK → dim_promo |
| brand_id | INT64 NOT NULL | FK → dim_brand |
| brand_name | STRING NOT NULL | Denormalizzato |
| category_id | INT64 NOT NULL | Parent category (1–10) |
| date | DATE NOT NULL | |
| attributed_sales_pln | NUMERIC(14,2) NOT NULL | |
| incremental_sales_pln | NUMERIC(14,2) NOT NULL | |
| discount_cost_pln | NUMERIC(14,2) NOT NULL | |
| media_cost_pln | NUMERIC(14,2) NOT NULL | |
| roi | NUMERIC(10,4) | ROI = (attr - disc - media) / (disc + media) |

---

### Tabella `mart.fact_orders`

| Campo | Tipo | Descrizione |
|-------|------|-------------|
| order_id | INT64 NOT NULL | PK. 1–380000 |
| date | DATE NOT NULL | |
| customer_id | INT64 NOT NULL | FK → dim_customer |
| channel | STRING NOT NULL | web, app, store |
| gross_pln | NUMERIC(14,2) NOT NULL | |
| net_pln | NUMERIC(14,2) NOT NULL | gross/1.23 |
| units | INT64 NOT NULL | |
| promo_flag | BOOL NOT NULL | |
| promo_id | INT64 | FK → dim_promo, NULL se non promo |
| discount_depth_pct | NUMERIC(5,1) | NULL se non promo |

---

### Tabella `mart.fact_order_items`

| Campo | Tipo | Descrizione |
|-------|------|-------------|
| order_id | INT64 NOT NULL | FK → fact_orders |
| product_id | INT64 NOT NULL | FK → dim_product |
| quantity | INT64 NOT NULL | |
| gross_pln | NUMERIC(14,2) NOT NULL | |

---

## fact_sales_daily e fact_promo_performance

**Non più nel seed.** Derivati esclusivamente da `bigquery/derive_sales_from_orders.sql`:
- `v_sales_daily_by_channel` → aggregazione senza channel → `fact_sales_daily`
- `fact_promo_performance`: incremental_sales_pln = attributed - baseline (media non-promo ultimi 28 gg per brand+parent_category)

---

## CTE usate in `INSERT mart.fact_orders` (schema_and_seed.sql)

### CTE `promo_mech`, `order_dates`, `order_cust`, `order_peak`, `gen`

- **channel**: da `dim_customer.preferred_channel` (78% preferito)
- **promo_flag**: da `dim_date.peak_event` (Black Friday/Christmas 55%) + segment (Outcasts 52%, Go-Getters 22%)
- **promo_id**: da peak_event (Black Friday→9, Christmas→10, Back to School→2, Summer Sales→6, Tech Launch→7)
- **discount_depth_pct**: da `promo_mechanic_profile` (10, 20, 30, … % per promo_id)

---

## CTE usate in `INSERT mart.fact_order_items` (schema_and_seed.sql)

### CTE `lines`, `with_price`

- **product_id**: 10001–11200 (da dim_product)
- **quantity**: 1 per Large Appliances (cat 5) → 1–2 per Small (6) → 1–3 per Computers/Gaming (3,4)
- **gross_pln**: `price_pln * qty * (1 - discount_depth_pct/100)` se promo, altrimenti prezzo pieno

---

## File: `bigquery/derive_sales_from_orders.sql`

### Vista `mart.v_sales_daily_by_channel`

| Campo | Tipo | Descrizione |
|-------|------|-------------|
| date | DATE | |
| brand_id | INT64 | |
| brand_name | STRING | |
| category_id | INT64 | Subcategory (da dim_product.subcategory_id) |
| parent_category_id | INT64 | Parent (da dim_product.category_id) |
| segment_id | INT64 | |
| gender | STRING | M, F, other |
| channel | STRING | web, app, store |
| gross_pln | NUMERIC | |
| net_pln | NUMERIC | |
| units | INT64 | |
| promo_flag | BOOL | |
| promo_id | INT64 | |
| discount_depth_pct | NUMERIC | |

**Nota:** derivata da fact_orders + fact_order_items + dim_product + dim_customer.  
`dim_product.category_id` = parent, `dim_product.subcategory_id` = subcategory.  
Nella vista: `category_id` = subcategory, `parent_category_id` = parent.

---

### Tabella `mart.fact_sales_daily` (ricreata da derive)

Aggregata da `v_sales_daily_by_channel` senza channel. Fonte unica: fact_orders + fact_order_items.

---

### Tabella `mart.fact_promo_performance` (ricreata da derive)

**incremental_sales_pln** = attributed - baseline (media vendite non-promo ultimi 28 giorni per brand+parent_category). CTE: `non_promo_daily`, `baseline`, `pcfg`, `yadj`.

---

## Riepilogo ID

| Entità | Range |
|--------|-------|
| brand_id | 1–55 |
| category_id (parent) | 1–10 |
| category_id (sub) | 101–108, 201–208, 301–310, 401–408, 501–508, 601–608, 701–706, 801–806, 901–905, 1001–1005 |
| product_id | 10001+ |
| promo_id | 1–10 |
| segment_id | 1–6 |
| customer_id | 1–12000 |
| order_id | 1–380000 |
