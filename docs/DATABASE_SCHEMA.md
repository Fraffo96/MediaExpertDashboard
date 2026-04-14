# Media Expert Dashboard – Database Schema

> **Progetto GCP:** `mediaexpertdashboard` | **Dataset:** `mart` | **Periodo:** 2023–2025 | **Seed:** `bigquery/schema_and_seed.sql`

## Star Schema

```
                         ┌────────────────┐
                         │   dim_date     │
                         │   1,096 righe  │
                         └───────┬────────┘
                                 │ date
  ┌──────────────┐               │              ┌──────────────────┐
  │  dim_brand   │               │              │  dim_category    │
  │  59 righe    │               │              │  82 righe        │
  └──────┬───────┘               │              │  (10 parent +   │
         │ brand_id              │              │   72 subcategory)│
         │                       │              └───────┬──────────┘
         │                       │                      │ category_id
  ┌──────┴───────────────────────┴──────────────────────┴─────────┐
  │                    fact_sales_daily                             │
  │              (derivata da ordini)                               │
  │  (date × brand × subcategory × segment, con parent_category)  │
  └───────┬──────────────────┬──────────────────┬─────────────────┘
          │ segment_id       │ promo_id         │
  ┌───────┴────────┐  ┌─────┴──────────┐       │
  │  dim_segment   │  │   dim_promo    │       │
  │  6 righe (HCG) │  │   10 righe     │       │
  └────────────────┘  └─────┬──────────┘       │ brand_id, parent_category_id
                            │                   │
                     ┌──────┴───────────────────┴──────────────┐
                     │       fact_promo_performance              │
                     │       (derivata da derive)                │
                     │  (ROI e costi per promo/brand/category)   │
                     └──────────────────────────────────────────┘

  ┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
  │  dim_customer   │     │   fact_orders    │     │ fact_order_items │
  │  (buyers)       │────▶│  date × customer │────▶│  order × product │
  │  12.000 righe   │     │  ~380.000 righe  │     │  ~1M+ righe      │
  └────────┬────────┘     └────────┬────────┘     └────────┬────────┘
           │ segment_id            │ order_id              │ product_id
           ▼                       │ channel, promo       │ quantity, gross_pln
  ┌─────────────────┐              └──────────────────────┘
  │  dim_segment    │   Buyer analytics: loyalty, canale, coorti, AOV, repeat rate
  └─────────────────┘
```

---

## Tabelle Dimensione

### dim_brand — 59 brand

Colonne: `brand_id`, `brand_name`, `brand_country`, `brand_category_focus`.  
Samsung, LG, Sony, Philips, TCL, Hisense, Panasonic, Apple, Xiaomi, Oppo, Realme, Huawei, Motorola, OnePlus, Garmin, Dell, HP, Lenovo, Asus, Acer, MSI, Logitech, TP-Link, Microsoft, Nintendo, Razer, SteelSeries, HyperX, Bosch, Siemens, Whirlpool, Beko, Electrolux, Amica, Tefal, Dyson, DeLonghi, Krups, Google, Amazon, Ring, Bose, JBL, Marshall, Beats, Sennheiser, Braun, Oral-B, Canon, Nikon, GoPro, DJI, Remington, Withings, Fitbit, Honor, Vivo, Nothing, POCO.

Non tutti i brand vendono in tutte le categorie: combinazioni brand-categoria realistiche per le 10 parent.

### dim_category — 10 parent + 72 sottocategorie

| ID | Parent Category | # Sub | Note |
|---:|-----------------|------:|------|
| 1 | TV & Home Entertainment | 8 | LED, OLED, QLED, Mini LED TV, Soundbars, Home cinema, Projectors, Streaming |
| 2 | Mobile e smartwatches | 8 | Flagship, Mid-range, Entry, Foldable, Tablets, Smartwatches, Fitness trackers, Accessories |
| 3 | Computers & IT | 10 | Laptops, Gaming laptops, Desktops, Monitors, Keyboards, Mice, Webcams, Storage, Routers, Mesh WiFi |
| 4 | Gaming | 8 | Consoles, Gaming PCs, Gaming laptops, Controllers, Headsets, Keyboards, Mice, VR |
| 5 | Large Appliances | 8 | Refrigerators, Washing, Dryers, Dishwashers, Ovens, Induction, Built-in, Freezers |
| 6 | Small Appliances | 8 | Coffee, Blenders, Air fryers, Vacuums, Robot vacuums, Processors, Kettles, Toasters |
| 7 | Audio | 6 | Wireless HP, Noise cancelling, Earbuds, Portable speakers, Hi-Fi, DJ |
| 8 | Smart Home | 6 | Smart speakers, Lighting, Thermostats, Security cameras, Locks, Plugs |
| 9 | Health & Beauty Tech | 5 | Electric toothbrushes, Hair dryers, Hair straighteners, Grooming kits, Smart scales |
| 10 | Photo & Video | 5 | Cameras, Mirrorless, Lenses, Action cameras, Drones |

Schema: `category_id, category_name, level (1=parent, 2=sub), parent_category_id, category_path`

ID scheme: parent 1–10; sub 101–108, 201–208, …, 901–905, 1001–1005.

### dim_product — catalogo prodotti (1200)

Colonne: `product_id`, `product_name`, `brand_id`, `category_id`, `subcategory_id`, `price_pln`, `launch_year`, `premium_flag`.  
Generati da `python scripts/generate_seed_data.py` → `bigquery/dim_product_generated.sql`. Range 10001–11200. Coerenti con brand_category_focus e subcategory.

### dim_segment — 6 segmenti HCG (MediaWorld)

| ID | Nome | Età | Reddito | Top Categories | Brand Affinity |
|---:|------|-----|---------|---------------|----------------|
| 1 | **Liberals** | 45-64 | high | Smart Home, Health, Computers | Apple, Philips, Dyson, Garmin |
| 2 | **Balancers** | 35-54 | high | Smartphones, TV premium, Audio | Apple, Samsung, Sony, Bose |
| 3 | **Go-Getters** | 25-44 | very high | Computers, Smartphones, Audio | Apple, Dell, Lenovo, Samsung |
| 4 | **Outcasts** | 18-24 | low | Gaming, Smartphones mid, Audio budget | Xiaomi, Realme, HyperX, Logitech |
| 5 | **Contributors** | 45-54 | low (70% F) | Small Appliances, Large Appliances | Philips, Bosch, Tefal, Electrolux |
| 6 | **Floaters** | 45-54 | low | Large Appliances, TV mid-range | Samsung, LG, Beko, Whirlpool, Amica |

### dim_promo — 10 tipi

| ID | Nome | Tipo | ROI medio |
|---:|------|------|----------:|
| 1 | Rabat -10% | percentage_discount | 1.80 |
| 2 | Rabat -20% | percentage_discount | 1.30 |
| 3 | Rabat -30% | percentage_discount | 0.84 |
| 4 | 2+1 Gratis | bundle | 1.59 |
| 5 | Cashback 15% | cashback | 1.39 |
| 6 | Hit Dnia | flash_sale | 2.09 |
| 7 | Tylko w App | app_only | 1.70 |
| 8 | Drugi za 1 PLN | bundle | 1.20 |
| 9 | Black Friday | seasonal | 1.09 |
| 10 | Swieta | seasonal | 1.00 |

### dim_date — 1,096 righe (2023-2025)

Peak events: Black Friday (Nov 22-30), Xmas (Dec), Back to School (Aug-Sep 15), Regular.

### dim_customer (buyers) — 12.000 righe

Entità **acquirente**: collegamento a segmento HCG, canali, loyalty, demografia. Usata per buyer analytics senza esplodere fact_sales_daily.

| Gruppo | Colonne |
|--------|---------|
| **Identità** | customer_id (PK), global_user_id, is_registered, registration_date, first_purchase_date, last_purchase_date, customer_status (active/dormant/churned) |
| **Canali** | has_app, app_first_seen_date, app_last_seen_date, has_website_account, preferred_channel (web/app/store), omnichannel_flag |
| **Loyalty** | has_loyalty_card, loyalty_tier (none/basic/silver/gold), loyalty_join_date |
| **Demografia** | gender (male/female/unknown), age_band, birth_year, city, region, urbanicity, income_band |
| **Segmento** | segment_id (FK → dim_segment), segment_confidence (0–1) |
| **Consensi** | marketing_optin_email, marketing_optin_push, marketing_optin_sms |

I dati demografici e di canale sono simulati nel seed; lo schema è pronto per dati reali (app/web/store, CRM).

---

## Tabelle Fatti

### fact_sales_daily — derivata da ordini

**Non più nel seed.** Creata da `derive_sales_from_orders.sql`: aggregazione di `v_sales_daily_by_channel` (senza channel). Fonte: fact_orders + fact_order_items + dim_product + dim_customer. Ogni riga = (date × brand × subcategory × segment).

| Colonna | Tipo | Descrizione |
|---------|------|-------------|
| date | DATE | Data vendita |
| brand_id | INT64 | FK → dim_brand |
| brand_name | STRING | Denormalizzato per RLS |
| category_id | INT64 | FK → dim_category (subcategory, level 2) |
| parent_category_id | INT64 | FK → dim_category (parent, level 1) |
| segment_id | INT64 | FK → dim_segment |
| gender | STRING | M / F / other (derivato da segment demographics) |
| gross_pln | NUMERIC | Vendite lorde PLN |
| net_pln | NUMERIC | Vendite nette (gross / 1.23) |
| units | INT64 | Unita vendute |
| promo_flag | BOOL | In promozione? |
| promo_id | INT64 | FK → dim_promo (NULL se non promo) |
| discount_depth_pct | NUMERIC | Profondita sconto % |

I volumi dipendono dagli ordini generati (380k ordini, ~1M+ righe fact_order_items). Differenziazione per segmento, channel, peak_event e promo.

### fact_promo_performance — derivata da derive

Creata da `derive_sales_from_orders.sql`. **incremental_sales_pln** = attributed - baseline (media non-promo ultimi 28 gg per brand+parent_category). Aggregato per (promo × brand × parent_category × date).

| Colonna | Tipo | Descrizione |
|---------|------|-------------|
| promo_id | INT64 | FK → dim_promo |
| brand_id | INT64 | FK → dim_brand |
| brand_name | STRING | Denormalizzato |
| category_id | INT64 | Parent category ID (1-10) |
| date | DATE | Data |
| attributed_sales_pln | NUMERIC | Vendite attribuite alla promo |
| incremental_sales_pln | NUMERIC | Vendite incrementali |
| discount_cost_pln | NUMERIC | Costo sconti |
| media_cost_pln | NUMERIC | Costo media |
| roi | NUMERIC | ROI = (attr - disc - media) / (disc + media) |

**incremental_sales_pln** = attributed - baseline (media non-promo ultimi 28 gg per brand+parent_category).

### fact_orders — ~380.000 righe (date × customer)

Un ordine per riga: collegamento **buyer ↔ vendita** per analisi a livello cliente senza toccare fact_sales_daily.

| Colonna | Tipo | Descrizione |
|---------|------|-------------|
| order_id | INT64 | Chiave ordine |
| date | DATE | Data ordine |
| customer_id | INT64 | FK → dim_customer |
| channel | STRING | web / app / store |
| gross_pln, net_pln | NUMERIC | Totale ordine |
| units | INT64 | Unità nell’ordine |
| promo_flag, promo_id, discount_depth_pct | | Promo applicata |

### product_pool_seg_channel_gender — pool prodotti per (segment, channel, gender)

Definisce quali prodotti possono essere scelti per ogni combinazione segmento×canale×genere. Logica:
- **seg_pref**, **ch_pref**, **gender_pref**: preferenze per categoria (parent_category_id 1–10)
- **INTERSECTION**: quando segment, channel e gender concordano su una categoria → pool ristretto
- **UNION**: quando intersection vuota → pool più ampio
- **fact_order_items**: 88% ordini da pool, 12% random. Bilanciamento vendite per categoria tramite preferenze.

### fact_order_items — ~1M+ righe (order × product)

Righe ordine: prodotto, quantità, valore. Per **basket mix**, brand/categoria per ordine e per segmento (via customer → segment_id). Prodotti scelti da `product_pool_seg_channel_gender` (88%) o random (12%).

| Colonna | Tipo | Descrizione |
|---------|------|-------------|
| order_id | INT64 | FK → fact_orders (implicito) |
| product_id | INT64 | FK → dim_product |
| quantity | INT64 | Quantità |
| gross_pln | NUMERIC | Valore riga |

### KPI sbloccati con dim_customer + fact_orders

- **Buyers unici** per periodo (COUNT DISTINCT customer_id su fact_orders).
- **Mix canale**: % ordini/buyers app vs web vs store (fact_orders.channel).
- **Loyalty penetration**: % buyers con carta (dim_customer.has_loyalty_card + fact_orders).
- **Repeat rate e retention** (30/60/90 gg) su fact_orders + first_purchase_date/last_purchase_date.
- **AOV e frequency per segmento**: JOIN fact_orders → dim_customer.segment_id.
- **Promo responsiveness per segmento**: promo_orders / total_orders per segment_id.
- **Basket mix per segmento**: fact_order_items JOIN fact_orders JOIN dim_customer JOIN dim_product (categoria/brand).

---

## Query tipiche

```sql
-- Vendite per parent category
SELECT pc.category_name, SUM(f.gross_pln) AS sales
FROM mart.fact_sales_daily f
JOIN mart.dim_category pc ON pc.category_id = f.parent_category_id
GROUP BY pc.category_name ORDER BY sales DESC;

-- Drill-down subcategorie (es. solo TV)
SELECT c.category_name AS subcategory, SUM(f.gross_pln) AS sales
FROM mart.fact_sales_daily f
JOIN mart.dim_category c ON c.category_id = f.category_id
WHERE f.parent_category_id = 1
GROUP BY c.category_name ORDER BY sales DESC;

-- Filtro: funziona sia per parent (1-10) che subcategory (101+)
WHERE (@cat IS NULL OR f.parent_category_id = @cat OR f.category_id = @cat)

-- Buyer analytics: buyers unici e mix canale
SELECT o.channel, COUNT(DISTINCT o.customer_id) AS buyers, COUNT(*) AS orders, SUM(o.gross_pln) AS sales
FROM mart.fact_orders o
WHERE o.date BETWEEN '2024-01-01' AND '2024-12-31'
GROUP BY o.channel ORDER BY sales DESC;

-- Loyalty penetration (% buyers con carta)
SELECT
  ROUND(100.0 * COUNT(DISTINCT CASE WHEN c.has_loyalty_card THEN o.customer_id END) / NULLIF(COUNT(DISTINCT o.customer_id), 0), 1) AS pct_loyalty_buyers
FROM mart.fact_orders o
JOIN mart.dim_customer c ON c.customer_id = o.customer_id
WHERE o.date >= '2024-01-01';

-- AOV e ordini per segmento HCG
SELECT s.segment_name, COUNT(DISTINCT o.customer_id) AS buyers, COUNT(*) AS orders,
  ROUND(AVG(o.gross_pln), 2) AS aov, ROUND(SUM(o.gross_pln), 2) AS total_sales
FROM mart.fact_orders o
JOIN mart.dim_customer c ON c.customer_id = o.customer_id
JOIN mart.dim_segment s ON s.segment_id = c.segment_id
WHERE o.date BETWEEN '2024-01-01' AND '2024-12-31'
GROUP BY s.segment_name ORDER BY total_sales DESC;
```

---

## Vista v_sales_daily_by_channel

Derivata da `fact_orders` + `fact_order_items` + `dim_product` + `dim_customer`: vendite giornaliere per (date, brand, category, segment, gender, **channel**).  
`fact_sales_daily` e `fact_promo_performance` vengono create da `derive_sales_from_orders.sql`.

## Come rieseguire il seed

```bash
gcloud auth application-default login
python scripts/generate_seed_data.py   # opzionale: rigenera dim_product_generated.sql
python scripts/run_bigquery_schema.py
```

Idempotente (CREATE OR REPLACE). Esegue `schema_and_seed.sql`, poi `dim_product_generated.sql`, poi `derive_sales_from_orders.sql`.
