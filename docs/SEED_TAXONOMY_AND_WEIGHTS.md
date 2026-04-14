# Tassonomia seed e pesi (revisione esterna)

Documento di riferimento per **ChatGPT o altri revisori**: elenca categorie, sottocategorie, brand, segmenti, caratteristiche prodotto e **tutti i pesi / moltiplicatori** usati nella generazione sintetica BigQuery.  
**Non sono dati reali Media Expert**: è un seed calibrato per dashboard demo/analytics.

**File sorgente principali**

| Cosa | Path |
|------|------|
| Brand, categorie, segmenti, SQL pool ordini | `bigquery/schema_and_seed.sql` |
| Catalogo `dim_product` (generato) | `scripts/generate_seed_data.py` → `bigquery/dim_product_generated.sql` |
| Focus brand × macro, prezzi subcat | `scripts/seed_catalog/constants.py` |
| Pesi brand × macro + moltiplicatori catalogo | `scripts/seed_catalog/brand_parent_revenue_weights.json` |
| Prior mercato CE + merge pesi | `scripts/seed_catalog/market_reality.py` |
| Vendite derivate (moltiplicatori feed) | `bigquery/derive_sales_from_orders.sql` |

---

## 1. Categorie parent (`category_id` 1–10, `level = 1`)

| ID | Nome |
|----|------|
| 1 | TV & Home Entertainment |
| 2 | Smartphones, tablets & wearables |
| 3 | Computers & IT |
| 4 | Gaming |
| 5 | Large Appliances |
| 6 | Small Appliances |
| 7 | Audio |
| 8 | Smart Home |
| 9 | Health & Beauty Tech |
| 10 | Photo & Video |

---

## 2. Sottocategorie (`level = 2`)

Ogni riga: `subcategory_id` → nome → `parent_category_id`.

### Parent 1 — TV & Home Entertainment

| ID | Nome |
|----|------|
| 101 | LED TV |
| 102 | OLED TV |
| 103 | QLED TV |
| 104 | Mini LED TV |
| 105 | Soundbars |
| 106 | Home cinema systems |
| 107 | Projectors |
| 108 | Streaming devices |

### Parent 2 — Smartphones, tablets & wearables

| ID | Nome |
|----|------|
| 201 | Smartphones flagship |
| 202 | Smartphones mid-range |
| 203 | Smartphones entry |
| 204 | Foldable smartphones |
| 205 | Tablets |
| 206 | Smartwatches |
| 207 | Fitness trackers |
| 208 | Phone accessories |

### Parent 3 — Computers & IT

| ID | Nome |
|----|------|
| 301 | Laptops |
| 302 | Gaming laptops |
| 303 | Desktop PCs |
| 304 | Monitors |
| 305 | Keyboards |
| 306 | Mice |
| 307 | Webcams |
| 308 | External storage |
| 309 | Routers |
| 310 | Mesh WiFi systems |

### Parent 4 — Gaming

| ID | Nome |
|----|------|
| 401 | Consoles |
| 402 | Gaming PCs |
| 403 | Gaming laptops |
| 404 | Controllers |
| 405 | Gaming headsets |
| 406 | Gaming keyboards |
| 407 | Gaming mice |
| 408 | VR headsets |

### Parent 5 — Large Appliances

| ID | Nome |
|----|------|
| 501 | Refrigerators |
| 502 | Washing machines |
| 503 | Dryers |
| 504 | Dishwashers |
| 505 | Ovens |
| 506 | Induction hobs |
| 507 | Built-in appliances |
| 508 | Freezers |

### Parent 6 — Small Appliances

| ID | Nome |
|----|------|
| 601 | Coffee machines |
| 602 | Blenders |
| 603 | Air fryers |
| 604 | Vacuum cleaners |
| 605 | Robot vacuums |
| 606 | Kitchen processors |
| 607 | Electric kettles |
| 608 | Toasters |

### Parent 7 — Audio

| ID | Nome |
|----|------|
| 701 | Wireless headphones |
| 702 | Noise cancelling headphones |
| 703 | Earbuds |
| 704 | Portable speakers |
| 705 | Hi-Fi systems |
| 706 | DJ equipment |

### Parent 8 — Smart Home

| ID | Nome |
|----|------|
| 801 | Smart speakers |
| 802 | Smart lighting |
| 803 | Smart thermostats |
| 804 | Security cameras |
| 805 | Smart locks |
| 806 | Smart plugs |

### Parent 9 — Health & Beauty Tech

| ID | Nome |
|----|------|
| 901 | Electric toothbrushes |
| 902 | Hair dryers |
| 903 | Hair straighteners |
| 904 | Grooming kits |
| 905 | Smart scales |

### Parent 10 — Photo & Video

| ID | Nome |
|----|------|
| 1001 | Cameras |
| 1002 | Mirrorless cameras |
| 1003 | Lenses |
| 1004 | Action cameras |
| 1005 | Drones |

---

## 3. Brand (`dim_brand`, 59 righe)

| brand_id | brand_name | Paese | brand_category_focus (testo descrittivo) |
|----------|------------|-------|-------------------------------------------|
| 1 | Samsung | KR | TV, Large Appliances, Smartphones |
| 2 | LG | KR | TV, Large Appliances |
| 3 | Sony | JP | TV, Audio, Gaming, Photo |
| 4 | Philips | NL | TV, Small Appliances, Smart Home, Health |
| 5 | TCL | CN | TV |
| 6 | Hisense | CN | TV |
| 7 | Panasonic | JP | TV |
| 8 | Apple | US | Smartphones, Computers, Audio |
| 9 | Xiaomi | CN | Smartphones, Computers (mesh/router), Small Appliances, Smart Home |
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
| 39 | Google | US | Smartphones, Smart Home |
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
| 56 | Honor | CN | Smartphones |
| 57 | Vivo | CN | Smartphones |
| 58 | Nothing | UK | Smartphones |
| 59 | POCO | CN | Smartphones |

### 3bis. `BRAND_ALLOWED_SUBCATEGORIES` (`constants.py`)

`None` = tutte le subcat dei parent in `BRAND_FOCUS`. Altrimenti solo le `subcategory_id` elencate (whitelist). Esempi: Samsung `101–104`, `201–206`, `501–508`; LG TV `101–104` + bianchi `501–508`; Apple mobile `201–202,204–206,208` + IT completo + audio; Xiaomi include `309,310` e non tutto il parent 3; Garmin `206–207`; Fitbit `207`; Google `201,204` + Nest `801–804,806`; Ring `804`; Oral-B `901`; Canon/Nikon `1001–1003`; DJI `1004–1005`; Huawei `201–204,206`; Honor/Vivo/Nothing/POCO `201–204,208`.

---

## 4. BRAND_FOCUS (macro dove il brand ha SKU nel seed)

Definito in `scripts/seed_catalog/constants.py`. Valori: lista di `parent_category_id`.

| brand_id | Parent IDs | Note |
|----------|------------|------|
| 1 | 1, 2, 5 | Samsung |
| 2 | 1, 5 | LG |
| 3 | 1, 7, 4, 10 | Sony |
| 4 | 1, 6, 8, 9 | Philips |
| 5–7 | 1 | TV OEM |
| 8 | 2, 3, 7 | Apple |
| 9 | 2, 3, 6, 8 | Xiaomi (+ solo router/mesh nel parent 3) |
| 10–15, 55 | 2 | Smartphone OEM / wearables |
| 39 | 2, 8 | Google |
| 56–59 | 2 | Honor, Vivo, Nothing, POCO |
| 16–18, 20 | 3 | PC OEM |
| 19 | 3, 4 | Asus |
| 21–22 | 3, 4 | MSI, Logitech |
| 23 | 3, 8 | TP-Link |
| 24–28 | 4 | Gaming |
| 29 | 5, 6 | Bosch |
| 30–34 | 5 | Bianchi |
| 35–38 | 6 | Small app |
| 40–41 | 8 | Smart home (Google anche parent 2) |
| 42–46 | 7 | Audio |
| 47–48, 53–54 | 9 | Health beauty |
| 49–52 | 10 | Photo |

---

## 5. Segmenti cliente (`dim_segment`, 6 segmenti HCG-style)

| segment_id | segment_name | Descrizione sintetica | age_range | income_level | gender_skew | top_driver |
|------------|--------------|----------------------|-----------|--------------|-------------|------------|
| 1 | Liberals | Wellness, knowledge, sustainability | 45-64 | high | 57% male | wellness |
| 2 | Optimistic Doers | Status, image, work-life | 35-54 | high | balanced | status |
| 3 | Go-Getters | Performance, career | 25-44 | very_high | balanced | performance |
| 4 | Outcasts | Entertainment, price sensitive, young | 18-24 | low | 58% male | entertainment |
| 5 | Contributors | Family, home | 45-54 | low | 70% female | family |
| 6 | Floaters | Necessity, stability | 45-54 | low | balanced | necessity |

---

## 6. Caratteristiche prodotto nel catalogo (`dim_product`)

| Campo | Significato |
|-------|-------------|
| `product_id` | 1…1200 (fisso nello schema attuale) |
| `brand_id`, `category_id` (parent), `subcategory_id` | Legame a tassonomia sopra |
| `price_pln` | Da profilo subcat + rumore (vedi §8) |
| `launch_year` | Opzionale |
| `premium_flag` | BOOL: influenza pool ordini, qty, moltiplicatore riga |

**Profilo prezzo per sottocategoria** (`SUBCAT_PRICE` in `constants.py`): terna`(avg_price_pln, spread_pct, premium_share)` — media PLN, spread relativo, frazione SKU marcati premium.

**Insiemi speciali**

- `SPECIALIST_BRANDS` = {36, 42, 49, 50, 51, 52} (Dyson, Bose, Canon, Nikon, GoPro, DJI)
- `MASS_BRANDS` = {9, 32, 34, 59} (Xiaomi, Beko, Amica, POCO)
- `POOL_UNIVERSAL_EXCLUDED_SUBCATEGORIES`: subcat alta specificità escluse dal blocco universal nel pool (sync con SQL).

**Varianti nome prodotto** (`VARIANTS`): suffissi tipo `" Pro"`, `" Ultra"`, taglie TV, ecc.

**Moltiplicatore “smartphone Poland”** (`brand_phone_mass_multiplier`): solo se l’allowlist del brand interseca **201–204**; `max(0.55, min(2.6, score/14))` con score proxy. **Wearables** (`brand_wearables_mass_multiplier`): se allowlist interseca **206–207**, boost leggero per marchi tipo Garmin/Fitbit/Apple.

---

## 7. Pesi JSON brand × macro (`brand_parent_revenue_weights.json`)

### 7.1 `catalog_share_multiplier` (moltiplica il “peso” SKU nel catalogo per quel `brand_id`)

| brand_id | Moltiplicatore |
|----------|----------------|
| 1 | 2.35 |
| 2 | 1.8 |
| 3 | 1.35 |
| 4 | 1.25 |
| 8 | 2.0 |
| 9 | 1.45 |
| 39 | 1.35 |
| 56 | 1.12 |
| 57 | 1.1 |
| 58 | 1.08 |
| 59 | 1.15 |
| 19 | 1.15 |
| 29 | 1.35 |
| 30 | 1.2 |
| 36 | 1.2 |

(Altri brand: default 1.0 nel codice se non listati.)

### 7.2 `brands` — pesi **non normalizzati** sul sottoinsieme di parent del `BRAND_FOCUS`

Chiavi stringa = `brand_id`. Valori interni = `parent_category_id` → peso relativo **prima** del merge con prior mercato (§7.3).

| brand_id | Pesi (parent → valore) |
|----------|-------------------------|
| 1 | 1→0.30, 2→0.52, 5→0.18 |
| 2 | 1→0.48, 5→0.52 |
| 3 | 1→0.38, 4→0.24, 7→0.22, 10→0.16 |
| 4 | 1→0.34, 6→0.30, 8→0.22, 9→0.14 |
| 8 | 2→0.62, 3→0.28, 7→0.10 |
| 9 | 2→0.41, 3→0.10, 6→0.31, 8→0.18 |
| 39 | 2→0.72, 8→0.28 |
| 56–59 | 2→1.0 (ciascuno) |
| 19 | 3→0.55, 4→0.45 |
| 21 | 3→0.52, 4→0.48 |
| 22 | 3→0.55, 4→0.45 |
| 29 | 5→0.62, 6→0.38 |
| 36 | 6→0.72, 9→0.28 |
| 42 | 7→1.0 |
| 49 | 10→1.0 |
| 50 | 10→1.0 |

Meta JSON: `as_of` 2026-04, note fonti Samsung IR / mix proxy (non contabilità retailer).

### 7.3 Prior valore mercato CE (`MARKET_PARENT_VALUE_PRIOR` in `market_reality.py`)

Somma = 1. Usato nel merge con i pesi JSON.

| parent_category_id | Prior |
|--------------------|-------|
| 1 (TV) | 0.16 |
| 2 (Mobile / tablet / wearables) | 0.20 |
| 3 (Computers) | 0.22 |
| 4 (Gaming) | 0.08 |
| 5 (Large app) | 0.17 |
| 6 (Small app) | 0.10 |
| 7 (Audio) | 0.04 |
| 8 (Smart home) | 0.02 |
| 9 (Health/beauty) | 0.01 |
| 10 (Photo) | 0.01 |

### 7.4 Formula merge JSON × prior

Per ogni parent `p` nel focus del brand:

- `avg_mp` = media dei prior sui parent del brand
- `rel = prior[p] / avg_mp`
- `blend = 0.78 + 0.22 * clamp(rel, 0.45, 1.65)` (clamp operativo: `min(1.65, max(0.45, rel))`)
- peso grezzo = `max(1e-6, json_weight[p]) * blend`
- poi **normalizzazione** sui parent del brand a somma 1.

Override env: `SEED_BRAND_FOCUS_JSON` può sostituire `BRAND_FOCUS` (`env_overrides.py`).

---

## 8. Pool acquisti `product_pool_seg_channel_gender` (bias segmento / canale / genere)

Tripletta ammessa = intersezione di `seg_pref` ∧ `ch_pref` ∧ `gender_pref`; se vuota, fallback da unione (`union_pref` / `all_pref`). Vedi SQL in `schema_and_seed.sql`.

### 8.1 `seg_pref` — parent ammessi per `segment_id`

| segment_id | parent_category_id (insieme) |
|------------|------------------------------|
| 1 | 8, 9, 3, 2, 6, 1, 7, 5 |
| 2 | 1, 2, 7, 3, 6, 8, 5, 9 |
| 3 | 3, 2, 7, 1, 6, 8, 4, 9, 5 |
| 4 | 2, 7, 4, 1, 8, 6, 3, 5 |
| 5 | 5, 6, 1, 2, 9, 8 |
| 6 | 5, 6, 1, 2, 9, 8, 7 |

### 8.2 `ch_pref` — parent per `channel`

| channel | parent_category_id |
|---------|-------------------|
| store | 5, 1, 6, 2, 8 |
| app | 1, 2, 4, 7, 3, 8, 5 |
| web | 1, 3, 4, 10, 2, 6, 8, 5 |

### 8.3 `gender_pref` — parent per `gender`

| gender | parent_category_id |
|--------|-------------------|
| male | 3, 2, 1, 5, 7, 8 |
| female | 6, 9, 5, 2, 1, 8 |

### 8.4 Duplicazioni righe pool (più sorteggi = più frequenza nel `ARRAY_AGG`)

| Condizione | Moltiplicatore righe (`GENERATE_ARRAY` upper bound) |
|------------|-----------------------------------------------------|
| Base `all_pref` × `dim_product`, brand 1 o 2 e parent 5 | **3** |
| Base, parent 5 e brand in (29,30,31,32,33,34) | **4** |
| Altrimenti base | **1** |
| Segmenti 1–3, `premium_flag` | **7** |
| Segmenti 1–3, premium e subcat in (201,202,204) | **7** |
| Segmenti 4–6, premium e subcat in (201,202,204) | **3** |
| Segmenti 4–6, premium e subcat in (102,103,105) | **2** |
| Copertura universale | **1×** riga per SKU; esclusi `premium_flag`, brand specialist (36,42,49,50,51,52), subcat in (102,103,104,204,408,1002,1003,1005) |
| Esclusione | Segmenti 4–6: nel blocco base esclusi i `premium_flag` (premium rientrano dai UNION dedicati) |

---

## 9. Ordini (`fact_orders`) — parametri rilevanti

- **Canale ordine**: ~68% usa `preferred_channel` del cliente; ~32% split pseudo-casuale `web` / `app` / `store`.
- **Ticket ordine** (header): `ROUND(95 + MOD(hash(order_id), 42000)/6.5, 2)` PLN (range indicativo fino ~6.5k+).
- **Promo**: probabilità da mix `segment_id` + picco calendario + rumore cliente; soglia tra ~22% e ~87%.
- **Profondità sconto** (se promo): base 11± + offset per segmento (es. seg 4 +4, seg 1 -1.5), clamp 6–28%.

---

## 10. Righe ordine (`fact_order_items`) — scelta prodotto e qty

- **Linee per ordine**: `1 + MOD(order_id, 4)` (1–4 righe).
- **Pool hit**: ~82% + offset da `segment_id` usa prodotto dal pool; altrimenti fallback (non-premium per seg 4–6, o `10001 + MOD(..., 1200)`).
- **Qty base** da categoria (es. parent 5 → 1;6 → 1–2; 3/4 → 1–3).
- **Moltiplicatore qty** (`lines_qty`): premium ×1.08 se seg 1–3, ×0.90 se seg 4–6; non-premium ×1.52 se seg 4–6, ×0.80 se seg 1–3.
- **Swap premium** (seg 4–6): ~20% chance di sostituire premium (fuori allowlist smartphone flagship/mid/fold + OLED/QLED/soundbar) con SKU non-premium.

### 10.1 `gross_pln` riga (`with_price`)

Moltiplicatori concatenati:

1. `price_pln * quantity`
2. Se ordine in promo: `(1 - discount_depth_pct/100)`
3. Premium: ×**1.22** (seg 1–3) o ×**0.62** (seg 4–6); altrimenti ×1
4. Rumore brand×subcat: `0.68 + MOD(hash('br'…), 55)/100` → circa **0.68–1.22**
5. Rumore evento: `0.72 + 0.46 * (MOD(hash('evt'…),1000)/1000)` → **0.72–1.18**
6. **Samsung/LG bianchi**: se `brand_id IN (1,2)` e `category_id = 5` → ×**1.04**

---

## 11. Derivazione `fact_sales_daily` (`derive_sales_from_orders.sql`)

- Vista `v_sales_daily_by_channel`: ogni riga `oi.gross_pln` moltiplicata per  `ch_f = 0.93 + 0.14 * (MOD(hash(brand|channel),1000)/1000)` → ~**0.93–1.07**  
  Profondità sconto aggregata usa anche `dd_f = 0.87 + 0.26 * …` → ~**0.87–1.13**
- Tabella `fact_sales_daily`: somma da vista **senza** channel, con ulteriore fattore su gross/net per brand:  
  `0.90 + 0.20 * (MOD(hash('bsd'|brand_id),1000)/1000)` → ~**0.90–1.10**

---

## 12. Tipi promo (`dim_promo`, id 1–10)

| promo_id | promo_name | promo_type |
|----------|------------|------------|
| 1 | Rabat -10% | percentage_discount |
| 2 | Rabat -20% | percentage_discount |
| 3 | Rabat -30% | percentage_discount |
| 4 | 2+1 Gratis | bundle |
| 5 | Cashback 15% | cashback |
| 6 | Hit Dnia | flash_sale |
| 7 | Tylko w App | app_only |
| 8 | Drugi za 1 PLN | bundle |
| 9 | Black Friday | seasonal |
| 10 | Swieta | seasonal |

---

## 13. Calendario (`dim_date`)

Periodo seed: **2023-01-01 → 2026-12-31**. Campo `peak_event` (Black Friday, Christmas, Back to School, ecc.) influenza probabilità promo e moltiplicatore evento sulle righe.

---

*Fine documento. Aggiornare questa pagina quando si cambiano JSON, `market_reality.py`, `constants.py` o il blocco pool in `schema_and_seed.sql`.*
