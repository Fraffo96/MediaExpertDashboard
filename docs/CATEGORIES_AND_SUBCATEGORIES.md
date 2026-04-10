# Categorie e sottocategorie (seed)

Riferimento rapido per **ID usati in BigQuery e nelle dashboard**.  
Dettagli, brand e pesi: [SEED_TAXONOMY_AND_WEIGHTS.md](SEED_TAXONOMY_AND_WEIGHTS.md).

## Convenzione `category_id`

| Livello | ID | Note |
|---------|-----|------|
| Parent (macro) | **1–10** | `parent_category_id`, filtri “categoria” |
| Subcategoria | **101–108, 201–208, …** | `category_id` su `dim_product` / `fact_sales_daily`; centinaia = parent × 100 + offset |

---

## Parent1–10

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

## Sottocategorie (72)

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
