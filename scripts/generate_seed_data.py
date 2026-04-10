#!/usr/bin/env python3
"""
Genera dati seed per Media Expert Dashboard.
Output: bigquery/seed_generated.sql (da includere in schema_and_seed.sql o eseguire separatamente)
Mantiene ESATTAMENTE brand 1-55, category 1-10, subcategory ids, promo 1-10, segment 1-6.
"""
from __future__ import annotations

import json
import os

# Brand focus: brand_id -> [parent_category_ids]
BRAND_FOCUS: dict[int, list[int]] = {
    23: [3, 8],     # TP-Link: Computers, Smart Home
    1: [1, 2, 5],   # Samsung: TV, Smartphones, Large Appliances
    2: [1, 5],      # LG
    3: [1, 7, 4, 10],  # Sony: TV, Audio, Gaming, Photo
    4: [1, 6, 8, 9],   # Philips
    5: [1], 6: [1], 7: [1],
    8: [2, 3, 7],   # Apple
    9: [2, 6, 8],   # Xiaomi
    10: [2], 11: [2], 12: [2], 13: [2], 14: [2],
    15: [2],        # Garmin
    16: [3], 17: [3], 18: [3],
    19: [3, 4],     # Asus
    20: [3], 21: [3, 4], 22: [3, 4], 23: [3, 8],
    24: [4], 25: [4], 26: [4], 27: [4], 28: [4],
    29: [5, 6],     # Bosch
    30: [5], 31: [5], 32: [5], 33: [5], 34: [5],
    35: [6], 36: [6, 9], 37: [6], 38: [6],
    39: [8], 40: [8], 41: [8],
    42: [7], 43: [7], 44: [7], 45: [7], 46: [7],
    47: [9], 48: [9], 53: [9], 54: [9],
    49: [10], 50: [10], 51: [10], 52: [10],
    55: [2],        # Fitbit
}

# Parent -> subcategory ids
PARENT_TO_SUB: dict[int, list[int]] = {
    1: [101, 102, 103, 104, 105, 106, 107, 108],
    2: [201, 202, 203, 204, 205, 206, 207, 208],
    3: [301, 302, 303, 304, 305, 306, 307, 308, 309, 310],
    4: [401, 402, 403, 404, 405, 406, 407, 408],
    5: [501, 502, 503, 504, 505, 506, 507, 508],
    6: [601, 602, 603, 604, 605, 606, 607, 608],
    7: [701, 702, 703, 704, 705, 706],
    8: [801, 802, 803, 804, 805, 806],
    9: [901, 902, 903, 904, 905],
    10: [1001, 1002, 1003, 1004, 1005],
}

SUBCAT_NAMES: dict[int, str] = {
    101: "LED TV", 102: "OLED TV", 103: "QLED TV", 104: "Mini LED TV",
    105: "Soundbars", 106: "Home cinema systems", 107: "Projectors", 108: "Streaming devices",
    201: "Smartphones flagship", 202: "Smartphones mid-range", 203: "Smartphones entry",
    204: "Foldable smartphones", 205: "Tablets", 206: "Smartwatches", 207: "Fitness trackers", 208: "Phone accessories",
    301: "Laptops", 302: "Gaming laptops", 303: "Desktop PCs", 304: "Monitors",
    305: "Keyboards", 306: "Mice", 307: "Webcams", 308: "External storage", 309: "Routers", 310: "Mesh WiFi systems",
    401: "Consoles", 402: "Gaming PCs", 403: "Gaming laptops", 404: "Controllers",
    405: "Gaming headsets", 406: "Gaming keyboards", 407: "Gaming mice", 408: "VR headsets",
    501: "Refrigerators", 502: "Washing machines", 503: "Dryers", 504: "Dishwashers",
    505: "Ovens", 506: "Induction hobs", 507: "Built-in appliances", 508: "Freezers",
    601: "Coffee machines", 602: "Blenders", 603: "Air fryers", 604: "Vacuum cleaners",
    605: "Robot vacuums", 606: "Kitchen processors", 607: "Electric kettles", 608: "Toasters",
    701: "Wireless headphones", 702: "Noise cancelling headphones", 703: "Earbuds",
    704: "Portable speakers", 705: "Hi-Fi systems", 706: "DJ equipment",
    801: "Smart speakers", 802: "Smart lighting", 803: "Smart thermostats",
    804: "Security cameras", 805: "Smart locks", 806: "Smart plugs",
    901: "Electric toothbrushes", 902: "Hair dryers", 903: "Hair straighteners",
    904: "Grooming kits", 905: "Smart scales",
    1001: "Cameras", 1002: "Mirrorless cameras", 1003: "Lenses",
    1004: "Action cameras", 1005: "Drones",
}

BRAND_NAMES: dict[int, str] = {
    1: "Samsung", 2: "LG", 3: "Sony", 4: "Philips", 5: "TCL", 6: "Hisense", 7: "Panasonic",
    8: "Apple", 9: "Xiaomi", 10: "Oppo", 11: "Realme", 12: "Huawei", 13: "Motorola", 14: "OnePlus",
    15: "Garmin", 16: "Dell", 17: "HP", 18: "Lenovo", 19: "Asus", 20: "Acer", 21: "MSI",
    22: "Logitech", 23: "TP-Link", 24: "Microsoft", 25: "Nintendo", 26: "Razer", 27: "SteelSeries", 28: "HyperX",
    29: "Bosch", 30: "Siemens", 31: "Whirlpool", 32: "Beko", 33: "Electrolux", 34: "Amica",
    35: "Tefal", 36: "Dyson", 37: "DeLonghi", 38: "Krups",
    39: "Google", 40: "Amazon", 41: "Ring",
    42: "Bose", 43: "JBL", 44: "Marshall", 45: "Beats", 46: "Sennheiser",
    47: "Braun", 48: "Oral-B", 49: "Canon", 50: "Nikon", 51: "GoPro", 52: "DJI",
    53: "Remington", 54: "Withings", 55: "Fitbit",
}

# Subcategory price profile: (avg_price_pln, spread_pct, premium_share)
# spread = ±% for variance, premium_share = % of products that are premium
SUBCAT_PRICE: dict[int, tuple[int, float, float]] = {
    101: (3500, 0.35, 0.4), 102: (6500, 0.4, 0.8), 103: (5000, 0.35, 0.7), 104: (4500, 0.3, 0.6),
    105: (1200, 0.5, 0.3), 106: (2500, 0.5, 0.5), 107: (3500, 0.5, 0.6), 108: (350, 0.6, 0.2),
    201: (4500, 0.4, 0.7), 202: (2200, 0.45, 0.3), 203: (900, 0.5, 0.1), 204: (6000, 0.3, 0.9),
    205: (2800, 0.4, 0.5), 206: (1200, 0.5, 0.4), 207: (450, 0.5, 0.2), 208: (150, 0.7, 0.1),
    301: (4500, 0.5, 0.4), 302: (6500, 0.4, 0.7), 303: (5000, 0.5, 0.5), 304: (1500, 0.5, 0.4),
    305: (350, 0.6, 0.3), 306: (200, 0.6, 0.2), 307: (400, 0.5, 0.3), 308: (350, 0.6, 0.2),
    309: (250, 0.6, 0.2), 310: (450, 0.5, 0.3),
    401: (2200, 0.4, 0.6), 402: (5500, 0.45, 0.7), 403: (6000, 0.4, 0.7), 404: (350, 0.5, 0.3),
    405: (450, 0.5, 0.3), 406: (400, 0.5, 0.4), 407: (350, 0.5, 0.4), 408: (2500, 0.5, 0.6),
    501: (4500, 0.5, 0.5), 502: (3500, 0.45, 0.5), 503: (3000, 0.5, 0.4), 504: (2800, 0.45, 0.5),
    505: (2500, 0.5, 0.4), 506: (1800, 0.5, 0.4), 507: (3500, 0.5, 0.5), 508: (1500, 0.5, 0.3),
    601: (1200, 0.5, 0.4), 602: (350, 0.6, 0.2), 603: (550, 0.5, 0.3), 604: (1200, 0.5, 0.4),
    605: (2200, 0.45, 0.5), 606: (450, 0.5, 0.2), 607: (180, 0.5, 0.1), 608: (220, 0.5, 0.2),
    701: (800, 0.5, 0.4), 702: (1500, 0.45, 0.6), 703: (450, 0.5, 0.3), 704: (400, 0.5, 0.3),
    705: (2500, 0.5, 0.6), 706: (1500, 0.6, 0.5),
    801: (450, 0.5, 0.2), 802: (250, 0.6, 0.2), 803: (350, 0.5, 0.3), 804: (450, 0.5, 0.3),
    805: (650, 0.5, 0.4), 806: (120, 0.6, 0.1),
    901: (350, 0.5, 0.3), 902: (280, 0.5, 0.2), 903: (220, 0.5, 0.2), 904: (180, 0.5, 0.2), 905: (350, 0.5, 0.3),
    1001: (3500, 0.5, 0.5), 1002: (9000, 0.45, 0.8), 1003: (2500, 0.6, 0.5), 1004: (1800, 0.5, 0.5), 1005: (3500, 0.5, 0.6),
}

# Specialist brands: fewer SKUs, concentrated
SPECIALIST_BRANDS = {36, 42, 49, 50, 51, 52}  # Dyson, Bose, Canon, Nikon, GoPro, DJI
# Mass brands: more SKUs, lower price tier
MASS_BRANDS = {9, 32, 34}  # Xiaomi, Beko, Amica

VARIANTS = ["", " Pro", " Plus", " Ultra", " Max", " Mini", " 55in", " 65in", " 75in", " Standard", " Premium", " Basic", " Lite"]


def effective_brand_focus() -> dict[int, list[int]]:
    raw = (os.environ.get("SEED_BRAND_FOCUS_JSON") or "").strip()
    if not raw:
        return BRAND_FOCUS
    try:
        d = json.loads(raw)
        out = dict(BRAND_FOCUS)
        for k, v in d.items():
            bid = int(k)
            if isinstance(v, list):
                out[bid] = [int(x) for x in v]
        return out
    except (TypeError, ValueError, json.JSONDecodeError):
        return BRAND_FOCUS


def build_brand_subcat_pairs() -> list[tuple[int, int]]:
    pairs = []
    for bid, parents in effective_brand_focus().items():
        for p in parents:
            for sid in PARENT_TO_SUB.get(p, []):
                pairs.append((bid, sid))
    return pairs


def generate_products(n: int = 1200) -> str:
    pairs = build_brand_subcat_pairs()
    products = []
    pid = 10001
    for i in range(n):
        idx = i % len(pairs)
        bid, sid = pairs[idx]
        parent = next((p for p, subs in PARENT_TO_SUB.items() if sid in subs), 1)
        avg, spread, prem_share = SUBCAT_PRICE.get(sid, (1000, 0.4, 0.3))
        # Mass brands: lower price
        if bid in MASS_BRANDS:
            avg = int(avg * 0.7)
            prem_share *= 0.5
        # Specialist: higher price
        if bid in SPECIALIST_BRANDS:
            avg = int(avg * 1.2)
            prem_share = min(0.9, prem_share * 1.3)
        # Variance
        var = 1 + (spread * ((i * 17 + 31) % 100 - 50) / 100)
        price = max(99, int(avg * var / 50) * 50)
        premium = (i * 13 + 7) % 100 < int(prem_share * 100)
        variant = VARIANTS[(i * 11) % len(VARIANTS)]
        name = f"{BRAND_NAMES[bid]} {SUBCAT_NAMES[sid]}{variant}".strip().replace("'", "''")
        launch = 2020 + (i % 5)
        products.append(f"  ({pid},'{name}',{bid},{parent},{sid},{price},{launch},{str(premium).upper()})")
        pid += 1
    return "INSERT mart.dim_product (product_id, product_name, brand_id, category_id, subcategory_id, price_pln, launch_year, premium_flag) VALUES\n" + ",\n".join(products)


def main():
    try:
        n = int(os.environ.get("SEED_NUM_PRODUCTS", "1200"))
    except ValueError:
        n = 1200
    sql = generate_products(n)
    out = __file__.replace("generate_seed_data.py", "").replace("scripts", "bigquery") + "../bigquery/dim_product_generated.sql"
    from pathlib import Path
    out_path = Path(__file__).parent.parent / "bigquery" / "dim_product_generated.sql"
    out_path.write_text(sql, encoding="utf-8")
    print(f"Written {out_path}")


if __name__ == "__main__":
    main()
