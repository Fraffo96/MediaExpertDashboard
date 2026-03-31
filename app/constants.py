"""Costanti condivise: periodo default, fallback admin quando BigQuery non disponibile."""
# Periodo default per le dashboard
DP = ("2023-01-01", "2025-12-31")
# Promo Creator: usa ultimo anno completo per benchmark (no periodo nel form)
PC_DEFAULT_PERIOD = ("2024-01-01", "2024-12-31")
# Marketing: anno intero per usare precalc (top categories/SKUs per segment)
MKT_DEFAULT_PERIOD = ("2024-01-01", "2024-12-31")

# Check Live Promo: ultima data presente nel seed (preset = min(oggi, questo))
CLP_DATA_MAX_DATE = "2025-12-31"

# Fallback per admin quando BigQuery non disponibile
ADMIN_CATEGORIES = [
    {"category_id": 1, "category_name": "TV & Home Entertainment", "level": 1},
    {"category_id": 2, "category_name": "Mobile and smartwatches", "level": 1},
    {"category_id": 3, "category_name": "Computers & IT", "level": 1},
    {"category_id": 4, "category_name": "Gaming", "level": 1},
    {"category_id": 5, "category_name": "Large Appliances", "level": 1},
    {"category_id": 6, "category_name": "Small Appliances", "level": 1},
    {"category_id": 7, "category_name": "Audio", "level": 1},
    {"category_id": 8, "category_name": "Smart Home", "level": 1},
    {"category_id": 9, "category_name": "Health & Beauty Tech", "level": 1},
    {"category_id": 10, "category_name": "Photo & Video", "level": 1},
]

ADMIN_SUBCATEGORIES = [
    {"category_id": 101, "category_name": "LED TV", "parent_category_id": 1},
    {"category_id": 102, "category_name": "OLED TV", "parent_category_id": 1},
    {"category_id": 103, "category_name": "QLED TV", "parent_category_id": 1},
    {"category_id": 104, "category_name": "Mini LED TV", "parent_category_id": 1},
    {"category_id": 105, "category_name": "Soundbars", "parent_category_id": 1},
    {"category_id": 106, "category_name": "Home cinema systems", "parent_category_id": 1},
    {"category_id": 107, "category_name": "Projectors", "parent_category_id": 1},
    {"category_id": 108, "category_name": "Streaming devices", "parent_category_id": 1},
    {"category_id": 201, "category_name": "Smartphones flagship", "parent_category_id": 2},
    {"category_id": 202, "category_name": "Smartphones mid-range", "parent_category_id": 2},
    {"category_id": 203, "category_name": "Smartphones entry", "parent_category_id": 2},
    {"category_id": 204, "category_name": "Foldable smartphones", "parent_category_id": 2},
    {"category_id": 205, "category_name": "Tablets", "parent_category_id": 2},
    {"category_id": 206, "category_name": "Smartwatches", "parent_category_id": 2},
    {"category_id": 207, "category_name": "Fitness trackers", "parent_category_id": 2},
    {"category_id": 208, "category_name": "Phone accessories", "parent_category_id": 2},
    {"category_id": 301, "category_name": "Laptops", "parent_category_id": 3},
    {"category_id": 302, "category_name": "Gaming laptops", "parent_category_id": 3},
    {"category_id": 303, "category_name": "Desktop PCs", "parent_category_id": 3},
    {"category_id": 304, "category_name": "Monitors", "parent_category_id": 3},
    {"category_id": 305, "category_name": "Keyboards", "parent_category_id": 3},
    {"category_id": 306, "category_name": "Mice", "parent_category_id": 3},
    {"category_id": 307, "category_name": "Webcams", "parent_category_id": 3},
    {"category_id": 308, "category_name": "External storage", "parent_category_id": 3},
    {"category_id": 309, "category_name": "Routers", "parent_category_id": 3},
    {"category_id": 310, "category_name": "Mesh WiFi systems", "parent_category_id": 3},
    {"category_id": 401, "category_name": "Consoles", "parent_category_id": 4},
    {"category_id": 402, "category_name": "Gaming PCs", "parent_category_id": 4},
    {"category_id": 403, "category_name": "Gaming laptops", "parent_category_id": 4},
    {"category_id": 404, "category_name": "Controllers", "parent_category_id": 4},
    {"category_id": 405, "category_name": "Gaming headsets", "parent_category_id": 4},
    {"category_id": 406, "category_name": "Gaming keyboards", "parent_category_id": 4},
    {"category_id": 407, "category_name": "Gaming mice", "parent_category_id": 4},
    {"category_id": 408, "category_name": "VR headsets", "parent_category_id": 4},
    {"category_id": 501, "category_name": "Refrigerators", "parent_category_id": 5},
    {"category_id": 502, "category_name": "Washing machines", "parent_category_id": 5},
    {"category_id": 503, "category_name": "Dryers", "parent_category_id": 5},
    {"category_id": 504, "category_name": "Dishwashers", "parent_category_id": 5},
    {"category_id": 505, "category_name": "Ovens", "parent_category_id": 5},
    {"category_id": 506, "category_name": "Induction hobs", "parent_category_id": 5},
    {"category_id": 507, "category_name": "Built-in appliances", "parent_category_id": 5},
    {"category_id": 508, "category_name": "Freezers", "parent_category_id": 5},
    {"category_id": 601, "category_name": "Coffee machines", "parent_category_id": 6},
    {"category_id": 602, "category_name": "Blenders", "parent_category_id": 6},
    {"category_id": 603, "category_name": "Air fryers", "parent_category_id": 6},
    {"category_id": 604, "category_name": "Vacuum cleaners", "parent_category_id": 6},
    {"category_id": 605, "category_name": "Robot vacuums", "parent_category_id": 6},
    {"category_id": 606, "category_name": "Kitchen processors", "parent_category_id": 6},
    {"category_id": 607, "category_name": "Electric kettles", "parent_category_id": 6},
    {"category_id": 608, "category_name": "Toasters", "parent_category_id": 6},
    {"category_id": 701, "category_name": "Wireless headphones", "parent_category_id": 7},
    {"category_id": 702, "category_name": "Noise cancelling headphones", "parent_category_id": 7},
    {"category_id": 703, "category_name": "Earbuds", "parent_category_id": 7},
    {"category_id": 704, "category_name": "Portable speakers", "parent_category_id": 7},
    {"category_id": 705, "category_name": "Hi-Fi systems", "parent_category_id": 7},
    {"category_id": 706, "category_name": "DJ equipment", "parent_category_id": 7},
    {"category_id": 801, "category_name": "Smart speakers", "parent_category_id": 8},
    {"category_id": 802, "category_name": "Smart lighting", "parent_category_id": 8},
    {"category_id": 803, "category_name": "Smart thermostats", "parent_category_id": 8},
    {"category_id": 804, "category_name": "Security cameras", "parent_category_id": 8},
    {"category_id": 805, "category_name": "Smart locks", "parent_category_id": 8},
    {"category_id": 806, "category_name": "Smart plugs", "parent_category_id": 8},
    {"category_id": 901, "category_name": "Electric toothbrushes", "parent_category_id": 9},
    {"category_id": 902, "category_name": "Hair dryers", "parent_category_id": 9},
    {"category_id": 903, "category_name": "Hair straighteners", "parent_category_id": 9},
    {"category_id": 904, "category_name": "Grooming kits", "parent_category_id": 9},
    {"category_id": 905, "category_name": "Smart scales", "parent_category_id": 9},
    {"category_id": 1001, "category_name": "Cameras", "parent_category_id": 10},
    {"category_id": 1002, "category_name": "Mirrorless cameras", "parent_category_id": 10},
    {"category_id": 1003, "category_name": "Lenses", "parent_category_id": 10},
    {"category_id": 1004, "category_name": "Action cameras", "parent_category_id": 10},
    {"category_id": 1005, "category_name": "Drones", "parent_category_id": 10},
]

ADMIN_BRANDS = [
    {"brand_id": i, "brand_name": n}
    for i, n in [
        (1, "Samsung"), (2, "LG"), (3, "Sony"), (4, "Philips"), (5, "TCL"), (6, "Hisense"), (7, "Panasonic"),
        (8, "Apple"), (9, "Xiaomi"), (10, "Oppo"), (11, "Realme"), (12, "Huawei"), (13, "Motorola"), (14, "OnePlus"),
        (15, "Garmin"), (16, "Dell"), (17, "HP"), (18, "Lenovo"), (19, "Asus"), (20, "Acer"), (21, "MSI"),
        (22, "Logitech"), (23, "TP-Link"), (24, "Microsoft"), (25, "Nintendo"), (26, "Razer"), (27, "SteelSeries"),
        (28, "HyperX"), (29, "Bosch"), (30, "Siemens"), (31, "Whirlpool"), (32, "Beko"), (33, "Electrolux"),
        (34, "Amica"), (35, "Tefal"), (36, "Dyson"), (37, "DeLonghi"), (38, "Krups"), (39, "Google"), (40, "Amazon"),
        (41, "Ring"), (42, "Bose"), (43, "JBL"), (44, "Marshall"), (45, "Beats"), (46, "Sennheiser"), (47, "Braun"),
        (48, "Oral-B"), (49, "Canon"), (50, "Nikon"), (51, "GoPro"), (52, "DJI"), (53, "Remington"), (54, "Withings"),
        (55, "Fitbit"),
    ]
]
