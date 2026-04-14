#!/usr/bin/env python3
"""
Genera dati seed per Media Expert Dashboard.

Output: bigquery/dim_product_generated.sql (INSERT mart.dim_product, ~SEED_NUM_PRODUCTS righe).
Mantiene brand 1–55, category 1–10, subcategory ids, allineati allo schema mart.

Logica: scripts/seed_catalog/ (JSON pesi × market_reality.py con fonti in docs/SEED_MARKET_RESEARCH.md).
Pesi SQL (clienti, segmenti, promo, pool): docs/SEED_PIPELINE_AND_WEIGHTS.md e bigquery/schema_and_seed.sql.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

from seed_catalog.dim_product_sql import generate_products  # noqa: E402


def main() -> None:
    try:
        n = int(os.environ.get("SEED_NUM_PRODUCTS", "1200"))
    except ValueError:
        n = 1200
    sql = generate_products(n)
    out_path = SCRIPT_DIR.parent / "bigquery" / "dim_product_generated.sql"
    out_path.write_text(sql, encoding="utf-8")
    print(f"Written {out_path}")


if __name__ == "__main__":
    main()
