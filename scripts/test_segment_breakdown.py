"""
Test segment breakdown API - verifica che la query restituisca dati.
Esegui: python scripts/test_segment_breakdown.py

Richiede: BigQuery configurato (gcloud auth application-default login).
"""
import os
import sys

os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.getcwd())

from app.db.queries.check_live_promo import (
    query_segment_breakdown_for_product,
    query_segment_breakdown_aggregate,
)


def main():
    brand_id = 1
    date_start = "2025-12-25"
    date_end = "2025-12-31"
    product_id = 11134  # SKU dall'immagine

    print("=" * 60)
    print("TEST SEGMENT BREAKDOWN")
    print("=" * 60)
    print(f"Brand: {brand_id}, Date: {date_start} - {date_end}")
    print()

    print("1. Per product_id", product_id, ":")
    try:
        rows = query_segment_breakdown_for_product(
            product_id, brand_id, date_start, date_end,
            promo_id=None, category_id=None, channel=None,
        )
        print(f"   Righe: {len(rows)}")
        for r in rows[:5]:
            print(f"   - {r}")
        if not rows:
            print("   (nessun risultato)")
    except Exception as e:
        print(f"   ERRORE: {e}")

    print()
    print("2. Aggregate (tutti i prodotti):")
    try:
        rows = query_segment_breakdown_aggregate(
            brand_id, date_start, date_end,
            promo_id=None, category_id=None, channel=None,
        )
        print(f"   Righe: {len(rows)}")
        for r in rows[:5]:
            print(f"   - {r}")
        if not rows:
            print("   (nessun risultato)")
    except Exception as e:
        print(f"   ERRORE: {e}")

    print()
    print("Fine test.")


if __name__ == "__main__":
    main()
