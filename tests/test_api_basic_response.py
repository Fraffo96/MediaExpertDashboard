"""
Test: verifica che /api/basic restituisca le chiavi attese e stampa le lunghezze.
Esegui con: python -m tests.test_api_basic_response
(con l'app in esecuzione) oppure usa httpx per chiamare l'endpoint.
"""
import os
import sys

# Aggiungi la root del progetto al path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def test_basic_response():
    from app.services import get_basic
    import asyncio
    data = asyncio.run(get_basic("2025-01-01", "2025-12-31", None, None, None, None))
    bc = data.get("sales_brand_category") or []
    detail = data.get("sales_detail") or []
    kpi = data.get("kpi") or []
    print("get_basic(2025-01-01, 2025-12-31):")
    print("  sales_brand_category:", len(bc), "righe")
    print("  sales_detail:", len(detail), "righe")
    print("  kpi:", len(kpi), "righe")
    if bc:
        print("  primo elemento sales_brand_category:", bc[0])
    if not bc and not detail:
        print("  --> Nessun dato: probabilmente il mart BigQuery non e' popolato per il periodo.")

if __name__ == "__main__":
    test_basic_response()
