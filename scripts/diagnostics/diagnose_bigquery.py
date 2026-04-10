"""
Script di diagnostica BigQuery – individua perché i chart sono vuoti.
Esegui: python scripts/diagnostics/diagnose_bigquery.py

Possibili cause:
1. Credenziali GCP non configurate (GOOGLE_APPLICATION_CREDENTIALS)
2. Progetto/dataset/tabelle inesistenti
3. Tabelle vuote
4. Schema non allineato alle query
"""
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

# Carica .env prima di qualsiasi import che usa variabili d'ambiente
from dotenv import load_dotenv
load_dotenv()


def main():
    print("=" * 60)
    print("DIAGNOSTICA BIGQUERY – Media Expert Dashboard")
    print("=" * 60)

    # 1. Variabili d'ambiente
    print("\n1. VARIABILI D'AMBIENTE")
    project = os.environ.get("GCP_PROJECT_ID", "mediaexpertdashboard")
    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "(non impostato)")
    print(f"   GCP_PROJECT_ID: {project}")
    print(f"   GOOGLE_APPLICATION_CREDENTIALS: {creds_path}")
    if creds_path == "(non impostato)":
        print("   [!] GOOGLE_APPLICATION_CREDENTIALS non impostato - BigQuery non puo' autenticarsi")
        print("      Imposta il path al file JSON delle credenziali di servizio GCP")
    elif not os.path.isfile(creds_path):
        print(f"   [!] Il file {creds_path} non esiste")
    else:
        print(f"   [OK] File credenziali trovato")

    # 2. Connessione BigQuery
    print("\n2. CONNESSIONE BIGQUERY")
    try:
        from google.cloud import bigquery
        client = bigquery.Client(project=project)
        print(f"   [OK] Client BigQuery creato (progetto: {project})")
    except Exception as e:
        print(f"   [ERR] Errore: {e}")
        print("\n   Possibili soluzioni:")
        print("   - gcloud auth application-default login")
        print("   - Oppure: export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json")
        return 1

    # 3. Dataset mart
    print("\n3. DATASET mart")
    try:
        dataset = client.get_dataset(f"{project}.mart")
        print(f"   [OK] Dataset 'mart' trovato")
    except Exception as e:
        print(f"   [ERR] Errore: {e}")
        print("\n   Il dataset 'mart' non esiste o non e' accessibile.")
        print("   Verifica che il progetto contenga il dataset e che lo schema sia stato eseguito.")
        return 1

    # 4. Tabelle richieste
    print("\n4. TABELLE RICHIESTE")
    tables = ["dim_brand", "dim_category", "fact_sales_daily"]
    for t in tables:
        try:
            client.get_table(f"{project}.mart.{t}")
            print(f"   [OK] mart.{t}")
        except Exception as e:
            print(f"   [ERR] mart.{t}: {e}")

    # 5. Query di test – brands
    print("\n5. QUERY DI TEST – dim_brand")
    try:
        from app.db.client import run_query
        rows = run_query("SELECT brand_id, brand_name FROM mart.dim_brand ORDER BY brand_name LIMIT 5")
        if rows:
            print(f"   [OK] {len(rows)} brand trovati (es. {rows[0]})")
        else:
            print("   [!] Query OK ma 0 righe - tabella vuota?")
    except Exception as e:
        print(f"   [ERR] Errore: {e}")
        import traceback
        traceback.print_exc()

    # 6. Query di test – fact_sales_daily (Samsung brand_id=1)
    print("\n6. QUERY DI TEST – fact_sales_daily (brand_id=1, Samsung)")
    try:
        from app.db.client import run_query
        q = """
        SELECT COUNT(*) AS cnt, MIN(date) AS min_date, MAX(date) AS max_date
        FROM mart.fact_sales_daily
        WHERE brand_id = 1
          AND date BETWEEN '2023-01-01' AND '2025-12-31'
        """
        rows = run_query(q)
        if rows and rows[0].get("cnt", 0) > 0:
            r = rows[0]
            print(f"   [OK] {r.get('cnt')} righe per Samsung, date {r.get('min_date')} - {r.get('max_date')}")
        else:
            print("   [!] 0 righe per brand_id=1 nel periodo 2023-2025")
            print("      Verifica che fact_sales_daily contenga dati per Samsung")
    except Exception as e:
        print(f"   [ERR] Errore: {e}")
        import traceback
        traceback.print_exc()

    # 7. Market Intelligence – query reale
    print("\n7. MARKET INTELLIGENCE – query sales_value")
    try:
        from app.db.queries.market_intelligence import query_sales_value_brand_vs_media
        rows = query_sales_value_brand_vs_media("2023-01-01", "2025-12-31", 1, cat=1, subcat=None)
        if rows:
            print(f"   [OK] {len(rows)} righe restituite")
        else:
            print("   [!] Query OK ma 0 righe")
    except Exception as e:
        print(f"   [ERR] Errore: {e}")
        import traceback
        traceback.print_exc()

    print("\n" + "=" * 60)
    print("Fine diagnostica")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
