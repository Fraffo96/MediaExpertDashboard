"""
Esegue bigquery/schema_and_seed.sql su BigQuery (un statement alla volta).
Richiede: gcloud auth application-default login e dataset raw/mart già creati.
"""
import os
import re
import sys
import time
from pathlib import Path

os.environ.setdefault("GCP_PROJECT_ID", "mediaexpertdashboard")

from google.cloud import bigquery

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "mediaexpertdashboard")
SCRIPT_DIR = Path(__file__).resolve().parent
SQL_FILE = SCRIPT_DIR.parent / "bigquery" / "schema_and_seed.sql"


def apply_seed_numeric_overrides(content: str) -> str:
    """Sostituisce i valori default ordini/clienti nel seed (env SEED_NUM_ORDERS / SEED_NUM_CUSTOMERS)."""
    try:
        n_c = int(os.environ.get("SEED_NUM_CUSTOMERS", "24000"))
        n_o = int(os.environ.get("SEED_NUM_ORDERS", "380000"))
    except ValueError:
        return content
    if n_c == 24000 and n_o == 380000:
        return content
    c = content.replace("GENERATE_ARRAY(1, 24000)", f"GENERATE_ARRAY(1, {n_c})")
    c = c.replace("GENERATE_ARRAY(1, 380000)", f"GENERATE_ARRAY(1, {n_o})")
    c = c.replace(
        "MOD(ABS(FARM_FINGERPRINT(CONCAT('c', CAST(od.i AS STRING)))), 24000)",
        f"MOD(ABS(FARM_FINGERPRINT(CONCAT('c', CAST(od.i AS STRING)))), {n_c})",
    )
    return c
DIM_PRODUCT_FILE = SCRIPT_DIR.parent / "bigquery" / "dim_product_generated.sql"
DERIVE_FILE = SCRIPT_DIR.parent / "bigquery" / "derive_sales_from_orders.sql"
MATERIALIZED_VIEWS_FILE = SCRIPT_DIR.parent / "bigquery" / "materialized_views.sql"
PRECALC_DDL_FILE = SCRIPT_DIR.parent / "bigquery" / "precalc_tables.sql"


def extract_raw_blocks(content: str) -> dict[str, str]:
    """Estrae blocchi INSERT...WITH dal file raw (con commenti) per statement che li richiedono."""
    blocks = {}

    # dim_customer: INSERT mart.dim_customer ... FROM gen;
    m = re.search(r"(INSERT\s+mart\.dim_customer\b.*?FROM\s+gen\s*;)", content, re.DOTALL)
    if m:
        blocks["dim_customer"] = m.group(1).strip()

    # fact_orders: INSERT mart.fact_orders ... FROM gen;
    m = re.search(r"(INSERT\s+mart\.fact_orders\b.*?FROM\s+gen\s*;)", content, re.DOTALL)
    if m:
        blocks["fact_orders"] = m.group(1).strip()

    # fact_order_items: INSERT mart.fact_order_items ... FROM with_price;
    m = re.search(r"(INSERT\s+mart\.fact_order_items\b.*?FROM\s+with_price\s*;)", content, re.DOTALL)
    if m:
        blocks["fact_order_items"] = m.group(1).strip()

    # fact_sales_daily e fact_promo_performance: derivati da derive_sales_from_orders.sql
    return blocks


def split_sql(content: str) -> list[str]:
    """Split SQL in singoli statement. Rimuove commenti -- a inizio riga, poi split per ';'."""
    lines = []
    for line in content.splitlines():
        s = line.strip()
        if s.startswith("--") or not s:
            continue
        lines.append(line)
    text = "\n".join(lines)
    parts = re.split(r";\s*\n", text)
    return [p.strip() for p in parts if p.strip()]


def match_raw_block(stmt: str, raw_blocks: dict[str, str]) -> str | None:
    """Se lo statement è un INSERT+WITH per una tabella nota, restituisce il blocco raw."""
    if "WITH" not in stmt or not stmt.strip().startswith("INSERT"):
        return None
    for table, block in raw_blocks.items():
        marker = f"INSERT mart.{table}"
        if marker.replace("_", "_") in stmt.replace("\n", " "):
            return block
    return None


def main():
    if not SQL_FILE.exists():
        print(f"File non trovato: {SQL_FILE}", file=sys.stderr)
        sys.exit(1)
    content = apply_seed_numeric_overrides(SQL_FILE.read_text(encoding="utf-8"))
    statements = split_sql(content)
    raw_blocks = extract_raw_blocks(content)
    client = bigquery.Client(project=PROJECT_ID)
    ok = 0
    used_blocks = set()

    print(f"Trovati {len(statements)} statement, {len(raw_blocks)} blocchi INSERT+CTE raw.")

    for i, stmt in enumerate(statements):
        if not stmt.strip():
            continue

        raw = match_raw_block(stmt, raw_blocks)
        if raw:
            table = [k for k, v in raw_blocks.items() if v == raw][0]
            if table in used_blocks:
                continue
            used_blocks.add(table)
            t0 = time.time()
            try:
                job = client.query(raw)
                job.result()
                elapsed = time.time() - t0
                ok += 1
                print(f"  OK [{ok}] INSERT mart.{table} (blocco raw, {elapsed:.1f}s)")
            except Exception as e:
                print(f"  ERRORE su INSERT mart.{table}: {e}", file=sys.stderr)
                sys.exit(1)
            continue

        t0 = time.time()
        try:
            job = client.query(stmt)
            job.result()
            elapsed = time.time() - t0
            ok += 1
            preview = stmt[:60].replace("\n", " ") + "..." if len(stmt) > 60 else stmt.replace("\n", " ")
            print(f"  OK [{ok}] {preview} ({elapsed:.1f}s)")
        except Exception as e:
            print(f"  ERRORE su statement {i+1}: {e}", file=sys.stderr)
            print(f"  Preview: {stmt[:200]}...", file=sys.stderr)
            sys.exit(1)

        # Subito dopo CREATE TABLE mart.dim_product: popola con dim_product_generated.sql
        if "CREATE" in stmt and "mart.dim_product" in stmt and DIM_PRODUCT_FILE.exists():
            t0 = time.time()
            try:
                job = client.query(DIM_PRODUCT_FILE.read_text(encoding="utf-8"))
                job.result()
                ok += 1
                print(f"  OK [{ok}] INSERT mart.dim_product ({time.time() - t0:.1f}s)")
            except Exception as e:
                print(f"  ERRORE dim_product: {e}", file=sys.stderr)
                sys.exit(1)

    # Fase 2: deriva fact_sales_daily dagli ordini (consistenza + channel)
    if DERIVE_FILE.exists():
        print("\n--- Fase 2: derive_sales_from_orders ---")
        derive_content = DERIVE_FILE.read_text(encoding="utf-8")
        derive_stmts = split_sql(derive_content)
        for i, stmt in enumerate(derive_stmts):
            if not stmt.strip():
                continue
            t0 = time.time()
            try:
                job = client.query(stmt)
                job.result()
                elapsed = time.time() - t0
                ok += 1
                preview = stmt[:60].replace("\n", " ") + "..." if len(stmt) > 60 else stmt.replace("\n", " ")
                print(f"  OK [{ok}] {preview} ({elapsed:.1f}s)")
            except Exception as e:
                print(f"  ERRORE derive statement {i+1}: {e}", file=sys.stderr)
                sys.exit(1)

    # Fase 3: materialized views (opzionale, accelera query full-year)
    if MATERIALIZED_VIEWS_FILE.exists():
        print("\n--- Fase 3: materialized_views ---")
        mv_content = MATERIALIZED_VIEWS_FILE.read_text(encoding="utf-8")
        mv_stmts = split_sql(mv_content)
        for i, stmt in enumerate(mv_stmts):
            if not stmt.strip():
                continue
            t0 = time.time()
            try:
                job = client.query(stmt)
                job.result()
                elapsed = time.time() - t0
                ok += 1
                preview = stmt[:60].replace("\n", " ") + "..." if len(stmt) > 60 else stmt.replace("\n", " ")
                print(f"  OK [{ok}] {preview} ({elapsed:.1f}s)")
            except Exception as e:
                print(f"  ERRORE materialized view {i+1}: {e}", file=sys.stderr)
                sys.exit(1)

    # Fase 4: DDL tabelle precalcolate (schema vuoto; popolamento con refresh_precalc_tables.py)
    if PRECALC_DDL_FILE.exists():
        print("\n--- Fase 4: precalc_tables (DDL) ---")
        precalc_content = PRECALC_DDL_FILE.read_text(encoding="utf-8")
        precalc_stmts = split_sql(precalc_content)
        for i, stmt in enumerate(precalc_stmts):
            if not stmt.strip():
                continue
            t0 = time.time()
            try:
                job = client.query(stmt)
                job.result()
                elapsed = time.time() - t0
                ok += 1
                preview = stmt[:60].replace("\n", " ") + "..." if len(stmt) > 60 else stmt.replace("\n", " ")
                print(f"  OK [{ok}] {preview} ({elapsed:.1f}s)")
            except Exception as e:
                print(f"  ERRORE precalc DDL {i+1}: {e}", file=sys.stderr)
                sys.exit(1)
        print("  (Popolamento: python scripts/refresh_precalc_tables.py)")

    print(f"\nCompletato: {ok} statement eseguiti su BigQuery (progetto {PROJECT_ID}).")


if __name__ == "__main__":
    main()
