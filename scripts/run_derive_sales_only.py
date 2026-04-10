"""
Esegue solo bigquery/derive_sales_from_orders.sql (ricrea v_sales, fact_sales_daily, fact_promo_performance).
Uso: python scripts/run_derive_sales_only.py
Richiede: ADC / service account, GCP_PROJECT_ID (default mediaexpertdashboard).
"""
from __future__ import annotations

import os
import re
import sys
import time
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(ROOT))

try:
    from dotenv import load_dotenv

    load_dotenv(ROOT / ".env")
except ImportError:
    pass

os.environ.setdefault("GCP_PROJECT_ID", "mediaexpertdashboard")

from google.cloud import bigquery

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "mediaexpertdashboard")
DERIVE_FILE = ROOT / "bigquery" / "derive_sales_from_orders.sql"


def split_sql(content: str) -> list[str]:
    lines = []
    for line in content.splitlines():
        s = line.strip()
        if s.startswith("--") or not s:
            continue
        lines.append(line)
    text = "\n".join(lines)
    parts = re.split(r";\s*\n", text)
    return [p.strip() for p in parts if p.strip()]


def main() -> None:
    if not DERIVE_FILE.exists():
        print(f"File non trovato: {DERIVE_FILE}", file=sys.stderr)
        sys.exit(1)
    client = bigquery.Client(project=PROJECT_ID)
    stmts = split_sql(DERIVE_FILE.read_text(encoding="utf-8"))
    print(f"Derive: {len(stmts)} statement, progetto {PROJECT_ID}")
    for i, stmt in enumerate(stmts):
        if not stmt.strip():
            continue
        t0 = time.time()
        job = client.query(stmt)
        job.result()
        preview = stmt[:70].replace("\n", " ") + "..." if len(stmt) > 70 else stmt.replace("\n", " ")
        print(f"  OK [{i + 1}] {preview} ({time.time() - t0:.1f}s)")
    print("Derive completato.")


if __name__ == "__main__":
    main()
