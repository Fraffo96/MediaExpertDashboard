"""Esecuzione sequenza CREATE OR REPLACE precalc_* su BigQuery."""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

from dotenv import load_dotenv
from google.cloud import bigquery

from precalc_refresh.sql_steps import build_sql_steps

# repo root: .../scripts/precalc_refresh/runner.py -> parents[2] == repo
REPO_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(REPO_ROOT / ".env")

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "mediaexpertdashboard")
DATASET = "mart"


def run_query(client: bigquery.Client, sql: str, description: str) -> bool:
    try:
        t0 = time.time()
        job = client.query(sql, project=PROJECT_ID)
        job.result(timeout=300)
        elapsed = time.time() - t0
        print(f"  OK {description} ({elapsed:.1f}s)")
        return True
    except Exception as e:
        print(f"  ERRORE {description}: {e}", file=sys.stderr)
        return False


def main() -> None:
    client = bigquery.Client(project=PROJECT_ID)
    print(f"Refresh tabelle precalcolate su {PROJECT_ID}.{DATASET}")
    print("-" * 50)
    for label, sql in build_sql_steps(DATASET, PROJECT_ID):
        if not run_query(client, sql, label):
            sys.exit(1)
    print("-" * 50)
    print("Refresh completato.")


if __name__ == "__main__":
    main()
