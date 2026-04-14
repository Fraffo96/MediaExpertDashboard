"""Verifica rapida post-seed su BigQuery (conteggi, duplicati product_id). Da root: PYTHONPATH=. python scripts/verify_seed_bq.py"""
from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv
from google.cloud import bigquery

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")
PROJECT = os.environ.get("GCP_PROJECT_ID", "mediaexpertdashboard")
c = bigquery.Client(project=PROJECT)


def main() -> None:
    q1 = f"""
    SELECT COUNT(*) AS n_rows, COUNT(DISTINCT product_id) AS n_distinct
    FROM `{PROJECT}.mart.dim_product`
    """
    q2 = f"""
    SELECT COUNT(*) AS n_duplicate_id_groups
    FROM (
      SELECT product_id, COUNT(*) AS c
      FROM `{PROJECT}.mart.dim_product`
      GROUP BY 1
      HAVING c > 1
    )
    """
    q3 = f"SELECT COUNT(*) AS n_lines FROM `{PROJECT}.mart.fact_order_items`"
    q4 = f"SELECT COUNT(*) AS n_rows FROM `{PROJECT}.mart.precalc_sales_agg`"
    for label, sql in [
        ("dim_product", q1),
        ("dim_product duplicate product_id groups", q2),
        ("fact_order_items lines", q3),
        ("precalc_sales_agg rows", q4),
    ]:
        row = list(c.query(sql).result())[0]
        print(label, dict(row))


if __name__ == "__main__":
    main()
