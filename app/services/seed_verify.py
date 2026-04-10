"""Verifica post-seed su BigQuery (solo metriche leggere)."""
from __future__ import annotations

import os
from typing import Any


def bq_seed_verify_report(dataset: str = "mart") -> dict[str, Any]:
    project = (
        (os.environ.get("GCP_PROJECT_ID") or os.environ.get("GOOGLE_CLOUD_PROJECT") or "").strip()
        or "mediaexpertdashboard"
    )
    from google.cloud import bigquery

    client = bigquery.Client(project=project)

    q_seg = f"""
SELECT segment_id, COUNT(*) AS c
FROM `{project}.{dataset}.dim_customer`
GROUP BY segment_id
ORDER BY segment_id
"""
    q_promo = f"""
SELECT
  COUNT(*) AS orders,
  COUNTIF(promo_flag) AS promo_orders
FROM `{project}.{dataset}.fact_orders`
"""
    rows = list(client.query(q_seg).result())
    total_c = sum(int(r["c"]) for r in rows) or 1
    seg_share = {str(int(r["segment_id"])): round(float(r["c"]) / total_c, 6) for r in rows}
    pr = list(client.query(q_promo).result())
    promo_rate = None
    if pr:
        o = int(pr[0]["orders"] or 0)
        p = int(pr[0]["promo_orders"] or 0)
        promo_rate = round(p / o, 6) if o else None
    return {
        "project_id": project,
        "dataset": dataset,
        "segment_shares_observed": seg_share,
        "customer_rows": total_c,
        "orders_promo_rate": promo_rate,
    }
