"""
Misura durata delle 3 query Promo Creator tipiche (subcat + tipo + sconto).
Dry-run per bytes su slice fact_sales_daily.

Uso: python scripts/diagnostics/benchmark_promo_creator_queries.py
Richiede: ADC, GCP_PROJECT_ID, dataset mart popolato.
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

try:
    from dotenv import load_dotenv

    load_dotenv(ROOT / ".env")
except ImportError:
    pass

os.environ.setdefault("GCP_PROJECT_ID", "mediaexpertdashboard")

from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPICallError
from google.cloud.bigquery import QueryJobConfig, ScalarQueryParameter

from app.db.queries import promo_creator
from app.db.queries.precalc.misc import query_promo_creator_subcat_from_precalc


def _run_bench(label: str, fn) -> None:
    t0 = time.time()
    try:
        out = fn()
        print(f"{label}: {time.time() - t0:.2f}s  n_rows={len(out) if out else 0}")
    except GoogleAPICallError as e:
        msg = getattr(e, "message", None) or str(e)
        print(f"{label}: ERRORE ({time.time() - t0:.2f}s) - {msg}")


def main() -> None:
    client = bigquery.Client(project=os.environ.get("GCP_PROJECT_ID", "mediaexpertdashboard"))
    ps, pe = "2024-01-01", "2024-12-31"
    brand_id = 1
    cat, subcat = "3", "301"
    promo_type = "bundle"
    discount_depth = "10"

    print(f"Progetto: {client.project}")
    print(f"Parametri: ps={ps} pe={pe} cat={cat} subcat={subcat} pt={promo_type} dd={discount_depth}\n")

    for label, fn in [
        (
            "0_precalc_subcat",
            lambda: query_promo_creator_subcat_from_precalc(
                int(ps[:4]), int(cat), int(subcat), promo_type, float(discount_depth)
            ),
        ),
        ("1_discount_bench", lambda: promo_creator.query_category_discount_benchmark(ps, pe, brand_id, cat, subcat)),
        (
            "2_roi_merged",
            lambda: promo_creator.query_roi_and_top_competitor_discount_subcat(
                ps, pe, promo_type, int(subcat), discount_depth, int(brand_id)
            ),
        ),
        (
            "3_segments",
            lambda: promo_creator.query_segment_promo_responsiveness(ps, pe, cat, subcat, promo_type),
        ),
    ]:
        _run_bench(label, fn)

    q = """
    SELECT 1 FROM mart.fact_sales_daily f
    WHERE f.date BETWEEN @ps AND @pe AND f.parent_category_id = @cat AND f.category_id = @subcat
    LIMIT 1
    """
    cfg = QueryJobConfig(
        query_parameters=[
            ScalarQueryParameter("ps", "DATE", "2024-01-01"),
            ScalarQueryParameter("pe", "DATE", "2024-12-31"),
            ScalarQueryParameter("cat", "INT64", int(cat)),
            ScalarQueryParameter("subcat", "INT64", int(subcat)),
        ],
        dry_run=True,
        use_query_cache=False,
    )
    dj = client.query(q, job_config=cfg)
    print(f"\nDry-run sample scan subcat 2024: total_bytes_processed={dj.total_bytes_processed}")

    redis_url = (os.environ.get("REDIS_URL") or "").strip()
    print(f"REDIS_URL in .env locale: {'impostato' if redis_url else 'vuoto (verificare env Cloud Run)'}")


if __name__ == "__main__":
    main()
