"""
Refresh tabelle precalcolate su BigQuery.
Esegue CREATE OR REPLACE TABLE per ogni tabella precalc, popolando da fact_sales_daily,
v_sales_daily_by_channel e fact_promo_performance.

Uso: python scripts/refresh_precalc_tables.py
Richiede: gcloud auth application-default login, GCP_PROJECT_ID (default: mediaexpertdashboard)
"""
import os
import sys
import time
from pathlib import Path

os.environ.setdefault("GCP_PROJECT_ID", "mediaexpertdashboard")

from dotenv import load_dotenv
load_dotenv()

from google.cloud import bigquery

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "mediaexpertdashboard")
DATASET = "mart"


def run_query(client: bigquery.Client, sql: str, description: str) -> bool:
    """Esegue una query e ritorna True se ok."""
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


def main():
    client = bigquery.Client(project=PROJECT_ID)
    print(f"Refresh tabelle precalcolate su {PROJECT_ID}.{DATASET}")
    print("-" * 50)

    # 1. precalc_sales_agg: da fact_sales_daily (channel='') + v_sales_daily_by_channel (web, app, store)
    sql_sales = f"""
CREATE OR REPLACE TABLE {DATASET}.precalc_sales_agg
PARTITION BY RANGE_BUCKET(year, GENERATE_ARRAY(2023, 2026))
CLUSTER BY brand_id, parent_category_id, category_id, channel
AS
SELECT
  CAST(EXTRACT(YEAR FROM f.date) AS INT64) AS year,
  f.brand_id,
  f.brand_name,
  f.category_id,
  f.parent_category_id,
  CAST('' AS STRING) AS channel,
  SUM(f.gross_pln) AS gross_pln,
  SUM(f.units) AS units,
  SUM(CASE WHEN f.promo_flag THEN f.gross_pln ELSE 0 END) AS promo_gross,
  SUM(CASE WHEN f.promo_flag THEN f.discount_depth_pct * f.gross_pln ELSE 0 END) AS discount_depth_weighted
FROM mart.fact_sales_daily f
WHERE f.date IS NOT NULL
GROUP BY 1, 2, 3, 4, 5
UNION ALL
SELECT
  CAST(EXTRACT(YEAR FROM f.date) AS INT64) AS year,
  f.brand_id,
  f.brand_name,
  f.category_id,
  f.parent_category_id,
  f.channel,
  SUM(f.gross_pln) AS gross_pln,
  SUM(f.units) AS units,
  SUM(CASE WHEN f.promo_flag THEN f.gross_pln ELSE 0 END) AS promo_gross,
  SUM(CASE WHEN f.promo_flag THEN f.discount_depth_pct * f.gross_pln ELSE 0 END) AS discount_depth_weighted
FROM mart.v_sales_daily_by_channel f
WHERE f.date IS NOT NULL AND f.channel IN ('web', 'app', 'store')
GROUP BY 1, 2, 3, 4, 5, 6
"""
    if not run_query(client, sql_sales, "precalc_sales_agg"):
        sys.exit(1)

    # 2. precalc_peak_agg: da fact_sales_daily + dim_date
    sql_peak = f"""
CREATE OR REPLACE TABLE {DATASET}.precalc_peak_agg
PARTITION BY RANGE_BUCKET(year, GENERATE_ARRAY(2023, 2026))
CLUSTER BY brand_id, parent_category_id, peak_event
AS
SELECT
  CAST(EXTRACT(YEAR FROM f.date) AS INT64) AS year,
  f.brand_id,
  f.category_id,
  f.parent_category_id,
  CAST('' AS STRING) AS channel,
  d.peak_event,
  SUM(f.gross_pln) AS gross_pln
FROM mart.fact_sales_daily f
JOIN mart.dim_date d ON d.date = f.date
WHERE f.date IS NOT NULL AND d.peak_event IS NOT NULL
GROUP BY 1, 2, 3, 4, 5, 6
UNION ALL
SELECT
  CAST(EXTRACT(YEAR FROM f.date) AS INT64) AS year,
  f.brand_id,
  f.category_id,
  f.parent_category_id,
  f.channel,
  d.peak_event,
  SUM(f.gross_pln) AS gross_pln
FROM mart.v_sales_daily_by_channel f
JOIN mart.dim_date d ON d.date = f.date
WHERE f.date IS NOT NULL AND f.channel IN ('web', 'app', 'store') AND d.peak_event IS NOT NULL
GROUP BY 1, 2, 3, 4, 5, 6
"""
    if not run_query(client, sql_peak, "precalc_peak_agg"):
        sys.exit(1)

    # 3. precalc_roi_agg: da fact_promo_performance
    sql_roi = f"""
CREATE OR REPLACE TABLE {DATASET}.precalc_roi_agg
PARTITION BY RANGE_BUCKET(year, GENERATE_ARRAY(2023, 2026))
CLUSTER BY brand_id, category_id
AS
SELECT
  CAST(EXTRACT(YEAR FROM fp.date) AS INT64) AS year,
  fp.brand_id,
  fp.category_id,
  p.promo_type,
  ROUND(AVG(fp.roi), 4) AS avg_roi,
  SUM(fp.incremental_sales_pln) AS incremental_sales_pln
FROM mart.fact_promo_performance fp
JOIN mart.dim_promo p ON p.promo_id = fp.promo_id
WHERE fp.date IS NOT NULL
GROUP BY 1, 2, 3, 4
"""
    if not run_query(client, sql_roi, "precalc_roi_agg"):
        sys.exit(1)

    # 4. precalc_incremental_yoy: da fact_sales_daily + fact_promo_performance
    sql_inc = f"""
CREATE OR REPLACE TABLE {DATASET}.precalc_incremental_yoy
PARTITION BY RANGE_BUCKET(year, GENERATE_ARRAY(2023, 2026))
CLUSTER BY brand_id, parent_category_id
AS
WITH yt AS (
  SELECT
    CAST(EXTRACT(YEAR FROM f.date) AS INT64) AS year,
    f.brand_id,
    f.parent_category_id,
    f.category_id,
    SUM(f.gross_pln) AS total_gross
  FROM mart.fact_sales_daily f
  WHERE f.date IS NOT NULL
  GROUP BY 1, 2, 3, 4
),
yp AS (
  SELECT
    CAST(EXTRACT(YEAR FROM fp.date) AS INT64) AS year,
    fp.brand_id,
    fp.category_id,
    SUM(fp.incremental_sales_pln) AS incremental_sales_pln
  FROM mart.fact_promo_performance fp
  WHERE fp.date IS NOT NULL
  GROUP BY 1, 2, 3
)
SELECT
  yt.year,
  yt.brand_id,
  yt.category_id,
  yt.parent_category_id,
  yt.total_gross,
  COALESCE(yp.incremental_sales_pln, 0) AS incremental_sales_pln
FROM yt
LEFT JOIN yp ON yp.year = yt.year AND yp.brand_id = yt.brand_id AND yp.category_id = yt.parent_category_id
"""
    if not run_query(client, sql_inc, "precalc_incremental_yoy"):
        sys.exit(1)

    # 5. precalc_pie_brands_category: pie per parent category
    sql_pie_cat = f"""
CREATE OR REPLACE TABLE {DATASET}.precalc_pie_brands_category
PARTITION BY RANGE_BUCKET(year, GENERATE_ARRAY(2023, 2026))
CLUSTER BY category_id, channel
AS
WITH base AS (
  SELECT
    CAST(EXTRACT(YEAR FROM f.date) AS INT64) AS year,
    f.parent_category_id AS category_id,
    f.brand_id,
    f.brand_name,
    CAST('' AS STRING) AS channel,
    SUM(f.gross_pln) AS gross_pln,
    SUM(f.units) AS units
  FROM mart.fact_sales_daily f
  WHERE f.date IS NOT NULL AND f.gross_pln > 0
  GROUP BY 1, 2, 3, 4, 5
  UNION ALL
  SELECT
    CAST(EXTRACT(YEAR FROM f.date) AS INT64) AS year,
    f.parent_category_id AS category_id,
    f.brand_id,
    f.brand_name,
    f.channel,
    SUM(f.gross_pln) AS gross_pln,
    SUM(f.units) AS units
  FROM mart.v_sales_daily_by_channel f
  WHERE f.date IS NOT NULL AND f.channel IN ('web', 'app', 'store') AND f.gross_pln > 0
  GROUP BY 1, 2, 3, 4, 5
),
totals AS (
  SELECT year, category_id, channel, SUM(gross_pln) AS total_gross, SUM(units) AS total_units
  FROM base GROUP BY 1, 2, 3
)
SELECT
  b.year,
  b.category_id,
  b.brand_id,
  b.brand_name,
  b.channel,
  b.gross_pln,
  b.units,
  ROUND(100.0 * b.gross_pln / NULLIF(t.total_gross, 0), 1) AS pct_value,
  ROUND(100.0 * b.units / NULLIF(t.total_units, 0), 1) AS pct_volume
FROM base b
JOIN totals t ON t.year = b.year AND t.category_id = b.category_id AND t.channel = b.channel
"""
    if not run_query(client, sql_pie_cat, "precalc_pie_brands_category"):
        sys.exit(1)

    # 6. precalc_pie_brands_subcategory: pie per subcategory
    sql_pie_sub = f"""
CREATE OR REPLACE TABLE {DATASET}.precalc_pie_brands_subcategory
PARTITION BY RANGE_BUCKET(year, GENERATE_ARRAY(2023, 2026))
CLUSTER BY category_id, channel
AS
WITH base AS (
  SELECT
    CAST(EXTRACT(YEAR FROM f.date) AS INT64) AS year,
    f.category_id,
    f.brand_id,
    f.brand_name,
    CAST('' AS STRING) AS channel,
    SUM(f.gross_pln) AS gross_pln,
    SUM(f.units) AS units
  FROM mart.fact_sales_daily f
  WHERE f.date IS NOT NULL AND f.category_id >= 100 AND f.gross_pln > 0
  GROUP BY 1, 2, 3, 4, 5
  UNION ALL
  SELECT
    CAST(EXTRACT(YEAR FROM f.date) AS INT64) AS year,
    f.category_id,
    f.brand_id,
    f.brand_name,
    f.channel,
    SUM(f.gross_pln) AS gross_pln,
    SUM(f.units) AS units
  FROM mart.v_sales_daily_by_channel f
  WHERE f.date IS NOT NULL AND f.category_id >= 100 AND f.channel IN ('web', 'app', 'store') AND f.gross_pln > 0
  GROUP BY 1, 2, 3, 4, 5
),
totals AS (
  SELECT year, category_id, channel, SUM(gross_pln) AS total_gross, SUM(units) AS total_units
  FROM base GROUP BY 1, 2, 3
)
SELECT
  b.year,
  b.category_id,
  b.brand_id,
  b.brand_name,
  b.channel,
  b.gross_pln,
  b.units,
  ROUND(100.0 * b.gross_pln / NULLIF(t.total_gross, 0), 1) AS pct_value,
  ROUND(100.0 * b.units / NULLIF(t.total_units, 0), 1) AS pct_volume
FROM base b
JOIN totals t ON t.year = b.year AND t.category_id = b.category_id AND t.channel = b.channel
"""
    if not run_query(client, sql_pie_sub, "precalc_pie_brands_subcategory"):
        sys.exit(1)

    # 7. precalc_prev_year_pct: market share anno precedente (parent + subcategory)
    sql_prev = f"""
CREATE OR REPLACE TABLE {DATASET}.precalc_prev_year_pct
PARTITION BY RANGE_BUCKET(year, GENERATE_ARRAY(2023, 2026))
CLUSTER BY category_id, channel
AS
WITH per_cat AS (
  SELECT CAST(EXTRACT(YEAR FROM f.date) AS INT64) AS year, f.parent_category_id AS category_id, f.brand_id, CAST('' AS STRING) AS channel, SUM(f.gross_pln) AS gross_pln
  FROM mart.fact_sales_daily f WHERE f.date IS NOT NULL AND f.gross_pln > 0 GROUP BY 1, 2, 3, 4
  UNION ALL
  SELECT CAST(EXTRACT(YEAR FROM f.date) AS INT64), f.parent_category_id, f.brand_id, f.channel, SUM(f.gross_pln)
  FROM mart.v_sales_daily_by_channel f WHERE f.date IS NOT NULL AND f.channel IN ('web','app','store') AND f.gross_pln > 0 GROUP BY 1, 2, 3, 4
  UNION ALL
  SELECT CAST(EXTRACT(YEAR FROM f.date) AS INT64), f.category_id, f.brand_id, CAST('' AS STRING), SUM(f.gross_pln)
  FROM mart.fact_sales_daily f WHERE f.date IS NOT NULL AND f.category_id >= 100 AND f.gross_pln > 0 GROUP BY 1, 2, 3, 4
  UNION ALL
  SELECT CAST(EXTRACT(YEAR FROM f.date) AS INT64), f.category_id, f.brand_id, f.channel, SUM(f.gross_pln)
  FROM mart.v_sales_daily_by_channel f WHERE f.date IS NOT NULL AND f.category_id >= 100 AND f.channel IN ('web','app','store') AND f.gross_pln > 0 GROUP BY 1, 2, 3, 4
),
totals AS (
  SELECT year, category_id, channel, SUM(gross_pln) AS total_gross
  FROM per_cat GROUP BY 1, 2, 3
)
SELECT
  p.year,
  p.category_id,
  p.brand_id,
  p.channel,
  ROUND(100.0 * p.gross_pln / NULLIF(t.total_gross, 0), 1) AS pct_value_prev
FROM per_cat p
JOIN totals t ON t.year = p.year AND t.category_id = p.category_id AND t.channel = p.channel
"""
    if not run_query(client, sql_prev, "precalc_prev_year_pct"):
        sys.exit(1)

    # 8. precalc_sales_bar_category: dati pronti per grafici bar value/volume by category (zero aggregazioni a runtime)
    sql_bar_cat = f"""
CREATE OR REPLACE TABLE {DATASET}.precalc_sales_bar_category
PARTITION BY RANGE_BUCKET(year, GENERATE_ARRAY(2023, 2026))
CLUSTER BY brand_id, category_id
AS
WITH brand_sales AS (
  SELECT year, brand_id, parent_category_id AS category_id, SUM(gross_pln) AS brand_gross_pln, SUM(units) AS brand_units
  FROM mart.precalc_sales_agg
  WHERE channel = '' AND gross_pln > 0
  GROUP BY 1, 2, 3
),
media_sales AS (
  SELECT year, parent_category_id AS category_id, SUM(gross_pln) AS media_gross_pln, SUM(units) AS media_units
  FROM mart.precalc_sales_agg
  WHERE channel = ''
  GROUP BY 1, 2
)
SELECT
  b.year,
  b.brand_id,
  b.category_id,
  c.category_name,
  b.brand_gross_pln,
  b.brand_units,
  m.media_gross_pln,
  m.media_units
FROM brand_sales b
JOIN media_sales m ON m.year = b.year AND m.category_id = b.category_id
JOIN mart.dim_category c ON c.category_id = b.category_id AND c.level = 1
"""
    if not run_query(client, sql_bar_cat, "precalc_sales_bar_category"):
        sys.exit(1)

    # 9. precalc_sales_bar_subcategory: dati pronti per grafici bar value/volume by subcategory (zero aggregazioni a runtime)
    sql_bar_sub = f"""
CREATE OR REPLACE TABLE {DATASET}.precalc_sales_bar_subcategory
PARTITION BY RANGE_BUCKET(year, GENERATE_ARRAY(2023, 2026))
CLUSTER BY brand_id, parent_category_id, category_id
AS
WITH brand_sales AS (
  SELECT year, brand_id, parent_category_id, category_id, SUM(gross_pln) AS brand_gross_pln, SUM(units) AS brand_units
  FROM mart.precalc_sales_agg
  WHERE channel = '' AND category_id >= 100 AND gross_pln > 0
  GROUP BY 1, 2, 3, 4
),
media_sales AS (
  SELECT year, parent_category_id, category_id, SUM(gross_pln) AS media_gross_pln, SUM(units) AS media_units
  FROM mart.precalc_sales_agg
  WHERE channel = '' AND category_id >= 100
  GROUP BY 1, 2, 3
)
SELECT
  b.year,
  b.brand_id,
  b.parent_category_id,
  b.category_id,
  c.category_name,
  b.brand_gross_pln,
  b.brand_units,
  m.media_gross_pln,
  m.media_units
FROM brand_sales b
JOIN media_sales m ON m.year = b.year AND m.parent_category_id = b.parent_category_id AND m.category_id = b.category_id
JOIN mart.dim_category c ON c.category_id = b.category_id AND c.level = 2
"""
    if not run_query(client, sql_bar_sub, "precalc_sales_bar_subcategory"):
        sys.exit(1)

    # 10. precalc_promo_creator_benchmark (fase 2): discount e ROI per year/category/brand
    sql_pc = f"""
CREATE OR REPLACE TABLE {DATASET}.precalc_promo_creator_benchmark
PARTITION BY RANGE_BUCKET(year, GENERATE_ARRAY(2023, 2026))
CLUSTER BY category_id, brand_id
AS
WITH discount AS (
  SELECT
    CAST(EXTRACT(YEAR FROM f.date) AS INT64) AS year,
    f.parent_category_id AS category_id,
    f.brand_id,
    ROUND(COALESCE(SUM(CASE WHEN f.promo_flag THEN f.discount_depth_pct * f.gross_pln ELSE 0 END)
      / NULLIF(SUM(CASE WHEN f.promo_flag THEN f.gross_pln ELSE 0 END), 0), 0), 1) AS brand_avg_discount
  FROM mart.fact_sales_daily f
  WHERE f.date IS NOT NULL AND f.gross_pln > 0
  GROUP BY 1, 2, 3
),
media_disc AS (
  SELECT
    CAST(EXTRACT(YEAR FROM f.date) AS INT64) AS year,
    f.parent_category_id AS category_id,
    ROUND(COALESCE(SUM(CASE WHEN f.promo_flag THEN f.discount_depth_pct * f.gross_pln ELSE 0 END)
      / NULLIF(SUM(CASE WHEN f.promo_flag THEN f.gross_pln ELSE 0 END), 0), 0), 1) AS media_avg_discount
  FROM mart.fact_sales_daily f
  WHERE f.date IS NOT NULL
  GROUP BY 1, 2
)
SELECT
  d.year,
  d.category_id,
  d.brand_id,
  COALESCE(m.media_avg_discount, 0) AS media_avg_discount,
  COALESCE(d.brand_avg_discount, 0) AS brand_avg_discount,
  CAST(NULL AS STRING) AS promo_type,
  CAST(NULL AS NUMERIC) AS avg_roi
FROM discount d
LEFT JOIN media_disc m ON m.year = d.year AND m.category_id = d.category_id
"""
    if not run_query(client, sql_pc, "precalc_promo_creator_benchmark"):
        sys.exit(1)

    # 11. precalc_promo_live_sku: SKU-level promo performance for Check Live Promo (partition by date for fast last 7/30 days)
    sql_promo_live = f"""
CREATE OR REPLACE TABLE {DATASET}.precalc_promo_live_sku
PARTITION BY date
CLUSTER BY brand_id, promo_id, channel
AS
WITH base AS (
  SELECT
    o.date,
    o.channel,
    oi.product_id,
    p.product_name,
    p.brand_id,
    b.brand_name,
    p.subcategory_id AS category_id,
    c_sub.category_name,
    p.category_id AS parent_category_id,
    o.promo_id,
    pr.promo_name,
    SUM(oi.gross_pln * (0.93 + 0.14 * (MOD(ABS(FARM_FINGERPRINT(CONCAT('chx', CAST(p.brand_id AS STRING), '|', o.channel))), 1000) / 1000.0))) AS gross_pln,
    SUM(oi.quantity) AS units,
    COUNT(DISTINCT o.order_id) AS order_count
  FROM mart.fact_order_items oi
  JOIN mart.fact_orders o ON o.order_id = oi.order_id
  JOIN mart.dim_product p ON p.product_id = oi.product_id
  JOIN mart.dim_brand b ON b.brand_id = p.brand_id
  JOIN mart.dim_category c_sub ON c_sub.category_id = p.subcategory_id AND c_sub.level = 2
  JOIN mart.dim_promo pr ON pr.promo_id = o.promo_id
  WHERE o.promo_flag AND o.promo_id IS NOT NULL AND o.date IS NOT NULL
  GROUP BY o.date, o.channel, oi.product_id, p.product_name, p.brand_id, b.brand_name,
    p.subcategory_id, c_sub.category_name, p.category_id, o.promo_id, pr.promo_name
)
SELECT date, product_id, product_name, brand_id, brand_name, category_id, category_name, parent_category_id,
  promo_id, promo_name, CAST('' AS STRING) AS channel,
  SUM(gross_pln) AS gross_pln, SUM(units) AS units, SUM(order_count) AS order_count
FROM base
GROUP BY date, product_id, product_name, brand_id, brand_name, category_id, category_name, parent_category_id, promo_id, promo_name
UNION ALL
SELECT date, product_id, product_name, brand_id, brand_name, category_id, category_name, parent_category_id,
  promo_id, promo_name, channel, gross_pln, units, order_count
FROM base
WHERE channel IN ('web', 'app', 'store')
"""
    if not run_query(client, sql_promo_live, "precalc_promo_live_sku"):
        sys.exit(1)

    # 12. precalc_mkt_segment_categories: Marketing segment summary – top categories per segment
    sql_mkt_cat = f"""
CREATE OR REPLACE TABLE {DATASET}.precalc_mkt_segment_categories
PARTITION BY RANGE_BUCKET(year, GENERATE_ARRAY(2023, 2026))
CLUSTER BY segment_id, level
AS
WITH parent_agg AS (
  SELECT
    CAST(EXTRACT(YEAR FROM f.date) AS INT64) AS year,
    f.segment_id,
    f.parent_category_id AS category_id,
    f.parent_category_id AS parent_category_id,
    dc.category_name,
    1 AS level,
    SUM(f.gross_pln) AS gross_pln
  FROM mart.fact_sales_daily f
  JOIN mart.dim_category dc ON dc.category_id = f.parent_category_id AND dc.level = 1
  WHERE f.date IS NOT NULL AND f.gross_pln > 0
  GROUP BY 1, 2, 3, 4, 5
),
subcat_agg AS (
  SELECT
    CAST(EXTRACT(YEAR FROM f.date) AS INT64) AS year,
    f.segment_id,
    f.category_id,
    f.parent_category_id,
    dc.category_name,
    2 AS level,
    SUM(f.gross_pln) AS gross_pln
  FROM mart.fact_sales_daily f
  JOIN mart.dim_category dc ON dc.category_id = f.category_id AND dc.level = 2
  WHERE f.date IS NOT NULL AND f.gross_pln > 0
  GROUP BY 1, 2, 3, 4, 5
)
SELECT * FROM parent_agg
UNION ALL
SELECT * FROM subcat_agg
"""
    if not run_query(client, sql_mkt_cat, "precalc_mkt_segment_categories"):
        sys.exit(1)

    # 13. precalc_mkt_segment_skus: Marketing segment summary – top SKUs per segment
    sql_mkt_skus = f"""
CREATE OR REPLACE TABLE {DATASET}.precalc_mkt_segment_skus
PARTITION BY RANGE_BUCKET(year, GENERATE_ARRAY(2023, 2026))
CLUSTER BY segment_id, parent_category_id
AS
SELECT
  CAST(EXTRACT(YEAR FROM o.date) AS INT64) AS year,
  c.segment_id,
  oi.product_id,
  p.product_name,
  b.brand_name,
  p.subcategory_id AS category_id,
  p.category_id AS parent_category_id,
  SUM(oi.gross_pln * (0.93 + 0.14 * (MOD(ABS(FARM_FINGERPRINT(CONCAT('chx', CAST(p.brand_id AS STRING), '|', o.channel))), 1000) / 1000.0))) AS gross_pln,
  SUM(oi.quantity) AS units
FROM mart.fact_order_items oi
JOIN mart.fact_orders o ON o.order_id = oi.order_id
JOIN mart.dim_customer c ON c.customer_id = o.customer_id
JOIN mart.dim_product p ON p.product_id = oi.product_id
JOIN mart.dim_brand b ON b.brand_id = p.brand_id
WHERE o.date IS NOT NULL
GROUP BY 1, 2, 3, 4, 5, 6, 7
"""
    if not run_query(client, sql_mkt_skus, "precalc_mkt_segment_skus"):
        sys.exit(1)

    # 13b. precalc_mi_segment_by_product: MI "Segment by SKU" (stesso aggregato di fact_order_items live)
    sql_mi_seg_by_prod = f"""
CREATE OR REPLACE TABLE {DATASET}.precalc_mi_segment_by_product
PARTITION BY RANGE_BUCKET(year, GENERATE_ARRAY(2023, 2026))
CLUSTER BY brand_id, product_id, year
AS
SELECT
  CAST(EXTRACT(YEAR FROM o.date) AS INT64) AS year,
  oi.product_id,
  p.brand_id,
  c.segment_id,
  ANY_VALUE(s.segment_name) AS segment_name,
  CAST('' AS STRING) AS channel,
  CAST(ROUND(SUM(oi.gross_pln), 2) AS NUMERIC) AS gross_pln,
  SUM(oi.quantity) AS units
FROM mart.fact_order_items oi
JOIN mart.fact_orders o ON o.order_id = oi.order_id
JOIN mart.dim_customer c ON c.customer_id = o.customer_id
JOIN mart.dim_segment s ON s.segment_id = c.segment_id
JOIN mart.dim_product p ON p.product_id = oi.product_id
WHERE o.date IS NOT NULL
GROUP BY 1, 2, 3, 4, 6
UNION ALL
SELECT
  CAST(EXTRACT(YEAR FROM o.date) AS INT64) AS year,
  oi.product_id,
  p.brand_id,
  c.segment_id,
  ANY_VALUE(s.segment_name) AS segment_name,
  o.channel,
  CAST(ROUND(SUM(oi.gross_pln), 2) AS NUMERIC) AS gross_pln,
  SUM(oi.quantity) AS units
FROM mart.fact_order_items oi
JOIN mart.fact_orders o ON o.order_id = oi.order_id
JOIN mart.dim_customer c ON c.customer_id = o.customer_id
JOIN mart.dim_segment s ON s.segment_id = c.segment_id
JOIN mart.dim_product p ON p.product_id = oi.product_id
WHERE o.date IS NOT NULL AND o.channel IN ('web', 'app', 'store')
GROUP BY 1, 2, 3, 4, 6
"""
    if not run_query(client, sql_mi_seg_by_prod, "precalc_mi_segment_by_product"):
        sys.exit(1)

    # 14–15: BQ non consente OR REPLACE se cambia CLUSTER; drop esplicito quando si aggiunge parent_category_id
    for _tbl in ("precalc_mkt_purchasing_channel", "precalc_mkt_purchasing_peak"):
        run_query(client, f"DROP TABLE IF EXISTS `{PROJECT_ID}.{DATASET}.{_tbl}`", f"drop {_tbl} (recreate clustering)")

    # 14. precalc_mkt_purchasing_channel: Marketing purchasing – channel mix (NULL parent = tutte le macro)
    sql_mkt_ch = f"""
CREATE OR REPLACE TABLE {DATASET}.precalc_mkt_purchasing_channel
PARTITION BY RANGE_BUCKET(year, GENERATE_ARRAY(2023, 2026))
CLUSTER BY segment_id, parent_category_id
AS
SELECT
  CAST(EXTRACT(YEAR FROM o.date) AS INT64) AS year,
  c.segment_id,
  ANY_VALUE(s.segment_name) AS segment_name,
  o.channel,
  CAST(NULL AS INT64) AS parent_category_id,
  SUM(o.gross_pln) AS gross_pln
FROM mart.fact_orders o
JOIN mart.dim_customer c ON c.customer_id = o.customer_id
JOIN mart.dim_segment s ON s.segment_id = c.segment_id
WHERE o.date IS NOT NULL
GROUP BY 1, 2, 4
UNION ALL
SELECT
  CAST(EXTRACT(YEAR FROM o.date) AS INT64) AS year,
  c.segment_id,
  ANY_VALUE(s.segment_name) AS segment_name,
  o.channel,
  macro_cat AS parent_category_id,
  SUM(o.gross_pln) AS gross_pln
FROM mart.fact_orders o
JOIN mart.dim_customer c ON c.customer_id = o.customer_id
JOIN mart.dim_segment s ON s.segment_id = c.segment_id
CROSS JOIN UNNEST(GENERATE_ARRAY(1, 10)) AS macro_cat
WHERE o.date IS NOT NULL
  AND EXISTS (
    SELECT 1
    FROM mart.fact_order_items oi
    JOIN mart.dim_product pr ON pr.product_id = oi.product_id
    JOIN mart.dim_category dc ON dc.category_id = pr.category_id
    WHERE oi.order_id = o.order_id
      AND (dc.parent_category_id = macro_cat OR (dc.level = 1 AND dc.category_id = macro_cat))
  )
GROUP BY 1, 2, 4, 5
"""
    if not run_query(client, sql_mkt_ch, "precalc_mkt_purchasing_channel"):
        sys.exit(1)

    # 15. precalc_mkt_purchasing_peak: Marketing purchasing – peak events per segment × macro categoria
    sql_mkt_peak = f"""
CREATE OR REPLACE TABLE {DATASET}.precalc_mkt_purchasing_peak
PARTITION BY RANGE_BUCKET(year, GENERATE_ARRAY(2023, 2026))
CLUSTER BY segment_id, parent_category_id
AS
WITH base_all AS (
  SELECT
    CAST(EXTRACT(YEAR FROM o.date) AS INT64) AS year,
    c.segment_id,
    ANY_VALUE(s.segment_name) AS segment_name,
    d.peak_event,
    CAST(NULL AS INT64) AS parent_category_id,
    SUM(o.gross_pln) AS gross_pln
  FROM mart.fact_orders o
  JOIN mart.dim_customer c ON c.customer_id = o.customer_id
  JOIN mart.dim_segment s ON s.segment_id = c.segment_id
  JOIN mart.dim_date d ON d.date = o.date
  WHERE o.date IS NOT NULL AND d.peak_event IS NOT NULL
  GROUP BY 1, 2, 4, 5
),
base_cat AS (
  SELECT
    CAST(EXTRACT(YEAR FROM o.date) AS INT64) AS year,
    c.segment_id,
    ANY_VALUE(s.segment_name) AS segment_name,
    d.peak_event,
    macro_cat AS parent_category_id,
    SUM(o.gross_pln) AS gross_pln
  FROM mart.fact_orders o
  JOIN mart.dim_customer c ON c.customer_id = o.customer_id
  JOIN mart.dim_segment s ON s.segment_id = c.segment_id
  JOIN mart.dim_date d ON d.date = o.date
  CROSS JOIN UNNEST(GENERATE_ARRAY(1, 10)) AS macro_cat
  WHERE o.date IS NOT NULL AND d.peak_event IS NOT NULL
    AND EXISTS (
      SELECT 1
      FROM mart.fact_order_items oi
      JOIN mart.dim_product pr ON pr.product_id = oi.product_id
      JOIN mart.dim_category dc ON dc.category_id = pr.category_id
      WHERE oi.order_id = o.order_id
        AND (dc.parent_category_id = macro_cat OR (dc.level = 1 AND dc.category_id = macro_cat))
    )
  GROUP BY 1, 2, 4, 5
),
base AS (
  SELECT * FROM base_all
  UNION ALL
  SELECT * FROM base_cat
),
totals AS (
  SELECT year, segment_id, parent_category_id, SUM(gross_pln) AS total
  FROM base
  GROUP BY 1, 2, 3
)
SELECT
  b.year,
  b.segment_id,
  b.segment_name,
  b.peak_event,
  b.parent_category_id,
  ROUND(100.0 * b.gross_pln / NULLIF(t.total, 0), 1) AS orders_pct,
  b.gross_pln
FROM base b
JOIN totals t
  ON t.year = b.year
  AND t.segment_id = b.segment_id
  AND ((t.parent_category_id IS NULL AND b.parent_category_id IS NULL)
    OR (t.parent_category_id = b.parent_category_id))
"""
    if not run_query(client, sql_mkt_peak, "precalc_mkt_purchasing_peak"):
        sys.exit(1)

    print("-" * 50)
    print("Refresh completato.")


if __name__ == "__main__":
    main()
