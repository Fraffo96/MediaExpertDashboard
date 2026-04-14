"""Step SQL precalc — generato da scripts/dev/_gen_precalc_steps.py; non editare a mano (rigenera)."""
from __future__ import annotations


def build_sql_steps(dataset: str, project_id: str) -> list[tuple[str, str]]:
    steps: list[tuple[str, str]] = []
    # 1. precalc_sales_agg: da fact_sales_daily (channel='') + v_sales_daily_by_channel (web, app, store)
    sql_sales = f"""
CREATE OR REPLACE TABLE {dataset}.precalc_sales_agg
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
    steps.append(("precalc_sales_agg", sql_sales))

    # 2. precalc_peak_agg: da fact_sales_daily + dim_date
    sql_peak = f"""
CREATE OR REPLACE TABLE {dataset}.precalc_peak_agg
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
    steps.append(("precalc_peak_agg", sql_peak))

    # 3. precalc_roi_agg: da fact_promo_performance
    sql_roi = f"""
CREATE OR REPLACE TABLE {dataset}.precalc_roi_agg
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
    steps.append(("precalc_roi_agg", sql_roi))

    # 4. precalc_incremental_yoy: da fact_sales_daily + fact_promo_performance
    sql_inc = f"""
CREATE OR REPLACE TABLE {dataset}.precalc_incremental_yoy
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
    steps.append(("precalc_incremental_yoy", sql_inc))

    # 5. precalc_pie_brands_category: pie per parent category
    sql_pie_cat = f"""
CREATE OR REPLACE TABLE {dataset}.precalc_pie_brands_category
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
    steps.append(("precalc_pie_brands_category", sql_pie_cat))

    # 6. precalc_pie_brands_subcategory: pie per subcategory
    sql_pie_sub = f"""
CREATE OR REPLACE TABLE {dataset}.precalc_pie_brands_subcategory
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
    steps.append(("precalc_pie_brands_subcategory", sql_pie_sub))

    # 7. precalc_prev_year_pct: market share anno precedente (parent + subcategory)
    sql_prev = f"""
CREATE OR REPLACE TABLE {dataset}.precalc_prev_year_pct
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
    steps.append(("precalc_prev_year_pct", sql_prev))

    # 8. precalc_sales_bar_category: dati pronti per grafici bar value/volume by category (zero aggregazioni a runtime)
    sql_bar_cat = f"""
CREATE OR REPLACE TABLE {dataset}.precalc_sales_bar_category
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
    steps.append(("precalc_sales_bar_category", sql_bar_cat))

    # 9. precalc_sales_bar_subcategory: dati pronti per grafici bar value/volume by subcategory (zero aggregazioni a runtime)
    sql_bar_sub = f"""
CREATE OR REPLACE TABLE {dataset}.precalc_sales_bar_subcategory
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
    steps.append(("precalc_sales_bar_subcategory", sql_bar_sub))

    # 10. precalc_promo_creator_benchmark (fase 2): discount e ROI per year/category/brand
    sql_pc = f"""
CREATE OR REPLACE TABLE {dataset}.precalc_promo_creator_benchmark
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
    steps.append(("precalc_promo_creator_benchmark", sql_pc))

    # 10b. precalc_promo_creator_subcat: Promo Creator con subcategory senza scan live su fact_sales_daily
    sql_pc_sub = f"""
CREATE OR REPLACE TABLE {dataset}.precalc_promo_creator_subcat
PARTITION BY RANGE_BUCKET(year, GENERATE_ARRAY(2023, 2026))
CLUSTER BY parent_category_id, category_id, promo_type, discount_target
AS
WITH pcfg AS (
  SELECT 1 AS pid, 1.52 AS br UNION ALL SELECT 2 AS pid, 1.02 AS br UNION ALL SELECT 3 AS pid, 0.68 AS br
  UNION ALL SELECT 4 AS pid, 1.38 AS br UNION ALL SELECT 5 AS pid, 1.18 AS br UNION ALL SELECT 6 AS pid, 1.92 AS br
  UNION ALL SELECT 7 AS pid, 1.48 AS br UNION ALL SELECT 8 AS pid, 1.02 AS br UNION ALL SELECT 9 AS pid, 0.88 AS br UNION ALL SELECT 10 AS pid, 0.82 AS br
),
yadj AS (
  SELECT 2023 AS yr, 1.15 AS ra UNION ALL SELECT 2024, 1.00 UNION ALL SELECT 2025, 0.85 UNION ALL SELECT 2026, 0.85
),
agg AS (
  SELECT EXTRACT(YEAR FROM f.date) AS year, f.category_id AS subcat, f.promo_id, f.brand_id, f.date,
    ANY_VALUE(f.parent_category_id) AS pcat,
    SUM(f.gross_pln) AS att
  FROM mart.fact_sales_daily f
  WHERE f.date IS NOT NULL AND f.promo_flag AND f.promo_id IS NOT NULL AND f.category_id >= 100
  GROUP BY 1, 2, 3, 4, 5
),
non_promo AS (
  SELECT brand_id, category_id AS subcat, date, SUM(gross_pln) AS gross
  FROM mart.fact_sales_daily
  WHERE NOT promo_flag AND category_id >= 100 AND date IS NOT NULL
  GROUP BY brand_id, category_id, date
),
baseline AS (
  SELECT a.year, a.subcat, a.promo_id, a.brand_id, a.date, a.pcat, a.att,
    AVG(np.gross) AS bl
  FROM agg a
  LEFT JOIN non_promo np ON np.brand_id = a.brand_id AND np.subcat = a.subcat
    AND np.date BETWEEN DATE_SUB(a.date, INTERVAL 28 DAY) AND DATE_SUB(a.date, INTERVAL 1 DAY)
  GROUP BY a.year, a.subcat, a.promo_id, a.brand_id, a.date, a.pcat, a.att
),
roi_computed AS (
  SELECT b.year, b.subcat, b.pcat AS parent_cat, dm.promo_type, b.promo_id, b.brand_id,
    ROUND(
      (
        (p.br * y.ra + 0.04 * (MOD(ABS(FARM_FINGERPRINT(CONCAT(CAST(b.date AS STRING), CAST(b.promo_id AS STRING), CAST(b.brand_id AS STRING)))), 21) - 10) / 10.0)
        * (0.76 + 0.42 * (MOD(ABS(FARM_FINGERPRINT(CONCAT('bmul', CAST(b.brand_id AS STRING)))), 1000) / 1000.0))
        + 0.28 * (MOD(ABS(FARM_FINGERPRINT(CONCAT('padj', CAST(b.brand_id AS STRING), '|', CAST(b.promo_id AS STRING)))), 21) - 10) / 10.0
      )
      * (0.80 + 0.42 * (MOD(ABS(FARM_FINGERPRINT(CONCAT('pcat', CAST(b.pcat AS STRING)))), 1000) / 1000.0))
      * (0.74 + 0.48 * (MOD(ABS(FARM_FINGERPRINT(CONCAT('ptype', COALESCE(dm.promo_type, 'na'), '|', CAST(b.pcat AS STRING)))), 1000) / 1000.0)),
      4) AS roi
  FROM baseline b
  JOIN pcfg p ON p.pid = b.promo_id
  JOIN yadj y ON y.yr = b.year
  JOIN mart.dim_promo dm ON dm.promo_id = b.promo_id
),
pm AS (
  SELECT 1 AS promo_id, 10 AS d UNION ALL SELECT 2, 20 UNION ALL SELECT 3, 30 UNION ALL SELECT 4, 15
  UNION ALL SELECT 5, 15 UNION ALL SELECT 6, 12 UNION ALL SELECT 7, 8 UNION ALL SELECT 8, 18
  UNION ALL SELECT 9, 25 UNION ALL SELECT 10, 20
),
targets AS (
  SELECT DISTINCT d AS discount_target FROM pm
),
bucket_map AS (
  SELECT t.discount_target, pm.promo_id
  FROM targets t
  JOIN pm ON ABS(pm.d - t.discount_target) <= 7
),
filt AS (
  SELECT r.year, r.parent_cat, r.subcat, r.promo_type, r.promo_id, r.brand_id, bm.discount_target, r.roi
  FROM roi_computed r
  INNER JOIN bucket_map bm ON r.promo_id = bm.promo_id
),
discount_sub AS (
  SELECT
    EXTRACT(YEAR FROM f.date) AS year,
    f.parent_category_id,
    f.category_id AS subcat,
    ROUND(COALESCE(
      SUM(CASE WHEN f.promo_flag THEN f.discount_depth_pct * f.gross_pln ELSE 0 END)
      / NULLIF(SUM(CASE WHEN f.promo_flag THEN f.gross_pln ELSE 0 END), 0), 0), 1) AS media_avg_discount
  FROM mart.fact_sales_daily f
  WHERE f.category_id >= 100
  GROUP BY 1, 2, 3
),
main_agg AS (
  SELECT
    f.year,
    f.parent_cat AS parent_category_id,
    f.subcat AS category_id,
    f.promo_type,
    f.discount_target,
    ROUND(AVG(f.roi), 2) AS avg_roi,
    COUNT(*) AS n_promos,
    ANY_VALUE(d.media_avg_discount) AS media_avg_discount
  FROM filt f
  LEFT JOIN discount_sub d
    ON d.year = f.year AND d.parent_category_id = f.parent_cat AND d.subcat = f.subcat
  GROUP BY 1, 2, 3, 4, 5
),
br AS (
  SELECT year, parent_cat, subcat, promo_type, discount_target, brand_id,
    ROUND(AVG(roi), 2) AS br_avg_roi
  FROM filt
  GROUP BY 1, 2, 3, 4, 5, 6
),
top_brands AS (
  SELECT
    br.year, br.parent_cat, br.subcat, br.promo_type, br.discount_target,
    ARRAY_AGG(STRUCT(br.brand_id AS brand_id, b.brand_name AS brand_name, br.br_avg_roi AS avg_roi) ORDER BY br.br_avg_roi DESC LIMIT 5) AS top_brands
  FROM br
  JOIN mart.dim_brand b ON b.brand_id = br.brand_id
  GROUP BY 1, 2, 3, 4, 5
),
seg_raw AS (
  SELECT
    EXTRACT(YEAR FROM f.date) AS year,
    f.parent_category_id,
    f.category_id AS subcat,
    pt.promo_type AS promo_type,
    s.segment_id,
    s.segment_name,
    ROUND(COALESCE(
      SUM(CASE WHEN f.promo_flag AND p.promo_type = pt.promo_type THEN f.gross_pln ELSE 0 END)
      / NULLIF(SUM(f.gross_pln), 0) * 100, 0), 1) AS promo_share_pct,
    SUM(f.gross_pln) AS total_gross
  FROM mart.fact_sales_daily f
  JOIN mart.dim_segment s ON s.segment_id = f.segment_id
  LEFT JOIN mart.dim_promo p ON p.promo_id = f.promo_id
  CROSS JOIN (SELECT DISTINCT promo_type FROM mart.dim_promo WHERE promo_type IS NOT NULL) pt
  WHERE f.category_id >= 100 AND f.gross_pln > 0
  GROUP BY 1, 2, 3, 4, 5, 6
),
seg_ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY year, parent_category_id, subcat, promo_type ORDER BY promo_share_pct DESC) AS rn
  FROM seg_raw
),
seg_top AS (
  SELECT year, parent_category_id, subcat, promo_type,
    ARRAY_AGG(STRUCT(segment_name AS segment_name, promo_share_pct AS promo_share_pct) ORDER BY promo_share_pct DESC LIMIT 3) AS top_segments
  FROM seg_ranked
  WHERE rn <= 3
  GROUP BY 1, 2, 3, 4
)
SELECT
  m.year,
  m.parent_category_id,
  m.category_id,
  m.promo_type,
  m.discount_target,
  m.avg_roi,
  m.n_promos,
  COALESCE(m.media_avg_discount, 0) AS media_avg_discount,
  tb.top_brands,
  COALESCE(st.top_segments, []) AS top_segments
FROM main_agg m
LEFT JOIN top_brands tb
  ON m.year = tb.year AND m.parent_category_id = tb.parent_cat AND m.category_id = tb.subcat
  AND m.promo_type = tb.promo_type AND m.discount_target = tb.discount_target
LEFT JOIN seg_top st
  ON m.year = st.year AND m.parent_category_id = st.parent_category_id AND m.category_id = st.subcat
  AND m.promo_type = st.promo_type
"""
    steps.append(("precalc_promo_creator_subcat", sql_pc_sub))

    # 11. precalc_promo_live_sku: SKU-level promo performance for Check Live Promo (partition by date for fast last 7/30 days)
    sql_promo_live = f"""
CREATE OR REPLACE TABLE {dataset}.precalc_promo_live_sku
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
    steps.append(("precalc_promo_live_sku", sql_promo_live))

    # 12. precalc_mkt_segment_categories: Marketing segment summary – top categories per segment
    sql_mkt_cat = f"""
CREATE OR REPLACE TABLE {dataset}.precalc_mkt_segment_categories
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
    steps.append(("precalc_mkt_segment_categories", sql_mkt_cat))

    # 13. precalc_mkt_segment_skus: Marketing segment summary – top SKUs per segment
    sql_mkt_skus = f"""
CREATE OR REPLACE TABLE {dataset}.precalc_mkt_segment_skus
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
    steps.append(("precalc_mkt_segment_skus", sql_mkt_skus))

    # 13b. precalc_mi_segment_by_product: MI "Segment by SKU" (stesso aggregato di fact_order_items live)
    sql_mi_seg_by_prod = f"""
CREATE OR REPLACE TABLE {dataset}.precalc_mi_segment_by_product
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
    steps.append(("precalc_mi_segment_by_product", sql_mi_seg_by_prod))

    # 14–15: BQ non consente OR REPLACE se cambia CLUSTER; drop esplicito quando si aggiunge parent_category_id
    for _tbl in ("precalc_mkt_purchasing_channel", "precalc_mkt_purchasing_peak"):
        steps.append((f"drop_{_tbl}", f"DROP TABLE IF EXISTS `{project_id}.{dataset}.{_tbl}`"))

    # 14. precalc_mkt_purchasing_channel: Marketing purchasing – channel mix (NULL parent = tutte le macro)
    sql_mkt_ch = f"""
CREATE OR REPLACE TABLE {dataset}.precalc_mkt_purchasing_channel
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
    steps.append(("precalc_mkt_purchasing_channel", sql_mkt_ch))

    # 15. precalc_mkt_purchasing_peak: Marketing purchasing – peak events per segment × macro categoria
    sql_mkt_peak = f"""
CREATE OR REPLACE TABLE {dataset}.precalc_mkt_purchasing_peak
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
    steps.append(("precalc_mkt_purchasing_peak", sql_mkt_peak))
    return steps
