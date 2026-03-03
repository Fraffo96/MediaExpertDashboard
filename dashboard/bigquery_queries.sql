-- =============================================================================
-- Query dashboard BigQuery - ottimizzate con window functions (YoY, rolling)
-- Filtri suggeriti: @period_start, @period_end, @category_id, @promo_type
-- In Metabase usa parametri nativi o variabili.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1) Category Sales (PLN)
-- -----------------------------------------------------------------------------
SELECT
  d.date,
  d.year,
  d.quarter,
  d.month,
  c.category_id,
  c.category_name,
  SUM(f.gross_pln) AS gross_pln,
  SUM(f.net_pln)   AS net_pln,
  SUM(f.units)     AS units
FROM mart.fact_sales_daily f
JOIN mart.dim_date d ON d.date = f.date
JOIN mart.dim_category c ON c.category_id = f.category_id
WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @period_start) AND PARSE_DATE('%Y-%m-%d', @period_end)
  AND (@category_id IS NULL OR f.category_id = @category_id)
GROUP BY d.date, d.year, d.quarter, d.month, c.category_id, c.category_name
ORDER BY d.date, gross_pln DESC;

-- Versione senza parametri (sostituisci date in Metabase)
-- WHERE f.date BETWEEN '2025-01-01' AND '2025-12-31'

-- -----------------------------------------------------------------------------
-- 2) Promo Share of Sales
-- -----------------------------------------------------------------------------
WITH totals AS (
  SELECT
    SUM(gross_pln) AS total_gross,
    SUM(CASE WHEN promo_flag THEN gross_pln ELSE 0 END) AS promo_gross
  FROM mart.fact_sales_daily
  WHERE date BETWEEN PARSE_DATE('%Y-%m-%d', @period_start) AND PARSE_DATE('%Y-%m-%d', @period_end)
    AND (@category_id IS NULL OR category_id = @category_id)
)
SELECT
  total_gross,
  promo_gross,
  ROUND(100.0 * promo_gross / NULLIF(total_gross, 0), 2) AS promo_share_pct
FROM totals;

-- -----------------------------------------------------------------------------
-- 3) YoY Incremental (con window: anno corrente vs anno precedente)
-- -----------------------------------------------------------------------------
WITH yearly AS (
  SELECT
    EXTRACT(YEAR FROM date) AS year,
    SUM(gross_pln) AS total_gross
  FROM mart.fact_sales_daily
  WHERE date BETWEEN PARSE_DATE('%Y-%m-%d', @period_start) AND PARSE_DATE('%Y-%m-%d', @period_end)
    AND (@category_id IS NULL OR category_id = @category_id)
  GROUP BY EXTRACT(YEAR FROM date)
),
yoy AS (
  SELECT
    year,
    total_gross,
    LAG(total_gross) OVER (ORDER BY year) AS prior_year_gross,
    total_gross - LAG(total_gross) OVER (ORDER BY year) AS incremental_pln,
    ROUND(100.0 * (total_gross - LAG(total_gross) OVER (ORDER BY year)) / NULLIF(LAG(total_gross) OVER (ORDER BY year), 0), 2) AS yoy_pct
  FROM yearly
)
SELECT * FROM yoy WHERE prior_year_gross IS NOT NULL;

-- -----------------------------------------------------------------------------
-- 4) Promo ROI
-- -----------------------------------------------------------------------------
SELECT
  p.promo_id,
  p.promo_name,
  p.promo_type,
  SUM(f.attributed_sales_pln) AS attributed_sales_pln,
  SUM(f.discount_cost_pln + f.media_cost_pln) AS total_cost_pln,
  ROUND((SUM(f.attributed_sales_pln) - SUM(f.discount_cost_pln + f.media_cost_pln)) / NULLIF(SUM(f.discount_cost_pln + f.media_cost_pln), 0), 4) AS roi
FROM mart.fact_promo_performance f
JOIN mart.dim_promo p ON p.promo_id = f.promo_id
WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @period_start) AND PARSE_DATE('%Y-%m-%d', @period_end)
  AND (@promo_type IS NULL OR p.promo_type = @promo_type)
GROUP BY p.promo_id, p.promo_name, p.promo_type
ORDER BY roi DESC;

-- -----------------------------------------------------------------------------
-- 5) Peak Events Dependence
-- -----------------------------------------------------------------------------
SELECT
  CASE
    WHEN d.is_black_friday_week THEN 'Black Friday'
    WHEN d.is_xmas_period THEN 'Xmas'
    WHEN d.is_back_to_school THEN 'Back to School'
    ELSE 'Normal'
  END AS peak_event,
  COUNT(DISTINCT f.date) AS days_count,
  SUM(f.gross_pln) AS gross_pln,
  SUM(f.units)     AS units,
  ROUND(SUM(f.gross_pln) / NULLIF(COUNT(DISTINCT f.date), 0), 2) AS avg_daily_gross
FROM mart.fact_sales_daily f
JOIN mart.dim_date d ON d.date = f.date
WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @period_start) AND PARSE_DATE('%Y-%m-%d', @period_end)
  AND (@category_id IS NULL OR f.category_id = @category_id)
GROUP BY peak_event
ORDER BY gross_pln DESC;

-- -----------------------------------------------------------------------------
-- 6) Promo Performance by Type
-- -----------------------------------------------------------------------------
SELECT
  p.promo_type,
  SUM(f.attributed_sales_pln) AS attributed_sales_pln,
  SUM(f.discount_cost_pln)    AS discount_cost_pln,
  SUM(f.media_cost_pln)       AS media_cost_pln,
  ROUND(AVG(f.roi), 4)        AS avg_roi
FROM mart.fact_promo_performance f
JOIN mart.dim_promo p ON p.promo_id = f.promo_id
WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @period_start) AND PARSE_DATE('%Y-%m-%d', @period_end)
  AND (@promo_type IS NULL OR p.promo_type = @promo_type)
GROUP BY p.promo_type
ORDER BY attributed_sales_pln DESC;

-- -----------------------------------------------------------------------------
-- 7) Pre / During / Post Promo comparison (rolling baseline)
-- Per una promo scelta: confronto pre (N giorni prima) vs during vs post (N giorni dopo)
-- -----------------------------------------------------------------------------
WITH promo_period AS (
  SELECT
    promo_id,
    MIN(date) AS start_date,
    MAX(date) AS end_date,
    DATE_DIFF(MAX(date), MIN(date), DAY) + 1 AS days_len
  FROM mart.fact_promo_performance
  WHERE date BETWEEN PARSE_DATE('%Y-%m-%d', @period_start) AND PARSE_DATE('%Y-%m-%d', @period_end)
  GROUP BY promo_id
),
windows AS (
  SELECT
    pp.promo_id,
    pp.start_date,
    pp.end_date,
    pp.days_len,
    DATE_SUB(pp.start_date, INTERVAL pp.days_len DAY) AS pre_start,
    DATE_SUB(pp.start_date, INTERVAL 1 DAY)          AS pre_end,
    DATE_ADD(pp.end_date, INTERVAL 1 DAY)             AS post_start,
    DATE_ADD(pp.end_date, INTERVAL pp.days_len DAY)   AS post_end
  FROM promo_period pp
),
pre_sales AS (
  SELECT w.promo_id, SUM(f.gross_pln) AS pre_gross
  FROM mart.fact_sales_daily f
  JOIN windows w ON f.date BETWEEN w.pre_start AND w.pre_end AND f.promo_id = w.promo_id
  GROUP BY w.promo_id
),
during_sales AS (
  SELECT w.promo_id, SUM(f.gross_pln) AS during_gross
  FROM mart.fact_sales_daily f
  JOIN windows w ON f.date BETWEEN w.start_date AND w.end_date AND f.promo_id = w.promo_id
  GROUP BY w.promo_id
),
post_sales AS (
  SELECT w.promo_id, SUM(f.gross_pln) AS post_gross
  FROM mart.fact_sales_daily f
  JOIN windows w ON f.date BETWEEN w.post_start AND w.post_end
  GROUP BY w.promo_id
)
SELECT
  pr.promo_id,
  p.promo_name,
  p.promo_type,
  pre.pre_gross,
  dur.during_gross,
  post.post_gross,
  ROUND(100.0 * (dur.during_gross - pre.pre_gross) / NULLIF(pre.pre_gross, 0), 2) AS uplift_pct,
  ROUND(100.0 * (post.post_gross - pre.pre_gross) / NULLIF(pre.pre_gross, 0), 2) AS post_vs_pre_pct
FROM windows pr
JOIN mart.dim_promo p ON p.promo_id = pr.promo_id
LEFT JOIN pre_sales pre ON pre.promo_id = pr.promo_id
LEFT JOIN during_sales dur ON dur.promo_id = pr.promo_id
LEFT JOIN post_sales post ON post.promo_id = pr.promo_id;

-- Versione semplificata Pre/During/Post (senza join su brand in post_sales)
WITH ranges AS (
  SELECT 1 AS promo_id, DATE('2025-11-22') AS start_d, DATE('2025-11-30') AS end_d
),
pre_during_post AS (
  SELECT
    r.promo_id,
    SUM(CASE WHEN f.date < r.start_d THEN f.gross_pln ELSE 0 END) AS pre_gross,
    SUM(CASE WHEN f.date BETWEEN r.start_d AND r.end_d THEN f.gross_pln ELSE 0 END) AS during_gross,
    SUM(CASE WHEN f.date > r.end_d THEN f.gross_pln ELSE 0 END) AS post_gross
  FROM mart.fact_sales_daily f
  CROSS JOIN ranges r
  WHERE f.date BETWEEN DATE_SUB(r.start_d, INTERVAL 30 DAY) AND DATE_ADD(r.end_d, INTERVAL 30 DAY)
  GROUP BY r.promo_id
)
SELECT * FROM pre_during_post;
