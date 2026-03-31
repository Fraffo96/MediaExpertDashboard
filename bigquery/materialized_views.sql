-- =============================================================================
-- Viste aggregate su fact_sales_daily (accelerano query full-year).
-- Nota: le MATERIALIZED VIEW incrementali BQ non consentono ROUND/EXTRACT sulla
-- SELECT aggregata; qui usiamo VIEW classiche (stessa logica, niente storage duplicato).
-- =============================================================================

-- Aggregato vendite per (anno, brand, parent_category): usato per category-level
CREATE OR REPLACE VIEW mart.mv_sales_by_year_brand_parent AS
SELECT
  EXTRACT(YEAR FROM date) AS year,
  brand_id,
  brand_name,
  parent_category_id AS category_id,
  SUM(gross_pln) AS gross_pln,
  SUM(net_pln) AS net_pln,
  SUM(units) AS units,
  SUM(CASE WHEN promo_flag THEN 1 ELSE 0 END) AS promo_days,
  ROUND(LEAST(100, COALESCE(
    SUM(CASE WHEN promo_flag THEN discount_depth_pct * gross_pln ELSE 0 END)
    / NULLIF(SUM(CASE WHEN promo_flag THEN gross_pln ELSE 0 END), 0)
  , 0)), 1) AS discount_depth_pct
FROM mart.fact_sales_daily
WHERE date IS NOT NULL
GROUP BY year, brand_id, brand_name, parent_category_id;

-- Aggregato vendite per (anno, brand, subcategory): usato per subcategory-level
CREATE OR REPLACE VIEW mart.mv_sales_by_year_brand_subcategory AS
SELECT
  EXTRACT(YEAR FROM date) AS year,
  brand_id,
  brand_name,
  category_id,
  parent_category_id,
  SUM(gross_pln) AS gross_pln,
  SUM(net_pln) AS net_pln,
  SUM(units) AS units,
  SUM(CASE WHEN promo_flag THEN 1 ELSE 0 END) AS promo_days,
  ROUND(LEAST(100, COALESCE(
    SUM(CASE WHEN promo_flag THEN discount_depth_pct * gross_pln ELSE 0 END)
    / NULLIF(SUM(CASE WHEN promo_flag THEN gross_pln ELSE 0 END), 0)
  , 0)), 1) AS discount_depth_pct
FROM mart.fact_sales_daily
WHERE date IS NOT NULL
GROUP BY year, brand_id, brand_name, category_id, parent_category_id;
