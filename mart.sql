-- =============================================================================
-- ANALYTICS_MART - Star schema (views / materialized) per dashboard
-- Dipende da: core_commerce, promotions_marketing, digital_analytics
-- =============================================================================

SET search_path = analytics_mart, pg_catalog;

-- -----------------------------------------------------------------------------
-- DIM_DATE (calendario con peak events)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW analytics_mart.dim_date AS
WITH date_series AS (
  SELECT d::date AS dt
  FROM generate_series(
    '2024-01-01'::date,
    '2026-12-31'::date,
    '1 day'::interval
  ) AS d
),
week_attrs AS (
  SELECT
    dt,
    to_char(dt, 'YYYYMMDD')::int AS date_key,
    extract(week FROM dt)::int AS week_of_year,
    extract(month FROM dt)::int AS month_num,
    extract(quarter FROM dt)::int AS quarter_num,
    extract(year FROM dt)::int AS year_num,
    to_char(dt, 'Dy') AS day_name_short,
    extract(dow FROM dt)::int AS day_of_week
  FROM date_series
),
peak_flags AS (
  SELECT
    *,
    -- Black Friday: ultima settimana novembre (es. 22-30 nov)
    (extract(month FROM dt) = 11 AND extract(day FROM dt) >= 22) AS is_black_friday_week,
    -- Xmas: dicembre
    (extract(month FROM dt) = 12) AS is_xmas_period,
    -- Back to school: agosto + prima metà settembre
    (extract(month FROM dt) = 8 OR (extract(month FROM dt) = 9 AND extract(day FROM dt) <= 15)) AS is_back_to_school,
    -- New Year: prima settimana gennaio
    (extract(month FROM dt) = 1 AND extract(day FROM dt) <= 7) AS is_new_year_week,
    -- Easter: approssimato aprile (1-14)
    (extract(month FROM dt) = 4 AND extract(day FROM dt) BETWEEN 1 AND 14) AS is_easter_period
  FROM week_attrs
)
SELECT
  date_key,
  dt AS date,
  week_of_year AS week,
  month_num AS month,
  quarter_num AS quarter,
  year_num AS year,
  day_name_short,
  day_of_week,
  is_black_friday_week,
  is_xmas_period,
  is_back_to_school,
  is_new_year_week,
  is_easter_period,
  CASE
    WHEN is_black_friday_week THEN 'black_friday'
    WHEN is_xmas_period THEN 'xmas'
    WHEN is_back_to_school THEN 'back_to_school'
    WHEN is_new_year_week THEN 'new_year'
    WHEN is_easter_period THEN 'easter'
    ELSE 'normal'
  END AS peak_event
FROM peak_flags;

-- -----------------------------------------------------------------------------
-- DIM_CATEGORY (da core_commerce.categories)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW analytics_mart.dim_category AS
SELECT
  category_id,
  parent_category_id,
  level,
  category_name,
  category_path
FROM core_commerce.categories;

-- -----------------------------------------------------------------------------
-- DIM_BRAND
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW analytics_mart.dim_brand AS
SELECT brand_id, brand_name
FROM core_commerce.brands;

-- -----------------------------------------------------------------------------
-- DIM_PRODUCT (surrogate-friendly, con category_path e brand)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW analytics_mart.dim_product AS
SELECT
  p.product_id,
  p.sku,
  p.brand_id,
  b.brand_name,
  p.category_id,
  c.category_name,
  c.category_path,
  c.level AS category_level,
  p.name AS product_name,
  p.base_price_pln,
  p.vat_rate,
  p.cost_pln,
  p.launch_date,
  p.is_outlet_flag
FROM core_commerce.products p
JOIN core_commerce.brands b ON b.brand_id = p.brand_id
JOIN core_commerce.categories c ON c.category_id = p.category_id;

-- -----------------------------------------------------------------------------
-- DIM_PROMO
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW analytics_mart.dim_promo AS
SELECT
  promo_id,
  promo_name,
  promo_type,
  start_ts,
  end_ts,
  funding_type,
  planned_budget_pln,
  notes
FROM promotions_marketing.promos;

-- -----------------------------------------------------------------------------
-- DIM_CHANNEL (canali ordini: web, app, store)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW analytics_mart.dim_channel AS
SELECT 1 AS channel_id, 'web' AS channel_name
UNION ALL SELECT 2, 'app'
UNION ALL SELECT 3, 'store';

-- -----------------------------------------------------------------------------
-- DIM_CUSTOMER (base only; HCG/segment = placeholder)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW analytics_mart.dim_customer AS
SELECT
  global_user_id,
  created_at AS customer_since,
  country,
  marketing_opt_in,
  first_purchase_at,
  is_employee_flag,
  NULL::varchar(50) AS segment_hcg_placeholder  -- da popolare in seguito
FROM core_commerce.users;

-- -----------------------------------------------------------------------------
-- FACT_SALES_DAILY (aggregato giornaliero per categoria, brand, canale, promo)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW analytics_mart.fact_sales_daily AS
SELECT
  to_char((o.order_ts AT TIME ZONE 'Europe/Warsaw')::date, 'YYYYMMDD')::int AS date_key,
  p.category_id,
  p.brand_id,
  o.channel,
  (oi.promo_applied_id IS NOT NULL) AS promo_flag,
  oi.promo_applied_id AS promo_id,
  sum(oi.qty * oi.unit_gross_pln) AS gross_pln,
  sum(oi.qty * oi.unit_net_pln) AS net_pln,
  sum(oi.qty) AS units,
  count(DISTINCT o.order_id) AS orders
FROM core_commerce.orders o
JOIN core_commerce.order_items oi ON oi.order_id = o.order_id
JOIN core_commerce.products p ON p.product_id = oi.product_id
WHERE o.status NOT IN ('cancelled')
GROUP BY
  (o.order_ts AT TIME ZONE 'Europe/Warsaw')::date,
  o.channel,
  p.category_id,
  p.brand_id,
  (oi.promo_applied_id IS NOT NULL),
  oi.promo_applied_id;

-- Versione materializzata (opzionale, per performance)
-- CREATE MATERIALIZED VIEW analytics_mart.fact_sales_daily_mv AS SELECT * FROM analytics_mart.fact_sales_daily;

-- -----------------------------------------------------------------------------
-- FACT_PROMO_PERFORMANCE (per promo, data, categoria/brand: exposures, clicks, attributed sales, costi, roi, uplift)
-- Baseline = media 4 settimane precedenti (stessa weekday); uplift = during - baseline
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW analytics_mart.fact_promo_performance AS
WITH promo_dates AS (
  SELECT
    promo_id,
    (start_ts AT TIME ZONE 'Europe/Warsaw')::date AS start_date,
    (end_ts AT TIME ZONE 'Europe/Warsaw')::date AS end_date
  FROM promotions_marketing.promos
),
daily_sales_by_promo AS (
  SELECT
    to_char((o.order_ts AT TIME ZONE 'Europe/Warsaw')::date, 'YYYYMMDD')::int AS date_key,
    oi.promo_applied_id AS promo_id,
    p.category_id,
    p.brand_id,
    sum(oi.qty * oi.unit_gross_pln) AS attributed_sales_pln,
    count(DISTINCT o.order_id) AS attributed_orders
  FROM core_commerce.orders o
  JOIN core_commerce.order_items oi ON oi.order_id = o.order_id
  JOIN core_commerce.products p ON p.product_id = oi.product_id
  WHERE o.status NOT IN ('cancelled')
    AND oi.promo_applied_id IS NOT NULL
  GROUP BY (o.order_ts AT TIME ZONE 'Europe/Warsaw')::date, oi.promo_applied_id, p.category_id, p.brand_id
),
exposures_daily AS (
  SELECT
    to_char((exposure_ts AT TIME ZONE 'Europe/Warsaw')::date, 'YYYYMMDD')::int AS date_key,
    promo_id,
    count(*) AS exposures
  FROM promotions_marketing.promo_exposures
  GROUP BY (exposure_ts AT TIME ZONE 'Europe/Warsaw')::date, promo_id
),
clicks_daily AS (
  SELECT
    to_char((c.click_ts AT TIME ZONE 'Europe/Warsaw')::date, 'YYYYMMDD')::int AS date_key,
    e.promo_id,
    count(*) AS clicks
  FROM promotions_marketing.promo_clicks c
  JOIN promotions_marketing.promo_exposures e ON e.exposure_id = c.exposure_id
  GROUP BY (c.click_ts AT TIME ZONE 'Europe/Warsaw')::date, e.promo_id
),
costs_by_promo AS (
  SELECT
    promo_id,
    sum(CASE WHEN cost_type IN ('media_spend', 'influencer_fee', 'coop_fee') THEN cost_pln ELSE 0 END) AS media_cost_pln,
    sum(CASE WHEN cost_type IN ('discount_cost', 'cashback_cost') THEN cost_pln ELSE 0 END) AS discount_cost_pln
  FROM promotions_marketing.promo_costs
  GROUP BY promo_id
)
SELECT
  COALESCE(s.promo_id, ex.promo_id, cl.promo_id) AS promo_id,
  COALESCE(s.date_key, ex.date_key, cl.date_key) AS date_key,
  s.category_id,
  s.brand_id,
  COALESCE(ex.exposures, 0) AS exposures,
  COALESCE(cl.clicks, 0) AS clicks,
  COALESCE(s.attributed_orders, 0) AS attributed_orders,
  COALESCE(s.attributed_sales_pln, 0) AS attributed_sales_pln,
  COALESCE(c.media_cost_pln, 0) AS media_cost_pln,
  COALESCE(c.discount_cost_pln, 0) AS discount_cost_pln,
  NULL::numeric AS roi,
  NULL::numeric AS uplift_vs_baseline
FROM daily_sales_by_promo s
FULL OUTER JOIN exposures_daily ex ON ex.promo_id = s.promo_id AND ex.date_key = s.date_key
FULL OUTER JOIN clicks_daily cl ON cl.promo_id = COALESCE(s.promo_id, ex.promo_id) AND cl.date_key = COALESCE(s.date_key, ex.date_key)
LEFT JOIN costs_by_promo c ON c.promo_id = COALESCE(s.promo_id, ex.promo_id, cl.promo_id);

-- -----------------------------------------------------------------------------
-- FACT_CUSTOMER_ACTIVITY (per date_key, global_user_id: sessions, events, purchases, spend, promo_orders, nonpromo_orders)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW analytics_mart.fact_customer_activity AS
WITH order_activity AS (
  SELECT
    o.global_user_id,
    to_char((o.order_ts AT TIME ZONE 'Europe/Warsaw')::date, 'YYYYMMDD')::int AS date_key,
    count(*) AS purchases,
    sum(o.gross_pln) AS spend_pln,
    count(*) FILTER (WHERE EXISTS (SELECT 1 FROM core_commerce.order_items oi WHERE oi.order_id = o.order_id AND oi.promo_applied_id IS NOT NULL)) AS promo_orders,
    count(*) FILTER (WHERE NOT EXISTS (SELECT 1 FROM core_commerce.order_items oi WHERE oi.order_id = o.order_id AND oi.promo_applied_id IS NOT NULL)) AS nonpromo_orders
  FROM core_commerce.orders o
  WHERE o.status NOT IN ('cancelled')
    AND o.global_user_id IS NOT NULL
  GROUP BY o.global_user_id, (o.order_ts AT TIME ZONE 'Europe/Warsaw')::date
),
session_activity AS (
  SELECT
    s.global_user_id,
    to_char((s.session_start_ts AT TIME ZONE 'Europe/Warsaw')::date, 'YYYYMMDD')::int AS date_key,
    count(DISTINCT s.session_id) AS sessions,
    count(e.event_id) AS events
  FROM digital_analytics.sessions s
  LEFT JOIN digital_analytics.events e ON e.session_id = s.session_id
  WHERE s.global_user_id IS NOT NULL
  GROUP BY s.global_user_id, (s.session_start_ts AT TIME ZONE 'Europe/Warsaw')::date
)
SELECT
  COALESCE(o.global_user_id, s.global_user_id) AS global_user_id,
  COALESCE(o.date_key, s.date_key) AS date_key,
  COALESCE(s.sessions, 0)::bigint AS sessions,
  COALESCE(s.events, 0)::bigint AS events,
  COALESCE(o.purchases, 0)::bigint AS purchases,
  COALESCE(o.spend_pln, 0) AS spend_pln,
  COALESCE(o.promo_orders, 0)::bigint AS promo_orders,
  COALESCE(o.nonpromo_orders, 0)::bigint AS nonpromo_orders
FROM order_activity o
FULL OUTER JOIN session_activity s ON s.global_user_id = o.global_user_id AND s.date_key = o.date_key;
