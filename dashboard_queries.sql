-- =============================================================================
-- DASHBOARD QUERIES - Retailer PL (PLN)
-- 1) BASIC  2) PROMO PERFORMANCE  3) CUSTOMER
-- Baseline: media 4 settimane precedenti (stessa weekday); uplift = during - baseline
-- =============================================================================

SET search_path = analytics_mart, core_commerce, promotions_marketing, digital_analytics;

-- -----------------------------------------------------------------------------
-- 1) BASIC DASHBOARD
-- -----------------------------------------------------------------------------

-- 1.1 Category sales (PLN) per categoria e periodo
SELECT
  d.date,
  d.year,
  d.quarter,
  d.month,
  c.category_id,
  c.category_name,
  c.category_path,
  sum(f.gross_pln) AS gross_pln,
  sum(f.net_pln) AS net_pln,
  sum(f.units) AS units,
  sum(f.orders) AS orders
FROM analytics_mart.fact_sales_daily f
JOIN analytics_mart.dim_date d ON d.date_key = f.date_key
JOIN analytics_mart.dim_category c ON c.category_id = f.category_id
WHERE d.date BETWEEN :period_start AND :period_end
  AND c.level = 1  -- macro categoria
GROUP BY d.date, d.year, d.quarter, d.month, c.category_id, c.category_name, c.category_path
ORDER BY d.date, gross_pln DESC;

-- 1.2 Promo share of sales (quota vendite in promo sul totale)
WITH totals AS (
  SELECT
    sum(gross_pln) AS total_gross,
    sum(CASE WHEN promo_flag THEN gross_pln ELSE 0 END) AS promo_gross
  FROM analytics_mart.fact_sales_daily f
  JOIN analytics_mart.dim_date d ON d.date_key = f.date_key
  WHERE d.date BETWEEN :period_start AND :period_end
)
SELECT
  total_gross,
  promo_gross,
  round(100.0 * promo_gross / NULLIF(total_gross, 0), 2) AS promo_share_pct
FROM totals;

-- 1.3 Avg incremental YoY (crescita media anno su anno, es. 2025 vs 2024)
WITH yearly AS (
  SELECT
    d.year,
    sum(f.gross_pln) AS total_gross
  FROM analytics_mart.fact_sales_daily f
  JOIN analytics_mart.dim_date d ON d.date_key = f.date_key
  WHERE d.year IN (:year_current, :year_prior)
  GROUP BY d.year
),
yoy AS (
  SELECT
    (SELECT total_gross FROM yearly WHERE year = :year_current) AS current_gross,
    (SELECT total_gross FROM yearly WHERE year = :year_prior) AS prior_gross
)
SELECT
  current_gross,
  prior_gross,
  current_gross - prior_gross AS incremental_pln,
  round(100.0 * (current_gross - prior_gross) / NULLIF(prior_gross, 0), 2) AS yoy_pct
FROM yoy;

-- 1.4 Promo ROI: (attributed_sales - costs) / costs (per promo o aggregato)
WITH promo_metrics AS (
  SELECT
    p.promo_id,
    p.promo_name,
    p.promo_type,
    sum(f.attributed_sales_pln) AS attributed_sales_pln,
    sum(f.media_cost_pln + f.discount_cost_pln) AS total_cost_pln
  FROM analytics_mart.fact_promo_performance f
  JOIN analytics_mart.dim_promo p ON p.promo_id = f.promo_id
  JOIN analytics_mart.dim_date d ON d.date_key = f.date_key
  WHERE d.date BETWEEN :period_start AND :period_end
  GROUP BY p.promo_id, p.promo_name, p.promo_type
)
SELECT
  promo_id,
  promo_name,
  promo_type,
  attributed_sales_pln,
  total_cost_pln,
  CASE WHEN total_cost_pln > 0
    THEN round((attributed_sales_pln - total_cost_pln) / total_cost_pln, 4)
    ELSE NULL
  END AS roi
FROM promo_metrics
ORDER BY roi DESC NULLS LAST;

-- 1.5 Avg discount depth (profondità sconto medio su ordini in promo)
SELECT
  round(avg(100.0 * oi.discount_gross_pln / NULLIF(oi.qty * (oi.unit_gross_pln + oi.discount_gross_pln / oi.qty), 0)), 2) AS avg_discount_depth_pct
FROM core_commerce.order_items oi
JOIN core_commerce.orders o ON o.order_id = oi.order_id
WHERE oi.promo_applied_id IS NOT NULL
  AND o.status NOT IN ('cancelled')
  AND o.order_ts BETWEEN :period_start AND :period_end;

-- Alternativa: discount vs prezzo di listino (base_price da products)
SELECT
  round(avg(100.0 * oi.discount_gross_pln / NULLIF(oi.qty * p.base_price_pln, 0)), 2) AS avg_discount_depth_pct
FROM core_commerce.order_items oi
JOIN core_commerce.products p ON p.product_id = oi.product_id
JOIN core_commerce.orders o ON o.order_id = oi.order_id
WHERE oi.promo_applied_id IS NOT NULL
  AND o.status NOT IN ('cancelled')
  AND o.order_ts BETWEEN :period_start AND :period_end;

-- 1.6 Peak events dependence (vendite durante Black Friday, Xmas, Back-to-school vs resto)
SELECT
  d.peak_event,
  count(DISTINCT f.date_key) AS days_count,
  sum(f.gross_pln) AS gross_pln,
  sum(f.orders) AS orders,
  round(sum(f.gross_pln) / NULLIF(count(DISTINCT f.date_key), 0), 2) AS avg_daily_gross
FROM analytics_mart.fact_sales_daily f
JOIN analytics_mart.dim_date d ON d.date_key = f.date_key
WHERE d.date BETWEEN :period_start AND :period_end
GROUP BY d.peak_event
ORDER BY gross_pln DESC;


-- -----------------------------------------------------------------------------
-- 2) PROMO PERFORMANCE (per type/mechanics, categoria/brand)
-- -----------------------------------------------------------------------------

-- 2.1 Promo performance per tipo e categoria/brand
SELECT
  p.promo_type,
  c.category_id,
  c.category_name,
  b.brand_id,
  b.brand_name,
  sum(f.attributed_sales_pln) AS attributed_sales_pln,
  sum(f.attributed_orders) AS attributed_orders,
  sum(f.exposures) AS exposures,
  sum(f.clicks) AS clicks,
  sum(f.media_cost_pln + f.discount_cost_pln) AS total_cost_pln
FROM analytics_mart.fact_promo_performance f
JOIN analytics_mart.dim_promo p ON p.promo_id = f.promo_id
JOIN analytics_mart.dim_date d ON d.date_key = f.date_key
LEFT JOIN analytics_mart.dim_category c ON c.category_id = f.category_id
LEFT JOIN analytics_mart.dim_brand b ON b.brand_id = f.brand_id
WHERE d.date BETWEEN :period_start AND :period_end
GROUP BY p.promo_type, c.category_id, c.category_name, b.brand_id, b.brand_name
ORDER BY attributed_sales_pln DESC;

-- 2.2 Promo sales uplift vs baseline (baseline = media 4 settimane precedenti, stessa weekday)
WITH promo_periods AS (
  SELECT
    promo_id,
    (start_ts AT TIME ZONE 'Europe/Warsaw')::date AS start_date,
    (end_ts AT TIME ZONE 'Europe/Warsaw')::date AS end_date
  FROM promotions_marketing.promos
  WHERE end_ts BETWEEN :period_start AND :period_end
),
daily_sales AS (
  SELECT
    (o.order_ts AT TIME ZONE 'Europe/Warsaw')::date AS dt,
    extract(dow FROM (o.order_ts AT TIME ZONE 'Europe/Warsaw')::date) AS dow,
    sum(oi.qty * oi.unit_gross_pln) AS gross_pln
  FROM core_commerce.orders o
  JOIN core_commerce.order_items oi ON oi.order_id = o.order_id
  WHERE o.status NOT IN ('cancelled')
  GROUP BY (o.order_ts AT TIME ZONE 'Europe/Warsaw')::date
),
baseline_dow AS (
  SELECT dow, avg(gross_pln) AS avg_gross_pln
  FROM daily_sales
  WHERE dt BETWEEN (:period_start - interval '28 days')::date AND :period_start - interval '1 day'
  GROUP BY dow
),
during_promo AS (
  SELECT
    pp.promo_id,
    sum(ds.gross_pln) AS during_gross
  FROM promo_periods pp
  JOIN daily_sales ds ON ds.dt BETWEEN pp.start_date AND pp.end_date
  GROUP BY pp.promo_id
),
baseline_expected AS (
  SELECT
    pp.promo_id,
    sum(b.avg_gross_pln) AS baseline_gross
  FROM promo_periods pp
  CROSS JOIN LATERAL (
    SELECT dt, extract(dow FROM dt) AS dow
    FROM generate_series(pp.start_date, pp.end_date, '1 day'::interval) AS dt
  ) days
  JOIN baseline_dow b ON b.dow = days.dow
  GROUP BY pp.promo_id
)
SELECT
  d.promo_id,
  pr.promo_name,
  pr.promo_type,
  d.during_gross,
  be.baseline_gross,
  d.during_gross - be.baseline_gross AS uplift_pln
FROM during_promo d
JOIN baseline_expected be ON be.promo_id = d.promo_id
JOIN promotions_marketing.promos pr ON pr.promo_id = d.promo_id;

-- 2.3 Promo incremental vs prior period (stesso numero di giorni prima della promo)
WITH promo_periods AS (
  SELECT
    promo_id,
    (start_ts AT TIME ZONE 'Europe/Warsaw')::date AS start_date,
    (end_ts AT TIME ZONE 'Europe/Warsaw')::date AS end_date,
    (end_ts::date - start_ts::date) + 1 AS days_len
  FROM promotions_marketing.promos
  WHERE end_ts BETWEEN :period_start AND :period_end
),
sales_during AS (
  SELECT
    pp.promo_id,
    sum(oi.qty * oi.unit_gross_pln) AS during_gross
  FROM promo_periods pp
  JOIN core_commerce.orders o ON (o.order_ts AT TIME ZONE 'Europe/Warsaw')::date BETWEEN pp.start_date AND pp.end_date
  JOIN core_commerce.order_items oi ON oi.order_id = o.order_id AND oi.promo_applied_id = pp.promo_id
  WHERE o.status NOT IN ('cancelled')
  GROUP BY pp.promo_id
),
sales_prior AS (
  SELECT
    pp.promo_id,
    sum(oi.qty * oi.unit_gross_pln) AS prior_gross
  FROM promo_periods pp
  JOIN core_commerce.orders o ON (o.order_ts AT TIME ZONE 'Europe/Warsaw')::date
    BETWEEN pp.start_date - pp.days_len AND pp.start_date - 1
  JOIN core_commerce.order_items oi ON oi.order_id = o.order_id AND oi.promo_applied_id = pp.promo_id
  WHERE o.status NOT IN ('cancelled')
  GROUP BY pp.promo_id
)
SELECT
  d.promo_id,
  p.promo_name,
  p.promo_type,
  d.during_gross,
  pr.prior_gross,
  d.during_gross - pr.prior_gross AS incremental_pln,
  round(100.0 * (d.during_gross - pr.prior_gross) / NULLIF(pr.prior_gross, 0), 2) AS incremental_pct
FROM sales_during d
JOIN sales_prior pr ON pr.promo_id = d.promo_id
JOIN promotions_marketing.promos p ON p.promo_id = d.promo_id;

-- 2.4 Post-promo sales (stesso numero di giorni del pre-promo, per confronto pre vs post)
WITH promo_periods AS (
  SELECT
    promo_id,
    (start_ts AT TIME ZONE 'Europe/Warsaw')::date AS start_date,
    (end_ts AT TIME ZONE 'Europe/Warsaw')::date AS end_date,
    (end_ts::date - start_ts::date) + 1 AS days_len
  FROM promotions_marketing.promos
  WHERE end_ts BETWEEN :period_start AND :period_end
),
pre_sales AS (
  SELECT
    pp.promo_id,
    sum(oi.qty * oi.unit_gross_pln) AS pre_gross
  FROM promo_periods pp
  JOIN core_commerce.orders o ON (o.order_ts AT TIME ZONE 'Europe/Warsaw')::date
    BETWEEN pp.start_date - pp.days_len AND pp.start_date - 1
  JOIN core_commerce.order_items oi ON oi.order_id = o.order_id
  JOIN core_commerce.products p ON p.product_id = oi.product_id
  WHERE o.status NOT IN ('cancelled')
  GROUP BY pp.promo_id
),
post_sales AS (
  SELECT
    pp.promo_id,
    sum(oi.qty * oi.unit_gross_pln) AS post_gross
  FROM promo_periods pp
  JOIN core_commerce.orders o ON (o.order_ts AT TIME ZONE 'Europe/Warsaw')::date
    BETWEEN pp.end_date + 1 AND pp.end_date + pp.days_len
  JOIN core_commerce.order_items oi ON oi.order_id = o.order_id
  JOIN core_commerce.products p ON p.product_id = oi.product_id
  WHERE o.status NOT IN ('cancelled')
  GROUP BY pp.promo_id
)
SELECT
  pr.promo_id,
  p.promo_name,
  pre.pre_gross,
  post.post_gross,
  post.post_gross - pre.pre_gross AS post_promo_dip_pln,
  round(100.0 * (post.post_gross - pre.pre_gross) / NULLIF(pre.pre_gross, 0), 2) AS post_promo_dip_pct
FROM promo_periods pr
JOIN promotions_marketing.promos p ON p.promo_id = pr.promo_id
LEFT JOIN pre_sales pre ON pre.promo_id = pr.promo_id
LEFT JOIN post_sales post ON post.promo_id = pr.promo_id;


-- -----------------------------------------------------------------------------
-- 3) CUSTOMER (base behavior: quando acquistano, quarter, dipendenza calendario promo)
-- -----------------------------------------------------------------------------

-- 3.1 Customer buy timing by quarter
SELECT
  d.year,
  d.quarter,
  count(DISTINCT f.global_user_id) AS customers_with_purchase,
  sum(f.purchases) AS total_orders,
  sum(f.spend_pln) AS total_spend_pln,
  round(avg(f.spend_pln), 2) AS avg_spend_per_customer
FROM analytics_mart.fact_customer_activity f
JOIN analytics_mart.dim_date d ON d.date_key = f.date_key
WHERE f.purchases > 0
  AND d.date BETWEEN :period_start AND :period_end
GROUP BY d.year, d.quarter
ORDER BY d.year, d.quarter;

-- 3.2 Customer dependency on promo calendar (ordini in promo vs non promo per quarter)
SELECT
  d.year,
  d.quarter,
  sum(f.promo_orders) AS promo_orders,
  sum(f.nonpromo_orders) AS nonpromo_orders,
  sum(f.purchases) AS total_orders,
  round(100.0 * sum(f.promo_orders) / NULLIF(sum(f.purchases), 0), 2) AS promo_share_pct
FROM analytics_mart.fact_customer_activity f
JOIN analytics_mart.dim_date d ON d.date_key = f.date_key
WHERE d.date BETWEEN :period_start AND :period_end
GROUP BY d.year, d.quarter
ORDER BY d.year, d.quarter;

-- 3.3 Quando acquistano (distribuzione per giorno della settimana e ora - da OLTP)
SELECT
  extract(dow FROM (o.order_ts AT TIME ZONE 'Europe/Warsaw')) AS day_of_week,
  extract(hour FROM (o.order_ts AT TIME ZONE 'Europe/Warsaw')) AS hour_of_day,
  o.channel,
  count(*) AS orders,
  sum(o.gross_pln) AS gross_pln
FROM core_commerce.orders o
WHERE o.status NOT IN ('cancelled')
  AND o.order_ts BETWEEN :period_start AND :period_end
GROUP BY extract(dow FROM (o.order_ts AT TIME ZONE 'Europe/Warsaw')),
         extract(hour FROM (o.order_ts AT TIME ZONE 'Europe/Warsaw')),
         o.channel
ORDER BY day_of_week, hour_of_day, o.channel;

-- 3.4 Placeholder HCG/segment (schema pronto; query esempio quando popolato)
-- SELECT global_user_id, segment_hcg_placeholder, count(*) AS orders, sum(spend_pln) AS spend
-- FROM analytics_mart.fact_customer_activity f
-- JOIN analytics_mart.dim_customer c ON c.global_user_id = f.global_user_id
-- WHERE c.segment_hcg_placeholder IS NOT NULL
-- GROUP BY f.global_user_id, c.segment_hcg_placeholder;


-- -----------------------------------------------------------------------------
-- PARAMETRI (esempio valori per esecuzione)
-- Sostituire :period_start, :period_end, :year_current, :year_prior
-- Es: '2025-01-01', '2025-12-31', 2025, 2024
-- -----------------------------------------------------------------------------
