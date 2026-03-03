-- =============================================================================
-- BigQuery: dataset raw + mart, tabelle e seed per Sodastream, Nespresso, Samsung
-- Progetto: mediaexpertdashboard
-- Periodo seed: 2025-01-01 → 2025-12-31 (picchi Black Friday, Xmas)
--
-- Esecuzione: Console BigQuery (progetto mediaexpertdashboard) oppure:
--   bq mk --dataset --location=EU mediaexpertdashboard:raw
--   bq mk --dataset --location=EU mediaexpertdashboard:mart
--   bq query --use_legacy_sql=false < bigquery/schema_and_seed.sql
-- Se CREATE SCHEMA non è supportato, crea i dataset con: bq mk --dataset ...
-- =============================================================================

-- -----------------------------------------------------------------------------
-- DATASETS (se già creati da script, salta o usa IF NOT EXISTS)
-- -----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS raw
  OPTIONS(
    location = "EU"
  );

CREATE SCHEMA IF NOT EXISTS mart
  OPTIONS(
    location = "EU"
  );

-- -----------------------------------------------------------------------------
-- MART: dim_brand
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE mart.dim_brand (
  brand_id   INT64 NOT NULL,
  brand_name STRING NOT NULL
);

INSERT mart.dim_brand (brand_id, brand_name) VALUES
  (1, 'Sodastream'),
  (2, 'Nespresso'),
  (3, 'Samsung');

-- -----------------------------------------------------------------------------
-- MART: dim_category
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE mart.dim_category (
  category_id   INT64 NOT NULL,
  category_name STRING NOT NULL,
  level         INT64 NOT NULL
);

INSERT mart.dim_category (category_id, category_name, level) VALUES
  (1, 'AGD małe', 1),
  (2, 'TV, Audio i RTV', 1),
  (3, 'Smartfony i zegarki', 1),
  (4, 'Dom', 1),
  (5, 'Supermarket', 1);

-- -----------------------------------------------------------------------------
-- MART: dim_date (2025)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE mart.dim_date AS
SELECT
  FORMAT_DATE('%Y%m%d', d) AS date_key,
  d AS date,
  EXTRACT(WEEK FROM d) AS week,
  EXTRACT(MONTH FROM d) AS month,
  EXTRACT(QUARTER FROM d) AS quarter,
  EXTRACT(YEAR FROM d) AS year,
  EXTRACT(DAYOFWEEK FROM d) AS day_of_week,
  (EXTRACT(MONTH FROM d) = 11 AND EXTRACT(DAY FROM d) >= 22) AS is_black_friday_week,
  (EXTRACT(MONTH FROM d) = 12) AS is_xmas_period,
  (EXTRACT(MONTH FROM d) = 8 OR (EXTRACT(MONTH FROM d) = 9 AND EXTRACT(DAY FROM d) <= 15)) AS is_back_to_school
FROM UNNEST(GENERATE_DATE_ARRAY('2025-01-01', '2025-12-31')) AS d;

-- -----------------------------------------------------------------------------
-- MART: dim_promo
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE mart.dim_promo (
  promo_id   INT64 NOT NULL,
  promo_name STRING NOT NULL,
  promo_type STRING NOT NULL
);

INSERT mart.dim_promo (promo_id, promo_name, promo_type) VALUES
  (1, 'Black Friday - AGD', 'category_discount'),
  (2, 'Black Friday - TV', 'sitewide_discount'),
  (3, 'Święta - bundle', 'bundle'),
  (4, 'Hit Dnia Sodastream', 'hit_dnia'),
  (5, 'Nespresso Cashback', 'cashback'),
  (6, 'Samsung App Only', 'app_only'),
  (7, 'Xmas sitewide', 'sitewide_discount'),
  (8, 'Drugi za 1 zł', 'drugi_produkt_1zl');

-- -----------------------------------------------------------------------------
-- MART: fact_sales_daily (con brand_name per RLS Metabase)
-- Sodastream, Nespresso, Samsung - 2025 con picchi BF e Xmas
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE mart.fact_sales_daily (
  date        DATE NOT NULL,
  brand_id    INT64 NOT NULL,
  brand_name  STRING NOT NULL,  -- per Row Level Security in Metabase
  category_id INT64 NOT NULL,
  gross_pln    NUMERIC(14, 2) NOT NULL,
  net_pln      NUMERIC(14, 2) NOT NULL,
  units        INT64 NOT NULL,
  promo_flag   BOOL NOT NULL,
  promo_id     INT64
);

-- Seed: generiamo dati giornalieri per 2025 con picchi Black Friday (nov) e Xmas (dic)
-- Usiamo una CTE che espande date x brand x category e applica moltiplicatori per BF/Xmas
INSERT mart.fact_sales_daily (date, brand_id, brand_name, category_id, gross_pln, net_pln, units, promo_flag, promo_id)
WITH brands AS (
  SELECT 1 AS brand_id, 'Sodastream' AS brand_name UNION ALL
  SELECT 2, 'Nespresso' UNION ALL
  SELECT 3, 'Samsung'
),
categories AS (SELECT category_id FROM mart.dim_category WHERE level = 1),
date_series AS (
  SELECT d FROM UNNEST(GENERATE_DATE_ARRAY('2025-01-01', '2025-12-31')) AS d
),
daily_base AS (
  SELECT
    d AS date,
    b.brand_id,
    b.brand_name,
    c.category_id,
    -- base giornaliera variabile per brand/categoria (PLN)
    MOD(ABS(FARM_FINGERPRINT(CONCAT(CAST(d AS STRING), CAST(b.brand_id AS STRING), CAST(c.category_id AS STRING)))), 5000) + 2000 AS base_gross,
    MOD(ABS(FARM_FINGERPRINT(CONCAT(CAST(d AS STRING), CAST(b.brand_id AS STRING), 'u'))), 50) + 10 AS base_units
  FROM date_series
  CROSS JOIN brands b
  CROSS JOIN categories c
),
peak_multiplier AS (
  SELECT
    *,
    CASE
      WHEN EXTRACT(MONTH FROM date) = 11 AND EXTRACT(DAY FROM date) >= 22 THEN 2.8
      WHEN EXTRACT(MONTH FROM date) = 12 THEN 2.2
      WHEN EXTRACT(MONTH FROM date) = 8 OR (EXTRACT(MONTH FROM date) = 9 AND EXTRACT(DAY FROM date) <= 15) THEN 1.3
      ELSE 1.0
    END AS mult
  FROM daily_base
),
with_promo AS (
  SELECT
    date,
    brand_id,
    brand_name,
    category_id,
    base_gross * mult AS gross_pln,
    ROUND(base_gross * mult / 1.23, 2) AS net_pln,
    base_units * CAST(mult AS INT64) AS units,
    -- ~35% dei giorni con promo
    MOD(ABS(FARM_FINGERPRINT(CONCAT(CAST(date AS STRING), CAST(brand_id AS STRING)))), 100) < 35 AS promo_flag,
    MOD(ABS(FARM_FINGERPRINT(CONCAT(CAST(date AS STRING), CAST(brand_id AS STRING)))), 8) + 1 AS promo_id
  FROM peak_multiplier
)
SELECT
  date,
  brand_id,
  brand_name,
  category_id,
  ROUND(gross_pln, 2) AS gross_pln,
  net_pln,
  units,
  promo_flag,
  IF(promo_flag, promo_id, NULL) AS promo_id
FROM with_promo;

-- -----------------------------------------------------------------------------
-- MART: fact_promo_performance (con brand_name per RLS)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE mart.fact_promo_performance (
  promo_id            INT64 NOT NULL,
  brand_id             INT64 NOT NULL,
  brand_name           STRING NOT NULL,
  date                 DATE NOT NULL,
  attributed_sales_pln NUMERIC(14, 2) NOT NULL,
  discount_cost_pln    NUMERIC(14, 2) NOT NULL,
  media_cost_pln       NUMERIC(14, 2) NOT NULL,
  roi                  NUMERIC(10, 4)
);

-- Seed: una riga per (promo, brand, date) nei periodi promo, con costi e ROI
INSERT mart.fact_promo_performance (promo_id, brand_id, brand_name, date, attributed_sales_pln, discount_cost_pln, media_cost_pln, roi)
WITH brands AS (
  SELECT 1 AS brand_id, 'Sodastream' AS brand_name UNION ALL
  SELECT 2, 'Nespresso' UNION ALL
  SELECT 3, 'Samsung'
),
-- promos attive in periodi noti
promo_dates AS (
  SELECT 1 AS promo_id, DATE('2025-11-22') AS d_start, DATE('2025-11-30') AS d_end UNION ALL
  SELECT 2, DATE('2025-11-22'), DATE('2025-11-30') UNION ALL
  SELECT 3, DATE('2025-12-01'), DATE('2025-12-24') UNION ALL
  SELECT 4, DATE('2025-06-01'), DATE('2025-06-07') UNION ALL
  SELECT 5, DATE('2025-03-10'), DATE('2025-03-16') UNION ALL
  SELECT 6, DATE('2025-11-25'), DATE('2025-11-30') UNION ALL
  SELECT 7, DATE('2025-12-01'), DATE('2025-12-31') UNION ALL
  SELECT 8, DATE('2025-12-15'), DATE('2025-12-22')
),
days AS (
  SELECT promo_id, d
  FROM promo_dates,
  UNNEST(GENERATE_DATE_ARRAY(d_start, d_end)) AS d
),
agg AS (
  SELECT
    f.promo_id,
    f.brand_id,
    f.brand_name,
    f.date,
    SUM(f.gross_pln) AS attributed_sales_pln
  FROM mart.fact_sales_daily f
  WHERE f.promo_flag AND f.promo_id IS NOT NULL
  GROUP BY f.promo_id, f.brand_id, f.brand_name, f.date
),
with_costs AS (
  SELECT
    a.promo_id,
    a.brand_id,
    a.brand_name,
    a.date,
    a.attributed_sales_pln,
    ROUND(a.attributed_sales_pln * 0.08, 2) AS discount_cost_pln,
    ROUND(a.attributed_sales_pln * 0.02, 2) AS media_cost_pln
  FROM agg a
)
SELECT
  promo_id,
  brand_id,
  brand_name,
  date,
  attributed_sales_pln,
  discount_cost_pln,
  media_cost_pln,
  ROUND((attributed_sales_pln - discount_cost_pln - media_cost_pln) / NULLIF(discount_cost_pln + media_cost_pln, 0), 4) AS roi
FROM with_costs;
