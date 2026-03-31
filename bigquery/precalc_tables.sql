-- =============================================================================
-- Tabelle precalcolate per dashboard (Market Intelligence, Brand Comparison)
-- Popolate da scripts/refresh_precalc_tables.py
-- Aggiornamento: pulsante "Re-calculate dashboards" o python scripts/refresh_precalc_tables.py
-- =============================================================================

-- Aggregato vendite per (year, brand_id, category_id, parent_category_id, channel)
-- Usato per: MI sales value/volume, pie charts, promo share, discount depth
-- category_id = subcategory (101+) o parent (1-10) per aggregati parent-level
CREATE TABLE IF NOT EXISTS mart.precalc_sales_agg (
  year INT64 NOT NULL,
  brand_id INT64 NOT NULL,
  brand_name STRING,
  category_id INT64 NOT NULL,
  parent_category_id INT64 NOT NULL,
  channel STRING NOT NULL,
  gross_pln NUMERIC(14,2) NOT NULL,
  units INT64 NOT NULL,
  promo_gross NUMERIC(14,2) NOT NULL,
  discount_depth_weighted NUMERIC(14,2) NOT NULL
)
PARTITION BY RANGE_BUCKET(year, GENERATE_ARRAY(2023, 2026))
CLUSTER BY brand_id, parent_category_id, category_id, channel;

-- Aggregato peak events per (year, brand_id, category_id, parent_category_id, channel, peak_event)
CREATE TABLE IF NOT EXISTS mart.precalc_peak_agg (
  year INT64 NOT NULL,
  brand_id INT64 NOT NULL,
  category_id INT64 NOT NULL,
  parent_category_id INT64 NOT NULL,
  channel STRING NOT NULL,
  peak_event STRING NOT NULL,
  gross_pln NUMERIC(14,2) NOT NULL
)
PARTITION BY RANGE_BUCKET(year, GENERATE_ARRAY(2023, 2026))
CLUSTER BY brand_id, parent_category_id, peak_event;

-- ROI per (year, brand_id, category_id parent, promo_type) da fact_promo_performance
CREATE TABLE IF NOT EXISTS mart.precalc_roi_agg (
  year INT64 NOT NULL,
  brand_id INT64 NOT NULL,
  category_id INT64 NOT NULL,
  promo_type STRING NOT NULL,
  avg_roi NUMERIC(10,4) NOT NULL,
  incremental_sales_pln NUMERIC(14,2) NOT NULL
)
PARTITION BY RANGE_BUCKET(year, GENERATE_ARRAY(2023, 2026))
CLUSTER BY brand_id, category_id;

-- Incremental YoY: total_gross e incremental per (year, brand_id, category_id)
CREATE TABLE IF NOT EXISTS mart.precalc_incremental_yoy (
  year INT64 NOT NULL,
  brand_id INT64 NOT NULL,
  category_id INT64,
  parent_category_id INT64,
  total_gross NUMERIC(14,2) NOT NULL,
  incremental_sales_pln NUMERIC(14,2) NOT NULL
)
PARTITION BY RANGE_BUCKET(year, GENERATE_ARRAY(2023, 2026))
CLUSTER BY brand_id, parent_category_id;

-- Pie brands per category: (year, category_id, brand_id, channel, gross_pln, units, pct_value, pct_volume)
-- Precalcolato per evitare calcolo percentuali on-the-fly
CREATE TABLE IF NOT EXISTS mart.precalc_pie_brands_category (
  year INT64 NOT NULL,
  category_id INT64 NOT NULL,
  brand_id INT64 NOT NULL,
  brand_name STRING,
  channel STRING NOT NULL,
  gross_pln NUMERIC(14,2) NOT NULL,
  units INT64 NOT NULL,
  pct_value NUMERIC(5,1) NOT NULL,
  pct_volume NUMERIC(5,1) NOT NULL
)
PARTITION BY RANGE_BUCKET(year, GENERATE_ARRAY(2023, 2026))
CLUSTER BY category_id, channel;

-- Pie brands per subcategory
CREATE TABLE IF NOT EXISTS mart.precalc_pie_brands_subcategory (
  year INT64 NOT NULL,
  category_id INT64 NOT NULL,
  brand_id INT64 NOT NULL,
  brand_name STRING,
  channel STRING NOT NULL,
  gross_pln NUMERIC(14,2) NOT NULL,
  units INT64 NOT NULL,
  pct_value NUMERIC(5,1) NOT NULL,
  pct_volume NUMERIC(5,1) NOT NULL
)
PARTITION BY RANGE_BUCKET(year, GENERATE_ARRAY(2023, 2026))
CLUSTER BY category_id, channel;

-- Prev year pct per brand (market share anno precedente) - per delta
CREATE TABLE IF NOT EXISTS mart.precalc_prev_year_pct (
  year INT64 NOT NULL,
  category_id INT64 NOT NULL,
  brand_id INT64 NOT NULL,
  channel STRING NOT NULL,
  pct_value_prev NUMERIC(5,1) NOT NULL
)
PARTITION BY RANGE_BUCKET(year, GENERATE_ARRAY(2023, 2026))
CLUSTER BY category_id, channel;

-- Check Live Promo: SKU-level promo performance per (date, product, brand, promo, channel)
-- Partition by date for fast "last 7 days" / "last 30 days" queries
CREATE TABLE IF NOT EXISTS mart.precalc_promo_live_sku (
  date DATE NOT NULL,
  product_id INT64 NOT NULL,
  product_name STRING,
  brand_id INT64 NOT NULL,
  brand_name STRING,
  category_id INT64 NOT NULL,
  category_name STRING,
  parent_category_id INT64 NOT NULL,
  promo_id INT64 NOT NULL,
  promo_name STRING,
  channel STRING NOT NULL,
  gross_pln NUMERIC(14,2) NOT NULL,
  units INT64 NOT NULL,
  order_count INT64 NOT NULL
)
PARTITION BY date
CLUSTER BY brand_id, promo_id, channel;

-- Promo Creator: benchmark discount e ROI per (year, category_id, brand_id)
-- Fase 2: usato da get_promo_creator_suggestions
CREATE TABLE IF NOT EXISTS mart.precalc_promo_creator_benchmark (
  year INT64 NOT NULL,
  category_id INT64 NOT NULL,
  brand_id INT64 NOT NULL,
  media_avg_discount NUMERIC(5,1) NOT NULL,
  brand_avg_discount NUMERIC(5,1) NOT NULL,
  promo_type STRING,
  avg_roi NUMERIC(10,4)
)
PARTITION BY RANGE_BUCKET(year, GENERATE_ARRAY(2023, 2026))
CLUSTER BY category_id, brand_id;

-- Market Intelligence: breakdown segmenti per SKU (all sales), con channel '' = tutti i canali
CREATE TABLE IF NOT EXISTS mart.precalc_mi_segment_by_product (
  year INT64 NOT NULL,
  product_id INT64 NOT NULL,
  brand_id INT64 NOT NULL,
  segment_id INT64 NOT NULL,
  segment_name STRING,
  channel STRING NOT NULL,
  gross_pln NUMERIC(14,2) NOT NULL,
  units INT64 NOT NULL
)
PARTITION BY RANGE_BUCKET(year, GENERATE_ARRAY(2023, 2026))
CLUSTER BY brand_id, product_id, year;

-- Marketing: segment top categories (year, segment_id, parent_category_id or category_id, gross_pln)
CREATE TABLE IF NOT EXISTS mart.precalc_mkt_segment_categories (
  year INT64 NOT NULL,
  segment_id INT64 NOT NULL,
  category_id INT64 NOT NULL,
  parent_category_id INT64 NOT NULL,
  category_name STRING NOT NULL,
  level INT64 NOT NULL,
  gross_pln NUMERIC(14,2) NOT NULL
)
PARTITION BY RANGE_BUCKET(year, GENERATE_ARRAY(2023, 2026))
CLUSTER BY segment_id, level;

-- Marketing: segment top SKUs (year, segment_id, product_id, gross_pln, units)
CREATE TABLE IF NOT EXISTS mart.precalc_mkt_segment_skus (
  year INT64 NOT NULL,
  segment_id INT64 NOT NULL,
  product_id INT64 NOT NULL,
  product_name STRING,
  brand_name STRING,
  category_id INT64 NOT NULL,
  parent_category_id INT64 NOT NULL,
  gross_pln NUMERIC(14,2) NOT NULL,
  units INT64 NOT NULL
)
PARTITION BY RANGE_BUCKET(year, GENERATE_ARRAY(2023, 2026))
CLUSTER BY segment_id, parent_category_id;

-- Marketing: purchasing channel mix (year, segment_id, channel, gross_pln)
CREATE TABLE IF NOT EXISTS mart.precalc_mkt_purchasing_channel (
  year INT64 NOT NULL,
  segment_id INT64 NOT NULL,
  segment_name STRING,
  channel STRING NOT NULL,
  gross_pln NUMERIC(14,2) NOT NULL
)
PARTITION BY RANGE_BUCKET(year, GENERATE_ARRAY(2023, 2026))
CLUSTER BY segment_id;

-- Marketing: purchasing peak events (year, segment_id, peak_event, orders_pct, gross_pln)
CREATE TABLE IF NOT EXISTS mart.precalc_mkt_purchasing_peak (
  year INT64 NOT NULL,
  segment_id INT64 NOT NULL,
  segment_name STRING,
  peak_event STRING NOT NULL,
  orders_pct NUMERIC(5,1) NOT NULL,
  gross_pln NUMERIC(14,2) NOT NULL
)
PARTITION BY RANGE_BUCKET(year, GENERATE_ARRAY(2023, 2026))
CLUSTER BY segment_id;
