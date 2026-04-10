-- =============================================================================
-- Deriva fact_sales_daily e v_sales_by_channel da fact_orders + fact_order_items
-- Fonte unica: ordini. Valore / sconto / incrementale hanno moltiplicatori sintetici
-- per brand (e brand×canale) così MI/BC non appiattiscono su confronti brand vs media.
-- Allineare eventuali SUM(oi.gross_pln) altrove allo stesso fattore ch_f se servono KPI coerenti.
-- Eseguire DOPO schema_and_seed.sql (richiede fact_orders, fact_order_items)
-- =============================================================================

-- Vista: vendite giornaliere per (date, brand, category, segment, gender, channel)
-- Usata per filtro channel su KPI/categoria/promo.
-- Feed: moltiplicatori sintetici (brand×channel sul valore, brand sulla profondità sconto) per
-- differenziare grafici MI/BC (promo share per canale, discount depth, ecc.) senza appiattire i confronti.
CREATE OR REPLACE VIEW mart.v_sales_daily_by_channel AS
SELECT
  o.date,
  p.brand_id,
  b.brand_name,
  p.subcategory_id AS category_id,
  p.category_id AS parent_category_id,
  c.segment_id,
  CASE LOWER(c.gender) WHEN 'male' THEN 'M' WHEN 'female' THEN 'F' ELSE 'M' END AS gender,
  o.channel,
  SUM(oi.gross_pln * feed_w.ch_f) AS gross_pln,
  CAST(ROUND(SUM(oi.gross_pln / 1.23 * feed_w.ch_f), 2) AS NUMERIC) AS net_pln,
  SUM(oi.quantity) AS units,
  LOGICAL_OR(o.promo_flag) AS promo_flag,
  ANY_VALUE(o.promo_id) AS promo_id,
  ROUND(IF(
    SUM(CASE WHEN o.promo_flag THEN oi.gross_pln * feed_w.ch_f ELSE 0 END) > 0,
    LEAST(100, GREATEST(4.0, COALESCE(
      SUM(CASE WHEN o.promo_flag THEN
        o.discount_depth_pct * feed_w.dd_f * oi.gross_pln * feed_w.ch_f
      ELSE 0 END)
      / NULLIF(SUM(CASE WHEN o.promo_flag THEN oi.gross_pln * feed_w.ch_f ELSE 0 END), 0)
    , 0))),
    0
  ), 1) AS discount_depth_pct
FROM mart.fact_order_items oi
JOIN mart.fact_orders o ON o.order_id = oi.order_id
JOIN mart.dim_product p ON p.product_id = oi.product_id
JOIN mart.dim_brand b ON b.brand_id = p.brand_id
JOIN mart.dim_customer c ON c.customer_id = o.customer_id
CROSS JOIN UNNEST([STRUCT(
  (0.93 + 0.14 * (MOD(ABS(FARM_FINGERPRINT(CONCAT('chx', CAST(p.brand_id AS STRING), '|', o.channel))), 1000) / 1000.0)) AS ch_f,
  (0.87 + 0.26 * (MOD(ABS(FARM_FINGERPRINT(CONCAT('ddp', CAST(p.brand_id AS STRING)))), 1000) / 1000.0))
    * (0.92 + 0.14 * (MOD(ABS(FARM_FINGERPRINT(CONCAT('dds', CAST(p.subcategory_id AS STRING)))), 1000) / 1000.0)) AS dd_f
)]) AS feed_w
GROUP BY o.date, p.brand_id, b.brand_name, p.subcategory_id, p.category_id, c.segment_id, c.gender, o.channel;

-- Ricrea fact_sales_daily aggregando da v_sales_daily_by_channel (senza channel)
-- Sostituisce i dati sintetici con dati coerenti dagli ordini
-- Partitioning + clustering per ridurre bytes scanned
-- DROP necessario se la tabella esisteva senza partitioning (BigQuery non consente di cambiarlo con OR REPLACE)
DROP TABLE IF EXISTS mart.fact_sales_daily;

CREATE TABLE mart.fact_sales_daily
PARTITION BY date
CLUSTER BY brand_id, parent_category_id, category_id
AS
SELECT
  date,
  brand_id,
  brand_name,
  category_id,
  parent_category_id,
  segment_id,
  gender,
  SUM(gross_pln * (0.90 + 0.20 * (MOD(ABS(FARM_FINGERPRINT(CONCAT('bsd', CAST(brand_id AS STRING)))), 1000) / 1000.0))) AS gross_pln,
  SUM(net_pln * (0.90 + 0.20 * (MOD(ABS(FARM_FINGERPRINT(CONCAT('bsd', CAST(brand_id AS STRING)))), 1000) / 1000.0))) AS net_pln,
  SUM(units) AS units,
  LOGICAL_OR(promo_flag) AS promo_flag,
  ANY_VALUE(promo_id) AS promo_id,
  IF(
    LOGICAL_OR(promo_flag),
    ROUND(LEAST(100, GREATEST(4.0, COALESCE(
      SUM(CASE WHEN promo_flag THEN discount_depth_pct * gross_pln ELSE 0 END)
      / NULLIF(SUM(CASE WHEN promo_flag THEN gross_pln ELSE 0 END), 0)
    , 0))), 1),
    CAST(0 AS NUMERIC)
  ) AS discount_depth_pct
FROM mart.v_sales_daily_by_channel
GROUP BY date, brand_id, brand_name, category_id, parent_category_id, segment_id, gender;

-- Ricrea fact_promo_performance (dipende da fact_sales_daily)
-- Partitioning + clustering per ridurre bytes scanned
DROP TABLE IF EXISTS mart.fact_promo_performance;

CREATE TABLE mart.fact_promo_performance (
  promo_id INT64 NOT NULL, brand_id INT64 NOT NULL, brand_name STRING NOT NULL,
  category_id INT64 NOT NULL, date DATE NOT NULL,
  attributed_sales_pln NUMERIC(14,2) NOT NULL, incremental_sales_pln NUMERIC(14,2) NOT NULL,
  discount_cost_pln NUMERIC(14,2) NOT NULL, media_cost_pln NUMERIC(14,2) NOT NULL,
  roi NUMERIC(10,4)
)
PARTITION BY date
CLUSTER BY brand_id, category_id;

INSERT mart.fact_promo_performance
  (promo_id, brand_id, brand_name, category_id, date,
   attributed_sales_pln, incremental_sales_pln, discount_cost_pln, media_cost_pln, roi)
WITH
pcfg AS (
  SELECT 1 AS pid,0.10 AS dr,0.02 AS mr,1.52 AS br UNION ALL SELECT 2,0.20,0.02,1.02 UNION ALL
  SELECT 3,0.30,0.03,0.68 UNION ALL SELECT 4,0.15,0.01,1.38 UNION ALL SELECT 5,0.15,0.02,1.18 UNION ALL
  SELECT 6,0.12,0.05,1.92 UNION ALL SELECT 7,0.08,0.01,1.48 UNION ALL SELECT 8,0.18,0.01,1.02 UNION ALL
  SELECT 9,0.22,0.08,0.88 UNION ALL SELECT 10,0.20,0.06,0.82
),
non_promo_daily AS (
  SELECT brand_id, parent_category_id, date, SUM(gross_pln) AS gross
  FROM mart.fact_sales_daily WHERE NOT promo_flag
  GROUP BY brand_id, parent_category_id, date
),
agg AS (
  SELECT f.promo_id, f.brand_id, MAX(f.brand_name) AS bn, f.parent_category_id AS cid, f.date,
    SUM(f.gross_pln) AS att
  FROM mart.fact_sales_daily f WHERE f.promo_flag AND f.promo_id IS NOT NULL
  GROUP BY f.promo_id, f.brand_id, f.parent_category_id, f.date
),
baseline AS (
  SELECT a.promo_id, a.brand_id, a.bn, a.cid, a.date, a.att,
    AVG(np.gross) AS bl
  FROM agg a
  LEFT JOIN non_promo_daily np
    ON np.brand_id = a.brand_id AND np.parent_category_id = a.cid
    AND np.date BETWEEN DATE_SUB(a.date, INTERVAL 28 DAY) AND DATE_SUB(a.date, INTERVAL 1 DAY)
  GROUP BY a.promo_id, a.brand_id, a.bn, a.cid, a.date, a.att
),
yadj AS (
  SELECT 2023 AS yr, 1.15 AS ra UNION ALL SELECT 2024, 1.00 UNION ALL SELECT 2025, 0.85 UNION ALL
  SELECT 2026 AS yr, 0.85 AS ra
),
wc AS (
  SELECT b.promo_id, b.brand_id, b.bn, b.cid, b.date, b.att,
    ROUND(b.att*p.dr,2) AS dc, ROUND(b.att*p.mr,2) AS mc,
    GREATEST(0, ROUND(
      (b.att - COALESCE(b.bl, b.att*0.4))
      * (0.91 + 0.18 * (MOD(ABS(FARM_FINGERPRINT(CONCAT('inc', CAST(b.brand_id AS STRING)))), 1000) / 1000.0)),
      2)) AS inc,
    ROUND(
      (
        (p.br * y.ra + 0.04 * (MOD(ABS(FARM_FINGERPRINT(CONCAT(CAST(b.date AS STRING), CAST(b.promo_id AS STRING), CAST(b.brand_id AS STRING)))), 21) - 10) / 10.0)
        * (0.76 + 0.42 * (MOD(ABS(FARM_FINGERPRINT(CONCAT('bmul', CAST(b.brand_id AS STRING)))), 1000) / 1000.0))
        + 0.28 * (MOD(ABS(FARM_FINGERPRINT(CONCAT('padj', CAST(b.brand_id AS STRING), '|', CAST(b.promo_id AS STRING)))), 21) - 10) / 10.0
      )
      * (0.80 + 0.42 * (MOD(ABS(FARM_FINGERPRINT(CONCAT('pcat', CAST(b.cid AS STRING)))), 1000) / 1000.0))
      * (0.74 + 0.48 * (MOD(ABS(FARM_FINGERPRINT(CONCAT('ptype', COALESCE(dm.promo_type, 'na'), '|', CAST(b.cid AS STRING)))), 1000) / 1000.0)),
      4) AS roi
  FROM baseline b
  JOIN pcfg p ON p.pid = b.promo_id
  JOIN yadj y ON y.yr = EXTRACT(YEAR FROM b.date)
  LEFT JOIN mart.dim_promo dm ON dm.promo_id = b.promo_id
)
SELECT promo_id,brand_id,bn,cid,date,
  CAST(att AS NUMERIC),CAST(inc AS NUMERIC),CAST(dc AS NUMERIC),CAST(mc AS NUMERIC),CAST(roi AS NUMERIC)
FROM wc;
