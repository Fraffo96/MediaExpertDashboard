-- =============================================================================
-- BigQuery: viste per RLS (Opzione B - uso opzionale)
-- Se preferisci isolare i dati per brand con viste invece che filtri Metabase
-- =============================================================================

-- Vista vendite solo Sodastream
CREATE OR REPLACE VIEW mart.view_sodastream_sales AS
SELECT date, brand_id, brand_name, category_id, gross_pln, net_pln, units, promo_flag, promo_id
FROM mart.fact_sales_daily
WHERE brand_name = 'Sodastream';

-- Vista vendite solo Nespresso
CREATE OR REPLACE VIEW mart.view_nespresso_sales AS
SELECT date, brand_id, brand_name, category_id, gross_pln, net_pln, units, promo_flag, promo_id
FROM mart.fact_sales_daily
WHERE brand_name = 'Nespresso';

-- Vista vendite solo Samsung
CREATE OR REPLACE VIEW mart.view_samsung_sales AS
SELECT date, brand_id, brand_name, category_id, gross_pln, net_pln, units, promo_flag, promo_id
FROM mart.fact_sales_daily
WHERE brand_name = 'Samsung';

-- Vista promo performance solo Sodastream
CREATE OR REPLACE VIEW mart.view_sodastream_promo AS
SELECT promo_id, brand_id, brand_name, date, attributed_sales_pln, discount_cost_pln, media_cost_pln, roi
FROM mart.fact_promo_performance
WHERE brand_name = 'Sodastream';

CREATE OR REPLACE VIEW mart.view_nespresso_promo AS
SELECT promo_id, brand_id, brand_name, date, attributed_sales_pln, discount_cost_pln, media_cost_pln, roi
FROM mart.fact_promo_performance
WHERE brand_name = 'Nespresso';

CREATE OR REPLACE VIEW mart.view_samsung_promo AS
SELECT promo_id, brand_id, brand_name, date, attributed_sales_pln, discount_cost_pln, media_cost_pln, roi
FROM mart.fact_promo_performance
WHERE brand_name = 'Samsung';
