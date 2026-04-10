-- =============================================================================
-- Cleanup idempotente dataset mart (eseguire manualmente in BigQuery Console o bq query).
-- DROP IF EXISTS: tabelle di test legacy, eventuali MV vuote / oggetti non più usati.
-- Sicuro da ripetere: ignora errori "Not found" se l'oggetto non esiste.
-- =============================================================================

-- Pool / test (nomi comuni da ambienti di prova)
DROP TABLE IF EXISTS `mart.product_pool_test`;
DROP TABLE IF EXISTS `mart.product_pool_test2`;

-- Se in passato sono state create MATERIALIZED VIEW con prefisso mv_sales_* e non servono
-- (le dashboard usano precalc_* e le VIEW in materialized_views.sql sono VIEW classiche):
-- DROP MATERIALIZED VIEW IF EXISTS `mart.mv_sales_by_year_brand_parent`;
-- DROP MATERIALIZED VIEW IF EXISTS `mart.mv_sales_by_year_brand_subcategory`;
