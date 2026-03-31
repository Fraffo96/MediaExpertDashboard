"""Query per dashboard SIMULATION (what-if)."""
from google.cloud import bigquery
from app.db.client import run_query


def query_historical_baseline(ps, pe, promo_type=None, segment_id=None, category_id=None):
    """Dati storici per calcolo baseline e uplift medio."""
    params = [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
        bigquery.ScalarQueryParameter("pt", "STRING", promo_type if promo_type else None),
        bigquery.ScalarQueryParameter("seg", "INT64", int(segment_id) if segment_id else None),
        bigquery.ScalarQueryParameter("cat", "INT64", int(category_id) if category_id else None),
    ]
    q = """
    SELECT
      ROUND(AVG(CASE WHEN NOT f.promo_flag THEN f.gross_pln END), 2) AS avg_baseline_daily,
      ROUND(AVG(CASE WHEN f.promo_flag THEN f.gross_pln END), 2) AS avg_promo_daily,
      ROUND(AVG(CASE WHEN f.promo_flag THEN f.discount_depth_pct END), 2) AS avg_discount,
      COUNT(DISTINCT f.date) AS total_days,
      SUM(f.gross_pln) AS total_sales
    FROM mart.fact_sales_daily f
    WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
      AND (@seg IS NULL OR f.segment_id = @seg)
      AND (@cat IS NULL OR f.category_id = @cat)
    """
    return run_query(q, params)


def query_uplift_by_promo_type(ps, pe):
    """Uplift medio storico per tipo promo."""
    params = [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
    ]
    q = """
    SELECT p.promo_type, p.promo_mechanic,
      ROUND(AVG(fp.uplift_pct), 2) AS avg_uplift,
      ROUND(AVG(fp.roi), 4) AS avg_roi,
      COUNT(*) AS n_obs
    FROM mart.fact_promo_performance fp
    JOIN mart.dim_promo p ON p.promo_id = fp.promo_id
    WHERE fp.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
    GROUP BY p.promo_type, p.promo_mechanic ORDER BY avg_uplift DESC
    """
    return run_query(q, params)


def query_segment_response(ps, pe):
    """Response rate per segmento (storico)."""
    params = [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
    ]
    q = """
    SELECT s.segment_name, s.segment_id,
      ROUND(100.0 * SUM(a.promo_clicks) / NULLIF(SUM(a.promo_exposures), 0), 2) AS avg_ctr,
      ROUND(100.0 * SUM(a.purchases) / NULLIF(SUM(a.sessions), 0), 2) AS conversion_rate,
      ROUND(AVG(a.spend_pln), 2) AS avg_spend
    FROM mart.fact_customer_activity a
    JOIN mart.dim_segment s ON s.segment_id = a.segment_id
    WHERE a.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
    GROUP BY s.segment_name, s.segment_id ORDER BY avg_ctr DESC
    """
    return run_query(q, params)
